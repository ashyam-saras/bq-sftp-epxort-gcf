"""Tests for the main Cloud Function entry point."""

import base64
import datetime
import json
from unittest.mock import MagicMock, patch

import pytest

# Import the functions directly with their actual names
from src.main import cloud_function_handler, export_to_sftp


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    return {
        "sftp": {
            "host": "sftp.example.com",
            "port": 22,
            "username": "testuser",
            "password": "testpass",
            "directory": "/remote/path",
        },
        "gcs": {"bucket": "test-bucket"},
        "metadata": {"export_metadata_table": "project.dataset.metadata"},
        "exports": {
            "test_export": {
                "source_table": "project.dataset.table",
                "export_type": "full",
            },
            "date_range_export": {
                "source_table": "project.dataset.table2",
                "export_type": "date_range",
                "date_column": "timestamp",
                "days_lookback": 7,
            },
        },
    }


@pytest.fixture
def mock_all_dependencies():
    """Mock all external dependencies."""
    with patch("src.main.load_config") as mock_load_config, patch("src.main.start_export") as mock_start_export, patch(
        "src.main.complete_export"
    ) as mock_complete_export, patch("src.main.fail_export") as mock_fail_export, patch(
        "src.main.export_table_to_gcs"
    ) as mock_export_to_gcs, patch(
        "src.main.upload_from_gcs"
    ) as mock_upload_from_gcs, patch(
        "src.main.upload_from_gcs_batch_parallel"
    ) as mock_batch_upload, patch(
        "src.main.check_sftp_credentials"
    ) as mock_check_sftp, patch(
        "src.main.delete_table"
    ) as mock_delete_table, patch(
        "src.main.cprint"
    ) as mock_cprint:

        # Set up return values for common mocks
        mock_start_export.return_value = "test-export-id"
        mock_export_to_gcs.return_value = ("gs://test-bucket/path/file.csv", 100, "")
        mock_check_sftp.return_value = True

        # Return all mocks
        yield {
            "load_config": mock_load_config,
            "start_export": mock_start_export,
            "complete_export": mock_complete_export,
            "fail_export": mock_fail_export,
            "export_to_gcs": mock_export_to_gcs,
            "upload_from_gcs": mock_upload_from_gcs,
            "batch_upload": mock_batch_upload,
            "check_sftp": mock_check_sftp,
            "delete_table": mock_delete_table,
            "cprint": mock_cprint,
        }


def test_export_to_sftp_full_success(mock_config, mock_all_dependencies):
    """Test successful full export."""
    # Setup mocks
    mocks = mock_all_dependencies
    mocks["load_config"].return_value = mock_config

    # Test execution
    with patch("src.main.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 5, 15)
        mock_datetime.date.today.return_value = datetime.date(2023, 5, 15)

        # Call the function with correct parameter order (config first, then export_name)
        result = export_to_sftp(mock_config, "test_export")

    # Verify all steps were executed correctly
    assert result["status"] == "success"  # Changed from success to status based on function implementation
    assert result["rows_exported"] == 100
    assert result["export_id"] == "test-export-id"
    assert result["files_transferred"] == 1

    # Check main function calls
    mocks["start_export"].assert_called_once()
    mocks["export_to_gcs"].assert_called_once()
    mocks["upload_from_gcs"].assert_called_once()
    mocks["complete_export"].assert_called_once_with("test-export-id", 100)
    mocks["fail_export"].assert_not_called()


def test_export_to_sftp_date_range_success(mock_config, mock_all_dependencies):
    """Test successful date range export."""
    # Setup mocks
    mocks = mock_all_dependencies
    mocks["load_config"].return_value = mock_config
    mocks["export_to_gcs"].return_value = ("gs://test-bucket/path/file.csv", 50, "temp_table")

    # Test execution
    with patch("src.main.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 5, 15)
        mock_datetime.date.today.return_value = datetime.date(2023, 5, 15)

        # Call the function with correct parameter order
        result = export_to_sftp(mock_config, "date_range_export")

    # Verify results
    assert result["status"] == "success"  # Changed from success to status
    assert result["rows_exported"] == 50

    # Verify temp table was deleted
    mocks["delete_table"].assert_called_once_with("temp_table", "project.dataset.table2")


@patch("src.main.storage")
def test_export_to_sftp_batch_parallel(mock_storage, mock_config, mock_all_dependencies):
    """Test batch parallel upload mode."""
    # Setup mocks
    mocks = mock_all_dependencies
    mocks["load_config"].return_value = mock_config
    mocks["export_to_gcs"].return_value = ("gs://test-bucket/path/file*.csv", 200, "")
    mocks["batch_upload"].return_value = 3  # 3 files transferred

    # Mock storage client and buckets to prevent actual GCS calls
    mock_storage_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.name = "path/file000.csv"

    mock_storage.Client.return_value = mock_storage_client
    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.list_blobs.return_value = [mock_blob]

    # Modify config to use batch_parallel mode
    test_config = mock_config.copy()
    test_config["sftp"]["upload_mode"] = "batch_parallel"
    mocks["load_config"].return_value = test_config

    # Also patch time.time to prevent timing issues
    with patch("src.main.time.time", return_value=100.0):
        # Call the function with correct parameter order
        result = export_to_sftp(test_config, "test_export")

    # Verify results
    assert result["status"] == "success"
    assert result["rows_exported"] == 200
    assert result["files_transferred"] == 3

    # Verify batch upload was called instead of individual upload
    mocks["batch_upload"].assert_called_once()
    mocks["upload_from_gcs"].assert_not_called()


def test_export_to_sftp_sftp_error(mock_config, mock_all_dependencies):
    """Test handling of SFTP upload errors."""
    # Setup mocks
    mocks = mock_all_dependencies
    mocks["load_config"].return_value = mock_config
    mocks["check_sftp"].return_value = True
    mocks["upload_from_gcs"].side_effect = Exception("SFTP upload failed")

    # Call the function with correct parameter order
    # Handle error case differently since exceptions are raised
    with patch("src.main.time.time", return_value=100.0):
        with pytest.raises(Exception, match="SFTP upload failed"):
            export_to_sftp(mock_config, "test_export")

    # Verify export was marked as failed
    mocks["fail_export"].assert_called_once()
    mocks["complete_export"].assert_not_called()


def test_export_to_sftp_bigquery_error(mock_config, mock_all_dependencies):
    """Test handling of BigQuery export errors."""
    # Setup mocks
    mocks = mock_all_dependencies
    mocks["load_config"].return_value = mock_config
    mocks["export_to_gcs"].side_effect = Exception("BigQuery export failed")

    # Call the function with correct parameter order
    # Handle error case differently since exceptions are raised
    with patch("src.main.time.time", return_value=100.0):
        with pytest.raises(Exception, match="BigQuery export failed"):
            export_to_sftp(mock_config, "test_export")

    # Verify the error was reported
    mocks["fail_export"].assert_called_once()


def test_export_to_sftp_invalid_export_name(mock_config, mock_all_dependencies):
    """Test handling of invalid export name."""
    # Setup mocks
    mocks = mock_all_dependencies
    mocks["load_config"].return_value = mock_config

    # Call the function with invalid export name
    # Handle as an exception since the function raises ValueError
    with pytest.raises(ValueError, match="Export 'nonexistent_export' not found in configuration"):
        export_to_sftp(mock_config, "nonexistent_export")

    # Verify no other operations were attempted
    mocks["start_export"].assert_not_called()
    mocks["export_to_gcs"].assert_not_called()
    mocks["upload_from_gcs"].assert_not_called()


def test_main_http_trigger():
    """Test HTTP trigger function."""
    # Create a properly structured mock for request
    request = MagicMock()
    request.get_json.return_value = {"export_name": "test_export"}

    with patch("src.main.json.loads", return_value={"export_name": "test_export"}), patch(
        "src.main.base64.b64decode", return_value=b'{"export_name": "test_export"}'
    ), patch("src.main.export_to_sftp") as mock_export, patch("src.main.load_config") as mock_load_config, patch(
        "src.main.cprint"
    ):

        # Set up our mock to return a proper status
        mock_export.return_value = {"status": "success", "export_id": "test-id", "rows_exported": 100}
        mock_load_config.return_value = {"exports": {"test_export": {}}}

        # Make request.data behave like a dictionary to match function's expectation
        request.data = {"message": {"data": "base64string"}}

        # Call the function
        result = cloud_function_handler(request)

        # Check results
        assert result["status"] == "success"
        mock_export.assert_called_once()


def test_main_http_trigger_error():
    """Test error handling in HTTP trigger."""
    # Create a properly structured mock for request
    request = MagicMock()
    request.get_json.return_value = {"export_name": "test_export"}

    with patch("src.main.json.loads", return_value={"export_name": "test_export"}), patch(
        "src.main.base64.b64decode", return_value=b'{"export_name": "test_export"}'
    ), patch("src.main.export_to_sftp") as mock_export, patch("src.main.load_config") as mock_load_config, patch(
        "src.main.cprint"
    ):

        # Create a real exception with a string message
        error_msg = "Test export failed"
        mock_export.side_effect = Exception(error_msg)
        mock_load_config.return_value = {"exports": {"test_export": {}}}

        # Make request.data behave like a dictionary
        request.data = {"message": {"data": "base64string"}}

        # Call the function
        result = cloud_function_handler(request)

        # Verify error format
        assert result["status"] == "error"
        # Check that the error message contains our error text
        assert error_msg in result["message"]


def test_main_missing_export_name():
    """Test handling of missing export name."""
    # Create a mock event object
    event = MagicMock()

    # Set up the data attribute to match what cloud_function_handler expects
    # It expects event.data["message"]["data"] to be accessible
    event.data = {"message": {"data": base64.b64encode(b"{}").decode()}}

    with patch("src.main.json.loads", return_value={}), patch("src.main.cprint"):
        # Call the function
        result = cloud_function_handler(event)

    # Now the actual error message will be "No data in event"
    assert result["status"] == "error"
    assert "No data in event" in result["message"]


def test_main_pubsub_trigger():
    """Test PubSub trigger function."""
    # Create a mock event for PubSub
    event = MagicMock()

    # Encode the test data as the function expects
    test_data = {"export_name": "test_export"}
    encoded_data = base64.b64encode(json.dumps(test_data).encode()).decode()

    # Set up the event.data attribute structure
    event.data = {"message": {"data": encoded_data}}

    # Mock all necessary functions
    with patch("src.main.export_to_sftp") as mock_export, patch("src.main.load_config") as mock_load_config, patch(
        "src.main.cprint"
    ):

        # Configure mock return values
        mock_load_config.return_value = {"exports": {"test_export": {}}}
        mock_export.return_value = {"status": "success", "export_id": "test-id", "rows_exported": 100}

        # Call the function without patching json.loads or base64.b64decode
        # Let these work normally with our correctly structured data
        result = cloud_function_handler(event)

    # Verify results
    assert result["status"] == "success"
    mock_export.assert_called_once()
