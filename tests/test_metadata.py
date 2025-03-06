"""Tests for metadata tracking functionality."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.exceptions import NotFound

from src.config import ConfigError
from src.metadata import (
    ensure_metadata_table_exists,
    get_incremental_filter,
    get_last_export,
    record_export_failure,
    record_export_start,
    record_export_success,
)


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    mock_client = MagicMock()
    return mock_client


def test_ensure_metadata_table_exists_table_already_exists(mock_bigquery_client):
    """Test ensuring metadata table exists when it already exists."""
    with patch("src.metadata.client", mock_bigquery_client):
        # Configure mock to indicate table exists
        mock_bigquery_client.get_table.return_value = MagicMock()

        # Call function
        ensure_metadata_table_exists("project.dataset.table")

        # Verify client was used correctly
        mock_bigquery_client.get_table.assert_called_once_with("project.dataset.table")
        mock_bigquery_client.create_table.assert_not_called()


def test_ensure_metadata_table_exists_create_table(mock_bigquery_client):
    """Test ensuring metadata table exists when it needs to be created."""
    with patch("src.metadata.client", mock_bigquery_client):
        # Configure mock to indicate table doesn't exist
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        # Call function
        ensure_metadata_table_exists("project.dataset.table")

        # Verify client was used correctly
        mock_bigquery_client.get_table.assert_called_once()
        mock_bigquery_client.create_table.assert_called_once()


def test_ensure_metadata_table_exists_create_error(mock_bigquery_client):
    """Test error handling when creating metadata table fails."""
    with patch("src.metadata.client", mock_bigquery_client):
        # Configure mock to indicate table doesn't exist and creation fails
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")
        mock_bigquery_client.create_table.side_effect = Exception("Creation failed")

        # Call function and check for error
        with pytest.raises(ConfigError, match="Failed to create metadata table"):
            ensure_metadata_table_exists("project.dataset.table")


def test_get_last_export_found(mock_bigquery_client):
    """Test retrieving last export when one exists."""
    with patch("src.metadata.client", mock_bigquery_client):
        # Create mock query result
        mock_row = MagicMock()
        mock_row.export_name = "test_export"
        mock_row.source_table = "project.dataset.source"
        mock_row.last_export_timestamp = datetime(2023, 1, 1)
        mock_row.last_exported_value = "2023-01-01"
        mock_row.rows_exported = 100
        mock_row.file_name = "export.csv"
        mock_row.export_status = "SUCCESS"
        mock_row.started_at = datetime(2023, 1, 1, 12, 0)
        mock_row.completed_at = datetime(2023, 1, 1, 12, 5)
        mock_row.error_message = None

        # Configure mock query to return our mock row
        mock_query_job = MagicMock()
        mock_query_job.__iter__.return_value = [mock_row]
        mock_bigquery_client.query.return_value = mock_query_job

        # Call function
        result = get_last_export("project.dataset.metadata", "test_export")

        # Verify expected result
        assert result["export_name"] == "test_export"
        assert result["source_table"] == "project.dataset.source"
        assert result["rows_exported"] == 100
        assert result["export_status"] == "SUCCESS"


def test_get_last_export_not_found(mock_bigquery_client):
    """Test retrieving last export when none exists."""
    with patch("src.metadata.client", mock_bigquery_client):
        # Configure mock query to return empty result
        mock_query_job = MagicMock()
        mock_query_job.__iter__.return_value = []
        mock_bigquery_client.query.return_value = mock_query_job

        # Call function
        result = get_last_export("project.dataset.metadata", "test_export")

        # Verify expected result (empty dict)
        assert result == {}


def test_record_export_start(mock_bigquery_client):
    """Test recording the start of an export."""
    with patch("src.metadata.client", mock_bigquery_client), patch("src.metadata.datetime") as mock_datetime:
        # Set a fixed current time
        mock_now = datetime(2023, 1, 1, 12, 0)
        mock_datetime.now.return_value = mock_now

        # Configure mock for successful insert
        mock_bigquery_client.insert_rows_json.return_value = []

        # Call function
        record_export_start("project.dataset.metadata", "test_export", "project.dataset.source")

        # Verify client was called correctly
        mock_bigquery_client.insert_rows_json.assert_called_once()
        args = mock_bigquery_client.insert_rows_json.call_args[0]
        assert args[0] == "project.dataset.metadata"
        assert len(args[1]) == 1  # One row
        assert args[1][0]["export_name"] == "test_export"
        assert args[1][0]["source_table"] == "project.dataset.source"
        assert args[1][0]["export_status"] == "RUNNING"
        assert args[1][0]["started_at"] == mock_now


def test_get_incremental_filter():
    """Test generating filter for incremental exports."""
    # With last exported value
    metadata = {"last_exported_value": "2023-01-01T00:00:00"}

    where_clause, params = get_incremental_filter(metadata, "updated_at")

    assert where_clause == "updated_at > @last_value"
    assert len(params) == 1
    assert params[0].name == "last_value"
    assert params[0].value == "2023-01-01T00:00:00"

    # Without last exported value
    metadata = {}

    where_clause, params = get_incremental_filter(metadata, "updated_at")

    assert where_clause == ""
    assert len(params) == 0


def test_record_export_success(mock_bigquery_client):
    """Test recording a successful export."""
    with patch("src.metadata.client", mock_bigquery_client), patch("src.metadata.datetime") as mock_datetime:
        # Set a fixed current time
        mock_now = datetime(2023, 1, 1, 12, 0)
        mock_datetime.now.return_value = mock_now

        # Configure mock for successful insert
        mock_bigquery_client.insert_rows_json.return_value = []

        # Test with all parameters
        record_export_success(
            table_id="project.dataset.metadata",
            export_name="test_export",
            source_table="project.dataset.source",
            rows_exported=100,
            file_name="export.csv",
            last_exported_value="2023-01-01T00:00:00",
            last_export_timestamp=datetime(2023, 1, 1, 11, 0),
        )

        # Verify client was called correctly
        mock_bigquery_client.insert_rows_json.assert_called_once()
        args = mock_bigquery_client.insert_rows_json.call_args[0]
        assert args[0] == "project.dataset.metadata"
        assert len(args[1]) == 1  # One row
        assert args[1][0]["export_name"] == "test_export"
        assert args[1][0]["source_table"] == "project.dataset.source"
        assert args[1][0]["rows_exported"] == 100
        assert args[1][0]["file_name"] == "export.csv"
        assert args[1][0]["last_exported_value"] == "2023-01-01T00:00:00"
        assert args[1][0]["export_status"] == "SUCCESS"

        # Reset mock
        mock_bigquery_client.reset_mock()

        # Test with minimal parameters
        record_export_success(
            table_id="project.dataset.metadata",
            export_name="test_export",
            source_table="project.dataset.source",
            rows_exported=100,
            file_name="export.csv",
        )

        # Verify default values used
        args = mock_bigquery_client.insert_rows_json.call_args[0]
        assert args[1][0]["last_exported_value"] is None
        assert args[1][0]["last_export_timestamp"] == mock_now

        # Reset mock
        mock_bigquery_client.reset_mock()

        # Test with insert error
        mock_bigquery_client.insert_rows_json.return_value = ["Error"]
        record_export_failure(
            table_id="project.dataset.metadata",
            export_name="test_export",
            source_table="project.dataset.source",
            error_message="Something went wrong",
        )
        # No assertion needed; we're just confirming it doesn't throw an exception


def test_record_export_failure(mock_bigquery_client):
    """Test recording a failed export."""
    with patch("src.metadata.client", mock_bigquery_client), patch("src.metadata.datetime") as mock_datetime:
        # Set a fixed current time
        mock_now = datetime(2023, 1, 1, 12, 0)
        mock_datetime.now.return_value = mock_now

        # Configure mock for successful insert
        mock_bigquery_client.insert_rows_json.return_value = []

        # Call function
        record_export_failure(
            table_id="project.dataset.metadata",
            export_name="test_export",
            source_table="project.dataset.source",
            error_message="Something went wrong",
        )

        # Verify client was called correctly
        mock_bigquery_client.insert_rows_json.assert_called_once()
        args = mock_bigquery_client.insert_rows_json.call_args[0]
        assert args[0] == "project.dataset.metadata"
        assert len(args[1]) == 1  # One row
        assert args[1][0]["export_name"] == "test_export"
        assert args[1][0]["source_table"] == "project.dataset.source"
        assert args[1][0]["export_status"] == "FAILED"
        assert args[1][0]["error_message"] == "Something went wrong"
        assert args[1][0]["started_at"] == mock_now
        assert args[1][0]["completed_at"] == mock_now

        # Reset mock
        mock_bigquery_client.reset_mock()

        # Test with insert error
        mock_bigquery_client.insert_rows_json.return_value = ["Error"]
        record_export_failure(
            table_id="project.dataset.metadata",
            export_name="test_export",
            source_table="project.dataset.source",
            error_message="Something went wrong",
        )
        # No assertion needed; we're just confirming it doesn't throw an exception
