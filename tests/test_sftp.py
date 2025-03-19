"""Tests for SFTP operations."""

from pathlib import PurePosixPath
from unittest.mock import MagicMock, patch

import paramiko
import pytest

from src.config import ConfigError
from src.sftp import (
    check_sftp_credentials,
    create_sftp_connection,
    ensure_sftp_directory,
    parse_gcs_uri,
    upload_from_gcs,
    upload_from_gcs_batch,
    upload_from_gcs_batch_parallel,
)


# Fully mock paramiko to prevent any real connections
@pytest.fixture(autouse=True)
def mock_paramiko():
    """Mock all paramiko functionality to prevent real connections."""
    with patch("paramiko.Transport", autospec=True), patch("paramiko.SFTPClient", autospec=True), patch(
        "paramiko.SSHClient", autospec=True
    ):
        yield


def test_parse_gcs_uri_valid():
    """Test parsing valid GCS URIs."""
    # Test a simple URI
    bucket, blob = parse_gcs_uri("gs://bucket-name/path/to/file.csv")
    assert bucket == "bucket-name"
    assert blob == "path/to/file.csv"

    # Test a URI with no subdirectories
    bucket, blob = parse_gcs_uri("gs://bucket-name/file.csv")
    assert bucket == "bucket-name"
    assert blob == "file.csv"


def test_parse_gcs_uri_invalid():
    """Test parsing invalid GCS URIs."""
    # Test invalid prefix
    with pytest.raises(ValueError, match="Invalid GCS URI"):
        parse_gcs_uri("s3://bucket-name/path/to/file.csv")

    # Test malformed URI (missing blob path)
    with pytest.raises(ValueError, match="Invalid GCS URI format"):
        parse_gcs_uri("gs://bucket-name")


@pytest.fixture
def mock_sftp_connection():
    """Create mocks for SFTP connection."""
    mock_transport = MagicMock()
    mock_sftp = MagicMock()

    # Return the mocks directly, don't set mock_transport.return_value
    with patch("src.sftp.create_sftp_connection", return_value=(mock_transport, mock_sftp)):
        yield mock_transport, mock_sftp


@pytest.fixture
def mock_gcs():
    """Create mocks for Google Cloud Storage."""
    mock_storage_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.get_blob.return_value = mock_blob
    mock_blob.size = 1024 * 1024  # 1 MB

    with patch("src.sftp.storage.Client", return_value=mock_storage_client):
        yield mock_storage_client, mock_bucket, mock_blob


@pytest.fixture
def sftp_config():
    """Create a sample SFTP configuration."""
    return {
        "host": "sftp.example.com",
        "port": 22,
        "username": "testuser",
        "password": "testpass",
        "directory": "/remote/path",
    }


def test_upload_from_gcs(mock_sftp_connection, mock_gcs, sftp_config, tmp_path):
    """Test uploading a file from GCS to SFTP."""
    mock_transport, mock_sftp = mock_sftp_connection
    _, _, mock_blob = mock_gcs

    # Mock temporary file
    with patch("src.sftp.tempfile.NamedTemporaryFile") as mock_temp_file:
        mock_temp_file_instance = MagicMock()
        mock_temp_file_instance.name = str(tmp_path / "temp_file")
        mock_temp_file.return_value = mock_temp_file_instance

        # Mock os.path.getsize to return file size
        with patch("src.sftp.os.path.getsize", return_value=1024 * 1024):
            # Mock os.path.exists and os.unlink for temp file cleanup
            with patch("src.sftp.os.path.exists", return_value=True), patch(
                "src.sftp.os.unlink"
            ) as mock_unlink, patch(
                "src.sftp.cprint"
            ):  # Silence logging

                # Call the function being tested
                upload_from_gcs(sftp_config, "gs://bucket-name/path/to/file.csv", "remote_file.csv")

                # Verify the temporary file was created and cleaned up
                mock_unlink.assert_called_once_with(mock_temp_file_instance.name)

                # Verify the file was uploaded to SFTP
                mock_sftp.put.assert_called_once_with(mock_temp_file_instance.name, "/remote/path/remote_file.csv")

                # Verify the blob was downloaded
                mock_blob.download_to_filename.assert_called_once_with(mock_temp_file_instance.name)

                # Verify the connection was closed
                mock_sftp.close.assert_called_once()
                mock_transport.close.assert_called_once()


def test_upload_from_gcs_with_gcs_error(mock_sftp_connection, mock_gcs, sftp_config):
    """Test error handling when GCS file doesn't exist."""
    _, _, mock_blob = mock_gcs
    mock_storage_client, mock_bucket, _ = mock_gcs

    # Make get_blob return None to simulate file not found
    mock_bucket.get_blob.return_value = None

    with patch("src.sftp.cprint"):
        with pytest.raises(ConfigError, match="File not found in GCS"):
            upload_from_gcs(sftp_config, "gs://bucket-name/nonexistent.csv", "remote.csv")


def test_upload_from_gcs_batch(mock_sftp_connection, mock_gcs, sftp_config):
    """Test batch uploading files from GCS to SFTP."""
    mock_transport, mock_sftp = mock_sftp_connection

    # Mock the _download_and_upload function
    with patch("src.sftp._download_and_upload") as mock_download_upload, patch("src.sftp.cprint"):

        file_mappings = [
            ("gs://bucket/file1.csv", "remote1.csv"),
            ("gs://bucket/file2.csv", "remote2.csv"),
            ("gs://bucket/file3.csv", "remote3.csv"),
        ]

        result = upload_from_gcs_batch(sftp_config, file_mappings)

        # Verify the result and function calls
        assert result == 3  # All 3 files transferred
        assert mock_download_upload.call_count == 3

        # Verify connection was established and closed once
        mock_sftp.close.assert_called_once()
        mock_transport.close.assert_called_once()


def test_upload_from_gcs_batch_empty(sftp_config):
    """Test batch upload with empty file list."""
    with patch("src.sftp.cprint"):
        result = upload_from_gcs_batch(sftp_config, [])
        assert result == 0


def test_upload_from_gcs_batch_connection_error(sftp_config):
    """Test error handling for batch upload when connection fails."""
    with patch("src.sftp.create_sftp_connection") as mock_create_conn, patch("src.sftp.cprint"):
        # Simulate connection error
        mock_create_conn.side_effect = paramiko.ssh_exception.SSHException("Connection failed")

        with pytest.raises(ConfigError, match="SFTP batch upload failed"):
            upload_from_gcs_batch(sftp_config, [("gs://bucket/file.csv", "remote.csv")])


def test_ensure_sftp_directory_exists(mock_sftp_connection):
    """Test ensuring a directory exists when it already exists."""
    _, mock_sftp = mock_sftp_connection

    # Make stat() not raise an exception, indicating directory exists
    mock_sftp.stat.return_value = MagicMock()

    with patch("src.sftp.cprint"):
        ensure_sftp_directory(mock_sftp, PurePosixPath("/existing/dir"))

        # Should check if directory exists but not create it
        mock_sftp.stat.assert_called_once_with("/existing/dir")
        mock_sftp.mkdir.assert_not_called()


def test_ensure_sftp_directory_create(mock_sftp_connection):
    """Test creating a directory hierarchy when it doesn't exist."""
    _, mock_sftp = mock_sftp_connection

    # Make stat() raise FileNotFoundError for the main directory and subdirectories
    def mock_stat_side_effect(path):
        if path == "/":
            return MagicMock()  # Root exists
        raise FileNotFoundError("Directory not found")

    mock_sftp.stat.side_effect = mock_stat_side_effect

    with patch("src.sftp.cprint"):
        ensure_sftp_directory(mock_sftp, PurePosixPath("/new/nested/dir"))

        # Should have tried to create each directory in the path
        assert mock_sftp.mkdir.call_count == 3
        mock_sftp.mkdir.assert_any_call("/new")
        mock_sftp.mkdir.assert_any_call("/new/nested")
        mock_sftp.mkdir.assert_any_call("/new/nested/dir")


def test_check_sftp_credentials(mock_sftp_connection):
    """Test checking SFTP credentials successfully."""
    _, mock_sftp = mock_sftp_connection

    # Make the directory listing return some files
    mock_sftp.listdir.return_value = ["file1.txt", "file2.txt"]

    with patch("src.sftp.cprint"):
        result = check_sftp_credentials(
            {"host": "test.example.com", "port": 22, "username": "user", "password": "pass", "directory": "/test"}
        )

        assert result is True
        mock_sftp.listdir.assert_called_once_with("/test")
        mock_sftp.close.assert_called_once()


def test_check_sftp_credentials_directory_not_found(mock_sftp_connection):
    """Test checking SFTP credentials when directory doesn't exist."""
    _, mock_sftp = mock_sftp_connection

    # Make listdir raise FileNotFoundError
    mock_sftp.listdir.side_effect = FileNotFoundError("Directory not found")

    with patch("src.sftp.cprint"):
        result = check_sftp_credentials(
            {
                "host": "test.example.com",
                "port": 22,
                "username": "user",
                "password": "pass",
                "directory": "/nonexistent",
            }
        )

        # Should still succeed since directory will be created during upload
        assert result is True
        mock_sftp.close.assert_called_once()


def test_check_sftp_credentials_connection_error():
    """Test handling connection errors when checking credentials."""
    with patch("src.sftp.create_sftp_connection") as mock_create_connection, patch("src.sftp.cprint"):
        # Simulate connection failure
        mock_create_connection.side_effect = paramiko.ssh_exception.AuthenticationException("Auth failed")

        with pytest.raises(ConfigError, match="SFTP connection failed"):
            check_sftp_credentials(
                {"host": "test.example.com", "port": 22, "username": "user", "password": "wrong", "directory": "/test"}
            )


def test_create_sftp_connection():
    """Test creating SFTP connection."""
    mock_transport = MagicMock()
    mock_sftp_client = MagicMock()

    # Make sure we're mocking at the lowest level to prevent real connections
    with patch("paramiko.Transport", return_value=mock_transport) as mock_transport_class, patch(
        "paramiko.SFTPClient.from_transport", return_value=mock_sftp_client
    ) as mock_sftp_from_transport, patch("src.sftp.cprint"):

        # Apply direct patches to ensure we don't hit the real transport methods
        with patch.object(mock_transport, "connect"):
            transport, sftp = create_sftp_connection("test.example.com", 22, "user", "pass")

            # Verify the connection was created correctly
            mock_transport_class.assert_called_once_with(("test.example.com", 22))
            mock_transport.connect.assert_called_once_with(username="user", password="pass")
            mock_sftp_from_transport.assert_called_once_with(mock_transport)

            assert transport == mock_transport
            assert sftp == mock_sftp_client


# For the methods that call _download_and_upload internally, make sure we're mocking that
@patch("src.sftp._download_and_upload")
def test_upload_from_gcs_fully_mocked(mock_download_upload, mock_sftp_connection, mock_gcs, sftp_config, tmp_path):
    """Test uploading a file from GCS to SFTP with everything fully mocked."""
    mock_transport, mock_sftp = mock_sftp_connection
    mock_storage_client, mock_bucket, mock_blob = mock_gcs

    # Use a regular patch instead of autospec=True which causes issues
    with patch("src.sftp.storage.Client", return_value=mock_storage_client), patch(
        "src.sftp.time.time", return_value=100.0
    ), patch("src.sftp.tempfile.NamedTemporaryFile"), patch("src.sftp.os.path.exists", return_value=True), patch(
        "src.sftp.os.path.getsize", return_value=1024
    ), patch(
        "src.sftp.os.unlink"
    ), patch(
        "src.sftp.cprint"
    ):

        # Call the function
        upload_from_gcs(sftp_config, "gs://bucket-name/path/to/file.csv", "remote_file.csv")

        # Verify mocks were called correctly
        mock_download_upload.assert_called_once()
        mock_sftp.close.assert_called_once()
        mock_transport.close.assert_called_once()


# Override the batch parallel test to completely isolate it
def test_upload_from_gcs_batch_parallel_isolated(mock_gcs, sftp_config):
    """Test parallel batch upload with complete isolation."""
    file_mappings = [
        ("gs://bucket/file1.csv", "remote1.csv"),
        ("gs://bucket/file2.csv", "remote2.csv"),
    ]

    # Setup mocks for concurrent.futures
    mock_future1 = MagicMock()
    mock_future1.result.return_value = True
    mock_future2 = MagicMock()
    mock_future2.result.return_value = True

    mock_executor = MagicMock()
    mock_executor.__enter__.return_value = mock_executor
    mock_executor.submit.side_effect = [mock_future1, mock_future2]

    # Patch real functions that would be called by the parallel upload
    with patch("src.sftp.upload_from_gcs") as mock_upload, patch(
        "src.sftp.concurrent.futures.ThreadPoolExecutor", return_value=mock_executor
    ), patch("src.sftp.concurrent.futures.as_completed", return_value=[mock_future1, mock_future2]), patch(
        "src.sftp.cprint"
    ):

        # Call the function under test
        result = upload_from_gcs_batch_parallel(sftp_config, file_mappings, max_workers=2)

        # Verify results
        assert result == 2
        assert mock_executor.submit.call_count == 2


def test_upload_from_gcs_batch_parallel_with_failures(mock_gcs, sftp_config):
    """Test parallel batch upload with some failures."""
    file_mappings = [
        ("gs://bucket/file1.csv", "remote1.csv"),
        ("gs://bucket/file2.csv", "remote2.csv"),
    ]

    # Create mock futures with different results
    mock_future1 = MagicMock()
    mock_future1.result.return_value = True  # Success
    mock_future2 = MagicMock()
    mock_future2.result.return_value = False  # Failure

    # Create a mock executor that returns our prepared futures
    mock_executor = MagicMock()
    mock_executor.__enter__.return_value = mock_executor
    mock_executor.submit.side_effect = [mock_future1, mock_future2]

    with patch("src.sftp.upload_from_gcs") as mock_upload, patch(
        "src.sftp.concurrent.futures.ThreadPoolExecutor", return_value=mock_executor
    ), patch("src.sftp.concurrent.futures.as_completed", return_value=[mock_future1, mock_future2]), patch(
        "src.sftp.cprint"
    ):

        # Call the function
        result = upload_from_gcs_batch_parallel(sftp_config, file_mappings, max_workers=2)

        # Verify results - should have 1 success and 1 failure
        assert result == 1
        assert mock_executor.submit.call_count == 2
