"""
SFTP operations for the export function.
Handles connecting to SFTP and uploading files.
"""

import os
import shutil
import tempfile
import time
from pathlib import PurePosixPath
from typing import Any, Dict, Tuple

import paramiko
from google.cloud import storage

from src.config import ConfigError
from src.helpers import cprint


def parse_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
    """
    Parse a GCS URI into bucket name and blob name.

    Args:
        gcs_uri: GCS URI (gs://bucket/path/to/file)

    Returns:
        Tuple of bucket name and blob name

    Example:
        >>> parse_gcs_uri("gs://my-bucket/path/to/file.txt")
        ('my-bucket', 'path/to/file.txt')
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")

    # Remove gs:// prefix and split
    path = gcs_uri[5:]
    parts = path.split("/", 1)

    if len(parts) != 2:
        raise ValueError(f"Invalid GCS URI format: {gcs_uri}")

    return parts[0], parts[1]  # bucket_name, blob_name


def upload_from_gcs(sftp_config: Dict[str, Any], gcs_uri: str, remote_filename: str) -> None:
    """
    Upload a file from GCS to SFTP server using the specified method.

    Args:
        sftp_config: Dictionary with SFTP connection parameters
        gcs_uri: GCS URI of the file to upload
        remote_filename: Filename to use on SFTP server
    """
    # Extract common parameters
    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]
    directory = sftp_config["directory"]

    # Extract method preference (only two options now: "download" or "stream")
    upload_method = sftp_config.get("upload_method", "download")  # download or stream
    size_threshold = sftp_config.get("size_threshold", 1073741824)  # 1GB default

    # Use PurePosixPath for SFTP paths (always Unix-style)
    remote_path = PurePosixPath(directory)
    remote_file_path = remote_path / remote_filename

    cprint(
        f"Starting upload from GCS to SFTP",
        severity="INFO",
        source=gcs_uri,
        destination=f"{host}:{remote_file_path}",
        method=upload_method,
    )

    # Get GCS blob
    try:
        start_time = time.time()
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        if not blob:
            raise FileNotFoundError(f"File not found in GCS: {gcs_uri}")

        # Get blob size for reporting
        blob_size = blob.size
        cprint(f"File located in GCS", file_size=f"{blob_size / (1024*1024):.2f} MB")

        # Connect to SFTP
        cprint(f"Connecting to SFTP server at {host}:{port}", severity="INFO")
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create directories if needed
        ensure_sftp_directory(sftp, remote_path)

        # Choose method based on settings or file size
        method = upload_method
        if upload_method == "auto":
            method = "stream" if blob_size >= size_threshold else "download"
            cprint(
                f"Auto-selected '{method}' method based on file size",
                file_size=f"{blob_size/(1024*1024):.2f} MB",
                threshold=f"{size_threshold/(1024*1024):.2f} MB",
            )

        # Use the selected method
        transfer_start = time.time()
        if method == "stream":
            cprint("Using direct streaming method", severity="INFO")
            _stream_direct(sftp, blob, str(remote_file_path))
        else:
            cprint("Using download-upload method", severity="INFO")
            _download_and_upload(sftp, blob, str(remote_file_path))

        # Calculate total transfer time
        transfer_time = time.time() - transfer_start
        total_time = time.time() - start_time

        # Close connection
        sftp.close()
        transport.close()

        cprint(
            f"Upload completed successfully",
            severity="INFO",
            file=remote_filename,
            size=f"{blob_size/(1024*1024):.2f} MB",
            transfer_time=f"{transfer_time:.2f}s",
            total_time=f"{total_time:.2f}s",
        )

    except Exception as e:
        error_time = time.time() - start_time
        error_message = f"SFTP upload failed after {error_time:.2f}s: {str(e)}"
        cprint(error_message, severity="ERROR", gcs_uri=gcs_uri, destination=str(remote_file_path))
        raise ConfigError(error_message)


def ensure_sftp_directory(sftp: paramiko.SFTPClient, remote_path: PurePosixPath) -> None:
    """
    Create directory tree if it doesn't exist.

    This function checks if a directory exists on the SFTP server and creates
    it recursively if it does not exist.

    Args:
        sftp: Paramiko SFTP client connected to the server
        remote_path: Path to ensure exists on the SFTP server

    Returns:
        None
    """
    try:
        sftp.stat(str(remote_path))
        cprint(f"Directory {remote_path} exists")
    except FileNotFoundError:
        cprint(f"Creating directory path: {remote_path}")

        # Create directories recursively using pathlib
        current = PurePosixPath("/")
        for part in remote_path.parts[1:]:  # Skip the first empty part from root
            current = current / part
            try:
                sftp.stat(str(current))
            except FileNotFoundError:
                cprint(f"Creating directory: {current}")
                sftp.mkdir(str(current))


def _stream_direct(sftp: paramiko.SFTPClient, blob: storage.Blob, remote_file_path: str) -> None:
    """
    Stream directly from GCS to SFTP without chunking.

    This method opens a direct pipe between GCS and SFTP, streaming the file
    contents without storing the complete file in memory or on disk. It uses
    shutil.copyfileobj with a large buffer for efficient transfer.

    Args:
        sftp: Paramiko SFTP client connected to the server
        blob: Google Cloud Storage blob object to download
        remote_file_path: Destination path on the SFTP server

    Returns:
        None
    """
    blob_size = blob.size
    start_time = time.time()

    cprint(
        f"Starting direct stream transfer",
        severity="INFO",
        file_size=f"{blob_size/(1024*1024):.2f} MB",
        destination=remote_file_path,
    )

    with blob.open("rb") as source_file:
        with sftp.file(remote_file_path, "wb") as sftp_file:
            # Let the libraries handle the data transfer
            shutil.copyfileobj(source_file, sftp_file)

            # Report completion metrics
            total_time = time.time() - start_time
            transfer_rate = blob_size / total_time if total_time > 0 else 0

            cprint(
                f"Stream transfer completed",
                severity="INFO",
                time=f"{total_time:.2f}s",
                rate=f"{transfer_rate/(1024*1024):.2f} MB/s",
                size=f"{blob_size/(1024*1024)::.2f} MB",
            )


def _download_and_upload(sftp: paramiko.SFTPClient, blob: storage.Blob, remote_file_path: str) -> None:
    """
    Two-step process: Download to temp file then upload to SFTP.

    This method first downloads the GCS blob to a local temporary file,
    then uploads that file to the SFTP server using paramiko's optimized
    put() method. This approach is generally faster for small to medium
    sized files than streaming.

    Args:
        sftp: Paramiko SFTP client connected to the server
        blob: Google Cloud Storage blob object to download
        remote_file_path: Destination path on the SFTP server

    Returns:
        None

    Note:
        The temporary file is automatically cleaned up after the upload
        completes or if an error occurs.
    """
    overall_start = time.time()
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_path = temp_file.name
    temp_file.close()  # Close but don't delete

    try:
        # Step 1: Download
        cprint(f"Starting download from GCS", severity="INFO")
        download_start = time.time()
        blob.download_to_filename(temp_path)
        download_time = time.time() - download_start
        file_size = os.path.getsize(temp_path)

        download_rate = file_size / download_time / 1024 / 1024 if download_time > 0 else 0
        cprint(
            f"GCS download completed",
            severity="INFO",
            size=f"{file_size/1024/1024:.2f} MB",
            time=f"{download_time:.2f}s",
            rate=f"{download_rate:.2f} MB/s",
        )

        # Step 2: SFTP upload
        cprint(f"Starting SFTP upload", severity="INFO", destination=remote_file_path)
        upload_start = time.time()
        sftp.put(temp_path, remote_file_path)
        upload_time = time.time() - upload_start

        # Calculate metrics
        upload_rate = file_size / upload_time / 1024 / 1024 if upload_time > 0 else 0
        total_time = time.time() - overall_start

        # Log completion with detailed metrics
        cprint(
            f"SFTP upload completed",
            severity="INFO",
            upload_time=f"{upload_time:.2f}s",
            upload_rate=f"{upload_rate:.2f} MB/s",
            download_time=f"{download_time:.2f}s",
            download_rate=f"{download_rate:.2f} MB/s",
            total_time=f"{total_time:.2f}s",
        )

    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            cprint("Temporary file removed")


def check_sftp_credentials(sftp_config: Dict[str, Any], timeout: int = 10) -> bool:
    """
    Checks if SFTP credentials are valid by attempting to connect and list directory.

    Args:
        sftp_config: Dictionary with SFTP connection parameters
        timeout: Connection timeout in seconds

    Returns:
        bool: True if connection is successful

    Raises:
        ConfigError: If connection fails
    """
    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]
    remote_path = sftp_config.get("directory", sftp_config.get("path", "/"))

    start_time = time.time()
    cprint(
        f"Verifying SFTP credentials", severity="INFO", host=host, port=port, username=username, directory=remote_path
    )

    # Create a "transport" directly (lower level than SSHClient)
    try:
        # Create transport
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=sftp_config["password"])

        # Create SFTP client from transport
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Try listing directory
        try:
            cprint(f"Checking directory access")
            files = sftp.listdir(remote_path)
            cprint(f"Directory access confirmed", file_count=len(files), directory=remote_path)
        except FileNotFoundError:
            cprint(
                f"Directory does not exist, will be created during upload", severity="WARNING", directory=remote_path
            )

        # Clean up
        sftp.close()
        transport.close()

        elapsed = time.time() - start_time
        cprint(f"SFTP credentials verified in {elapsed:.2f}s", severity="INFO")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        error_message = f"SFTP connection failed after {elapsed:.2f}s: {str(e)}"
        cprint(error_message, severity="ERROR", host=host, port=port, username=username)
        raise ConfigError(error_message)


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    # Create main parser
    parser = argparse.ArgumentParser(description="SFTP operations")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Common SFTP arguments
    def add_sftp_args(parser):
        parser.add_argument("--host", help="SFTP host")
        parser.add_argument("--port", type=int, default=22, help="SFTP port (default: 22)")
        parser.add_argument("--username", help="SFTP username")
        parser.add_argument("--password", help="SFTP password")
        parser.add_argument("--directory", help="Remote directory")

    # Create 'check' command parser
    check_parser = subparsers.add_parser("check", help="Check SFTP connection")
    add_sftp_args(check_parser)
    check_parser.add_argument("--timeout", type=int, default=3000, help="Connection timeout in seconds")

    # Create 'upload' command parser
    upload_parser = subparsers.add_parser("upload", help="Upload file from GCS to SFTP")
    add_sftp_args(upload_parser)
    upload_parser.add_argument("--gcs-uri", required=True, help="GCS URI of file to upload (gs://bucket/path)")
    upload_parser.add_argument("--remote-file", required=True, help="Filename to use on SFTP server")
    upload_parser.add_argument(
        "--method",
        choices=["download", "stream", "auto"],
        default="download",
        help="Transfer method: download (default, fastest), stream (for very large files), or auto (size-based)",
    )
    upload_parser.add_argument(
        "--size-threshold",
        type=int,
        default=1073741824,  # 1GB
        help="Size threshold for auto method in bytes (default: 1GB)",
    )

    args = parser.parse_args()

    # If arguments are not provided, try to get them from environment variables
    host = args.host or os.environ.get("SFTP_HOST")
    port = args.port or int(os.environ.get("SFTP_PORT", "22"))
    username = args.username or os.environ.get("SFTP_USERNAME")
    password = args.password or os.environ.get("SFTP_PASSWORD")
    directory = args.directory or os.environ.get("SFTP_DIRECTORY", "/")

    if not host or not username or not password:
        print("Error: SFTP host, username, and password are required.")
        print("Provide them as arguments or in .env file.")
        exit(1)

    sftp_config = {"host": host, "port": port, "username": username, "password": password, "directory": directory}

    if args.command == "check" or args.command is None:
        # Check SFTP connection
        try:
            print(f"Testing SFTP connection to {host}:{port} as {username}...")
            check_sftp_credentials(sftp_config, timeout=args.timeout if hasattr(args, "timeout") else 10)
            print("✅ Connection successful!")
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            exit(1)

    elif args.command == "upload":
        # Upload file from GCS to SFTP
        try:
            # Configure parameters
            sftp_config["upload_method"] = args.method
            sftp_config["size_threshold"] = args.size_threshold

            print(f"Uploading {args.gcs_uri} to SFTP at {host}:{port}{directory}/{args.remote_file}")
            upload_from_gcs(sftp_config, args.gcs_uri, args.remote_file)
            print("✅ Upload successful!")
        except Exception as e:
            print(f"❌ Upload failed: {str(e)}")
            exit(1)
