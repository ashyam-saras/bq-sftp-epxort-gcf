"""
SFTP operations for the export function.
Handles connecting to SFTP and uploading files.
"""

import os
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
    Upload a file from GCS to SFTP server, choosing the optimal method based on file size.

    Args:
        sftp_config: Dictionary with SFTP connection parameters
        gcs_uri: GCS URI of the file to upload
        remote_filename: Filename to use on SFTP server
    """
    import time

    # Extract common parameters
    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]
    directory = sftp_config["directory"]
    buffer_size = sftp_config.get("buffer_size", 262144)  # 256KB default

    # Extract upload method preference - update default to "direct"
    upload_method = sftp_config.get("upload_method", "direct")  # direct, stream, or download
    size_threshold = sftp_config.get("size_threshold", 1073741824)  # 1GB default

    # Use PurePosixPath for SFTP paths (always Unix-style)
    remote_path = PurePosixPath(directory)
    remote_file_path = remote_path / remote_filename

    cprint(f"Uploading file from GCS to SFTP: {remote_file_path}")

    # Get GCS blob
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    # Get blob size
    blob_size = blob.size
    cprint(f"File size: {blob_size:,} bytes ({blob_size / (1024*1024):.2f} MB)")

    try:
        # Connect to SFTP FIRST - before uploading
        cprint(f"Connecting to SFTP server at {host}:{port}")
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create directories if needed
        ensure_sftp_directory(sftp, remote_path)

        # Now choose and execute upload method
        if upload_method == "stream" or (upload_method == "auto" and blob_size >= size_threshold):
            cprint("Using streaming method for transfer")
            _upload_streaming(sftp, blob, str(remote_file_path), buffer_size)
        elif upload_method == "download":
            cprint("Using chunked download-upload method")
            _upload_with_download(sftp, blob, str(remote_file_path), buffer_size)
        else:
            # Make direct the default option
            cprint("Using direct download-upload method")
            _upload_direct(sftp, blob, str(remote_file_path), buffer_size)

        # Close connection
        sftp.close()
        transport.close()

    except Exception as e:
        error_message = f"SFTP upload failed: {str(e)}"
        cprint(error_message, severity="ERROR")
        raise ConfigError(error_message)


def ensure_sftp_directory(sftp, remote_path):
    """Create directory tree if it doesn't exist."""
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


def _upload_streaming(sftp, blob, remote_file_path, buffer_size):
    """Stream file directly from GCS to SFTP."""
    import time

    blob_size = blob.size

    # Stream the file with progress reporting
    cprint(f"Streaming file from GCS to SFTP with buffer size {buffer_size}")

    # Manual chunked download with progress reporting
    with sftp.file(remote_file_path, "wb", bufsize=buffer_size) as sftp_file:
        # Download in chunks and report progress
        chunk_size = buffer_size
        downloaded = 0
        last_progress_time = time.time()
        start_time = time.time()

        # Download the file in chunks
        with blob.open("rb") as source_file:
            while True:
                chunk = source_file.read(chunk_size)
                if not chunk:
                    break

                # Write to SFTP
                sftp_file.write(chunk)

                # Update progress
                downloaded += len(chunk)
                current_time = time.time()

                # Report progress every second
                if current_time - last_progress_time >= 1.0:
                    percent = (downloaded / blob_size) * 100
                    elapsed = current_time - start_time
                    speed = downloaded / elapsed if elapsed > 0 else 0
                    eta = (blob_size - downloaded) / speed if speed > 0 else 0

                    cprint(
                        f"Progress: {percent:.1f}% ({downloaded/1024/1024:,.2f}/{blob_size/1024/1024:,.2f} MB) "
                        f"- {speed/1024/1024:.2f} MB/s - ETA: {eta/60:.1f} mins"
                    )
                    last_progress_time = current_time

    total_time = time.time() - start_time
    avg_speed = blob_size / total_time if total_time > 0 else 0
    cprint(
        f"Successfully uploaded {blob_size/1024/1024:,.2f} MB in {total_time/60:.1f} mins "
        f"({avg_speed/1024/1024:.2f} MB/s)"
    )


def _upload_with_download(sftp, blob, remote_file_path, buffer_size):
    """Two-step process: Download to temp file then upload to SFTP."""
    import os
    import tempfile
    import time

    # Step 1: Download to temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_path = temp_file.name
    temp_file.close()  # Close but don't delete

    try:
        cprint(f"Downloading from GCS to temporary file: {temp_path}")
        download_start = time.time()
        blob.download_to_filename(temp_path)
        download_time = time.time() - download_start
        file_size = os.path.getsize(temp_path)

        cprint(
            f"Downloaded {file_size/1024/1024:,.2f} MB in {download_time:.1f}s "
            f"({file_size/download_time/1024/1024:.2f} MB/s)"
        )

        # Step 2: Upload to SFTP with progress reporting
        cprint(f"Uploading file to SFTP: {remote_file_path}")
        upload_start = time.time()
        last_progress_time = upload_start
        uploaded = 0

        with open(temp_path, "rb") as local_file:
            # Create SFTP file with specified buffer
            with sftp.file(remote_file_path, "wb", bufsize=buffer_size) as sftp_file:
                while True:
                    chunk = local_file.read(buffer_size)
                    if not chunk:
                        break

                    sftp_file.write(chunk)
                    uploaded += len(chunk)

                    current_time = time.time()
                    if current_time - last_progress_time >= 1.0:
                        percent = (uploaded / file_size) * 100
                        elapsed = current_time - upload_start
                        speed = uploaded / elapsed if elapsed > 0 else 0
                        eta = (file_size - uploaded) / speed if speed > 0 else 0

                        cprint(
                            f"Progress: {percent:.1f}% ({uploaded/1024/1024:,.2f}/{file_size/1024/1024:,.2f} MB) "
                            f"- {speed/1024/1024:.2f} MB/s - ETA: {eta/60:.1f} mins"
                        )
                        last_progress_time = current_time

        # Report results
        upload_time = time.time() - upload_start
        total_time = download_time + upload_time
        avg_speed = file_size / upload_time if upload_time > 0 else 0

        cprint(f"Upload completed in {upload_time:.1f}s ({avg_speed/1024/1024:.2f} MB/s)")
        cprint(f"Total transfer time: {total_time:.1f}s (download + upload)")

    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            cprint("Temporary file removed")


def _upload_direct(sftp, blob, remote_file_path, buffer_size):
    """Direct download and upload without chunking the upload."""
    import os
    import tempfile
    import time

    # Step 1: Download to temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_path = temp_file.name
    temp_file.close()  # Close but don't delete

    try:
        cprint(f"Downloading from GCS to temporary file: {temp_path}")
        download_start = time.time()
        blob.download_to_filename(temp_path)
        download_time = time.time() - download_start
        file_size = os.path.getsize(temp_path)

        cprint(
            f"Downloaded {file_size/1024/1024:,.2f} MB in {download_time:.1f}s "
            f"({file_size/download_time/1024/1024:.2f} MB/s)"
        )

        # Step 2: Direct upload using put (no manual chunking)
        cprint(f"Directly uploading file to SFTP: {remote_file_path}")
        upload_start = time.time()

        # Use direct put method instead of manual chunking
        sftp.put(temp_path, remote_file_path)

        # Report results
        upload_time = time.time() - upload_start
        total_time = download_time + upload_time
        avg_speed = file_size / upload_time if upload_time > 0 else 0

        cprint(f"Upload completed in {upload_time:.1f}s ({avg_speed/1024/1024:.2f} MB/s)")
        cprint(f"Total transfer time: {total_time:.1f}s (download + upload)")

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

    # Create a "transport" directly (lower level than SSHClient)
    try:
        cprint(f"Testing SFTP connection to {host}:{port} as {username}")

        # Create transport
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)

        # Create SFTP client from transport
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Try listing directory
        try:
            sftp.listdir(remote_path)
            cprint(f"Successfully listed directory {remote_path}")
        except FileNotFoundError:
            cprint(f"Directory {remote_path} does not exist", severity="WARNING")

        # Clean up
        sftp.close()
        transport.close()

        cprint(f"SFTP credentials valid, successfully connected to {host}:{port}")
        return True

    except Exception as e:
        error_message = f"SFTP credential check failed: {str(e)}"
        cprint(error_message, severity="ERROR")
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
    check_parser.add_argument("--timeout", type=int, default=10, help="Connection timeout in seconds")

    # Create 'upload' command parser
    upload_parser = subparsers.add_parser("upload", help="Upload file from GCS to SFTP")
    add_sftp_args(upload_parser)
    upload_parser.add_argument("--gcs-uri", required=True, help="GCS URI of file to upload (gs://bucket/path)")
    upload_parser.add_argument("--remote-file", required=True, help="Filename to use on SFTP server")
    upload_parser.add_argument("--buffer-size", type=int, default=262144, help="Buffer size in bytes (default: 256KB)")
    upload_parser.add_argument(
        "--method",
        choices=["direct", "auto", "stream", "download"],
        default="direct",
        help="Transfer method: direct (fastest), auto (size-based), stream, or download (chunked)",
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
            sftp_config["buffer_size"] = args.buffer_size
            sftp_config["upload_method"] = args.method
            sftp_config["size_threshold"] = args.size_threshold

            print(f"Uploading {args.gcs_uri} to SFTP at {host}:{port}{directory}/{args.remote_file}")
            upload_from_gcs(sftp_config, args.gcs_uri, args.remote_file)
            print("✅ Upload successful!")
        except Exception as e:
            print(f"❌ Upload failed: {str(e)}")
            exit(1)
