"""
SFTP operations for the export function.
Handles connecting to SFTP and uploading files.
"""

import concurrent.futures
import os
import tempfile
import time
from pathlib import PurePosixPath
from typing import Any, Dict, List, Tuple

import paramiko
from google.cloud import storage
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

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
    Upload a file from GCS to SFTP server using download-upload method.

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

    # Use PurePosixPath for SFTP paths (always Unix-style)
    remote_path = PurePosixPath(directory)
    remote_file_path = remote_path / remote_filename

    cprint(
        f"Starting upload from GCS to SFTP",
        severity="INFO",
        source=gcs_uri,
        destination=f"{host}:{remote_file_path}",
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
        transport, sftp = create_sftp_connection(host, port, username, password)

        # Create directories if needed
        ensure_sftp_directory(sftp, remote_path)

        # Use download-upload method
        cprint("Using download-upload method", severity="INFO")
        transfer_start = time.time()
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


def upload_from_gcs_parallel(
    sftp_config: Dict[str, Any],
    file_mappings: List[Tuple[str, str]],
    max_workers: int = None,
) -> int:
    """
    Upload multiple files from GCS to SFTP server in parallel using a simplified approach.
    Each worker thread creates its own SFTP connection.

    Args:
        sftp_config: SFTP connection configuration
        file_mappings: List of (gcs_uri, remote_filename) tuples
        max_workers: Maximum number of concurrent workers

    Returns:
        int: Number of files successfully transferred
    """
    if not file_mappings:
        cprint("No files to transfer", severity="WARNING")
        return 0

    # Set default thread count if not specified
    if max_workers is None:
        max_workers = min(20, (os.cpu_count() or 1) * 4)  # Cap at reasonable limit

    total_files = len(file_mappings)
    successful = 0
    failed = 0
    start_time = time.time()

    cprint(f"Starting parallel upload of {total_files} files with {max_workers} workers", severity="INFO")

    # Create a copy of the config for each worker
    def upload_file(args):
        """Worker function that handles a single file transfer"""
        idx, (gcs_uri, remote_filename) = args
        file_start = time.time()

        try:
            # Create a deep copy of the config for each thread
            thread_config = sftp_config.copy()

            # Upload the file using existing function (creates its own connection)
            upload_from_gcs(thread_config, gcs_uri, remote_filename)

            file_time = time.time() - file_start
            cprint(
                f"File {idx+1}/{total_files}: {remote_filename} transferred successfully",
                severity="INFO",
                time_taken=f"{file_time:.2f}s",
            )
            return True

        except Exception as e:
            file_time = time.time() - file_start
            cprint(
                f"File {idx+1}/{total_files}: {remote_filename} transfer failed: {str(e)}",
                severity="ERROR",
                time_taken=f"{file_time:.2f}s",
            )
            return False

    try:
        # Process files with ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and collect results
            future_to_file = {
                executor.submit(upload_file, (i, mapping)): (i, mapping) for i, mapping in enumerate(file_mappings)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                if future.result():
                    successful += 1
                else:
                    failed += 1

                # Report progress
                completed = successful + failed
                cprint(
                    f"Progress: {completed}/{total_files} files processed ({successful} successful, {failed} failed)",
                    severity="DEBUG",
                )

        total_time = time.time() - start_time
        cprint(
            f"Parallel upload complete: {successful}/{total_files} files transferred",
            severity="INFO",
            failed=failed,
            total_time=f"{total_time:.2f}s",
        )

        return successful

    except Exception as e:
        cprint(f"Parallel upload operation failed: {str(e)}", severity="ERROR")
        raise


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


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((IOError, OSError, paramiko.ssh_exception.SSHException, ConnectionError)),
    before_sleep=lambda retry_state: cprint(
        f"Transfer attempt {retry_state.attempt_number} failed, retrying in {retry_state.next_action.sleep:.1f} seconds...",
        severity="WARNING",
        error=str(retry_state.outcome.exception()),
    ),
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
        transport, sftp = create_sftp_connection(host, port, username, password)

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


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    retry=retry_if_exception_type((ConnectionError, paramiko.ssh_exception.SSHException)),
    before_sleep=lambda retry_state: cprint(
        f"Connection attempt {retry_state.attempt_number} failed, retrying in {retry_state.next_action.sleep:.1f} seconds...",
        severity="WARNING",
        error=str(retry_state.outcome.exception()),
    ),
)
def create_sftp_connection(host: str, port: int, username: str, password: str):
    """Create an SFTP connection with retry logic."""
    cprint(f"Connecting to SFTP server at {host}:{port}", severity="INFO")
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return transport, sftp


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
            print(f"Uploading {args.gcs_uri} to SFTP at {host}:{port}{directory}/{args.remote_file}")
            upload_from_gcs(sftp_config, args.gcs_uri, args.remote_file)
            print("✅ Upload successful!")
        except Exception as e:
            print(f"❌ Upload failed: {str(e)}")
            exit(1)
