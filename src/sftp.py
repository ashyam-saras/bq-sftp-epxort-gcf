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


def upload_from_gcs_sequential(
    sftp_config: Dict[str, Any],
    file_mappings: List[Tuple[str, str]],
) -> int:
    """
    Upload multiple files from GCS to SFTP using a single persistent connection.
    More reliable than parallel uploads - avoids connection limit issues.

    Args:
        sftp_config: SFTP connection configuration
        file_mappings: List of (gcs_uri, remote_filename) tuples

    Returns:
        int: Number of files successfully transferred

    Raises:
        Exception: If any file fails to transfer
    """
    if not file_mappings:
        cprint("No files to transfer", severity="WARNING")
        return 0

    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]
    directory = sftp_config["directory"]

    total_files = len(file_mappings)
    start_time = time.time()

    cprint(f"Starting sequential upload of {total_files} files (single connection)", severity="INFO")

    # Create single SFTP connection
    transport, sftp = create_sftp_connection(host, port, username, password)

    # Ensure target directory exists
    remote_path = PurePosixPath(directory)
    ensure_sftp_directory(sftp, remote_path)

    # Initialize GCS client once
    storage_client = storage.Client()
    transferred = 0

    try:
        for idx, (gcs_uri, remote_filename) in enumerate(file_mappings):
            file_start = time.time()
            remote_file_path = remote_path / remote_filename

            try:
                # Get blob from GCS
                bucket_name, blob_name = parse_gcs_uri(gcs_uri)
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.get_blob(blob_name)

                if not blob:
                    raise FileNotFoundError(f"File not found in GCS: {gcs_uri}")

                # Upload using existing connection
                _download_and_upload(sftp, blob, str(remote_file_path))

                file_time = time.time() - file_start
                cprint(
                    f"File {idx+1}/{total_files}: {remote_filename} transferred successfully",
                    severity="INFO",
                    time_taken=f"{file_time:.2f}s",
                )
                transferred += 1

            except Exception as e:
                file_time = time.time() - file_start
                cprint(
                    f"File {idx+1}/{total_files}: {remote_filename} transfer failed: {str(e)}",
                    severity="ERROR",
                    time_taken=f"{file_time:.2f}s",
                )
                # Close connection and raise - don't continue with partial transfers
                sftp.close()
                transport.close()
                raise Exception(f"Transfer failed on file {idx+1}/{total_files} ({remote_filename}): {str(e)}")

        total_time = time.time() - start_time
        cprint(
            f"Sequential upload complete: {transferred}/{total_files} files transferred",
            severity="INFO",
            total_time=f"{total_time:.2f}s",
        )

        return transferred

    finally:
        # Ensure connection is closed
        try:
            sftp.close()
            transport.close()
        except Exception:
            pass


def upload_from_gcs_parallel(
    sftp_config: Dict[str, Any],
    file_mappings: List[Tuple[str, str]],
    max_workers: int = None,
) -> int:
    """
    Upload multiple files from GCS to SFTP server in parallel.
    Each worker thread creates its own SFTP connection.
    
    WARNING: May cause connection limit issues on some SFTP servers.
    Consider using upload_from_gcs_sequential() for more reliability.

    Args:
        sftp_config: SFTP connection configuration
        file_mappings: List of (gcs_uri, remote_filename) tuples
        max_workers: Maximum number of concurrent workers

    Returns:
        int: Number of files successfully transferred

    Raises:
        Exception: If any file fails to transfer
    """
    if not file_mappings:
        cprint("No files to transfer", severity="WARNING")
        return 0

    # Set default thread count if not specified
    if max_workers is None:
        max_workers = 3  # Keep low to avoid SFTP server connection limits

    total_files = len(file_mappings)
    successful_files = []
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
            return remote_filename  # Return filename on success

        except Exception as e:
            file_time = time.time() - file_start
            cprint(
                f"File {idx+1}/{total_files}: {remote_filename} transfer failed: {str(e)}",
                severity="ERROR",
                time_taken=f"{file_time:.2f}s",
            )
            return None  # Return None on failure

    try:
        # Process files with ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and collect results
            future_to_file = {
                executor.submit(upload_file, (i, mapping)): (i, mapping) for i, mapping in enumerate(file_mappings)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                result = future.result()
                if result is not None:
                    successful_files.append(result)
                else:
                    failed += 1

                # Report progress
                completed = len(successful_files) + failed
                cprint(
                    f"Progress: {completed}/{total_files} files processed ({len(successful_files)} successful, {failed} failed)",
                    severity="DEBUG",
                )

        total_time = time.time() - start_time
        cprint(
            f"Parallel upload complete: {len(successful_files)}/{total_files} files transferred",
            severity="INFO",
            failed=failed,
            total_time=f"{total_time:.2f}s",
        )

        # Fail if any files failed to transfer
        if failed > 0:
            raise Exception(f"Transfer failed: {failed}/{total_files} files failed to upload")

        return len(successful_files)

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
        f"Verifying SFTP credentials",
        severity="INFO",
        host=host,
        port=port,
        username=username,
        directory=remote_path,
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


def list_sftp_files(sftp_config: Dict[str, Any], directory: str) -> Dict[str, Dict[str, Any]]:
    """
    List files in an SFTP directory with their metadata.

    Args:
        sftp_config: SFTP connection configuration
        directory: Remote directory to list

    Returns:
        Dictionary mapping filename to metadata (size, mtime, etc.)
    """
    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]

    cprint(f"Listing SFTP directory", severity="INFO", directory=directory)

    try:
        transport, sftp = create_sftp_connection(host, port, username, password)

        files = {}
        try:
            for attr in sftp.listdir_attr(directory):
                # Skip directories
                if attr.st_mode and not (attr.st_mode & 0o40000):  # Not a directory
                    files[attr.filename] = {
                        "size": attr.st_size,
                        "mtime": attr.st_mtime,
                        "atime": attr.st_atime,
                    }
        except FileNotFoundError:
            cprint(f"Directory not found on SFTP", severity="WARNING", directory=directory)
            return {}

        sftp.close()
        transport.close()

        cprint(f"Found {len(files)} files in SFTP directory", severity="INFO", directory=directory)
        return files

    except Exception as e:
        cprint(f"Failed to list SFTP directory: {str(e)}", severity="ERROR", directory=directory)
        raise


def list_sftp_directory(sftp_config: Dict[str, Any], directory: str, long_format: bool = False) -> List[Dict[str, Any]]:
    """
    List all entries (files and directories) in an SFTP directory.

    Args:
        sftp_config: SFTP connection configuration
        directory: Remote directory to list
        long_format: Include detailed metadata

    Returns:
        List of entries with metadata
    """
    import stat
    from datetime import datetime

    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]

    transport, sftp = create_sftp_connection(host, port, username, password)

    try:
        entries = []
        try:
            for attr in sftp.listdir_attr(directory):
                is_dir = stat.S_ISDIR(attr.st_mode) if attr.st_mode else False
                entry = {
                    "name": attr.filename,
                    "is_dir": is_dir,
                    "size": attr.st_size,
                    "mtime": datetime.fromtimestamp(attr.st_mtime) if attr.st_mtime else None,
                    "mode": attr.st_mode,
                }
                entries.append(entry)
        except FileNotFoundError:
            raise FileNotFoundError(f"Directory not found: {directory}")

        # Sort: directories first, then alphabetically
        entries.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
        return entries
    finally:
        sftp.close()
        transport.close()


def list_sftp_tree(sftp_config: Dict[str, Any], directory: str, max_depth: int = 3) -> None:
    """
    Print a tree view of SFTP directory structure.

    Args:
        sftp_config: SFTP connection configuration
        directory: Root directory to start from
        max_depth: Maximum depth to traverse
    """
    import stat

    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]

    transport, sftp = create_sftp_connection(host, port, username, password)

    try:
        def _print_tree(path: str, prefix: str = "", depth: int = 0):
            if depth > max_depth:
                return

            try:
                entries = sftp.listdir_attr(path)
            except (PermissionError, FileNotFoundError):
                return

            # Sort: directories first, then alphabetically
            entries.sort(key=lambda x: (not stat.S_ISDIR(x.st_mode) if x.st_mode else True, x.filename.lower()))

            for i, attr in enumerate(entries):
                is_last = i == len(entries) - 1
                connector = "└── " if is_last else "├── "
                is_dir = stat.S_ISDIR(attr.st_mode) if attr.st_mode else False

                if is_dir:
                    print(f"{prefix}{connector}{attr.filename}/")
                    new_prefix = prefix + ("    " if is_last else "│   ")
                    new_path = f"{path.rstrip('/')}/{attr.filename}"
                    _print_tree(new_path, new_prefix, depth + 1)
                else:
                    size_str = _format_size(attr.st_size) if attr.st_size else "0B"
                    print(f"{prefix}{connector}{attr.filename} ({size_str})")

        print(f"{directory}")
        _print_tree(directory)
    finally:
        sftp.close()
        transport.close()


def _format_size(size: int) -> str:
    """Format file size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f}{unit}" if unit != "B" else f"{size}{unit}"
        size /= 1024
    return f"{size:.1f}PB"


def delete_sftp_path(sftp_config: Dict[str, Any], path: str, recursive: bool = False) -> Tuple[int, int]:
    """
    Delete a file or directory on SFTP server.

    Args:
        sftp_config: SFTP connection configuration
        path: Path to file or directory to delete
        recursive: If True, delete directory contents recursively

    Returns:
        Tuple of (files_deleted, dirs_deleted)
    """
    import stat

    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]

    transport, sftp = create_sftp_connection(host, port, username, password)

    files_deleted = 0
    dirs_deleted = 0

    def _delete_recursive(target_path: str) -> Tuple[int, int]:
        """Recursively delete a path."""
        nonlocal files_deleted, dirs_deleted

        try:
            attr = sftp.stat(target_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Path not found: {target_path}")

        is_dir = stat.S_ISDIR(attr.st_mode) if attr.st_mode else False

        if is_dir:
            if not recursive:
                raise IsADirectoryError(f"Cannot delete directory without --recursive: {target_path}")

            # Delete contents first
            for entry in sftp.listdir_attr(target_path):
                entry_path = f"{target_path.rstrip('/')}/{entry.filename}"
                entry_is_dir = stat.S_ISDIR(entry.st_mode) if entry.st_mode else False

                if entry_is_dir:
                    _delete_recursive(entry_path)
                else:
                    sftp.remove(entry_path)
                    files_deleted += 1
                    print(f"  Deleted file: {entry_path}")

            # Delete the directory itself
            sftp.rmdir(target_path)
            dirs_deleted += 1
            print(f"  Deleted directory: {target_path}")
        else:
            sftp.remove(target_path)
            files_deleted += 1
            print(f"  Deleted file: {target_path}")

        return files_deleted, dirs_deleted

    try:
        _delete_recursive(path)
    finally:
        sftp.close()
        transport.close()

    return files_deleted, dirs_deleted


def clear_sftp_directory(sftp_config: Dict[str, Any], directory: str) -> Tuple[int, int]:
    """
    Delete all contents of a directory without deleting the directory itself.

    Args:
        sftp_config: SFTP connection configuration
        directory: Directory to clear

    Returns:
        Tuple of (files_deleted, dirs_deleted)
    """
    import stat

    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]

    transport, sftp = create_sftp_connection(host, port, username, password)

    files_deleted = 0
    dirs_deleted = 0

    def _delete_recursive(target_path: str):
        """Recursively delete a path."""
        nonlocal files_deleted, dirs_deleted

        attr = sftp.stat(target_path)
        is_dir = stat.S_ISDIR(attr.st_mode) if attr.st_mode else False

        if is_dir:
            # Delete contents first
            for entry in sftp.listdir_attr(target_path):
                entry_path = f"{target_path.rstrip('/')}/{entry.filename}"
                _delete_recursive(entry_path)

            # Delete the directory itself
            sftp.rmdir(target_path)
            dirs_deleted += 1
            print(f"  Deleted directory: {target_path}")
        else:
            sftp.remove(target_path)
            files_deleted += 1
            print(f"  Deleted file: {target_path}")

    try:
        # Check directory exists
        try:
            attr = sftp.stat(directory)
            if not stat.S_ISDIR(attr.st_mode):
                raise NotADirectoryError(f"Not a directory: {directory}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Directory not found: {directory}")

        # Delete all contents
        for entry in sftp.listdir_attr(directory):
            entry_path = f"{directory.rstrip('/')}/{entry.filename}"
            _delete_recursive(entry_path)

    finally:
        sftp.close()
        transport.close()

    return files_deleted, dirs_deleted


def main():
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

    # Create 'ls' command parser
    ls_parser = subparsers.add_parser("ls", help="List files and directories")
    add_sftp_args(ls_parser)
    ls_parser.add_argument("-l", "--long", action="store_true", help="Use long listing format")

    # Create 'tree' command parser
    tree_parser = subparsers.add_parser("tree", help="Show directory tree")
    add_sftp_args(tree_parser)
    tree_parser.add_argument("--depth", type=int, default=3, help="Maximum depth to traverse (default: 3)")

    # Create 'rm' command parser
    rm_parser = subparsers.add_parser("rm", help="Delete file or directory")
    add_sftp_args(rm_parser)
    rm_parser.add_argument("path", help="Path to file or directory to delete")
    rm_parser.add_argument("-r", "--recursive", action="store_true", help="Delete directories recursively")
    rm_parser.add_argument("-f", "--force", action="store_true", help="Skip confirmation prompt")

    # Create 'clear' command parser
    clear_parser = subparsers.add_parser("clear", help="Delete all contents of a directory (keeps directory)")
    add_sftp_args(clear_parser)
    clear_parser.add_argument("-f", "--force", action="store_true", help="Skip confirmation prompt")

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
        try:
            print(f"Uploading {args.gcs_uri} to SFTP at {host}:{port}{directory}/{args.remote_file}")
            upload_from_gcs(sftp_config, args.gcs_uri, args.remote_file)
            print("✅ Upload successful!")
        except Exception as e:
            print(f"❌ Upload failed: {str(e)}")
            exit(1)

    elif args.command == "ls":
        try:
            entries = list_sftp_directory(sftp_config, directory, long_format=args.long)
            if not entries:
                print(f"Directory is empty: {directory}")
            elif args.long:
                # Long format: permissions, size, date, name
                print(f"{'Type':<5} {'Size':>10}  {'Modified':<20} Name")
                print("-" * 60)
                for entry in entries:
                    type_str = "dir" if entry["is_dir"] else "file"
                    size_str = _format_size(entry["size"]) if entry["size"] else "-"
                    mtime_str = entry["mtime"].strftime("%Y-%m-%d %H:%M:%S") if entry["mtime"] else "-"
                    name = entry["name"] + "/" if entry["is_dir"] else entry["name"]
                    print(f"{type_str:<5} {size_str:>10}  {mtime_str:<20} {name}")
                print(f"\nTotal: {len(entries)} entries")
            else:
                # Short format: just names
                for entry in entries:
                    name = entry["name"] + "/" if entry["is_dir"] else entry["name"]
                    print(name)
        except FileNotFoundError as e:
            print(f"❌ {str(e)}")
            exit(1)
        except Exception as e:
            print(f"❌ Failed to list directory: {str(e)}")
            exit(1)

    elif args.command == "tree":
        try:
            print(f"Directory tree for {host}:{port}")
            list_sftp_tree(sftp_config, directory, max_depth=args.depth)
        except Exception as e:
            print(f"❌ Failed to show tree: {str(e)}")
            exit(1)

    elif args.command == "rm":
        target_path = args.path
        # Confirmation prompt unless --force
        if not args.force:
            if args.recursive:
                confirm = input(f"Delete '{target_path}' and all contents? [y/N]: ")
            else:
                confirm = input(f"Delete '{target_path}'? [y/N]: ")
            if confirm.lower() != "y":
                print("Cancelled.")
                exit(0)

        try:
            print(f"Deleting {target_path}...")
            files, dirs = delete_sftp_path(sftp_config, target_path, recursive=args.recursive)
            print(f"✅ Deleted {files} file(s) and {dirs} directory(ies)")
        except FileNotFoundError as e:
            print(f"❌ {str(e)}")
            exit(1)
        except IsADirectoryError as e:
            print(f"❌ {str(e)}")
            print("Use --recursive (-r) to delete directories")
            exit(1)
        except Exception as e:
            print(f"❌ Delete failed: {str(e)}")
            exit(1)

    elif args.command == "clear":
        # Confirmation prompt unless --force
        if not args.force:
            # Show what will be deleted first
            try:
                entries = list_sftp_directory(sftp_config, directory)
                if not entries:
                    print(f"Directory is already empty: {directory}")
                    exit(0)
                print(f"Contents of {directory}:")
                for entry in entries[:10]:  # Show first 10
                    name = entry["name"] + "/" if entry["is_dir"] else entry["name"]
                    print(f"  {name}")
                if len(entries) > 10:
                    print(f"  ... and {len(entries) - 10} more")
                confirm = input(f"\nDelete ALL {len(entries)} items in '{directory}'? [y/N]: ")
                if confirm.lower() != "y":
                    print("Cancelled.")
                    exit(0)
            except Exception as e:
                print(f"❌ Failed to list directory: {str(e)}")
                exit(1)

        try:
            print(f"Clearing {directory}...")
            files, dirs = clear_sftp_directory(sftp_config, directory)
            print(f"✅ Deleted {files} file(s) and {dirs} directory(ies)")
        except FileNotFoundError as e:
            print(f"❌ {str(e)}")
            exit(1)
        except Exception as e:
            print(f"❌ Clear failed: {str(e)}")
            exit(1)


if __name__ == "__main__":
    main()