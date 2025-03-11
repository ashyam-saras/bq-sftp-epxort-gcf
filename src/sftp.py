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
    Stream a file from GCS to SFTP server with optimized buffer size.

    Args:
        sftp_config: Dictionary with SFTP connection parameters
        gcs_uri: GCS URI of the file to upload
        remote_filename: Filename to use on SFTP server
    """
    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]
    directory = sftp_config["directory"]
    buffer_size = sftp_config.get("buffer_size", 32768)  # 32KB default buffer

    # Use PurePosixPath for SFTP paths (always Unix-style)
    remote_path = PurePosixPath(directory)
    remote_file_path = remote_path / remote_filename

    cprint(f"Uploading file from GCS to SFTP: {remote_file_path}")

    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        cprint(f"Connecting to SFTP server at {host}:{port}")

        # Create transport and connect
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Ensure directory exists
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

        # Stream the file
        with sftp.file(str(remote_file_path), "wb", bufsize=buffer_size) as sftp_file:
            blob.download_to_file(sftp_file)

        cprint(f"Successfully uploaded to {remote_file_path}")
        sftp.close()
        transport.close()

    except Exception as e:
        error_message = f"SFTP upload failed: {str(e)}"
        cprint(error_message, severity="ERROR")
        raise ConfigError(error_message)


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


def check_sftp_connection_with_pysftp(sftp_config: Dict[str, Any], timeout: int = 10) -> bool:
    """
    Check SFTP connection using the pysftp library (alternative to paramiko).

    Args:
        sftp_config: Dictionary with SFTP connection parameters
        timeout: Connection timeout in seconds

    Returns:
        bool: True if connection is successful

    Raises:
        ConfigError: If connection fails
    """
    # Import here to make it optional
    import pysftp

    host = sftp_config["host"]
    port = int(sftp_config.get("port", 22))
    username = sftp_config["username"]
    password = sftp_config["password"]
    remote_path = sftp_config.get("directory", sftp_config.get("path", "/"))

    # Disable host key checking for testing
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking

    try:
        cprint(f"Testing SFTP connection via pysftp to {host}:{port} as {username}")

        # Connect with timeout
        with pysftp.Connection(
            host=host,
            port=port,
            username=username,
            password=password,
            cnopts=cnopts,
            default_path=remote_path,
        ) as sftp:
            # Try listing files
            file_list = sftp.listdir()
            cprint(f"Connected successfully. Files in directory: {len(file_list)}")

            # Check directory exists and permissions
            if remote_path != "/":
                try:
                    if not sftp.exists(remote_path):
                        cprint(f"Warning: Directory {remote_path} does not exist", severity="WARNING")
                    else:
                        cprint(f"Directory {remote_path} exists")
                except Exception as path_error:
                    cprint(f"Could not check path: {str(path_error)}", severity="WARNING")

        cprint(f"SFTP credentials valid using pysftp, successfully connected to {host}:{port}")
        return True

    except Exception as e:
        error_message = f"SFTP connection failed via pysftp: {str(e)}"
        cprint(error_message, severity="ERROR")
        raise ConfigError(error_message)


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(description="Test SFTP connection")
    parser.add_argument("--host", help="SFTP host")
    parser.add_argument("--port", type=int, default=22, help="SFTP port (default: 22)")
    parser.add_argument("--username", help="SFTP username")
    parser.add_argument("--password", help="SFTP password")
    parser.add_argument("--directory", help="Remote directory to test")
    parser.add_argument("--timeout", type=int, default=10, help="Connection timeout in seconds")
    parser.add_argument("--use-pysftp", action="store_true", help="Use pysftp library instead of paramiko")

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

    try:
        print(f"Testing SFTP connection to {host}:{port} as {username}...")

        if args.use_pysftp:
            try:
                import pysftp
            except ImportError:
                print("Error: pysftp library not installed. Please install it with:")
                print("pip install pysftp")
                exit(1)

            check_sftp_connection_with_pysftp(sftp_config, timeout=args.timeout)
        else:
            check_sftp_credentials(sftp_config, timeout=args.timeout)

        print("✅ Connection successful!")
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        exit(1)
