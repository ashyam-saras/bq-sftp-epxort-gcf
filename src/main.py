"""
Main entry point for GCS to SFTP export function.
Orchestrates moving pre-exported data from GCS to SFTP.
"""

import base64
import datetime
import fnmatch
import json
import time
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import storage

from src.config import load_config
from src.helpers import cprint
from src.sftp import check_sftp_credentials, upload_from_gcs, upload_from_gcs_parallel


def _resolve_date_token(value: str, date: datetime.date) -> str:
    """Replace {date} token with YYYYMMDD in provided string."""
    if not isinstance(value, str):
        return value
    date_str = date.strftime(r"%Y%m%d")
    return value.replace("{date}", date_str)


def _parse_gcs_url(url: str) -> Tuple[str, str]:
    """Split gs://bucket/path into (bucket, path)."""
    if not url.startswith("gs://"):
        raise ValueError(f"Invalid GCS URL: {url}")
    path = url[5:]
    parts = path.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid GCS URL format: {url}")
    return parts[0], parts[1]


def _list_gcs_files_by_pattern(storage_client: storage.Client, pattern_url: str) -> List[storage.Blob]:
    """List blobs matching a pattern (supports * wildcard on basename)."""
    bucket_name, path = _parse_gcs_url(pattern_url)

    # Derive prefix up to first * to limit listing
    star_index = path.find("*")
    if star_index == -1:
        # Exact object path
        prefix = path
        filter_pattern = None
    else:
        # include preceding path segment up to last '/'
        slash_index = path.rfind("/", 0, star_index)
        prefix = path[: slash_index + 1] if slash_index != -1 else ""
        filter_pattern = path

    bucket = storage_client.bucket(bucket_name)
    blobs_iter = bucket.list_blobs(prefix=prefix)
    blobs = list(blobs_iter)

    if filter_pattern:
        matched = [
            b for b in blobs if fnmatch.fnmatchcase(f"{bucket_name}/{b.name}", f"{bucket_name}/{filter_pattern}")
        ]
        return matched
    return blobs


def _list_gcs_files_by_prefix(
    storage_client: storage.Client, prefix_url: str, name_pattern: Optional[str]
) -> List[storage.Blob]:
    bucket_name, path_prefix = _parse_gcs_url(prefix_url)
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=path_prefix))
    if name_pattern:
        matched = [b for b in blobs if fnmatch.fnmatchcase(b.name.split("/")[-1], name_pattern)]
        return matched
    return blobs


def export_from_pattern(config: Dict[str, Any], export_name: str, resolved_pattern_or_prefix: str) -> Dict[str, Any]:
    """Upload files matching a resolved GCS pattern/prefix to SFTP using current date for logs only."""
    overall_start_time = time.time()
    date = datetime.datetime.now().date()
    date_str = date.strftime(r"%Y%m%d")

    cprint(
        f"Starting export '{export_name}' from '{resolved_pattern_or_prefix}'",
        severity="INFO",
        export_name=export_name,
        date=date_str,
    )

    sftp_config = {**config["sftp"], "directory": str(PurePosixPath(config["sftp"]["directory"]))}

    # SFTP credentials check (fail fast before any GCS operations)
    check_sftp_credentials(sftp_config)

    storage_client = storage.Client()

    # Decide if it is a pattern (contains *) or a plain prefix
    if "*" in resolved_pattern_or_prefix:
        blobs = _list_gcs_files_by_pattern(storage_client, resolved_pattern_or_prefix)
    else:
        blobs = _list_gcs_files_by_prefix(storage_client, resolved_pattern_or_prefix, None)

    if not blobs:
        raise FileNotFoundError(
            f"No files found in GCS for export '{export_name}' using source '{resolved_pattern_or_prefix}'"
        )

    total_bytes = sum(int(b.size or 0) for b in blobs)
    cprint(
        f"Found {len(blobs)} files to upload to SFTP",
        severity="INFO",
        total_mb=f"{total_bytes/(1024*1024):.2f}",
        source=resolved_pattern_or_prefix,
    )

    # Prepare mappings and upload
    file_mappings = []
    for blob in blobs:
        gcs_uri = f"gs://{blob.bucket.name}/{blob.name}"
        original_filename = blob.name.split("/")[-1]
        parts = original_filename.split("-", 1)
        remote_filename = f"{parts[0]}_{parts[1]}" if len(parts) > 1 else original_filename
        file_mappings.append((gcs_uri, remote_filename))

    if len(file_mappings) == 1:
        gcs_uri, remote_filename = file_mappings[0]
        if gcs_uri.endswith(".gz") and not remote_filename.endswith(".gz"):
            remote_filename = f"{remote_filename}.gz"
        upload_from_gcs(sftp_config, gcs_uri, remote_filename)
        files_transferred = 1
        destination_path = f"{sftp_config['directory']}/{remote_filename}"
    else:
        files_transferred = upload_from_gcs_parallel(sftp_config, file_mappings, max_workers=10)
        destination_path = f"{sftp_config['directory']}/*"

    total_time = time.time() - overall_start_time
    cprint(
        f"Export '{export_name}' completed",
        severity="INFO",
        files=files_transferred,
        destination=destination_path,
        total_time=f"{total_time:.2f}s",
    )

    return {
        "status": "success",
        "export_name": export_name,
        "files_transferred": files_transferred,
        "destination": destination_path,
        "total_time_seconds": round(total_time, 2),
        "total_mb": round(total_bytes / (1024 * 1024), 2),
        "source": resolved_pattern_or_prefix,
    }


def cloud_function_handler(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cloud Function entry point that processes a Pub/Sub event.

    Args:
        event: The dictionary with event payload (from Pub/Sub)

    Returns:
        Dictionary with processing results
    """
    start_time = time.time()

    try:
        # Parse message first so we can get context info
        message = json.loads(base64.b64decode(event.data["message"]["data"]).decode("utf-8"))
        if not message:
            return {"status": "error", "message": "No data in event"}

        # Load configuration
        config = load_config()

        # Extract export parameters from message
        export_name = message.get("export_name")
        date_str = message.get("date")

        # Validate required parameters
        if not export_name:
            return {"status": "error", "message": "No export_name specified in message"}

        # Parse date if provided, otherwise use today
        export_date = None
        if date_str:
            try:
                export_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                return {"status": "error", "message": f"Invalid date format: {date_str}, use YYYY-MM-DD"}

        # Run the export process
        result = export_to_sftp(config, export_name, export_date)

        # Add execution time
        execution_time = time.time() - start_time
        result["execution_time"] = f"{execution_time:.1f} seconds"

        return result

    except Exception as e:
        # Handle any uncaught exceptions
        execution_time = time.time() - start_time
        return {"status": "error", "message": str(e), "execution_time": f"{execution_time:.1f} seconds"}


# For local testing
if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GCS to SFTP Export")
    parser.add_argument("--export", required=True, help="Name of the export to run")
    parser.add_argument("--date", help="Export date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--config", help="Path to config file (default: from environment)")
    args = parser.parse_args()

    # Load configuration
    print(args.config)
    config = load_config(args.config)

    # Parse date if provided
    export_date = None
    if args.date:
        try:
            export_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Invalid date format: {args.date}, use YYYY-MM-DD")
            exit(1)

    # Run the export
    try:
        result = export_to_sftp(config, args.export, export_date)
        print("Export completed successfully!")
        print(f"Transferred {result['files_transferred']} file(s) to {result['destination']}")
    except Exception as e:
        print(f"Export failed: {str(e)}")
        exit(1)
