"""
GCS to SFTP transfer logic.
"""

import fnmatch
import re
import time
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import storage

from src.helpers import cprint
from src.sftp import check_sftp_credentials, upload_from_gcs, upload_from_gcs_sequential


def _parse_gcs_url(url: str) -> Tuple[str, str]:
    """Split gs://bucket/path into (bucket, path)."""
    if not url.startswith("gs://"):
        raise ValueError(f"Invalid GCS URL: {url}")
    path = url[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def _extract_date_from_gcs_path(gcs_path: str) -> Optional[str]:
    """
    Extract date from GCS path.
    
    Expects paths like: gs://bucket/export_name/20250108/ or gs://bucket/export_name/2025-01-08/
    
    Returns:
        Date string (YYYYMMDD format) or None if not found
    """
    # Match YYYYMMDD or YYYY-MM-DD patterns
    match = re.search(r'/(\d{4})-?(\d{2})-?(\d{2})/?', gcs_path)
    if match:
        return f"{match.group(1)}{match.group(2)}{match.group(3)}"
    return None


def _build_sftp_filename(original_filename: str, export_name: str, date: Optional[str]) -> str:
    """
    Build SFTP filename. New exports already use final format, so usually no change needed.
    
    Args:
        original_filename: GCS filename (e.g., "Product_20250108-000000000000.csv.gz")
        export_name: Name of the export
        date: Date string in YYYYMMDD format
    
    Returns:
        Filename for SFTP (usually same as original)
    """
    # New format: {name}_{date}-{shard}.ext - already correct, no change needed
    if re.match(r'^.+_\d{8}-\d+\.', original_filename):
        return original_filename
    
    # Legacy: {name}-{shard}.ext (no date) - add date
    if date:
        match = re.match(r'^(.+?)-(\d+)(\..*)?$', original_filename)
        if match:
            part = match.group(2)
            ext = match.group(3) or ""
            return f"{export_name}_{date}-{part}{ext}"
    
    return original_filename


def _list_gcs_files(
    storage_client: storage.Client,
    gcs_path: str,
    pattern: Optional[str] = None,
) -> List[storage.Blob]:
    """
    List files in GCS path, optionally filtering by pattern.

    Args:
        storage_client: GCS client
        gcs_path: GCS URI (gs://bucket/prefix/ or gs://bucket/prefix/*.csv.gz)
        pattern: Optional filename pattern (e.g., "*.csv.gz")

    Returns:
        List of matching blobs
    """
    bucket_name, prefix = _parse_gcs_url(gcs_path)

    # Check if path contains wildcard
    if "*" in prefix:
        # Extract prefix up to the wildcard
        star_idx = prefix.find("*")
        slash_idx = prefix.rfind("/", 0, star_idx)
        actual_prefix = prefix[: slash_idx + 1] if slash_idx != -1 else ""
        filter_pattern = prefix[slash_idx + 1 :] if slash_idx != -1 else prefix
    else:
        actual_prefix = prefix
        filter_pattern = pattern

    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=actual_prefix))

    # Filter out "directory" blobs (ending with /)
    blobs = [b for b in blobs if not b.name.endswith("/")]

    if filter_pattern:
        blobs = [b for b in blobs if fnmatch.fnmatchcase(b.name.split("/")[-1], filter_pattern)]

    return blobs


def transfer_gcs_to_sftp(
    sftp_config: Dict[str, Any],
    gcs_path: str,
    export_name: str,
) -> Dict[str, Any]:
    """
    Transfer files from GCS to SFTP.

    Args:
        sftp_config: SFTP connection configuration
        gcs_path: GCS path (gs://bucket/prefix/ or gs://bucket/prefix/*.csv.gz)
        export_name: Name of the export (for logging)
        max_workers: Max parallel upload threads

    Returns:
        Result dictionary with transfer details
    """
    overall_start = time.time()

    cprint(
        f"Starting transfer '{export_name}'",
        severity="INFO",
        export_name=export_name,
        gcs_path=gcs_path,
    )

    # Normalize SFTP directory path
    sftp_config = {**sftp_config, "directory": str(PurePosixPath(sftp_config["directory"]))}

    # Validate SFTP credentials before doing any work
    check_sftp_credentials(sftp_config)

    # List files in GCS
    storage_client = storage.Client()
    blobs = _list_gcs_files(storage_client, gcs_path)

    if not blobs:
        raise FileNotFoundError(f"No files found in GCS for export '{export_name}' at '{gcs_path}'")

    total_bytes = sum(int(b.size or 0) for b in blobs)
    cprint(
        f"Found {len(blobs)} files to transfer",
        severity="INFO",
        export_name=export_name,
        total_mb=f"{total_bytes / (1024 * 1024):.2f}",
    )

    # Extract date from GCS path for filename formatting
    date_str = _extract_date_from_gcs_path(gcs_path)
    if date_str:
        cprint(f"Extracted date from GCS path", severity="INFO", date=date_str)
    
    # Build file mappings: (gcs_uri, remote_filename)
    file_mappings = []
    transferred_files = []

    for blob in blobs:
        gcs_uri = f"gs://{blob.bucket.name}/{blob.name}"
        original_filename = blob.name.split("/")[-1]
        # Rename to: {export_name}_{date}-{part}.csv.gz
        remote_filename = _build_sftp_filename(original_filename, export_name, date_str)
        file_mappings.append((gcs_uri, remote_filename))
        transferred_files.append(remote_filename)
        
        if original_filename != remote_filename:
            cprint(f"File rename", severity="DEBUG", original=original_filename, renamed=remote_filename)

    # Upload to SFTP
    if len(file_mappings) == 1:
        gcs_uri, remote_filename = file_mappings[0]
        upload_from_gcs(sftp_config, gcs_uri, remote_filename)
        files_transferred = 1
        destination = f"{sftp_config['directory']}/{remote_filename}"
    else:
        # Use sequential upload with single connection for reliability
        files_transferred = upload_from_gcs_sequential(sftp_config, file_mappings)
        destination = f"{sftp_config['directory']}/"

    total_time = time.time() - overall_start

    cprint(
        f"Transfer '{export_name}' completed",
        severity="INFO",
        export_name=export_name,
        files=len(transferred_files),
        destination=destination,
        total_time=f"{total_time:.2f}s",
    )

    return {
        "status": "success",
        "export_name": export_name,
        "files_transferred": len(transferred_files),
        "files": transferred_files,
        "total_mb": round(total_bytes / (1024 * 1024), 2),
        "destination": destination,
        "total_time_seconds": round(total_time, 2),
        "gcs_path": gcs_path,
    }