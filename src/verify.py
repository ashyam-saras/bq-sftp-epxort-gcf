"""
Verification logic to ensure GCS and SFTP are in sync.
"""

from typing import Any, Dict, List, Set

from google.cloud import storage

from src.helpers import cprint
from src.sftp import list_sftp_files
from src.transfer import _list_gcs_files, _parse_gcs_url


def verify_gcs_sftp_sync(
    sftp_config: Dict[str, Any],
    gcs_path: str,
    export_name: str,
) -> Dict[str, Any]:
    """
    Verify that files in GCS have been successfully transferred to SFTP.
    
    Args:
        sftp_config: SFTP connection configuration
        gcs_path: GCS path that was transferred
        export_name: Name of the export
    
    Returns:
        Verification result with sync status and file comparisons
    """
    cprint(
        f"Verifying sync for '{export_name}'",
        severity="INFO",
        export_name=export_name,
        gcs_path=gcs_path,
    )

    # Get files from GCS
    storage_client = storage.Client()
    gcs_blobs = _list_gcs_files(storage_client, gcs_path)
    gcs_files: Set[str] = {blob.name.split("/")[-1] for blob in gcs_blobs}

    # Get file sizes from GCS for comparison
    gcs_file_sizes = {
        blob.name.split("/")[-1]: blob.size
        for blob in gcs_blobs
    }

    # Get files from SFTP
    sftp_directory = sftp_config["directory"]
    sftp_file_info = list_sftp_files(sftp_config, sftp_directory)
    sftp_files: Set[str] = set(sftp_file_info.keys())

    # Filter SFTP files to only those that match GCS filenames
    # (SFTP directory may contain other files)
    relevant_sftp_files = sftp_files.intersection(gcs_files)

    # Calculate differences
    missing_on_sftp = gcs_files - sftp_files

    # Check file sizes match
    size_mismatches = []
    for filename in relevant_sftp_files:
        gcs_size = gcs_file_sizes.get(filename, 0)
        sftp_size = sftp_file_info.get(filename, {}).get("size", 0)
        if gcs_size != sftp_size:
            size_mismatches.append({
                "filename": filename,
                "gcs_size": gcs_size,
                "sftp_size": sftp_size,
            })

    # Require at least one file in GCS - empty GCS indicates failed export
    has_files = len(gcs_files) > 0
    in_sync = has_files and len(missing_on_sftp) == 0 and len(size_mismatches) == 0

    result = {
        "status": "success",
        "in_sync": in_sync,
        "export_name": export_name,
        "gcs_path": gcs_path,
        "gcs_file_count": len(gcs_files),
        "sftp_file_count": len(relevant_sftp_files),
        "gcs_files": sorted(list(gcs_files)),
        "sftp_files": sorted(list(relevant_sftp_files)),
        "missing_on_sftp": sorted(list(missing_on_sftp)),
        "size_mismatches": size_mismatches,
        "no_files_found": not has_files,
    }

    if not has_files:
        cprint(
            f"Verification FAILED for '{export_name}': no files found in GCS",
            severity="ERROR",
            export_name=export_name,
            gcs_path=gcs_path,
        )
    elif in_sync:
        cprint(
            f"Verification passed for '{export_name}'",
            severity="INFO",
            export_name=export_name,
            file_count=len(gcs_files),
        )
    else:
        cprint(
            f"Verification FAILED for '{export_name}'",
            severity="ERROR",
            export_name=export_name,
            missing_count=len(missing_on_sftp),
            size_mismatch_count=len(size_mismatches),
        )

    return result
