#!/usr/bin/env python3
"""
Retry SFTP transfers for missing files identified by failed_transfers.py
"""

import argparse
import json
import os
from typing import Any, Dict, List

from src.helpers import cprint
from src.sftp import upload_from_gcs


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a JSON file."""
    with open(config_path, "r") as f:
        config = json.load(f)
    return config


def retry_transfers(sftp_config: Dict[str, Any], missing_files_path: str, sftp_directory: str):
    """Retry transferring files that were missed."""
    print(f"Loading missing files list from {missing_files_path}")

    # Read GCS URIs from file
    with open(missing_files_path, "r") as f:
        gcs_uris = [line.strip() for line in f if line.strip()]

    if not gcs_uris:
        print("No files to retry.")
        return

    print(f"Found {len(gcs_uris)} files to transfer")

    # Update SFTP directory in config
    sftp_config = {**sftp_config, "directory": sftp_directory}

    successful = 0
    failed = 0

    # Process each file
    for i, gcs_uri in enumerate(gcs_uris):
        try:
            # Extract just the filename from the GCS URI
            filename = gcs_uri.split("/")[-1]
            print(f"[{i+1}/{len(gcs_uris)}] Transferring {filename}...")

            # Use the existing upload function
            upload_from_gcs(sftp_config, gcs_uri, filename)
            successful += 1
            print(f"✅ Successfully transferred {filename}")

        except Exception as e:
            print(f"❌ Failed to transfer {filename}: {str(e)}")
            failed += 1

    # Print summary
    print("\nTransfer Summary:")
    print(f"- Total attempted: {len(gcs_uris)}")
    print(f"- Successfully transferred: {successful}")
    print(f"- Failed: {failed}")


def main():
    parser = argparse.ArgumentParser(description="Retry failed SFTP transfers")
    parser.add_argument("--config", "-c", default="configs/default.json", help="Path to config file")
    parser.add_argument("--missing-files", "-f", default="missing_files.txt", help="Path to missing files list")
    parser.add_argument("--sftp-directory", "-d", help="SFTP destination directory")
    parser.add_argument("--date", help="Date folder (YYYYMMDD)")
    parser.add_argument("--export-name", help="Export name subfolder")

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    sftp_config = config["sftp"]

    # Determine SFTP directory
    sftp_directory = args.sftp_directory or sftp_config["directory"]
    if args.date:
        sftp_directory = f"{sftp_directory}/{args.date}"
    if args.export_name:
        sftp_directory = f"{sftp_directory}/{args.export_name}"

    # Run the retry process
    retry_transfers(sftp_config, args.missing_files, sftp_directory)


if __name__ == "__main__":
    main()
