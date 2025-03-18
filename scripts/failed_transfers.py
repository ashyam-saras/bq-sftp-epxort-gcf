#!/usr/bin/env python3
"""
Comparison tool to find files that failed to transfer from GCS to SFTP.
"""

import argparse
import json
from pathlib import PurePosixPath
from typing import Any, Dict, List, Set, Tuple

import paramiko
from google.cloud import storage
from rich.console import Console
from rich.table import Table


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a JSON file."""
    with open(config_path, "r") as f:
        config = json.load(f)
    return config


def list_gcs_files(bucket_name: str, prefix: str = None) -> List[str]:
    """List all files in a GCS bucket with optional prefix."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))

    # Extract just the filenames (not full paths)
    filenames = [blob.name.split("/")[-1] for blob in blobs]

    return filenames


def list_sftp_files(host: str, port: int, username: str, password: str, directory: str) -> List[str]:
    """List all files in an SFTP directory."""
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    try:
        # List files in directory
        files = sftp.listdir(directory)
        return files
    except Exception as e:
        print(f"Error listing SFTP directory: {e}")
        return []
    finally:
        sftp.close()
        transport.close()


def compare_files(gcs_files: List[str], sftp_files: List[str]) -> Tuple[Set[str], Set[str]]:
    """
    Compare GCS files with SFTP files.

    Returns:
        Tuple of (missing_on_sftp, extra_on_sftp)
    """
    gcs_set = set(gcs_files)
    sftp_set = set(sftp_files)

    missing_on_sftp = gcs_set - sftp_set
    extra_on_sftp = sftp_set - gcs_set

    return missing_on_sftp, extra_on_sftp


def main():
    parser = argparse.ArgumentParser(description="Compare files between GCS and SFTP")
    parser.add_argument("--config", "-c", default="configs/default.json", help="Path to config file")
    parser.add_argument("--gcs-bucket", help="GCS bucket name (overrides config)")
    parser.add_argument("--gcs-prefix", help="GCS prefix/folder")
    parser.add_argument("--sftp-host", help="SFTP hostname (overrides config)")
    parser.add_argument("--sftp-username", help="SFTP username (overrides config)")
    parser.add_argument("--sftp-password", help="SFTP password (overrides config)")
    parser.add_argument("--sftp-directory", help="SFTP directory (overrides config)")
    parser.add_argument("--date", help="Date folder to check (YYYYMMDD)")
    parser.add_argument("--export-name", help="Export name subfolder")

    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # GCS settings
    gcs_bucket = args.gcs_bucket or config["gcs"]["bucket"]
    gcs_prefix = args.gcs_prefix

    # SFTP settings
    sftp_config = config["sftp"]
    sftp_host = args.sftp_host or sftp_config["host"]
    sftp_port = int(sftp_config.get("port", 22))
    sftp_username = args.sftp_username or sftp_config["username"]
    sftp_password = args.sftp_password or sftp_config["password"]
    sftp_base_directory = args.sftp_directory or sftp_config["directory"]

    # Build full SFTP directory path if date and export name are provided
    sftp_directory = sftp_base_directory
    if args.date:
        sftp_directory = str(PurePosixPath(sftp_directory) / args.date)
    if args.export_name:
        sftp_directory = str(PurePosixPath(sftp_directory) / args.export_name)

    # List files
    print(f"Listing GCS files in gs://{gcs_bucket}/{gcs_prefix or ''}")
    gcs_files = list_gcs_files(gcs_bucket, gcs_prefix)
    print(f"Found {len(gcs_files)} files in GCS")

    print(f"Listing SFTP files in {sftp_directory}")
    sftp_files = list_sftp_files(sftp_host, sftp_port, sftp_username, sftp_password, sftp_directory)
    print(f"Found {len(sftp_files)} files on SFTP")

    # Compare files
    missing_on_sftp, extra_on_sftp = compare_files(gcs_files, sftp_files)

    # Pretty output with rich
    console = Console()

    if missing_on_sftp:
        console.print(f"\n[bold red]Files missing on SFTP ({len(missing_on_sftp)}):[/bold red]")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Missing Files")
        for file in sorted(missing_on_sftp):
            table.add_row(file)
        console.print(table)
    else:
        console.print("\n[bold green]No files missing on SFTP![/bold green]")

    if extra_on_sftp:
        console.print(f"\n[bold yellow]Extra files on SFTP ({len(extra_on_sftp)}):[/bold yellow]")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Extra Files")
        for file in sorted(extra_on_sftp):
            table.add_row(file)
        console.print(table)
    else:
        console.print("\n[bold green]No extra files on SFTP![/bold green]")

    # Summary
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"- GCS files: {len(gcs_files)}")
    console.print(f"- SFTP files: {len(sftp_files)}")
    console.print(f"- Missing on SFTP: {len(missing_on_sftp)}")
    console.print(f"- Extra on SFTP: {len(extra_on_sftp)}")

    # Write missing files to a text file for easy reprocessing
    if missing_on_sftp:
        with open("missing_files.txt", "w") as f:
            for file in sorted(missing_on_sftp):
                # Include full GCS path instead of just filename
                if gcs_prefix:
                    full_path = f"gs://{gcs_bucket}/{gcs_prefix}/{file}"
                else:
                    full_path = f"gs://{gcs_bucket}/{file}"
                f.write(f"{full_path}\n")
        console.print(f"\n[bold]Missing files written to missing_files.txt with full GCS paths[/bold]")


if __name__ == "__main__":
    main()
