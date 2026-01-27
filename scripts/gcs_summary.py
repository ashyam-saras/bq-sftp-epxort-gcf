#!/usr/bin/env python3
"""
Storage summary - shows statistics for exports in GCS and/or SFTP.

Usage:
    # GCS only
    python -m scripts.gcs_summary --bucket boxout-sftp-transfer
    
    # SFTP only (uses SFTP_PASSWORD from .env)
    python -m scripts.gcs_summary --sftp-host sftp.example.com --sftp-user user --sftp-dir /uploads
    
    # Both GCS and SFTP
    python -m scripts.gcs_summary --bucket boxout-sftp-transfer --sftp-host sftp.example.com --sftp-user user --sftp-dir /uploads
"""

import argparse
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Load .env file (but preserve existing GOOGLE_APPLICATION_CREDENTIALS to use gcloud auth)
from dotenv import load_dotenv
_gcp_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
load_dotenv(Path(__file__).parent.parent / ".env")
if _gcp_creds:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _gcp_creds
elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    # Remove invalid key from .env, use gcloud default instead
    del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

from google.cloud import storage


def format_size(size_bytes: int) -> str:
    """Format bytes into human readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def extract_export_and_date(filename: str) -> tuple:
    """
    Extract export name and date from filename.
    Expects format: {export_name}_{date}-{shard}.csv.gz
    Example: Product_20260127-000000000000.csv.gz -> (Product, 20260127)
    """
    # Pattern: name_YYYYMMDD-shard.ext
    match = re.match(r'^(.+?)_(\d{8})-\d+\.', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None


def get_gcs_summary(bucket_name: str, export_filter: str = None) -> Dict[str, Any]:
    """Get summary statistics for each export in GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Structure: {export_name: {date: [blobs]}}
    exports: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    
    print(f"Scanning gs://{bucket_name}/...")
    
    for blob in bucket.list_blobs():
        if blob.name.endswith("/"):
            continue
        
        parts = blob.name.split("/")
        if len(parts) < 3:
            continue
        
        export_name = parts[0]
        date_folder = parts[1]
        
        if export_filter and export_name != export_filter:
            continue
        
        exports[export_name][date_folder].append({
            "name": blob.name.split("/")[-1],
            "size": blob.size or 0,
        })
    
    return _calculate_stats(exports)


def get_sftp_summary(
    host: str, 
    username: str, 
    password: str, 
    directory: str,
    port: int = 22,
    export_filter: str = None
) -> Dict[str, Any]:
    """Get summary statistics for exports on SFTP server."""
    import paramiko
    
    print(f"Scanning sftp://{host}{directory}/...")
    
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    # Structure: {export_name: {date: [files]}}
    exports: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    
    try:
        for attr in sftp.listdir_attr(directory):
            # Skip directories
            if attr.st_mode and (attr.st_mode & 0o40000):
                continue
            
            filename = attr.filename
            export_name, date_str = extract_export_and_date(filename)
            
            if not export_name or not date_str:
                continue
            
            if export_filter and export_name != export_filter:
                continue
            
            exports[export_name][date_str].append({
                "name": filename,
                "size": attr.st_size or 0,
            })
    finally:
        sftp.close()
        transport.close()
    
    return _calculate_stats(exports)


def _calculate_stats(exports: Dict[str, Dict[str, List]]) -> Dict[str, Any]:
    """Calculate summary statistics from exports dict."""
    summaries = {}
    for export_name, dates in sorted(exports.items()):
        sorted_dates = sorted(dates.keys())
        
        total_files = sum(len(files) for files in dates.values())
        total_size = sum(f["size"] for files in dates.values() for f in files)
        
        summaries[export_name] = {
            "oldest_date": sorted_dates[0] if sorted_dates else None,
            "newest_date": sorted_dates[-1] if sorted_dates else None,
            "num_dates": len(sorted_dates),
            "total_files": total_files,
            "total_size": total_size,
            "avg_files_per_date": total_files / len(dates) if dates else 0,
            "avg_size_per_file": total_size / total_files if total_files else 0,
        }
    
    return summaries


def print_summary(gcs_summaries: Dict[str, Any], sftp_summaries: Dict[str, Any] = None) -> None:
    """Print formatted summary table."""
    
    if gcs_summaries:
        print("\n=== GCS Summary ===")
        _print_table(gcs_summaries)
    
    if sftp_summaries:
        print("\n=== SFTP Summary ===")
        _print_table(sftp_summaries)
    
    # Comparison if both
    if gcs_summaries and sftp_summaries:
        print("\n=== GCS vs SFTP Comparison ===")
        all_exports = set(gcs_summaries.keys()) | set(sftp_summaries.keys())
        
        print(f"\n{'Export':<40} {'GCS Files':>10} {'SFTP Files':>11} {'GCS Size':>12} {'SFTP Size':>12} {'Status':<10}")
        print("-" * 100)
        
        for export in sorted(all_exports):
            gcs = gcs_summaries.get(export, {})
            sftp = sftp_summaries.get(export, {})
            
            gcs_files = gcs.get("total_files", 0)
            sftp_files = sftp.get("total_files", 0)
            gcs_size = gcs.get("total_size", 0)
            sftp_size = sftp.get("total_size", 0)
            
            if gcs_files == sftp_files and gcs_size == sftp_size:
                status = "OK"
            elif gcs_files == 0:
                status = "GCS empty"
            elif sftp_files == 0:
                status = "SFTP empty"
            else:
                status = "MISMATCH"
            
            print(
                f"{export:<40} "
                f"{gcs_files:>10} "
                f"{sftp_files:>11} "
                f"{format_size(gcs_size):>12} "
                f"{format_size(sftp_size):>12} "
                f"{status:<10}"
            )
        print()


def _print_table(summaries: Dict[str, Any]) -> None:
    """Print a single summary table."""
    if not summaries:
        print("No exports found.")
        return
    
    print(f"\n{'Export':<40} {'Oldest':<10} {'Newest':<10} {'Dates':>6} {'Files':>7} {'Total Size':>12} {'Avg Files/Date':>14} {'Avg Size/File':>14}")
    print("-" * 125)
    
    grand_total_files = 0
    grand_total_size = 0
    
    for export_name, stats in summaries.items():
        grand_total_files += stats["total_files"]
        grand_total_size += stats["total_size"]
        
        print(
            f"{export_name:<40} "
            f"{stats['oldest_date'] or 'N/A':<10} "
            f"{stats['newest_date'] or 'N/A':<10} "
            f"{stats['num_dates']:>6} "
            f"{stats['total_files']:>7} "
            f"{format_size(stats['total_size']):>12} "
            f"{stats['avg_files_per_date']:>14.1f} "
            f"{format_size(stats['avg_size_per_file']):>14}"
        )
    
    print("-" * 125)
    print(f"{'TOTAL':<40} {'':<10} {'':<10} {'':<6} {grand_total_files:>7} {format_size(grand_total_size):>12}")


def main():
    parser = argparse.ArgumentParser(description="GCS/SFTP export summary")
    
    # GCS options
    parser.add_argument("--bucket", "-b", default=os.environ.get("GCS_BUCKET"), help="GCS bucket name")
    
    # SFTP options (defaults from .env)
    parser.add_argument("--sftp-host", default=os.environ.get("SFTP_HOST"), help="SFTP host")
    parser.add_argument("--sftp-port", type=int, default=int(os.environ.get("SFTP_PORT", 22)), help="SFTP port")
    parser.add_argument("--sftp-user", default=os.environ.get("SFTP_USERNAME"), help="SFTP username")
    parser.add_argument("--sftp-pass", default=os.environ.get("SFTP_PASSWORD"), help="SFTP password")
    parser.add_argument("--sftp-dir", default=os.environ.get("SFTP_DIRECTORY"), help="SFTP directory")
    
    # Common options
    parser.add_argument("--export", "-e", help="Filter to specific export name")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--gcs", action="store_true", help="Show GCS summary")
    parser.add_argument("--sftp", action="store_true", help="Show SFTP summary")
    
    args = parser.parse_args()
    
    # If neither --gcs nor --sftp specified, show both if configured
    show_gcs = args.gcs or (not args.gcs and not args.sftp and args.bucket)
    show_sftp = args.sftp or (not args.gcs and not args.sftp and args.sftp_host)
    
    if not show_gcs and not show_sftp:
        parser.error("No storage configured. Set GCS_BUCKET or SFTP_HOST in .env, or use --bucket/--sftp-host")
    
    gcs_summaries = None
    sftp_summaries = None
    
    if show_gcs and args.bucket:
        gcs_summaries = get_gcs_summary(args.bucket, args.export)
    
    if show_sftp and args.sftp_host:
        if not all([args.sftp_user, args.sftp_pass, args.sftp_dir]):
            parser.error("SFTP requires host, user, password, and directory (check .env)")
        sftp_summaries = get_sftp_summary(
            args.sftp_host,
            args.sftp_user,
            args.sftp_pass,
            args.sftp_dir,
            args.sftp_port,
            args.export,
        )
    
    if args.json:
        import json
        result = {}
        if gcs_summaries:
            result["gcs"] = gcs_summaries
        if sftp_summaries:
            result["sftp"] = sftp_summaries
        print(json.dumps(result, indent=2))
    else:
        print_summary(gcs_summaries, sftp_summaries)


if __name__ == "__main__":
    main()
