#!/usr/bin/env python3
"""
GCS bucket summary - shows statistics for each export.

Usage:
    python -m scripts.gcs_summary --bucket boxout-sftp-transfer
    python -m scripts.gcs_summary --bucket boxout-sftp-transfer --export Product
"""

import argparse
from collections import defaultdict
from typing import Dict, List, Any

from google.cloud import storage


def format_size(size_bytes: int) -> str:
    """Format bytes into human readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def get_bucket_summary(bucket_name: str, export_filter: str = None) -> Dict[str, Any]:
    """
    Get summary statistics for each export in the bucket.
    
    Returns dict with stats per export.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Structure: {export_name: {date: [blobs]}}
    exports: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    
    print(f"Scanning gs://{bucket_name}/...")
    
    for blob in bucket.list_blobs():
        # Skip "directory" markers
        if blob.name.endswith("/"):
            continue
        
        parts = blob.name.split("/")
        if len(parts) < 3:
            continue
        
        export_name = parts[0]
        date_folder = parts[1]
        
        # Filter if specified
        if export_filter and export_name != export_filter:
            continue
        
        exports[export_name][date_folder].append({
            "name": blob.name,
            "size": blob.size or 0,
        })
    
    # Calculate stats
    summaries = {}
    for export_name, dates in sorted(exports.items()):
        sorted_dates = sorted(dates.keys())
        
        total_files = sum(len(files) for files in dates.values())
        total_size = sum(f["size"] for files in dates.values() for f in files)
        
        files_per_date = [len(files) for files in dates.values()]
        sizes_per_file = [f["size"] for files in dates.values() for f in files]
        
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


def print_summary(summaries: Dict[str, Any]) -> None:
    """Print formatted summary table."""
    if not summaries:
        print("No exports found.")
        return
    
    # Header
    print()
    print(f"{'Export':<45} {'Oldest':<10} {'Newest':<10} {'Dates':>6} {'Files':>7} {'Total Size':>12} {'Avg Files/Date':>14} {'Avg Size/File':>14}")
    print("-" * 130)
    
    grand_total_files = 0
    grand_total_size = 0
    
    for export_name, stats in summaries.items():
        grand_total_files += stats["total_files"]
        grand_total_size += stats["total_size"]
        
        print(
            f"{export_name:<45} "
            f"{stats['oldest_date'] or 'N/A':<10} "
            f"{stats['newest_date'] or 'N/A':<10} "
            f"{stats['num_dates']:>6} "
            f"{stats['total_files']:>7} "
            f"{format_size(stats['total_size']):>12} "
            f"{stats['avg_files_per_date']:>14.1f} "
            f"{format_size(stats['avg_size_per_file']):>14}"
        )
    
    # Footer
    print("-" * 130)
    print(f"{'TOTAL':<45} {'':<10} {'':<10} {'':<6} {grand_total_files:>7} {format_size(grand_total_size):>12}")
    print()


def main():
    parser = argparse.ArgumentParser(description="GCS bucket export summary")
    parser.add_argument("--bucket", "-b", required=True, help="GCS bucket name")
    parser.add_argument("--export", "-e", help="Filter to specific export name")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    summaries = get_bucket_summary(args.bucket, args.export)
    
    if args.json:
        import json
        print(json.dumps(summaries, indent=2))
    else:
        print_summary(summaries)


if __name__ == "__main__":
    main()
