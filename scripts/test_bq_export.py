#!/usr/bin/env python3
"""
Test script for BigQuery → GCS export flow.

Usage:
    # Dry run - just print the query
    python scripts/test_bq_export.py --export example_daily_export --dry-run

    # Actually run the export
    python scripts/test_bq_export.py --export example_daily_export --date 2025-01-08

    # With interval (simulates data_interval_start and end)
    python scripts/test_bq_export.py --export example_interval_export --start 2025-01-08 --end 2025-01-09

    # List files after export
    python scripts/test_bq_export.py --export example_daily_export --list-files
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def resolve_placeholders(
    query: str,
    ds: str,
    ds_nodash: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> str:
    """
    Replace placeholders in query with actual values.

    Supported placeholders:
    - {ds}                  - Date as YYYY-MM-DD
    - {ds_nodash}           - Date as YYYYMMDD
    - {date}                - Alias for {ds_nodash}
    - {date_dash}           - Alias for {ds}
    - {data_interval_start} - Interval start as YYYY-MM-DD HH:MM:SS
    - {data_interval_end}   - Interval end as YYYY-MM-DD HH:MM:SS
    """
    replacements = {
        "{ds}": ds,
        "{ds_nodash}": ds_nodash,
        "{date}": ds_nodash,
        "{date_dash}": ds,
        "{data_interval_start}": data_interval_start.strftime("%Y-%m-%d %H:%M:%S"),
        "{data_interval_end}": data_interval_end.strftime("%Y-%m-%d %H:%M:%S"),
    }

    for placeholder, value in replacements.items():
        query = query.replace(placeholder, value)

    return query


def build_export_query(
    query: str,
    gcs_bucket: str,
    export_name: str,
    ds_nodash: str,
    format: str = "CSV",
    compression: str = "GZIP",
    overwrite: bool = True,
) -> str:
    """Build a BigQuery EXPORT DATA statement."""
    # Build GCS URI pattern
    extension = format.lower()
    if compression.upper() == "GZIP":
        extension += ".gz"
    elif compression.upper() == "SNAPPY":
        extension += ".snappy"

    gcs_uri = f"gs://{gcs_bucket}/{export_name}/{ds_nodash}/{export_name}-*.{extension}"

    # Build EXPORT DATA statement
    export_sql = f"""EXPORT DATA OPTIONS(
    uri='{gcs_uri}',
    format='{format}',
    compression='{compression}',
    overwrite={str(overwrite).lower()},
    header=true,
    field_delimiter='|'
) AS
{query}"""
    return export_sql


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file."""
    with open(config_path) as f:
        return json.load(f)


def run_export(query: str, project_id: str = None, location: str = "US") -> dict:
    """Execute the EXPORT DATA query on BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    print(f"\n{'='*60}")
    print("Executing query on BigQuery...")
    print(f"{'='*60}\n")

    job = client.query(query, location=location)
    result = job.result()  # Wait for completion

    return {
        "job_id": job.job_id,
        "state": job.state,
        "total_bytes_processed": job.total_bytes_processed,
        "total_bytes_billed": job.total_bytes_billed,
        "creation_time": str(job.created),
        "end_time": str(job.ended),
    }


def list_exported_files(gcs_path: str) -> list:
    """List files exported to GCS."""
    from google.cloud import storage

    # Parse gs://bucket/prefix
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    path = gcs_path[5:]
    parts = path.split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    # Remove wildcard for listing
    if "*" in prefix:
        prefix = prefix[: prefix.find("*")]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    files = []
    for blob in blobs:
        if not blob.name.endswith("/"):
            files.append(
                {
                    "name": blob.name,
                    "size_mb": round(blob.size / (1024 * 1024), 2),
                    "updated": str(blob.updated),
                }
            )

    return files


def main():
    parser = argparse.ArgumentParser(
        description="Test BigQuery to GCS export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run for a specific date
  python scripts/test_bq_export.py --export my_export --date 2025-01-08 --dry-run

  # Run with date interval (for queries using data_interval_start/end)
  python scripts/test_bq_export.py --export my_export --start 2025-01-08 --end 2025-01-09

  # Execute and list exported files
  python scripts/test_bq_export.py --export my_export --date 2025-01-08 --list-files
        """,
    )
    parser.add_argument("--config", default="configs/exports.json", help="Config file path")
    parser.add_argument("--export", required=True, help="Export name from config")
    parser.add_argument("--date", help="Export date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--start", help="Data interval start (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)")
    parser.add_argument("--end", help="Data interval end (YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Only print query, don't execute")
    parser.add_argument("--project", help="GCP project ID (uses default if not specified)")
    parser.add_argument("--location", default="US", help="BigQuery location")
    parser.add_argument("--list-files", action="store_true", help="List exported files after export")

    args = parser.parse_args()

    # Load config
    print(f"Loading config from: {args.config}")
    config = load_config(args.config)

    # Get export config
    exports = config.get("exports", {})
    if args.export not in exports:
        print(f"❌ Export '{args.export}' not found in config")
        print(f"   Available exports: {list(exports.keys())}")
        sys.exit(1)

    export_config = exports[args.export]
    gcs_bucket = config["gcs_bucket"]

    # Parse dates
    if args.date:
        base_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    ds = base_date.strftime("%Y-%m-%d")
    ds_nodash = base_date.strftime("%Y%m%d")

    # Parse interval (for queries that use data_interval_start/end)
    if args.start:
        try:
            data_interval_start = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            data_interval_start = datetime.strptime(args.start, "%Y-%m-%d")
    else:
        data_interval_start = base_date

    if args.end:
        try:
            data_interval_end = datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            data_interval_end = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        data_interval_end = data_interval_start + timedelta(days=1)

    print(f"\n{'='*60}")
    print(f"Export: {args.export}")
    print(f"Date (ds): {ds}")
    print(f"Date (ds_nodash): {ds_nodash}")
    print(f"Data Interval Start: {data_interval_start}")
    print(f"Data Interval End: {data_interval_end}")
    print(f"Bucket: {gcs_bucket}")
    print(f"{'='*60}")

    # Resolve placeholders in the query
    resolved_query = resolve_placeholders(
        query=export_config["query"],
        ds=ds,
        ds_nodash=ds_nodash,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
    )

    # Build full export query
    query = build_export_query(
        query=resolved_query,
        gcs_bucket=gcs_bucket,
        export_name=args.export,
        ds_nodash=ds_nodash,
        format=export_config.get("format", "CSV"),
        compression=export_config.get("compression", "GZIP"),
    )

    print(f"\nGenerated EXPORT DATA query:\n")
    print("-" * 60)
    print(query)
    print("-" * 60)

    if args.dry_run:
        print("\n✅ Dry run complete. Query looks good!")
        print("   Remove --dry-run to actually execute the export.")
        return

    # Execute export
    try:
        result = run_export(query, project_id=args.project, location=args.location)

        print(f"\n✅ Export completed successfully!")
        print(f"   Job ID: {result['job_id']}")
        print(f"   State: {result['state']}")
        print(f"   Bytes processed: {result['total_bytes_processed']:,}")
        print(f"   Bytes billed: {result['total_bytes_billed']:,}")

        # List exported files
        if args.list_files:
            gcs_path = f"gs://{gcs_bucket}/{args.export}/{ds_nodash}/"
            print(f"\n📁 Files exported to {gcs_path}:")
            files = list_exported_files(gcs_path)
            if files:
                for f in files:
                    print(f"   - {f['name']} ({f['size_mb']} MB)")
            else:
                print("   (no files found)")

    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
