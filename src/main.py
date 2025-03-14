"""
Main entry point for BigQuery to SFTP export function.
Orchestrates the entire export process from BigQuery query to SFTP upload.
"""

import base64
import datetime
import json
import time
from pathlib import PurePosixPath
from typing import Any, Dict, Optional

from google.cloud import storage

from src.bigquery import construct_gcs_uri, delete_table, export_table_to_gcs
from src.config import load_config
from src.helpers import cprint
from src.metadata import complete_export, fail_export, record_processed_hashes, start_export
from src.sftp import check_sftp_credentials, upload_from_gcs


def export_to_sftp(config: Dict[str, Any], export_name: str, date: Optional[datetime.date] = None) -> Dict[str, Any]:
    """
    Orchestrate the end-to-end export process from BigQuery to SFTP.

    Args:
        config: Configuration dictionary containing all necessary parameters
        export_name: Name of the export to process
        date: Date to use for the export (defaults to today)

    Returns:
        Dict with export results information
    """
    date = date or datetime.datetime.now().date()
    date_str = date.strftime(r"%Y%m%d")
    export_id = None
    temp_table = None

    # Get export-specific config
    export_config = config["exports"].get(export_name)
    if not export_config:
        raise ValueError(f"Export '{export_name}' not found in configuration")

    # Extract parameters
    source_table = export_config["source_table"]
    gcs_bucket = config["gcs"]["bucket"]
    sftp_config = config["sftp"]

    # Determine export type and parameters
    export_type = export_config.get("export_type", "full")

    # Parameters for incremental exports
    hash_columns = None
    processed_hashes_table = None
    if export_type == "incremental":
        hash_columns = export_config.get("hash_columns", [])
        processed_hashes_table = config["metadata"].get("processed_hashes_table")

    # Parameters for date range exports
    date_column = None
    days_lookback = None
    if export_type == "date_range":
        date_column = export_config.get("date_column")
        days_lookback = export_config.get("days_lookback")

    # Prepare paths and filenames
    gcs_uri_prefix = construct_gcs_uri(gcs_bucket, export_name, date)
    remote_filename = f"{export_name}-{date_str}.csv"

    # SFTP folder structure - use export name as folder
    base_dir = PurePosixPath(sftp_config["directory"])
    sftp_dir = str(base_dir / export_name)
    sftp_config = {**sftp_config, "directory": sftp_dir}

    try:
        # 1. Record start of export
        cprint(f"Starting export process for {export_name}")
        export_id = start_export(export_name, source_table, gcs_uri_prefix)

        # 2. Export from BigQuery to GCS
        cprint(f"Exporting {source_table} to GCS using {export_type} method")
        destination_uri, row_count, temp_table = export_table_to_gcs(
            source_table=source_table,
            gcs_uri=gcs_uri_prefix,
            hash_columns=hash_columns,
            processed_hashes_table=processed_hashes_table,
            date_column=date_column,
            days_lookback=days_lookback,
            compression=export_config.get("compress", True),
        )

        # 3. Check SFTP credentials before attempting upload
        cprint("Verifying SFTP credentials")
        check_sftp_credentials(sftp_config)

        # 4. Upload from GCS to SFTP - Modified to handle sharded files
        cprint(f"Uploading exported files to SFTP")

        # Handle GCS sharding by checking if destination_uri has a wildcard
        if "*" in destination_uri:
            # Get the bucket and prefix from the destination URI
            bucket_name, prefix = destination_uri.strip("gs://").split("/", 1)

            # List all files in the bucket with the prefix
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(match_glob=prefix))

            cprint(f"Found {len(blobs)} files to upload to SFTP")

            # Upload each file with an appropriate name
            for i, blob in enumerate(blobs):
                # Create suffix for multiple files
                file_suffix = f"-part{i+1:012d}" if len(blobs) > 1 else ""
                # Remove .gz extension if needed for naming but keep for upload
                remote_file = f"{export_name}-{date_str}{file_suffix}.csv"
                if blob.name.endswith(".gz"):
                    remote_file += ".gz"

                cprint(f"Uploading file {i+1} of {len(blobs)}: {blob.name} to {remote_file}")
                upload_from_gcs(
                    sftp_config=sftp_config,
                    gcs_uri=f"gs://{bucket_name}/{blob.name}",
                    remote_filename=remote_file,
                )
        else:
            # Single file case
            remote_filename = f"{export_name}-{date_str}.csv"
            if destination_uri.endswith(".gz"):
                remote_filename += ".gz"

            upload_from_gcs(sftp_config=sftp_config, gcs_uri=destination_uri, remote_filename=remote_filename)

        # 5. For incremental exports, record processed hashes
        if export_type == "incremental" and temp_table:
            cprint("Recording processed hashes for incremental export")
            hashes_recorded = record_processed_hashes(export_id, export_name, temp_table)
            cprint(f"Recorded {hashes_recorded} new hash records for incremental tracking")

        # 6. Record export completion
        cprint(f"Export completed successfully with {row_count} rows")
        complete_export(export_id, row_count)

        # 7. Clean up temporary table if it exists (now using the dedicated function)
        if temp_table:
            cprint(f"Cleaning up temporary table {temp_table}")
            delete_table(temp_table, source_table)

        # Use path joining with / operator for display path
        destination_path = f"{sftp_dir}/{remote_filename if 'remote_filename' in locals() else '*.csv.gz'}"

        return {
            "status": "success",
            "export_id": export_id,
            "export_name": export_name,
            "rows_exported": row_count,
            "destination": destination_path,
            "date": date_str,
            "files_transferred": len(blobs) if "blobs" in locals() else 1,
        }

    except Exception as e:
        # Handle any errors that occur during the export process
        cprint(f"Export failed: {str(e)}", severity="ERROR")
        if export_id:
            fail_export(export_id, str(e))

        # Clean up temporary table on failure (also using the dedicated function)
        if temp_table:
            delete_table(temp_table, source_table)

        raise


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
    parser = argparse.ArgumentParser(description="BigQuery to SFTP Export")
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
        print(f"Exported {result['rows_exported']} rows to {result['destination']}")
    except Exception as e:
        print(f"Export failed: {str(e)}")
        exit(1)
