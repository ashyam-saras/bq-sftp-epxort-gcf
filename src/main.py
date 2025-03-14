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
from src.sftp import check_sftp_credentials, upload_from_gcs, upload_from_gcs_batch


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
    overall_start_time = time.time()
    date = date or datetime.datetime.now().date()
    date_str = date.strftime(r"%Y%m%d")
    export_id = None
    temp_table = None

    cprint(
        f"Starting export process '{export_name}' for date {date_str}",
        severity="INFO",
        export_name=export_name,
        date=date_str,
    )

    # Get export-specific config
    export_config = config["exports"].get(export_name)
    if not export_config:
        cprint(f"Export '{export_name}' not found in configuration", severity="ERROR")
        raise ValueError(f"Export '{export_name}' not found in configuration")

    # Extract parameters
    source_table = export_config["source_table"]
    gcs_bucket = config["gcs"]["bucket"]
    sftp_config = config["sftp"]

    # Determine export type and parameters
    export_type = export_config.get("export_type", "full")
    cprint(f"Export type: {export_type}", source_table=source_table, gcs_bucket=gcs_bucket)

    # Parameters for incremental exports
    hash_columns = None
    processed_hashes_table = None
    if export_type == "incremental":
        hash_columns = export_config.get("hash_columns", [])
        processed_hashes_table = config["metadata"].get("processed_hashes_table")
        cprint(f"Using incremental export with {len(hash_columns)} hash columns")

    # Parameters for date range exports
    date_column = None
    days_lookback = None
    if export_type == "date_range":
        date_column = export_config.get("date_column")
        days_lookback = export_config.get("days_lookback")
        cprint(f"Using date range export with column '{date_column}' and {days_lookback} days lookback")

    # Prepare paths and filenames
    gcs_uri_prefix = construct_gcs_uri(gcs_bucket, export_name, date)
    remote_filename = f"{export_name}-{date_str}.csv"

    # SFTP folder structure - use export name as folder
    base_dir = PurePosixPath(sftp_config["directory"])
    sftp_dir = str(base_dir / export_name)
    sftp_config = {**sftp_config, "directory": sftp_dir}
    cprint(f"SFTP destination directory: {sftp_dir}")

    try:
        # 1. Record start of export
        cprint(f"Starting export process for {export_name}")
        export_id = start_export(export_name, source_table, gcs_uri_prefix)
        cprint(f"Export ID: {export_id} assigned", severity="INFO")

        # 2. Export from BigQuery to GCS
        destination_uri, row_count, temp_table = export_table_to_gcs(
            source_table=source_table,
            gcs_uri=gcs_uri_prefix,
            hash_columns=hash_columns,
            processed_hashes_table=processed_hashes_table,
            date_column=date_column,
            days_lookback=days_lookback,
            compression=export_config.get("compress", True),
        )
        cprint(
            f"BigQuery export complete with {row_count} rows",
            severity="INFO",
            destination=destination_uri,
            temp_table=temp_table,
        )

        # 3. Check SFTP credentials before attempting upload
        check_sftp_credentials(sftp_config)

        # 4. Upload from GCS to SFTP - Modified to handle sharded files
        step_start = time.time()
        cprint(f"Uploading files to SFTP", severity="INFO")

        # Handle GCS sharding by checking if destination_uri has a wildcard
        if "*" in destination_uri:
            # Get the bucket and prefix from the destination URI
            bucket_name, prefix = destination_uri.strip("gs://").split("/", 1)

            # List all files in the bucket with the prefix
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            list_start = time.time()
            blobs = list(bucket.list_blobs(prefix=prefix.split("*")[0]))

            # Filter only matching files if we have a pattern
            if "*" in prefix:
                pattern = prefix.split("/")[-1].replace("*", "")
                filtered_blobs = [b for b in blobs if pattern in b.name]
                blobs = filtered_blobs if filtered_blobs else blobs

            cprint(
                f"Found {len(blobs)} files to upload to SFTP",
                severity="INFO",
                list_time=f"{time.time() - list_start:.2f}s",
            )

            # Prepare the file mappings for batch upload
            file_mappings = []
            for i, blob in enumerate(blobs):
                # Create suffix for multiple files
                file_suffix = f"-{i+1:012d}" if len(blobs) > 1 else ""
                remote_file = f"{export_name}-{date_str}{file_suffix}.csv"
                if blob.name.endswith(".gz"):
                    remote_file += ".gz"

                file_mappings.append((f"gs://{bucket_name}/{blob.name}", remote_file))

            # Show names of files being transferred (still useful for debugging)
            if file_mappings:
                sample_files = file_mappings[:5]
                cprint(f"Files to transfer: {sample_files}{'...' if len(file_mappings) > 5 else ''}")

            # Use the optimized batch upload function
            files_transferred = upload_from_gcs_batch(sftp_config, file_mappings)

            cprint(
                f"SFTP upload complete: {files_transferred} files transferred",
                severity="INFO",
                step_time=f"{time.time() - step_start:.2f}s",
            )

        else:
            # Single file case remains unchanged
            remote_filename = f"{export_name}-{date_str}.csv"
            if destination_uri.endswith(".gz"):
                remote_filename += ".gz"

            cprint(
                f"Uploading single file",
                severity="INFO",
                source=destination_uri,
                destination=f"{sftp_dir}/{remote_filename}",
            )

            upload_from_gcs(sftp_config=sftp_config, gcs_uri=destination_uri, remote_filename=remote_filename)
            files_transferred = 1

        cprint(
            f"SFTP upload complete: {files_transferred} files transferred",
            severity="INFO",
            step_time=f"{time.time() - step_start:.2f}s",
        )

        # 5. For incremental exports, record processed hashes
        if export_type == "incremental" and temp_table:
            cprint("Recording processed hashes", severity="INFO")
            hashes_recorded = record_processed_hashes(export_id, export_name, temp_table)
            cprint(f"Recorded {hashes_recorded} new hash records", severity="INFO")

        # 6. Record export completion
        complete_export(export_id, row_count)

        # 7. Clean up temporary table if it exists
        if temp_table:
            cprint("Cleaning up temporary resources", severity="INFO")
            delete_table(temp_table, source_table)

        # Calculate total process time
        total_time = time.time() - overall_start_time

        # Use path joining with / operator for display path
        destination_path = f"{sftp_dir}/{remote_filename if 'remote_filename' in locals() else '*.csv.gz'}"

        cprint(
            f"Export process '{export_name}' completed successfully",
            severity="INFO",
            rows=row_count,
            files=files_transferred,
            destination=destination_path,
            total_time=f"{total_time:.2f}s",
        )

        return {
            "status": "success",
            "export_id": export_id,
            "export_name": export_name,
            "rows_exported": row_count,
            "destination": destination_path,
            "date": date_str,
            "files_transferred": files_transferred,
            "total_time_seconds": round(total_time, 2),
        }

    except Exception as e:
        # Handle any errors that occur during the export process
        error_time = time.time() - overall_start_time
        cprint(
            f"Export failed after {error_time:.2f}s: {str(e)}",
            severity="ERROR",
            export_name=export_name,
            export_id=export_id,
        )

        if export_id:
            fail_export(export_id, str(e))

        # Clean up temporary table on failure
        if temp_table:
            try:
                delete_table(temp_table, source_table)
                cprint("Cleaned up temporary table after failure", severity="INFO")
            except Exception as cleanup_error:
                cprint(f"Failed to clean up temporary table: {str(cleanup_error)}", severity="WARNING")

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
