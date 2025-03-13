"""
Metadata tracking for BigQuery to SFTP export jobs.
Handles tracking export status and maintaining incremental export hash tables.
"""

import argparse
import datetime
import uuid
from typing import Any, Dict

from google.cloud import bigquery

from src.config import ConfigError, load_config
from src.helpers import cprint

# Create a shared BigQuery client
client = bigquery.Client()

# Load config when module is imported - will fail if config is missing or invalid
try:
    config = load_config()
    metadata_config = config.get("metadata", {})

    # Get fully qualified table names directly from config - required, no fallbacks
    if "export_metadata_table" not in metadata_config:
        raise ConfigError("Required configuration 'metadata.export_metadata_table' is missing")

    if "processed_hashes_table" not in metadata_config:
        raise ConfigError("Required configuration 'metadata.processed_hashes_table' is missing")

    EXPORT_METADATA_TABLE = metadata_config["export_metadata_table"]
    EXPORT_PROCESSED_HASHES_TABLE = metadata_config["processed_hashes_table"]

    cprint(f"Metadata tables configured: {EXPORT_METADATA_TABLE}, {EXPORT_PROCESSED_HASHES_TABLE}")

except Exception as e:
    # Re-raise any configuration errors
    cprint(f"Configuration error in metadata module: {str(e)}", severity="ERROR")
    raise

# Export status constants
STATUS_STARTED = "STARTED"
STATUS_SUCCESS = "SUCCESS"
STATUS_ERROR = "ERROR"


def generate_export_id() -> str:
    """Generate a unique export ID."""
    return str(uuid.uuid4())


def start_export(export_name: str, source_table: str, destination_uri: str) -> str:
    """
    Record the start of an export job.

    Args:
        export_name: Name of the export
        source_table: Source table for the export
        destination_uri: Destination URI for the export

    Returns:
        str: Export ID
    """
    export_id = generate_export_id()

    # Insert new export record using SQL DML
    now = datetime.datetime.now(datetime.timezone.utc)

    query = f"""
    INSERT INTO `{EXPORT_METADATA_TABLE}` (export_id, export_name, source_table, destination_uri, status, started_at)
    VALUES (@export_id, @export_name, @source_table, @destination_uri, @status, @started_at)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
            bigquery.ScalarQueryParameter("export_name", "STRING", export_name),
            bigquery.ScalarQueryParameter("source_table", "STRING", source_table),
            bigquery.ScalarQueryParameter("destination_uri", "STRING", destination_uri),
            bigquery.ScalarQueryParameter("status", "STRING", STATUS_STARTED),
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", now),
        ]
    )

    cprint(f"Starting export {export_id} for {export_name}")
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    cprint(f"Started export {export_id} for {export_name}")
    return export_id


def complete_export(export_id: str, rows_exported: int) -> None:
    """
    Mark an export as successfully completed.

    Args:
        export_id: Export ID to update
        rows_exported: Number of rows exported
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    query = f"""
    UPDATE `{EXPORT_METADATA_TABLE}`
    SET status = @status,
        rows_exported = @rows_exported,
        completed_at = @completed_at
    WHERE export_id = @export_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", STATUS_SUCCESS),
            bigquery.ScalarQueryParameter("rows_exported", "INTEGER", rows_exported),
            bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
        ]
    )

    cprint(f"Marking export {export_id} as complete", rows_exported=rows_exported)
    query_job = client.query(query, job_config=job_config)
    query_job.result()


def fail_export(export_id: str, error_message: str) -> None:
    """
    Mark an export as failed.

    Args:
        export_id: Export ID to update
        error_message: Error message for the failure
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    # Truncate error message if it's too long
    if len(error_message) > 1024:
        error_message = error_message[:1021] + "..."

    query = f"""
    UPDATE `{EXPORT_METADATA_TABLE}`
    SET status = @status,
        error_message = @error_message,
        completed_at = @completed_at
    WHERE export_id = @export_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", STATUS_ERROR),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
        ]
    )

    cprint(f"Marking export {export_id} as failed", error=error_message, severity="ERROR")
    query_job = client.query(query, job_config=job_config)
    query_job.result()


def get_export_status(export_id: str) -> Dict[str, Any]:
    """
    Get the status of an export.

    Args:
        export_id: Export ID to check

    Returns:
        Dict[str, Any]: Export status details
    """
    query = f"""
    SELECT *
    FROM `{EXPORT_METADATA_TABLE}`
    WHERE export_id = @export_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    for row in results:
        # Convert to dictionary
        result = {key: value for key, value in row.items()}
        return result

    raise Exception(f"Export {export_id} not found")


def record_processed_hashes(export_id: str, export_name: str, temp_table_name: str) -> int:
    """
    Record hashes for processed rows.

    Args:
        export_id: Export ID
        export_name: Export name
        temp_table_name: Temporary table containing rows with hashes

    Returns:
        int: Number of hashes recorded
    """
    if not temp_table_name:
        cprint("No temp table provided for hash recording", severity="WARNING")
        return 0

    now = datetime.datetime.now(datetime.timezone.utc)

    # Insert hashes from temp table
    # Note: We can't parameterize the column values in the SELECT statement,
    # but we can use parameters in a WITH clause
    query = f"""
    WITH parameters AS (
      SELECT 
        @export_id AS export_id,
        @export_name AS export_name,
        @processed_at AS processed_at
    )
    INSERT INTO `{EXPORT_PROCESSED_HASHES_TABLE}` (export_id, export_name, row_hash, processed_at)
    SELECT 
      parameters.export_id, 
      parameters.export_name,
      temp.row_hash,
      parameters.processed_at
    FROM `{temp_table_name}` temp, parameters
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
            bigquery.ScalarQueryParameter("export_name", "STRING", export_name),
            bigquery.ScalarQueryParameter("processed_at", "TIMESTAMP", now),
        ]
    )

    cprint(f"Recording processed hashes from {temp_table_name} to {EXPORT_PROCESSED_HASHES_TABLE}")
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    # Count inserted rows
    count_query = f"""
    SELECT COUNT(*) as count
    FROM `{EXPORT_PROCESSED_HASHES_TABLE}`
    WHERE export_id = @export_id
    """

    count_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
        ]
    )

    count_job = client.query(count_query, job_config=count_config)
    results = count_job.result()

    for row in results:
        return row.count

    return 0


if __name__ == "__main__":
    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    # Set up command line argument parsing for testing
    parser = argparse.ArgumentParser(description="Manage export metadata")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Show tables command
    show_parser = subparsers.add_parser("show-tables", help="Show metadata table names")

    # Start export command
    start_parser = subparsers.add_parser("start", help="Start a new export")
    start_parser.add_argument("--export-name", required=True, help="Name of the export")
    start_parser.add_argument("--source-table", required=True, help="Source table for the export")
    start_parser.add_argument("--destination-uri", required=True, help="Destination URI for the export")

    # Complete export command
    complete_parser = subparsers.add_parser("complete", help="Mark an export as complete")
    complete_parser.add_argument("--export-id", required=True, help="ID of the export to mark as complete")
    complete_parser.add_argument("--rows", type=int, required=True, help="Number of rows exported")

    # Fail export command
    fail_parser = subparsers.add_parser("fail", help="Mark an export as failed")
    fail_parser.add_argument("--export-id", required=True, help="ID of the export to mark as failed")
    fail_parser.add_argument("--error", required=True, help="Error message")

    # Get status command
    status_parser = subparsers.add_parser("status", help="Get status of an export")
    status_parser.add_argument("--export-id", required=True, help="ID of the export to check")

    args = parser.parse_args()

    # Execute the requested command
    if args.command == "show-tables" or args.command is None:
        # Default action is to show table names
        print(f"Export metadata table: {EXPORT_METADATA_TABLE}")
        print(f"Processed hashes table: {EXPORT_PROCESSED_HASHES_TABLE}")
        print("Use SQL scripts to create these tables manually")

    elif args.command == "start":
        export_id = start_export(args.export_name, args.source_table, args.destination_uri)
        print(f"Started export with ID: {export_id}")

    elif args.command == "complete":
        complete_export(args.export_id, args.rows)
        print(f"Marked export {args.export_id} as complete with {args.rows} rows")

    elif args.command == "fail":
        fail_export(args.export_id, args.error)
        print(f"Marked export {args.export_id} as failed")

    elif args.command == "status":
        try:
            status = get_export_status(args.export_id)
            print(f"Export status for {args.export_id}:")
            for key, value in status.items():
                print(f"  {key}: {value}")
        except Exception as e:
            print(f"Error retrieving status: {e}")
