"""
Metadata tracking for BigQuery to SFTP exports.
Handles the creation and management of export metadata table.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.config import ConfigError, validate_config
from src.helpers import cprint

# BigQuery client
client = bigquery.Client()


def ensure_metadata_table_exists(table_id: str) -> None:
    """
    Ensure the metadata table exists, create it if it doesn't.

    Args:
        table_id: Fully qualified table ID (project.dataset.table)
    """
    try:
        # Check if table exists
        client.get_table(table_id)
        cprint(f"Metadata table {table_id} exists", severity="INFO")
    except NotFound:
        # Table doesn't exist, create it
        cprint(f"Metadata table {table_id} not found, creating...", severity="INFO")

        # Define table schema
        schema = [
            bigquery.SchemaField("export_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_export_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("last_exported_value", "STRING"),
            bigquery.SchemaField("rows_exported", "INTEGER"),
            bigquery.SchemaField("file_name", "STRING"),
            bigquery.SchemaField("export_status", "STRING"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("completed_at", "TIMESTAMP"),
            bigquery.SchemaField("error_message", "STRING"),
        ]

        # Create the table
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.create_table(table)
            cprint(f"Created metadata table {table_id}", severity="INFO")
        except Exception as e:
            raise ConfigError(f"Failed to create metadata table: {str(e)}")


def get_last_export(table_id: str, export_name: str) -> Dict[str, Any]:
    """
    Get metadata about the last export for a specific export job.

    Args:
        table_id: Fully qualified metadata table ID
        export_name: Name of the export job

    Returns:
        Dict with metadata about the last export, or empty dict if none found
    """
    query = f"""
    SELECT 
      export_name,
      source_table,
      last_export_timestamp,
      last_exported_value,
      rows_exported,
      file_name,
      export_status,
      started_at,
      completed_at,
      error_message
    FROM `{table_id}`
    WHERE export_name = @export_name
    ORDER BY started_at DESC
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("export_name", "STRING", export_name),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = list(query_job)

        if not results:
            return {}

        # Convert result to dict
        row = results[0]
        return {
            "export_name": row.export_name,
            "source_table": row.source_table,
            "last_export_timestamp": row.last_export_timestamp,
            "last_exported_value": row.last_exported_value,
            "rows_exported": row.rows_exported,
            "file_name": row.file_name,
            "export_status": row.export_status,
            "started_at": row.started_at,
            "completed_at": row.completed_at,
            "error_message": row.error_message,
        }
    except Exception as e:
        cprint(f"Error fetching last export: {str(e)}", severity="ERROR")
        return {}


def record_export_start(table_id: str, export_name: str, source_table: str) -> None:
    """
    Record the start of an export job.

    Args:
        table_id: Fully qualified metadata table ID
        export_name: Name of the export job
        source_table: Source table being exported
    """
    now = datetime.now()

    row = {
        "export_name": export_name,
        "source_table": source_table,
        "export_status": "RUNNING",
        "started_at": now,
    }

    errors = client.insert_rows_json(table_id, [row])
    if errors:
        cprint(f"Error recording export start: {errors}", severity="ERROR")
    else:
        cprint(f"Recorded start of export '{export_name}'", severity="INFO")


def record_export_success(
    table_id: str,
    export_name: str,
    source_table: str,
    rows_exported: int,
    file_name: str,
    last_exported_value: Optional[str] = None,
    last_timestamp: Optional[datetime] = None,
) -> None:
    """
    Record a successful export completion.

    Args:
        table_id: Fully qualified metadata table ID
        export_name: Name of the export job
        source_table: Source table that was exported
        rows_exported: Number of rows exported
        file_name: Name of the exported file
        last_exported_value: Last value of the timestamp column exported (for incremental)
        last_timestamp: Timestamp of the last record processed
    """
    now = datetime.now()

    row = {
        "export_name": export_name,
        "source_table": source_table,
        "last_export_timestamp": last_timestamp or now,
        "last_exported_value": last_exported_value,
        "rows_exported": rows_exported,
        "file_name": file_name,
        "export_status": "SUCCESS",
        "started_at": now,  # Simplifying by using now instead of tracking real start time
        "completed_at": now,
    }

    errors = client.insert_rows_json(table_id, [row])
    if errors:
        cprint(f"Error recording export success: {errors}", severity="ERROR")
    else:
        cprint(f"Recorded successful export '{export_name}' with {rows_exported} rows", severity="INFO")


def record_export_failure(table_id: str, export_name: str, source_table: str, error_message: str) -> None:
    """
    Record a failed export.

    Args:
        table_id: Fully qualified metadata table ID
        export_name: Name of the export job
        source_table: Source table that was being exported
        error_message: Error message
    """
    now = datetime.now()

    row = {
        "export_name": export_name,
        "source_table": source_table,
        "export_status": "FAILED",
        "started_at": now,  # Simplifying by using now instead of tracking real start time
        "completed_at": now,
        "error_message": error_message,
    }

    errors = client.insert_rows_json(table_id, [row])
    if errors:
        cprint(f"Error recording export failure: {errors}", severity="ERROR")
    else:
        cprint(f"Recorded failed export '{export_name}': {error_message}", severity="INFO")


def get_incremental_filter(
    metadata: Dict[str, Any],
    timestamp_column: str,
) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
    """
    Generate a WHERE clause filter for incremental exports.

    Args:
        metadata: Metadata from the last export
        timestamp_column: Column to use for incremental exports

    Returns:
        Tuple of (SQL WHERE clause, Query parameters)
    """
    where_clause = ""
    query_params = []

    # If we have a previous export with a last exported value
    if metadata and metadata.get("last_exported_value"):
        where_clause = f"{timestamp_column} > @last_value"
        query_params.append(bigquery.ScalarQueryParameter("last_value", "STRING", metadata["last_exported_value"]))

    return where_clause, query_params

