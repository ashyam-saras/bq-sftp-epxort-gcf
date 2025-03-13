"""
BigQuery operations for exporting data to GCS.
Handles query building, hash-based incremental exports,
and managing the export jobs.
"""

import argparse
import datetime
import uuid
from typing import List, Optional, Tuple

from google.cloud import bigquery

from src.helpers import cprint

# Create a shared BigQuery client
client = bigquery.Client()


def construct_gcs_uri(bucket: str, export_name: str, date: datetime.date) -> str:
    """
    Constructs GCS URI for export files using the defined folder structure.

    Args:
        bucket: GCS bucket name without gs:// prefix
        export_name: Name of the export for folder organization
        date: Export date for daily partitioning

    Returns:
        str: GCS URI prefix for export files
    """
    date_str = date.strftime(r"%Y%m%d")
    return f"gs://{bucket}/{export_name}/{date_str}/{export_name}-{date_str}-*.csv"


def build_export_query(
    source_table: str,
    hash_columns: Optional[List[str]] = None,
    processed_hashes_table: Optional[str] = None,
) -> str:
    """
    Builds SQL query for data export, handling incremental exports with hash-based filtering.

    Args:
        source_table: Fully qualified source table (project.dataset.table)
        hash_columns: List of column names to use for row hashing (for incremental)
        processed_hashes_table: Table containing already processed hashes

    Returns:
        str: SQL query for selecting data to export

    Examples:
        For a full export:
        ```sql
        SELECT * FROM `project.dataset.customers`
        ```

        For an incremental export with hash-based filtering:
        ```sql
        SELECT t.*, TO_HEX(MD5(CONCAT(CAST(customer_id AS STRING), ' | ', CAST(last_updated AS STRING)))) AS row_hash
        FROM `project.dataset.customers` t
        WHERE TO_HEX(MD5(CONCAT(CAST(customer_id AS STRING), ' | ', CAST(last_updated AS STRING)))) NOT IN (
          SELECT row_hash FROM `project.dataset.processed_hashes`
        )
        ```
    """
    if not hash_columns or not processed_hashes_table:
        # Full export - select all data
        return f"SELECT * FROM `{source_table}`"

    # Build the hash expression from specified columns
    hash_columns_concat = ", ' | ', ".join([f"CAST({col} AS STRING)" for col in hash_columns])
    hash_expression = f"TO_HEX(MD5(CONCAT({hash_columns_concat})))"

    # Build incremental query that filters out already processed hashes
    query = f"""
    SELECT t.*, {hash_expression} AS row_hash 
    FROM `{source_table}` t
    WHERE {hash_expression} NOT IN (
      SELECT row_hash FROM `{processed_hashes_table}`
    )
    """

    return query


def export_table_to_gcs(
    source_table: str,
    gcs_uri: str,
    hash_columns: Optional[List[str]] = None,
    processed_hashes_table: Optional[str] = None,
    compression: bool = True,
) -> Tuple[str, int, str]:
    """
    Exports BigQuery data to GCS with optional compression.

    Args:
        source_table: Fully qualified source table (project.dataset.table)
        gcs_uri: GCS URI prefix to write files
        hash_columns: List of column names to use for row hashing (for incremental)
        processed_hashes_table: Table containing already processed hashes
        compression: Whether to compress the output files (GZIP)

    Returns:
        Tuple[str, int, str]: (destination_uri, row_count, temp_table_name or empty string)
    """
    if hash_columns and processed_hashes_table:
        # For incremental exports, create a temporary table with unprocessed rows
        temp_table_name = f"temp_export_{uuid.uuid4().hex[:8]}"
        temp_table_id = f"{source_table.split('.')[0]}.{source_table.split('.')[1]}.{temp_table_name}"

        query = build_export_query(source_table, hash_columns, processed_hashes_table)

        cprint(f"Creating temporary table {temp_table_id} for incremental export", query=query)

        # Execute query to create temp table
        job_config = bigquery.QueryJobConfig(destination=temp_table_id)
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for query to complete

        # Use the temp table as our source
        source_to_extract = temp_table_id
    else:
        # For full exports, use the source table directly
        source_to_extract = source_table
        temp_table_name = ""

    # Configure the extract job
    job_config = bigquery.ExtractJobConfig()
    job_config.print_header = True

    if compression:
        cprint(f"Exporting {source_to_extract} to {gcs_uri} with GZIP compression")
        job_config.compression = bigquery.Compression.GZIP
        if not gcs_uri.endswith(".gz"):
            gcs_uri = f"{gcs_uri}.gz"
    else:
        cprint(f"Exporting {source_to_extract} to {gcs_uri} without compression")

    # Start the extract job
    extract_job = client.extract_table(source_to_extract, gcs_uri, job_config=job_config)

    # Wait for job to complete
    extract_job.result()

    # Get row count
    row_count = get_row_count(source_to_extract)

    return gcs_uri, row_count, temp_table_name


def get_row_count(table_name: str) -> int:
    """
    Gets the row count of a BigQuery table.

    Args:
        table_name: Fully qualified table name (project.dataset.table)

    Returns:
        int: Number of rows in the table
    """
    query = f"SELECT COUNT(*) as count FROM `{table_name}`"
    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        return row.count

    return 0


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Export BigQuery data to GCS")
    parser.add_argument("--source", required=True, help="Source table (project.dataset.table)")
    parser.add_argument("--bucket", required=True, help="GCS bucket name without gs:// prefix")
    parser.add_argument("--export-name", required=True, help="Export name for file organization")
    parser.add_argument("--hash-columns", help="Comma-separated list of columns for hashing")
    parser.add_argument("--processed-hashes-table", help="Table with already processed hashes")
    parser.add_argument("--no-compression", action="store_true", help="Disable GZIP compression")
    args = parser.parse_args()

    # Process arguments
    hash_columns = args.hash_columns.split(",") if args.hash_columns else None
    compression = not args.no_compression

    # Run the export
    today = datetime.datetime.now().date()
    gcs_uri = construct_gcs_uri(args.bucket, args.export_name, today)

    cprint(
        f"Starting BigQuery export test",
        source_table=args.source,
        gcs_uri=gcs_uri,
        hash_columns=hash_columns,
        processed_hashes_table=args.processed_hashes_table,
        compression=compression,
    )

    destination_uri, row_count, temp_table = export_table_to_gcs(
        source_table=args.source,
        gcs_uri=gcs_uri,
        hash_columns=hash_columns,
        processed_hashes_table=args.processed_hashes_table,
        compression=compression,
    )

    cprint(
        f"Export complete!",
        destination_uri=destination_uri,
        row_count=row_count,
        temp_table_created=bool(temp_table),
    )
