"""
BigQuery operations for exporting data to GCS.
Handles query building, hash-based incremental exports,
and managing the export jobs.
"""

import argparse
import datetime
import time
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
    uri = f"gs://{bucket}/{export_name}/{date_str}/{export_name}-{date_str}-*.csv"
    cprint(f"Constructed GCS URI: {uri}")
    return uri


def build_export_query(
    source_table: str,
    hash_columns: Optional[List[str]] = None,
    processed_hashes_table: Optional[str] = None,
    date_column: Optional[str] = None,
    days_lookback: Optional[int] = None,
) -> str:
    """
    Builds SQL query for data export, handling incremental exports with hash-based filtering
    or date-range based filtering.

    Args:
        source_table: Fully qualified source table (project.dataset.table)
        hash_columns: List of column names to use for row hashing (for incremental)
        processed_hashes_table: Table containing already processed hashes
        date_column: Column name to use for date filtering (for date range exports)
        days_lookback: Number of days to look back for date range exports

    Returns:
        str: SQL query for selecting data to export
    """
    # Option 1: Date range filter
    if date_column and days_lookback is not None:
        query = f"""
        SELECT * FROM `{source_table}`
        WHERE {date_column} >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_lookback} DAY)
        """
        cprint(f"Building date range query with lookback of {days_lookback} days on column {date_column}")
        return query

    # Option 2: Hash-based incremental export
    if hash_columns and processed_hashes_table:
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
        cprint(f"Building incremental hash-based query with {len(hash_columns)} columns")
        return query

    # Option 3: Full export - select all data
    cprint(f"Building full export query for {source_table}")
    return f"SELECT * FROM `{source_table}`"


def export_table_to_gcs(
    source_table: str,
    gcs_uri: str,
    hash_columns: Optional[List[str]] = None,
    processed_hashes_table: Optional[str] = None,
    date_column: Optional[str] = None,
    days_lookback: Optional[int] = None,
    compression: bool = True,
) -> Tuple[str, int, str]:
    """
    Exports BigQuery data to GCS with optional compression.

    Args:
        source_table: Fully qualified source table (project.dataset.table)
        gcs_uri: GCS URI prefix to write files
        hash_columns: List of column names to use for row hashing (for incremental)
        processed_hashes_table: Table containing already processed hashes
        date_column: Column name to use for date filtering
        days_lookback: Number of days to look back
        compression: Whether to compress the output files (GZIP)

    Returns:
        Tuple[str, int, str]: (destination_uri, row_count, temp_table_name or empty string)
    """
    start_time = time.time()
    temp_table_name = ""
    cprint(f"Starting export process for {source_table}", severity="INFO")

    # For hash-based incremental exports, create a temporary table
    if hash_columns and processed_hashes_table:
        temp_table_name = f"temp_export_{uuid.uuid4().hex[:8]}"
        temp_table_id = f"{source_table.split('.')[0]}.{source_table.split('.')[1]}.{temp_table_name}"

        query = build_export_query(
            source_table=source_table,
            hash_columns=hash_columns,
            processed_hashes_table=processed_hashes_table,
        )
        source_to_extract = temp_table_id
        cprint(f"Using incremental export with temp table {temp_table_id}")

    # For date range exports, create a temporary table
    elif date_column and days_lookback is not None:
        temp_table_name = f"temp_export_date_{uuid.uuid4().hex[:8]}"
        temp_table_id = f"{source_table.split('.')[0]}.{source_table.split('.')[1]}.{temp_table_name}"

        query = build_export_query(source_table=source_table, date_column=date_column, days_lookback=days_lookback)
        source_to_extract = temp_table_id
        cprint(f"Using date range export with temp table {temp_table_id}")

    # For full exports, use the source table directly
    else:
        source_to_extract = source_table
        cprint(f"Using full export directly from source table")

    # If we need to create a temp table
    if temp_table_name:
        cprint(f"Creating temporary table {temp_table_id} for filtered export", severity="INFO", query=query)

        # Execute query to create temp table
        job_config = bigquery.QueryJobConfig(destination=temp_table_id)
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for query to complete

    # Configure the extract job
    job_config = bigquery.ExtractJobConfig()
    job_config.print_header = True

    if compression:
        cprint(f"Exporting {source_to_extract} to {gcs_uri} with GZIP compression", severity="INFO")
        job_config.compression = bigquery.Compression.GZIP
        if not gcs_uri.endswith(".gz"):
            gcs_uri = f"{gcs_uri}.gz"
    else:
        cprint(f"Exporting {source_to_extract} to {gcs_uri} without compression", severity="INFO")

    # Start the extract job
    extract_start_time = time.time()
    extract_job = client.extract_table(source_to_extract, gcs_uri, job_config=job_config)

    # Wait for job to complete
    result = extract_job.result()
    extract_time = time.time() - extract_start_time
    cprint(f"Extract job completed in {extract_time:.2f} seconds", severity="INFO")

    # Get row count
    row_count = get_row_count(source_to_extract)

    total_time = time.time() - start_time
    cprint(
        f"Export process complete for {source_table} in {total_time:.2f} seconds",
        severity="INFO",
        rows=row_count,
        destination=gcs_uri,
        total_time_seconds=f"{total_time:.2f}",
        temp_table=temp_table_name if temp_table_name else "none",
    )

    return gcs_uri, row_count, temp_table_name


def get_row_count(table_name: str) -> int:
    """
    Gets the row count of a BigQuery table.

    Args:
        table_name: Fully qualified table name (project.dataset.table)

    Returns:
        int: Number of rows in the table
    """
    cprint(f"Getting row count for {table_name}")
    query = f"SELECT COUNT(*) as count FROM `{table_name}`"
    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        count = row.count
        cprint(f"Row count for {table_name}: {count:,}")
        return count

    cprint(f"No results found when getting row count for {table_name}", severity="WARNING")
    return 0


def delete_table(table_name: str, source_table: str = None) -> bool:
    """
    Deletes a BigQuery table, especially useful for cleaning up temporary tables.

    Args:
        table_name: Name of the table to delete, can be just the name or fully qualified
        source_table: Optional source table to derive project and dataset if not provided in table_name

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        # Handle case where we only have the table name without project and dataset
        if "." not in table_name and source_table:
            # Extract project and dataset from source_table
            parts = source_table.split(".")
            if len(parts) >= 2:
                table_id = f"{parts[0]}.{parts[1]}.{table_name}"
            else:
                table_id = table_name
        else:
            table_id = table_name

        cprint(f"Deleting table {table_id}", severity="INFO")
        client.delete_table(table_id)
        cprint(f"Table {table_id} deleted successfully", severity="INFO")
        return True

    except Exception as e:
        cprint(f"Failed to delete table {table_name}: {str(e)}", severity="WARNING", error=str(e))
        return False


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Export BigQuery data to GCS")
    parser.add_argument("--source", required=True, help="Source table (project.dataset.table)")
    parser.add_argument("--bucket", required=True, help="GCS bucket name without gs:// prefix")
    parser.add_argument("--export-name", required=True, help="Export name for file organization")
    parser.add_argument("--hash-columns", help="Comma-separated list of columns for hashing")
    parser.add_argument("--processed-hashes-table", help="Table with already processed hashes")
    parser.add_argument("--date-column", help="Column name to use for date filtering")
    parser.add_argument("--days-lookback", type=int, help="Number of days to look back for date filtering")
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
        date_column=args.date_column,
        days_lookback=args.days_lookback,
        compression=compression,
    )

    destination_uri, row_count, temp_table = export_table_to_gcs(
        source_table=args.source,
        gcs_uri=gcs_uri,
        hash_columns=hash_columns,
        processed_hashes_table=args.processed_hashes_table,
        date_column=args.date_column,
        days_lookback=args.days_lookback,
        compression=compression,
    )

    cprint(
        f"Export complete!",
        destination_uri=destination_uri,
        row_count=row_count,
        temp_table_created=bool(temp_table),
    )
