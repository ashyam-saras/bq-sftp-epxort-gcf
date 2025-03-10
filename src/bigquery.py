"""
BigQuery operations for the SFTP export function.
Handles data extraction and query generation.
"""

import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery, storage

from src.config import ConfigError
from src.helpers import cprint

# Clients
bq_client = bigquery.Client()
storage_client = storage.Client()


def extract_data_to_gcs(
    export_config: Dict[str, Any],
    metadata_entry: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, int, Optional[str]]:
    """
    Extract data from BigQuery to GCS based on configuration.

    Args:
        export_config: Export configuration dictionary
        metadata_entry: Previous export metadata (for incremental exports)

    Returns:
        Tuple of (gcs_uri, filename, row_count, last_exported_value)
    """
    source_table = export_config["source_table"]
    incremental = export_config.get("incremental", False)
    bucket_name = os.environ.get("GCS_BUCKET")

    # Get field delimiter from config or default to comma
    field_delimiter = export_config.get("field_delimiter", ",")

    if not bucket_name:
        raise ValueError("GCS_BUCKET environment variable not set")

    # Generate a unique filename base in GCS
    timestamp = datetime.now().strftime(r"%Y%m%d")
    unique_id = str(uuid.uuid4())[:8]
    gcs_filename_base = f"{export_config['name']}_{timestamp}_{unique_id}"

    # Use wildcard pattern for large datasets
    gcs_uri = f"gs://{bucket_name}/{gcs_filename_base}-*.csv.gz"

    cprint(
        f"Extracting data from {source_table} to {gcs_uri}",
        export_name=export_config["name"],
        incremental=incremental,
        delimiter=field_delimiter,
    )

    # Build query
    query, query_params = build_query(export_config, metadata_entry)

    # Step 1: Execute query to get results in a temporary table
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = bq_client.query(query, job_config=job_config)

    # Wait for query to complete
    results = query_job.result()

    # Get row count
    row_count = results.total_rows

    # Step 2: Extract query results to GCS
    extract_job_config = bigquery.ExtractJobConfig(
        # Configure CSV options using the configured delimiter
        field_delimiter=field_delimiter,
        print_header=True,
        destination_format=bigquery.DestinationFormat.CSV,
        # Add compression to reduce the number of sharded files
        compression=bigquery.Compression.GZIP,
    )

    extract_job = bq_client.extract_table(query_job.destination, gcs_uri, job_config=extract_job_config)

    # Wait for extract job to complete
    extract_job.result()

    # Find all the generated files
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"{gcs_filename_base}-"))

    # Single filename for metadata (use the pattern if multiple files)
    result_filename = f"{gcs_filename_base}.csv.gz"
    if len(blobs) > 1:
        cprint(f"Large dataset was exported as {len(blobs)} sharded files")
        result_filename = f"{gcs_filename_base}-*.csv.gz"  # Pattern representing all files

    # Get the last exported timestamp value (for incremental exports)
    last_exported_value = None
    if incremental and row_count > 0:
        timestamp_column = export_config["timestamp_column"]
        # Get the maximum timestamp from the results
        last_value_query = f"""
        SELECT MAX({timestamp_column}) as max_ts
        FROM ({query})
        """
        last_value_job = bq_client.query(last_value_query)
        for row in last_value_job:
            if row.max_ts:
                last_exported_value = row.max_ts.isoformat()

    cprint(
        f"Extracted {row_count} rows from {source_table} to {len(blobs)} file(s)",
        row_count=row_count,
        export_name=export_config["name"],
        files_count=len(blobs),
    )

    # Return the base URI pattern and filename pattern
    base_uri = f"gs://{bucket_name}/{gcs_filename_base}"
    return base_uri, result_filename, int(row_count), last_exported_value


def build_query(
    export_config: Dict[str, Any],
    metadata_table: str,
) -> Tuple[str, List[bigquery.ScalarQueryParameter]]:
    """
    Build a BigQuery query based on export configuration.

    Args:
        export_config: Export configuration dictionary
        metadata_table: Metadata table from config

    Returns:
        Tuple of (query_string, query_parameters)
    """
    source_table = export_config["source_table"]
    columns = export_config.get("columns", ["*"])
    incremental = export_config.get("incremental", False)
    incremental_type = export_config.get("incremental_type", "hash")
    export_name = export_config["name"]

    # Get hash table name
    hash_table = f"{metadata_table}_hashes"

    # Format column selection
    if columns and columns != ["*"]:
        columns_str = ", ".join(columns)
    else:
        columns_str = "*"

    query_params = []

    # For hash-based incremental, use an anti-join pattern
    if incremental and incremental_type == "hash":
        hash_columns = export_config.get("hash_columns", [])
        if not hash_columns:
            raise ConfigError("hash_columns must be specified for hash-based incremental exports")

        # Add hash calculation to SELECT clause
        hash_concat = ", ".join([f"CAST(t.{col} AS STRING)" for col in hash_columns])
        select_clause = f"t.{columns_str.replace('*', '*')}, TO_HEX(MD5(CONCAT({hash_concat}))) as row_hash"

        # Create query with anti-join to hash table
        query = f"""
        SELECT {select_clause}
        FROM `{source_table}` t
        LEFT JOIN `{hash_table}` h
          ON TO_HEX(MD5(CONCAT({hash_concat}))) = h.row_hash
          AND h.export_name = @export_name
        WHERE h.row_hash IS NULL
        """

        # Add parameter for export name
        query_params.append(bigquery.ScalarQueryParameter("export_name", "STRING", export_name))

    else:
        # Full export - just select all rows
        query = f"SELECT {columns_str} FROM `{source_table}`"

        # Add any custom filters if specified
        if export_config.get("filter"):
            query += f" WHERE {export_config['filter']}"

    return query, query_params


def delete_gcs_file(gcs_uri: str) -> None:
    """
    Delete a file from GCS after processing.

    Args:
        gcs_uri: GCS URI of the file to delete
    """
    try:
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        cprint(f"Deleted temporary file from GCS: {gcs_uri}")
    except Exception as e:
        cprint(f"Failed to delete GCS file {gcs_uri}: {str(e)}", severity="WARNING")


def delete_gcs_files(gcs_uri_base: str) -> None:
    """
    Delete file(s) from GCS after processing.

    Args:
        gcs_uri_base: Base GCS URI (can include wildcard) of the file(s) to delete
    """
    try:
        # If we have a wildcard URI, extract just the prefix
        if "*" in gcs_uri_base:
            gcs_uri_base = gcs_uri_base.split("*")[0]

        bucket_name, blob_prefix = parse_gcs_uri(gcs_uri_base)
        bucket = storage_client.bucket(bucket_name)

        # List all blobs with this prefix
        blobs = list(bucket.list_blobs(prefix=blob_prefix))

        if not blobs:
            cprint(f"No files found to delete with prefix: gs://{bucket_name}/{blob_prefix}", severity="WARNING")
            return

        # Delete each blob
        for blob in blobs:
            blob.delete()
            cprint(f"Deleted file from GCS: gs://{bucket_name}/{blob.name}")

        cprint(f"Deleted {len(blobs)} file(s) from GCS")

    except Exception as e:
        cprint(f"Failed to delete GCS files with prefix {gcs_uri_base}: {str(e)}", severity="WARNING")


def parse_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
    """
    Parse a GCS URI into bucket name and blob name.

    Args:
        gcs_uri: GCS URI (e.g., gs://bucket-name/path/to/file.csv)

    Returns:
        Tuple of (bucket_name, blob_name)
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI format: {gcs_uri}")

    # Remove gs:// prefix
    path = gcs_uri[5:]

    # Split into bucket and blob path
    parts = path.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid GCS URI format: {gcs_uri}")

    bucket_name, blob_name = parts
    return bucket_name, blob_name


if __name__ == "__main__":
    import json
    from pathlib import Path

    from dotenv import load_dotenv

    # Load environment variables from .env file
    load_dotenv()

    # Check if required environment variables are set
    if not os.environ.get("GCS_BUCKET"):
        print("❌ Error: GCS_BUCKET environment variable is not set in .env file")
        exit(1)

    # Load the config file
    config_path = Path(__file__).parent.parent / "configs" / "default.json"
    print(f"Loading config from: {config_path}")

    with open(config_path, "r") as f:
        config = json.load(f)

    # Get the specific export config
    export_config = config["exports"][0]

    print(f"Testing export: {export_config['name']}")
    print(f"Source table: {export_config['source_table']}")
    print(f"Using GCS bucket: {os.environ.get('GCS_BUCKET')}")

    # Run the extraction
    try:
        gcs_uri, filename, row_count, last_exported_value = extract_data_to_gcs(
            export_config=export_config,
            metadata_entry=None,  # No previous metadata for testing
        )

        print("\n✅ Extraction completed successfully!")
        print(f"GCS URI: {gcs_uri}")
        print(f"Filename: {filename}")
        print(f"Rows exported: {row_count}")
        if last_exported_value:
            print(f"Last exported value: {last_exported_value}")

        # Optionally, clean up the temporary file
        if input("Delete temporary GCS file(s)? (y/n): ").lower() == "y":
            delete_gcs_files(gcs_uri)
            print(f"Deleted temporary file(s) from GCS")

    except Exception as e:
        print(f"\n❌ Extraction failed: {str(e)}")
        import traceback

        traceback.print_exc()
