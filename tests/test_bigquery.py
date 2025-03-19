"""Tests for BigQuery operations."""

import datetime
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import bigquery

from src.bigquery import build_export_query, construct_gcs_uri, delete_table, export_table_to_gcs, get_row_count


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    with patch("src.bigquery.client") as mock_client:
        yield mock_client


def test_construct_gcs_uri():
    """Test constructing GCS URI with correct format."""
    # Test with a specific date
    date = datetime.date(2023, 5, 15)
    bucket = "test-bucket"
    export_name = "sales_report"

    # Execute the function
    with patch("src.bigquery.cprint"):  # Silence cprint
        result = construct_gcs_uri(bucket, export_name, date)

    # Verify result
    expected = f"gs://{bucket}/{export_name}/20230515/{export_name}-20230515-*.csv"
    assert result == expected


def test_build_export_query_date_range():
    """Test building query with date range filter."""
    source_table = "project.dataset.sales"
    date_column = "transaction_date"
    days_lookback = 30

    with patch("src.bigquery.cprint"):
        query = build_export_query(source_table=source_table, date_column=date_column, days_lookback=days_lookback)

    # Verify the query has correct structure
    assert f"FROM `{source_table}`" in query
    assert f"{date_column} >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_lookback} DAY)" in query


def test_build_export_query_full():
    """Test building query for full export."""
    source_table = "project.dataset.sales"

    with patch("src.bigquery.cprint"):
        query = build_export_query(source_table=source_table)

    # Verify it's a simple SELECT *
    assert query.strip() == f"SELECT * FROM `{source_table}`"


def test_export_table_to_gcs_full(mock_bigquery_client):
    """Test exporting a full table to GCS without filtering."""
    source_table = "project.dataset.sales"
    gcs_uri = "gs://bucket/path/file-*.csv"

    # Mock the extract job
    mock_extract_job = MagicMock()
    mock_extract_job.result.return_value = None
    mock_bigquery_client.extract_table.return_value = mock_extract_job

    # Mock row count query
    mock_query_job = MagicMock()
    mock_row = MagicMock()
    mock_row.count = 100
    mock_query_job.result.return_value = [mock_row]
    mock_bigquery_client.query.return_value = mock_query_job

    # Mock time.time() to return specific values for timing calculations
    with patch("src.bigquery.cprint"), patch("src.bigquery.time") as mock_time:
        # Setup time.time() to return increasing values - we need 4 values:
        # 1. start_time, 2. extract_start_time, 3. after extract completes, 4. for total_time
        mock_time.time.side_effect = [100.0, 105.0, 110.0, 115.0]

        # Call the function
        result_uri, row_count, temp_table = export_table_to_gcs(
            source_table=source_table, gcs_uri=gcs_uri, compression=True
        )

    # Verify results
    assert result_uri == f"{gcs_uri}.gz"  # Should add .gz with compression
    assert row_count == 100
    assert temp_table == ""  # No temp table for full export

    # Verify extract was called with right params
    mock_bigquery_client.extract_table.assert_called_once()

    # Get call arguments - args are positional args, kwargs are keyword args
    args, kwargs = mock_bigquery_client.extract_table.call_args

    # Check positional arguments
    assert args[0] == source_table  # First arg is source table
    assert args[1] == f"{gcs_uri}.gz"  # Second arg is destination URI

    # Check job_config from kwargs (keyword arguments)
    assert "job_config" in kwargs
    assert kwargs["job_config"].compression == bigquery.Compression.GZIP


def test_export_table_to_gcs_date_range(mock_bigquery_client):
    """Test exporting with date range filter that creates a temp table."""
    source_table = "project.dataset.sales"
    gcs_uri = "gs://bucket/path/file-*.csv"
    date_column = "transaction_date"
    days_lookback = 30

    # Mock query job for temp table creation
    mock_query_job = MagicMock()

    # Mock extract job
    mock_extract_job = MagicMock()

    # Mock row count query
    mock_row_count_job = MagicMock()
    mock_row = MagicMock()
    mock_row.count = 50
    mock_row_count_job.result.return_value = [mock_row]

    # Set up query method to return different results for different calls
    def mock_query_side_effect(query, job_config=None):
        if "COUNT" in query:
            return mock_row_count_job
        return mock_query_job

    mock_bigquery_client.query.side_effect = mock_query_side_effect
    mock_bigquery_client.extract_table.return_value = mock_extract_job

    # Mock time.time() to return specific values for timing calculations
    with patch("src.bigquery.cprint"), patch("src.bigquery.time") as mock_time, patch(
        "src.bigquery.uuid.uuid4", return_value=MagicMock(hex="12345678")
    ):
        # Setup time.time() to return increasing values - we need 4 values for the time calls
        mock_time.time.side_effect = [100.0, 120.0, 130.0, 140.0]

        # Call the function
        result_uri, row_count, temp_table = export_table_to_gcs(
            source_table=source_table,
            gcs_uri=gcs_uri,
            date_column=date_column,
            days_lookback=days_lookback,
            compression=False,
        )

    # Verify results
    assert result_uri == gcs_uri  # No .gz without compression
    assert row_count == 50
    assert temp_table == "temp_export_date_12345678"  # Should create a temp table

    # Verify temp table creation query was executed
    mock_bigquery_client.query.assert_called()

    # Verify extract was called with temp table
    mock_bigquery_client.extract_table.assert_called_once()

    # Get the arguments that extract_table was called with
    args, kwargs = mock_bigquery_client.extract_table.call_args

    # In this case, we expect the temp table ID to be used
    temp_table_id = f"{source_table.split('.')[0]}.{source_table.split('.')[1]}.{temp_table}"
    assert args[0] == temp_table_id


def test_get_row_count(mock_bigquery_client):
    """Test getting row count from a table."""
    table_name = "project.dataset.table"

    # Mock the query result
    mock_query_job = MagicMock()
    mock_row = MagicMock()
    mock_row.count = 150
    mock_query_job.result.return_value = [mock_row]
    mock_bigquery_client.query.return_value = mock_query_job

    with patch("src.bigquery.cprint"):
        result = get_row_count(table_name)

    assert result == 150
    mock_bigquery_client.query.assert_called_once()
    call_args = mock_bigquery_client.query.call_args
    query = call_args[0][0]
    assert f"SELECT COUNT(*) as count FROM `{table_name}`" in query


def test_get_row_count_empty_result(mock_bigquery_client):
    """Test getting row count with empty result."""
    table_name = "project.dataset.table"

    # Mock empty query result
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = []
    mock_bigquery_client.query.return_value = mock_query_job

    with patch("src.bigquery.cprint"):
        result = get_row_count(table_name)

    assert result == 0


def test_delete_table_full_name(mock_bigquery_client):
    """Test deleting table with fully qualified name."""
    table_id = "project.dataset.table"

    with patch("src.bigquery.cprint"):
        result = delete_table(table_id)

    assert result is True
    mock_bigquery_client.delete_table.assert_called_once_with(table_id)


def test_delete_table_with_source(mock_bigquery_client):
    """Test deleting table with just table name and source table."""
    table_name = "temp_table"
    source_table = "project.dataset.source"

    with patch("src.bigquery.cprint"):
        result = delete_table(table_name, source_table)

    assert result is True
    mock_bigquery_client.delete_table.assert_called_once_with("project.dataset.temp_table")


def test_delete_table_error(mock_bigquery_client):
    """Test error handling when deleting table."""
    table_id = "project.dataset.table"

    # Mock an exception
    mock_bigquery_client.delete_table.side_effect = Exception("Delete failed")

    with patch("src.bigquery.cprint"):
        result = delete_table(table_id)

    assert result is False
