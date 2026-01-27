"""
Airflow DAG for BigQuery to SFTP exports using TaskFlow API.

This DAG:
1. Exports data from BigQuery to GCS (using EXPORT DATA)
2. Triggers Cloud Run to transfer files from GCS to SFTP
3. Verifies that GCS and SFTP are in sync

All exports run in parallel using dynamic task mapping.

Supported placeholders in queries:
- {ds}                    - Data interval start date (YYYY-MM-DD)
- {ds_nodash}             - Data interval start date (YYYYMMDD)
- {data_interval_start}   - Data interval start (YYYY-MM-DD HH:MM:SS)
- {data_interval_end}     - Data interval end (YYYY-MM-DD HH:MM:SS)

Note: Use BigQuery date functions like DATE_SUB('{ds}', INTERVAL 7 DAY) for lookback queries.

Manual Runs with Query Override (Trigger DAG with config):
Each export has a {export_name}_query param pre-filled with the config query.
- Modify the query as needed for one-time runs
- Set to empty string to skip that export entirely

Example: To backfill only account_overview_sales from Jan 1st, modify its query
and set the other exports to empty string.

Backfilling:
- CLI: airflow dags backfill sftp_export -s 2025-01-01 -e 2025-01-07
- UI: Trigger DAG with specific execution date
- Failed tasks: Clear task in UI to re-run
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# =============================================================================
# Configuration
# =============================================================================


def get_config() -> dict[str, Any]:
    """Load export configuration from Airflow Variable or default file."""
    try:
        config_json = Variable.get("SFTP_EXPORT_CONFIG", default_var=None)
        if config_json:
            return json.loads(config_json)
    except Exception:
        pass

    # Fallback: load from file (for local development)
    config_path = os.environ.get("SFTP_EXPORT_CONFIG_PATH", "/opt/airflow/configs/exports.json")
    if os.path.exists(config_path):
        with open(config_path) as f:
            return json.load(f)

    raise ValueError("No configuration found. Set 'SFTP_EXPORT_CONFIG' Airflow Variable.")


# =============================================================================
# Slack Notification
# =============================================================================


def send_slack_alert(context: dict[str, Any]) -> None:
    """Send Slack notification on task failure."""
    try:
        webhook_url = Variable.get("slack_webhook_url", default_var=None)
        if not webhook_url:
            print("No slack_webhook_url configured, skipping notification")
            return

        ti = context.get("task_instance")
        dag_id = context.get("dag").dag_id
        task_id = ti.task_id if ti else "unknown"
        data_interval = context.get("data_interval_start", datetime.now())
        exception = context.get("exception", "Unknown error")
        log_url = ti.log_url if ti else ""

        message = {
            "text": "🚨 *SFTP Export Failed*",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": "🚨 SFTP Export Failed"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                        {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                        {"type": "mrkdwn", "text": f"*Data Interval:*\n{data_interval}"},
                        {"type": "mrkdwn", "text": f"*Error:*\n```{str(exception)[:200]}```"},
                    ],
                },
                (
                    {
                        "type": "actions",
                        "elements": [
                            {"type": "button", "text": {"type": "plain_text", "text": "View Logs"}, "url": log_url}
                        ],
                    }
                    if log_url
                    else {"type": "divider"}
                ),
            ],
        }

        requests.post(webhook_url, json=message, timeout=10)
        print("Slack notification sent")
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")


# =============================================================================
# Helper Functions
# =============================================================================


def resolve_placeholders(
    query: str,
    ds: str,
    ds_nodash: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
) -> str:
    """Replace placeholders in query with actual values."""
    return (
        query.replace("{ds}", ds)
        .replace("{ds_nodash}", ds_nodash)
        .replace("{date}", ds_nodash)
        .replace("{date_dash}", ds)
        .replace("{data_interval_start}", data_interval_start.strftime(r"%Y-%m-%d %H:%M:%S"))
        .replace("{data_interval_end}", data_interval_end.strftime(r"%Y-%m-%d %H:%M:%S"))
    )


def get_file_extension(format: str, compression: str) -> str:
    """Get file extension based on format and compression."""
    ext = format.lower()
    if compression.upper() == "GZIP":
        ext += ".gz"
    elif compression.upper() == "SNAPPY":
        ext += ".snappy"
    return ext


def build_export_query(
    query: str,
    gcs_bucket: str,
    export_name: str,
    ds_nodash: str,
    format: str,
    compression: str,
) -> str:
    """Build BigQuery EXPORT DATA statement."""
    extension = get_file_extension(format, compression)
    gcs_uri = f"gs://{gcs_bucket}/{export_name}/{ds_nodash}/{export_name}-*.{extension}"

    return f"""EXPORT DATA OPTIONS(
    uri='{gcs_uri}',
    format='{format}',
    compression='{compression}',
    overwrite=true,
    header=true,
    field_delimiter='|'
) AS
{query}"""


# =============================================================================
# DAG Definition
# =============================================================================


def build_dag_params(exports: dict) -> dict:
    """Build dynamic params for each export query."""
    from airflow.models.param import Param

    params = {}
    for export_name, export_config in exports.items():
        default_query = export_config.get("query", "")
        params[f"{export_name}_query"] = Param(
            default=default_query,
            type="string",
            description=f"Query for {export_name}. Set to empty string to skip this export.",
        )
    return params


# Load config at parse time for params
_config = get_config()
_exports = _config.get("exports", {})


@dag(
    dag_id="boxout_sftp_export",
    description="Export BigQuery data to SFTP via GCS",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    max_active_runs=3,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": send_slack_alert,
    },
    tags=["sftp", "export", "bigquery"],
    doc_md=__doc__,
    params=build_dag_params(_exports),
)
def sftp_export():
    """Main DAG definition using TaskFlow API."""

    config = get_config()
    gcs_bucket = config["gcs_bucket"]
    cloud_run_url = config["cloud_run_url"]
    exports = config.get("exports", {})

    @task_group(group_id="export")
    def export_task_group(export_name: str, export_config: dict):
        """Task group for a single export: BQ → GCS → SFTP → Verify."""

        @task
        def bq_export(
            export_name: str,
            export_config: dict,
            gcs_bucket: str,
            **context,
        ) -> dict | None:
            """Export data from BigQuery to GCS."""
            # Get runtime context
            ds = context["ds"]
            ds_nodash = context["ds_nodash"]
            data_interval_start = context["data_interval_start"]
            data_interval_end = context["data_interval_end"]
            params = context.get("params", {})

            # Check for query in params
            param_key = f"{export_name}_query"
            query_param = params.get(param_key, export_config["query"])

            # If param is empty string, skip this export
            if query_param == "":
                print(f"=== Skipping Export: {export_name} (empty query param) ===")
                return None

            base_query = query_param
            print(f"=== Export: {export_name} ===")

            print(f"ds (data interval start date): {ds}")
            print(f"data_interval_start: {data_interval_start}")
            print(f"data_interval_end: {data_interval_end}")

            # Resolve placeholders in query
            resolved_query = resolve_placeholders(
                query=base_query,
                ds=ds,
                ds_nodash=ds_nodash,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
            )

            # Build EXPORT DATA statement
            export_sql = build_export_query(
                query=resolved_query,
                gcs_bucket=gcs_bucket,
                export_name=export_name,
                ds_nodash=ds_nodash,
                format=export_config.get("format", "CSV"),
                compression=export_config.get("compression", "GZIP"),
            )

            print(f"Resolved query (with placeholders replaced):\n{resolved_query}")
            print(f"Full EXPORT DATA statement:\n{export_sql}")

            # Execute using BigQuery hook
            hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
            hook.run_query(sql=export_sql, use_legacy_sql=False)

            gcs_path = f"gs://{gcs_bucket}/{export_name}/{ds_nodash}/"
            print(f"Export complete. Files at: {gcs_path}")

            return {
                "export_name": export_name,
                "gcs_path": gcs_path,
                "ds": ds,
                "ds_nodash": ds_nodash,
            }

        @task(execution_timeout=timedelta(minutes=45))
        def transfer_to_sftp(export_result: dict | None, cloud_run_url: str) -> dict | None:
            """Trigger Cloud Run to transfer files from GCS to SFTP."""
            if export_result is None:
                print("Skipping transfer (export was skipped)")
                return None

            payload = {
                "export_name": export_result["export_name"],
                "gcs_path": export_result["gcs_path"],
                "date": export_result["ds"],
            }

            print(f"Calling Cloud Run: {cloud_run_url}/transfer")
            print(f"Payload: {json.dumps(payload)}")

            response = requests.post(
                f"{cloud_run_url}/transfer",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=1800,  # 30 minutes - must be less than Cloud Run timeout
            )
            response.raise_for_status()
            result = response.json()

            if result.get("status") != "success":
                raise Exception(f"Transfer failed: {result.get('message', 'Unknown error')}")

            print(f"Transfer complete: {result.get('files_transferred')} files")
            return {**export_result, "transfer_result": result}

        @task(execution_timeout=timedelta(minutes=10))
        def verify_sync(transfer_result: dict | None, cloud_run_url: str) -> dict | None:
            """Verify GCS and SFTP are in sync."""
            if transfer_result is None:
                print("Skipping verification (export was skipped)")
                return None

            payload = {
                "export_name": transfer_result["export_name"],
                "gcs_path": transfer_result["gcs_path"],
            }

            print(f"Calling Cloud Run: {cloud_run_url}/verify")

            response = requests.post(
                f"{cloud_run_url}/verify",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=300,  # 5 minutes
            )
            response.raise_for_status()
            result = response.json()

            if not result.get("in_sync"):
                missing = result.get("missing_on_sftp", [])
                raise Exception(f"Sync verification failed. Missing files: {missing}")

            print(f"✅ Verification passed: {result.get('gcs_file_count')} files in sync")
            return result

        # Chain tasks within group
        export_result = bq_export(export_name, export_config, gcs_bucket)
        transfer_result = transfer_to_sftp(export_result, cloud_run_url)
        verify_sync(transfer_result, cloud_run_url)

    # Create task groups for each export (run in parallel)
    for export_name, export_config in exports.items():
        export_task_group.override(group_id=export_name)(export_name, export_config)


# Instantiate the DAG
sftp_export()
