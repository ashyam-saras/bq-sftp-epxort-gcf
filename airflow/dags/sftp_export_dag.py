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

Note: GCS folder is named by data_interval_end date (YYYYMMDD) to match query filter dates.
Use BigQuery date functions like DATE_SUB('{ds}', INTERVAL 7 DAY) for lookback queries.

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

Retries:
- All tasks retry once after 5 minutes.
- transfer_to_sftp retries 5 times with exponential backoff (~2, 4, 8, 16, 32
  minutes, jittered) because the GCS -> SFTP hop is the flakiest step.

Alerting:
- Task failures post to Slack via the shared `slack_default` bot connection.
- Channel comes from the `boxout_sftp_slack_channel` Variable
  (default: #prod-pulse-job-alerts). Change it without redeploying:
      airflow variables set boxout_sftp_slack_channel '#some-other-channel'
- Alerts fire only after retries are exhausted, so one message == one dead task.
  Every export group alerts independently, so a systemic outage produces one
  message per export.
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


# Uses the shared `slack_default` bot-token connection, same as bq_quota_monitor
# and data-latency-alerts. Channel is a Variable so it can be repointed without
# redeploying the DAG.
SLACK_CONN_ID = "slack_default"
SLACK_CHANNEL_VAR = "boxout_sftp_slack_channel"
DEFAULT_SLACK_CHANNEL = "#prod-pulse-job-alerts"


def _describe_exception(exception: Any) -> str:
    """Render the task exception as a short, readable line for Slack."""
    if exception is None:
        return "Unknown error (no exception in callback context)"

    text = str(exception).strip() or exception.__class__.__name__
    if len(text) > 800:
        text = text[:800] + " …(truncated)"
    return f"{type(exception).__name__}: {text}"


def _format_duration(seconds: float | None) -> str:
    """Render a task duration as e.g. '5m 17s'."""
    if not seconds:
        return "unknown"
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m {secs}s" if minutes else f"{secs}s"


def send_slack_alert(context: dict[str, Any]) -> None:
    """
    Post a task-failure alert to Slack.

    Airflow fires on_failure_callback only once retries are exhausted, so every
    message here represents a genuinely dead task, not a transient blip.
    """
    try:
        from airflow.providers.slack.hooks.slack import SlackHook

        ti = context["task_instance"]
        channel = Variable.get(SLACK_CHANNEL_VAR, default_var=DEFAULT_SLACK_CHANNEL)

        # Inside a task group the task_id is "<export_name>.<step>",
        # e.g. "account_overview_sales.transfer_to_sftp".
        export_name, _, step = ti.task_id.rpartition(".")
        export_name = export_name or "(ungrouped)"
        step = step or ti.task_id

        dag_id = context["dag"].dag_id
        dag_run = context.get("dag_run")
        run_id = dag_run.run_id if dag_run else "unknown"
        interval_end = context.get("data_interval_end")
        folder_date = interval_end.strftime("%Y%m%d") if interval_end else "unknown"
        attempts = (ti.max_tries or 0) + 1
        error_text = _describe_exception(context.get("exception"))

        blocks: list[dict[str, Any]] = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🚨 SFTP export failed: {export_name}", "emoji": True},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Export*\n`{export_name}`"},
                    {"type": "mrkdwn", "text": f"*Step*\n`{step}`"},
                    {"type": "mrkdwn", "text": f"*Data date*\n{folder_date}"},
                    {"type": "mrkdwn", "text": f"*Duration*\n{_format_duration(ti.duration)}"},
                    {"type": "mrkdwn", "text": f"*Attempts*\n{attempts} (all failed)"},
                    {"type": "mrkdwn", "text": f"*DAG*\n`{dag_id}`"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Error*\n```{error_text}```"}},
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Run `{run_id}` • data interval ending {interval_end}"}],
            },
        ]

        log_url = getattr(ti, "log_url", "")
        if log_url:
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "View logs", "emoji": True},
                            "url": log_url,
                            "style": "danger",
                        }
                    ],
                }
            )

        hook = SlackHook(slack_conn_id=SLACK_CONN_ID)
        response = hook.call(
            api_method="chat.postMessage",
            json={
                "channel": channel,
                "blocks": blocks,
                # Fallback text drives the mobile/desktop notification preview.
                "text": f"🚨 SFTP export failed: {export_name}.{step} ({folder_date}) after {attempts} attempts",
            },
        )

        if response.get("ok"):
            print(f"Slack alert sent to {channel}")
        else:
            print(f"Slack rejected the alert for {channel}: {response.get('error')}")
    except Exception as e:
        # Never let alerting failure mask the underlying task failure.
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
    folder_date: str,
    format: str,
    compression: str,
) -> str:
    """Build BigQuery EXPORT DATA statement."""
    extension = get_file_extension(format, compression)
    # Filename pattern: {export_name}_{date}-{shard}.{extension} (final format, no rename needed)
    gcs_uri = f"gs://{gcs_bucket}/{export_name}/{folder_date}/{export_name}_{folder_date}-*.{extension}"

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
            type=["null", "string"],  # Allow empty/null values
            description=f"Query for {export_name}. Clear field to skip this export.",
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
        # Baseline safety net. transfer_to_sftp overrides this with exponential
        # backoff — it is the step that actually flakes.
        "retries": 1,
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

            # If param is empty/None, skip this export
            if not query_param:
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

            # Use data_interval_end for folder naming (matches query filter date)
            folder_date = data_interval_end.strftime("%Y%m%d")

            # Build EXPORT DATA statement
            export_sql = build_export_query(
                query=resolved_query,
                gcs_bucket=gcs_bucket,
                export_name=export_name,
                folder_date=folder_date,
                format=export_config.get("format", "CSV"),
                compression=export_config.get("compression", "GZIP"),
            )

            print(f"Resolved query (with placeholders replaced):\n{resolved_query}")
            print(f"Full EXPORT DATA statement:\n{export_sql}")

            # Execute using BigQuery hook.
            # run_query() was removed in newer apache-airflow-providers-google;
            # insert_job() is the supported replacement and blocks until the job finishes.
            hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
            hook.insert_job(
                configuration={
                    "query": {
                        "query": export_sql,
                        "useLegacySql": False,
                    }
                },
                project_id=hook.project_id,
            )

            gcs_path = f"gs://{gcs_bucket}/{export_name}/{folder_date}/"
            print(f"Export complete. Files at: {gcs_path}")

            return {
                "export_name": export_name,
                "gcs_path": gcs_path,
                "ds": ds,
                "folder_date": folder_date,
            }

        @task(
            execution_timeout=timedelta(minutes=45),
            # SFTP is the flakiest hop, so back off hard rather than hammering it.
            # Airflow jitters each delay into [base*2^n, 2*base*2^n), giving roughly
            # 2-4, 4-8, 8-16, 16-32, then 32 minutes. The jitter is deliberate: all
            # export groups fail together, and lockstep retries would stampede the
            # same SFTP server.
            retries=5,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=32),
        )
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

            try:
                response = requests.post(
                    f"{cloud_run_url}/transfer",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=1800,
                )
                print(f"Cloud Run response status: {response.status_code}")
                print(f"Cloud Run response body: {response.text[:2000]}")
                response.raise_for_status()
                result = response.json()
            except requests.exceptions.RequestException as e:
                print(f"Cloud Run request failed: {e}")
                if hasattr(e, "response") and e.response is not None:
                    print(f"Error response body: {e.response.text[:2000]}")
                raise

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
            print(f"Payload: {json.dumps(payload)}")

            try:
                response = requests.post(
                    f"{cloud_run_url}/verify",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=300,
                )
                print(f"Cloud Run response status: {response.status_code}")
                print(f"Cloud Run response body: {response.text[:2000]}")
                response.raise_for_status()
                result = response.json()
            except requests.exceptions.RequestException as e:
                print(f"Cloud Run request failed: {e}")
                if hasattr(e, "response") and e.response is not None:
                    print(f"Error response body: {e.response.text[:2000]}")
                raise

            if not result.get("in_sync"):
                missing = result.get("missing_on_sftp", [])
                raise Exception(f"Sync verification failed. Missing files: {missing}")

            print(f"Verification passed: {result.get('gcs_file_count')} files in sync")
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
