import base64
import datetime
import json
import os
import re

import requests
from flask import Flask, jsonify, request

from src.config import load_config
from src.helpers import cprint
from src.main import export_from_pattern

app = Flask(__name__)


def send_slack_notification(export_name: str, result: dict):
    """Send Slack notification for completed export."""
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        cprint("No SLACK_WEBHOOK_URL configured, skipping notification", severity="WARNING")
        return

    try:
        # Format the destination path for display
        destination = result.get("destination", "Unknown")
        if destination.endswith("/*"):
            destination = destination[:-2] + " (multiple files)"

        # Create the Slack message
        message = {
            "text": f"📊 *Export Complete: {export_name}*",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"📊 Export Complete: {export_name}"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Table:*\n{export_name}"},
                        {"type": "mrkdwn", "text": f"*Files Transferred:*\n{result.get('files_transferred', 0)}"},
                        {"type": "mrkdwn", "text": f"*Destination:*\n{destination}"},
                        {"type": "mrkdwn", "text": f"*Size:*\n{result.get('total_mb', 0):.2f} MB"},
                        {"type": "mrkdwn", "text": f"*Duration:*\n{result.get('total_time_seconds', 0):.1f}s"},
                        {"type": "mrkdwn", "text": f"*Source:*\n{result.get('source', 'Unknown')}"},
                    ],
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"✅ Export completed successfully at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                        }
                    ],
                },
            ],
        }

        response = requests.post(slack_webhook_url, json=message, timeout=10)
        response.raise_for_status()
        cprint("Slack notification sent successfully", severity="INFO")

    except Exception as e:
        cprint(f"Failed to send Slack notification: {str(e)}", severity="ERROR")


@app.route("/", methods=["POST"])
def handle_request():
    """Endpoint that handles Scheduled Query Pub/Sub events only."""
    try:
        envelope = request.get_json()
        if not envelope or "message" not in envelope or "data" not in envelope["message"]:
            return jsonify({"status": "error", "message": "Invalid Pub/Sub envelope"}), 400

        # Decode base64 data
        data_str = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
        try:
            # Parse JSON payload
            data = json.loads(data_str)
        except json.JSONDecodeError:
            return jsonify({"status": "error", "message": "Invalid JSON in Pub/Sub message"}), 400

        if data.get("dataSourceId") != "scheduled_query" or not isinstance(data.get("params"), dict):
            return jsonify({"status": "error", "message": "Unsupported payload; expected Scheduled Query"}), 400

        query_text = data["params"].get("query", "")
        run_time = data.get("runTime") or data.get("endTime")

        # Extract the EXPORT DATA URI from the query
        # Expect: uri = 'gs://bucket/export_name/%s/export_name-%s-*.csv.gz'
        uri_match = re.search(r"uri\s*=\s*'([^']+)'", query_text)
        if not uri_match:
            return (
                jsonify({"status": "error", "message": "Unable to find EXPORT DATA uri in Scheduled Query"}),
                400,
            )
        uri_template = uri_match.group(1)

        # Resolve %s placeholders using date from run_time
        export_date = None
        if run_time:
            try:
                dt = datetime.datetime.fromisoformat(run_time.replace("Z", "+00:00"))
                export_date = dt.strftime(r"%Y%m%d")
            except Exception:
                export_date = None

        if not export_date:
            return jsonify({"status": "error", "message": "Missing or invalid runTime for date resolution"}), 400

        # Replace all %s with export_date
        resolved_uri = uri_template.replace("%s", export_date)

        # Use the export_name as the first path segment after bucket
        m = re.match(r"gs://[^/]+/([^/]+)/", resolved_uri)
        export_name = m.group(1) if m else "scheduled_export"

        # Call export from pattern/prefix
        config = load_config("configs/default.json")
        cprint(
            f"Starting export via Scheduled Query payload",
            severity="INFO",
            export_name=export_name,
            source=resolved_uri,
            run_time=run_time,
        )
        result = export_from_pattern(config, export_name, resolved_uri)

        # Send Slack notification if export was successful
        if result.get("status") == "success":
            send_slack_notification(export_name, result)

        return jsonify(result), 200

    except Exception as e:
        cprint(f"Error processing request: {str(e)}", severity="ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Cloud Run"""
    return jsonify({"status": "healthy", "timestamp": datetime.datetime.now().isoformat()}), 200
