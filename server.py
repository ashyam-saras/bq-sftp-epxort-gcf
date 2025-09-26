import base64
import datetime
import json
import os
import re

from flask import Flask, jsonify, request

from src.config import load_config
from src.helpers import cprint
from src.main import export_from_pattern

app = Flask(__name__)


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
        return jsonify(result)

    except Exception as e:
        cprint(f"Error processing request: {str(e)}", severity="ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Cloud Run"""
    return jsonify({"status": "healthy", "timestamp": datetime.datetime.now().isoformat()})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
