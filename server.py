import base64
import datetime
import json
import os

from flask import Flask, jsonify, request

from src.config import load_config
from src.helpers import cprint
from src.main import export_to_sftp

app = Flask(__name__)


@app.route("/", methods=["POST"])
def handle_request():
    """Main endpoint that handles both Pub/Sub events and direct API calls"""
    try:
        envelope = request.get_json()

        # Check if this is a Pub/Sub message
        if envelope and "message" in envelope:
            # Extract the Pub/Sub message
            message = envelope["message"]

            if "data" in message:
                # Decode base64 data
                data_str = base64.b64decode(message["data"]).decode("utf-8")
                try:
                    # Parse JSON payload
                    data = json.loads(data_str)
                    cprint(f"Received Pub/Sub message: {data}", severity="INFO")

                    # Extract export parameters
                    export_name = data.get("export_name")
                    date_str = data.get("date")

                    if not export_name:
                        return jsonify({"status": "error", "message": "export_name is required"}), 400

                    # Parse date if provided
                    export_date = None
                    if date_str:
                        try:
                            export_date = datetime.datetime.strptime(date_str, r"%Y-%m-%d").date()
                        except ValueError:
                            return (
                                jsonify(
                                    {"status": "error", "message": f"Invalid date format: {date_str}, use YYYY-MM-DD"}
                                ),
                                400,
                            )

                    # Run the export process
                    config = load_config("configs/default.json")  # Add explicit path
                    cprint(f"Starting export via Pub/Sub: {export_name}", severity="INFO")
                    result = export_to_sftp(config, export_name, export_date)
                    return jsonify(result)
                except json.JSONDecodeError:
                    return jsonify({"status": "error", "message": "Invalid JSON in Pub/Sub message"}), 400

        # Handle direct API invocation
        config = load_config("configs/default.json")  # Add explicit path
        export_name = envelope.get("export_name")
        date_str = envelope.get("date")

        if not export_name:
            return jsonify({"status": "error", "message": "export_name is required"}), 400

        # Parse date if provided
        export_date = None
        if date_str:
            try:
                export_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"status": "error", "message": f"Invalid date format: {date_str}, use YYYY-MM-DD"}), 400

        # Run the export process
        cprint(f"Starting export via API request: {export_name}", severity="INFO")
        result = export_to_sftp(config, export_name, export_date)
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
