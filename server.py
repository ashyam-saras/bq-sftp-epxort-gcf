import datetime
import os

from flask import Flask, jsonify, request

from src.config import load_config
from src.helpers import cprint
from src.main import cloud_function_handler, export_to_sftp

app = Flask(__name__)


@app.route("/", methods=["POST"])
def handle_request():
    """Main endpoint that handles both Pub/Sub events and direct API calls"""
    try:
        if not request.is_json:
            return jsonify({"status": "error", "message": "Request must be JSON"}), 400

        request_data = request.get_json()

        # Check if this is a Pub/Sub event
        if "message" in request_data and "data" in request_data.get("message", {}):
            return jsonify(cloud_function_handler(request_data))

        # Handle direct API invocation
        config = load_config()
        export_name = request_data.get("export_name")
        date_str = request_data.get("date")

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
