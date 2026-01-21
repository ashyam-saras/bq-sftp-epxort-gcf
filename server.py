"""
Cloud Run server for GCS → SFTP file transfer.
Triggered by Airflow via HTTP POST requests.
"""

import datetime
import json
import os

from flask import Flask, jsonify, request

from src.config import load_config
from src.helpers import cprint
from src.transfer import transfer_gcs_to_sftp

app = Flask(__name__)

# Load config once at startup
CONFIG_PATH = os.environ.get("CONFIG_PATH", "configs/exports.json")


@app.route("/transfer", methods=["POST"])
def handle_transfer():
    """
    Transfer files from GCS to SFTP.

    Expected payload:
    {
        "export_name": "product_data",
        "gcs_path": "gs://bucket/exports/product_data/20250108/",
        "date": "2025-01-08"  # optional, for logging
    }

    Returns:
    {
        "status": "success",
        "export_name": "product_data",
        "files_transferred": 3,
        "total_mb": 12.5,
        "destination": "/saras/product_data_20250108.csv.gz"
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON payload"}), 400

        export_name = data.get("export_name")
        gcs_path = data.get("gcs_path")
        export_date = data.get("date", datetime.date.today().isoformat())

        if not export_name:
            return jsonify({"status": "error", "message": "Missing export_name"}), 400
        if not gcs_path:
            return jsonify({"status": "error", "message": "Missing gcs_path"}), 400

        cprint(
            f"Received transfer request",
            severity="INFO",
            export_name=export_name,
            gcs_path=gcs_path,
            date=export_date,
        )

        # Load config and execute transfer
        config = load_config(CONFIG_PATH)
        result = transfer_gcs_to_sftp(
            sftp_config=config["sftp"],
            gcs_path=gcs_path,
            export_name=export_name,
        )

        return jsonify(result), 200 if result["status"] == "success" else 500

    except FileNotFoundError as e:
        cprint(f"No files found: {str(e)}", severity="WARNING")
        return jsonify({"status": "error", "message": str(e)}), 404

    except Exception as e:
        cprint(f"Error processing transfer: {str(e)}", severity="ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/verify", methods=["POST"])
def handle_verify():
    """
    Verify GCS and SFTP are in sync.

    Expected payload:
    {
        "export_name": "product_data",
        "gcs_path": "gs://bucket/exports/product_data/20250108/",
        "expected_files": ["file1.csv.gz", "file2.csv.gz"]  # optional
    }

    Returns:
    {
        "status": "success",
        "in_sync": true,
        "gcs_files": ["file1.csv.gz", "file2.csv.gz"],
        "sftp_files": ["file1.csv.gz", "file2.csv.gz"],
        "missing_on_sftp": [],
        "extra_on_sftp": []
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON payload"}), 400

        export_name = data.get("export_name")
        gcs_path = data.get("gcs_path")

        if not export_name or not gcs_path:
            return jsonify({"status": "error", "message": "Missing export_name or gcs_path"}), 400

        config = load_config(CONFIG_PATH)

        from src.verify import verify_gcs_sftp_sync

        result = verify_gcs_sftp_sync(
            sftp_config=config["sftp"],
            gcs_path=gcs_path,
            export_name=export_name,
        )

        status_code = 200 if result.get("in_sync", False) else 409
        return jsonify(result), status_code

    except Exception as e:
        cprint(f"Error during verification: {str(e)}", severity="ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Cloud Run."""
    return (
        jsonify(
            {
                "status": "healthy",
                "timestamp": datetime.datetime.now().isoformat(),
                "config_path": CONFIG_PATH,
            }
        ),
        200,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
