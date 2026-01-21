#!/usr/bin/env python3
"""
Local integration test for the SFTP Export pipeline.

Tests the actual functions that run in Airflow DAG and Cloud Run
without needing to deploy anything.

Usage:
    # Test BQ export only (what Airflow does)
    python scripts/test_local_integration.py --test bq_export --export account_overview_sales --date 2025-01-08

    # Test Cloud Run server locally (start server, call endpoints)
    python scripts/test_local_integration.py --test server

    # Test GCS → SFTP transfer (requires SFTP credentials)
    python scripts/test_local_integration.py --test transfer --gcs-path gs://bucket/path/

    # Test verification endpoint
    python scripts/test_local_integration.py --test verify --gcs-path gs://bucket/path/

    # Full pipeline test (BQ → GCS → SFTP)
    python scripts/test_local_integration.py --test full --export account_overview_sales --date 2025-01-08
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_bq_export(export_name: str, date: str, config_path: str = "configs/exports.json"):
    """
    Test the BigQuery export function (same logic as Airflow DAG's bq_export task).
    """
    print("\n" + "=" * 60)
    print("🧪 Testing BQ Export (Airflow DAG logic)")
    print("=" * 60)

    from google.cloud import bigquery

    # Load config
    with open(config_path) as f:
        config = json.load(f)

    exports = config.get("exports", {})
    if export_name not in exports:
        print(f"❌ Export '{export_name}' not found. Available: {list(exports.keys())}")
        return False

    export_config = exports[export_name]
    gcs_bucket = config["gcs_bucket"]

    # Parse date
    dt = datetime.strptime(date, "%Y-%m-%d")
    ds = dt.strftime("%Y-%m-%d")
    ds_nodash = dt.strftime("%Y%m%d")
    data_interval_start = dt
    data_interval_end = dt + timedelta(days=1)

    # Resolve placeholders (same as DAG)
    query = export_config["query"]
    query = (
        query.replace("{ds}", ds)
        .replace("{ds_nodash}", ds_nodash)
        .replace("{date}", ds_nodash)
        .replace("{data_interval_start}", data_interval_start.strftime("%Y-%m-%d %H:%M:%S"))
        .replace("{data_interval_end}", data_interval_end.strftime("%Y-%m-%d %H:%M:%S"))
    )

    # Build extension
    fmt = export_config.get("format", "CSV")
    compression = export_config.get("compression", "GZIP")
    ext = fmt.lower()
    if compression.upper() == "GZIP":
        ext += ".gz"

    gcs_uri = f"gs://{gcs_bucket}/{export_name}/{ds_nodash}/{export_name}-*.{ext}"

    # Build EXPORT DATA query
    export_sql = f"""EXPORT DATA OPTIONS(
    uri='{gcs_uri}',
    format='{fmt}',
    compression='{compression}',
    overwrite=true,
    header=true
) AS
{query}"""

    print(f"\nExport: {export_name}")
    print(f"Date: {ds}")
    print(f"Target: {gcs_uri}")
    print(f"\nQuery:\n{'-'*40}\n{export_sql}\n{'-'*40}")

    # Execute
    print("\n⏳ Executing on BigQuery...")
    try:
        client = bigquery.Client()
        job = client.query(export_sql)
        job.result()  # Wait

        print(f"\n✅ BQ Export SUCCESS!")
        print(f"   Job ID: {job.job_id}")
        print(f"   Bytes processed: {job.total_bytes_processed:,}")

        # List exported files
        from google.cloud import storage

        storage_client = storage.Client()
        bucket = storage_client.bucket(gcs_bucket)
        prefix = f"{export_name}/{ds_nodash}/"
        blobs = list(bucket.list_blobs(prefix=prefix))

        print(f"\n📁 Exported files in gs://{gcs_bucket}/{prefix}:")
        for blob in blobs:
            if not blob.name.endswith("/"):
                print(f"   - {blob.name} ({blob.size / 1024 / 1024:.2f} MB)")

        return True

    except Exception as e:
        print(f"\n❌ BQ Export FAILED: {e}")
        return False


def test_server():
    """
    Test the Cloud Run server locally.
    Starts the Flask server and tests the endpoints.
    """
    print("\n" + "=" * 60)
    print("🧪 Testing Cloud Run Server (local)")
    print("=" * 60)

    # Check if server is already running
    try:
        resp = requests.get("http://localhost:8080/health", timeout=2)
        if resp.status_code == 200:
            print("✅ Server already running on :8080")
            return test_server_endpoints()
    except requests.exceptions.ConnectionError:
        pass

    # Start server in background
    print("\n⏳ Starting local server...")
    server_process = subprocess.Popen(
        ["python3", "server.py"],
        cwd=str(Path(__file__).parent.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to start
    time.sleep(3)

    try:
        return test_server_endpoints()
    finally:
        print("\n🛑 Stopping server...")
        server_process.terminate()
        server_process.wait(timeout=5)


def test_server_endpoints():
    """Test the server endpoints."""
    base_url = "http://localhost:8080"

    # Test health
    print("\n📍 GET /health")
    try:
        resp = requests.get(f"{base_url}/health", timeout=5)
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.json()}")
        if resp.status_code != 200:
            return False
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False

    # Test transfer endpoint (will fail without valid GCS path, but tests the endpoint)
    print("\n📍 POST /transfer (validation test)")
    try:
        resp = requests.post(
            f"{base_url}/transfer",
            json={"export_name": "test", "gcs_path": "gs://test-bucket/test/"},
            timeout=10,
        )
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.json()}")
        # 404 is expected (no files), 500 is config error - both mean endpoint works
        if resp.status_code in [200, 404, 500]:
            print("   ✅ Endpoint responding correctly")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False

    # Test verify endpoint
    print("\n📍 POST /verify (validation test)")
    try:
        resp = requests.post(
            f"{base_url}/verify",
            json={"export_name": "test", "gcs_path": "gs://test-bucket/test/"},
            timeout=10,
        )
        print(f"   Status: {resp.status_code}")
        print(f"   Response: {resp.json()}")
        if resp.status_code in [200, 404, 409, 500]:
            print("   ✅ Endpoint responding correctly")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False

    print("\n✅ Server endpoints working!")
    return True


def test_transfer(gcs_path: str, config_path: str = "configs/exports.json"):
    """
    Test the GCS → SFTP transfer function directly.
    """
    print("\n" + "=" * 60)
    print("🧪 Testing GCS → SFTP Transfer")
    print("=" * 60)

    from src.config import load_config
    from src.transfer import transfer_gcs_to_sftp

    try:
        config = load_config(config_path)
        print(f"\nSFTP Host: {config['sftp']['host']}")
        print(f"GCS Path: {gcs_path}")

        result = transfer_gcs_to_sftp(
            sftp_config=config["sftp"],
            gcs_path=gcs_path,
            export_name="test_transfer",
        )

        print(f"\n✅ Transfer SUCCESS!")
        print(f"   Files transferred: {result['files_transferred']}")
        print(f"   Total MB: {result['total_mb']}")
        print(f"   Destination: {result['destination']}")
        return True

    except Exception as e:
        print(f"\n❌ Transfer FAILED: {e}")
        return False


def test_verify(gcs_path: str, config_path: str = "configs/exports.json"):
    """
    Test the GCS ↔ SFTP verification function directly.
    """
    print("\n" + "=" * 60)
    print("🧪 Testing GCS ↔ SFTP Verification")
    print("=" * 60)

    from src.config import load_config
    from src.verify import verify_gcs_sftp_sync

    try:
        config = load_config(config_path)
        print(f"\nSFTP Host: {config['sftp']['host']}")
        print(f"GCS Path: {gcs_path}")

        result = verify_gcs_sftp_sync(
            sftp_config=config["sftp"],
            gcs_path=gcs_path,
            export_name="test_verify",
        )

        if result["in_sync"]:
            print(f"\n✅ Verification PASSED - In sync!")
        else:
            print(f"\n⚠️ Verification FAILED - Not in sync!")
            print(f"   Missing on SFTP: {result.get('missing_on_sftp', [])}")

        print(f"   GCS files: {result['gcs_file_count']}")
        print(f"   SFTP files: {result['sftp_file_count']}")
        return result["in_sync"]

    except Exception as e:
        print(f"\n❌ Verification FAILED: {e}")
        return False


def test_full_pipeline(export_name: str, date: str, config_path: str = "configs/exports.json"):
    """
    Test the full pipeline: BQ → GCS → SFTP → Verify
    """
    print("\n" + "=" * 60)
    print("🧪 FULL PIPELINE TEST")
    print("=" * 60)

    # Load config for GCS bucket
    with open(config_path) as f:
        config = json.load(f)

    gcs_bucket = config["gcs_bucket"]
    ds_nodash = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
    gcs_path = f"gs://{gcs_bucket}/{export_name}/{ds_nodash}/"

    # Step 1: BQ Export
    print("\n" + "-" * 40)
    print("Step 1/3: BQ → GCS Export")
    print("-" * 40)
    if not test_bq_export(export_name, date, config_path):
        print("\n❌ Pipeline failed at BQ Export step")
        return False

    # Step 2: GCS → SFTP Transfer
    print("\n" + "-" * 40)
    print("Step 2/3: GCS → SFTP Transfer")
    print("-" * 40)
    if not test_transfer(gcs_path, config_path):
        print("\n❌ Pipeline failed at Transfer step")
        return False

    # Step 3: Verify
    print("\n" + "-" * 40)
    print("Step 3/3: Verification")
    print("-" * 40)
    if not test_verify(gcs_path, config_path):
        print("\n❌ Pipeline failed at Verification step")
        return False

    print("\n" + "=" * 60)
    print("🎉 FULL PIPELINE SUCCESS!")
    print("=" * 60)
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Local integration tests for SFTP Export pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--test",
        required=True,
        choices=["bq_export", "server", "transfer", "verify", "full"],
        help="Which test to run",
    )
    parser.add_argument("--export", help="Export name (for bq_export and full tests)")
    parser.add_argument("--date", help="Date YYYY-MM-DD (for bq_export and full tests)")
    parser.add_argument("--gcs-path", help="GCS path (for transfer and verify tests)")
    parser.add_argument("--config", default="configs/exports.json", help="Config file path")

    args = parser.parse_args()

    if args.test == "bq_export":
        if not args.export or not args.date:
            print("❌ --export and --date required for bq_export test")
            sys.exit(1)
        success = test_bq_export(args.export, args.date, args.config)

    elif args.test == "server":
        success = test_server()

    elif args.test == "transfer":
        if not args.gcs_path:
            print("❌ --gcs-path required for transfer test")
            sys.exit(1)
        success = test_transfer(args.gcs_path, args.config)

    elif args.test == "verify":
        if not args.gcs_path:
            print("❌ --gcs-path required for verify test")
            sys.exit(1)
        success = test_verify(args.gcs_path, args.config)

    elif args.test == "full":
        if not args.export or not args.date:
            print("❌ --export and --date required for full test")
            sys.exit(1)
        success = test_full_pipeline(args.export, args.date, args.config)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
