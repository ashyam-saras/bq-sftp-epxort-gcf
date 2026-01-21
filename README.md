# BigQuery to SFTP Export Service

Export data from BigQuery to an SFTP server via Google Cloud Storage, orchestrated by Airflow.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Airflow DAG: sftp_export                                 │
│                         (schedule: 0 6 * * *)                                    │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─ TaskGroup: export_1 ──────────────────────────────────────────────────────┐  │
│  │  bq_export ──────▶ gcf_transfer ──────▶ verify_sync                        │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                              ║ (parallel)                                        │
│  ┌─ TaskGroup: export_2 ──────────────────────────────────────────────────────┐  │
│  │  bq_export ──────▶ gcf_transfer ──────▶ verify_sync                        │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  on_failure_callback ──────▶ Slack notification                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           Cloud Run Service                                      │
├──────────────────────────────────────────────────────────────────────────────────┤
│  POST /transfer   - Download from GCS, upload to SFTP                            │
│  POST /verify     - Compare GCS files with SFTP files                            │
│  GET  /health     - Health check                                                 │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Airflow** runs `EXPORT DATA` query to export BigQuery table to GCS
2. **Airflow** triggers Cloud Run `/transfer` endpoint
3. **Cloud Run** downloads files from GCS and uploads to SFTP
4. **Airflow** calls Cloud Run `/verify` to confirm sync
5. On failure, **Slack notification** is sent

## Project Structure

```
bq-sftp-export/
├── airflow/
│   └── dags/
│       └── sftp_export_dag.py      # Airflow DAG definition
├── configs/
│   └── exports.json                # Export configuration
├── src/
│   ├── config.py                   # Configuration loading
│   ├── helpers.py                  # Logging utilities
│   ├── sftp.py                     # SFTP operations
│   ├── transfer.py                 # GCS → SFTP transfer logic
│   └── verify.py                   # Sync verification logic
├── server.py                       # Cloud Run HTTP server
├── Dockerfile                      # Container definition
└── requirements.txt                # Python dependencies
```

## Configuration

### Export Configuration (`configs/exports.json`)

```json
{
  "gcs_bucket": "your-bucket-name",
  "gcs_expiration_days": 30,
  "cloud_run_url": "https://your-service.run.app",
  "sftp": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "user",
    "password": "password",
    "directory": "/uploads"
  },
  "exports": {
    "product_data": {
      "query": "SELECT * FROM `project.dataset.products` WHERE DATE(created_at) = '{date}'",
      "format": "CSV",
      "compression": "GZIP"
    },
    "customer_data": {
      "query": "SELECT id, name, email FROM `project.dataset.customers`",
      "format": "CSV",
      "compression": "GZIP"
    }
  }
}
```

### Configuration Options

| Field | Description | Default |
|-------|-------------|---------|
| `gcs_bucket` | GCS bucket for exports | Required |
| `gcs_expiration_days` | Auto-delete files after N days | 30 |
| `cloud_run_url` | Cloud Run service URL | Required |
| `sftp.host` | SFTP server hostname | Required |
| `sftp.port` | SFTP server port | 22 |
| `sftp.username` | SFTP username | Required |
| `sftp.password` | SFTP password | Required |
| `sftp.directory` | Remote directory for uploads | Required |

### Export Options

| Field | Description | Default |
|-------|-------------|---------|
| `query` | SELECT query (use `{date}` placeholder for YYYYMMDD) | Required |
| `format` | Export format: CSV, JSON, AVRO, PARQUET | CSV |
| `compression` | Compression: GZIP, SNAPPY, NONE | GZIP |

## Airflow Setup

### 1. Create Airflow Variable

Store the config JSON in an Airflow Variable named `sftp_export_config`:

```bash
airflow variables set sftp_export_config "$(cat configs/exports.json)"
```

### 2. Create HTTP Connection

Create an Airflow HTTP connection for Cloud Run:

- **Connection ID**: `cloud_run_sftp_export`
- **Connection Type**: HTTP
- **Host**: `https://your-service.run.app`
- **Extra**: `{"Authorization": "Bearer <ID_TOKEN>"}`

### 3. Set Slack Webhook (Optional)

For failure notifications:

```bash
airflow variables set slack_webhook_url "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

## Cloud Run Deployment

### Build and Deploy

```bash
# Build container
gcloud builds submit --tag gcr.io/PROJECT_ID/bq-sftp-export

# Deploy to Cloud Run
# NOTE: timeout must be >= Airflow task's requests timeout (1800s for transfer)
gcloud run deploy bq-sftp-export \
  --image gcr.io/PROJECT_ID/bq-sftp-export \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --timeout 2400 \
  --concurrency 10 \
  --set-env-vars "SFTP_HOST=sftp.example.com,SFTP_USERNAME=user,SFTP_PASSWORD=pass,SFTP_DIRECTORY=/uploads"
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `CONFIG_PATH` | Path to config file (default: `configs/exports.json`) |
| `SFTP_HOST` | SFTP host (if not using config file) |
| `SFTP_USERNAME` | SFTP username |
| `SFTP_PASSWORD` | SFTP password |
| `SFTP_DIRECTORY` | SFTP target directory |

## API Reference

### POST /transfer

Transfer files from GCS to SFTP.

**Request:**
```json
{
  "export_name": "product_data",
  "gcs_path": "gs://bucket/product_data/20250108/",
  "date": "2025-01-08"
}
```

**Response:**
```json
{
  "status": "success",
  "export_name": "product_data",
  "files_transferred": 3,
  "files": ["file1.csv.gz", "file2.csv.gz", "file3.csv.gz"],
  "total_mb": 12.5,
  "destination": "/uploads/",
  "total_time_seconds": 45.2
}
```

### POST /verify

Verify GCS and SFTP are in sync.

**Request:**
```json
{
  "export_name": "product_data",
  "gcs_path": "gs://bucket/product_data/20250108/"
}
```

**Response:**
```json
{
  "status": "success",
  "in_sync": true,
  "gcs_file_count": 3,
  "sftp_file_count": 3,
  "missing_on_sftp": [],
  "size_mismatches": []
}
```

## Local Development

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Set environment variables
export SFTP_HOST="sftp.example.com"
export SFTP_USERNAME="user"
export SFTP_PASSWORD="password"
export SFTP_DIRECTORY="/uploads"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

### Run Server Locally

```bash
python server.py
```

### Test Transfer

```bash
curl -X POST http://localhost:8080/transfer \
  -H "Content-Type: application/json" \
  -d '{
    "export_name": "test_export",
    "gcs_path": "gs://your-bucket/test/20250108/"
  }'
```

## GCS Lifecycle Policy

To auto-delete exported files after 30 days, set a lifecycle rule on your bucket:

```bash
gsutil lifecycle set lifecycle.json gs://your-bucket
```

**lifecycle.json:**
```json
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {
        "age": 30,
        "matchesPrefix": ["exports/"]
      }
    }
  ]
}
```

## Monitoring

- **Airflow UI**: Task status, logs, and DAG runs
- **Cloud Run Logs**: Transfer details and errors
- **Slack**: Failure notifications

## Troubleshooting

### Common Issues

1. **SFTP Connection Failed**
   - Verify SFTP credentials
   - Check firewall rules (port 22)
   - Test with: `python -m src.sftp check`

2. **No Files Found in GCS**
   - Verify BigQuery export completed
   - Check GCS path format: `gs://bucket/export_name/YYYYMMDD/`

3. **Verification Failed**
   - Check SFTP directory permissions
   - Compare file sizes in logs
   - Re-run transfer task

### Debug Commands

```bash
# Test SFTP connection
python -m src.sftp check

# List GCS files
gsutil ls "gs://bucket/export_name/20250108/"

# Check Cloud Run logs
gcloud run services logs read bq-sftp-export --limit 100
```
