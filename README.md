# BigQuery to SFTP Export Service

This service extracts data from BigQuery tables and uploads it to an SFTP server. It runs as a Cloud Run service, with support for both incremental and full exports, and can be triggered via HTTP requests or Pub/Sub messages.

## Features

- **Cloud Run Service**: Runs as a scalable, containerized service
- **Pub/Sub Integration**: Can be triggered via Pub/Sub messages for asynchronous processing
- **Cloud Scheduler**: Automated scheduling with Google Cloud Scheduler
- **Configurable Table Exports**: Define tables to export using a JSON configuration file
- **Date Range Exports**: Export data from a specific date range
- **Full Table Exports**: Export entire tables regardless of date
- **SFTP Export**: Secure upload to SFTP servers with multiple file handling
- **Parallel Transfer**: Multi-threaded uploads for improved performance
- **Export Tracking**: Metadata table for tracking export history and status
- **Structured Logging**: JSON-formatted logs for better observability

## Configuration

The service uses a JSON configuration file to define export settings:

```json
{
    "sftp": {
        "host": "sftp.example.com",
        "port": 22,
        "username": "user",
        "password": "password",
        "directory": "/uploads"
    },
    "gcs": {
        "bucket": "my-export-bucket"
    },
    "metadata": {
        "export_metadata_table": "project.dataset.export_metadata"
    },
    "exports": {
        "product_data": {
            "source_table": "project.dataset.products",
            "compress": true,
            "export_type": "date_range",
            "date_column": "modified_date",
            "days_lookback": 30
        },
        "customer_data": {
            "source_table": "project.dataset.customers",
            "compress": true,
            "export_type": "full"
        }
    }
}
```

## Project Structure

```
bq-sftp-export/
├── .github/
│   └── workflows/
│       └── deploy-cloud-run.yml    # CI/CD workflow for deployment
├── configs/
│   └── default.json                # Default export configuration
├── scripts/
│   ├── failed_transfers.py         # Utility to detect failed transfers
│   ├── retry_transfers.py          # Utility to retry failed transfers
│   ├── setup_cloudrun.sh           # Script to deploy to Cloud Run
│   ├── setup_pubsub.sh             # Script to set up Pub/Sub integration
│   └── setup_schedulers.sh         # Script to set up Cloud Scheduler jobs
├── src/
│   ├── __init__.py
│   ├── bigquery.py                 # BigQuery operations
│   ├── config.py                   # Configuration handling
│   ├── helpers.py                  # Helper functions
│   ├── main.py                     # Main service logic
│   ├── metadata.py                 # Export metadata tracking
│   └── sftp.py                     # SFTP operations
├── tests/
│   ├── __init__.py
│   ├── test_bigquery.py
│   ├── test_config.py
│   ├── test_helpers.py
│   ├── test_main.py
│   ├── test_metadata.py
│   └── test_sftp.py
├── .gitignore
├── Dockerfile                      # Container definition
├── README.md
├── requirements.txt                # Production dependencies
├── requirements-dev.txt            # Development dependencies
└── server.py                       # HTTP server for Cloud Run
```

## Deployment

### Local Development

```bash
# Set up virtual environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Set required environment variables
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
export EXPORT_METADATA_TABLE="project.dataset.export_metadata"
export GCS_BUCKET="my-export-bucket"
export SFTP_HOST="sftp.example.com"
export SFTP_USERNAME="username"
export SFTP_PASSWORD="password"
export SFTP_DIRECTORY="/uploads"

# Run the server locally
python server.py
```

### Build and Deploy to Cloud Run

```bash
# Make the script executable
chmod +x scripts/setup_cloudrun.sh

# Run deployment script
./scripts/setup_cloudrun.sh
```

### Setting Up Pub/Sub Integration

```bash
# Make the script executable
chmod +x scripts/setup_pubsub.sh  

# Run the Pub/Sub setup script
./scripts/setup_pubsub.sh
```

### Setting Up Cloud Scheduler

```bash
# Make the script executable
chmod +x scripts/setup_schedulers.sh

# Run the scheduler setup script
./scripts/setup_schedulers.sh
```

## Usage

### Triggering Exports via HTTP

```bash
# Direct API call 
curl -X POST https://your-service-url/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '{"export_name":"product_data","date":"2025-03-17"}'
```

### Triggering Exports via Pub/Sub

```bash
# Publish message to Pub/Sub topic
gcloud pubsub topics publish boxout-sftp-export-triggers \
  --message='{"export_name":"product_data","date":"2025-03-17"}'
```

### Checking for Failed Transfers

```bash
# Run the check script
python scripts/failed_transfers.py --export-name PnL_Amazon --date 20250317 \
  --gcs-prefix PnL_Amazon/20250317/
```

### Retrying Failed Transfers

```bash
# Retry failed transfers
python scripts/retry_transfers.py --export-name PnL_Amazon --date 20250317 \
  --missing-files missing_files.txt
```

## Metadata Table Schema

```sql
CREATE TABLE IF NOT EXISTS `project.dataset.export_metadata` (
  export_id STRING NOT NULL,
  export_name STRING NOT NULL,
  source_table STRING NOT NULL,
  destination_uri STRING NOT NULL,
  status STRING NOT NULL,
  rows_exported INT64,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  error_message STRING,
  PRIMARY KEY(export_id) NOT ENFORCED
)
PARTITION BY DATE(started_at)
CLUSTER BY export_name, status;
```

## Monitoring

Monitor the service through:
- Cloud Run logs
- Cloud Logging
- BigQuery metadata table
- Cloud Scheduler execution history
- Pub/Sub message delivery logs

## Environment Variables

- `SFTP_HOST`: SFTP server hostname
- `SFTP_USERNAME`: SFTP username  
- `SFTP_PASSWORD`: SFTP password
- `SFTP_DIRECTORY`: Base directory on SFTP server
- `GCS_BUCKET`: GCS bucket for temporary storage
- `EXPORT_METADATA_TABLE`: BigQuery table for export metadata
- `DEBUG`: Set to "True" for verbose logging
