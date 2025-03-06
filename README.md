# BigQuery to SFTP Export Cloud Function

This Cloud Function extracts data from BigQuery tables and uploads it to an SFTP server. It supports both incremental and full exports, with configurable table mappings and scheduling.

## Features

- **Configurable Table Exports**: Define tables to export using a configuration file
- **Incremental Exports**: Export only new/changed records since the last export
- **Full Table Exports**: Option to export entire tables regardless of last export date
- **Export Tracking**: Metadata table to track export history and prevent duplicate exports
- **Notifications**: Automated email and Slack notifications upon completion or failure
- **Secure Credentials**: Environment variables for sensitive information
- **Scheduling**: Cloud Scheduler integration for periodic execution

## Configuration

The function uses a JSON configuration file to define export settings:

```json
{
  "exports": [
    {
      "name": "shopify_customers",
      "source_table": "prod_staging.shopify_customerdata",
      "destination_filename": "SHOPIFYCUSTOMERDATA_{datetime}.csv",
      "timestamp_column": "last_updated_at",
      "incremental": true,
      "columns": ["id", "address_key", "name_key", "phone_key", "email_key", "source_type", "customer_id", "address_type", "addr_line_1", "addr_line_2", "city", "state", "country", "postal_code", "first_name", "last_name", "phone", "email", "last_updated_at", "created_at", "batch_runtime", "_run_id"]
    },
    {
      "name": "klaviyo_customers",
      "source_table": "prod_staging.klaviyo_customerdata",
      "destination_filename": "KLAVIYOCUSTOMERDATA_{datetime}.csv",
      "timestamp_column": "last_updated_at",
      "incremental": true,
      "columns": ["id", "address_key", "name_key", "phone_key", "email_key", "source_type", "customer_id", "address_type", "addr_line_1", "addr_line_2", "city", "state", "country", "postal_code", "first_name", "last_name", "phone", "email", "last_updated_at", "created_at", "batch_runtime", "_run_id"]
    }
  ],
  "sftp": {
    "path": "/To_Verite",
    "port": 22
  },
  "metadata_table": "project.dataset.export_metadata"
}
```

## Project Structure

The project follows a modular structure to ensure maintainability and testability:

```
bq-sftp-export-gcf/
├── .github/
│   └── workflows/
│       ├── deploy.yml          # CI/CD workflow for deployment
│       └── test.yml            # CI/CD workflow for testing
├── src/
│   ├── __init__.py
│   ├── main.py                 # Cloud Function entry point
│   ├── bigquery.py             # BigQuery operations
│   ├── sftp.py                 # SFTP operations
│   ├── metadata.py             # Metadata tracking
│   ├── notification.py         # Email/Slack notifications
│   └── config.py               # Configuration handling
├── tests/
│   ├── __init__.py
│   ├── test_bigquery.py
│   ├── test_sftp.py
│   ├── test_metadata.py
│   └── test_main.py
├── configs/
│   └── default.json            # Default export configuration
├── deploy/
│   ├── deploy.sh               # Deployment script
│   └── setup_scheduler.sh      # Setup Cloud Scheduler jobs
├── .gitignore
├── requirements.txt            # Production dependencies
├── requirements-dev.txt        # Development dependencies
└── README.md
```

### Key Components

- **main.py**: Entry point for the Cloud Function that orchestrates the export process
- **bigquery.py**: Handles BigQuery connections, query execution, and data extraction
- **sftp.py**: Manages SFTP connections and file uploads
- **metadata.py**: Tracks export history and manages the metadata table
- **notification.py**: Sends email and Slack notifications
- **config.py**: Processes and validates export configurations

### Development Workflow

1. Define export configurations in `configs/default.json`
2. Run tests locally using pytest
3. Deploy using the deployment scripts or GitHub Actions
4. Monitor exports through the metadata table

### Design Principles

- **Modularity**: Each module has a single responsibility
- **Configurability**: Export settings defined externally
- **Security**: Credentials managed through environment variables
- **Testability**: Components designed for unit testing
- **Error Handling**: Comprehensive error handling with notifications

## Metadata Table

The function uses a BigQuery table to track export history and manage incremental exports:

### Table Structure

```sql
CREATE TABLE IF NOT EXISTS `project.dataset.export_metadata` (
  export_name STRING NOT NULL,
  source_table STRING NOT NULL,
  last_export_timestamp TIMESTAMP,
  last_exported_value STRING,
  rows_exported INT64,
  file_name STRING,
  export_status STRING,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message STRING
);
```

### Purpose
The metadata table serves several key functions:
- Tracks the last successful export timestamp for each export job
- Stores the last exported value for incremental exports
- Records statistics about each export (row count, file names)
- Maintains error information for failed exports
- Prevents duplicate exports and data loss

For incremental exports, the function queries this table to determine the timestamp cutoff point. After a successful export, it updates the table with the new timestamp and export details.

You can also query this table directly to monitor export history and troubleshoot any issues.

## Environment Variables

- `SFTP_HOST`: SFTP server hostname
- `SFTP_USERNAME`: SFTP username
- `SFTP_PASSWORD`: SFTP password
- `EMAIL_PASSWORD`: Password for sending email notifications
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key (for local testing)

## Development

### Local Testing

To test the function locally:

```bash
# Set environment variables
export SFTP_HOST=your-sftp-host
export SFTP_USERNAME=your-username
export SFTP_PASSWORD=your-password
export EMAIL_PASSWORD=your-email-password
export GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json

# Run the function locally
functions-framework-python --target=export_to_sftp --debug
```

### Unit Testing

```bash
python -m pytest
```

## Deployment

### Manual Deployment

```bash
gcloud functions deploy export_to_sftp \
  --runtime python312 \
  --trigger-topic export-to-sftp-trigger \
  --entry-point export_to_sftp \
  --service-account your-service-account@your-project.iam.gserviceaccount.com \
  --set-env-vars SFTP_HOST=your-sftp-host,SFTP_USERNAME=your-username
```

### GitHub Actions Deployment
This repository includes GitHub Actions workflows for CI/CD deployment.

## Scheduling
Configure Cloud Scheduler to run the export function on a schedule:

```bash
gcloud scheduler jobs create pubsub export-job \
  --schedule "0 5 * * *" \
  --topic export-to-sftp-trigger \
  --message-body '{"config": "default"}' \
  --time-zone "America/Chicago"
```

## Monitoring
Monitor function execution through:
- Cloud Functions logs
- Email notifications
- Slack alerts
- BigQuery metadata table
