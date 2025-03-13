-- Create export metadata tracking table
CREATE TABLE IF NOT EXISTS `insightsprod.<METADATA_DATASET>.export_metadata` (
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
CLUSTER BY export_name, status
OPTIONS(
  description = 'Tracks status and details of BigQuery to SFTP export jobs'
);