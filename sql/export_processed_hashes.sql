CREATE TABLE IF NOT EXISTS `insightsprod.<METADATA_DATASET>.export_processed_hashes` (
  export_id STRING NOT NULL,
  export_name STRING NOT NULL,
  row_hash STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  
  -- Primary key constraint using clustering
  PRIMARY KEY(row_hash, export_name) NOT ENFORCED
) 
PARTITION BY DATE(processed_at)
CLUSTER BY export_name, row_hash
OPTIONS(
  description = 'Stores hashes of rows that have been processed in data exports',
  require_partition_filter = false
);