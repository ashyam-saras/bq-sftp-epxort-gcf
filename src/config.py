"""
Configuration loading and validation for BigQuery to SFTP export function.
"""

import json
import os
from typing import Any, Dict, Optional

from src.helpers import cprint


class ConfigError(Exception):
    """Exception raised for configuration errors."""

    pass


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from file or environment variables.

    Args:
        config_path: Optional path to JSON config file

    Returns:
        Dictionary with configuration

    Raises:
        ConfigError: If configuration is invalid or missing required fields
    """
    # If path provided, load from file
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
        except Exception as e:
            raise ConfigError(f"Failed to load config file: {str(e)}")
    else:
        # Load from environment variables
        config_json = os.environ.get("EXPORT_CONFIG")
        if config_json:
            try:
                config = json.loads(config_json)
            except json.JSONDecodeError:
                raise ConfigError("Invalid JSON in EXPORT_CONFIG environment variable")
        else:
            # Build config from individual environment variables
            config = {
                "sftp": {
                    "host": os.environ.get("SFTP_HOST"),
                    "port": int(os.environ.get("SFTP_PORT", "22")),
                    "username": os.environ.get("SFTP_USERNAME"),
                    "password": os.environ.get("SFTP_PASSWORD"),
                    "directory": os.environ.get("SFTP_DIRECTORY", "/"),
                    "upload_method": os.environ.get("SFTP_UPLOAD_METHOD", "download"),
                },
                "gcs": {"bucket": os.environ.get("GCS_BUCKET")},
                "metadata": {"export_metadata_table": os.environ.get("EXPORT_METADATA_TABLE", "")},
                "exports": {},
            }

    # Validate config
    _validate_config(config)
    return config


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration and set defaults."""
    if "exports" not in config or not isinstance(config["exports"], dict):
        raise ConfigError("Missing or invalid exports section in config")

    for name, export in config["exports"].items():
        # Validate required fields
        if "source_table" not in export:
            raise ConfigError(f"Missing source_table in export config: {name}")

        # Set defaults for optional fields
        export["export_type"] = export.get("export_type", "full")

        # Validate date range export configuration
        if export.get("date_column") and "days_lookback" not in export:
            export["days_lookback"] = 7
            cprint(f"Export '{name}' has date_column but no days_lookback, using default: 7 days", severity="INFO")

    # SFTP configuration
    if "sftp" not in config:
        # Set default SFTP config from environment variables
        config["sftp"] = {
            "host": os.environ.get("SFTP_HOST", ""),
            "port": int(os.environ.get("SFTP_PORT", 22)),
            "username": os.environ.get("SFTP_USERNAME", ""),
            "password": os.environ.get("SFTP_PASSWORD", ""),
            "directory": os.environ.get("SFTP_DIRECTORY", "/"),
        }

    # GCS configuration
    if "gcs" not in config:
        # Set default GCS config from environment variables
        config["gcs"] = {"bucket": os.environ.get("GCS_BUCKET", "")}

    # Keep just the export_metadata_table part
    if "metadata" not in config:
        # Set default metadata config from environment variables
        config["metadata"] = {"export_metadata_table": os.environ.get("EXPORT_METADATA_TABLE", "")}
