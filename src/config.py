"""
Configuration loading and validation for BigQuery to SFTP export function.
"""

import json
import os
from typing import Any, Dict, Optional


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
                "metadata": {
                    "dataset": os.environ.get("METADATA_DATASET", "metadata"),
                    "processed_hashes_table": os.environ.get("PROCESSED_HASHES_TABLE"),
                },
                "exports": {},
            }

            # Look for export definitions in environment variables
            for key in os.environ:
                if key.startswith("EXPORT_"):
                    export_name = key[7:].lower()
                    try:
                        export_config = json.loads(os.environ[key])
                        config["exports"][export_name] = export_config
                    except json.JSONDecodeError:
                        raise ConfigError(f"Invalid JSON in {key} environment variable")

    # Validate config
    _validate_config(config)
    return config


def _validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration.

    Args:
        config: Configuration dictionary

    Raises:
        ConfigError: If configuration is invalid
    """
    # Check required sections
    required_sections = ["sftp", "gcs", "exports"]
    for section in required_sections:
        if section not in config:
            raise ConfigError(f"Missing required section in config: {section}")

    # Check SFTP config
    sftp_required = ["host", "username", "password", "directory"]
    for field in sftp_required:
        if not config["sftp"].get(field):
            raise ConfigError(f"Missing required SFTP config: {field}")

    # Check GCS config
    if not config["gcs"].get("bucket"):
        raise ConfigError("Missing GCS bucket name")

    # Check exports
    if not config["exports"]:
        raise ConfigError("No exports defined in configuration")

    for name, export in config["exports"].items():
        if not export.get("source_table"):
            raise ConfigError(f"Missing source_table in export: {name}")
