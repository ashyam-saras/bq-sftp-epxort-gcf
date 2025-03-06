"""
Configuration handling for BigQuery to SFTP export.
Handles loading, validating, and providing access to configuration settings.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from src.helpers import cprint


class ConfigError(Exception):
    """Exception raised for configuration errors."""

    pass


def load_config(config_data: Optional[Dict[str, Any]] = None, config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and validate configuration from data or file.

    Args:
        config_data: Configuration dictionary (from Cloud Function trigger)
        config_file: Path to config file (used for local dev/testing)

    Returns:
        Dict: Validated configuration dictionary

    Raises:
        ConfigError: If the configuration is invalid or missing required fields
    """
    # Load from direct data (from Pub/Sub trigger)
    if config_data:
        cprint("Loading configuration from provided data")
        config = config_data

    # Load from file (for local testing)
    elif config_file:
        cprint(f"Loading configuration from file: {config_file}")
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            raise ConfigError(f"Failed to load config file: {str(e)}")

    # Load from default location
    else:
        default_config = Path(__file__).parent.parent / "configs" / "default.json"
        cprint(f"Loading configuration from default path: {default_config}")
        try:
            with open(default_config, "r") as f:
                config = json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            raise ConfigError(f"Failed to load default config file: {str(e)}")

    # Validate the configuration
    validate_config(config)
    return config


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate the configuration structure and required fields.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ConfigError: If validation fails
    """
    # Check for required top-level keys
    required_keys = ["exports", "metadata_table"]
    for key in required_keys:
        if key not in config:
            raise ConfigError(f"Missing required configuration key: {key}")

    # Check exports is a list and not empty
    if not isinstance(config["exports"], list) or len(config["exports"]) == 0:
        raise ConfigError("'exports' must be a non-empty list")

    # Validate each export configuration
    for i, export in enumerate(config["exports"]):
        validate_export_config(export, i)

    # Validate SFTP config if present
    if "sftp" in config:
        if not isinstance(config["sftp"], dict):
            raise ConfigError("'sftp' must be a dictionary")

        # Check for path (port is optional, default is 22)
        if "path" not in config["sftp"]:
            raise ConfigError("'sftp.path' is required")


def validate_export_config(export: Dict[str, Any], index: int) -> None:
    """
    Validate an individual export configuration.

    Args:
        export: Export configuration dictionary
        index: Index in the exports list (for error messages)

    Raises:
        ConfigError: If validation fails
    """
    # Check required fields
    required_fields = ["name", "source_table", "destination_filename"]
    for field in required_fields:
        if field not in export:
            raise ConfigError(f"Export #{index}: Missing required field '{field}'")

    # Check if incremental export has timestamp column
    if export.get("incremental", False) and "timestamp_column" not in export:
        raise ConfigError(f"Export '{export['name']}': Incremental export requires 'timestamp_column'")


def format_filename(filename_template: str) -> str:
    """
    Format a filename template with current datetime.

    Args:
        filename_template: Filename template with optional {datetime} placeholder

    Returns:
        Formatted filename
    """
    now = datetime.now()
    datetime_str = now.strftime("%Y%m%d_%H%M%S")

    return filename_template.replace("{datetime}", datetime_str)


def get_sftp_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get SFTP configuration with environment variables for credentials.

    Args:
        config: Full configuration dictionary

    Returns:
        Dict with merged SFTP config including credentials
    """
    sftp_config = config.get("sftp", {})

    # Add credentials from environment variables
    sftp_config["host"] = os.environ.get("SFTP_HOST")
    sftp_config["username"] = os.environ.get("SFTP_USERNAME")
    sftp_config["password"] = os.environ.get("SFTP_PASSWORD")

    # Default port if not specified
    if "port" not in sftp_config:
        sftp_config["port"] = 22

    # Validate required environment variables
    if not sftp_config["host"] or not sftp_config["username"] or not sftp_config["password"]:
        raise ConfigError("SFTP credentials not found in environment variables")

    return sftp_config


def get_metadata_table(config: Dict[str, Any]) -> str:
    """
    Get the fully qualified metadata table name.

    Args:
        config: Configuration dictionary

    Returns:
        Fully qualified table name (project.dataset.table)
    """
    return config["metadata_table"]
