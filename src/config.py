"""
Configuration loading and validation for GCS→SFTP export function.
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
            # Build minimal config from individual environment variables
            config = {
                "sftp": {
                    "host": os.environ.get("SFTP_HOST"),
                    "port": int(os.environ.get("SFTP_PORT", "22")),
                    "username": os.environ.get("SFTP_USERNAME"),
                    "password": os.environ.get("SFTP_PASSWORD"),
                    "directory": os.environ.get("SFTP_DIRECTORY", "/"),
                },
                # Optional: keep for compatibility; not required by the service
                "gcs": {"bucket": os.environ.get("GCS_BUCKET")},
            }

    # Validate config
    _validate_config(config)
    return config


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration for GCS-only operation."""
    # SFTP configuration
    if "sftp" not in config or not isinstance(config["sftp"], dict):
        raise ConfigError("Missing sftp configuration")

    required = ["host", "username", "password", "directory"]
    missing = [k for k in required if not config["sftp"].get(k)]
    if missing:
        raise ConfigError(f"Missing SFTP configuration values: {', '.join(missing)}")

    # No exports required; any legacy keys are ignored
    if "exports" in config:
        cprint("Ignoring legacy 'exports' configuration in GCS-only mode", severity="WARNING")

    cprint("Configuration validated for scheduled-query GCS-only mode", severity="INFO")
