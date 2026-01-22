"""
Configuration loading and validation for GCS→SFTP export service.
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
    config = None
    
    # Priority 1: Explicit path provided
    if config_path:
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
                    cprint(f"Loaded config from file", severity="INFO", path=config_path)
            except Exception as e:
                raise ConfigError(f"Failed to load config file '{config_path}': {str(e)}")
        else:
            raise ConfigError(f"Config file not found: {config_path}")
    
    # Priority 2: EXPORT_CONFIG environment variable (full JSON)
    if config is None:
        config_json = os.environ.get("EXPORT_CONFIG")
        if config_json:
            try:
                config = json.loads(config_json)
                cprint("Loaded config from EXPORT_CONFIG env var", severity="INFO")
            except json.JSONDecodeError as e:
                raise ConfigError(f"Invalid JSON in EXPORT_CONFIG environment variable: {e}")
    
    # Priority 3: Individual environment variables (minimal config)
    if config is None:
        sftp_host = os.environ.get("SFTP_HOST")
        if sftp_host:
            config = {
                "sftp": {
                    "host": sftp_host,
                    "port": int(os.environ.get("SFTP_PORT", "22")),
                    "username": os.environ.get("SFTP_USERNAME"),
                    "password": os.environ.get("SFTP_PASSWORD"),
                    "directory": os.environ.get("SFTP_DIRECTORY", "/"),
                },
                "gcs": {
                    "bucket": os.environ.get("GCS_BUCKET"),
                    "expiration_days": int(os.environ.get("GCS_EXPIRATION_DAYS", "30")),
                },
            }
            cprint("Loaded config from individual env vars", severity="INFO")
        else:
            raise ConfigError(
                "No configuration found. Provide config_path, EXPORT_CONFIG env var, "
                "or individual SFTP_* environment variables."
            )

    # Merge environment variable overrides (env vars take precedence)
    config = _merge_env_overrides(config)
    
    # Validate config
    _validate_config(config)
    return config


def _merge_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge environment variable overrides into config."""
    # Ensure sftp section exists
    if "sftp" not in config:
        config["sftp"] = {}
    
    # SFTP overrides from env vars
    env_overrides = {
        "host": os.environ.get("SFTP_HOST"),
        "port": os.environ.get("SFTP_PORT"),
        "username": os.environ.get("SFTP_USERNAME"),
        "password": os.environ.get("SFTP_PASSWORD"),
        "directory": os.environ.get("SFTP_DIRECTORY"),
    }
    
    for key, value in env_overrides.items():
        if value is not None:
            if key == "port":
                config["sftp"][key] = int(value)
            else:
                config["sftp"][key] = value
    
    # GCS overrides
    if "gcs" not in config:
        config["gcs"] = {}
    
    if os.environ.get("GCS_BUCKET"):
        config["gcs"]["bucket"] = os.environ.get("GCS_BUCKET")
    if os.environ.get("GCS_EXPIRATION_DAYS"):
        config["gcs"]["expiration_days"] = int(os.environ.get("GCS_EXPIRATION_DAYS"))
    
    return config


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration structure."""
    # SFTP configuration is required
    if "sftp" not in config or not isinstance(config["sftp"], dict):
        raise ConfigError("Missing 'sftp' configuration section")

    required_sftp = ["host", "username", "password", "directory"]
    missing = [k for k in required_sftp if not config["sftp"].get(k)]
    if missing:
        raise ConfigError(f"Missing required SFTP configuration: {', '.join(missing)}")

    # GCS configuration is optional but has defaults
    if "gcs" not in config:
        config["gcs"] = {}
    
    config["gcs"].setdefault("expiration_days", 30)
    
    # Exports configuration is optional (can be passed at runtime)
    if "exports" in config:
        for name, export_config in config["exports"].items():
            if "query" not in export_config:
                raise ConfigError(f"Export '{name}' missing required 'query' field")

    cprint("Configuration validated successfully", severity="INFO")


def get_export_config(config: Dict[str, Any], export_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific export.
    
    Args:
        config: Full configuration dictionary
        export_name: Name of the export
    
    Returns:
        Export-specific configuration
    
    Raises:
        ConfigError: If export not found
    """
    exports = config.get("exports", {})
    if export_name not in exports:
        available = list(exports.keys())
        raise ConfigError(
            f"Export '{export_name}' not found. Available exports: {available}"
        )
    return exports[export_name]
