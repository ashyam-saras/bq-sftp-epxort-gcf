"""Tests for configuration handling."""

import json
import os
from unittest.mock import mock_open, patch

import pytest

from src.config import ConfigError, load_config


def test_load_config_from_file():
    """Test loading config from a valid JSON file."""
    config_data = {
        "sftp": {
            "host": "sftp.example.com",
            "port": 22,
            "username": "user",
            "password": "pass",
            "directory": "/uploads",
        },
        "gcs": {"bucket": "test-bucket"},
        "metadata": {"export_metadata_table": "project.dataset.table"},
        "exports": {"test_export": {"source_table": "project.dataset.source", "export_type": "full"}},
    }

    m = mock_open(read_data=json.dumps(config_data))
    with patch("builtins.open", m):
        with patch("os.path.exists", return_value=True):
            result = load_config("config.json")
            assert result == config_data


def test_load_config_from_env_json():
    """Test loading config from EXPORT_CONFIG environment variable."""
    config_data = {
        "sftp": {"host": "sftp.example.com"},
        "gcs": {"bucket": "test-bucket"},
        "exports": {"test": {"source_table": "project.dataset.table"}},
    }

    with patch.dict(os.environ, {"EXPORT_CONFIG": json.dumps(config_data)}):
        result = load_config()
        assert result["sftp"]["host"] == "sftp.example.com"
        assert result["gcs"]["bucket"] == "test-bucket"
        assert result["exports"]["test"]["source_table"] == "project.dataset.table"


def test_load_config_from_env_vars():
    """Test loading config from individual environment variables."""
    env_vars = {
        "SFTP_HOST": "sftp.example.com",
        "SFTP_PORT": "2222",
        "SFTP_USERNAME": "user",
        "SFTP_PASSWORD": "pass",
        "SFTP_DIRECTORY": "/test",
        "GCS_BUCKET": "test-bucket",
        "EXPORT_METADATA_TABLE": "project.dataset.metadata",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        config = load_config()
        assert config["sftp"]["host"] == "sftp.example.com"
        assert config["sftp"]["port"] == 2222
        assert config["sftp"]["username"] == "user"
        assert config["sftp"]["password"] == "pass"
        assert config["sftp"]["directory"] == "/test"
        assert config["gcs"]["bucket"] == "test-bucket"
        assert config["metadata"]["export_metadata_table"] == "project.dataset.metadata"


def test_validate_config_missing_exports():
    """Test validation fails when exports section is missing."""
    config = {"sftp": {"host": "test"}, "gcs": {"bucket": "test"}}

    # Use patch to return our test config from open() call
    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            with pytest.raises(ConfigError, match="Missing or invalid exports section in config"):
                load_config("fake_config.json")


def test_validate_config_missing_source_table():
    """Test validation fails when source_table is missing in export config."""
    config = {
        "exports": {
            "test_export": {
                # Missing source_table
                "export_type": "full"
            }
        }
    }

    # Use patch to return our test config from open() call
    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            with pytest.raises(ConfigError, match="Missing source_table in export config: test_export"):
                load_config("fake_config.json")


def test_validate_config_default_values():
    """Test that default values are set correctly during validation."""
    config = {
        "exports": {
            "test_export": {
                "source_table": "project.dataset.table",
                "date_column": "timestamp",  # Has date_column but no days_lookback
            }
        }
    }

    # Use patch to return our test config from open() call
    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            with patch.dict(os.environ, {"SFTP_HOST": "test.com"}):
                result = load_config("fake_config.json")
                assert result["exports"]["test_export"]["days_lookback"] == 7
                assert result["exports"]["test_export"]["export_type"] == "full"


def test_load_config_invalid_json_file():
    """Test handling of invalid JSON in config file."""
    with patch("builtins.open", mock_open(read_data="invalid json")):
        with patch("os.path.exists", return_value=True):
            with pytest.raises(ConfigError, match="Failed to load config file"):
                load_config("config.json")


def test_load_config_invalid_env_json():
    """Test handling of invalid JSON in EXPORT_CONFIG environment variable."""
    with patch.dict(os.environ, {"EXPORT_CONFIG": "invalid json"}):
        with pytest.raises(ConfigError, match="Invalid JSON in EXPORT_CONFIG environment variable"):
            load_config()
