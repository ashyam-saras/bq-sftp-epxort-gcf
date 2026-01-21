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
        "exports": {"test_export": {"query": "SELECT * FROM table"}},
    }

    m = mock_open(read_data=json.dumps(config_data))
    with patch("builtins.open", m):
        with patch("os.path.exists", return_value=True):
            result = load_config("config.json")
            assert result["sftp"]["host"] == "sftp.example.com"
            assert result["exports"]["test_export"]["query"] == "SELECT * FROM table"


def test_load_config_from_env_json():
    """Test loading config from EXPORT_CONFIG environment variable."""
    config_data = {
        "sftp": {
            "host": "sftp.example.com",
            "username": "user",
            "password": "pass",
            "directory": "/uploads",
        },
        "gcs": {"bucket": "test-bucket"},
    }

    with patch.dict(os.environ, {"EXPORT_CONFIG": json.dumps(config_data)}, clear=True):
        result = load_config()
        assert result["sftp"]["host"] == "sftp.example.com"
        assert result["gcs"]["bucket"] == "test-bucket"


def test_load_config_from_env_vars():
    """Test loading config from individual environment variables."""
    env_vars = {
        "SFTP_HOST": "sftp.example.com",
        "SFTP_PORT": "2222",
        "SFTP_USERNAME": "user",
        "SFTP_PASSWORD": "pass",
        "SFTP_DIRECTORY": "/test",
        "GCS_BUCKET": "test-bucket",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        config = load_config()
        assert config["sftp"]["host"] == "sftp.example.com"
        assert config["sftp"]["port"] == 2222
        assert config["sftp"]["username"] == "user"
        assert config["sftp"]["password"] == "pass"
        assert config["sftp"]["directory"] == "/test"
        assert config["gcs"]["bucket"] == "test-bucket"


def test_validate_config_missing_sftp():
    """Test validation fails when sftp section is missing."""
    config = {"gcs": {"bucket": "test"}}

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            with pytest.raises(ConfigError, match="Missing 'sftp' configuration section"):
                load_config("fake_config.json")


def test_validate_config_missing_sftp_fields():
    """Test validation fails when required SFTP fields are missing."""
    config = {
        "sftp": {"host": "test.com"},  # Missing username, password, directory
        "gcs": {"bucket": "test"},
    }

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            with pytest.raises(ConfigError, match="Missing required SFTP configuration"):
                load_config("fake_config.json")


def test_validate_config_export_missing_query():
    """Test validation fails when export is missing required query field."""
    config = {
        "sftp": {
            "host": "test.com",
            "username": "user",
            "password": "pass",
            "directory": "/test",
        },
        "exports": {"test_export": {"name": "test"}},  # Missing query
    }

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            with pytest.raises(ConfigError, match="missing required 'query' field"):
                load_config("fake_config.json")


def test_load_config_invalid_json_file():
    """Test handling of invalid JSON in config file."""
    with patch("builtins.open", mock_open(read_data="invalid json")):
        with patch("os.path.exists", return_value=True):
            with pytest.raises(ConfigError, match="Failed to load config file"):
                load_config("config.json")


def test_load_config_invalid_env_json():
    """Test handling of invalid JSON in EXPORT_CONFIG environment variable."""
    with patch.dict(os.environ, {"EXPORT_CONFIG": "invalid json"}, clear=True):
        with pytest.raises(ConfigError, match="Invalid JSON in EXPORT_CONFIG environment variable"):
            load_config()


def test_load_config_no_config_found():
    """Test error when no configuration source is available."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigError, match="No configuration found"):
            load_config()


def test_gcs_expiration_days_default():
    """Test that GCS expiration_days defaults to 30."""
    config = {
        "sftp": {
            "host": "test.com",
            "username": "user",
            "password": "pass",
            "directory": "/test",
        },
    }

    with patch("os.path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=json.dumps(config))):
            result = load_config("fake_config.json")
            assert result["gcs"]["expiration_days"] == 30
