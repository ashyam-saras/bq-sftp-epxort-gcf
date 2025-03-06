"""Tests for configuration handling."""

import json
import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.config import (
    ConfigError,
    format_filename,
    get_metadata_table,
    get_sftp_config,
    load_config,
    validate_config,
    validate_export_config,
)


def test_format_filename():
    """Test formatting filename with datetime placeholder."""
    with patch("src.config.datetime") as mock_datetime:
        # Set a fixed datetime for testing
        mock_date = MagicMock()
        mock_date.strftime.return_value = "20230115_123045"
        mock_datetime.now.return_value = mock_date

        # Test with datetime placeholder
        result = format_filename("export_{datetime}.csv")
        assert result == "export_20230115_123045.csv"

        # Test without datetime placeholder
        result = format_filename("export.csv")
        assert result == "export.csv"


def test_validate_config_valid():
    """Test validating a valid configuration."""
    config = {
        "exports": [
            {"name": "test_export", "source_table": "project.dataset.table", "destination_filename": "export.csv"}
        ],
        "metadata_table": "project.dataset.metadata",
    }
    # Should not raise an exception
    validate_config(config)


def test_validate_config_missing_required():
    """Test validating a configuration missing required fields."""
    # First test - missing metadata_table
    config = {"exports": []}
    with pytest.raises(ConfigError, match="Missing required configuration key: metadata_table"):
        validate_config(config)

    # Second test - missing exports
    config = {
        # Missing exports
        "metadata_table": "project.dataset.metadata"
    }
    with pytest.raises(ConfigError, match="Missing required configuration key: exports"):
        validate_config(config)


def test_validate_config_empty_exports():
    """Test validating a configuration with empty exports list."""
    config = {"exports": [], "metadata_table": "project.dataset.metadata"}  # Empty list
    with pytest.raises(ConfigError, match="'exports' must be a non-empty list"):
        validate_config(config)


def test_validate_export_config():
    """Test validating export configurations."""
    # Valid config
    export = {"name": "test_export", "source_table": "project.dataset.table", "destination_filename": "export.csv"}
    # Should not raise an exception
    validate_export_config(export, 0)

    # Missing required field
    export = {
        "name": "test_export",
        "source_table": "project.dataset.table",
        # Missing destination_filename
    }
    with pytest.raises(ConfigError, match="Missing required field 'destination_filename'"):
        validate_export_config(export, 0)

    # Incremental without timestamp
    export = {
        "name": "test_export",
        "source_table": "project.dataset.table",
        "destination_filename": "export.csv",
        "incremental": True,
        # Missing timestamp_column
    }
    with pytest.raises(ConfigError, match="Incremental export requires 'timestamp_column'"):
        validate_export_config(export, 0)


def test_get_sftp_config():
    """Test getting SFTP config with environment variables."""
    config = {"sftp": {"path": "/uploads", "port": 2222}}

    # Mock environment variables
    with patch.dict(os.environ, {"SFTP_HOST": "sftp.example.com", "SFTP_USERNAME": "user", "SFTP_PASSWORD": "pass"}):
        result = get_sftp_config(config)
        assert result["host"] == "sftp.example.com"
        assert result["username"] == "user"
        assert result["password"] == "pass"
        assert result["path"] == "/uploads"
        assert result["port"] == 2222

    # Test missing credentials
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigError, match="SFTP credentials not found"):
            get_sftp_config(config)


def test_load_config_from_data():
    """Test loading config directly from data."""
    config_data = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
    }

    with patch("src.config.validate_config"):
        result = load_config(config_data=config_data)
        assert result == config_data


def test_load_config_from_file():
    """Test loading config from a file."""
    config_data = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
    }

    m = mock_open(read_data=json.dumps(config_data))
    with patch("builtins.open", m), patch("src.config.validate_config"):
        result = load_config(config_file="config.json")
        assert result == config_data


def test_get_metadata_table():
    """Test getting metadata table name."""
    config = {"metadata_table": "project.dataset.metadata"}
    assert get_metadata_table(config) == "project.dataset.metadata"


# Add this test for the default config loading
def test_load_config_default(tmp_path):
    """Test loading config from default location."""
    # Create a mock config dir structure
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    default_config = config_dir / "default.json"

    config_data = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
    }

    default_config.write_text(json.dumps(config_data))

    with patch("src.config.Path.parent", return_value=tmp_path), patch("src.config.validate_config"):
        result = load_config()
        assert result == config_data


# Add test for SFTP config validation
def test_validate_config_sftp():
    """Test validating SFTP configuration."""
    # Valid SFTP config
    config = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
        "sftp": {"path": "/uploads", "port": 22},
    }
    # Should not raise exception
    validate_config(config)

    # Invalid SFTP config (missing path)
    config = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
        "sftp": {"port": 22},  # Missing path
    }
    with pytest.raises(ConfigError, match="'sftp.path' is required"):
        validate_config(config)

    # Invalid SFTP config (not a dict)
    config = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
        "sftp": "not_a_dict",
    }
    with pytest.raises(ConfigError, match="'sftp' must be a dictionary"):
        validate_config(config)


def test_load_config_default():
    """Test loading config from default location."""
    config_data = {
        "exports": [{"name": "test", "source_table": "table", "destination_filename": "file.csv"}],
        "metadata_table": "metadata_table",
    }

    # Mock the open function to return our config data regardless of path
    m = mock_open(read_data=json.dumps(config_data))

    with patch("builtins.open", m), patch("src.config.validate_config"):
        # Mock Path(__file__).parent.parent to avoid path resolution issues
        with patch("pathlib.Path.__truediv__", return_value=MagicMock()):
            result = load_config()
            assert result == config_data

            # Verify open was called
            m.assert_called_once()
