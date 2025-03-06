"""Tests for helper functions."""

import json
import re
from unittest.mock import patch


from src.helpers import cprint


def test_cprint_formats_json_output():
    """Test that cprint outputs correctly formatted JSON with expected fields."""
    with patch("builtins.print") as mock_print:
        cprint("Test message", severity="INFO", extra_field="test_value")

        # Verify print was called once
        assert mock_print.call_count == 1

        # Get the argument that was passed to print
        output = mock_print.call_args[0][0]

        # Parse the JSON output
        parsed = json.loads(output)

        # Verify the expected fields
        assert parsed["message"] == "Test message"
        assert parsed["severity"] == "INFO"
        assert parsed["extra_field"] == "test_value"

        # Verify timestamp is in ISO format
        timestamp_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+"
        assert re.match(timestamp_pattern, parsed["timestamp"])
