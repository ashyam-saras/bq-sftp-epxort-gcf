"""
Helper functions used throughout the BigQuery to SFTP export project.
"""

import json
from datetime import datetime
from typing import Any, Dict


def cprint(message: str, severity: str = "DEBUG", **kwargs: Any) -> None:
    """
    Cloud logging wrapper with timestamp and structured output.

    Args:
        message: Main log message
        severity: Log level (DEBUG, INFO, WARNING, ERROR), defaults to DEBUG
        **kwargs: Additional fields to include in log entry
    """
    entry: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "severity": severity.upper(),
        "message": message,
        **kwargs,
    }
    print(json.dumps(entry))
