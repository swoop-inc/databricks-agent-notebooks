"""Shared constants for the databricks_agent_notebooks library."""

from __future__ import annotations

# Kernel metadata per language — single source of truth.
# Used by _converter.py, _dbr_source.py, and cli.py to set/read kernel metadata.
KERNELSPECS: dict[str, dict[str, str]] = {
    "python": {
        "name": "python3",
        "display_name": "Python 3",
        "language": "python",
    },
    "scala": {
        "name": "scala212-dbr-connect",
        "display_name": "Scala 2.12 (Databricks Connect)",
        "language": "scala",
    },
    "sql": {
        "name": "python3",
        "display_name": "Python 3 (SQL wrapper)",
        "language": "sql",
    },
}

DATABRICKS_CONNECT_VERSION = "16.4.7"
DATABRICKS_CONNECT_LINE = "16.4"
