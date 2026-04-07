"""Built-in Jinja2 filters and globals for the preprocessing environment.

These are plain functions registered on the Jinja2 Environment -- not
PreprocessorPlugins.  They provide stdlib-based utilities for common
template operations: JSON parsing, path manipulation, environment variable
access, string splitting, regex, and datetime.
"""

from __future__ import annotations

import datetime as _datetime
import json
import os
import os.path
import re as _re

import jinja2

from databricks_agent_notebooks.preprocessing.errors import PreprocessorError


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _fromjson(value: str) -> object:
    """Parse a JSON string into a Python object."""
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError) as exc:
        raise PreprocessorError(
            None,
            f"Invalid JSON: {exc}",
            detail={"value": str(value)[:200]},
        ) from exc


def _split(value: str, sep: str | None = None) -> list[str]:
    """Split a string by *sep* (default: whitespace)."""
    return value.split(sep)


def _regex_search(value: str, pattern: str) -> str:
    """Return first capture group if any, else full match. No match -> ""."""
    try:
        m = _re.search(pattern, value)
    except _re.error as exc:
        raise PreprocessorError(
            None,
            f"Invalid regex pattern: {exc}",
            detail={"pattern": pattern[:200]},
        ) from exc
    if m is None:
        return ""
    if m.lastindex:
        return m.group(1)
    return m.group(0)


def _regex_replace(value: str, pattern: str, replacement: str) -> str:
    """Replace all occurrences of *pattern* in *value* with *replacement*."""
    try:
        return _re.sub(pattern, replacement, value)
    except _re.error as exc:
        raise PreprocessorError(
            None,
            f"Invalid regex pattern: {exc}",
            detail={"pattern": pattern[:200]},
        ) from exc


# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------


def _env(key: str, default: str = "") -> str:
    """Look up an environment variable, returning *default* if unset."""
    return os.environ.get(key, default)


def _now() -> _datetime.datetime:
    """Return the current local datetime."""
    return _datetime.datetime.now()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def install_builtins(env: jinja2.Environment) -> None:
    """Register all built-in filters and globals on *env*."""
    # Filters
    env.filters["fromjson"] = _fromjson
    env.filters["basename"] = os.path.basename
    env.filters["dirname"] = os.path.dirname
    env.filters["split"] = _split
    env.filters["regex_search"] = _regex_search
    env.filters["regex_replace"] = _regex_replace

    # Globals
    env.globals["env"] = _env
    env.globals["now"] = _now
