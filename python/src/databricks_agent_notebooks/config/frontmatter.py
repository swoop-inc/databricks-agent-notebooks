"""YAML frontmatter parsing for Databricks notebook markdown files.

Extracts Databricks connection configuration (profile, cluster, language)
from ``---`` delimited YAML frontmatter blocks, and merges with CLI overrides.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from databricks_agent_notebooks._constants import LOCAL_SPARK_PROFILE


@dataclass(frozen=True)
class DatabricksConfig:
    """Immutable Databricks connection parameters extracted from frontmatter.

    All fields default to None, meaning "not specified" — callers decide
    what defaults to apply when a field is absent.
    """

    profile: str | None = None
    cluster: str | None = None
    language: str | None = None


_FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---", re.DOTALL)


def parse_frontmatter(path: Path) -> DatabricksConfig:
    """Parse YAML frontmatter from a markdown file and extract Databricks config.

    Looks for a ``---`` delimited YAML block at the very start of the file.
    Within that block, reads the ``databricks:`` mapping for ``profile``,
    ``cluster``, and ``language`` keys.

    Returns a default (all-None) config when:
    - The file has no frontmatter block
    - The frontmatter has no ``databricks:`` key
    - The ``databricks:`` value is not a mapping
    """
    text = path.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(text)
    if match is None:
        return DatabricksConfig()

    try:
        parsed: Any = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return DatabricksConfig()

    if not isinstance(parsed, dict):
        return DatabricksConfig()

    db = parsed.get("databricks")
    if not isinstance(db, dict):
        return DatabricksConfig()

    return DatabricksConfig(
        profile=db.get("profile"),
        cluster=db.get("cluster"),
        language=db.get("language"),
    )


def merge_config(
    frontmatter: DatabricksConfig,
    cli_profile: str | None = None,
    cli_cluster: str | None = None,
    cli_language: str | None = None,
) -> DatabricksConfig:
    """Merge frontmatter config with CLI overrides.

    CLI arguments take precedence: a non-None CLI value always wins.
    When the CLI value is None, the frontmatter value is preserved.
    """
    return DatabricksConfig(
        profile=cli_profile if cli_profile is not None else frontmatter.profile,
        cluster=cli_cluster if cli_cluster is not None else frontmatter.cluster,
        language=cli_language if cli_language is not None else frontmatter.language,
    )


def is_local_spark(config: DatabricksConfig) -> bool:
    """Detect whether *config* specifies the reserved LOCAL_SPARK profile.

    Case-insensitive: ``local_spark``, ``Local_Spark``, ``LOCAL_SPARK``
    all match.
    """
    return config.profile is not None and config.profile.upper() == LOCAL_SPARK_PROFILE
