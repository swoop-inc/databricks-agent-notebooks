"""Tests for YAML frontmatter parsing and config merging."""

from __future__ import annotations

from pathlib import Path

from databricks_agent_notebooks.config.frontmatter import (
    DatabricksConfig,
    is_local_spark,
    merge_config,
    parse_frontmatter,
)


def test_valid_frontmatter_all_fields(sample_markdown: Path) -> None:
    config = parse_frontmatter(sample_markdown)
    assert config.profile == "nonhealth-prod"
    assert config.cluster == "rnd-alpha"
    assert config.language == "scala"


def test_missing_databricks_key(tmp_path: Path) -> None:
    path = tmp_path / "no_db.md"
    path.write_text("---\ntitle: Hello\n---\n# Body\n", encoding="utf-8")
    config = parse_frontmatter(path)
    assert config == DatabricksConfig()


def test_no_frontmatter(sample_markdown_no_frontmatter: Path) -> None:
    config = parse_frontmatter(sample_markdown_no_frontmatter)
    assert config == DatabricksConfig()


def test_partial_frontmatter_profile_only(tmp_path: Path) -> None:
    path = tmp_path / "partial.md"
    path.write_text("---\ndatabricks:\n  profile: dev\n---\n# Body\n", encoding="utf-8")

    config = parse_frontmatter(path)

    assert config.profile == "dev"
    assert config.cluster is None
    assert config.language is None


def test_cli_overrides_frontmatter() -> None:
    frontmatter = DatabricksConfig(profile="file-profile", cluster="file-cluster")
    merged = merge_config(frontmatter, cli_profile="cli-profile", cli_cluster="cli-cluster")

    assert merged.profile == "cli-profile"
    assert merged.cluster == "cli-cluster"


def test_frontmatter_used_when_cli_is_none() -> None:
    frontmatter = DatabricksConfig(profile="file-profile", cluster="file-cluster", language="scala")
    merged = merge_config(frontmatter)

    assert merged.profile == "file-profile"
    assert merged.cluster == "file-cluster"
    assert merged.language == "scala"


# ---------------------------------------------------------------------------
# is_local_spark
# ---------------------------------------------------------------------------


def test_is_local_spark_true() -> None:
    assert is_local_spark(DatabricksConfig(profile="LOCAL_SPARK")) is True


def test_is_local_spark_case_insensitive() -> None:
    assert is_local_spark(DatabricksConfig(profile="local_spark")) is True
    assert is_local_spark(DatabricksConfig(profile="Local_Spark")) is True
    assert is_local_spark(DatabricksConfig(profile="LOCAL_spark")) is True


def test_is_local_spark_false_for_regular_profile() -> None:
    assert is_local_spark(DatabricksConfig(profile="dev")) is False
    assert is_local_spark(DatabricksConfig(profile="nonhealth-prod")) is False


def test_is_local_spark_false_for_none() -> None:
    assert is_local_spark(DatabricksConfig()) is False
    assert is_local_spark(DatabricksConfig(profile=None)) is False
