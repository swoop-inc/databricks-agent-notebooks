"""Tests for frontmatter parsing, config merging, and the AgentNotebookConfig dataclass."""

from __future__ import annotations

from pathlib import Path

from databricks_agent_notebooks.config.frontmatter import (
    AgentNotebookConfig,
    DatabricksConfig,
    _parse_config_block,
    is_local_master,
    is_local_spark,
    is_serverless_cluster,
    load_frontmatter_source_map,
    merge_config,
    parse_frontmatter,
)


# ---------------------------------------------------------------------------
# Backward-compatible alias
# ---------------------------------------------------------------------------


def test_databricks_config_alias() -> None:
    assert DatabricksConfig is AgentNotebookConfig


# ---------------------------------------------------------------------------
# parse_frontmatter -- agent-notebook: key
# ---------------------------------------------------------------------------


def test_agent_notebook_key_all_fields(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text(
        "---\nagent-notebook:\n  profile: prod\n  cluster: c1\n  language: python\n---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.profile == "prod"
    assert config.cluster == "c1"
    assert config.language == "python"


def test_agent_notebook_key_new_fields(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text(
        "---\nagent-notebook:\n  format: md\n  timeout: 300\n"
        "  allow-errors: true\n  inject-session: false\n"
        "  clean: true\n  preprocess: false\n"
        "  output-dir: out\n"
        "  params:\n    env: staging\n    key: value\n---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.format == "md"
    assert config.timeout == 300
    assert config.allow_errors is True
    assert config.inject_session is False
    assert config.clean is True
    assert config.preprocess is False
    assert config.output_dir == "out"
    assert config.params == {"env": "staging", "key": "value"}


# ---------------------------------------------------------------------------
# parse_frontmatter -- databricks: key (backward compat)
# ---------------------------------------------------------------------------


def test_valid_frontmatter_all_fields(sample_markdown: Path) -> None:
    """Existing databricks: key still works."""
    config = parse_frontmatter(sample_markdown)
    assert config.profile == "nonhealth-prod"
    assert config.cluster == "rnd-alpha"
    assert config.language == "scala"


def test_missing_databricks_key(tmp_path: Path) -> None:
    path = tmp_path / "no_db.md"
    path.write_text("---\ntitle: Hello\n---\n# Body\n", encoding="utf-8")
    config = parse_frontmatter(path)
    assert config == AgentNotebookConfig()


def test_no_frontmatter(sample_markdown_no_frontmatter: Path) -> None:
    config = parse_frontmatter(sample_markdown_no_frontmatter)
    assert config == AgentNotebookConfig()


def test_partial_frontmatter_profile_only(tmp_path: Path) -> None:
    path = tmp_path / "partial.md"
    path.write_text("---\ndatabricks:\n  profile: dev\n---\n# Body\n", encoding="utf-8")
    config = parse_frontmatter(path)
    assert config.profile == "dev"
    assert config.cluster is None
    assert config.language is None


# ---------------------------------------------------------------------------
# parse_frontmatter -- both keys present (agent-notebook wins)
# ---------------------------------------------------------------------------


def test_both_keys_agent_notebook_wins(tmp_path: Path) -> None:
    path = tmp_path / "both.md"
    path.write_text(
        "---\n"
        "databricks:\n  profile: db-profile\n  cluster: db-cluster\n"
        "agent-notebook:\n  profile: an-profile\n"
        "---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.profile == "an-profile"
    # cluster from databricks: is preserved since agent-notebook: doesn't set it
    assert config.cluster == "db-cluster"


def test_both_keys_libraries_override(tmp_path: Path) -> None:
    path = tmp_path / "libs.md"
    path.write_text(
        "---\n"
        "databricks:\n  libraries:\n    - /db/lib\n"
        "agent-notebook:\n  libraries:\n    - /an/lib\n"
        "---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.libraries == ("/an/lib",)


def test_both_keys_params_merge(tmp_path: Path) -> None:
    path = tmp_path / "params.md"
    path.write_text(
        "---\n"
        "databricks:\n  params:\n    a: '1'\n    b: '2'\n"
        "agent-notebook:\n  params:\n    b: '3'\n    c: '4'\n"
        "---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.params == {"a": "1", "b": "3", "c": "4"}


# ---------------------------------------------------------------------------
# merge_config (base, override)
# ---------------------------------------------------------------------------


def test_cli_overrides_frontmatter() -> None:
    base = AgentNotebookConfig(profile="file-profile", cluster="file-cluster")
    override = AgentNotebookConfig(profile="cli-profile", cluster="cli-cluster")
    merged = merge_config(base, override)
    assert merged.profile == "cli-profile"
    assert merged.cluster == "cli-cluster"


def test_base_used_when_override_is_none() -> None:
    base = AgentNotebookConfig(profile="file-profile", cluster="file-cluster", language="scala")
    merged = merge_config(base, AgentNotebookConfig())
    assert merged.profile == "file-profile"
    assert merged.cluster == "file-cluster"
    assert merged.language == "scala"


def test_merge_config_overrides_libraries() -> None:
    base = AgentNotebookConfig(libraries=("/a",))
    override = AgentNotebookConfig(libraries=("/b",))
    merged = merge_config(base, override)
    assert merged.libraries == ("/b",)


def test_merge_config_override_only_libraries() -> None:
    merged = merge_config(AgentNotebookConfig(), AgentNotebookConfig(libraries=("/b",)))
    assert merged.libraries == ("/b",)


def test_merge_config_base_only_libraries() -> None:
    merged = merge_config(AgentNotebookConfig(libraries=("/a",)), AgentNotebookConfig())
    assert merged.libraries == ("/a",)


def test_merge_config_neither_has_libraries() -> None:
    merged = merge_config(AgentNotebookConfig(), AgentNotebookConfig())
    assert merged.libraries is None


def test_merge_config_empty_library_list_overrides_base() -> None:
    """An explicit empty library list replaces base libraries (not a no-op)."""
    base = AgentNotebookConfig(libraries=("/a",))
    override = AgentNotebookConfig(libraries=())
    merged = merge_config(base, override)
    assert merged.libraries == ()


def test_merge_config_params_merge() -> None:
    base = AgentNotebookConfig(params={"a": "1", "b": "2"})
    override = AgentNotebookConfig(params={"b": "3", "c": "4"})
    merged = merge_config(base, override)
    assert merged.params == {"a": "1", "b": "3", "c": "4"}


def test_merge_config_params_none_both() -> None:
    merged = merge_config(AgentNotebookConfig(), AgentNotebookConfig())
    assert merged.params is None


def test_merge_config_params_one_side() -> None:
    merged = merge_config(AgentNotebookConfig(), AgentNotebookConfig(params={"x": "1"}))
    assert merged.params == {"x": "1"}


def test_merge_config_scalars_and_booleans() -> None:
    base = AgentNotebookConfig(format="md", timeout=100, allow_errors=True, inject_session=True)
    override = AgentNotebookConfig(format="html", allow_errors=False)
    merged = merge_config(base, override)
    assert merged.format == "html"
    assert merged.timeout == 100  # base preserved
    assert merged.allow_errors is False
    assert merged.inject_session is True  # base preserved


# ---------------------------------------------------------------------------
# with_defaults
# ---------------------------------------------------------------------------


def test_with_defaults_fills_none() -> None:
    config = AgentNotebookConfig(format="md")
    filled = config.with_defaults(format="all", inject_session=True, clean=False)
    assert filled.format == "md"  # not overwritten
    assert filled.inject_session is True
    assert filled.clean is False


def test_with_defaults_no_change() -> None:
    config = AgentNotebookConfig(format="all")
    assert config.with_defaults(format="md") is config  # same object returned


# ---------------------------------------------------------------------------
# _parse_config_block
# ---------------------------------------------------------------------------


def test_parse_config_block_full() -> None:
    block = {
        "profile": "prod",
        "cluster": "c1",
        "language": "python",
        "libraries": ["/a", "/b"],
        "format": "all",
        "timeout": 600,
        "output-dir": "/out",
        "allow-errors": True,
        "inject-session": False,
        "preprocess": True,
        "clean": False,
        "params": {"k": "v"},
    }
    config = _parse_config_block(block)
    assert config.profile == "prod"
    assert config.libraries == ("/a", "/b")
    assert config.format == "all"
    assert config.timeout == 600
    assert config.output_dir == "/out"
    assert config.allow_errors is True
    assert config.inject_session is False
    assert config.params == {"k": "v"}


def test_parse_config_block_ignores_unknown_keys() -> None:
    config = _parse_config_block({"profile": "dev", "unknown_key": "value"})
    assert config.profile == "dev"


def test_parse_config_block_type_checks() -> None:
    """Wrong types are silently ignored."""
    config = _parse_config_block({
        "timeout": "not-an-int",
        "allow-errors": "yes",
        "libraries": "not-a-list",
        "params": ["not", "a", "dict"],
    })
    assert config == AgentNotebookConfig()


# ---------------------------------------------------------------------------
# is_local_spark
# ---------------------------------------------------------------------------


def test_is_local_spark_true() -> None:
    assert is_local_spark(AgentNotebookConfig(profile="LOCAL_SPARK")) is True


def test_is_local_spark_case_insensitive() -> None:
    assert is_local_spark(AgentNotebookConfig(profile="local_spark")) is True
    assert is_local_spark(AgentNotebookConfig(profile="Local_Spark")) is True
    assert is_local_spark(AgentNotebookConfig(profile="LOCAL_spark")) is True


def test_is_local_spark_false_for_regular_profile() -> None:
    assert is_local_spark(AgentNotebookConfig(profile="dev")) is False
    assert is_local_spark(AgentNotebookConfig(profile="nonhealth-prod")) is False


def test_is_local_spark_false_for_none() -> None:
    assert is_local_spark(AgentNotebookConfig()) is False
    assert is_local_spark(AgentNotebookConfig(profile=None)) is False


# ---------------------------------------------------------------------------
# Libraries (frontmatter parsing)
# ---------------------------------------------------------------------------


def test_frontmatter_parses_libraries(tmp_path: Path) -> None:
    path = tmp_path / "libs.md"
    path.write_text(
        "---\nagent-notebook:\n  profile: dev\n  libraries:\n    - /abs/path/lib\n    - ../rel/lib\n---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.libraries == ("/abs/path/lib", "../rel/lib")


def test_frontmatter_libraries_non_list_ignored(tmp_path: Path) -> None:
    path = tmp_path / "bad.md"
    path.write_text(
        "---\nagent-notebook:\n  libraries: not-a-list\n---\n# Body\n",
        encoding="utf-8",
    )
    config = parse_frontmatter(path)
    assert config.libraries is None


def test_frontmatter_libraries_absent(tmp_path: Path) -> None:
    path = tmp_path / "no_libs.md"
    path.write_text("---\nagent-notebook:\n  profile: dev\n---\n# Body\n", encoding="utf-8")
    config = parse_frontmatter(path)
    assert config.libraries is None


# ---------------------------------------------------------------------------
# Three-way merge (project < frontmatter < CLI)
# ---------------------------------------------------------------------------


def test_three_way_merge_profile() -> None:
    project = AgentNotebookConfig(profile="proj")
    frontmatter = AgentNotebookConfig(profile="fm")
    cli = AgentNotebookConfig(profile="cli")
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.profile == "cli"


def test_three_way_merge_fallthrough() -> None:
    project = AgentNotebookConfig(profile="proj", timeout=100)
    frontmatter = AgentNotebookConfig()
    cli = AgentNotebookConfig()
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.profile == "proj"
    assert result.timeout == 100


def test_three_way_merge_libraries_override() -> None:
    project = AgentNotebookConfig(libraries=("/proj",))
    frontmatter = AgentNotebookConfig(libraries=("/fm",))
    cli = AgentNotebookConfig(libraries=("/cli",))
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.libraries == ("/cli",)


def test_three_way_merge_libraries_mid_level_wins() -> None:
    """When project and frontmatter set libraries but CLI does not,
    frontmatter wins (it overrides project), and CLI preserves it."""
    project = AgentNotebookConfig(libraries=("/proj",))
    frontmatter = AgentNotebookConfig(libraries=("/fm",))
    cli = AgentNotebookConfig()
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.libraries == ("/fm",)


def test_three_way_merge_params() -> None:
    project = AgentNotebookConfig(params={"a": "1", "b": "2"})
    frontmatter = AgentNotebookConfig(params={"b": "3"})
    cli = AgentNotebookConfig(params={"c": "4"})
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.params == {"a": "1", "b": "3", "c": "4"}


def test_three_way_merge_booleans() -> None:
    project = AgentNotebookConfig(allow_errors=True, clean=False)
    frontmatter = AgentNotebookConfig(clean=True)
    cli = AgentNotebookConfig()
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.allow_errors is True
    assert result.clean is True


def test_three_way_merge_mixed_sources() -> None:
    project = AgentNotebookConfig(profile="proj", format="md", libraries=("/proj",))
    frontmatter = AgentNotebookConfig(timeout=300)
    cli = AgentNotebookConfig(profile="cli")
    result = merge_config(merge_config(project, frontmatter), cli)
    assert result.profile == "cli"
    assert result.format == "md"
    assert result.timeout == 300
    assert result.libraries == ("/proj",)


# ---------------------------------------------------------------------------
# Float timeout (F4 regression test)
# ---------------------------------------------------------------------------


def test_parse_config_block_accepts_float_timeout() -> None:
    """TOML timeout = 300.0 (float) should be accepted and truncated to int."""
    config = _parse_config_block({"timeout": 300.0})
    assert config.timeout == 300
    assert isinstance(config.timeout, int)


def test_parse_config_block_accepts_int_timeout() -> None:
    config = _parse_config_block({"timeout": 300})
    assert config.timeout == 300


def test_parse_config_block_rejects_bool_timeout() -> None:
    """bool is a subclass of int; timeout should not accept True/False."""
    config = _parse_config_block({"timeout": True})
    assert config.timeout is None


# ---------------------------------------------------------------------------
# is_local_master
# ---------------------------------------------------------------------------


def test_is_local_master_basic_patterns() -> None:
    assert is_local_master("local") is True
    assert is_local_master("local[*]") is True
    assert is_local_master("local[2]") is True
    assert is_local_master("local[4,2]") is True


def test_is_local_master_rejects_non_local() -> None:
    assert is_local_master("my-cluster") is False
    assert is_local_master("local-cluster[2,1g,1g]") is False
    assert is_local_master("SERVERLESS") is False
    assert is_local_master("") is False


def test_is_local_master_none() -> None:
    assert is_local_master(None) is False


# ---------------------------------------------------------------------------
# is_serverless_cluster
# ---------------------------------------------------------------------------


def test_is_serverless_cluster_case_insensitive() -> None:
    assert is_serverless_cluster("SERVERLESS") is True
    assert is_serverless_cluster("serverless") is True
    assert is_serverless_cluster("Serverless") is True


def test_is_serverless_cluster_rejects_non_serverless() -> None:
    assert is_serverless_cluster("my-cluster") is False
    assert is_serverless_cluster("local[*]") is False
    assert is_serverless_cluster("") is False


def test_is_serverless_cluster_none() -> None:
    assert is_serverless_cluster(None) is False


# ---------------------------------------------------------------------------
# load_frontmatter_source_map
# ---------------------------------------------------------------------------


def test_load_frontmatter_source_map_agent_notebook_key(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text(
        "---\nagent-notebook:\n  profile: prod\n  cluster: c1\n---\n# Body\n",
        encoding="utf-8",
    )
    result = load_frontmatter_source_map(path)
    assert result == {"profile": "prod", "cluster": "c1"}


def test_load_frontmatter_source_map_databricks_key(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text(
        "---\ndatabricks:\n  profile: dev\n---\n# Body\n",
        encoding="utf-8",
    )
    result = load_frontmatter_source_map(path)
    assert result == {"profile": "dev"}


def test_load_frontmatter_source_map_both_keys_merged(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text(
        "---\n"
        "databricks:\n  profile: db-profile\n  cluster: db-cluster\n"
        "agent-notebook:\n  profile: an-profile\n"
        "---\n# Body\n",
        encoding="utf-8",
    )
    result = load_frontmatter_source_map(path)
    assert result["profile"] == "an-profile"
    assert result["cluster"] == "db-cluster"


def test_load_frontmatter_source_map_both_keys_libraries_override(tmp_path: Path) -> None:
    """When both keys have libraries, agent-notebook replaces databricks."""
    path = tmp_path / "nb.md"
    path.write_text(
        "---\n"
        "databricks:\n  libraries:\n    - /db/lib\n"
        "agent-notebook:\n  libraries:\n    - /an/lib\n"
        "---\n# Body\n",
        encoding="utf-8",
    )
    result = load_frontmatter_source_map(path)
    assert result["libraries"] == ["/an/lib"]


def test_load_frontmatter_source_map_no_frontmatter(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text("# Body\n", encoding="utf-8")
    result = load_frontmatter_source_map(path)
    assert result == {}


def test_load_frontmatter_source_map_no_recognized_keys(tmp_path: Path) -> None:
    path = tmp_path / "nb.md"
    path.write_text("---\ntitle: Hello\n---\n# Body\n", encoding="utf-8")
    result = load_frontmatter_source_map(path)
    assert result == {}


def test_load_frontmatter_source_map_preserves_environments(tmp_path: Path) -> None:
    """Source map should preserve environments and params as raw dicts."""
    path = tmp_path / "nb.md"
    path.write_text(
        "---\n"
        "agent-notebook:\n"
        "  profile: dev\n"
        "  params:\n"
        "    region: us-east-1\n"
        "---\n# Body\n",
        encoding="utf-8",
    )
    result = load_frontmatter_source_map(path)
    assert result["profile"] == "dev"
    assert result["params"] == {"region": "us-east-1"}


# ---------------------------------------------------------------------------
# load_frontmatter_source_map -- hyphenated key normalization (FIX 2)
# ---------------------------------------------------------------------------


def test_frontmatter_source_map_normalizes_hyphenated_keys(tmp_path: Path) -> None:
    """Hyphenated YAML keys should be normalized to underscored in the source map."""
    path = tmp_path / "nb.md"
    path.write_text(
        "---\nagent-notebook:\n  inject-session: false\n  allow-errors: true\n  output-dir: out\n---\n# Body\n",
        encoding="utf-8",
    )
    source = load_frontmatter_source_map(path)
    assert "inject_session" in source
    assert "allow_errors" in source
    assert "output_dir" in source
    assert source["inject_session"] is False
