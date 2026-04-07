"""Tests for pyproject.toml project-level config discovery."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from databricks_agent_notebooks.config.frontmatter import AgentNotebookConfig
from databricks_agent_notebooks.config.project import find_project_config, load_project_source_map


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_finds_config_in_same_dir(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "prod"\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.profile == "prod"


def test_finds_config_in_parent_dir(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "prod"\n',
        encoding="utf-8",
    )
    subdir = tmp_path / "notebooks" / "python"
    subdir.mkdir(parents=True)
    config = find_project_config(subdir)
    assert config.profile == "prod"


def test_stops_at_git_boundary(tmp_path: Path) -> None:
    # Parent has config but child has .git -- should not find parent's config
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "parent"\n',
        encoding="utf-8",
    )
    child = tmp_path / "child"
    child.mkdir()
    (child / ".git").mkdir()
    config = find_project_config(child)
    assert config == AgentNotebookConfig()


def test_skips_pyproject_without_section(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.ruff]\nline-length = 100\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config == AgentNotebookConfig()


def test_walks_past_pyproject_without_section(tmp_path: Path) -> None:
    """A pyproject.toml without [tool.agent-notebook] is skipped; walk continues up."""
    root = tmp_path / "root"
    root.mkdir()
    (root / ".git").mkdir()
    (root / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "root"\n',
        encoding="utf-8",
    )
    sub = root / "sub"
    sub.mkdir()
    (sub / "pyproject.toml").write_text(
        '[tool.ruff]\nline-length = 100\n',
        encoding="utf-8",
    )
    config = find_project_config(sub)
    assert config.profile == "root"


def test_no_pyproject_returns_default(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    config = find_project_config(tmp_path)
    assert config == AgentNotebookConfig()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_extracts_all_fields(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\n'
        'profile = "prod"\n'
        'cluster = "c1"\n'
        'language = "python"\n'
        'libraries = ["python/src"]\n'
        'format = "all"\n'
        'timeout = 600\n'
        'output-dir = "output"\n'
        'allow-errors = true\n'
        'inject-session = false\n'
        'preprocess = true\n'
        'clean = false\n'
        '\n[tool.agent-notebook.params]\n'
        'env = "staging"\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.profile == "prod"
    assert config.cluster == "c1"
    assert config.language == "python"
    assert config.format == "all"
    assert config.timeout == 600
    assert config.allow_errors is True
    assert config.inject_session is False
    assert config.preprocess is True
    assert config.clean is False
    assert config.params == {"env": "staging"}


def test_partial_config(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "dev"\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.profile == "dev"
    assert config.cluster is None
    assert config.libraries is None


def test_ignores_unknown_keys(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "dev"\nunknown = "ignored"\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.profile == "dev"


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def test_libraries_resolved_relative_to_pyproject(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nlibraries = ["python/src", "/absolute/path"]\n',
        encoding="utf-8",
    )
    # Create the target so resolution works
    (tmp_path / "python" / "src").mkdir(parents=True)

    config = find_project_config(tmp_path / "notebooks")

    # Relative path should be resolved against pyproject.toml's parent
    assert str(tmp_path / "python" / "src") in config.libraries
    # Absolute path unchanged
    assert "/absolute/path" in config.libraries


def test_output_dir_resolved_relative_to_pyproject(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\noutput-dir = "output"\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.output_dir == str(tmp_path / "output")


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_bad_toml_raises(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        'this is not valid toml [[[',
        encoding="utf-8",
    )
    with pytest.raises(tomllib.TOMLDecodeError):
        find_project_config(tmp_path)


# ---------------------------------------------------------------------------
# src/ auto-detection (F6)
# ---------------------------------------------------------------------------


def test_library_src_layout_auto_detection(tmp_path: Path) -> None:
    """Library paths pointing at a package root with src/ layout should resolve to src/."""
    (tmp_path / ".git").mkdir()
    # Create a package directory with pyproject.toml + src/
    pkg = tmp_path / "mylib"
    pkg.mkdir()
    (pkg / "pyproject.toml").write_text("[project]\nname = 'mylib'\n", encoding="utf-8")
    (pkg / "src").mkdir()

    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nlibraries = ["mylib"]\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.libraries is not None
    assert len(config.libraries) == 1
    assert config.libraries[0].endswith("/src")
    assert "mylib/src" in config.libraries[0]


def test_library_without_src_layout_not_auto_detected(tmp_path: Path) -> None:
    """Library paths without src/ layout should NOT get src/ appended."""
    (tmp_path / ".git").mkdir()
    pkg = tmp_path / "mylib"
    pkg.mkdir()
    # Has pyproject.toml but no src/ dir
    (pkg / "pyproject.toml").write_text("[project]\nname = 'mylib'\n", encoding="utf-8")

    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nlibraries = ["mylib"]\n',
        encoding="utf-8",
    )
    config = find_project_config(tmp_path)
    assert config.libraries is not None
    assert not config.libraries[0].endswith("/src")


# ---------------------------------------------------------------------------
# load_project_source_map
# ---------------------------------------------------------------------------


def test_load_project_source_map_returns_raw_dict(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\n'
        'profile = "prod"\n'
        'libraries = ["python/src"]\n'
        '\n'
        '[tool.agent-notebook.params]\n'
        'region = "us-east-1"\n',
        encoding="utf-8",
    )
    source, base_dir = load_project_source_map(tmp_path)
    assert source["profile"] == "prod"
    # Relative library paths are resolved against pyproject.toml's parent
    assert source["libraries"] == [str((tmp_path / "python" / "src").resolve())]
    assert source["params"] == {"region": "us-east-1"}
    assert base_dir == tmp_path


def test_load_project_source_map_preserves_environments(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\n'
        'profile = "dev"\n'
        '\n'
        '[tool.agent-notebook.environments.staging]\n'
        'cluster = "staging-cluster"\n',
        encoding="utf-8",
    )
    source, base_dir = load_project_source_map(tmp_path)
    assert source["profile"] == "dev"
    assert source["environments"] == {"staging": {"cluster": "staging-cluster"}}
    assert base_dir == tmp_path


def test_load_project_source_map_no_config(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    source, base_dir = load_project_source_map(tmp_path)
    assert source == {}
    assert base_dir is None


def test_load_project_source_map_walks_up(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "root"\n',
        encoding="utf-8",
    )
    subdir = tmp_path / "notebooks" / "python"
    subdir.mkdir(parents=True)
    source, base_dir = load_project_source_map(subdir)
    assert source["profile"] == "root"
    assert base_dir == tmp_path


def test_load_project_source_map_stops_at_git_boundary(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nprofile = "parent"\n',
        encoding="utf-8",
    )
    child = tmp_path / "child"
    child.mkdir()
    (child / ".git").mkdir()
    source, base_dir = load_project_source_map(child)
    assert source == {}
    assert base_dir is None


# ---------------------------------------------------------------------------
# Hyphenated key normalization (FIX 2)
# ---------------------------------------------------------------------------


def test_project_source_map_normalizes_hyphenated_keys(tmp_path: Path) -> None:
    """Hyphenated TOML keys should be normalized to underscored in the source map."""
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\ninject-session = false\nallow-errors = true\n',
        encoding="utf-8",
    )
    source, base = load_project_source_map(tmp_path)
    assert "inject_session" in source
    assert "allow_errors" in source


# ---------------------------------------------------------------------------
# TOML path resolution in load_project_source_map (FIX 4+5)
# ---------------------------------------------------------------------------


def test_project_source_map_resolves_relative_output_dir(tmp_path: Path) -> None:
    """Relative output-dir in TOML should be resolved against pyproject.toml's parent."""
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\noutput-dir = "output"\n',
        encoding="utf-8",
    )
    source, base_dir = load_project_source_map(tmp_path)
    assert source["output_dir"] == str((tmp_path / "output").resolve())


def test_project_source_map_resolves_relative_libraries(tmp_path: Path) -> None:
    """Relative library paths in TOML should be resolved against pyproject.toml's parent."""
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\nlibraries = ["python/src", "/absolute/path"]\n',
        encoding="utf-8",
    )
    source, base_dir = load_project_source_map(tmp_path)
    assert source["libraries"][0] == str((tmp_path / "python" / "src").resolve())
    assert source["libraries"][1] == "/absolute/path"


def test_project_source_map_absolute_output_dir_unchanged(tmp_path: Path) -> None:
    """Absolute output-dir in TOML should be preserved as-is."""
    (tmp_path / ".git").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\noutput-dir = "/absolute/output"\n',
        encoding="utf-8",
    )
    source, base_dir = load_project_source_map(tmp_path)
    assert source["output_dir"] == "/absolute/output"
