"""Project-level configuration from ``pyproject.toml``.

Discovers ``[tool.agent-notebook]`` sections by walking up from a starting
directory to the nearest ``.git`` boundary.  Library and output-dir paths
are resolved relative to the pyproject.toml's parent directory at parse time
so that they remain stable regardless of which notebook is executed.

Uses only stdlib: :mod:`tomllib` (Python >= 3.11) + :mod:`pathlib`.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from databricks_agent_notebooks.config.file_refs import resolve_file_refs
from databricks_agent_notebooks.config.frontmatter import (
    AgentNotebookConfig,
    _normalize_source_keys,
    _parse_config_block,
)


def _resolve_paths(config: AgentNotebookConfig, base_dir: Path) -> AgentNotebookConfig:
    """Resolve relative library and output-dir paths against *base_dir*.

    Already-absolute paths are left unchanged.  Returns a new config
    with resolved paths.
    """
    from dataclasses import replace

    changes: dict[str, object] = {}

    if config.libraries:
        resolved_libs: list[str] = []
        for lib in config.libraries:
            p = Path(lib)
            if not p.is_absolute():
                p = (base_dir / p).resolve()
            else:
                p = p.resolve()
            # src/ layout auto-detection (same logic as cli._resolve_library_paths)
            if p.is_dir() and (p / "pyproject.toml").is_file() and (p / "src").is_dir():
                p = p / "src"
            resolved_libs.append(str(p))
        changes["libraries"] = tuple(resolved_libs)

    if config.output_dir and not Path(config.output_dir).is_absolute():
        changes["output_dir"] = str((base_dir / config.output_dir).resolve())

    return replace(config, **changes) if changes else config


def load_project_source_map(start: Path) -> tuple[dict[str, Any], Path | None]:
    """Discover ``[tool.agent-notebook]`` and return the raw section dict.

    Same directory-walk logic as :func:`find_project_config` (walk up to
    ``.git`` boundary), but returns the unprocessed TOML dict preserving
    ``environments``, ``params``, and all other keys.

    Returns ``({}, None)`` when no matching ``pyproject.toml`` is found.
    The second element is the ``pyproject.toml``'s parent directory (used
    for relative path resolution downstream).
    """
    current = start.resolve()

    while True:
        candidate = current / "pyproject.toml"
        if candidate.is_file():
            with open(candidate, "rb") as f:
                data = tomllib.load(f)

            section = data.get("tool", {}).get("agent-notebook")
            if isinstance(section, dict):
                # Resolve -FILE keys before normalization (normalization
                # converts hyphens to underscores, destroying the suffix).
                resolved = resolve_file_refs(dict(section), candidate.parent)
                normalized = _normalize_source_keys(resolved)
                # Resolve relative paths against TOML base dir
                if "output_dir" in normalized and not Path(normalized["output_dir"]).is_absolute():
                    normalized["output_dir"] = str((candidate.parent / normalized["output_dir"]).resolve())
                if "libraries" in normalized and isinstance(normalized["libraries"], list):
                    resolved_libs = []
                    for lib in normalized["libraries"]:
                        p = Path(lib)
                        if not p.is_absolute():
                            p = (candidate.parent / p).resolve()
                        resolved_libs.append(str(p))
                    normalized["libraries"] = resolved_libs
                return normalized, candidate.parent

        if (current / ".git").exists():
            break

        parent = current.parent
        if parent == current:
            break
        current = parent

    return {}, None


def find_project_config(start: Path) -> AgentNotebookConfig:
    """Discover ``[tool.agent-notebook]`` in the nearest ``pyproject.toml``.

    Walks up from *start* (typically the notebook file's parent directory).
    At each level:

    1. Check for ``pyproject.toml`` containing ``[tool.agent-notebook]``.
    2. If found, parse it and return.  If the file exists but has no
       ``[tool.agent-notebook]`` section, continue walking up.
    3. Stop at a ``.git`` directory boundary (inclusive -- that directory
       is checked before stopping).

    Relative ``libraries`` and ``output-dir`` paths are resolved against
    the pyproject.toml's parent directory.

    Returns a default (all-None) config if no matching file is found.
    Lets :exc:`tomllib.TOMLDecodeError` propagate for malformed TOML.
    """
    current = start.resolve()

    while True:
        candidate = current / "pyproject.toml"
        if candidate.is_file():
            with open(candidate, "rb") as f:
                data = tomllib.load(f)

            section = data.get("tool", {}).get("agent-notebook")
            if isinstance(section, dict):
                config = _parse_config_block(section)
                return _resolve_paths(config, candidate.parent)

        # Stop if we've hit a .git boundary (after checking this directory)
        if (current / ".git").exists():
            break

        parent = current.parent
        if parent == current:
            # Filesystem root
            break
        current = parent

    return AgentNotebookConfig()
