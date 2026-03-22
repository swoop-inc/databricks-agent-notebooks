"""Execution provenance metadata for notebook runs.

Captures pre- and post-execution context (source path, timestamps, git state,
timing) to support reproducibility and audit trails.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ExecutionLineage:
    """Provenance record for a single notebook execution.

    Pre-execution fields (source_path, timestamp, git_*) are filled by
    ``capture_pre_execution``.  Post-execution fields (duration_seconds,
    cell_timings) are populated after the run completes.
    """

    source_path: str | None = None
    timestamp: str | None = None
    git_branch: str | None = None
    git_commit: str | None = None
    git_dirty: bool = False
    duration_seconds: float | None = None
    cell_timings: list[float] | None = None


def _git_output(*args: str) -> str | None:
    """Run a git command and return stripped stdout, or None on failure."""
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def capture_pre_execution(source_path: Path | None = None) -> ExecutionLineage:
    """Snapshot provenance metadata before notebook execution begins.

    Captures the current UTC timestamp and, when inside a git repository,
    the branch name, commit hash, and dirty-tree status.  Gracefully
    degrades to None for git fields in non-git environments.
    """
    branch = _git_output("rev-parse", "--abbrev-ref", "HEAD")
    commit = _git_output("rev-parse", "--short", "HEAD")

    dirty_output = _git_output("status", "--porcelain")
    git_dirty = bool(dirty_output) if dirty_output is not None else False

    return ExecutionLineage(
        source_path=str(source_path) if source_path is not None else None,
        timestamp=datetime.now(timezone.utc).isoformat(),
        git_branch=branch,
        git_commit=commit,
        git_dirty=git_dirty,
    )


def capture_post_execution(
    lineage: ExecutionLineage,
    duration: float | None = None,
) -> ExecutionLineage:
    """Return a new lineage with post-execution timing filled in.

    This is a v2 stub — currently only sets ``duration_seconds``.
    """
    return replace(lineage, duration_seconds=duration)


def embed_in_notebook(notebook: Any, lineage: ExecutionLineage) -> Any:
    """Embed lineage metadata into a notebook (v2 stub).

    Currently passes the notebook through unchanged.  A future version
    will inject lineage into notebook metadata or a dedicated cell.
    """
    return notebook
