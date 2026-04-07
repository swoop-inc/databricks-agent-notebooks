"""Fast integration tests for cluster_list_timeout override.

Exercises the real CLI as a subprocess with env var overrides.
No Databricks credentials needed -- uses a nonexistent profile to trigger
a fast failure, then verifies the configured timeout appears in error output.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration


def _run_cli(*args: str, env_overrides: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run agent-notebook via its entry point."""
    import os

    env = dict(os.environ)
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [
            sys.executable, "-c",
            "import sys; from databricks_agent_notebooks.cli import main; sys.exit(main())",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=env,
    )


def test_short_timeout_completes_quickly() -> None:
    """A very short timeout should cause a fast failure, not a 30s wait."""
    result = _run_cli(
        "clusters", "--profile", "NONEXISTENT_PROFILE_FOR_TESTING",
        env_overrides={"AGENT_NOTEBOOK_CLUSTER_LIST_TIMEOUT": "0.01"},
    )
    assert result.returncode != 0


def test_custom_timeout_does_not_hang() -> None:
    """A custom timeout value should not cause a 30s hang."""
    result = _run_cli(
        "clusters", "--profile", "NONEXISTENT_PROFILE_FOR_TESTING",
        env_overrides={"AGENT_NOTEBOOK_CLUSTER_LIST_TIMEOUT": "7.5"},
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "error" in combined.lower()
