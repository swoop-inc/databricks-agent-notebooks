"""Shared fixtures for integration tests."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"
NOTEBOOKS_DIR = FIXTURES_DIR / "notebooks"


def _is_machine_real_mode() -> bool:
    """Determine if tests should use the real machine install location.

    Machine-real mode is active when:
    - CI=true (GitHub Actions — clean VM, no isolation needed)
    - AGENT_NOTEBOOK_TEST_MACHINE_INSTALL=1 (local opt-in)
    """
    if os.environ.get("CI", "").lower() == "true":
        return True
    return os.environ.get("AGENT_NOTEBOOK_TEST_MACHINE_INSTALL") == "1"


def _coursier_available() -> bool:
    """Check if coursier (``coursier`` or ``cs``) is available on PATH."""
    return shutil.which("coursier") is not None or shutil.which("cs") is not None


def _java_available() -> bool:
    """Check if a JDK is available on PATH."""
    return shutil.which("java") is not None


@pytest.fixture(scope="module")
def runtime_home(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Resolve DATABRICKS_AGENT_NOTEBOOKS_HOME for test isolation.

    In isolated mode (default locally): creates a temp dir and sets the env var.
    In machine-real mode (CI or opt-in): uses the real location.
    """
    if _is_machine_real_mode():
        # Use whatever the tool resolves to naturally
        from databricks_agent_notebooks.runtime.home import (
            ensure_runtime_home,
            resolve_runtime_home,
        )

        home = resolve_runtime_home()
        ensure_runtime_home(home)
        yield home.root
    else:
        tmp = tmp_path_factory.mktemp("agent-notebook-home")
        original = os.environ.get("DATABRICKS_AGENT_NOTEBOOKS_HOME")
        os.environ["DATABRICKS_AGENT_NOTEBOOKS_HOME"] = str(tmp)
        yield tmp
        # Restore original
        if original is not None:
            os.environ["DATABRICKS_AGENT_NOTEBOOKS_HOME"] = original
        else:
            os.environ.pop("DATABRICKS_AGENT_NOTEBOOKS_HOME", None)


@pytest.fixture(scope="session")
def has_coursier() -> bool:
    """Whether coursier is available for Scala kernel tests."""
    return _coursier_available()


@pytest.fixture(scope="session")
def has_java() -> bool:
    """Whether a JDK is available."""
    return _java_available()


skip_without_coursier = pytest.mark.skipif(
    not _coursier_available(),
    reason="coursier (cs) not found on PATH",
)

skip_without_java = pytest.mark.skipif(
    not _java_available(),
    reason="java not found on PATH",
)
