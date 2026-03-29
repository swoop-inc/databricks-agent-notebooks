"""Global install test — the exact install path that broke in 0.2.2.

Builds a wheel from source, installs it globally via ``uv tool install``,
and verifies the ``agent-notebook`` CLI resolves and passes basic smoke
tests through the globally installed binary.

Marked ``@pytest.mark.slow`` — ``uv tool install`` is a machine-level
operation; isolation mode doesn't apply here.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

# Path to python/ directory in the repo (relative to this file)
PYTHON_DIR = Path(__file__).resolve().parent.parent.parent


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a wheel from the local source."""
    dist_dir = tmp_path_factory.mktemp("dist")
    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(dist_dir)],
        cwd=str(PYTHON_DIR),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        # Try uv build as fallback
        result = subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", str(dist_dir)],
            cwd=str(PYTHON_DIR),
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )

    wheels = list(dist_dir.glob("*.whl"))
    assert len(wheels) == 1, f"Expected 1 wheel, found: {wheels}"
    return wheels[0]


@pytest.fixture(scope="module")
def global_agent_notebook(built_wheel: Path) -> str:
    """Install the wheel globally via uv tool install and return the binary path."""
    result = subprocess.run(
        [
            "uv", "tool", "install", "--force",
            "--from", str(built_wheel),
            "databricks-agent-notebooks",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, (
        f"uv tool install failed (rc={result.returncode}):\n{result.stdout}\n{result.stderr}"
    )

    # Resolve from uv's tool bin directory to avoid finding a different binary on PATH
    bin_dir_result = subprocess.run(
        ["uv", "tool", "dir", "--bin"],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    bin_dir = Path(bin_dir_result.stdout.strip())
    uv_bin = bin_dir / "agent-notebook"
    if uv_bin.exists():
        path = str(uv_bin)
    else:
        # Fallback: try shutil.which but warn
        path = shutil.which("agent-notebook")
    assert path is not None, (
        f"agent-notebook not found after uv tool install "
        f"(checked {uv_bin} and PATH)"
    )
    return path


class TestGlobalInstall:
    """Verify the globally installed agent-notebook works."""

    def test_help(self, global_agent_notebook: str) -> None:
        result = subprocess.run(
            [global_agent_notebook, "help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "agent-notebook" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_version(self, global_agent_notebook: str) -> None:
        result = subprocess.run(
            [global_agent_notebook, "--version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert result.stdout.strip(), "No version output"

    def test_doctor(self, global_agent_notebook: str) -> None:
        result = subprocess.run(
            [global_agent_notebook, "doctor"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        # doctor may exit non-zero if checks fail, but it should not crash
        assert result.returncode in (0, 1), (
            f"doctor crashed (rc={result.returncode}):\n{result.stderr}"
        )

    def test_runtimes_list(self, global_agent_notebook: str) -> None:
        result = subprocess.run(
            [global_agent_notebook, "runtimes", "list"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"runtimes list failed (rc={result.returncode}):\n{result.stderr}"
        )

    def test_render_help(self, global_agent_notebook: str) -> None:
        result = subprocess.run(
            [global_agent_notebook, "render", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
        assert "usage:" in result.stdout.lower()
