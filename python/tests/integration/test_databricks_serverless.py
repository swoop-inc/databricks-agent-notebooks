"""Live Databricks serverless integration tests.

Run ``agent-notebook run`` with full Databricks Connect injection against
the live workspace. These are the tests that would have caught the 0.2.2
execution bugs.

Requires Databricks credentials:
- CI: ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` env vars
- Local: ``AGENT_NOTEBOOK_TEST_PROFILE`` env var (default: ``DEFAULT``)
"""

from __future__ import annotations

import os
import subprocess
import sys
from configparser import ConfigParser
from pathlib import Path

import pytest

from .conftest import NOTEBOOKS_DIR

FIXTURES = NOTEBOOKS_DIR


def _profile_exists(name: str) -> bool:
    """Check if a Databricks CLI profile exists in ~/.databrickscfg."""
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return False
    config = ConfigParser()
    config.read(cfg_path)
    if name == "DEFAULT":
        # ConfigParser treats DEFAULT as a built-in section that always exists,
        # so ``"DEFAULT" in config`` is always True. Check for actual auth fields.
        defaults = config.defaults()
        return bool(defaults.get("host") and defaults.get("token"))
    return name in config


def _has_credentials() -> bool:
    """Check if Databricks credentials are available."""
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return True
    profile = os.environ.get("AGENT_NOTEBOOK_TEST_PROFILE", "DEFAULT")
    return _profile_exists(profile)


pytestmark = [
    pytest.mark.databricks,
    pytest.mark.slow,
    pytest.mark.skipif(
        not _has_credentials(),
        reason="No Databricks credentials available",
    ),
]


@pytest.fixture()
def profile_args() -> list[str]:
    """Return CLI args for Databricks profile selection.

    CI: DATABRICKS_HOST + DATABRICKS_TOKEN env vars (SDK auto-resolves)
    Local: uses AGENT_NOTEBOOK_TEST_PROFILE, defaulting to 'DEFAULT'
    """
    if os.environ.get("DATABRICKS_HOST"):
        return []  # SDK picks up env vars directly
    profile = os.environ.get("AGENT_NOTEBOOK_TEST_PROFILE", "DEFAULT")
    return ["--profile", profile]


def _run_notebook(
    notebook: str,
    *extra_args: str,
    profile_args: list[str],
    output_dir: Path,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a notebook via agent-notebook run."""
    notebook_path = str(FIXTURES / notebook)
    cmd = [
        sys.executable, "-c",
        "import sys; from databricks_agent_notebooks.cli import main; sys.exit(main())",
        "run", notebook_path,
        *profile_args,
        "--output-dir", str(output_dir),
        "--format", "md",
        *extra_args,
    ]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=check,
    )


class TestPythonNotebook:
    """Python notebook execution against live Databricks."""

    def test_smoke_python(self, tmp_path: Path, profile_args: list[str]) -> None:
        """Simple spark.range(10).count() — verify output contains '10'."""
        result = _run_notebook(
            "smoke_python.md",
            profile_args=profile_args,
            output_dir=tmp_path,
        )
        assert result.returncode == 0, (
            f"Execution failed (rc={result.returncode}):\n{result.stderr}"
        )

        # Check rendered output contains the expected value
        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        assert "count=10" in content, f"Expected 'count=10' in output:\n{content}"

    def test_multi_cell(self, tmp_path: Path, profile_args: list[str]) -> None:
        """Multi-cell notebook — verify sequential execution."""
        result = _run_notebook(
            "multi_cell_python.md",
            profile_args=profile_args,
            output_dir=tmp_path,
        )
        assert result.returncode == 0, (
            f"Execution failed (rc={result.returncode}):\n{result.stderr}"
        )

        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        # x=10, y=20, count=20
        assert "x=10" in content, f"Expected 'x=10' in output:\n{content}"
        assert "y=20" in content, f"Expected 'y=20' in output:\n{content}"
        assert "count=20" in content, f"Expected 'count=20' in output:\n{content}"


class TestScalaNotebook:
    """Scala notebook execution against live Databricks."""

    def test_smoke_scala(self, tmp_path: Path, profile_args: list[str]) -> None:
        """Scala spark.range(10).count() — verify Scala kernel + Connect works."""
        result = _run_notebook(
            "smoke_scala.md",
            "--language", "scala",
            profile_args=profile_args,
            output_dir=tmp_path,
        )
        assert result.returncode == 0, (
            f"Scala execution failed (rc={result.returncode}):\n{result.stderr}"
        )

        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        assert "count=10" in content, f"Expected 'count=10' in Scala output:\n{content}"


class TestErrorHandling:
    """Verify error reporting when a notebook cell fails."""

    def test_error_with_allow_errors(self, tmp_path: Path, profile_args: list[str]) -> None:
        """Notebook with intentional error using --allow-errors."""
        result = _run_notebook(
            "error_python.md",
            "--allow-errors",
            profile_args=profile_args,
            output_dir=tmp_path,
            check=False,
        )
        # With --allow-errors, execution should complete (may be non-zero exit)
        # The rendered output should contain evidence of the error
        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        assert "before error" in content, f"Expected 'before error' in output:\n{content}"

    def test_error_without_allow_errors(self, tmp_path: Path, profile_args: list[str]) -> None:
        """Notebook with error should fail without --allow-errors."""
        result = _run_notebook(
            "error_python.md",
            profile_args=profile_args,
            output_dir=tmp_path,
            check=False,
        )
        assert result.returncode != 0, (
            "Expected non-zero exit for erroring notebook without --allow-errors"
        )
