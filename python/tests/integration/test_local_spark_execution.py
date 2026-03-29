"""Local Spark execution tests — notebook execution with local PySpark.

Run ``agent-notebook run --no-inject-session`` with notebooks that create
their own local SparkSession. Tests the full execution pipeline without
Databricks connectivity.

Requires PySpark installed in the current environment.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import NOTEBOOKS_DIR

FIXTURES = NOTEBOOKS_DIR


def _pyspark_available() -> bool:
    """Check if PySpark is importable."""
    try:
        import pyspark  # noqa: F401

        return True
    except ImportError:
        return False


pytestmark = [
    pytest.mark.spark,
    pytest.mark.slow,
    pytest.mark.skipif(
        not _pyspark_available(),
        reason="PySpark not installed",
    ),
]


def _run_notebook(
    notebook: str,
    *extra_args: str,
    output_dir: Path,
    fmt: str = "md",
    timeout: int = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a notebook via agent-notebook run --no-inject-session."""
    notebook_path = str(FIXTURES / notebook)
    cmd = [
        sys.executable, "-c",
        "import sys; from databricks_agent_notebooks.cli import main; sys.exit(main())",
        "run", notebook_path,
        "--no-inject-session",
        "--output-dir", str(output_dir),
        "--format", fmt,
        *extra_args,
    ]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )


class TestLocalSparkPython:
    """Python notebook execution with local PySpark."""

    def test_local_spark_count(self, tmp_path: Path) -> None:
        """Self-contained notebook with local SparkSession and spark.range(10).count()."""
        result = _run_notebook(
            "local_spark_python.md",
            output_dir=tmp_path,
        )
        assert result.returncode == 0, (
            f"Local Spark execution failed (rc={result.returncode}):\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

        # Check rendered output contains expected result
        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}: {list(tmp_path.iterdir())}"
        content = md_files[0].read_text()
        assert "count=10" in content, (
            f"Expected 'count=10' in output:\n{content}"
        )

    def test_local_spark_html_output(self, tmp_path: Path) -> None:
        """Verify HTML rendering works for local Spark notebooks."""
        result = _run_notebook(
            "local_spark_python.md",
            output_dir=tmp_path,
            fmt="html",
        )
        assert result.returncode == 0, (
            f"Local Spark execution failed (rc={result.returncode}):\n{result.stderr}"
        )

        html_files = list(tmp_path.rglob("*.html"))
        assert html_files, f"No HTML output in {tmp_path}: {list(tmp_path.iterdir())}"
        content = html_files[0].read_text()
        assert "count=10" in content, (
            f"Expected 'count=10' in HTML output"
        )
