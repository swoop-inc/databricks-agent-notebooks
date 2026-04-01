"""Local Spark execution tests — notebook execution with local PySpark.

Tests two modes:
1. ``--no-inject-session`` with notebooks that create their own local SparkSession.
2. ``--profile LOCAL_SPARK`` which injects a local SparkSession automatically.

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


_pyspark_marks = [
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


def _run_notebook_with_profile(
    notebook: str,
    *extra_args: str,
    output_dir: Path,
    profile: str = "LOCAL_SPARK",
    fmt: str = "md",
    timeout: int = 120,
    check: bool = False,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a notebook via agent-notebook run --profile (session injected)."""
    import os

    notebook_path = str(FIXTURES / notebook)
    cmd = [
        sys.executable, "-c",
        "import sys; from databricks_agent_notebooks.cli import main; sys.exit(main())",
        "run", notebook_path,
        "--profile", profile,
        "--output-dir", str(output_dir),
        "--format", fmt,
        *extra_args,
    ]
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
        env=env,
    )


class TestLocalSparkPython:
    """Python notebook execution with local PySpark."""

    pytestmark = _pyspark_marks

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


class TestLocalSparkProfile:
    """Tests for --profile LOCAL_SPARK automatic session injection."""

    pytestmark = _pyspark_marks

    def test_profile_injects_and_executes(self, tmp_path: Path) -> None:
        """LOCAL_SPARK profile injects a local SparkSession and executes successfully."""
        result = _run_notebook_with_profile(
            "local_spark_profile_python.md",
            output_dir=tmp_path,
        )
        assert result.returncode == 0, (
            f"LOCAL_SPARK profile execution failed (rc={result.returncode}):\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        assert "count=10" in content, (
            f"Expected 'count=10' in output:\n{content}"
        )

    def test_profile_with_cluster_fails(self, tmp_path: Path) -> None:
        """LOCAL_SPARK + --cluster is mutually exclusive and returns exit 1."""
        result = _run_notebook_with_profile(
            "local_spark_profile_python.md",
            "--cluster", "foo",
            output_dir=tmp_path,
        )
        assert result.returncode == 1
        assert "mutually exclusive" in result.stderr

    def test_profile_custom_master(self, tmp_path: Path) -> None:
        """Custom master via AGENT_NOTEBOOK_LOCAL_SPARK_MASTER env var."""
        result = _run_notebook_with_profile(
            "local_spark_profile_python.md",
            output_dir=tmp_path,
            env_overrides={"AGENT_NOTEBOOK_LOCAL_SPARK_MASTER": "local[2]"},
        )
        assert result.returncode == 0, (
            f"LOCAL_SPARK with custom master failed (rc={result.returncode}):\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        assert "count=10" in content


class TestLocalSparkScalaProfile:
    """Scala notebook execution with LOCAL_SPARK profile via $ivy-imported Spark."""

    pytestmark = [pytest.mark.spark, pytest.mark.slow, pytest.mark.scala]

    def test_scala_profile_injects_and_executes(self, tmp_path: Path) -> None:
        """LOCAL_SPARK profile injects a local SparkSession via $ivy and executes."""
        import os

        env_overrides: dict[str, str] = {}
        spark_version = os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_VERSION")
        if spark_version:
            env_overrides["AGENT_NOTEBOOK_LOCAL_SPARK_VERSION"] = spark_version

        result = _run_notebook_with_profile(
            "local_spark_profile_scala.md",
            "--language", "scala",
            output_dir=tmp_path,
            env_overrides=env_overrides or None,
            timeout=300,
        )
        assert result.returncode == 0, (
            f"Scala LOCAL_SPARK execution failed (rc={result.returncode}):\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

        md_files = list(tmp_path.rglob("*.md"))
        assert md_files, f"No markdown output in {tmp_path}"
        content = md_files[0].read_text()
        assert "count=10" in content, (
            f"Expected 'count=10' in output:\n{content}"
        )
