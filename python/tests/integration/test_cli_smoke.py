"""Fast CLI smoke tests — exercise every subcommand without side effects.

These tests run the real ``agent-notebook`` CLI as a subprocess. No mocks.
They verify that the tool starts, parses arguments, and exits cleanly.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import NOTEBOOKS_DIR

pytestmark = pytest.mark.integration

# Every top-level subcommand and notable sub-subcommand.
HELP_SUBCOMMANDS = [
    [],  # bare --help
    ["run"],
    ["clusters"],
    ["install-kernel"],
    ["kernels"],
    ["kernels", "install"],
    ["kernels", "list"],
    ["kernels", "remove"],
    ["runtimes"],
    ["runtimes", "list"],
    ["render"],
    ["doctor"],
    ["help"],
]


def _run_cli(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run agent-notebook via its entry point to avoid PATH dependency.

    The cli module has no ``if __name__ == '__main__'`` guard, so we invoke
    ``main()`` explicitly via ``python -c``.
    """
    return subprocess.run(
        [
            sys.executable, "-c",
            "import sys; from databricks_agent_notebooks.cli import main; sys.exit(main())",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=check,
    )


class TestHelpFlags:
    """Every subcommand should accept --help and exit 0."""

    @pytest.mark.parametrize(
        "subcommand",
        HELP_SUBCOMMANDS,
        ids=[" ".join(s) if s else "root" for s in HELP_SUBCOMMANDS],
    )
    def test_help_exits_cleanly(self, subcommand: list[str]) -> None:
        result = _run_cli(*subcommand, "--help")
        assert result.returncode == 0
        assert "usage:" in result.stdout.lower() or "usage:" in result.stderr.lower()


class TestDoctor:
    """doctor should complete without crashing, even if checks report issues."""

    def test_doctor_runs(self) -> None:
        result = _run_cli("doctor", check=False)
        # doctor may exit non-zero if checks fail, but it should not crash
        assert result.returncode in (0, 1), (
            f"doctor crashed (rc={result.returncode}):\n{result.stderr}"
        )


class TestRuntimesList:
    """runtimes list should run without error."""

    def test_runtimes_list_runs(self) -> None:
        result = _run_cli("runtimes", "list", check=False)
        # May have nothing to list, but should not crash
        assert result.returncode == 0, (
            f"runtimes list failed (rc={result.returncode}):\n{result.stderr}"
        )


class TestRender:
    """render should produce output files from a pre-executed notebook."""

    def test_render_markdown(self, tmp_path: Path) -> None:
        src = NOTEBOOKS_DIR / "render_test.ipynb"
        assert src.exists(), f"Fixture not found: {src}"

        result = _run_cli(
            "render", str(src), "--format", "md", "--output-dir", str(tmp_path),
        )
        assert result.returncode == 0, (
            f"render failed (rc={result.returncode}):\n{result.stderr}"
        )

        md_files = list(tmp_path.glob("*.md"))
        assert len(md_files) >= 1, f"Expected .md output in {tmp_path}, got: {list(tmp_path.iterdir())}"

    def test_render_html(self, tmp_path: Path) -> None:
        src = NOTEBOOKS_DIR / "render_test.ipynb"
        assert src.exists(), f"Fixture not found: {src}"

        result = _run_cli(
            "render", str(src), "--format", "html", "--output-dir", str(tmp_path),
        )
        assert result.returncode == 0, (
            f"render failed (rc={result.returncode}):\n{result.stderr}"
        )

        html_files = list(tmp_path.glob("*.html"))
        assert len(html_files) >= 1, f"Expected .html output in {tmp_path}, got: {list(tmp_path.iterdir())}"


class TestVersion:
    """--version should print a version string."""

    def test_version_flag(self) -> None:
        result = _run_cli("--version")
        assert result.returncode == 0
        assert "agent-notebook" in result.stdout.lower() or result.stdout.strip()
