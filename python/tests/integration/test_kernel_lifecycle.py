"""Kernel installation lifecycle tests — real coursier/Almond downloads.

These tests install real Scala kernels using coursier and Almond, then
verify the installed artifacts (kernel.json, launcher contract, runtime
receipts). They use an isolated DATABRICKS_AGENT_NOTEBOOKS_HOME by
default to avoid polluting the developer's machine.

Marked ``@pytest.mark.slow`` — skipped by default, opt-in via
``make test.integration.slow`` or ``pytest -m slow``.

Requires: coursier (``cs``) and a JDK on PATH.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import skip_without_coursier, skip_without_java

pytestmark = [
    pytest.mark.slow,
    skip_without_coursier,
    skip_without_java,
]


def _run_cli(*args: str, check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    """Run agent-notebook via its entry point."""
    return subprocess.run(
        [
            sys.executable, "-c",
            "import sys; from databricks_agent_notebooks.cli import main; sys.exit(main())",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
    )


class TestKernelInstall:
    """Install a kernel and verify artifacts."""

    @pytest.fixture(autouse=True)
    def _setup(self, runtime_home: Path, tmp_path: Path) -> None:
        self.runtime_home = runtime_home
        self.kernels_dir = tmp_path / "kernels"
        self.kernels_dir.mkdir()

    def test_install_default_kernels(self) -> None:
        """Install default kernels via legacy install-kernel shim (installs both 2.12 + 2.13)."""
        result = _run_cli(
            "install-kernel",
            "--kernels-dir", str(self.kernels_dir),
        )
        assert result.returncode == 0, f"Install failed:\n{result.stderr}"

        # Both 2.12 and 2.13 kernel directories should exist
        kernel_dirs = list(self.kernels_dir.iterdir())
        assert len(kernel_dirs) >= 2, f"Expected both Scala variants, got {[d.name for d in kernel_dirs]}"

        kernel_dir = kernel_dirs[0]

        # kernel.json must exist and be valid JSON
        kernel_json = kernel_dir / "kernel.json"
        assert kernel_json.exists(), f"kernel.json not found in {kernel_dir}"
        data = json.loads(kernel_json.read_text())
        assert "argv" in data, f"kernel.json missing 'argv': {data}"
        assert "display_name" in data, f"kernel.json missing 'display_name': {data}"
        assert data.get("language") == "scala", f"Expected language=scala, got: {data.get('language')}"

        # Launcher contract should exist
        contract_files = list(kernel_dir.glob("launcher-contract*.json"))
        assert len(contract_files) >= 1, f"No launcher contract in {kernel_dir}: {list(kernel_dir.iterdir())}"

    def test_install_scala_213(self) -> None:
        """Install Scala 2.13 kernel and verify it coexists with 2.12."""
        # Install 2.12 via kernels install
        result_212 = _run_cli(
            "kernels", "install",
            "--jupyter-path", str(self.kernels_dir),
            "--scala-version", "2.12",
            "--force",
        )
        assert result_212.returncode == 0, f"Scala 2.12 install failed:\n{result_212.stderr}"

        # Install 2.13 via kernels install
        result_213 = _run_cli(
            "kernels", "install",
            "--jupyter-path", str(self.kernels_dir),
            "--scala-version", "2.13",
            "--force",
        )
        assert result_213.returncode == 0, f"Scala 2.13 install failed:\n{result_213.stderr}"

        # Both kernel directories should exist
        kernel_dirs = {d.name for d in self.kernels_dir.iterdir() if d.is_dir()}
        assert any("212" in name or "2.12" in name or "2_12" in name for name in kernel_dirs), (
            f"No Scala 2.12 kernel directory found in {kernel_dirs}"
        )
        assert any("213" in name or "2.13" in name or "2_13" in name for name in kernel_dirs), (
            f"No Scala 2.13 kernel directory found in {kernel_dirs}"
        )


class TestKernelList:
    """List kernels after installation."""

    def test_list_after_install(self, runtime_home: Path, tmp_path: Path) -> None:
        kernels_dir = tmp_path / "kernels"
        kernels_dir.mkdir()

        # Install a kernel
        install_result = _run_cli(
            "install-kernel",
            "--kernels-dir", str(kernels_dir),
        )
        assert install_result.returncode == 0, f"Install failed:\n{install_result.stderr}"

        # List should show it
        list_result = _run_cli(
            "kernels", "list",
            "--jupyter-path", str(kernels_dir),
        )
        assert list_result.returncode == 0, f"List failed:\n{list_result.stderr}"
        assert list_result.stdout.strip(), "kernels list produced no output after install"


class TestKernelRemove:
    """Remove a kernel and verify cleanup."""

    def test_remove_installed_kernel(self, runtime_home: Path, tmp_path: Path) -> None:
        kernels_dir = tmp_path / "kernels"
        kernels_dir.mkdir()

        # Install
        _run_cli("install-kernel", "--kernels-dir", str(kernels_dir))

        # Find the kernel name
        kernel_dirs = [d for d in kernels_dir.iterdir() if d.is_dir()]
        assert kernel_dirs, "No kernel installed"
        kernel_name = kernel_dirs[0].name

        # Remove
        result = _run_cli(
            "kernels", "remove", kernel_name,
            "--jupyter-path", str(kernels_dir),
            check=False,
        )
        assert result.returncode == 0, f"Remove failed:\n{result.stderr}"

        # Verify the kernel directory is gone
        remaining = [d for d in kernels_dir.iterdir() if d.is_dir()]
        removed_names = [d.name for d in remaining]
        assert kernel_name not in removed_names, (
            f"Kernel {kernel_name} still present after remove: {removed_names}"
        )


class TestRuntimeReceipts:
    """Verify runtime receipts are created during kernel install."""

    def test_receipt_created(self, runtime_home: Path, tmp_path: Path) -> None:
        kernels_dir = tmp_path / "kernels"
        kernels_dir.mkdir()

        _run_cli("install-kernel", "--kernels-dir", str(kernels_dir))

        # Runtime home should have receipts
        receipt_files = list(runtime_home.rglob("*.json"))
        assert len(receipt_files) >= 1, (
            f"No receipt files in {runtime_home}: {list(runtime_home.rglob('*'))}"
        )

        # At least one receipt should be valid JSON with expected fields
        for rf in receipt_files:
            data = json.loads(rf.read_text())
            # Receipts vary in structure; just verify they're valid JSON dicts
            assert isinstance(data, dict), f"Receipt {rf.name} is not a dict: {type(data)}"
