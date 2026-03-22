from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from databricks_agent_notebooks.runtime.home import RuntimeHome


def _make_runtime_home(root: Path) -> RuntimeHome:
    return RuntimeHome(
        root=root,
        cache_dir=root / "cache",
        runtimes_dir=root / "data" / "runtimes",
        kernels_dir=root / "data" / "kernels",
        installations_dir=root / "state" / "installations",
        links_dir=root / "state" / "links",
        logs_dir=root / "state" / "logs",
        bin_dir=root / "bin",
        config_dir=root / "config",
    )


def test_patch_kernel_json_adds_flag_and_clears_spark_home(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import ADD_OPENS_FLAG, patch_kernel_json

    kernel_dir = tmp_path / "scala212-dbr-connect"
    kernel_dir.mkdir()
    kernel_json = kernel_dir / "kernel.json"
    kernel_json.write_text(
        json.dumps(
            {
                "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                "env": {"EXISTING": "1", "SPARK_HOME": "/opt/spark"},
            }
        ),
        encoding="utf-8",
    )

    patch_kernel_json(kernel_dir)
    patch_kernel_json(kernel_dir)

    data = json.loads(kernel_json.read_text(encoding="utf-8"))
    assert data["argv"][1] == ADD_OPENS_FLAG
    assert data["argv"].count(ADD_OPENS_FLAG) == 1
    assert data["env"]["EXISTING"] == "1"
    assert data["env"]["SPARK_HOME"] == ""


def test_install_kernel_uses_runtime_home_by_default(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import KERNEL_DISPLAY_NAME, KERNEL_ID, install_kernel

    home = _make_runtime_home(tmp_path / "runtime-home")

    with (
        patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.kernel.ensure_runtime_home", return_value=home) as ensure_home,
        patch("databricks_agent_notebooks.runtime.kernel.find_coursier", return_value="/opt/bin/coursier"),
        patch("databricks_agent_notebooks.runtime.kernel.subprocess.run") as run,
        patch("databricks_agent_notebooks.runtime.kernel.patch_kernel_json") as patch_kernel,
    ):
        kernel_dir = install_kernel()

    assert kernel_dir == home.kernels_dir / KERNEL_ID
    ensure_home.assert_called_once_with(home)
    run.assert_called_once_with(
        [
            "/opt/bin/coursier",
            "launch",
            "--fork",
            "almond",
            "--scala",
            "2.12",
            "--",
            "--install",
            "--force",
            "--id",
            KERNEL_ID,
            "--display-name",
            KERNEL_DISPLAY_NAME,
            "--jupyter-path",
            str(home.kernels_dir),
        ],
        check=True,
    )
    patch_kernel.assert_called_once_with(home.kernels_dir / KERNEL_ID)


def test_install_kernel_requires_coursier(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import install_kernel

    home = _make_runtime_home(tmp_path / "runtime-home")

    with (
        patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.kernel.ensure_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.kernel.find_coursier", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="coursier is required"):
            install_kernel()


def test_verify_kernel_reports_missing_launcher_semantics(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import KERNEL_ID, verify_kernel

    kernel_dir = tmp_path / KERNEL_ID
    kernel_dir.mkdir()
    (kernel_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                "env": {"SPARK_HOME": "/opt/spark"},
            }
        ),
        encoding="utf-8",
    )

    issues = verify_kernel(tmp_path)

    assert any("add-opens" in issue.lower() for issue in issues)
    assert any("SPARK_HOME" in issue for issue in issues)
