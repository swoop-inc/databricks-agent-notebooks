"""Smoke tests for the standalone CLI surface."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks_agent_notebooks.cli import main
from databricks_agent_notebooks.config.frontmatter import DatabricksConfig
from databricks_agent_notebooks.integrations.databricks.clusters import Cluster
from databricks_agent_notebooks.runtime.kernel import KERNEL_DISPLAY_NAME, KERNEL_ID
from databricks_agent_notebooks.runtime.doctor import Check


def _make_notebook_mock():
    notebook = MagicMock()
    notebook.cells = []
    notebook.metadata = {}
    return notebook


def test_help_returns_zero(capsys) -> None:
    result = main(["help"])

    assert result == 0
    assert "agent-notebook" in capsys.readouterr().out


def test_run_file_not_found(capsys) -> None:
    result = main(["run", "/nonexistent/file.md"])

    assert result == 1
    assert "not found" in capsys.readouterr().err


def test_run_pipeline_delegates(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="13.3")

    with (
        patch("databricks_agent_notebooks.cli.to_notebook", return_value=(_make_notebook_mock(), DatabricksConfig(profile="prod", cluster="my-cluster"))),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.merge_config", return_value=DatabricksConfig(profile="prod", cluster="my-cluster")),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=_make_notebook_mock()),
        patch("databricks_agent_notebooks.cli.execute_notebook", return_value=MagicMock(success=True, output_path=input_file, duration_seconds=1.0, error=None)),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    assert "Execution succeeded" in capsys.readouterr().out


def test_install_kernel_command_delegates(tmp_path: Path, capsys) -> None:
    kernel_dir = tmp_path / "kernels" / "scala212-dbr-connect"

    with patch("databricks_agent_notebooks.cli.install_kernel", return_value=kernel_dir) as install_kernel:
        result = main(["install-kernel", "--kernels-dir", str(tmp_path / "kernels")])

    assert result == 0
    install_kernel.assert_called_once_with(
        kernel_id=KERNEL_ID,
        display_name=KERNEL_DISPLAY_NAME,
        kernels_dir=tmp_path / "kernels",
        user=False,
        prefix=None,
        sys_prefix=False,
        jupyter_path=None,
        force=True,
    )
    assert "Kernel installed" in capsys.readouterr().out


def test_kernels_install_command_delegates(tmp_path: Path, capsys) -> None:
    kernel_dir = tmp_path / "kernels" / "scala212-dbr-connect"

    with patch("databricks_agent_notebooks.cli.install_kernel", return_value=kernel_dir) as install_kernel:
        result = main(
            [
                "kernels",
                "install",
                "--id",
                "custom-scala",
                "--display-name",
                "Custom Scala",
                "--jupyter-path",
                str(tmp_path / "kernels"),
                "--force",
            ]
        )

    assert result == 0
    install_kernel.assert_called_once_with(
        kernel_id="custom-scala",
        display_name="Custom Scala",
        kernels_dir=None,
        user=False,
        prefix=None,
        sys_prefix=False,
        jupyter_path=tmp_path / "kernels",
        force=True,
    )
    assert "Kernel installed" in capsys.readouterr().out


def test_kernels_list_command_prints_runtime_and_override_dirs(tmp_path: Path, capsys) -> None:
    runtime_kernel = SimpleNamespace(
        name="scala212-dbr-connect",
        directory=tmp_path / "runtime" / "scala212-dbr-connect",
        source="runtime-home",
        launcher_contract_path=tmp_path / "runtime" / "scala212-dbr-connect" / "launcher-contract.json",
        receipt_path=tmp_path / "state" / "installations" / "kernels" / "scala212-dbr-connect.json",
    )
    override_kernel = SimpleNamespace(
        name="python3",
        directory=tmp_path / "custom" / "python3",
        source=str(tmp_path / "custom"),
        launcher_contract_path=None,
        receipt_path=None,
    )

    with patch(
        "databricks_agent_notebooks.cli.list_installed_kernels",
        return_value=[runtime_kernel, override_kernel],
    ) as list_installed_kernels:
        result = main(["kernels", "list", "--kernels-dir", str(tmp_path / "custom")])

    assert result == 0
    list_installed_kernels.assert_called_once_with(kernels_dirs=[tmp_path / "custom"])
    captured = capsys.readouterr()
    assert "scala212-dbr-connect" in captured.out
    assert "runtime-home" in captured.out
    assert "python3" in captured.out
    assert str(tmp_path / "custom") in captured.out
    assert str(runtime_kernel.launcher_contract_path) in captured.out
    assert "missing" in captured.out


def test_kernels_remove_command_delegates(tmp_path: Path, capsys) -> None:
    removed_dir = tmp_path / "runtime" / "scala212-dbr-connect"

    with patch(
        "databricks_agent_notebooks.cli.remove_kernel",
        return_value=removed_dir,
    ) as remove_kernel:
        result = main(["kernels", "remove", "scala212-dbr-connect", "--kernels-dir", str(tmp_path / "custom")])

    assert result == 0
    remove_kernel.assert_called_once_with("scala212-dbr-connect", kernels_dirs=[tmp_path / "custom"])
    assert str(removed_dir) in capsys.readouterr().out


def test_doctor_command_prints_failures(capsys) -> None:
    checks = [
        Check("coursier", "ok", "coursier found"),
        Check("kernel", "fail", "kernel missing"),
    ]

    with patch("databricks_agent_notebooks.cli.run_checks", return_value=checks) as run_checks:
        result = main(["doctor", "--profile", "DEFAULT"])

    assert result == 1
    run_checks.assert_called_once_with(profile="DEFAULT", kernel_id=KERNEL_ID)
    captured = capsys.readouterr()
    assert "[FAIL] kernel" in captured.out
    assert "1 check(s) failed." in captured.err


def test_doctor_command_accepts_custom_kernel_id(capsys) -> None:
    with patch("databricks_agent_notebooks.cli.run_checks", return_value=[]) as run_checks:
        result = main(["doctor", "--id", "custom-scala", "--profile", "DEFAULT"])

    assert result == 0
    run_checks.assert_called_once_with(profile="DEFAULT", kernel_id="custom-scala")
    assert "All checks passed." in capsys.readouterr().out


def test_kernels_doctor_command_prints_failures(capsys) -> None:
    checks = [
        Check("coursier", "ok", "coursier found"),
        Check("kernel_semantics", "fail", "launcher contract missing"),
    ]

    with patch("databricks_agent_notebooks.cli.run_checks", return_value=checks) as run_checks:
        result = main(["kernels", "doctor", "--profile", "DEFAULT"])

    assert result == 1
    run_checks.assert_called_once_with(profile="DEFAULT", kernel_id=KERNEL_ID)
    captured = capsys.readouterr()
    assert "[FAIL] kernel_semantics" in captured.out
    assert "launcher contract missing" in captured.out
    assert "1 check(s) failed." in captured.err


def test_kernels_doctor_command_accepts_custom_kernel_id(tmp_path: Path, capsys) -> None:
    with patch("databricks_agent_notebooks.cli.run_checks", return_value=[]) as run_checks:
        result = main(
            [
                "kernels",
                "doctor",
                "--id",
                "custom-scala",
                "--jupyter-path",
                str(tmp_path / "kernels"),
            ]
        )

    assert result == 0
    run_checks.assert_called_once_with(profile=None, kernels_dir=tmp_path / "kernels", kernel_id="custom-scala")
    assert "All checks passed." in capsys.readouterr().out
