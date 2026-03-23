"""Tests for notebook execution command construction."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from jupyter_client.kernelspec import NoSuchKernel

from databricks_agent_notebooks.execution.executor import (
    ExecutionResult,
    ensure_execution_kernel,
    execute_notebook,
)
from databricks_agent_notebooks.runtime.home import HOME_ENV_VAR, resolve_runtime_home


@pytest.fixture()
def notebook_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.ipynb"
    path.write_text("{}", encoding="utf-8")
    return path


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_kernel_and_timeout_are_passed_to_nbconvert(mock_run, ensure_kernel, notebook_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    execute_notebook(notebook_path, kernel="my-kernel", timeout=300)

    ensure_kernel.assert_called_once_with("my-kernel", extra_kernel_dirs=[str(resolve_runtime_home().kernels_dir)])
    cmd = mock_run.call_args[0][0]
    assert cmd[:3] == [os.sys.executable, "-m", "jupyter"]
    assert "--ExecutePreprocessor.kernel_name=my-kernel" in cmd
    assert "--ExecutePreprocessor.timeout=300" in cmd


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_spark_home_is_removed_from_env(mock_run, _ensure_kernel, notebook_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    with patch.dict(os.environ, {"SPARK_HOME": "/some/spark"}, clear=False):
        execute_notebook(notebook_path, kernel="python3")

    env = mock_run.call_args[1]["env"]
    assert "SPARK_HOME" not in env


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_runtime_home_kernel_path_is_added_to_jupyter_search_path(mock_run, _ensure_kernel, notebook_path: Path, tmp_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    runtime_home = tmp_path / "runtime-home"

    with patch.dict(os.environ, {HOME_ENV_VAR: str(runtime_home)}, clear=False):
        execute_notebook(notebook_path, kernel="scala212-dbr-connect")

    env = mock_run.call_args[1]["env"]
    assert env["JUPYTER_PATH"] == str(runtime_home / "data")


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_runtime_home_kernels_dir_is_passed_to_preflight(mock_run, ensure_kernel, notebook_path: Path, tmp_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    runtime_home = tmp_path / "runtime-home"

    with patch.dict(os.environ, {HOME_ENV_VAR: str(runtime_home)}, clear=False):
        execute_notebook(notebook_path, kernel="scala212-dbr-connect")

    ensure_kernel.assert_called_once_with(
        "scala212-dbr-connect",
        extra_kernel_dirs=[str(runtime_home / "data" / "kernels")],
    )


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_successful_run_returns_execution_result(mock_run, _ensure_kernel, notebook_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    result = execute_notebook(notebook_path, kernel="python3")

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.error is None


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
def test_missing_kernel_returns_execution_result_without_traceback(ensure_kernel, notebook_path: Path) -> None:
    ensure_kernel.side_effect = RuntimeError("missing kernel")

    result = execute_notebook(notebook_path, kernel="scala212-dbr-connect")

    assert isinstance(result, ExecutionResult)
    assert result.success is False
    assert result.error == "missing kernel"


def test_ensure_execution_kernel_bootstraps_python3_when_missing() -> None:
    manager = Mock()
    manager.get_kernel_spec.side_effect = [NoSuchKernel("python3"), object()]

    with (
        patch("databricks_agent_notebooks.execution.executor.KernelSpecManager", return_value=manager),
        patch("databricks_agent_notebooks.execution.executor.install_ipykernel", autospec=True) as install_ipykernel,
    ):
        ensure_execution_kernel("python3")

    install_ipykernel.assert_called_once_with(
        kernel_spec_manager=manager,
        kernel_name="python3",
        prefix=sys.prefix,
    )


def test_ensure_execution_kernel_raises_for_missing_non_python_kernel() -> None:
    manager = type("Manager", (), {"get_kernel_spec": lambda self, name: (_ for _ in ()).throw(NoSuchKernel(name))})()

    with patch("databricks_agent_notebooks.execution.executor.KernelSpecManager", return_value=manager):
        with pytest.raises(RuntimeError, match="my-kernel"):
            ensure_execution_kernel("my-kernel")
