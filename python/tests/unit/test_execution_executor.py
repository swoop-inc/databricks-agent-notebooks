"""Tests for notebook execution command construction."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from databricks_agent_notebooks.execution.executor import ExecutionResult, execute_notebook
from databricks_agent_notebooks.runtime.home import HOME_ENV_VAR


@pytest.fixture()
def notebook_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.ipynb"
    path.write_text("{}", encoding="utf-8")
    return path


@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_kernel_and_timeout_are_passed_to_nbconvert(mock_run, notebook_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    execute_notebook(notebook_path, kernel="my-kernel", timeout=300)

    cmd = mock_run.call_args[0][0]
    assert cmd[:3] == [os.sys.executable, "-m", "jupyter"]
    assert "--ExecutePreprocessor.kernel_name=my-kernel" in cmd
    assert "--ExecutePreprocessor.timeout=300" in cmd


@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_spark_home_is_removed_from_env(mock_run, notebook_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    with patch.dict(os.environ, {"SPARK_HOME": "/some/spark"}, clear=False):
        execute_notebook(notebook_path, kernel="python3")

    env = mock_run.call_args[1]["env"]
    assert "SPARK_HOME" not in env


@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_runtime_home_kernel_path_is_added_to_jupyter_search_path(mock_run, notebook_path: Path, tmp_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    runtime_home = tmp_path / "runtime-home"

    with patch.dict(os.environ, {HOME_ENV_VAR: str(runtime_home)}, clear=False):
        execute_notebook(notebook_path, kernel="scala212-dbr-connect")

    env = mock_run.call_args[1]["env"]
    assert env["JUPYTER_PATH"] == str(runtime_home / "data")


@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_successful_run_returns_execution_result(mock_run, notebook_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    result = execute_notebook(notebook_path, kernel="python3")

    assert isinstance(result, ExecutionResult)
    assert result.success is True
    assert result.error is None
