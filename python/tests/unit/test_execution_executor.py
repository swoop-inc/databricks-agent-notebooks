"""Tests for notebook execution command construction."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import ANY, Mock, patch

import nbformat
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
@patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
def test_kernel_and_timeout_are_passed_to_execute_preprocessor(mock_execute_preprocessor, ensure_kernel, notebook_path: Path) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("print(1)")])
    nbformat.write(notebook, notebook_path)
    mock_execute_preprocessor.return_value.preprocess.return_value = (notebook, {})

    execute_notebook(notebook_path, kernel="my-kernel", timeout=300)

    ensure_kernel.assert_called_once_with("my-kernel", extra_kernel_dirs=[str(resolve_runtime_home().kernels_dir)])
    mock_execute_preprocessor.assert_called_once_with(
        kernel_name="my-kernel",
        timeout=300,
        allow_errors=False,
        on_cell_execute=ANY,
        on_cell_complete=ANY,
        on_cell_executed=ANY,
    )


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
def test_spark_home_is_removed_from_env(mock_execute_preprocessor, _ensure_kernel, notebook_path: Path) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("print(1)")])
    nbformat.write(notebook, notebook_path)

    def _preprocess(nb, resources):
        assert "SPARK_HOME" not in os.environ
        return nb, resources

    mock_execute_preprocessor.return_value.preprocess.side_effect = _preprocess

    with patch.dict(os.environ, {"SPARK_HOME": "/some/spark"}, clear=False):
        execute_notebook(notebook_path, kernel="python3")


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
def test_runtime_home_kernel_path_is_added_to_jupyter_search_path(mock_execute_preprocessor, _ensure_kernel, notebook_path: Path, tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("print(1)")])
    nbformat.write(notebook, notebook_path)
    runtime_home = tmp_path / "runtime-home"

    def _preprocess(nb, resources):
        assert os.environ["JUPYTER_PATH"] == str(runtime_home / "data")
        return nb, resources

    mock_execute_preprocessor.return_value.preprocess.side_effect = _preprocess

    with patch.dict(os.environ, {HOME_ENV_VAR: str(runtime_home)}, clear=False):
        execute_notebook(notebook_path, kernel="scala212-dbr-connect")


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
def test_runtime_home_kernels_dir_is_passed_to_preflight(mock_execute_preprocessor, ensure_kernel, notebook_path: Path, tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("print(1)")])
    nbformat.write(notebook, notebook_path)
    mock_execute_preprocessor.return_value.preprocess.return_value = (notebook, {})
    runtime_home = tmp_path / "runtime-home"

    with patch.dict(os.environ, {HOME_ENV_VAR: str(runtime_home)}, clear=False):
        execute_notebook(notebook_path, kernel="scala212-dbr-connect")

    ensure_kernel.assert_called_once_with(
        "scala212-dbr-connect",
        extra_kernel_dirs=[str(runtime_home / "data" / "kernels")],
    )


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
def test_successful_run_returns_execution_result(mock_execute_preprocessor, _ensure_kernel, notebook_path: Path) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("print(1)")])
    nbformat.write(notebook, notebook_path)
    mock_execute_preprocessor.return_value.preprocess.return_value = (notebook, {})

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


@patch("databricks_agent_notebooks.execution.executor.subprocess.run")
def test_execute_notebook_delegates_to_managed_runtime_python(mock_run, notebook_path: Path) -> None:
    output_path = notebook_path.with_suffix(".managed.executed.ipynb")

    def fake_run(command, **kwargs):
        del kwargs
        output_path.write_text("{}", encoding="utf-8")
        result_path = Path(command[command.index("--result-path") + 1])
        result_path.write_text(
            '{"success": true, "output_path": "' + str(output_path) + '", "duration_seconds": 0.1, "error": null}',
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    mock_run.side_effect = fake_run

    result = execute_notebook(
        notebook_path,
        kernel="python3",
        output_path=output_path,
        python_executable=Path("/managed/runtime/bin/python"),
    )

    assert result.success is True
    mock_run.assert_called_once_with(
        [
            "/managed/runtime/bin/python",
            "-m",
            "databricks_agent_notebooks.execution.executor",
            "--notebook-path",
            str(notebook_path),
            "--output-path",
            str(output_path),
            "--kernel",
            "python3",
            "--result-path",
            ANY,
        ],
        capture_output=True,
        text=True,
        check=False,
    )


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


def test_execute_notebook_emits_safe_cell_progress_for_injected_cells(
    notebook_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_code_cell(
                "\n".join(
                    [
                        "# [AGENT-NOTEBOOK:INJECTED] - auto-generated, do not edit",
                        "# Source: /tmp/input.md",
                        "from databricks.connect import DatabricksSession",
                        "spark = DatabricksSession.builder.serverless().getOrCreate()",
                    ]
                )
            )
        ]
    )
    notebook.cells[0].metadata["agent_notebook_injected"] = True
    nbformat.write(notebook, notebook_path)

    class FakeExecutePreprocessor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def preprocess(self, nb, resources):
            self.kwargs["on_cell_execute"](cell=nb.cells[0], cell_index=0)
            time.sleep(0.03)
            self.kwargs["on_cell_complete"](cell=nb.cells[0], cell_index=0)
            self.kwargs["on_cell_executed"](cell=nb.cells[0], cell_index=0, execute_reply={})
            return nb, resources

    with (
        patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel"),
        patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor", FakeExecutePreprocessor),
        patch("databricks_agent_notebooks.execution.executor.HEARTBEAT_INTERVAL_SECONDS", 0.01),
    ):
        result = execute_notebook(notebook_path, kernel="python3")

    assert result.success is True
    progress_lines = [line for line in capsys.readouterr().err.splitlines() if line.startswith("agent-notebook:")]
    assert progress_lines[0].startswith("agent-notebook: phase=cell-start cell_index=1")
    assert 'cell_label="[AGENT-NOTEBOOK:INJECTED] Databricks session setup"' in progress_lines[0]
    assert 'cell_snippet="[source redacted]"' in progress_lines[0]
    assert "DatabricksSession.builder.serverless" not in progress_lines[0]
    assert any("phase=executing" in line and "heartbeat=1" in line and "cell_index=1" in line for line in progress_lines[1:])


def test_execute_notebook_stops_heartbeats_after_on_cell_executed(
    notebook_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("value = 1")])
    nbformat.write(notebook, notebook_path)

    class FakeExecutePreprocessor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def preprocess(self, nb, resources):
            self.kwargs["on_cell_execute"](cell=nb.cells[0], cell_index=0)
            time.sleep(0.02)
            self.kwargs["on_cell_complete"](cell=nb.cells[0], cell_index=0)
            time.sleep(0.03)
            self.kwargs["on_cell_executed"](cell=nb.cells[0], cell_index=0, execute_reply={})
            return nb, resources

    with (
        patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel"),
        patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor", FakeExecutePreprocessor),
        patch("databricks_agent_notebooks.execution.executor.HEARTBEAT_INTERVAL_SECONDS", 0.01),
    ):
        result = execute_notebook(notebook_path, kernel="python3")

    assert result.success is True
    progress_lines = [line for line in capsys.readouterr().err.splitlines() if "phase=executing" in line]
    assert len(progress_lines) >= 1
    time.sleep(0.03)
    assert capsys.readouterr().err == ""


def test_execute_notebook_keeps_heartbeats_running_until_on_cell_executed(
    notebook_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("value = 1")])
    nbformat.write(notebook, notebook_path)

    class FakeExecutePreprocessor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def preprocess(self, nb, resources):
            self.kwargs["on_cell_execute"](cell=nb.cells[0], cell_index=0)
            time.sleep(0.015)
            self.kwargs["on_cell_complete"](cell=nb.cells[0], cell_index=0)
            time.sleep(0.035)
            self.kwargs["on_cell_executed"](cell=nb.cells[0], cell_index=0, execute_reply={})
            return nb, resources

    with (
        patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel"),
        patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor", FakeExecutePreprocessor),
        patch("databricks_agent_notebooks.execution.executor.HEARTBEAT_INTERVAL_SECONDS", 0.01),
    ):
        result = execute_notebook(notebook_path, kernel="python3")

    assert result.success is True
    progress_lines = [line for line in capsys.readouterr().err.splitlines() if "phase=executing" in line]
    assert len(progress_lines) >= 3
    assert all("cell_index=1" in line for line in progress_lines)


def test_execute_notebook_redacts_user_cell_source_from_progress(
    notebook_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_code_cell(
                'token = "abc123secret"\n'
                "use(token)\n"
                "display(token)"
            )
        ]
    )
    nbformat.write(notebook, notebook_path)

    class FakeExecutePreprocessor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def preprocess(self, nb, resources):
            self.kwargs["on_cell_execute"](cell=nb.cells[0], cell_index=0)
            self.kwargs["on_cell_complete"](cell=nb.cells[0], cell_index=0)
            self.kwargs["on_cell_executed"](cell=nb.cells[0], cell_index=0, execute_reply={})
            return nb, resources

    with (
        patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel"),
        patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor", FakeExecutePreprocessor),
    ):
        result = execute_notebook(notebook_path, kernel="python3")

    assert result.success is True
    stderr = capsys.readouterr().err
    progress_lines = [line for line in stderr.splitlines() if "phase=cell-start" in line]
    assert len(progress_lines) == 1
    assert 'cell_label="[code cell]"' in progress_lines[0]
    assert 'cell_snippet="[source redacted]"' in progress_lines[0]
    assert "abc123secret" not in stderr
    assert 'token = "' not in stderr
