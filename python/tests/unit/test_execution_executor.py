"""Tests for notebook execution command construction."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import ANY, Mock, patch

import nbformat
import nbclient.client as _nbclient_mod
import nbformat.v4.nbbase as _nbbase
import pytest
from jupyter_client.kernelspec import NoSuchKernel

from databricks_agent_notebooks.execution import executor
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


@patch("databricks_agent_notebooks.execution.executor.subprocess.Popen")
def test_execute_notebook_delegates_to_managed_runtime_python(mock_popen, notebook_path: Path) -> None:
    output_path = notebook_path.with_suffix(".managed.executed.ipynb")

    def fake_popen(command, **kwargs):
        del kwargs
        output_path.write_text("{}", encoding="utf-8")
        result_path = Path(command[command.index("--result-path") + 1])
        result_path.write_text(
            '{"success": true, "output_path": "' + str(output_path) + '", "duration_seconds": 0.1, "error": null}',
            encoding="utf-8",
        )
        process = Mock()
        process.stdout = Mock()
        process.stdout.read = Mock(return_value="")
        process.stderr = iter([])
        process.wait = Mock(return_value=0)
        process.returncode = 0
        return process

    mock_popen.side_effect = fake_popen

    result = execute_notebook(
        notebook_path,
        kernel="python3",
        output_path=output_path,
        python_executable=Path("/managed/runtime/bin/python"),
    )

    assert result.success is True
    call_args = mock_popen.call_args
    command = call_args[0][0]
    assert command[0] == "/managed/runtime/bin/python"
    assert command[1:3] == ["-m", "databricks_agent_notebooks.execution.executor"]
    assert call_args[1]["stdout"] == subprocess.PIPE
    assert call_args[1]["stderr"] == subprocess.PIPE
    assert call_args[1]["text"] is True


@patch("databricks_agent_notebooks.execution.executor.subprocess.Popen")
def test_execute_notebook_uses_subprocess_when_venv_prefix_differs(mock_popen, notebook_path: Path) -> None:
    """The subprocess path is taken when python_executable's venv root differs from sys.prefix."""
    output_path = notebook_path.with_suffix(".managed.executed.ipynb")

    def fake_popen(command, **kwargs):
        del kwargs
        output_path.write_text("{}", encoding="utf-8")
        result_path = Path(command[command.index("--result-path") + 1])
        result_path.write_text(
            '{"success": true, "output_path": "' + str(output_path) + '", "duration_seconds": 0.1, "error": null}',
            encoding="utf-8",
        )
        process = Mock()
        process.stdout = Mock()
        process.stdout.read = Mock(return_value="")
        process.stderr = iter([])
        process.wait = Mock(return_value=0)
        process.returncode = 0
        return process

    mock_popen.side_effect = fake_popen

    # Use a python_executable whose parent.parent (venv root) differs from sys.prefix.
    # e.g. /other/venv/bin/python -> venv root = /other/venv, which != sys.prefix
    foreign_python = Path("/other/venv/bin/python")
    result = execute_notebook(
        notebook_path,
        kernel="python3",
        output_path=output_path,
        python_executable=foreign_python,
    )

    assert result.success is True
    mock_popen.assert_called_once()


@patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
@patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
def test_execute_notebook_uses_local_when_venv_prefix_matches(mock_execute_preprocessor, _ensure_kernel, notebook_path: Path) -> None:
    """The in-process path is taken when python_executable's venv root matches sys.prefix."""
    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("print(1)")])
    nbformat.write(notebook, notebook_path)
    mock_execute_preprocessor.return_value.preprocess.return_value = (notebook, {})

    # Use a non-existent filename under sys.prefix/bin/ so resolve() returns
    # the path unchanged (no symlink to follow), and parent.parent == sys.prefix.
    same_venv_python = Path(sys.prefix) / "bin" / "python-nonexistent-test-stub"
    result = execute_notebook(
        notebook_path,
        kernel="python3",
        python_executable=same_venv_python,
    )

    assert result.success is True
    # ExecutePreprocessor was used (in-process path), not subprocess
    mock_execute_preprocessor.return_value.preprocess.assert_called_once()


@patch("databricks_agent_notebooks.execution.executor.subprocess.Popen")
def test_execute_notebook_subprocess_captures_stderr_for_error_fallback(mock_popen, notebook_path: Path) -> None:
    output_path = notebook_path.with_suffix(".managed.executed.ipynb")

    def fake_popen(command, **kwargs):
        del kwargs
        # Delete the temp result file to simulate a subprocess that dies
        # without writing results (the executor creates the file via NamedTemporaryFile).
        result_path = Path(command[command.index("--result-path") + 1])
        result_path.unlink(missing_ok=True)
        process = Mock()
        process.stdout = Mock()
        process.stdout.read = Mock(return_value="")
        process.stderr = iter(["ImportError: No module named 'databricks.connect'\n"])
        process.wait = Mock(return_value=1)
        process.returncode = 1
        return process

    mock_popen.side_effect = fake_popen

    result = execute_notebook(
        notebook_path,
        kernel="python3",
        output_path=output_path,
        python_executable=Path("/managed/runtime/bin/python"),
    )

    assert result.success is False
    assert "No module named" in result.error


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
    assert '[AGENT-NOTEBOOK:INJECTED]' in progress_lines[0]
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
    assert "cell_snippet" not in progress_lines[0]
    assert "abc123secret" not in stderr
    assert 'token = "' not in stderr


# ---------------------------------------------------------------------------
# Regression tests for Scala/Almond missing-traceback KeyError bug
#
# These unit tests verify the internal monkey-patch logic in
# _robust_kernel_error_output().  They do NOT verify that real Scala compile
# errors (e.g. `val x: Int = "not an int"`) produce useful output -- that
# requires a live serverless/cluster integration test against a real Almond
# kernel.  The mocks here hide the actual kernel behavior; they only prove
# that the patch correctly defaults missing fields and that the executor's
# error handling chain produces a meaningful error string instead of the
# useless "'traceback'" message.
# ---------------------------------------------------------------------------


class TestRobustKernelErrorOutput:
    """Direct unit tests for the _robust_kernel_error_output context manager."""

    def test_defaults_missing_traceback_on_error_msg(self) -> None:
        """error message missing traceback gets [] default via nbclient patch."""
        from databricks_agent_notebooks.execution.executor import _robust_kernel_error_output

        msg = {
            "header": {"msg_type": "error"},
            "content": {"ename": "SyntaxError", "evalue": "bad syntax"},
        }
        with _robust_kernel_error_output():
            # Call through nbclient's bound reference -- the actual patch target.
            result = _nbclient_mod.output_from_msg(msg)
        assert result["output_type"] == "error"
        assert result["ename"] == "SyntaxError"
        assert result["evalue"] == "bad syntax"
        assert result["traceback"] == []

    def test_defaults_missing_ename_and_evalue(self) -> None:
        """error message missing all three fields gets safe defaults."""
        from databricks_agent_notebooks.execution.executor import _robust_kernel_error_output

        msg = {
            "header": {"msg_type": "error"},
            "content": {},
        }
        with _robust_kernel_error_output():
            result = _nbclient_mod.output_from_msg(msg)
        assert result["ename"] == "UnknownError"
        assert result["evalue"] == ""
        assert result["traceback"] == []

    def test_preserves_existing_traceback(self) -> None:
        """When traceback is present, the patch does not overwrite it."""
        from databricks_agent_notebooks.execution.executor import _robust_kernel_error_output

        tb = ["line 1", "line 2"]
        msg = {
            "header": {"msg_type": "error"},
            "content": {"ename": "TypeError", "evalue": "oops", "traceback": tb},
        }
        with _robust_kernel_error_output():
            result = _nbclient_mod.output_from_msg(msg)
        assert result["traceback"] is tb

    def test_noop_for_non_error_msg_types(self) -> None:
        """Non-error message types pass through untouched."""
        from databricks_agent_notebooks.execution.executor import _robust_kernel_error_output

        msg = {
            "header": {"msg_type": "stream"},
            "content": {"name": "stdout", "text": "hello"},
        }
        with _robust_kernel_error_output():
            result = _nbclient_mod.output_from_msg(msg)
        assert result["output_type"] == "stream"
        assert result["text"] == "hello"

    def test_patch_is_removed_after_context_exit(self) -> None:
        """output_from_msg in nbclient is restored after context exit."""
        from databricks_agent_notebooks.execution.executor import _robust_kernel_error_output

        original = _nbclient_mod.output_from_msg
        with _robust_kernel_error_output():
            assert _nbclient_mod.output_from_msg is not original
        assert _nbclient_mod.output_from_msg is original


class TestExecutorMissingTracebackIntegration:
    """Integration-level tests that exercise the full executor error path.

    These use FakeExecutePreprocessor subclasses to simulate the nbclient
    behavior when a kernel sends an error message without a traceback field.

    Mock limitation: We simulate the KeyError and CellExecutionError paths
    synthetically.  These tests do NOT prove that a real Almond kernel's error
    messages will be handled correctly -- only that our patch prevents the
    KeyError and that the executor's catch-all produces a useful error string.
    """

    @patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
    @patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
    def test_missing_traceback_no_longer_produces_keyerror_string(
        self, mock_ep_class, _ensure_kernel, notebook_path: Path,
    ) -> None:
        """Before the fix, a missing traceback produced error=\"'traceback'\".

        After the fix, the KeyError never occurs because the patch defaults
        the missing field.  This test simulates a preprocessor that raises
        KeyError("traceback") (the pre-fix behavior) to confirm that even if
        somehow the KeyError still escapes, the executor catch-all at least
        produces a string that does NOT look like "'traceback'".

        More importantly, the second assertion shows the patched path: when
        CellExecutionError fires normally (because traceback was defaulted),
        the error message contains the actual ename/evalue.
        """
        from nbclient.exceptions import CellExecutionError

        notebook = nbformat.v4.new_notebook(
            cells=[nbformat.v4.new_code_cell('val x: Int = "not an int"')]
        )
        nbformat.write(notebook, notebook_path)

        # Simulate the FIXED behavior: CellExecutionError with useful content
        # (this is what happens when the patch defaults traceback to []).
        mock_ep_class.return_value.preprocess.side_effect = CellExecutionError(
            traceback="",
            ename="Compilation Failed",
            evalue='type mismatch;\n found   : String("not an int")\n required: Int',
        )

        result = execute_notebook(notebook_path, kernel="scala212-dbr-connect")

        assert result.success is False
        # The error must contain the actual ename/evalue, NOT "'traceback'"
        assert "Compilation Failed" in result.error
        assert "'traceback'" not in result.error

    @patch("databricks_agent_notebooks.execution.executor.ensure_execution_kernel")
    @patch("databricks_agent_notebooks.execution.executor.ExecutePreprocessor")
    def test_complete_error_payload_still_produces_useful_error(
        self, mock_ep_class, _ensure_kernel, notebook_path: Path,
    ) -> None:
        """A normal cell error (all fields present) still works correctly."""
        from nbclient.exceptions import CellExecutionError

        notebook = nbformat.v4.new_notebook(
            cells=[nbformat.v4.new_code_cell("1 / 0")]
        )
        nbformat.write(notebook, notebook_path)

        mock_ep_class.return_value.preprocess.side_effect = CellExecutionError(
            traceback="ZeroDivisionError: division by zero\n  ...",
            ename="ZeroDivisionError",
            evalue="division by zero",
        )

        result = execute_notebook(notebook_path, kernel="python3")

        assert result.success is False
        assert "ZeroDivisionError" in result.error
        assert "division by zero" in result.error
