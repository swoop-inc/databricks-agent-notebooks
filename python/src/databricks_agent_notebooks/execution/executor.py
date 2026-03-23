"""Notebook execution via Jupyter nbconvert with compact progress signals.

Runs notebook execution through the in-process nbconvert/nbclient stack so
current-cell transitions and coarse heartbeats can be surfaced on stderr
without changing the output notebook or final success/failure contract.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from nbconvert.preprocessors import ExecutePreprocessor

import nbformat

from databricks_agent_notebooks.runtime.home import resolve_runtime_home

try:
    from ipykernel.kernelspec import install as install_ipykernel
except ModuleNotFoundError:  # pragma: no cover - exercised via packaging verification
    install_ipykernel = None


HEARTBEAT_INTERVAL_SECONDS = 30.0
REDACTED_CELL_SNIPPET = "[source redacted]"


class RawProgressValue(str):
    """Mark a string as safe to emit without JSON quoting."""


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of a headless notebook execution."""

    success: bool
    output_path: Path | None
    duration_seconds: float
    error: str | None = None


def _format_progress_value(value: object) -> str:
    if isinstance(value, RawProgressValue):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "none"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(str(value))


def format_progress_signal(phase: str, **fields: object) -> str:
    parts = [f"phase={phase}"]
    parts.extend(f"{key}={_format_progress_value(value)}" for key, value in fields.items())
    return f"agent-notebook: {' '.join(parts)}"


def emit_progress_signal(phase: str, *, stream = None, **fields: object) -> None:
    target = stream or sys.stderr
    print(format_progress_signal(phase, **fields), file=target, flush=True)


def _missing_kernel_error(kernel: str) -> RuntimeError:
    return RuntimeError(
        f"Jupyter kernel '{kernel}' is not available in the current environment. "
        "Install or repair the matching kernel before running the notebook."
    )


def ensure_execution_kernel(
    kernel: str,
    *,
    extra_kernel_dirs: Sequence[str] | None = None,
) -> None:
    """Ensure the requested Jupyter kernel exists before running nbconvert."""
    manager = KernelSpecManager()
    if extra_kernel_dirs:
        manager.kernel_dirs = [
            *list(extra_kernel_dirs),
            *[path for path in manager.kernel_dirs if path not in extra_kernel_dirs],
        ]
    try:
        manager.get_kernel_spec(kernel)
        return
    except NoSuchKernel as exc:
        if kernel != "python3":
            raise _missing_kernel_error(kernel) from exc

    if install_ipykernel is None:
        raise RuntimeError(
            "Jupyter kernel 'python3' is not available and ipykernel is not installed. "
            "Reinstall databricks-agent-notebooks with its packaged dependencies."
        )

    install_ipykernel(
        kernel_spec_manager=manager,
        kernel_name="python3",
        prefix=sys.prefix,
    )

    try:
        manager.get_kernel_spec("python3")
    except NoSuchKernel as exc:
        raise RuntimeError(
            "Jupyter kernel 'python3' is still unavailable after attempting an ipykernel repair."
        ) from exc


def _iter_meaningful_lines(source: str) -> list[str]:
    return [line.strip() for line in source.splitlines() if line.strip()]


def _build_cell_label(cell: nbformat.NotebookNode, lines: list[str]) -> str:
    if cell.metadata.get("agent_notebook_injected", False):
        return "[AGENT-NOTEBOOK:INJECTED] Databricks session setup"
    cell_type = str(cell.get("cell_type", "cell"))
    if not lines:
        return f"[empty {cell_type} cell]"
    return f"[{cell_type} cell]"


def _build_cell_snippet(_cell: nbformat.NotebookNode, lines: list[str]) -> str:
    if not lines:
        return "[empty cell]"
    return REDACTED_CELL_SNIPPET


def _describe_cell(cell: nbformat.NotebookNode, *, cell_index: int) -> dict[str, object]:
    lines = _iter_meaningful_lines(cell.source)
    return {
        "cell_index": cell_index + 1,
        "cell_label": _build_cell_label(cell, lines),
        "cell_snippet": _build_cell_snippet(cell, lines),
    }


class _ExecutionProgressReporter:
    def __init__(
        self,
        *,
        stream = None,
        heartbeat_interval: float | None = None,
        clock = time.monotonic,
    ) -> None:
        self._stream = stream
        self._heartbeat_interval = heartbeat_interval or HEARTBEAT_INTERVAL_SECONDS
        self._clock = clock
        self._started_at = clock()
        self._lock = threading.Lock()
        self._current_cell: dict[str, object] | None = None
        self._heartbeat_count = 0
        self._cell_done = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    def _heartbeat_loop(self, cell_done: threading.Event) -> None:
        while not cell_done.wait(self._heartbeat_interval):
            with self._lock:
                if cell_done is not self._cell_done or self._current_cell is None:
                    return
                self._heartbeat_count += 1
                heartbeat_fields = {
                    "elapsed_s": int(self._clock() - self._started_at),
                    "heartbeat": self._heartbeat_count,
                    **self._current_cell,
                }
            emit_progress_signal("executing", stream=self._stream, **heartbeat_fields)

    def _stop_heartbeat(self) -> None:
        with self._lock:
            cell_done = self._cell_done
            heartbeat_thread = self._heartbeat_thread
            self._current_cell = None
            self._heartbeat_thread = None
            self._cell_done = threading.Event()
        cell_done.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join()

    def on_cell_execute(self, *, cell: nbformat.NotebookNode, cell_index: int, **_: object) -> None:
        self._stop_heartbeat()
        cell_progress = _describe_cell(cell, cell_index=cell_index)
        emit_progress_signal("cell-start", stream=self._stream, **cell_progress)
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(self._cell_done,),
            daemon=True,
        )
        with self._lock:
            self._current_cell = cell_progress
            self._heartbeat_count = 0
            self._heartbeat_thread = heartbeat_thread
        heartbeat_thread.start()

    def on_cell_complete(self, **_: object) -> None:
        # nbclient fires this immediately after sending the execute request, not
        # after the cell has finished running.
        return

    def on_cell_executed(self, **_: object) -> None:
        self._stop_heartbeat()

    def close(self) -> None:
        self._stop_heartbeat()


@contextmanager
def _patched_execution_environment(runtime_data_dir: str):
    original_spark_home = os.environ.pop("SPARK_HOME", None)
    original_jupyter_path = os.environ.get("JUPYTER_PATH")

    if original_jupyter_path:
        search_paths = original_jupyter_path.split(os.pathsep)
        if runtime_data_dir not in search_paths:
            os.environ["JUPYTER_PATH"] = os.pathsep.join([runtime_data_dir, *search_paths])
    else:
        os.environ["JUPYTER_PATH"] = runtime_data_dir

    try:
        yield
    finally:
        if original_spark_home is not None:
            os.environ["SPARK_HOME"] = original_spark_home
        else:
            os.environ.pop("SPARK_HOME", None)

        if original_jupyter_path is not None:
            os.environ["JUPYTER_PATH"] = original_jupyter_path
        else:
            os.environ.pop("JUPYTER_PATH", None)


def execute_notebook(
    notebook_path: Path,
    *,
    output_path: Path | None = None,
    kernel: str,
    timeout: int | None = None,
    allow_errors: bool = False,
) -> ExecutionResult:
    """Execute a notebook headlessly via nbconvert's in-process executor.

    Parameters
    ----------
    notebook_path:
        Path to the ``.ipynb`` file to execute.
    output_path:
        Where to write the executed notebook.  Defaults to
        ``<notebook_path>.executed.ipynb`` alongside the original.
    kernel:
        Jupyter kernel name to use for execution.  Required — callers must
        read the kernel from notebook metadata or specify it explicitly.
    timeout:
        Per-cell timeout in seconds.
    allow_errors:
        When ``True``, execution continues even if individual cells error.

    Returns
    -------
    ExecutionResult
        Structured result with success flag, output path, wall-clock
        duration, and error details when the run fails.
    """
    if output_path is None:
        output_path = notebook_path.with_suffix(".executed.ipynb")

    start = time.monotonic()
    runtime_home = resolve_runtime_home()
    try:
        ensure_execution_kernel(kernel, extra_kernel_dirs=[str(runtime_home.kernels_dir)])
    except RuntimeError as exc:
        return ExecutionResult(
            success=False,
            output_path=output_path,
            duration_seconds=time.monotonic() - start,
            error=str(exc),
        )

    reporter = _ExecutionProgressReporter()
    notebook = nbformat.read(str(notebook_path), as_version=4)
    resources = {"metadata": {"path": str(notebook_path.parent)}}
    executor = ExecutePreprocessor(
        kernel_name=kernel,
        timeout=timeout,
        allow_errors=allow_errors,
        on_cell_execute=reporter.on_cell_execute,
        on_cell_complete=reporter.on_cell_complete,
        on_cell_executed=reporter.on_cell_executed,
    )

    try:
        with _patched_execution_environment(str(runtime_home.kernels_dir.parent)):
            executed_notebook, _ = executor.preprocess(notebook, resources)
    except Exception as exc:
        return ExecutionResult(
            success=False,
            output_path=output_path,
            duration_seconds=time.monotonic() - start,
            error=str(exc) or exc.__class__.__name__,
        )
    finally:
        reporter.close()

    nbformat.write(executed_notebook, str(output_path))
    return ExecutionResult(
        success=True,
        output_path=output_path,
        duration_seconds=time.monotonic() - start,
    )
