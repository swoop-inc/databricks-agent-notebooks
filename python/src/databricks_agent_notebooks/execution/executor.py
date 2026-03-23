"""Notebook execution via Jupyter nbconvert subprocess.

Wraps the ``jupyter nbconvert --execute`` workflow, handling kernel
selection, timeout, environment sanitization (SPARK_HOME removal), and
timing.  Returns a structured :class:`ExecutionResult` rather than
raising on failure so callers can inspect both success and error paths.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel

from databricks_agent_notebooks.runtime.home import resolve_runtime_home

try:
    from ipykernel.kernelspec import install as install_ipykernel
except ModuleNotFoundError:  # pragma: no cover - exercised via packaging verification
    install_ipykernel = None


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of a headless notebook execution."""

    success: bool
    output_path: Path | None
    duration_seconds: float
    error: str | None = None


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


def execute_notebook(
    notebook_path: Path,
    *,
    output_path: Path | None = None,
    kernel: str,
    timeout: int = 600,
    allow_errors: bool = False,
) -> ExecutionResult:
    """Execute a notebook headlessly via ``jupyter nbconvert``.

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

    cmd = [
        os.sys.executable,
        "-m",
        "jupyter",
        "nbconvert",
        "--to",
        "notebook",
        "--execute",
        f"--ExecutePreprocessor.kernel_name={kernel}",
        f"--ExecutePreprocessor.timeout={timeout}",
        f"--output={output_path}",
    ]

    if allow_errors:
        cmd.append("--ExecutePreprocessor.allow_errors=True")

    cmd.append(str(notebook_path))

    # Remove SPARK_HOME from the subprocess environment so the kernel does
    # not pick up a local Spark installation.
    env = os.environ.copy()
    env.pop("SPARK_HOME", None)
    runtime_home = resolve_runtime_home(env)
    runtime_data_dir = str(runtime_home.kernels_dir.parent)
    existing_jupyter_path = env.get("JUPYTER_PATH")
    if existing_jupyter_path:
        search_paths = existing_jupyter_path.split(os.pathsep)
        if runtime_data_dir not in search_paths:
            env["JUPYTER_PATH"] = os.pathsep.join([runtime_data_dir, *search_paths])
    else:
        env["JUPYTER_PATH"] = runtime_data_dir

    start = time.monotonic()
    try:
        ensure_execution_kernel(kernel, extra_kernel_dirs=[str(runtime_home.kernels_dir)])
    except RuntimeError as exc:
        return ExecutionResult(
            success=False,
            output_path=output_path,
            duration_seconds=time.monotonic() - start,
            error=str(exc),
        )

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)  # noqa: S603
    duration = time.monotonic() - start

    if result.returncode == 0:
        return ExecutionResult(
            success=True,
            output_path=output_path,
            duration_seconds=duration,
        )

    error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown error"
    return ExecutionResult(
        success=False,
        output_path=output_path,
        duration_seconds=duration,
        error=error_msg,
    )
