"""Notebook execution via Jupyter nbconvert subprocess.

Wraps the ``jupyter nbconvert --execute`` workflow, handling kernel
selection, timeout, environment sanitization (SPARK_HOME removal), and
timing.  Returns a structured :class:`ExecutionResult` rather than
raising on failure so callers can inspect both success and error paths.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of a headless notebook execution."""

    success: bool
    output_path: Path | None
    duration_seconds: float
    error: str | None = None


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

    start = time.monotonic()
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
