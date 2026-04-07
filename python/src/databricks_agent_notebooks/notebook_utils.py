"""Notebook utilities for dual-environment (local Spark + Databricks) development.

Three utilities:

- ``is_databricks(spark=None)`` -- detect whether the active Spark session
  is backed by Databricks.  Enables environment branching: local metastore
  vs Unity Catalog, local parquet vs Delta tables, etc.

- ``resolve_repo_root()`` -- find the repository root directory when
  ``__file__`` is unavailable (e.g. after ``include()`` inlining).

- ``set_query_execution_timeout(seconds, spark=None)`` -- set the
  per-query execution timeout on Databricks.  No-op outside Databricks.

Usage -- direct import works in all ``agent-notebook`` execution modes
(LOCAL_SPARK, serverless, cluster) because the package is always available
in the execution environment::

    from databricks_agent_notebooks.notebook_utils import *

    if is_databricks():
        df.write.saveAsTable("catalog.schema.table")
    else:
        df.write.parquet(f"{resolve_repo_root()}/tmp/scratch/table")
"""

import inspect
import os
from pathlib import Path

__all__ = ["is_databricks", "resolve_repo_root", "set_query_execution_timeout"]


def _resolve_spark(spark=None):
    """Resolve SparkSession: explicit arg, caller's global, active session.

    Resolution order:

    1. Explicit *spark* argument (returned as-is).
    2. Frame inspection -- walks 2 frames back (skipping ``_resolve_spark``
       and its caller) to find a ``spark`` global in user code.  Works
       whether the module was imported or inlined via ``include()``.
    3. ``SparkSession.getActiveSession()`` as final fallback.
    4. Returns ``None`` if no session is found.

    This function assumes it is called from a public API function that
    is called directly by user code (exactly 2 frames deep).  Adding
    intermediate call layers will break frame inspection silently.
    """
    if spark is not None:
        return spark
    frame = inspect.currentframe()
    try:
        caller = frame.f_back.f_back  # skip _resolve_spark + its caller
        if caller is not None:
            spark = caller.f_globals.get("spark")
    finally:
        del frame
    if spark is not None:
        return spark
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
    except ImportError:
        pass
    return spark


def is_databricks(spark=None) -> bool:
    """Return True when running on a Databricks-backed Spark session.

    Parameters
    ----------
    spark : SparkSession, optional
        Explicit session to check.  When omitted, resolved in order:

        1. Caller's ``spark`` global (frame inspection -- finds the
           notebook's injected ``spark`` whether this module was
           imported or inlined via ``include()``).
        2. ``SparkSession.getActiveSession()`` as final fallback.
        3. If no session is found, returns ``False``.

    Detection checks whether any Spark configuration key contains
    ``.databricks.`` (dot-bounded).  This is reliable across serverless,
    cluster-attached, and Databricks Connect sessions.
    """
    spark = _resolve_spark(spark)
    if spark is None:
        return False
    try:
        conf = spark.conf.getAll
        if callable(conf):
            conf = conf()
        return any(".databricks." in k for k in conf.keys())
    except (AttributeError, TypeError):
        return False


def resolve_repo_root() -> str:
    """Find the repository root directory.

    Resolution order:

    1. ``REPO_ROOT`` environment variable (set by run scripts).
    2. Walk up from the current working directory until a ``.git``
       entry (directory or file) is found.
    3. Fall back to the current working directory.

    Why this exists: when code is inlined into a notebook via
    ``include()``, Python's ``__file__`` is not set -- the code was
    eval'd from a string, not loaded from a ``.py`` file.  This
    function provides an explicit filesystem anchor so inlined code
    can resolve repo-relative paths (fixtures, scratch output, etc.).

    On Databricks the repo root is typically irrelevant (Delta tables
    replace local paths), but the function still returns a value so
    callers that unconditionally require a repo root do not need
    environment-specific construction logic.
    """
    root = os.environ.get("REPO_ROOT", "")
    if root:
        return root
    candidate = Path.cwd()
    while candidate != candidate.parent:
        if (candidate / ".git").exists():
            return str(candidate)
        candidate = candidate.parent
    return str(Path.cwd())


def set_query_execution_timeout(seconds: int = 9000, spark=None) -> None:
    """Set the per-query execution timeout on Databricks.

    Wraps ``spark.databricks.execution.timeout``.  No-op outside Databricks.

    The Databricks default is 9000 s (2.5 h).  Multi-hour transforms
    (large tables, heavy joins, full-graph traversals) typically need a
    higher value -- e.g. 86400 (24 h).

    Parameters
    ----------
    seconds : int
        Timeout in seconds.  Default 9000 (the Databricks platform default).
    spark : SparkSession, optional
        Explicit session.  Resolved the same way as ``is_databricks()``:
        caller's ``spark`` global, then ``SparkSession.getActiveSession()``.
    """
    spark = _resolve_spark(spark)
    if not is_databricks(spark):
        return
    spark.conf.set("spark.databricks.execution.timeout", str(seconds))
