"""Pipeline primitives for single-notebook data pipelines.

Provides reusable building blocks for idempotent, configurable, multi-step
pipelines within a single notebook -- with selective execution and selective
refresh controlled via JSON ``--param`` values.

Three layers:

1. **Cached computed value** (``read_or_compute_table``) -- get a Spark table
   from cache or build it.  The wrapper shape supports future lifecycle hooks
   without changing call sites.

2. **Optional step** (``read_or_compute_table_step``) -- a step that
   returns ``OptionalDataFrame`` instead of raising when a disabled step's
   cached table is missing.  Errors are deferred to the point of use (when
   a downstream consumer calls ``.get()``), so disabled steps whose output
   is never consumed do not cause failures.

3. **Step orchestration** (``StepRunner`` via ``Context``) -- arrange named
   computations with configurable execution (on/off) and cache busting
   (recompute).  ``Context`` is the unified interface for everything.

Usage::

    from databricks_agent_notebooks.pipeline import *

    ctx = Context(
        param("context").get(),
        defaults={"table_prefix": "default.pipeline_demo"})
    ctx.overlay_params({
        "steps": param("steps").get(),
        "recompute": param("recompute").get(),
    })
    ctx.set_runner(StepRunner(steps=["ingest", "clean", "enrich", "publish"]))
    set_context(ctx)

    # Table names are explicit -- define your own naming convention
    T = ctx["table_prefix"]  # e.g., "default.pipeline_demo"
    df = ctx.run("ingest", table=f"{T}_ingest",
                  compute=lambda: spark.range(1000).toDF("id"))

    # Optional step -- defers error to the consumer
    enriched = read_or_compute_table_step(
        "enrich", read=f"{T}_enriched",
        compute=lambda: enrich(df),
    )
    # Consumer calls .get() -- raises MissingTableError if step was off
    # and no cache exists.
    final = ctx.run("publish", table=f"{T}_final",
                     compute=lambda: publish(enriched.get()))

Spark resolution
~~~~~~~~~~~~~~~~

All functions that need a SparkSession resolve it automatically in this
order:

1. ``ctx.get("spark")`` -- from the pipeline ``Context``, if one is set.
2. ``builtins.spark`` -- the runtime-injected global that Databricks
   notebooks provide.  Code inlined via ``include()`` sees it here.
3. ``SparkSession.getActiveSession()`` -- covers local Spark and
   Databricks Connect sessions.

In the common case (a Databricks notebook), spark is found at step 2
and no explicit wiring is needed.  Functions like ``spark_conf`` and
``cluster_cores`` accept an optional ``spark=`` keyword for the rare
case where the caller needs to override resolution.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

__all__ = [
    "Context",
    "MissingDataFrameError",
    "MissingTableError",
    "OptionalDataFrame",
    "StepRunner",
    "read_or_compute_table_step",
    "cluster_cores",
    "core_based_parallelism",
    "read_or_compute_table",
    "get_context",
    "set_context",
    "spark_conf",
]

# ---------------------------------------------------------------------------
# Context variable for auto-resolution
# ---------------------------------------------------------------------------

_current_context: ContextVar[Context | None] = ContextVar(
    "pipeline_context", default=None
)


def get_context() -> Context | None:
    """Return the current pipeline context, or None if not set."""
    return _current_context.get()


def set_context(ctx: Context) -> None:
    """Set the current pipeline context for auto-resolution."""
    _current_context.set(ctx)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


class Context:
    """Mutable dict-like bag that any pipeline layer can populate.

    Auto-resolved via ``contextvars`` -- single-threaded notebook execution
    is a perfect fit.  ``read_or_compute_table`` picks up context
    automatically; ``StepRunner`` is wired in via ``set_runner``.
    """

    def __init__(
        self,
        initial: str | dict[str, Any] | None = None,
        *,
        defaults: dict[str, Any] | None = None,
    ) -> None:
        self._defaults: dict[str, Any] = dict(defaults) if defaults else {}
        self._data: dict[str, Any] = dict(self._defaults)
        self._runner: StepRunner | None = None

        if initial:
            parsed = _parse_json_or_dict(initial, "context")
            if isinstance(parsed, dict):
                self._data.update(parsed)

    # -- dict-like access ---------------------------------------------------

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    # -- parameter overlay --------------------------------------------------

    def overlay_params(self, params: dict[str, str | None]) -> None:
        """Overlay individual ``--param`` values into context.

        Non-empty string values are JSON-parsed if possible, otherwise stored
        as-is.  ``None`` and empty strings are skipped.
        """
        for key, raw in params.items():
            if not raw:
                continue
            self._data[key] = _try_parse_json(raw)

    # -- runner integration -------------------------------------------------

    def set_runner(self, runner: StepRunner) -> None:
        """Wire a ``StepRunner`` into this context."""
        self._runner = runner

    def should_run(self, step: str) -> bool:
        """Check whether *step* should execute (delegates to runner)."""
        if self._runner is None:
            raise RuntimeError("No StepRunner configured -- call set_runner() first")
        return self._runner.should_run(step, self._data.get("steps"))

    def should_recompute(self, value_id: str) -> bool:
        """Check whether *value_id* should be recomputed."""
        recompute = self._data.get("recompute")
        if not recompute:
            return False
        parsed = _parse_flexible_set(recompute)
        return value_id in parsed

    def run(self, step: str, *, table: str, compute: Callable[..., Any]) -> Any:
        """Execute a pipeline step through the runner.

        Parameters
        ----------
        step
            Step name for orchestration (should_run / should_recompute).
        table
            Fully qualified table name for materialization.  The framework
            does not construct table names -- the caller controls naming.
        compute
            Callable returning a DataFrame or DataFrameWriter.

        If the step is enabled, calls ``read_or_compute_table`` with refresh
        driven by ``should_recompute``.  If disabled but the table exists,
        reads from cache (data-flow safety).  If disabled and no table exists,
        raises.

        Returns a DataFrame (always).
        """
        if self._runner is None:
            raise RuntimeError("No StepRunner configured -- call set_runner() first")

        enabled = self.should_run(step)
        refresh = self.should_recompute(step)

        if enabled:
            # refresh already incorporates should_recompute(step);
            # read_or_compute_table's internal recompute check is a no-op
            # because id=step matches the same value
            return read_or_compute_table(
                read=table,
                compute=compute,
                id=step,
                refresh=refresh,
                context=self,
            )
        else:
            # Disabled step -- try to serve from cache
            spark = _resolve_spark(self)
            if _is_spark_connect(spark):
                if spark.catalog.tableExists(table):
                    df = spark.table(table)
                    logger.info("Step '%s' disabled -- serving from cache: %s", step, table)
                    return df
                else:
                    raise RuntimeError(
                        f"Step '{step}' is disabled but table '{table}' does not exist. "
                        f"Cannot serve from cache."
                    )
            else:
                # Classic Spark: spark.table() throws eagerly
                try:
                    df = spark.table(table)
                    logger.info("Step '%s' disabled -- serving from cache: %s", step, table)
                    return df
                except Exception as e:
                    if "AnalysisException" in type(e).__name__ or "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                        raise RuntimeError(
                            f"Step '{step}' is disabled but table '{table}' does not exist. "
                            f"Cannot serve from cache."
                        ) from e
                    raise

    # -- helpers ------------------------------------------------------------

    def print_config(self) -> None:
        """Print a human-readable summary of pipeline configuration."""
        lines = ["Pipeline configuration:"]
        for key, value in sorted(self._data.items()):
            if key == "spark":
                continue
            lines.append(f"  {key}: {value!r}")
        if self._runner:
            lines.append(f"  runner steps: {sorted(self._runner.known_steps)}")
        print("\n".join(lines))


# ---------------------------------------------------------------------------
# StepRunner
# ---------------------------------------------------------------------------


class StepRunner:
    """Arranges named computations with configurable execution and cache busting.

    The notebook author controls step ordering by cell placement.
    ``StepRunner`` is wired into ``Context`` via ``ctx.set_runner(runner)``
    and accessed through ``ctx.run()`` and ``ctx.should_run()``.
    """

    def __init__(
        self,
        steps: list[str],
        *,
        defaults: dict[str, bool] | None = None,
    ) -> None:
        self.known_steps: set[str] = set(steps)
        self.defaults: dict[str, bool] = defaults or {s: True for s in steps}

    def should_run(self, step: str, steps_config: Any = None) -> bool:
        """Determine whether *step* should execute.

        *steps_config* is the raw value from ``ctx["steps"]`` -- parsed
        flexibly (CSV, JSON array, JSON object, "ALL", empty).
        """
        if step not in self.known_steps:
            raise ValueError(
                f"Unknown step '{step}'. Known steps: {sorted(self.known_steps)}"
            )

        if steps_config is None or steps_config == "" or steps_config == {}:
            return self.defaults.get(step, True)

        parsed = _parse_step_config(steps_config, self.known_steps)
        if step in parsed:
            return parsed[step]
        return self.defaults.get(step, True)


# ---------------------------------------------------------------------------
# read_or_compute_table
# ---------------------------------------------------------------------------


def read_or_compute_table(
    read: str | Callable[..., str],
    compute: Callable[..., Any],
    *,
    id: str | None = None,
    refresh: bool = False,
    context: Context | None = None,
) -> Any:
    """Cached computed value for Spark tables.

    Two operations: **read** (get from cache, cheap) and **compute** (build,
    expensive).  The wrapper controls invocation order and handles
    DataFrame vs DataFrameWriter results.

    Parameters
    ----------
    read
        Table name (string) or callable returning a table name.
    compute
        Callable returning a DataFrame or DataFrameWriter.
    id
        Value name for recompute matching.  Defaults to the resolved table name.
    refresh
        If True, skip cache and go straight to compute.
    context
        Override auto-resolved context.
    """
    ctx = context or get_context()
    table_name = _call_with_optional_context(read, ctx) if callable(read) else read
    value_id = id if id is not None else table_name

    # Auto-trigger refresh from context recompute set
    if not refresh and ctx is not None:
        refresh = ctx.should_recompute(value_id)

    spark = _resolve_spark(ctx)

    if not refresh:
        if _is_spark_connect(spark):
            # Spark Connect: spark.table() is lazy -- use catalog for existence check
            if spark.catalog.tableExists(table_name):
                df = spark.table(table_name)
                logger.info("Cache hit: %s", table_name)
                return df
            else:
                logger.info("Cache miss: %s -- computing", table_name)
        else:
            # Classic Spark: spark.table() throws eagerly -- one round trip
            try:
                df = spark.table(table_name)
                logger.info("Cache hit: %s", table_name)
                return df
            except Exception as e:
                if "AnalysisException" in type(e).__name__ or "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                    logger.info("Cache miss: %s -- computing", table_name)
                else:
                    raise

    # Compute path
    result = compute()
    return _materialize_and_read(spark, table_name, result)


# ---------------------------------------------------------------------------
# OptionalDataFrame
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OptionalDataFrame:
    """Immutable wrapper around an optional DataFrame with error metadata.

    Constructed by ``read_or_compute_table_step``.  All fields are set at
    construction time and cannot be changed (frozen dataclass).

    Access patterns:

    - ``.get()`` -- returns the DataFrame or raises ``MissingDataFrameError``
      with a message built from the metadata captured at construction time.
    - ``.get_or_else(default)`` -- returns the DataFrame if present, otherwise
      *default*.  *default* can be a DataFrame or a zero-arg callable
      (invoked lazily only when the DataFrame is absent).
    - ``.dataframe`` -- returns the DataFrame or ``None`` (never raises).
    - ``bool(opt)`` -- ``True`` when a DataFrame is present.
    """

    step: str | None = None
    id: str | None = None
    table_name: str | None = None
    _df: Any = field(default=None, repr=False)
    _error: MissingDataFrameError | None = field(default=None, repr=False)

    @property
    def dataframe(self) -> Any:
        """The wrapped DataFrame, or ``None`` if absent. Never raises."""
        return self._df

    def get(self) -> Any:
        """Return the DataFrame, or raise with rich context if absent."""
        if self._df is not None:
            return self._df
        if self._error is not None:
            raise self._error
        raise MissingDataFrameError(step=self.step, id=self.id)

    def get_or_else(self, default: Any) -> Any:
        """Return the DataFrame if present, otherwise *default*.

        *default* may be a DataFrame (returned as-is) or a zero-arg callable
        (invoked only when the DataFrame is absent).
        """
        if self._df is not None:
            return self._df
        if callable(default):
            return default()
        return default

    def __bool__(self) -> bool:
        return self._df is not None


# ---------------------------------------------------------------------------
# read_or_compute_table_step
# ---------------------------------------------------------------------------


def read_or_compute_table_step(
    step: str,
    read: str | Callable[..., str],
    compute: Callable[..., Any],
    *,
    id: str | None = None,
    refresh: bool = False,
    context: Context | None = None,
) -> OptionalDataFrame:
    """Run a pipeline step that returns ``OptionalDataFrame`` instead of raising.

    Same parameters as ``read_or_compute_table`` plus ``step`` for
    orchestration.  When the step is enabled, delegates to
    ``read_or_compute_table`` and wraps the result.  When the step is
    disabled, attempts to read from cache; if the table does not exist,
    returns an empty ``OptionalDataFrame`` whose ``.get()`` will raise
    ``MissingTableError`` with full context.

    Parameters
    ----------
    step
        Step name for ``ctx.should_run()`` / ``ctx.should_recompute()``.
    read
        Table name (string) or callable returning a table name.
    compute
        Callable returning a DataFrame or DataFrameWriter.
    id
        Value name for recompute matching.  Defaults to *step*.
    refresh
        If True, skip cache and go straight to compute.
    context
        Override auto-resolved context.

    Returns
    -------
    OptionalDataFrame
        Always returned (never raises for disabled-step cache misses).
    """
    ctx = context or get_context()
    if ctx is None:
        raise RuntimeError(
            "No pipeline context available. "
            "Call set_context() or pass context= explicitly."
        )

    value_id = id if id is not None else step
    table_name = _call_with_optional_context(read, ctx) if callable(read) else read

    if ctx.should_run(step):
        effective_refresh = refresh or ctx.should_recompute(value_id)
        df = read_or_compute_table(
            read=table_name,
            compute=compute,
            id=value_id,
            refresh=effective_refresh,
            context=ctx,
        )
        return OptionalDataFrame(
            step=step, id=value_id, table_name=table_name, _df=df,
        )

    # Disabled step -- try to serve from cache using the dual-path
    # approach: Spark Connect makes spark.table() lazy, so we must
    # branch on runtime type.
    spark = _resolve_spark(ctx)
    if _is_spark_connect(spark):
        if spark.catalog.tableExists(table_name):
            df = spark.table(table_name)
            return OptionalDataFrame(
                step=step, id=value_id, table_name=table_name, _df=df,
            )
        else:
            return OptionalDataFrame(
                step=step,
                id=value_id,
                table_name=table_name,
                _error=MissingTableError(
                    step=step, id=value_id, table_name=table_name,
                ),
            )
    else:
        # Classic Spark: spark.table() throws eagerly
        try:
            df = spark.table(table_name)
            return OptionalDataFrame(
                step=step, id=value_id, table_name=table_name, _df=df,
            )
        except Exception as e:
            if "AnalysisException" in type(e).__name__ or "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                return OptionalDataFrame(
                    step=step,
                    id=value_id,
                    table_name=table_name,
                    _error=MissingTableError(
                        step=step, id=value_id, table_name=table_name,
                    ),
                )
            raise


# ---------------------------------------------------------------------------
# Block-level Spark configuration
# ---------------------------------------------------------------------------


@contextmanager
def spark_conf(settings: dict[str, str], *, spark=None, lenient: bool = False):
    """Temporarily override Spark SQL settings, restoring originals on exit.

    Captures the current value (or absence) of each key before applying
    overrides.  On exit -- even if the managed block raises -- every key
    is restored to its original value, or unset if it was not previously set.

    Parameters
    ----------
    settings
        Keys and string values to override for the duration of the block.
    spark
        SparkSession.  If None, resolved via ``_resolve_spark``.
    lenient
        When True, config-not-available errors at ``set()`` time are logged
        at WARNING and skipped.  Only keys that were successfully overridden
        go into the restore set.  When False (default), errors propagate.

        Use ``lenient=True`` for notebooks that run on both classic clusters
        (all keys settable) and serverless (restricted whitelist).

    Nesting is safe: each context manager captures and restores its own
    originals independently.
    """
    spark = spark or _resolve_spark(get_context())

    # Phase 1: capture originals
    originals: dict[str, str | None] = {}
    for key in settings:
        try:
            originals[key] = spark.conf.get(key)
        except Exception:
            originals[key] = None

    # Phase 2: apply overrides (track which keys were actually set)
    applied: dict[str, str | None] = {}
    for key, value in settings.items():
        try:
            spark.conf.set(key, str(value))
            applied[key] = originals[key]
            logger.info("spark_conf: set %s = %s", key, value)
        except Exception as exc:
            if lenient:
                logger.warning("spark_conf: skipping %s (not settable): %s", key, exc)
            else:
                raise

    try:
        yield
    finally:
        # Phase 3: restore only keys that were successfully overridden
        errors: list[tuple[str, Exception]] = []
        for key, prev in applied.items():
            try:
                if prev is None:
                    spark.conf.unset(key)
                else:
                    spark.conf.set(key, prev)
                logger.info("spark_conf: restored %s", key)
            except Exception as exc:
                errors.append((key, exc))
        if errors:
            summary = "; ".join(f"{k}: {e}" for k, e in errors)
            raise RuntimeError(
                f"spark_conf failed to restore {len(errors)} key(s): {summary}"
            ) from errors[0][1]


# ---------------------------------------------------------------------------
# Dynamic parallelism
# ---------------------------------------------------------------------------

def cluster_cores(*, spark=None, cores_per_worker: int | None = None) -> int:
    """Discover total cluster cores.

    Discovery strategy by environment:

    - **Databricks cluster:** worker count from
      ``spark.databricks.clusterUsageTags.clusterWorkers`` multiplied by
      cores per worker (probed via a single-row UDF job if *cores_per_worker*
      is not provided).
    - **Local Spark:** ``os.cpu_count()`` on the driver (no workers).
    - **Serverless:** raises -- no fixed cluster to probe.

    Not cached -- autoscaling clusters change worker count over time.
    Callers who need a stable value for the duration of a computation
    should capture the result themselves.

    Parameters
    ----------
    spark
        SparkSession.  If None, resolved via ``_resolve_spark``.
    cores_per_worker
        If provided, skip the UDF probe and use this value.
    """
    spark = spark or _resolve_spark(get_context())

    # Try Databricks cluster tags
    try:
        workers = int(spark.conf.get(
            "spark.databricks.clusterUsageTags.clusterWorkers"))
    except Exception:
        if _is_spark_connect(spark):
            raise RuntimeError(
                "cluster_cores: Spark Connect session but cluster tags not "
                "available via spark.conf. Provide cores_per_worker= explicitly, "
                "or use cores= on core_based_parallelism to skip discovery."
            )
        # Local Spark -- no cluster tags, use driver CPU count
        cores = os.cpu_count() or 1
        logger.info("cluster_cores: local Spark, using os.cpu_count() = %d", cores)
        return cores

    if workers == 0:
        raise RuntimeError(
            "cluster_cores: clusterWorkers is 0. "
            "This could be a single-node cluster or serverless environment. "
            "Provide cores_per_worker= explicitly, or use cores= on "
            "core_based_parallelism to skip discovery."
        )

    if cores_per_worker is None:
        # UDF probe -- runs on a single executor
        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType

        @udf(returnType=IntegerType())
        def _worker_cpu_count():
            import os as _os
            return _os.cpu_count() or 1

        cores_per_worker = spark.range(1).select(
            _worker_cpu_count()
        ).collect()[0][0]
        logger.info("cluster_cores: probed cores_per_worker = %d", cores_per_worker)

    total = workers * cores_per_worker
    logger.info("cluster_cores: %d workers x %d cores = %d total",
                workers, cores_per_worker, total)
    return total


def core_based_parallelism(
    *,
    min_parallelism: int,
    max_parallelism: int,
    compute: Callable[[int], int],
    spark=None,
    cores_per_worker: int | None = None,
    cores: int | None = None,
) -> int:
    """Compute a parallelism level based on cluster core count, clamped to [min_parallelism, max_parallelism].

    Parameters
    ----------
    min_parallelism
        Minimum parallelism (floor).
    max_parallelism
        Maximum parallelism (ceiling).
    compute
        Function receiving total cores and returning desired parallelism.
    spark
        SparkSession.  If None, resolved via ``_resolve_spark``.
    cores_per_worker
        Passed to ``cluster_cores`` if *cores* is not provided.
    cores
        Pre-computed core count (skip discovery entirely).
    """
    if cores is None:
        cores = cluster_cores(spark=spark, cores_per_worker=cores_per_worker)

    raw = compute(cores)
    clamped = max(min_parallelism, min(max_parallelism, int(raw)))
    logger.info("core_based_parallelism: cores=%d, raw=%d, clamped=%d (min=%d, max=%d)",
                cores, raw, clamped, min_parallelism, max_parallelism)
    return clamped


# ===========================================================================
# Internal: error types, helpers, parsing
# ===========================================================================


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------


class MissingDataFrameError(Exception):
    """A required DataFrame is not available.

    Raised by ``OptionalDataFrame.get()`` when the wrapped DataFrame is
    ``None``.  Carries metadata (``step``, ``id``) so that error messages
    identify the source without the caller needing to know those details.
    """

    def __init__(
        self,
        *,
        step: str | None = None,
        id: str | None = None,
        message: str | None = None,
    ) -> None:
        self.step = step
        self.id = id
        if message is None:
            parts: list[str] = []
            if step:
                parts.append(f"step '{step}'")
            if id and id != step:
                parts.append(f"id '{id}'")
            label = ", ".join(parts) if parts else "unknown source"
            message = f"DataFrame not available ({label})"
        super().__init__(message)


class MissingTableError(MissingDataFrameError):
    """A required table does not exist.

    Subclass of ``MissingDataFrameError`` that additionally carries the
    ``table_name``.  Produced by ``read_or_compute_table_step`` when a
    disabled step's table is not found.
    """

    def __init__(
        self,
        *,
        step: str | None = None,
        id: str | None = None,
        table_name: str | None = None,
    ) -> None:
        self.table_name = table_name
        parts: list[str] = []
        if table_name:
            parts.append(f"table '{table_name}'")
        if step:
            parts.append(f"step '{step}'")
        if parts:
            message = f"Table not found ({', '.join(parts)})"
        else:
            message = "Table not found"
        super().__init__(step=step, id=id, message=message)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_spark_connect(spark) -> bool:
    """Detect whether *spark* is a Spark Connect session."""
    return type(spark).__module__.startswith("pyspark.sql.connect")


def _call_with_optional_context(fn: Callable[..., Any], ctx: Context | None) -> Any:
    """Call *fn* with context if it accepts a parameter, otherwise without."""
    sig = inspect.signature(fn)
    params = [
        p
        for p in sig.parameters.values()
        if p.default is inspect.Parameter.empty
        and p.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    if len(params) >= 1:
        if ctx is None:
            raise RuntimeError(
                f"Callable {fn} requires context but no context is available. "
                f"Set a context via set_context() or pass context= explicitly."
            )
        return fn(ctx)
    return fn()


def _resolve_spark(ctx: Context | None) -> Any:
    """Resolve SparkSession: context, then runtime global, then active session."""
    # 1. From pipeline context
    if ctx is not None:
        spark = ctx.get("spark")
        if spark is not None:
            return spark
    # 2. From runtime-injected global (Databricks notebooks inject `spark`
    #    into builtins; included code sees it there)
    import builtins
    spark = getattr(builtins, "spark", None)
    if spark is not None and hasattr(spark, "conf") and hasattr(spark, "table"):
        return spark
    # 3. From active session (covers local Spark, Databricks Connect)
    from pyspark.sql import SparkSession
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("No active SparkSession found")
    return session


_TABLE_NAME_RE = re.compile(r"^[\w]+(?:\.[\w]+)*$")


def _validate_table_name(name: str) -> None:
    """Validate that *name* is a dotted identifier (e.g., 'catalog.schema.table')."""
    if not _TABLE_NAME_RE.match(name):
        raise ValueError(
            f"Invalid table name: {name!r}. "
            f"Expected dotted identifier (e.g., 'catalog.schema.table')."
        )


def _materialize_and_read(spark: Any, table_name: str, result: Any) -> Any:
    """Materialize a compute result (DataFrame or DataFrameWriter) as a table.

    Two result types:

    - **DataFrameWriter** (has ``saveAsTable`` but no ``collect``): the user
      configured the writer (mode, format, partitioning).  The framework
      calls ``saveAsTable`` and respects whatever the user set.
    - **DataFrame**: the framework owns the write strategy and uses
      ``mode("overwrite")`` as the default -- appropriate for a cached
      computed value where recompute should replace the previous result.
    """
    _validate_table_name(table_name)
    # Check if result is a DataFrameWriter (has saveAsTable method but no collect)
    if hasattr(result, "saveAsTable") and not hasattr(result, "collect"):
        result.saveAsTable(table_name)
    else:
        # Assume DataFrame -- overwrite is the right default for cached values
        result.write.mode("overwrite").saveAsTable(table_name)

    logger.info("Computed and saved: %s", table_name)
    # On Spark Connect, spark.table() returns a lazy reference. The caller's
    # first action on the returned DataFrame will trigger plan evaluation.
    return spark.table(table_name)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_json_or_dict(
    value: str | dict[str, Any], label: str
) -> dict[str, Any] | str:
    """Parse a JSON string or pass through a dict."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return value


def _try_parse_json(value: str) -> Any:
    """Try to parse *value* as JSON; return as-is if it fails."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    try:
        return json.loads(stripped)
    except (json.JSONDecodeError, TypeError):
        return value


def _parse_flexible_set(value: Any) -> set[str]:
    """Parse a flexible input into a set of names.

    Accepts: list, comma-delimited string, JSON array string, or a single
    string name.
    """
    if isinstance(value, set):
        return value
    if isinstance(value, (list, tuple)):
        return set(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return set()
        # Try JSON array
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return set(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
        # Comma-delimited
        return {s.strip() for s in stripped.split(",") if s.strip()}
    return set()


def _parse_step_config(
    value: Any, known_steps: set[str]
) -> dict[str, bool]:
    """Parse flexible step configuration into explicit on/off mapping.

    Accepts: "ALL", comma-delimited, JSON array, JSON object, list, dict.
    """
    if isinstance(value, dict):
        return {k: bool(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return {s: True for s in value}

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        if stripped.upper() == "ALL":
            return {s: True for s in known_steps}

        # Try JSON
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                return {k: bool(v) for k, v in parsed.items()}
            if isinstance(parsed, list):
                return {s: True for s in parsed}
        except (json.JSONDecodeError, TypeError):
            pass

        # Comma-delimited
        return {s.strip(): True for s in stripped.split(",") if s.strip()}

    return {}
