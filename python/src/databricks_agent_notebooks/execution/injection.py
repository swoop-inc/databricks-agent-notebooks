"""Notebook session injection — Databricks Connect and local Spark.

Injects a single setup code cell at the beginning of a notebook that
initializes either a Databricks Connect session or a local PySpark /
vanilla Spark session.  The injected cell is tagged with metadata so
downstream renderers can identify (and optionally hide) it.

Supports both **Scala** (ivy import + session builder) and **Python**
code generation, selected automatically from the notebook's kernel metadata.
When the reserved ``LOCAL_SPARK`` profile is active, the injected code
creates a standalone ``SparkSession`` instead of a ``DatabricksSession``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import nbformat
from nbformat import NotebookNode

from databricks_agent_notebooks._constants import (
    DATABRICKS_CONNECT_213_VERSION,
    DATABRICKS_CONNECT_VERSION,
    LOCAL_SPARK_DEFAULT_MASTER,
    LOCAL_SPARK_DEFAULT_VERSION,
    SCALA_212,
    ScalaVariant,
)
from databricks_agent_notebooks.config.frontmatter import DatabricksConfig, is_local_spark
from databricks_agent_notebooks.execution.lineage import ExecutionLineage, capture_pre_execution


def is_injected_cell(cell: NotebookNode) -> bool:
    """Return True if *cell* was injected by this module."""
    return cell.metadata.get("agent_notebook_injected", False)


# ---------------------------------------------------------------------------
# Source-comment builder (shared by both languages)
# ---------------------------------------------------------------------------


def _build_source_parts(
    config: DatabricksConfig, lineage: ExecutionLineage,
) -> list[str]:
    """Build the provenance metadata parts list."""
    source = lineage.source_path or "unknown"
    parts = [f"Source: {source}"]
    if lineage.timestamp:
        parts.append(lineage.timestamp)
    if lineage.git_branch or lineage.git_commit:
        git_part = "branch: "
        if lineage.git_branch:
            git_part += lineage.git_branch
        if lineage.git_commit:
            git_part += f" @ {lineage.git_commit}"
        parts.append(git_part)
    # LOCAL_SPARK mode appends its own label in the language-specific generators
    if not is_local_spark(config) and config.cluster is None:
        parts.append("Serverless")
    return parts


# ---------------------------------------------------------------------------
# Scala code generation
# ---------------------------------------------------------------------------


def _generate_scala_setup(
    config: DatabricksConfig,
    lineage: ExecutionLineage,
    *,
    connect_version: str | None = None,
    variant: ScalaVariant | None = None,
) -> str:
    """Build the Scala setup code string.

    Parameters
    ----------
    connect_version:
        Exact Databricks Connect version (e.g. ``"16.4.7"``).  When *None*,
        falls back to the constant matching the active *variant*.
    variant:
        Scala variant controlling ``$ivy`` import syntax and fallback version.
        Defaults to :data:`SCALA_212` for backward compatibility.
    """
    effective_variant = variant or SCALA_212
    if connect_version is not None:
        version = connect_version
    elif effective_variant.scala_version == "2.13":
        version = DATABRICKS_CONNECT_213_VERSION
    else:
        version = DATABRICKS_CONNECT_VERSION
    parts = _build_source_parts(config, lineage)
    parts.append(f"DB Connect: {version}")
    source_comment = f"// {' | '.join(parts)}"

    imports = (
        f'import $ivy.`com.databricks{effective_variant.ivy_separator}databricks-connect:{version}`\n'
        "import com.databricks.connect.DatabricksSession\n"
        "import com.databricks.sdk.core.DatabricksConfig"
    )

    if config.profile is not None:
        sdk_config = f'new DatabricksConfig().setProfile("{config.profile}")'
    else:
        sdk_config = "new DatabricksConfig()"

    if config.cluster is not None:
        builder = (
            "val spark = DatabricksSession.builder()\n"
            f"  .sdkConfig({sdk_config})\n"
            f'  .clusterId("{config.cluster}")\n'
            "  .getOrCreate()"
        )
    else:
        builder = (
            "val spark = DatabricksSession.builder()\n"
            f"  .sdkConfig({sdk_config})\n"
            "  .serverless()\n"
            "  .getOrCreate()"
        )

    return (
        "// [AGENT-NOTEBOOK:INJECTED] \u2014 auto-generated, do not edit\n"
        f"{source_comment}\n"
        f"{imports}\n"
        f"{builder}"
    )


# ---------------------------------------------------------------------------
# Python code generation
# ---------------------------------------------------------------------------


def _generate_python_setup(config: DatabricksConfig, lineage: ExecutionLineage) -> str:
    """Build the Python setup code string."""
    parts = _build_source_parts(config, lineage)
    source_comment = f"# {' | '.join(parts)}"

    lines = [
        "# [AGENT-NOTEBOOK:INJECTED] \u2014 auto-generated, do not edit",
        source_comment,
        "from databricks.connect import DatabricksSession",
    ]

    # Build the chained builder expression
    builder = "spark = DatabricksSession.builder"
    if config.profile is not None:
        builder += f'.profile("{config.profile}")'
    if config.cluster is not None:
        builder += f'.clusterId("{config.cluster}")'
    else:
        builder += ".serverless()"
    builder += ".getOrCreate()"

    lines.append(builder)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Local Spark code generation (LOCAL_SPARK profile)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LocalSparkEnv:
    """Resolved environment settings for local Spark execution."""

    master: str
    driver_memory: str | None
    executor_memory: str | None
    spark_version: str  # Scala only: version for $ivy import


def _resolve_local_spark_env() -> LocalSparkEnv:
    """Read local Spark tuning knobs from environment variables.

    Centralises env var names so both generators share one resolution point.
    When memory env vars are unset the corresponding SparkSession builder
    calls are omitted entirely, letting Spark use its own defaults.
    """
    return LocalSparkEnv(
        master=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", LOCAL_SPARK_DEFAULT_MASTER),
        driver_memory=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY"),
        executor_memory=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY"),
        spark_version=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_VERSION", LOCAL_SPARK_DEFAULT_VERSION),
    )


def _generate_python_local_setup(config: DatabricksConfig, lineage: ExecutionLineage) -> str:
    """Build Python setup code for a local SparkSession (no Databricks)."""
    env = _resolve_local_spark_env()
    parts = _build_source_parts(config, lineage)
    parts.append(f"Local Spark: {env.master}")
    source_comment = f"# {' | '.join(parts)}"

    lines = [
        "# [AGENT-NOTEBOOK:INJECTED] \u2014 auto-generated, do not edit",
        source_comment,
        "from pyspark.sql import SparkSession",
    ]

    builder = f'spark = SparkSession.builder.master("{env.master}").appName("agent-notebook")'
    if env.driver_memory:
        builder += f'.config("spark.driver.memory", "{env.driver_memory}")'
    if env.executor_memory:
        builder += f'.config("spark.executor.memory", "{env.executor_memory}")'
    builder += ".getOrCreate()"

    lines.append(builder)
    return "\n".join(lines)


def _generate_scala_local_setup(config: DatabricksConfig, lineage: ExecutionLineage) -> str:
    """Build Scala setup code for a local SparkSession via ``$ivy`` import."""
    env = _resolve_local_spark_env()
    parts = _build_source_parts(config, lineage)
    parts.append(f"Local Spark: {env.master} ({env.spark_version})")
    source_comment = f"// {' | '.join(parts)}"

    # `::` (double-colon) is correct — spark-sql is Scala-cross-published.
    # Almond resolves the Scala binary version from the running kernel.
    ivy_import = f'import $ivy.`org.apache.spark::spark-sql:{env.spark_version}`'
    scala_imports = "import org.apache.spark.sql.SparkSession"

    builder_lines = [
        "val spark = SparkSession.builder()",
        f'  .master("{env.master}")',
        '  .appName("agent-notebook")',
    ]
    if env.driver_memory:
        builder_lines.append(f'  .config("spark.driver.memory", "{env.driver_memory}")')
    # executor_memory is intentionally omitted for Scala — there are no separate
    # executor processes in any valid Scala local mode (the CLI rejects it).
    builder_lines.append("  .getOrCreate()")
    # PySpark sets WARN by default; Almond does not — suppress INFO log noise.
    builder_lines.append('spark.sparkContext.setLogLevel("WARN")')

    return (
        "// [AGENT-NOTEBOOK:INJECTED] \u2014 auto-generated, do not edit\n"
        f"{source_comment}\n"
        f"{ivy_import}\n"
        f"{scala_imports}\n"
        + "\n".join(builder_lines)
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_setup_code(
    config: DatabricksConfig,
    lineage: ExecutionLineage,
    language: str = "scala",
    *,
    local_spark: bool = False,
    scala_connect_version: str | None = None,
    scala_variant: ScalaVariant | None = None,
) -> str:
    """Build the session setup code for the injected cell.

    Parameters
    ----------
    config:
        Databricks connection configuration (profile, cluster).
    lineage:
        Execution provenance metadata for the source comment.
    language:
        Target language — ``"scala"``, ``"python"``, or ``"sql"``.
        SQL notebooks use Python for execution (spark.sql wrapper),
        so ``"sql"`` produces the same output as ``"python"``.
    local_spark:
        When True, generate a local ``SparkSession`` instead of a
        ``DatabricksSession``.  Activated by the reserved
        ``LOCAL_SPARK`` profile name.
    scala_connect_version:
        When provided, overrides the default Databricks Connect version
        in the Scala ``$ivy`` import.  Ignored for Python/SQL and
        local Spark mode.
    scala_variant:
        Scala variant controlling ``$ivy`` syntax and fallback version.
        Ignored for Python/SQL and local Spark mode.
    """
    if local_spark:
        if language in ("python", "sql"):
            return _generate_python_local_setup(config, lineage)
        if language == "scala":
            return _generate_scala_local_setup(config, lineage)
        raise ValueError(f"Unsupported language for local Spark code generation: {language!r}")

    if language in ("python", "sql"):
        return _generate_python_setup(config, lineage)
    if language == "scala":
        return _generate_scala_setup(
            config, lineage, connect_version=scala_connect_version, variant=scala_variant,
        )
    raise ValueError(f"Unsupported language for code generation: {language!r}")


def inject_cells(
    notebook: NotebookNode,
    config: DatabricksConfig,
    source_path: Path | None = None,
    *,
    local_spark: bool = False,
    scala_connect_version: str | None = None,
    scala_variant: ScalaVariant | None = None,
) -> NotebookNode:
    """Inject a session setup cell at the top of *notebook*.

    Injects either a Databricks Connect session or a local SparkSession,
    depending on the *local_spark* flag.

    The language is detected from the notebook's kernel metadata, falling
    back to ``config.language``, then ``"scala"`` as the default.

    Idempotent: if the first cell already carries the injected-cell tag,
    it is replaced rather than duplicated.

    Parameters
    ----------
    local_spark:
        When True, inject a local ``SparkSession`` instead of a
        ``DatabricksSession``.
    scala_connect_version:
        When provided, overrides the default Databricks Connect version
        in the Scala ``$ivy`` import.  Typically resolved dynamically
        from the cluster's DBR version.  Ignored in local Spark mode.
    scala_variant:
        Scala variant controlling ``$ivy`` syntax and fallback version.
        Ignored in local Spark mode.
    """
    language = (
        notebook.metadata.get("kernelspec", {}).get("language")
        or config.language
        or "scala"
    )
    # SQL notebooks use Python for execution (spark.sql wrapper)
    if language == "sql":
        language = "python"

    lineage = capture_pre_execution(source_path)
    code = generate_setup_code(
        config, lineage, language=language,
        local_spark=local_spark,
        scala_connect_version=scala_connect_version,
        scala_variant=scala_variant,
    )

    cell = nbformat.v4.new_code_cell(code)
    cell.metadata["agent_notebook_injected"] = True

    if notebook.cells and is_injected_cell(notebook.cells[0]):
        notebook.cells[0] = cell
    else:
        notebook.cells.insert(0, cell)

    return notebook
