"""Notebook session injection -- Databricks Connect and local Spark.

Injects lifecycle cells at the beginning of a notebook:

1. **parameters_setup** -- singleton hidden cell exposing all resolved config
   as ``agent_notebook_parameters`` (Python only; Scala deferred).
2. **session_setup** -- 0..1 cells creating a SparkSession or
   DatabricksSession (controlled by ``--no-inject-session``).
3. **prologue** -- 0..* user-defined cells from ``hooks.<language>.prologue_cells``.

All injected cells carry ``agent_notebook_injected = True`` metadata unless
they are explicitly visible (fenced markdown or fenced code cells in prologue).
Each cell also carries ``agent_notebook_cell_role`` for progress labeling.

Supports both **Scala** (ivy import + session builder) and **Python**
code generation, selected automatically from the notebook's kernel metadata.
When the reserved ``LOCAL_SPARK`` profile is active, the injected code
creates a standalone ``SparkSession`` instead of a ``DatabricksSession``.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

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
from databricks_agent_notebooks.config.frontmatter import AgentNotebookConfig, is_local_spark
from databricks_agent_notebooks.execution.lineage import ExecutionLineage, capture_pre_execution


def is_injected_cell(cell: NotebookNode) -> bool:
    """Return True if *cell* was injected by this module."""
    return cell.metadata.get("agent_notebook_injected", False)


_LIFECYCLE_ROLES = frozenset({"parameters", "session", "prologue", "epilogue"})


def _is_lifecycle_cell(cell: NotebookNode) -> bool:
    """Return True if *cell* is any lifecycle cell (hidden or visible)."""
    if is_injected_cell(cell):
        return True
    return cell.metadata.get("agent_notebook_cell_role") in _LIFECYCLE_ROLES


# ---------------------------------------------------------------------------
# Source-comment builder (shared by both languages)
# ---------------------------------------------------------------------------


def _build_source_parts(
    config: AgentNotebookConfig, lineage: ExecutionLineage,
    *, local_spark: bool = False,
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
    # Local Spark mode appends its own label in the language-specific generators.
    # Check both the explicit local_spark flag (from --cluster "local[N]") and
    # the legacy profile detection (from --profile LOCAL_SPARK).
    if not local_spark and not is_local_spark(config) and config.cluster is None:
        parts.append("Serverless")
    return parts


# ---------------------------------------------------------------------------
# User-provided library path injection
# ---------------------------------------------------------------------------


def _build_library_path_lines(libraries: tuple[str, ...] | None) -> list[str]:
    """Build sys.path.insert lines for user-provided library paths."""
    if not libraries:
        return []
    lines = ["import sys"]
    for lib_path in libraries:
        lines.append(f"if {lib_path!r} not in sys.path: sys.path.insert(0, {lib_path!r})")
    return lines


# ---------------------------------------------------------------------------
# Scala code generation
# ---------------------------------------------------------------------------


def _generate_scala_setup(
    config: AgentNotebookConfig,
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


def _generate_python_setup(config: AgentNotebookConfig, lineage: ExecutionLineage) -> str:
    """Build the Python setup code string."""
    parts = _build_source_parts(config, lineage)
    source_comment = f"# {' | '.join(parts)}"

    lines = [
        "# [AGENT-NOTEBOOK:INJECTED] \u2014 auto-generated, do not edit",
        source_comment,
    ]
    lines.extend(_build_library_path_lines(config.libraries))
    lines.append("from databricks.connect import DatabricksSession")

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


def _resolve_local_spark_env(master_override: str | None = None) -> LocalSparkEnv:
    """Read local Spark tuning knobs from environment variables.

    When *master_override* is provided (e.g. from ``--cluster "local[2]"``),
    it takes precedence over the ``AGENT_NOTEBOOK_LOCAL_SPARK_MASTER`` env var.

    Centralises env var names so both generators share one resolution point.
    When memory env vars are unset the corresponding SparkSession builder
    calls are omitted entirely, letting Spark use its own defaults.
    """
    if master_override is not None:
        master = master_override
    else:
        master = os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", LOCAL_SPARK_DEFAULT_MASTER)
    return LocalSparkEnv(
        master=master,
        driver_memory=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY"),
        executor_memory=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY"),
        spark_version=os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_VERSION", LOCAL_SPARK_DEFAULT_VERSION),
    )


def _generate_python_local_setup(
    config: AgentNotebookConfig,
    lineage: ExecutionLineage,
    *,
    master_override: str | None = None,
) -> str:
    """Build Python setup code for a local SparkSession (no Databricks)."""
    env = _resolve_local_spark_env(master_override)
    parts = _build_source_parts(config, lineage, local_spark=True)
    parts.append(f"Local Spark: {env.master}")
    source_comment = f"# {' | '.join(parts)}"

    lines = [
        "# [AGENT-NOTEBOOK:INJECTED] \u2014 auto-generated, do not edit",
        source_comment,
    ]
    lines.extend(_build_library_path_lines(config.libraries))
    lines.append("from pyspark.sql import SparkSession")

    builder = f'spark = SparkSession.builder.master("{env.master}").appName("agent-notebook")'
    if env.driver_memory:
        builder += f'.config("spark.driver.memory", "{env.driver_memory}")'
    if env.executor_memory:
        builder += f'.config("spark.executor.memory", "{env.executor_memory}")'
    builder += ".getOrCreate()"

    lines.append(builder)
    return "\n".join(lines)


def _generate_scala_local_setup(
    config: AgentNotebookConfig,
    lineage: ExecutionLineage,
    *,
    master_override: str | None = None,
) -> str:
    """Build Scala setup code for a local SparkSession via ``$ivy`` import."""
    env = _resolve_local_spark_env(master_override)
    parts = _build_source_parts(config, lineage, local_spark=True)
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
    config: AgentNotebookConfig,
    lineage: ExecutionLineage,
    language: str = "scala",
    *,
    local_spark: bool = False,
    master_override: str | None = None,
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
        ``LOCAL_SPARK`` profile name or ``--cluster "local[N]"``.
    master_override:
        Explicit Spark master URL from ``--cluster``.  When provided,
        takes precedence over the ``AGENT_NOTEBOOK_LOCAL_SPARK_MASTER``
        env var.  Only used in local Spark mode.
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
            return _generate_python_local_setup(config, lineage, master_override=master_override)
        if language == "scala":
            return _generate_scala_local_setup(config, lineage, master_override=master_override)
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
    config: AgentNotebookConfig,
    source_path: Path | None = None,
    *,
    local_spark: bool = False,
    master_override: str | None = None,
    scala_connect_version: str | None = None,
    scala_variant: ScalaVariant | None = None,
    language: str | None = None,
) -> NotebookNode:
    """Inject a session setup cell at the top of *notebook*.

    Injects either a Databricks Connect session or a local SparkSession,
    depending on the *local_spark* flag.

    When *language* is provided it is used directly; otherwise it is
    detected from ``config.language``, falling back to the notebook's
    kernel metadata, then ``"python"``.

    Idempotent: if the first cell already carries the injected-cell tag,
    it is replaced rather than duplicated.

    Parameters
    ----------
    local_spark:
        When True, inject a local ``SparkSession`` instead of a
        ``DatabricksSession``.
    master_override:
        Explicit Spark master URL from ``--cluster``.  Only used in
        local Spark mode.
    scala_connect_version:
        When provided, overrides the default Databricks Connect version
        in the Scala ``$ivy`` import.  Typically resolved dynamically
        from the cluster's DBR version.  Ignored in local Spark mode.
    scala_variant:
        Scala variant controlling ``$ivy`` syntax and fallback version.
        Ignored in local Spark mode.
    language:
        Execution language override.  When provided, skips kernel
        metadata / config detection entirely.
    """
    if language is None:
        language = (
            config.language
            or notebook.metadata.get("kernelspec", {}).get("language")
            or "python"
        )
    # SQL notebooks use Python for execution (spark.sql wrapper)
    if language == "sql":
        language = "python"

    lineage = capture_pre_execution(source_path)
    code = generate_setup_code(
        config, lineage, language=language,
        local_spark=local_spark,
        master_override=master_override,
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


# ---------------------------------------------------------------------------
# Parameters setup cell
# ---------------------------------------------------------------------------

# Regex matching a fenced code block opener: 3+ backticks followed by a
# language tag.  Captures (fence, tag).
_FENCE_OPEN_RE = re.compile(r"^(`{3,})(\w+)\s*$")


def _build_parameters_dict(
    config: AgentNotebookConfig,
    notebook_params: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build the dict exposed as ``agent_notebook_parameters`` at runtime.

    Includes all non-None config fields (except ``params`` and ``hooks``,
    which are internal) plus all user-defined notebook params with their
    native types preserved.
    """
    # Framework-internal fields excluded from runtime parameters.
    _exclude = frozenset({"params", "hooks", "inject_session", "preprocess", "clean", "format"})
    result: dict[str, Any] = {}
    for f in fields(config):
        if f.name in _exclude:
            continue
        v = getattr(config, f.name)
        if v is not None:
            # Tuples are not JSON-serializable; convert to list.
            result[f.name] = list(v) if isinstance(v, tuple) else v
    if notebook_params:
        result.update(notebook_params)
    return result


def _generate_parameters_code(params_dict: dict[str, Any]) -> str:
    """Generate Python code that exposes *params_dict* as a runtime variable."""
    json_str = json.dumps(params_dict, ensure_ascii=False, sort_keys=True)
    return (
        "import json as _json\n"
        f"agent_notebook_parameters = _json.loads({json_str!r})\n"
        "del _json"
    )


def _make_parameters_cell(
    config: AgentNotebookConfig,
    notebook_params: dict[str, Any] | None,
) -> NotebookNode:
    """Create the parameters_setup cell."""
    params_dict = _build_parameters_dict(config, notebook_params)
    code = _generate_parameters_code(params_dict)
    cell = nbformat.v4.new_code_cell(code)
    cell.metadata["agent_notebook_injected"] = True
    cell.metadata["agent_notebook_cell_role"] = "parameters"
    return cell


# ---------------------------------------------------------------------------
# Prologue / epilogue cell parsing
# ---------------------------------------------------------------------------


def parse_cell_spec(spec: str, language: str) -> NotebookNode:
    """Parse a cell spec string into a NotebookNode.

    Cell type is determined by the content:

    - Starts with ``fence + "markdown"``: markdown cell (visible).
    - Starts with ``fence + language`` (e.g. ``python``): visible code cell.
    - Otherwise: hidden code cell (tagged ``agent_notebook_injected``).

    Where ``fence`` is three or more backticks.  The closing fence must
    use the same number (or more) backticks on the last line.
    """
    lines = spec.split("\n")
    first_line = lines[0] if lines else ""
    m = _FENCE_OPEN_RE.match(first_line)

    if m:
        fence_len = len(m.group(1))
        tag = m.group(2).lower()
        # Strip opening and closing fences
        body_lines = lines[1:]
        # Remove closing fence (last non-empty line matching fence length)
        if body_lines:
            last = body_lines[-1].strip()
            if re.match(rf"^`{{{fence_len},}}$", last):
                body_lines = body_lines[:-1]
        body = "\n".join(body_lines)

        if tag == "markdown":
            cell = nbformat.v4.new_markdown_cell(body)
            cell.metadata["agent_notebook_cell_role"] = "prologue"
            return cell

        if tag == language.lower():
            cell = nbformat.v4.new_code_cell(body)
            cell.metadata["agent_notebook_cell_role"] = "prologue"
            return cell

        # Fenced with an unrecognized tag: strip fences, treat as hidden code.
        # This prevents fence markers from being injected as executable code.
        cell = nbformat.v4.new_code_cell(body)
        cell.metadata["agent_notebook_injected"] = True
        cell.metadata["agent_notebook_cell_role"] = "prologue"
        return cell

    # Default: hidden code cell (unfenced content)
    cell = nbformat.v4.new_code_cell(spec)
    cell.metadata["agent_notebook_injected"] = True
    cell.metadata["agent_notebook_cell_role"] = "prologue"
    return cell


# ---------------------------------------------------------------------------
# Unified lifecycle injection
# ---------------------------------------------------------------------------


def inject_lifecycle_cells(
    notebook: NotebookNode,
    config: AgentNotebookConfig,
    source_path: Path | None = None,
    *,
    notebook_params: dict[str, Any] | None = None,
    inject_session: bool = True,
    local_spark: bool = False,
    master_override: str | None = None,
    scala_connect_version: str | None = None,
    scala_variant: ScalaVariant | None = None,
    language: str | None = None,
    preprocess_fn: Any | None = None,
) -> NotebookNode:
    """Inject all lifecycle cells at the top of *notebook*.

    Builds the full sequence:

    1. **parameters_setup** -- always (Python/SQL only; skipped for Scala).
    2. **session_setup** -- when *inject_session* is True.
    3. **prologue** -- from ``hooks.<language>.prologue_cells``.

    Idempotent: any existing injected cells at the beginning of the notebook
    are removed before new ones are prepended.

    Parameters
    ----------
    notebook_params:
        User-defined notebook params with native types (not stringified).
        These are included in ``agent_notebook_parameters`` alongside
        config fields.
    inject_session:
        When False, the session setup cell is omitted.
    preprocess_fn:
        Optional callable ``(text) -> text`` for Jinja-preprocessing
        prologue cell content.  When None, prologue cells are used as-is.
    """
    if language is None:
        language = (
            config.language
            or notebook.metadata.get("kernelspec", {}).get("language")
            or "python"
        )
    effective_language = "python" if language == "sql" else language

    # Remove existing lifecycle cells from the front (both hidden and visible).
    # Use agent_notebook_cell_role to catch visible prologue cells that lack
    # the agent_notebook_injected flag.
    while notebook.cells and _is_lifecycle_cell(notebook.cells[0]):
        notebook.cells.pop(0)

    cells_to_inject: list[NotebookNode] = []

    # 1. Parameters setup (Python/SQL only; Scala deferred)
    if effective_language == "python":
        cells_to_inject.append(_make_parameters_cell(config, notebook_params))

    # 2. Session setup
    if inject_session:
        lineage = capture_pre_execution(source_path)
        code = generate_setup_code(
            config, lineage, language=effective_language,
            local_spark=local_spark,
            master_override=master_override,
            scala_connect_version=scala_connect_version,
            scala_variant=scala_variant,
        )
        session_cell = nbformat.v4.new_code_cell(code)
        session_cell.metadata["agent_notebook_injected"] = True
        session_cell.metadata["agent_notebook_cell_role"] = "session"
        cells_to_inject.append(session_cell)

    # 3. Prologue cells
    hooks = config.hooks or {}
    lang_hooks = hooks.get(effective_language, {})
    prologue_specs = lang_hooks.get("prologue_cells", [])
    if isinstance(prologue_specs, list):
        for spec in prologue_specs:
            if not isinstance(spec, str) or not spec.strip():
                continue
            # Jinja-preprocess if a preprocessing function is provided
            content = spec
            if preprocess_fn is not None:
                content = preprocess_fn(content)
            cell = parse_cell_spec(content, effective_language)
            cells_to_inject.append(cell)

    # Prepend all lifecycle cells
    notebook.cells = cells_to_inject + notebook.cells

    return notebook
