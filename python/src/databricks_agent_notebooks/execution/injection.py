"""Databricks Connect session injection for notebooks.

Injects a single setup code cell at the beginning of a notebook that
initializes a Databricks Connect session.  The injected cell is tagged
with metadata so downstream renderers can identify (and optionally hide) it.

Supports both **Scala** (ivy import + DatabricksSession builder) and
**Python** (``from databricks.connect import DatabricksSession``) code
generation, selected automatically from the notebook's kernel metadata.
"""

from __future__ import annotations

from pathlib import Path

import nbformat
from nbformat import NotebookNode

from databricks_agent_notebooks._constants import (
    DATABRICKS_CONNECT_213_VERSION,
    DATABRICKS_CONNECT_VERSION,
    SCALA_212,
    ScalaVariant,
)
from databricks_agent_notebooks.config.frontmatter import DatabricksConfig
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
    if config.cluster is None:
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
# Public API
# ---------------------------------------------------------------------------


def generate_setup_code(
    config: DatabricksConfig,
    lineage: ExecutionLineage,
    language: str = "scala",
    *,
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
    scala_connect_version:
        When provided, overrides the default Databricks Connect version
        in the Scala ``$ivy`` import.  Ignored for Python/SQL.
    scala_variant:
        Scala variant controlling ``$ivy`` syntax and fallback version.
        Ignored for Python/SQL.
    """
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
    scala_connect_version: str | None = None,
    scala_variant: ScalaVariant | None = None,
) -> NotebookNode:
    """Inject a Databricks Connect setup cell at the top of *notebook*.

    The language is detected from the notebook's kernel metadata, falling
    back to ``config.language``, then ``"scala"`` as the default.

    Idempotent: if the first cell already carries the injected-cell tag,
    it is replaced rather than duplicated.

    Parameters
    ----------
    scala_connect_version:
        When provided, overrides the default Databricks Connect version
        in the Scala ``$ivy`` import.  Typically resolved dynamically
        from the cluster's DBR version.
    scala_variant:
        Scala variant controlling ``$ivy`` syntax and fallback version.
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
