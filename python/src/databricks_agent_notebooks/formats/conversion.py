"""Normalize various notebook formats to standard ipynb (NotebookNode).

This module is the central dispatch hub: markdown, ipynb, and Databricks Source
Format files all converge here into a single ``NotebookNode`` representation
with consistent kernel metadata. Downstream phases (injection, execution,
rendering) consume only NotebookNode objects.
"""

from __future__ import annotations

from pathlib import Path

import jupytext
import nbformat
from nbformat import NotebookNode

from databricks_agent_notebooks._constants import KERNELSPECS
from databricks_agent_notebooks.config.frontmatter import AgentNotebookConfig, parse_frontmatter
from databricks_agent_notebooks.formats.dbr_source import (
    detect as dbr_detect,
    detected_language as dbr_detected_language,
    parse as dbr_parse,
)


# ---------------------------------------------------------------------------
# Helper predicates
# ---------------------------------------------------------------------------


def is_notebook(path: Path) -> bool:
    """Return True if *path* has the ``.ipynb`` extension."""
    return path.suffix == ".ipynb"


def is_markdown(path: Path) -> bool:
    """Return True if *path* has the ``.md`` extension."""
    return path.suffix == ".md"


def is_dbr_source(path: Path) -> bool:
    """Return True if *path* is a Databricks Source Format file.

    Delegates to the ``_dbr_source`` module, which inspects the first line
    of the file for a known header pattern.
    """
    return dbr_detect(path)


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------


def detect_language(notebook: NotebookNode) -> str | None:
    """Detect the primary language of a notebook.

    Checks kernel metadata first (the authoritative source when present).
    Falls back to scanning code cells for language hints when kernel metadata
    is absent.

    Returns ``"python"``, ``"scala"``, ``"sql"``, or ``None``.
    """
    # 1. Kernel metadata
    lang = notebook.metadata.get("kernelspec", {}).get("language")
    if lang:
        return lang

    # 2. Scan code cells for language hints
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        first_line = source.strip().split("\n", 1)[0].strip().lower() if source.strip() else ""
        # Databricks-style magic language hints (%python, %scala, %sql)
        if first_line.startswith("%python"):
            return "python"
        if first_line.startswith("%scala"):
            return "scala"
        if first_line.startswith("%sql"):
            return "sql"
        # Heuristic: Python-looking code
        if any(kw in source for kw in ("def ", "import ", "print(")):
            return "python"
        # Heuristic: Scala-looking code
        if any(kw in source for kw in ("val ", "var ", "println(", "object ")):
            return "scala"
        # Heuristic: SQL-looking code
        if any(kw in source.upper() for kw in ("SELECT ", "INSERT ", "CREATE TABLE")):
            return "sql"

    return None


def validate_single_language(notebook: NotebookNode) -> None:
    """Raise ``ValueError`` if code cells use more than one language.

    This is a consistency check, not detection. It inspects magic-line
    language directives (``%python``, ``%scala``, ``%sql``) across all
    code cells. A notebook that mixes ``%python`` and ``%scala`` cells,
    for example, will fail validation.
    """
    languages: set[str] = set()

    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        first_line = source.strip().split("\n", 1)[0].strip().lower() if source.strip() else ""
        if first_line.startswith("%python"):
            languages.add("python")
        elif first_line.startswith("%scala"):
            languages.add("scala")
        elif first_line.startswith("%sql"):
            languages.add("sql")

    if len(languages) > 1:
        raise ValueError(
            f"Notebook contains mixed languages: {', '.join(sorted(languages))}. "
            "All code cells must use a single language."
        )


# ---------------------------------------------------------------------------
# Main conversion entry point
# ---------------------------------------------------------------------------


def _set_kernel_metadata(notebook: NotebookNode, language: str) -> None:
    """Set kernel metadata on *notebook* for the given *language*."""
    if language in KERNELSPECS:
        notebook.metadata["kernelspec"] = KERNELSPECS[language]


def to_notebook(path: Path) -> tuple[NotebookNode, AgentNotebookConfig | None]:
    """Normalize any supported file format into an ipynb ``NotebookNode``.

    Supported formats:

    * ``.md`` -- Markdown with optional YAML frontmatter.  Parsed via
      Jupytext; frontmatter yields a ``AgentNotebookConfig``.
    * ``.ipynb`` -- Native Jupyter notebook.  Read as-is via nbformat.
    * ``.py`` / ``.scala`` / ``.sql`` -- Databricks Source Format files
      (identified by their header line).

    Returns:
        A ``(notebook, config)`` tuple.  *config* is a ``AgentNotebookConfig``
        for markdown files (possibly all-None if no frontmatter) and ``None``
        for all other formats.

    Raises:
        ValueError: If the file format is unrecognised or a ``.py`` /
            ``.scala`` / ``.sql`` file is not in Databricks Source Format.
    """
    suffix = path.suffix.lower()

    # ── Markdown ──────────────────────────────────────────────────────
    if suffix == ".md":
        config = parse_frontmatter(path)
        notebook = jupytext.read(str(path))

        # Determine language: frontmatter > cell detection
        language = config.language or detect_language(notebook)
        if language:
            _set_kernel_metadata(notebook, language)

        return notebook, config

    # ── Native ipynb ──────────────────────────────────────────────────
    if suffix == ".ipynb":
        with open(path, encoding="utf-8") as f:
            notebook = nbformat.read(f, as_version=4)
        return notebook, None

    # ── Databricks Source Format (.py, .scala, .sql) ──────────────────
    if suffix in (".py", ".scala", ".sql"):
        if not dbr_detect(path):
            raise ValueError(
                f"File {path.name} is not in Databricks Source Format. "
                "Only Databricks source files are supported for "
                f"{suffix} extension."
            )
        notebook = dbr_parse(path)
        return notebook, None

    # ── Unsupported ───────────────────────────────────────────────────
    raise ValueError(f"Unsupported file format: {suffix}")
