"""Parser for the Databricks Source Format (.py / .scala / .sql exports).

Databricks notebooks can be exported as source files with a special header
and COMMAND delimiters. Jupytext explicitly declined to support this format,
so we provide our own parser that converts these files into standard
``nbformat`` notebooks.

Each language variant uses its own comment prefix for the header, delimiters,
and MAGIC lines.  The parser auto-detects the language from the header and
produces a NotebookNode with appropriate kernel metadata.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

import nbformat

from databricks_agent_notebooks._constants import KERNELSPECS

# ---------------------------------------------------------------------------
# Language variant definitions
# ---------------------------------------------------------------------------

class _LangVariant(NamedTuple):
    """Comment conventions for one Databricks Source Format language."""

    language: str
    header: str
    delimiter: str
    magic_prefix: str


_VARIANTS: list[_LangVariant] = [
    _LangVariant(
        language="python",
        header="# Databricks notebook source",
        delimiter="# COMMAND ----------",
        magic_prefix="# MAGIC",
    ),
    _LangVariant(
        language="scala",
        header="// Databricks notebook source",
        delimiter="// COMMAND ----------",
        magic_prefix="// MAGIC",
    ),
    _LangVariant(
        language="sql",
        header="-- Databricks notebook source",
        delimiter="-- COMMAND ----------",
        magic_prefix="-- MAGIC",
    ),
]

_HEADER_TO_VARIANT: dict[str, _LangVariant] = {v.header: v for v in _VARIANTS}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _read_first_line(path: Path) -> str | None:
    """Return the first line of *path*, stripped, or None for empty files."""
    try:
        with path.open(encoding="utf-8") as f:
            first = f.readline()
    except OSError:
        return None
    return first.strip() if first else None


def _variant_for_path(path: Path) -> _LangVariant | None:
    """Return the language variant for a Databricks source file, or None."""
    first_line = _read_first_line(path)
    if first_line is None:
        return None
    return _HEADER_TO_VARIANT.get(first_line)


def _strip_blank_edges(lines: list[str]) -> list[str]:
    """Remove leading and trailing blank lines from a list of strings."""
    # Strip leading blanks
    while lines and lines[0].strip() == "":
        lines = lines[1:]
    # Strip trailing blanks
    while lines and lines[-1].strip() == "":
        lines = lines[:-1]
    return lines


def _parse_cell(lines: list[str], variant: _LangVariant) -> nbformat.NotebookNode | None:
    """Convert raw cell lines into a NotebookNode (code or markdown).

    Returns None if the cell is empty after stripping blank edges.
    """
    lines = _strip_blank_edges(lines)
    if not lines:
        return None

    # Check if ALL non-empty lines start with the MAGIC prefix
    non_empty = [ln for ln in lines if ln.strip()]
    magic_prefix_space = variant.magic_prefix + " "

    is_magic = all(
        ln.startswith(magic_prefix_space) or ln == variant.magic_prefix
        for ln in non_empty
    )

    if is_magic:
        # Strip MAGIC prefix from every line (empty lines stay empty)
        stripped: list[str] = []
        for ln in lines:
            if ln.startswith(magic_prefix_space):
                stripped.append(ln[len(magic_prefix_space):])
            elif ln == variant.magic_prefix:
                stripped.append("")
            else:
                # blank line — keep it
                stripped.append(ln)

        # Check for %md marker
        if stripped and stripped[0].startswith("%md"):
            md_first = stripped[0][3:]  # Remove "%md"
            # Strip leading space/newline from the first line's remainder
            md_first = md_first.lstrip(" ")
            if md_first:
                md_lines = [md_first] + stripped[1:]
            else:
                md_lines = stripped[1:]

            md_lines = _strip_blank_edges(md_lines)
            content = "\n".join(md_lines)
            return nbformat.v4.new_markdown_cell(content)

        # Other magic (e.g., %python, %sql) — treat as code
        stripped = _strip_blank_edges(stripped)
        content = "\n".join(stripped)
        return nbformat.v4.new_code_cell(content)

    # Regular code cell
    content = "\n".join(lines)
    return nbformat.v4.new_code_cell(content)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect(path: Path) -> bool:
    """Return True if *path* is a Databricks Source Format file.

    Checks whether the first line matches one of the known header patterns.
    Returns False for empty files or files that don't match.
    """
    return _variant_for_path(path) is not None


def detected_language(path: Path) -> str:
    """Return the language of a Databricks Source Format file.

    Returns ``"python"``, ``"scala"``, or ``"sql"`` based on the comment
    style used in the header line.

    Raises:
        ValueError: If *path* is not a Databricks source file.
    """
    variant = _variant_for_path(path)
    if variant is None:
        raise ValueError(f"Not a Databricks source file: {path}")
    return variant.language


def parse(path: Path) -> nbformat.NotebookNode:
    """Parse a Databricks Source Format file into an nbformat v4 notebook.

    The returned notebook has:
    - Appropriate kernel metadata based on the detected language
    - Code and markdown cells derived from COMMAND-delimited sections
    - Empty cells (no content between delimiters) are skipped

    Raises:
        ValueError: If *path* is not a Databricks source file.
    """
    variant = _variant_for_path(path)
    if variant is None:
        raise ValueError(f"Not a Databricks source file: {path}")

    text = path.read_text(encoding="utf-8")
    all_lines = text.split("\n")

    # Skip the header line (first line)
    remaining = all_lines[1:]

    # Split on delimiter lines
    chunks: list[list[str]] = []
    current: list[str] = []
    for line in remaining:
        if line.strip() == variant.delimiter.strip() or line == variant.delimiter:
            chunks.append(current)
            current = []
        else:
            current.append(line)
    # Don't forget the last chunk
    chunks.append(current)

    # Parse each chunk into a cell
    cells: list[nbformat.NotebookNode] = []
    for chunk in chunks:
        cell = _parse_cell(chunk, variant)
        if cell is not None:
            cells.append(cell)

    nb = nbformat.v4.new_notebook()
    nb.cells = cells
    nb.metadata["kernelspec"] = KERNELSPECS[variant.language]

    return nb
