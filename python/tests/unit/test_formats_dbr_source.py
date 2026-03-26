"""Tests for the Databricks Source Format parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from databricks_agent_notebooks.formats.dbr_source import detect, detected_language, parse


def test_detect_python_source(sample_dbr_python: Path) -> None:
    assert detect(sample_dbr_python) is True


def test_detect_regular_python_file(tmp_path: Path) -> None:
    path = tmp_path / "regular.py"
    path.write_text("print('hello')\n", encoding="utf-8")
    assert detect(path) is False


def test_detected_language_scala(sample_dbr_scala: Path) -> None:
    assert detected_language(sample_dbr_scala) == "scala"


def test_detected_language_non_dbr_raises(tmp_path: Path) -> None:
    path = tmp_path / "regular.py"
    path.write_text("print('hello')\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Not a Databricks source file"):
        detected_language(path)


def test_parse_python_source(sample_dbr_python: Path) -> None:
    notebook = parse(sample_dbr_python)

    assert len(notebook.cells) == 2
    assert notebook.cells[0].cell_type == "markdown"
    assert notebook.cells[1].cell_type == "code"
    assert notebook.metadata["kernelspec"]["language"] == "python"


def test_parse_sql_source_sets_sql_language(sample_dbr_sql: Path) -> None:
    notebook = parse(sample_dbr_sql)
    kernelspec = notebook.metadata["kernelspec"]

    assert kernelspec["name"] == "python3"
    assert kernelspec["language"] == "sql"


def test_parse_empty_cells_are_skipped(tmp_path: Path) -> None:
    content = """\
# Databricks notebook source
# COMMAND ----------

# COMMAND ----------

x = 1
"""
    path = tmp_path / "empty_cells.py"
    path.write_text(content, encoding="utf-8")

    notebook = parse(path)

    assert len(notebook.cells) == 1
    assert notebook.cells[0].cell_type == "code"
    assert "x = 1" in notebook.cells[0].source
