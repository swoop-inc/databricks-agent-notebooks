"""Tests for the normalize-to-ipynb conversion layer."""

from __future__ import annotations

import json
from pathlib import Path

import nbformat
import pytest

from databricks_agent_notebooks.config.frontmatter import DatabricksConfig
from databricks_agent_notebooks.formats.conversion import (
    detect_language,
    is_dbr_source,
    is_markdown,
    is_notebook,
    to_notebook,
    validate_single_language,
)


def test_to_notebook_markdown_with_frontmatter(sample_markdown: Path) -> None:
    notebook, config = to_notebook(sample_markdown)

    assert isinstance(config, DatabricksConfig)
    assert config.profile == "nonhealth-prod"
    assert config.cluster == "rnd-alpha"
    assert config.language == "scala"
    assert notebook.metadata["kernelspec"]["language"] == "scala"


def test_to_notebook_ipynb(tmp_path: Path) -> None:
    notebook_data = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "name": "python3",
                "display_name": "Python 3",
                "language": "python",
            }
        },
        "cells": [
            {
                "cell_type": "code",
                "source": "print('hello')",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            }
        ],
    }
    path = tmp_path / "test.ipynb"
    path.write_text(json.dumps(notebook_data), encoding="utf-8")

    notebook, config = to_notebook(path)

    assert config is None
    assert notebook.cells[0].source == "print('hello')"
    assert notebook.metadata["kernelspec"]["language"] == "python"


def test_to_notebook_dbr_source(sample_dbr_scala: Path) -> None:
    notebook, config = to_notebook(sample_dbr_scala)

    assert config is None
    assert notebook.metadata["kernelspec"]["language"] == "scala"


def test_to_notebook_non_dbr_python_raises(tmp_path: Path) -> None:
    plain_py = tmp_path / "plain.py"
    plain_py.write_text("x = 1 + 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="not in Databricks Source Format"):
        to_notebook(plain_py)


def test_to_notebook_unsupported_extension_raises(tmp_path: Path) -> None:
    notes = tmp_path / "notes.txt"
    notes.write_text("hello\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file format: .txt"):
        to_notebook(notes)


def test_helper_predicates(sample_dbr_python: Path, tmp_path: Path) -> None:
    assert is_notebook(tmp_path / "foo.ipynb") is True
    assert is_markdown(tmp_path / "foo.md") is True
    assert is_dbr_source(sample_dbr_python) is True


def test_detect_language_from_kernel_metadata() -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {"name": "python3", "language": "python"}
    assert detect_language(notebook) == "python"


def test_detect_language_from_cell_heuristics() -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.cells = [nbformat.v4.new_code_cell("val x = 1 + 1\nprintln(x)")]
    assert detect_language(notebook) == "scala"


def test_validate_single_language_rejects_mixed_magic_cells() -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.cells = [
        nbformat.v4.new_code_cell("%python\nx = 1"),
        nbformat.v4.new_code_cell("%scala\nval y = 2"),
    ]

    with pytest.raises(ValueError, match="mixed languages"):
        validate_single_language(notebook)


def test_packaged_smoke_examples_encode_their_intended_language() -> None:
    python_root = Path(__file__).resolve().parents[2]
    smoke_root = python_root / "src" / "databricks_agent_notebooks" / "for_agents" / "examples" / "smoke"

    python_notebook, python_config = to_notebook(smoke_root / "python_select_one.md")
    scala_notebook, scala_config = to_notebook(smoke_root / "scala_select_one.md")

    assert python_config == DatabricksConfig(language="python")
    assert python_notebook.metadata["kernelspec"]["language"] == "python"
    assert scala_config == DatabricksConfig(language="scala")
    assert scala_notebook.metadata["kernelspec"]["language"] == "scala"
