"""Tests for notebook session injection."""

from __future__ import annotations

from unittest.mock import patch

import nbformat

from databricks_agent_notebooks.config.frontmatter import DatabricksConfig
from databricks_agent_notebooks.execution.injection import (
    generate_setup_code,
    inject_cells,
    is_injected_cell,
)
from databricks_agent_notebooks.execution.lineage import ExecutionLineage


def _make_notebook(language: str = "scala") -> nbformat.NotebookNode:
    notebook = nbformat.v4.new_notebook()
    notebook.cells = [nbformat.v4.new_code_cell("val x = 1" if language == "scala" else "x = 1")]
    if language == "scala":
        notebook.metadata["kernelspec"] = {"name": "scala212-dbr-connect", "language": "scala"}
    else:
        notebook.metadata["kernelspec"] = {"name": "python3", "language": "python"}
    return notebook


def _make_lineage() -> ExecutionLineage:
    return ExecutionLineage(
        source_path="/tmp/test.md",
        timestamp="2025-01-15T10:30:00+00:00",
        git_branch="main",
        git_commit="abc1234",
    )


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_adds_cell_at_position_zero(mock_capture) -> None:
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook()
    config = DatabricksConfig(profile="dev", cluster="cls-123")

    result = inject_cells(notebook, config)

    assert len(result.cells) == 2
    assert result.cells[0].metadata["agent_notebook_injected"] is True
    assert result.cells[1].source == "val x = 1"


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_is_idempotent(mock_capture) -> None:
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook()

    inject_cells(notebook, DatabricksConfig(profile="dev", cluster="cls-123"))
    inject_cells(notebook, DatabricksConfig(profile="dev", cluster="cls-123"))

    assert len(notebook.cells) == 2
    assert is_injected_cell(notebook.cells[0]) is True


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_python_notebooks_get_python_setup_code(mock_capture) -> None:
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook(language="python")
    config = DatabricksConfig(profile="prod", cluster=None)

    inject_cells(notebook, config)

    code = notebook.cells[0].source
    assert "from databricks.connect import DatabricksSession" in code
    assert ".serverless()" in code
    assert "import $ivy" not in code


def test_generate_setup_code_includes_lineage() -> None:
    code = generate_setup_code(DatabricksConfig(profile="dev", cluster="cls-1"), _make_lineage())

    assert "Source: /tmp/test.md" in code
    assert "branch: main @ abc1234" in code
    assert "2025-01-15T10:30:00+00:00" in code


def test_is_injected_cell_false_for_normal_cells() -> None:
    cell = nbformat.v4.new_code_cell("val x = 1")
    assert is_injected_cell(cell) is False
