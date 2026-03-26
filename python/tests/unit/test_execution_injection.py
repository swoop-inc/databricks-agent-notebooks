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


# ---------------------------------------------------------------------------
# Dynamic Scala Connect version
# ---------------------------------------------------------------------------


def test_scala_injection_uses_dynamic_connect_version() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1"),
        _make_lineage(),
        language="scala",
        scala_connect_version="15.4.6",
    )
    assert "databricks-connect:15.4.6" in code
    assert "databricks-connect:16.4.7" not in code


def test_scala_injection_falls_back_to_constant_when_no_version() -> None:
    from databricks_agent_notebooks._constants import DATABRICKS_CONNECT_VERSION

    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1"),
        _make_lineage(),
        language="scala",
    )
    assert f"databricks-connect:{DATABRICKS_CONNECT_VERSION}" in code


def test_scala_injection_includes_version_in_source_comment() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1"),
        _make_lineage(),
        language="scala",
        scala_connect_version="13.3.0",
    )
    assert "DB Connect: 13.3.0" in code


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_threads_scala_connect_version(mock_capture) -> None:
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook()
    config = DatabricksConfig(profile="dev", cluster="cls-123")

    inject_cells(notebook, config, scala_connect_version="15.4.6")

    code = notebook.cells[0].source
    assert "databricks-connect:15.4.6" in code


def test_scala_injection_uses_double_colon_for_213_variant() -> None:
    from databricks_agent_notebooks._constants import SCALA_213

    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1"),
        _make_lineage(),
        language="scala",
        scala_connect_version="17.3.4",
        scala_variant=SCALA_213,
    )
    assert "com.databricks::databricks-connect:17.3.4" in code


def test_scala_injection_uses_single_colon_for_212_variant() -> None:
    from databricks_agent_notebooks._constants import SCALA_212

    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1"),
        _make_lineage(),
        language="scala",
        scala_connect_version="16.4.7",
        scala_variant=SCALA_212,
    )
    assert "com.databricks:databricks-connect:16.4.7" in code
    assert "::" not in code.split("databricks-connect")[0].split("com.databricks")[-1]


def test_scala_injection_213_falls_back_to_213_constant() -> None:
    from databricks_agent_notebooks._constants import DATABRICKS_CONNECT_213_VERSION, SCALA_213

    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1"),
        _make_lineage(),
        language="scala",
        scala_variant=SCALA_213,
    )
    assert f"databricks-connect:{DATABRICKS_CONNECT_213_VERSION}" in code


def test_python_injection_ignores_scala_connect_version() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster=None),
        _make_lineage(),
        language="python",
        scala_connect_version="15.4.6",
    )
    assert "import $ivy" not in code
    assert "15.4.6" not in code
