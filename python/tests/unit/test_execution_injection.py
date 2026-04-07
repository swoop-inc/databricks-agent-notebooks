"""Tests for notebook session injection."""

from __future__ import annotations

from unittest.mock import patch

import nbformat
import pytest

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


# ---------------------------------------------------------------------------
# LOCAL_SPARK injection
# ---------------------------------------------------------------------------


def test_local_spark_python_default() -> None:
    """Default local Python generates SparkSession with local[*]."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
    )
    assert "from pyspark.sql import SparkSession" in code
    assert 'master("local[*]")' in code
    assert '.appName("agent-notebook")' in code
    assert ".getOrCreate()" in code
    assert "[AGENT-NOTEBOOK:INJECTED]" in code
    assert "Local Spark: local[*]" in code


def test_local_spark_python_custom_master(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Custom master via env var."""
    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", "local[4]")
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
    )
    assert 'master("local[4]")' in code
    assert "Local Spark: local[4]" in code


def test_local_spark_python_with_memory(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Driver and executor memory are included when env vars are set."""
    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY", "2g")
    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY", "4g")
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
    )
    assert '.config("spark.driver.memory", "2g")' in code
    assert '.config("spark.executor.memory", "4g")' in code


def test_local_spark_python_omits_memory_when_unset() -> None:
    """No memory config calls when env vars are absent."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
    )
    assert "spark.driver.memory" not in code
    assert "spark.executor.memory" not in code


def test_local_spark_scala_default() -> None:
    """Default local Scala generates $ivy import + SparkSession."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="scala",
        local_spark=True,
    )
    assert "import $ivy.`org.apache.spark::spark-sql:3.5.4`" in code
    assert "import org.apache.spark.sql.SparkSession" in code
    assert '.master("local[*]")' in code
    assert '.appName("agent-notebook")' in code
    assert ".getOrCreate()" in code
    assert 'setLogLevel("WARN")' in code
    assert "Local Spark: local[*] (3.5.4)" in code


def test_local_spark_scala_custom_version(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Custom Spark version for Scala via env var."""
    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_VERSION", "3.4.2")
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="scala",
        local_spark=True,
    )
    assert "import $ivy.`org.apache.spark::spark-sql:3.4.2`" in code
    assert "Local Spark: local[*] (3.4.2)" in code


def test_local_spark_no_databricks_imports() -> None:
    """Neither local generator references DatabricksSession or DatabricksConfig."""
    for lang in ("python", "scala"):
        code = generate_setup_code(
            DatabricksConfig(profile="LOCAL_SPARK"),
            _make_lineage(),
            language=lang,
            local_spark=True,
        )
        assert "DatabricksSession" not in code
        assert "DatabricksConfig" not in code
        assert "databricks.connect" not in code


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_local_spark_produces_pyspark_import(mock_capture) -> None:
    """inject_cells threads local_spark=True to the Python local generator."""
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook(language="python")
    config = DatabricksConfig(profile="LOCAL_SPARK")

    inject_cells(notebook, config, local_spark=True)

    code = notebook.cells[0].source
    assert "from pyspark.sql import SparkSession" in code
    assert "DatabricksSession" not in code
    assert is_injected_cell(notebook.cells[0]) is True


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_local_spark_scala(mock_capture) -> None:
    """inject_cells threads local_spark=True to the Scala local generator."""
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook(language="scala")
    config = DatabricksConfig(profile="LOCAL_SPARK")

    inject_cells(notebook, config, local_spark=True)

    code = notebook.cells[0].source
    assert "org.apache.spark" in code
    assert "DatabricksSession" not in code
    assert is_injected_cell(notebook.cells[0]) is True


def test_local_spark_sql_routes_to_python() -> None:
    """language='sql' routes to the Python local generator in local_spark mode."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="sql",
        local_spark=True,
    )
    assert "from pyspark.sql import SparkSession" in code


def test_scala_local_setup_omits_executor_memory(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Scala local generator must not emit spark.executor.memory even when env var is set."""
    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY", "2g")
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="scala",
        local_spark=True,
    )
    assert "executor.memory" not in code


# ---------------------------------------------------------------------------
# User-provided library injection
# ---------------------------------------------------------------------------


def test_python_setup_includes_sys_path_for_libraries() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster=None, libraries=("/my/lib",)),
        _make_lineage(),
        language="python",
    )
    assert "import sys" in code
    assert "sys.path.insert(0, '/my/lib')" in code
    # Library paths should appear before the session import
    sys_path_pos = code.index("sys.path.insert")
    connect_pos = code.index("from databricks.connect")
    assert sys_path_pos < connect_pos


def test_python_local_setup_includes_sys_path_for_libraries() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK", libraries=("/my/lib",)),
        _make_lineage(),
        language="python",
        local_spark=True,
    )
    assert "import sys" in code
    assert "sys.path.insert(0, '/my/lib')" in code
    sys_path_pos = code.index("sys.path.insert")
    pyspark_pos = code.index("from pyspark.sql")
    assert sys_path_pos < pyspark_pos


def test_python_setup_no_libraries_no_sys_path() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster=None),
        _make_lineage(),
        language="python",
    )
    assert "import sys" not in code
    assert "sys.path" not in code


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_threads_libraries_to_python_setup(mock_capture) -> None:
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook(language="python")
    config = DatabricksConfig(profile="dev", cluster=None, libraries=("/my/lib",))

    inject_cells(notebook, config)

    code = notebook.cells[0].source
    assert "sys.path.insert(0, '/my/lib')" in code


def test_scala_setup_ignores_libraries() -> None:
    code = generate_setup_code(
        DatabricksConfig(profile="dev", cluster="cls-1", libraries=("/my/lib",)),
        _make_lineage(),
        language="scala",
    )
    assert "sys.path" not in code
    assert "import sys" not in code


# ---------------------------------------------------------------------------
# master_override (unified --cluster local[N] path)
# ---------------------------------------------------------------------------


def test_local_spark_python_master_override() -> None:
    """master_override from --cluster takes precedence over env var."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
        master_override="local[2]",
    )
    assert 'master("local[2]")' in code
    assert "Local Spark: local[2]" in code


def test_local_spark_python_master_override_over_env(monkeypatch: "pytest.MonkeyPatch") -> None:
    """master_override wins over AGENT_NOTEBOOK_LOCAL_SPARK_MASTER env var."""
    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", "local[8]")
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
        master_override="local[4]",
    )
    assert 'master("local[4]")' in code
    assert "local[8]" not in code


def test_local_spark_scala_master_override() -> None:
    """master_override threads through to Scala local setup."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="scala",
        local_spark=True,
        master_override="local[2]",
    )
    assert '.master("local[2]")' in code
    assert "Local Spark: local[2]" in code


def test_local_spark_no_override_uses_env_or_default() -> None:
    """Without master_override, env var or default local[*] is used."""
    code = generate_setup_code(
        DatabricksConfig(profile="LOCAL_SPARK"),
        _make_lineage(),
        language="python",
        local_spark=True,
    )
    assert 'master("local[*]")' in code


@patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
def test_inject_cells_threads_master_override(mock_capture) -> None:
    """inject_cells passes master_override through to generate_setup_code."""
    mock_capture.return_value = _make_lineage()
    notebook = _make_notebook(language="python")
    config = DatabricksConfig(profile="LOCAL_SPARK")

    inject_cells(notebook, config, local_spark=True, master_override="local[2]")

    code = notebook.cells[0].source
    assert 'master("local[2]")' in code


def test_local_spark_python_master_override_no_profile() -> None:
    """--cluster local[2] without --profile LOCAL_SPARK must not say Serverless."""
    code = generate_setup_code(
        DatabricksConfig(),  # no profile -- the new --cluster path
        _make_lineage(),
        language="python",
        local_spark=True,
        master_override="local[2]",
    )
    assert 'master("local[2]")' in code
    assert "Serverless" not in code
    assert "Local Spark: local[2]" in code


def test_local_spark_scala_master_override_no_profile() -> None:
    """Scala: --cluster local[2] without profile must not say Serverless."""
    code = generate_setup_code(
        DatabricksConfig(),
        _make_lineage(),
        language="scala",
        local_spark=True,
        master_override="local[2]",
    )
    assert '.master("local[2]")' in code
    assert "Serverless" not in code
    assert "Local Spark: local[2]" in code
