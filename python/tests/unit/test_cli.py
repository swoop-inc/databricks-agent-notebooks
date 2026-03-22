"""Smoke tests for the standalone CLI surface."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from databricks_agent_notebooks.cli import main
from databricks_agent_notebooks.config.frontmatter import DatabricksConfig
from databricks_agent_notebooks.integrations.databricks.clusters import Cluster


def _make_notebook_mock():
    notebook = MagicMock()
    notebook.cells = []
    notebook.metadata = {}
    return notebook


def test_help_returns_zero(capsys) -> None:
    result = main(["help"])

    assert result == 0
    assert "agent-notebook" in capsys.readouterr().out


def test_run_file_not_found(capsys) -> None:
    result = main(["run", "/nonexistent/file.md"])

    assert result == 1
    assert "not found" in capsys.readouterr().err


def test_run_pipeline_delegates(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="13.3")

    with (
        patch("databricks_agent_notebooks.cli.to_notebook", return_value=(_make_notebook_mock(), DatabricksConfig(profile="prod", cluster="my-cluster"))),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.merge_config", return_value=DatabricksConfig(profile="prod", cluster="my-cluster")),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=_make_notebook_mock()),
        patch("databricks_agent_notebooks.cli.execute_notebook", return_value=MagicMock(success=True, output_path=input_file, duration_seconds=1.0, error=None)),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    assert "Execution succeeded" in capsys.readouterr().out
