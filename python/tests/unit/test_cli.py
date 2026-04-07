"""Smoke tests for the standalone CLI surface."""

from __future__ import annotations

import builtins
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from databricks_agent_notebooks.cli import _build_cli_source_map, _build_parser, _resolve_execution_language, _validate_scala_local_spark, main
from databricks_agent_notebooks.config.frontmatter import DatabricksConfig
from databricks_agent_notebooks.integrations.databricks.clusters import Cluster, ClusterError
from databricks_agent_notebooks._constants import SCALA_212, SCALA_213
from databricks_agent_notebooks.runtime.kernel import KERNEL_DISPLAY_NAME, KERNEL_ID, KERNEL_ID_213
from databricks_agent_notebooks.runtime.doctor import Check


def _make_notebook_mock():
    notebook = MagicMock()
    notebook.cells = []
    notebook.metadata = {}
    return notebook


def test_help_returns_zero(capsys) -> None:
    installed_readme = Path("/tmp/site-packages/databricks_agent_notebooks/for_agents/README.md")

    with patch(
        "databricks_agent_notebooks.cli._resolve_installed_agent_docs_readme",
        return_value=installed_readme,
    ):
        result = main(["help"])

    assert result == 0
    output = capsys.readouterr().out
    assert "agent-notebook" in output
    assert "For agents:" in output
    assert str(installed_readme) in output
    assert str(installed_readme) in output


def test_run_file_not_found(capsys) -> None:
    result = main(["run", "/nonexistent/file.md"])

    assert result == 1
    assert "not found" in capsys.readouterr().err


def test_run_pipeline_delegates(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="13.3")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-13.3-python-3.12",
        python_executable=Path("/managed/runtime/bin/python"),
    )
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    service = MagicMock(resolve_cluster=MagicMock(return_value=cluster))

    with (
        patch("databricks_agent_notebooks.cli.to_notebook", return_value=(notebook, DatabricksConfig(profile="prod", cluster="my-cluster", language="python"))),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "cluster": "my-cluster", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.runtime.connect.ensure_cluster_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    service.resolve_cluster.assert_called_once_with("my-cluster", "prod")
    execute_notebook.assert_called_once()
    assert execute_notebook.call_args.kwargs["python_executable"] == Path("/managed/runtime/bin/python")
    captured = capsys.readouterr()
    assert "Execution succeeded" in captured.out
    phase_lines = [line for line in captured.err.splitlines() if line.startswith("agent-notebook:")]
    assert phase_lines == [
        'agent-notebook: phase=prepare input_path="' + str(input_file.resolve()) + '" notebook_stem="test"',
        'agent-notebook: phase=compute mode=cluster cluster_id="abc-123"',
        'agent-notebook: phase=execute-start kernel="python3" timeout=none',
        'agent-notebook: phase=render output_dir="' + str((tmp_path / "test_output").resolve()) + '"',
        'agent-notebook: phase=done success=true duration_s=1.0',
    ]


def test_run_without_cluster_uses_serverless_runtime_policy_for_injected_python(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-16.4-python-3.12",
        python_executable=Path("/managed/serverless/bin/python"),
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "env": "default"}),
        patch(
            "databricks_agent_notebooks.cli.inject_lifecycle_cells",
            return_value=notebook,
        ),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service"),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    assert execute_notebook.call_args.kwargs["python_executable"] == Path("/managed/serverless/bin/python")
    captured = capsys.readouterr()
    assert 'agent-notebook: phase=compute mode=serverless' in captured.err
    assert "Execution succeeded" in captured.out


def test_run_defaults_timeout_to_none(tmp_path: Path) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3"}}

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "inject_session": False, "env": "default"}),
        patch(
            "databricks_agent_notebooks.cli.inject_lifecycle_cells",
            return_value=notebook,
        ),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", "--no-inject-session", str(input_file)])

    assert result == 0
    assert execute_notebook.call_args.kwargs["timeout"] is None


def test_run_cluster_scala_with_injection_skips_managed_runtime_and_injects(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="16.4.x-scala2.12")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", cluster="my-cluster", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "cluster": "my-cluster", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.runtime.connect.ensure_cluster_runtime") as ensure_cluster_runtime,
        patch("databricks_agent_notebooks.runtime.scala_connect.resolve_scala_connect", return_value=("16.4", SCALA_212)),
        patch("databricks_agent_notebooks.runtime.scala_connect.prefetch_scala_connect", return_value="16.4.7"),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    ensure_cluster_runtime.assert_not_called()
    assert execute_notebook.call_args.kwargs["python_executable"] is None
    captured = capsys.readouterr()
    assert "Execution succeeded" in captured.out


def test_run_cluster_scala_calls_prefetch_and_passes_version(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="16.4.x-scala2.12")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", cluster="my-cluster", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "cluster": "my-cluster", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.runtime.scala_connect.resolve_scala_connect", return_value=("16.4", SCALA_212)) as mock_resolve,
        patch("databricks_agent_notebooks.runtime.scala_connect.prefetch_scala_connect", return_value="16.4.7") as mock_prefetch,
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    mock_resolve.assert_called_once_with(cluster)
    mock_prefetch.assert_called_once_with("16.4", SCALA_212)
    # Verify inject_cells received the dynamic version and variant
    inject_kwargs = mock_inject.call_args.kwargs
    assert inject_kwargs.get("scala_connect_version") == "16.4.7"
    assert inject_kwargs.get("scala_variant") is SCALA_212


def test_run_cluster_scala_prefetch_failure_returns_error(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="16.4.x-scala2.12")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", cluster="my-cluster", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "cluster": "my-cluster", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.runtime.scala_connect.resolve_scala_connect", return_value=("16.4", SCALA_212)),
        patch("databricks_agent_notebooks.runtime.scala_connect.prefetch_scala_connect", side_effect=RuntimeError("coursier is required")),
    ):
        result = main(["run", str(input_file)])

    assert result == 1
    captured = capsys.readouterr()
    assert "coursier is required" in captured.err


def test_run_dbr_17_cluster_selects_213_kernel(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="17.3.x-scala2.13")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", cluster="my-cluster", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "cluster": "my-cluster", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.runtime.scala_connect.resolve_scala_connect", return_value=("17.3", SCALA_213)) as mock_resolve,
        patch("databricks_agent_notebooks.runtime.scala_connect.prefetch_scala_connect", return_value="17.3.4") as mock_prefetch,
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    mock_resolve.assert_called_once_with(cluster)
    mock_prefetch.assert_called_once_with("17.3", SCALA_213)
    inject_kwargs = mock_inject.call_args.kwargs
    assert inject_kwargs.get("scala_connect_version") == "17.3.4"
    assert inject_kwargs.get("scala_variant") is SCALA_213
    # Verify kernel metadata was set to 2.13
    assert notebook.metadata["kernelspec"]["name"] == "scala213-dbr-connect"
    assert execute_notebook.call_args.kwargs["kernel"] == "scala213-dbr-connect"


def test_run_scala_serverless_uses_213_variant(tmp_path: Path, capsys) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service"),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    # Verify kernel metadata was set to 2.13 (serverless default)
    assert notebook.metadata["kernelspec"]["name"] == "scala213-dbr-connect"
    inject_kwargs = mock_inject.call_args.kwargs
    assert inject_kwargs.get("scala_variant") is SCALA_213
    assert execute_notebook.call_args.kwargs["kernel"] == "scala213-dbr-connect"


def test_run_cluster_no_inject_session_bypasses_managed_runtime_selection(tmp_path: Path) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="16.4.x-scala2.12")
    service = MagicMock(resolve_cluster=MagicMock(return_value=cluster))

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", cluster="my-cluster", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "prod", "cluster": "my-cluster", "language": "python", "inject_session": False, "env": "default"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service),
        patch("databricks_agent_notebooks.runtime.connect.ensure_cluster_runtime") as ensure_cluster_runtime,
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells") as inject_cells,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", "--no-inject-session", str(input_file)])

    assert result == 0
    service.resolve_cluster.assert_not_called()
    ensure_cluster_runtime.assert_not_called()
    inject_cells.assert_called_once()
    assert inject_cells.call_args.kwargs["inject_session"] is False
    assert execute_notebook.call_args.kwargs["python_executable"] is None


def test_run_cluster_without_explicit_profile_persists_default_profile_for_injection(tmp_path: Path) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    cluster = Cluster(cluster_id="abc-123", cluster_name="my-cluster", state="RUNNING", spark_version="16.4.x-scala2.12")
    service = MagicMock(resolve_cluster=MagicMock(return_value=cluster))
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-16.4-python-3.12",
        python_executable=Path("/managed/runtime/bin/python"),
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(cluster="my-cluster", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"cluster": "my-cluster", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service),
        patch("databricks_agent_notebooks.runtime.connect.ensure_cluster_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as inject_cells,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    service.resolve_cluster.assert_called_once_with("my-cluster", "DEFAULT")
    # After three-level merge + with_defaults, the config includes hardcoded defaults
    actual_config = inject_cells.call_args[0][1]
    assert actual_config.profile == "DEFAULT"
    assert actual_config.cluster == "abc-123"
    assert actual_config.language == "python"


def test_clusters_command_passes_profile_to_service_and_prints_clusters(capsys) -> None:
    cluster = Cluster(
        cluster_id="1003-184738-wkj97rxa",
        cluster_name="rnd-alpha",
        state="RUNNING",
        spark_version="16.4.x-scala2.12",
    )
    service = MagicMock(iter_clusters=MagicMock(return_value=iter([[cluster]])))

    with patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service):
        result = main(["clusters", "--profile", "prod"])

    assert result == 0
    service.iter_clusters.assert_called_once_with("prod")
    captured = capsys.readouterr()
    assert "rnd-alpha" in captured.out
    assert "1003-184738-wkj97rxa" in captured.out


def test_clusters_command_no_clusters_prints_message(capsys) -> None:
    service = MagicMock(iter_clusters=MagicMock(return_value=iter([[]])))

    with patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service):
        result = main(["clusters", "--profile", "prod"])

    assert result == 0
    captured = capsys.readouterr()
    assert "No clusters found." in captured.err
    assert captured.out == ""


def test_clusters_command_error_mid_stream_shows_partial_output(capsys) -> None:
    cluster = Cluster(
        cluster_id="1003-184738-wkj97rxa",
        cluster_name="rnd-alpha",
        state="RUNNING",
        spark_version="16.4.x-scala2.12",
    )

    def _iter_then_fail(_profile):
        yield [cluster]
        raise ClusterError("cluster listing did not complete within 120.0 seconds")

    service = MagicMock(iter_clusters=MagicMock(side_effect=_iter_then_fail))

    with patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service):
        result = main(["clusters", "--profile", "prod"])

    assert result == 1
    captured = capsys.readouterr()
    assert "rnd-alpha" in captured.out
    assert "did not complete within 120.0 seconds" in captured.err


def test_run_frontmatter_cluster_no_inject_session_bypasses_cluster_resolution(tmp_path: Path) -> None:
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    service = MagicMock(resolve_cluster=MagicMock())

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod", cluster="frontmatter-cluster", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service", return_value=service),
        patch("databricks_agent_notebooks.runtime.connect.ensure_cluster_runtime") as ensure_cluster_runtime,
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells") as inject_cells,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", "--no-inject-session", str(input_file)])

    assert result == 0
    service.resolve_cluster.assert_not_called()
    ensure_cluster_runtime.assert_not_called()
    inject_cells.assert_called_once()
    assert inject_cells.call_args.kwargs["inject_session"] is False
    assert execute_notebook.call_args.kwargs["python_executable"] is None


def test_install_kernel_command_delegates(tmp_path: Path, capsys) -> None:
    kernel_dir = tmp_path / "kernels" / "scala212-dbr-connect"

    with patch("databricks_agent_notebooks.runtime.kernel.install_kernel", return_value=kernel_dir) as mock_install:
        result = main(["install-kernel", "--kernels-dir", str(tmp_path / "kernels")])

    assert result == 0
    # Legacy shim installs both 2.12 and 2.13
    assert mock_install.call_count == 2
    mock_install.assert_any_call(
        kernel_id=KERNEL_ID,
        display_name=KERNEL_DISPLAY_NAME,
        kernels_dir=tmp_path / "kernels",
        scala_version="2.12",
        user=False,
        prefix=None,
        sys_prefix=False,
        jupyter_path=None,
        force=True,
    )
    mock_install.assert_any_call(
        kernel_id=KERNEL_ID,
        display_name=KERNEL_DISPLAY_NAME,
        kernels_dir=tmp_path / "kernels",
        scala_version="2.13",
        user=False,
        prefix=None,
        sys_prefix=False,
        jupyter_path=None,
        force=True,
    )
    assert "Kernel installed" in capsys.readouterr().out


def test_kernels_install_command_delegates(tmp_path: Path, capsys) -> None:
    kernel_dir = tmp_path / "kernels" / "scala212-dbr-connect"

    with patch("databricks_agent_notebooks.runtime.kernel.install_kernel", return_value=kernel_dir) as install_kernel:
        result = main(
            [
                "kernels",
                "install",
                "--id",
                "custom-scala",
                "--display-name",
                "Custom Scala",
                "--jupyter-path",
                str(tmp_path / "kernels"),
                "--force",
                "--scala-version",
                "2.12",
            ]
        )

    assert result == 0
    install_kernel.assert_called_once_with(
        kernel_id="custom-scala",
        display_name="Custom Scala",
        kernels_dir=None,
        scala_version="2.12",
        user=False,
        prefix=None,
        sys_prefix=False,
        jupyter_path=tmp_path / "kernels",
        force=True,
    )
    assert "Kernel installed" in capsys.readouterr().out


def test_kernels_list_command_prints_runtime_and_override_dirs(tmp_path: Path, capsys) -> None:
    runtime_kernel = SimpleNamespace(
        name="scala212-dbr-connect",
        directory=tmp_path / "runtime" / "scala212-dbr-connect",
        source="runtime-home",
        runtime_id="dbr-16.4-python-3.12",
        launcher_path="/usr/bin/python3",
        launcher_contract_path=tmp_path / "runtime" / "scala212-dbr-connect" / "launcher-contract.json",
        receipt_path=tmp_path / "state" / "installations" / "kernels" / "scala212-dbr-connect.json",
    )
    override_kernel = SimpleNamespace(
        name="python3",
        directory=tmp_path / "custom" / "python3",
        source=str(tmp_path / "custom"),
        runtime_id=None,
        launcher_path=None,
        launcher_contract_path=None,
        receipt_path=None,
    )

    with patch(
        "databricks_agent_notebooks.runtime.kernel.list_installed_kernels",
        return_value=[runtime_kernel, override_kernel],
    ) as list_installed_kernels:
        result = main(["kernels", "list", "--kernels-dir", str(tmp_path / "custom")])

    assert result == 0
    list_installed_kernels.assert_called_once_with(kernels_dirs=[tmp_path / "custom"])
    captured = capsys.readouterr()
    assert "scala212-dbr-connect" in captured.out
    assert "runtime-home" in captured.out
    assert "dbr-16.4-python-3.12" in captured.out
    assert "python3" in captured.out
    assert str(tmp_path / "custom") in captured.out
    assert "/usr/bin/python3" in captured.out
    assert str(runtime_kernel.launcher_contract_path) in captured.out
    assert "missing" in captured.out


def test_runtimes_list_command_prints_materialized_runtimes(tmp_path: Path, capsys) -> None:
    runtime = SimpleNamespace(
        runtime_id="dbr-16.4-python-3.12",
        status="materialized",
        databricks_line="16.4",
        python_line="3.12",
        receipt_path=tmp_path / "runtime-home" / "data" / "runtimes" / "dbr-16.4-python-3.12" / "runtime-receipt.json",
        install_root=tmp_path / "runtime-home" / "data" / "runtimes" / "dbr-16.4-python-3.12",
    )

    with patch("databricks_agent_notebooks.runtime.inventory.list_installed_runtimes", return_value=[runtime]) as list_installed_runtimes:
        result = main(["runtimes", "list"])

    assert result == 0
    list_installed_runtimes.assert_called_once_with()
    captured = capsys.readouterr()
    assert "dbr-16.4-python-3.12" in captured.out
    assert "materialized" in captured.out


def test_kernels_remove_command_delegates(tmp_path: Path, capsys) -> None:
    removed_dir = tmp_path / "runtime" / "scala212-dbr-connect"

    with patch(
        "databricks_agent_notebooks.runtime.kernel.remove_kernel",
        return_value=removed_dir,
    ) as remove_kernel:
        result = main(["kernels", "remove", "scala212-dbr-connect", "--kernels-dir", str(tmp_path / "custom")])

    assert result == 0
    remove_kernel.assert_called_once_with("scala212-dbr-connect", kernels_dirs=[tmp_path / "custom"])
    assert str(removed_dir) in capsys.readouterr().out


def test_doctor_command_runs_kernel_and_runtime_checks(capsys) -> None:
    kernel_checks = [
        Check("coursier", "ok", "coursier found"),
        Check("kernel", "fail", "kernel missing"),
    ]
    runtime_checks = [
        SimpleNamespace(name="dbr-16.4-python-3.12", status="warn", message="receipt metadata is stale"),
    ]

    with (
        patch("databricks_agent_notebooks.runtime.doctor.run_checks", return_value=kernel_checks) as mock_run_checks,
        patch("databricks_agent_notebooks.runtime.inventory.doctor_installed_runtimes", return_value=runtime_checks) as doctor_installed_runtimes,
        patch("databricks_agent_notebooks.runtime.doctor.doctor_scala_connect_readiness", return_value=[]),
    ):
        result = main(["doctor", "--profile", "DEFAULT"])

    assert result == 1
    # Default doctor checks both 2.12 and 2.13 kernels
    assert mock_run_checks.call_count == 2
    mock_run_checks.assert_any_call(profile="DEFAULT", kernel_id=KERNEL_ID)
    mock_run_checks.assert_any_call(profile="DEFAULT", kernel_id=KERNEL_ID_213)
    doctor_installed_runtimes.assert_called_once_with()
    captured = capsys.readouterr()
    assert "Running kernel readiness checks..." in captured.out
    assert "Running managed runtime checks..." in captured.out
    assert "[FAIL] kernel" in captured.out
    assert "[!!] dbr-16.4-python-3.12" in captured.out
    assert "check(s) failed." in captured.err


def test_doctor_command_accepts_custom_kernel_id_and_jupyter_path(tmp_path: Path, capsys) -> None:
    with (
        patch("databricks_agent_notebooks.runtime.doctor.run_checks", return_value=[]) as run_checks,
        patch("databricks_agent_notebooks.runtime.inventory.doctor_installed_runtimes", return_value=[]) as doctor_installed_runtimes,
        patch("databricks_agent_notebooks.runtime.doctor.doctor_scala_connect_readiness", return_value=[]),
    ):
        result = main(
            [
                "doctor",
                "--id",
                "custom-scala",
                "--jupyter-path",
                str(tmp_path / "kernels"),
                "--profile",
                "DEFAULT",
            ]
        )

    assert result == 0
    run_checks.assert_called_once_with(profile="DEFAULT", kernels_dir=tmp_path / "kernels", kernel_id="custom-scala")
    doctor_installed_runtimes.assert_called_once_with()
    assert "All checks passed." in capsys.readouterr().out


@pytest.mark.parametrize(
    ("argv", "expected_missing"),
    [
        (["kernels", "--help"], "doctor"),
        (["runtimes", "--help"], "doctor"),
    ],
)
def test_nested_help_hides_doctor_subcommands(argv: list[str], expected_missing: str, capsys) -> None:
    parser = _build_parser()

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(argv)

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert expected_missing not in output


def test_doctor_includes_scala_connect_section(capsys) -> None:
    kernel_checks = [Check("coursier", "ok", "coursier found")]
    runtime_checks = []
    scala_checks = [
        Check("scala-connect(16.4, 2.12)", "ok", "cached (databricks-connect 16.4.7)"),
    ]

    with (
        patch("databricks_agent_notebooks.runtime.doctor.run_checks", return_value=kernel_checks),
        patch("databricks_agent_notebooks.runtime.inventory.doctor_installed_runtimes", return_value=runtime_checks),
        patch("databricks_agent_notebooks.runtime.doctor.doctor_scala_connect_readiness", return_value=scala_checks),
    ):
        result = main(["doctor"])

    assert result == 0
    captured = capsys.readouterr()
    assert "Running Scala Connect cache checks..." in captured.out
    assert "[ok] scala-connect(16.4, 2.12)" in captured.out


def test_doctor_omits_scala_section_when_empty(capsys) -> None:
    kernel_checks = [Check("coursier", "ok", "coursier found")]
    runtime_checks = []

    with (
        patch("databricks_agent_notebooks.runtime.doctor.run_checks", return_value=kernel_checks),
        patch("databricks_agent_notebooks.runtime.inventory.doctor_installed_runtimes", return_value=runtime_checks),
        patch("databricks_agent_notebooks.runtime.doctor.doctor_scala_connect_readiness", return_value=[]),
    ):
        result = main(["doctor"])

    assert result == 0
    captured = capsys.readouterr()
    assert "Running Scala Connect cache checks..." not in captured.out


@pytest.mark.parametrize("argv", [["kernels", "doctor"], ["runtimes", "doctor"]])
def test_nested_doctor_commands_are_not_available(argv: list[str], capsys) -> None:
    parser = _build_parser()

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(argv)

    assert excinfo.value.code == 2
    error_output = capsys.readouterr().err
    assert "invalid choice" in error_output
    assert "doctor" in error_output


def test_run_python_local_spark_fails_fast_when_pyspark_missing(tmp_path: Path, capsys) -> None:
    """Python LOCAL_SPARK pre-flight: error with actionable message when pyspark is absent."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "pyspark":
            raise ImportError("No module named 'pyspark'")
        return original_import(name, *args, **kwargs)

    # Ensure pyspark is not cached in sys.modules (otherwise import bypasses __import__ mock)
    _saved = {k: sys.modules.pop(k) for k in [k for k in sys.modules if k == "pyspark" or k.startswith("pyspark.")]}
    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("builtins.__import__", side_effect=mock_import),
    ):
        result = main(["run", str(input_file)])
    sys.modules.update(_saved)

    assert result == 1
    captured = capsys.readouterr()
    assert "pyspark is required for Python LOCAL_SPARK" in captured.err
    assert "pip install pyspark" in captured.err


def test_run_python_local_spark_fails_fast_when_pyspark_broken(tmp_path: Path, capsys) -> None:
    """Python LOCAL_SPARK pre-flight: error when pyspark exists but can't be imported."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "pyspark":
            raise ImportError("stump directory, no real package")
        return original_import(name, *args, **kwargs)

    # Ensure pyspark is not cached in sys.modules (otherwise import bypasses __import__ mock)
    _saved = {k: sys.modules.pop(k) for k in [k for k in sys.modules if k == "pyspark" or k.startswith("pyspark.")]}
    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("builtins.__import__", side_effect=mock_import),
    ):
        result = main(["run", str(input_file)])
    sys.modules.update(_saved)

    assert result == 1
    captured = capsys.readouterr()
    assert "pyspark is required for Python LOCAL_SPARK" in captured.err


# ---------------------------------------------------------------------------
# LOCAL_SPARK: Python PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON
# ---------------------------------------------------------------------------


def test_run_python_local_spark_sets_pyspark_python(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """Python LOCAL_SPARK sets PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to sys.executable."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    assert os.environ.get("PYSPARK_PYTHON") == sys.executable
    assert os.environ.get("PYSPARK_DRIVER_PYTHON") == sys.executable


def test_run_python_local_spark_respects_existing_pyspark_python(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """Python LOCAL_SPARK does not override user-set PYSPARK_PYTHON or PYSPARK_DRIVER_PYTHON."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    monkeypatch.setenv("PYSPARK_PYTHON", "/usr/bin/python3.12")
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", "/usr/bin/python3.12")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    assert os.environ.get("PYSPARK_PYTHON") == "/usr/bin/python3.12"
    assert os.environ.get("PYSPARK_DRIVER_PYTHON") == "/usr/bin/python3.12"


# ---------------------------------------------------------------------------
# LOCAL_SPARK: Python kernelspec assignment (hotfix core behavior)
# ---------------------------------------------------------------------------


def test_run_python_local_spark_sets_kernelspec_from_empty_metadata(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """Python LOCAL_SPARK must set kernelspec=python3 even when metadata starts empty."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {}  # deliberately empty -- no kernelspec pre-seeded

    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)

    execute_mock = MagicMock(
        success=True, output_path=executed_notebook, duration_seconds=1.0, error=None,
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=execute_mock,
        ) as mock_execute,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    mock_execute.assert_called_once()
    assert mock_execute.call_args.kwargs["kernel"] == "python3"


# ---------------------------------------------------------------------------
# _resolve_execution_language unit tests
# ---------------------------------------------------------------------------


def test_resolve_execution_language_config_wins_over_kernelspec() -> None:
    nb = MagicMock()
    nb.metadata = {"kernelspec": {"language": "scala"}}
    assert _resolve_execution_language(nb, DatabricksConfig(language="python")) == "python"


def test_resolve_execution_language_falls_back_to_kernelspec() -> None:
    nb = MagicMock()
    nb.metadata = {"kernelspec": {"language": "scala"}}
    assert _resolve_execution_language(nb, DatabricksConfig()) == "scala"


def test_resolve_execution_language_from_config_no_kernelspec() -> None:
    nb = MagicMock()
    nb.metadata = {}
    assert _resolve_execution_language(nb, DatabricksConfig(language="scala")) == "scala"


def test_resolve_execution_language_default_is_python() -> None:
    nb = MagicMock()
    nb.metadata = {}
    assert _resolve_execution_language(nb, DatabricksConfig()) == "python"


def test_resolve_execution_language_sql_maps_to_python() -> None:
    nb = MagicMock()
    nb.metadata = {"kernelspec": {"language": "sql"}}
    assert _resolve_execution_language(nb, DatabricksConfig()) == "python"


def test_resolve_execution_language_sql_config_maps_to_python() -> None:
    nb = MagicMock()
    nb.metadata = {}
    assert _resolve_execution_language(nb, DatabricksConfig(language="sql")) == "python"


def test_resolve_execution_language_sql_config_overrides_scala_kernelspec() -> None:
    nb = MagicMock()
    nb.metadata = {"kernelspec": {"language": "scala"}}
    assert _resolve_execution_language(nb, DatabricksConfig(language="sql")) == "python"


# ---------------------------------------------------------------------------
# LOCAL_SPARK: Scala driver memory via JDK_JAVA_OPTIONS
# ---------------------------------------------------------------------------


def test_run_scala_local_spark_injects_xmx_into_jdk_java_options(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY=4g should produce -Xmx4g in JDK_JAVA_OPTIONS for Scala."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY", "4g")
    # Clear JDK_JAVA_OPTIONS to isolate the test
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    jdk_opts = os.environ.get("JDK_JAVA_OPTIONS", "")
    assert "-Xmx4g" in jdk_opts


def test_run_scala_local_spark_no_xmx_when_driver_memory_unset(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """No -Xmx injection when AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY is not set."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.delenv("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY", raising=False)
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    jdk_opts = os.environ.get("JDK_JAVA_OPTIONS", "")
    assert "-Xmx" not in jdk_opts


def test_run_scala_local_spark_skips_xmx_when_already_present(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", capsys,
) -> None:
    """If -Xmx is already in JDK_JAVA_OPTIONS, do not inject a second one and do not warn."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY", "4g")
    monkeypatch.setenv("JDK_JAVA_OPTIONS", "-Xmx2g")
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    jdk_opts = os.environ.get("JDK_JAVA_OPTIONS", "")
    # Should still have the original -Xmx2g, not -Xmx4g
    assert "-Xmx2g" in jdk_opts
    assert "-Xmx4g" not in jdk_opts
    # Warning should NOT be emitted when -Xmx was not injected
    captured = capsys.readouterr()
    assert "total JVM heap" not in captured.err


# ---------------------------------------------------------------------------
# LOCAL_SPARK: local-cluster + Scala hard error
# ---------------------------------------------------------------------------


def test_run_scala_local_cluster_errors(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", capsys,
) -> None:
    """local-cluster master with Scala should hard-error (return 1)."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", "local-cluster[2,1,1024]")
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
    ):
        rc = main(["run", str(input_file)])

    assert rc == 1
    captured = capsys.readouterr()
    assert "not supported for Scala" in captured.err
    assert "ClassNotFoundException" in captured.err


def test_run_python_local_cluster_no_warning(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", capsys,
) -> None:
    """local-cluster master with Python should NOT emit the Scala warning."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", "local-cluster[2,1,1024]")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "python", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    captured = capsys.readouterr()
    assert "local-cluster mode is not supported for Scala" not in captured.err


# ---------------------------------------------------------------------------
# Scala LOCAL_SPARK validation function tests
# ---------------------------------------------------------------------------


def test_validate_scala_local_spark_rejects_local_cluster() -> None:
    result = _validate_scala_local_spark("local-cluster[2,1,1024]", None)
    assert result is not None
    assert "not supported for Scala" in result


def test_validate_scala_local_spark_rejects_executor_memory() -> None:
    result = _validate_scala_local_spark("local[*]", "2g")
    assert result is not None
    assert "AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY is not supported" in result


@pytest.mark.parametrize("master", ["local", "local[*]", "local[4]", "local[*,3]", "local[4,2]"])
def test_validate_scala_local_spark_accepts_valid_masters(master: str) -> None:
    assert _validate_scala_local_spark(master, None) is None


@pytest.mark.parametrize("master", [
    "local-cluster[2,1,1024]", "spark://host:7077", "yarn", "k8s://host", "local[", "local[]",
])
def test_validate_scala_local_spark_rejects_invalid_masters(master: str) -> None:
    assert _validate_scala_local_spark(master, None) is not None


# ---------------------------------------------------------------------------
# Scala LOCAL_SPARK integration-style: executor memory hard error
# ---------------------------------------------------------------------------


def test_run_scala_local_spark_executor_memory_errors(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", capsys,
) -> None:
    """AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY with Scala should hard-error."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY", "2g")
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
    ):
        rc = main(["run", str(input_file)])

    assert rc == 1
    captured = capsys.readouterr()
    assert "AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY is not supported" in captured.err


# ---------------------------------------------------------------------------
# Scala LOCAL_SPARK integration-style: driver memory warning
# ---------------------------------------------------------------------------


def test_run_scala_local_spark_driver_memory_warning(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", capsys,
) -> None:
    """AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY emits informational warning after -Xmx injection."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY", "4g")
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)
    monkeypatch.delenv("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        main(["run", str(input_file)])

    captured = capsys.readouterr()
    assert "total JVM heap" in captured.err
    assert "-Xmx4g" in captured.err


# ---------------------------------------------------------------------------
# Scala LOCAL_SPARK integration-style: invalid master URL hard error
# ---------------------------------------------------------------------------


def test_run_scala_local_spark_invalid_master_errors(
    tmp_path: Path, monkeypatch: "pytest.MonkeyPatch", capsys,
) -> None:
    """Invalid Spark master URL with Scala should hard-error (return 1)."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```scala\nval x = 1\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "scala212-dbr-connect", "language": "scala"}}

    monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", "yarn")
    monkeypatch.delenv("JDK_JAVA_OPTIONS", raising=False)
    monkeypatch.delenv("JAVA_TOOL_OPTIONS", raising=False)

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="scala")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "LOCAL_SPARK", "language": "scala", "env": "default"}),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
    ):
        rc = main(["run", str(input_file)])

    assert rc == 1
    captured = capsys.readouterr()
    assert "not supported for Scala" in captured.err


# ---------------------------------------------------------------------------
# --clean flag
# ---------------------------------------------------------------------------


def _run_with_clean_flag(tmp_path: Path, cli_args: list[str]) -> tuple[int, str, str]:
    """Run a minimal serverless pipeline and return (rc, stdout, stderr)."""
    input_file = tmp_path / "test.md"
    if not input_file.exists():
        input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-16.4-python-3.12",
        python_executable=Path("/managed/serverless/bin/python"),
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="prod")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service"),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        import io, contextlib
        stderr_buf = io.StringIO()
        stdout_buf = io.StringIO()
        with contextlib.redirect_stderr(stderr_buf), contextlib.redirect_stdout(stdout_buf):
            rc = main(cli_args)
        return rc, stdout_buf.getvalue(), stderr_buf.getvalue()


def test_run_clean_removes_existing_output_directory(tmp_path: Path) -> None:
    output_dir = tmp_path / "test_output"
    output_dir.mkdir()
    (output_dir / "stale.md").write_text("old render", encoding="utf-8")
    sub = output_dir / "subdir"
    sub.mkdir()
    (sub / "deep.txt").write_text("deep stale", encoding="utf-8")

    rc, _stdout, stderr = _run_with_clean_flag(tmp_path, ["run", "--clean", str(tmp_path / "test.md")])

    assert rc == 0
    # Stale files should be gone
    assert not (output_dir / "stale.md").exists()
    assert not sub.exists()
    # Directory itself should be recreated
    assert output_dir.is_dir()
    # Clean phase signal should be emitted
    assert 'phase=clean' in stderr


def test_run_clean_noop_when_dir_does_not_exist(tmp_path: Path) -> None:
    output_dir = tmp_path / "test_output"
    assert not output_dir.exists()

    rc, _stdout, stderr = _run_with_clean_flag(tmp_path, ["run", "--clean", str(tmp_path / "test.md")])

    assert rc == 0
    # Directory should be created
    assert output_dir.is_dir()
    # No clean signal since directory didn't exist
    assert 'phase=clean' not in stderr


def test_run_without_clean_preserves_existing_files(tmp_path: Path) -> None:
    output_dir = tmp_path / "test_output"
    output_dir.mkdir()
    stale = output_dir / "stale.md"
    stale.write_text("old render", encoding="utf-8")

    rc, _stdout, _stderr = _run_with_clean_flag(tmp_path, ["run", str(tmp_path / "test.md")])

    assert rc == 0
    assert stale.exists()
    assert stale.read_text(encoding="utf-8") == "old render"


def test_parser_clean_flag_defaults_none() -> None:
    parser = _build_parser()
    args = parser.parse_args(["run", "notebook.md"])
    assert args.clean is None


def test_parser_clean_flag_set_true() -> None:
    parser = _build_parser()
    args = parser.parse_args(["run", "--clean", "notebook.md"])
    assert args.clean is True


# ---------------------------------------------------------------------------
# Library path resolution
# ---------------------------------------------------------------------------


def test_resolve_library_paths_absolute(tmp_path: Path) -> None:
    from databricks_agent_notebooks.cli import _resolve_library_paths
    lib_dir = tmp_path / "mylib"
    lib_dir.mkdir()
    result = _resolve_library_paths([str(lib_dir)], tmp_path)
    assert result == (str(lib_dir),)


def test_resolve_library_paths_relative(tmp_path: Path) -> None:
    from databricks_agent_notebooks.cli import _resolve_library_paths
    nb_dir = tmp_path / "notebooks"
    nb_dir.mkdir()
    lib_dir = tmp_path / "mylib"
    lib_dir.mkdir()
    result = _resolve_library_paths(["../mylib"], nb_dir)
    assert result == (str(lib_dir),)


def test_resolve_library_paths_src_layout_detection(tmp_path: Path) -> None:
    from databricks_agent_notebooks.cli import _resolve_library_paths
    lib_dir = tmp_path / "mylib"
    lib_dir.mkdir()
    (lib_dir / "pyproject.toml").write_text("[project]\nname = 'mylib'\n")
    (lib_dir / "src").mkdir()
    result = _resolve_library_paths([str(lib_dir)], tmp_path)
    assert result == (str(lib_dir / "src"),)


def test_resolve_library_paths_flat_layout(tmp_path: Path) -> None:
    from databricks_agent_notebooks.cli import _resolve_library_paths
    lib_dir = tmp_path / "mylib"
    lib_dir.mkdir()
    (lib_dir / "pyproject.toml").write_text("[project]\nname = 'mylib'\n")
    # No src/ subdirectory
    result = _resolve_library_paths([str(lib_dir)], tmp_path)
    assert result == (str(lib_dir),)


def test_resolve_library_paths_nonexistent_warns(tmp_path: Path, capsys) -> None:
    from databricks_agent_notebooks.cli import _resolve_library_paths
    result = _resolve_library_paths(["/nonexistent/path"], tmp_path)
    assert result == ("/nonexistent/path",)
    assert "warning" in capsys.readouterr().err.lower()


def test_no_inject_session_skips_library_resolution(tmp_path: Path, capsys) -> None:
    """--no-inject-session suppresses library path resolution and warnings."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="dev", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.resolve_params", return_value={"profile": "dev", "language": "python", "libraries": ["/nonexistent/lib"], "inject_session": False, "env": "default"}),
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime") as ensure_serverless,
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells") as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli._resolve_library_paths") as mock_resolve,
    ):
        result = main(["run", "--no-inject-session", "--library", "/nonexistent/lib", str(input_file)])

    assert result == 0
    mock_resolve.assert_not_called()
    mock_inject.assert_called_once()
    assert mock_inject.call_args.kwargs["inject_session"] is False
    ensure_serverless.assert_not_called()
    # No warnings should be emitted about the library path
    assert "warning" not in capsys.readouterr().err.lower()


# ---------------------------------------------------------------------------
# Three-level config integration tests (no merge_config mock)
# ---------------------------------------------------------------------------

def test_three_level_merge_project_frontmatter_cli(tmp_path: Path) -> None:
    """Integration test: real project + frontmatter + CLI merge, no merge_config mock.

    Verifies that pyproject.toml < frontmatter < CLI precedence works end-to-end
    through _cmd_run, including library override and params dict merge.
    """
    # Set up a .git boundary so find_project_config stops here
    (tmp_path / ".git").mkdir()

    # pyproject.toml: profile, timeout, a library, and a param
    (tmp_path / "pyproject.toml").write_text(
        '[tool.agent-notebook]\n'
        'profile = "project-profile"\n'
        'timeout = 600\n'
        'libraries = ["projlib"]\n'
        '\n'
        '[tool.agent-notebook.params]\n'
        'target_env = "staging"\n'
        'region = "us-east-1"\n',
        encoding="utf-8",
    )

    # Notebook frontmatter: overrides profile, adds a library and a param
    (tmp_path / "test.md").write_text(
        '---\n'
        'agent-notebook:\n'
        '  language: python\n'
        '  profile: frontmatter-profile\n'
        '  libraries:\n'
        '    - fmlib\n'
        '  params:\n'
        '    region: eu-west-1\n'
        '---\n'
        '\n'
        '# Test\n'
        '\n'
        '```python\n'
        'print(1)\n'
        '```\n',
        encoding="utf-8",
    )

    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")

    captured_config = {}

    def capture_inject(notebook, config, *args, **kwargs):
        captured_config["config"] = config
        return notebook

    managed_runtime = SimpleNamespace(
        runtime_id="dbr-16.4-python-3.12",
        python_executable=Path("/managed/runtime/bin/python"),
    )

    with (
        # DO NOT mock merge_config, parse_frontmatter, to_notebook, or find_project_config
        # -- those are what we're testing end-to-end.
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", side_effect=capture_inject),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service"),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
    ):
        # CLI: override profile, add another library, override one param
        result = main([
            "run",
            "--profile", "cli-profile",
            "--library", "clilib",
            "--param", "target_env=production",
            str(tmp_path / "test.md"),
        ])

    assert result == 0
    config = captured_config["config"]

    # CLI profile wins over frontmatter and project
    assert config.profile == "cli-profile"
    # Language from frontmatter (not in project or CLI)
    assert config.language == "python"
    # Timeout from project (not overridden by frontmatter or CLI)
    assert config.timeout == 600
    # Libraries: CLI overrides (last source wins, no concatenation)
    assert config.libraries is not None
    lib_basenames = [Path(p).name for p in config.libraries]
    assert lib_basenames == ["clilib"]
    # Params merge: project base, frontmatter overrides region, CLI overrides target_env
    assert config.params == {"target_env": "production", "region": "eu-west-1", "env": "default"}
    # Defaults applied
    assert config.format == "all"
    assert config.inject_session is True
    assert config.preprocess is True
    assert config.allow_errors is False
    assert config.clean is False


def test_frontmatter_preprocess_false_disables_preprocessing(tmp_path: Path) -> None:
    """Frontmatter preprocess: false should prevent directive expansion."""
    (tmp_path / ".git").mkdir()

    # No pyproject.toml -- project config is all-None

    (tmp_path / "test.md").write_text(
        '---\n'
        'agent-notebook:\n'
        '  language: python\n'
        '  preprocess: false\n'
        '---\n'
        '\n'
        '# Test\n'
        '\n'
        'This has a directive: {! param("x").with_default("hello").get() !}\n'
        '\n'
        '```python\n'
        'print(1)\n'
        '```\n',
        encoding="utf-8",
    )

    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")

    with (
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", side_effect=lambda nb, *a, **kw: nb),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.integrations.databricks.clusters.default_service"),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text") as mock_preprocess,
    ):
        result = main(["run", "--no-inject-session", str(tmp_path / "test.md")])

    assert result == 0
    # preprocess_text should NOT have been called since frontmatter says preprocess: false
    mock_preprocess.assert_not_called()


# ---------------------------------------------------------------------------
# Unified --cluster execution target normalization
# ---------------------------------------------------------------------------


def test_run_cluster_serverless_routes_to_serverless(tmp_path: Path, capsys) -> None:
    """--cluster SERVERLESS clears cluster and enters serverless path."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-managed",
        python_executable=Path("/managed/bin/python"),
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(cluster="SERVERLESS", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
    ):
        result = main(["run", "--cluster", "SERVERLESS", str(input_file)])

    assert result == 0
    # inject_cells should have been called with cluster=None (serverless)
    inject_config = mock_inject.call_args[0][1]
    assert inject_config.cluster is None
    # Verify serverless progress signal
    captured = capsys.readouterr()
    phase_lines = [line for line in captured.err.splitlines() if line.startswith("agent-notebook:")]
    assert any("mode=serverless" in line for line in phase_lines)


def test_run_cluster_serverless_case_insensitive(tmp_path: Path, capsys) -> None:
    """--cluster serverless (lowercase) also routes to serverless."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-managed",
        python_executable=Path("/managed/bin/python"),
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(cluster="serverless", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
    ):
        result = main(["run", "--cluster", "serverless", str(input_file)])

    assert result == 0
    inject_config = mock_inject.call_args[0][1]
    assert inject_config.cluster is None


def test_run_cluster_serverless_conflicts_with_local_spark_profile(tmp_path: Path, capsys) -> None:
    """--cluster SERVERLESS + --profile LOCAL_SPARK is an error."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
    ):
        result = main(["run", "--cluster", "SERVERLESS", "--profile", "LOCAL_SPARK", str(input_file)])

    assert result == 1
    captured = capsys.readouterr()
    assert "contradictory" in captured.err


def test_run_cluster_local_master_activates_local_spark(tmp_path: Path, capsys) -> None:
    """--cluster 'local[2]' activates local Spark with that master URL."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(cluster="local[2]", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
    ):
        result = main(["run", "--cluster", "local[2]", str(input_file)])

    assert result == 0
    # inject_cells called with local_spark=True and master_override="local[2]"
    assert mock_inject.call_args.kwargs["local_spark"] is True
    assert mock_inject.call_args.kwargs["master_override"] == "local[2]"
    # Cluster should be cleared on the config
    inject_config = mock_inject.call_args[0][1]
    assert inject_config.cluster is None
    # Verify local-spark progress signal
    captured = capsys.readouterr()
    phase_lines = [line for line in captured.err.splitlines() if line.startswith("agent-notebook:")]
    assert any("mode=local-spark" in line for line in phase_lines)


def test_run_cluster_bare_local_activates_local_spark(tmp_path: Path, capsys) -> None:
    """--cluster local (no brackets) activates local Spark."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(cluster="local", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
    ):
        result = main(["run", "--cluster", "local", str(input_file)])

    assert result == 0
    assert mock_inject.call_args.kwargs["local_spark"] is True
    assert mock_inject.call_args.kwargs["master_override"] == "local"


def test_run_legacy_local_spark_profile_emits_deprecation_warning(tmp_path: Path, capsys) -> None:
    """--profile LOCAL_SPARK (no --cluster) still works but emits deprecation warning."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
    ):
        result = main(["run", "--profile", "LOCAL_SPARK", str(input_file)])

    assert result == 0
    captured = capsys.readouterr()
    assert "deprecated" in captured.err.lower()
    assert 'local[*]' in captured.err


def test_run_cluster_local_plus_profile_local_spark_allowed_with_deprecation(tmp_path: Path, capsys) -> None:
    """--cluster local[2] + --profile LOCAL_SPARK is allowed but warns."""
    input_file = tmp_path / "test.md"
    input_file.write_text("# Test\n```python\nprint(1)\n```\n", encoding="utf-8")
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(profile="LOCAL_SPARK", cluster="local[2]", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
        patch.dict("sys.modules", {"pyspark": MagicMock()}),
    ):
        result = main(["run", "--cluster", "local[2]", "--profile", "LOCAL_SPARK", str(input_file)])

    assert result == 0
    # Should be local spark with master override
    assert mock_inject.call_args.kwargs["local_spark"] is True
    assert mock_inject.call_args.kwargs["master_override"] == "local[2]"
    # Should warn about deprecated profile
    captured = capsys.readouterr()
    assert "deprecated" in captured.err.lower()


def test_run_frontmatter_cluster_serverless_works(tmp_path: Path, capsys) -> None:
    """cluster: SERVERLESS in frontmatter routes to serverless (three-level config)."""
    input_file = tmp_path / "test.md"
    input_file.write_text(
        "---\nagent-notebook:\n  cluster: SERVERLESS\n  language: python\n---\n"
        "# Test\n```python\nprint(1)\n```\n",
        encoding="utf-8",
    )
    notebook = _make_notebook_mock()
    notebook.metadata = {"kernelspec": {"name": "python3", "language": "python"}}
    executed_notebook = tmp_path / "test.executed.ipynb"
    executed_notebook.write_text("{}", encoding="utf-8")
    managed_runtime = SimpleNamespace(
        runtime_id="dbr-managed",
        python_executable=Path("/managed/bin/python"),
    )

    with (
        patch(
            "databricks_agent_notebooks.cli.to_notebook",
            return_value=(notebook, DatabricksConfig(cluster="SERVERLESS", language="python")),
        ),
        patch("databricks_agent_notebooks.cli.validate_single_language"),
        patch("databricks_agent_notebooks.cli.load_project_source_map", return_value=({}, None)),
        patch("databricks_agent_notebooks.cli.inject_lifecycle_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ),
        patch("databricks_agent_notebooks.runtime.connect.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.nbformat.write"),
        patch("databricks_agent_notebooks.cli.preprocess_text", side_effect=lambda text, **kw: text),
    ):
        result = main(["run", str(input_file)])

    assert result == 0
    inject_config = mock_inject.call_args[0][1]
    assert inject_config.cluster is None


# ---------------------------------------------------------------------------
# _build_cli_source_map -- --params JSON validation
# ---------------------------------------------------------------------------


def test_params_json_rejects_list() -> None:
    """--params with a JSON array should raise SystemExit."""
    parser = _build_parser()
    args = parser.parse_args(["run", "nb.md", "--params", '["a", "b"]'])
    with pytest.raises(SystemExit, match="must be an object"):
        _build_cli_source_map(args)


def test_params_json_rejects_string() -> None:
    """--params with a JSON string should raise SystemExit."""
    parser = _build_parser()
    args = parser.parse_args(["run", "nb.md", "--params", '"hello"'])
    with pytest.raises(SystemExit, match="must be an object"):
        _build_cli_source_map(args)


def test_params_json_rejects_number() -> None:
    """--params with a JSON number should raise SystemExit."""
    parser = _build_parser()
    args = parser.parse_args(["run", "nb.md", "--params", "42"])
    with pytest.raises(SystemExit, match="must be an object"):
        _build_cli_source_map(args)


def test_params_json_accepts_object() -> None:
    """--params with a JSON object should work."""
    parser = _build_parser()
    args = parser.parse_args(["run", "nb.md", "--params", '{"key": "val"}'])
    source = _build_cli_source_map(args)
    assert source["params"] == {"key": "val"}
