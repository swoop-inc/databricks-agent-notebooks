"""Smoke tests for the standalone CLI surface."""

from __future__ import annotations

import builtins
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from databricks_agent_notebooks.cli import _build_parser, _validate_scala_local_spark, main
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
        patch("databricks_agent_notebooks.cli.merge_config", return_value=DatabricksConfig(profile="prod", cluster="my-cluster", language="python")),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.ensure_cluster_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.default_service", return_value=service),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod"),
        ),
        patch(
            "databricks_agent_notebooks.cli.inject_cells",
            return_value=notebook,
        ),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.ensure_serverless_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.default_service"),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod"),
        ),
        patch(
            "databricks_agent_notebooks.cli.inject_cells",
            return_value=notebook,
        ),
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.default_service"),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod", cluster="my-cluster", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.cli.ensure_cluster_runtime") as ensure_cluster_runtime,
        patch("databricks_agent_notebooks.cli.resolve_scala_connect", return_value=("16.4", SCALA_212)),
        patch("databricks_agent_notebooks.cli.prefetch_scala_connect", return_value="16.4.7"),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod", cluster="my-cluster", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.cli.resolve_scala_connect", return_value=("16.4", SCALA_212)) as mock_resolve,
        patch("databricks_agent_notebooks.cli.prefetch_scala_connect", return_value="16.4.7") as mock_prefetch,
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook) as mock_inject,
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod", cluster="my-cluster", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.cli.resolve_scala_connect", return_value=("16.4", SCALA_212)),
        patch("databricks_agent_notebooks.cli.prefetch_scala_connect", side_effect=RuntimeError("coursier is required")),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod", cluster="my-cluster", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.default_service", return_value=MagicMock(resolve_cluster=MagicMock(return_value=cluster))),
        patch("databricks_agent_notebooks.cli.resolve_scala_connect", return_value=("17.3", SCALA_213)) as mock_resolve,
        patch("databricks_agent_notebooks.cli.prefetch_scala_connect", return_value="17.3.4") as mock_prefetch,
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook) as mock_inject,
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook) as mock_inject,
        patch(
            "databricks_agent_notebooks.cli.execute_notebook",
            return_value=MagicMock(success=True, output_path=executed_notebook, duration_seconds=1.0, error=None),
        ) as execute_notebook,
        patch("databricks_agent_notebooks.cli.render", return_value={"md": tmp_path / "out.md"}),
        patch("databricks_agent_notebooks.cli.default_service"),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="prod", cluster="my-cluster", language="python"),
        ),
        patch("databricks_agent_notebooks.cli.default_service", return_value=service),
        patch("databricks_agent_notebooks.cli.ensure_cluster_runtime") as ensure_cluster_runtime,
        patch("databricks_agent_notebooks.cli.inject_cells") as inject_cells,
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
    inject_cells.assert_not_called()
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(cluster="my-cluster", language="python"),
        ),
        patch("databricks_agent_notebooks.cli.default_service", return_value=service),
        patch("databricks_agent_notebooks.cli.ensure_cluster_runtime", return_value=managed_runtime),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook) as inject_cells,
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
    inject_cells.assert_called_once_with(
        notebook,
        DatabricksConfig(profile="DEFAULT", cluster="abc-123", language="python"),
        input_file.resolve(),
        local_spark=False,
        scala_connect_version=None,
        scala_variant=None,
    )


def test_clusters_command_passes_profile_to_service_and_prints_clusters(capsys) -> None:
    cluster = Cluster(
        cluster_id="1003-184738-wkj97rxa",
        cluster_name="rnd-alpha",
        state="RUNNING",
        spark_version="16.4.x-scala2.12",
    )
    service = MagicMock(iter_clusters=MagicMock(return_value=iter([[cluster]])))

    with patch("databricks_agent_notebooks.cli.default_service", return_value=service):
        result = main(["clusters", "--profile", "prod"])

    assert result == 0
    service.iter_clusters.assert_called_once_with("prod")
    captured = capsys.readouterr()
    assert "rnd-alpha" in captured.out
    assert "1003-184738-wkj97rxa" in captured.out


def test_clusters_command_no_clusters_prints_message(capsys) -> None:
    service = MagicMock(iter_clusters=MagicMock(return_value=iter([[]])))

    with patch("databricks_agent_notebooks.cli.default_service", return_value=service):
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
        raise ClusterError("cluster listing did not complete within 30.0 seconds")

    service = MagicMock(iter_clusters=MagicMock(side_effect=_iter_then_fail))

    with patch("databricks_agent_notebooks.cli.default_service", return_value=service):
        result = main(["clusters", "--profile", "prod"])

    assert result == 1
    captured = capsys.readouterr()
    assert "rnd-alpha" in captured.out
    assert "did not complete within 30.0 seconds" in captured.err


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
        patch("databricks_agent_notebooks.cli.default_service", return_value=service),
        patch("databricks_agent_notebooks.cli.ensure_cluster_runtime") as ensure_cluster_runtime,
        patch("databricks_agent_notebooks.cli.inject_cells") as inject_cells,
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
    inject_cells.assert_not_called()
    assert execute_notebook.call_args.kwargs["python_executable"] is None


def test_install_kernel_command_delegates(tmp_path: Path, capsys) -> None:
    kernel_dir = tmp_path / "kernels" / "scala212-dbr-connect"

    with patch("databricks_agent_notebooks.cli.install_kernel", return_value=kernel_dir) as mock_install:
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

    with patch("databricks_agent_notebooks.cli.install_kernel", return_value=kernel_dir) as install_kernel:
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
        "databricks_agent_notebooks.cli.list_installed_kernels",
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

    with patch("databricks_agent_notebooks.cli.list_installed_runtimes", return_value=[runtime]) as list_installed_runtimes:
        result = main(["runtimes", "list"])

    assert result == 0
    list_installed_runtimes.assert_called_once_with()
    captured = capsys.readouterr()
    assert "dbr-16.4-python-3.12" in captured.out
    assert "materialized" in captured.out


def test_kernels_remove_command_delegates(tmp_path: Path, capsys) -> None:
    removed_dir = tmp_path / "runtime" / "scala212-dbr-connect"

    with patch(
        "databricks_agent_notebooks.cli.remove_kernel",
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
        patch("databricks_agent_notebooks.cli.run_checks", return_value=kernel_checks) as mock_run_checks,
        patch("databricks_agent_notebooks.cli.doctor_installed_runtimes", return_value=runtime_checks) as doctor_installed_runtimes,
        patch("databricks_agent_notebooks.cli.doctor_scala_connect_readiness", return_value=[]),
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
        patch("databricks_agent_notebooks.cli.run_checks", return_value=[]) as run_checks,
        patch("databricks_agent_notebooks.cli.doctor_installed_runtimes", return_value=[]) as doctor_installed_runtimes,
        patch("databricks_agent_notebooks.cli.doctor_scala_connect_readiness", return_value=[]),
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
        patch("databricks_agent_notebooks.cli.run_checks", return_value=kernel_checks),
        patch("databricks_agent_notebooks.cli.doctor_installed_runtimes", return_value=runtime_checks),
        patch("databricks_agent_notebooks.cli.doctor_scala_connect_readiness", return_value=scala_checks),
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
        patch("databricks_agent_notebooks.cli.run_checks", return_value=kernel_checks),
        patch("databricks_agent_notebooks.cli.doctor_installed_runtimes", return_value=runtime_checks),
        patch("databricks_agent_notebooks.cli.doctor_scala_connect_readiness", return_value=[]),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="python"),
        ),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="python"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("builtins.__import__", side_effect=mock_import),
    ):
        result = main(["run", str(input_file)])
    sys.modules.update(_saved)

    assert result == 1
    captured = capsys.readouterr()
    assert "pyspark is required for Python LOCAL_SPARK" in captured.err


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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="python"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("importlib.util.find_spec", return_value=MagicMock()),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
        patch("databricks_agent_notebooks.cli.inject_cells", return_value=notebook),
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
        patch(
            "databricks_agent_notebooks.cli.merge_config",
            return_value=DatabricksConfig(profile="LOCAL_SPARK", language="scala"),
        ),
        patch("databricks_agent_notebooks.cli.is_local_spark", return_value=True),
    ):
        rc = main(["run", str(input_file)])

    assert rc == 1
    captured = capsys.readouterr()
    assert "not supported for Scala" in captured.err
