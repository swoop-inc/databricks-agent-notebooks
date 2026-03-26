"""Tests for Scala Databricks Connect version resolution and pre-fetch."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from databricks_agent_notebooks._constants import SCALA_212, SCALA_213
from databricks_agent_notebooks.integrations.databricks.clusters import Cluster, ClusterError
from databricks_agent_notebooks.runtime.scala_connect import (
    prefetch_scala_connect,
    resolve_scala_connect,
    resolve_scala_connect_line,
)


def _make_cluster(spark_version: str = "16.4.x-scala2.12") -> Cluster:
    return Cluster(
        cluster_id="abc-123",
        cluster_name="test-cluster",
        state="RUNNING",
        spark_version=spark_version,
    )


# ---------------------------------------------------------------------------
# resolve_scala_connect_line
# ---------------------------------------------------------------------------


def test_resolve_scala_connect_line_extracts_16_4() -> None:
    cluster = _make_cluster("16.4.x-scala2.12")
    assert resolve_scala_connect_line(cluster) == "16.4"


def test_resolve_scala_connect_line_extracts_15_4() -> None:
    cluster = _make_cluster("15.4.x-photon-scala2.12")
    assert resolve_scala_connect_line(cluster) == "15.4"


def test_resolve_scala_connect_line_extracts_13_3() -> None:
    cluster = _make_cluster("13.3.x-scala2.12")
    assert resolve_scala_connect_line(cluster) == "13.3"


def test_resolve_scala_connect_returns_212_variant_for_dbr_16() -> None:
    cluster = _make_cluster("16.4.x-scala2.12")
    line, variant = resolve_scala_connect(cluster)
    assert line == "16.4"
    assert variant is SCALA_212


def test_resolve_scala_connect_returns_213_variant_for_dbr_17() -> None:
    cluster = _make_cluster("17.3.x-scala2.13")
    line, variant = resolve_scala_connect(cluster)
    assert line == "17.3"
    assert variant is SCALA_213


def test_resolve_scala_connect_returns_213_variant_for_dbr_18() -> None:
    cluster = _make_cluster("18.1.x-scala2.13")
    line, variant = resolve_scala_connect(cluster)
    assert line == "18.1"
    assert variant is SCALA_213


# ---------------------------------------------------------------------------
# prefetch_scala_connect
# ---------------------------------------------------------------------------

_FAKE_FETCH_OUTPUT = "\n".join([
    "/cache/com/databricks/databricks-connect/16.4.7/databricks-connect-16.4.7.jar",
    "/cache/com/databricks/databricks-sdk-java/0.60.0/databricks-sdk-java-0.60.0.jar",
    "/cache/org/json4s/json4s-core_2.12/3.7.0-M11/json4s-core_2.12-3.7.0-M11.jar",
])


def test_prefetch_runs_coursier_fetch_and_returns_version() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=_FAKE_FETCH_OUTPUT, stderr="")

    version = prefetch_scala_connect(
        "16.4",
        subprocess_run=fake_run,
        find_coursier_fn=lambda: "/opt/bin/coursier",
    )

    assert version == "16.4.7"
    assert len(calls) == 1
    assert calls[0][0] == "/opt/bin/coursier"
    assert calls[0][1] == "fetch"
    assert "com.databricks:databricks-connect:16.4.+" in calls[0][2]


def test_prefetch_extracts_version_from_15_4_output() -> None:
    output = "/cache/databricks-connect/15.4.6/databricks-connect-15.4.6.jar\n"

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, stdout=output, stderr="")

    version = prefetch_scala_connect(
        "15.4",
        subprocess_run=fake_run,
        find_coursier_fn=lambda: "/opt/bin/coursier",
    )
    assert version == "15.4.6"


def test_prefetch_raises_when_coursier_missing() -> None:
    with pytest.raises(RuntimeError, match="coursier is required"):
        prefetch_scala_connect(
            "16.4",
            find_coursier_fn=lambda: None,
        )


def test_prefetch_raises_on_resolution_failure() -> None:
    def fake_run(cmd, **kwargs):
        raise subprocess.CalledProcessError(1, cmd, stderr="Resolution error")

    with pytest.raises(subprocess.CalledProcessError):
        prefetch_scala_connect(
            "16.4",
            subprocess_run=fake_run,
            find_coursier_fn=lambda: "/opt/bin/coursier",
        )


def test_prefetch_uses_213_artifact_coordinate() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(
            cmd, 0,
            stdout="/cache/com/databricks/databricks-connect_2.13-17.3.4.jar\n",
            stderr="",
        )

    version = prefetch_scala_connect(
        "17.3",
        SCALA_213,
        subprocess_run=fake_run,
        find_coursier_fn=lambda: "/opt/bin/coursier",
    )

    assert version == "17.3.4"
    assert "com.databricks:databricks-connect_2.13:17.3.+" in calls[0][2]


def test_prefetch_extracts_version_from_213_jar_path() -> None:
    output = "/cache/com/databricks/databricks-connect_2.13/17.3.4/databricks-connect_2.13-17.3.4.jar\n"

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, stdout=output, stderr="")

    version = prefetch_scala_connect(
        "17.3",
        SCALA_213,
        subprocess_run=fake_run,
        find_coursier_fn=lambda: "/opt/bin/coursier",
    )
    assert version == "17.3.4"


def test_prefetch_raises_when_version_not_parseable() -> None:
    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, stdout="some-unexpected-output\n", stderr="")

    with pytest.raises(RuntimeError, match="Could not determine resolved version"):
        prefetch_scala_connect(
            "16.4",
            subprocess_run=fake_run,
            find_coursier_fn=lambda: "/opt/bin/coursier",
        )
