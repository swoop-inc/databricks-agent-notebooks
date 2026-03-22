"""Tests for Databricks cluster discovery."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from databricks_agent_notebooks.integrations.databricks.clusters import CliClusterService, Cluster, ClusterError


_TWO_CLUSTERS = {
    "clusters": [
        {
            "cluster_id": "1003-184738-wkj97rxa",
            "cluster_name": "rnd-alpha",
            "state": "RUNNING",
            "spark_version": "16.4.x-scala2.12",
        },
        {
            "cluster_id": "2004-295849-xyz12abc",
            "cluster_name": "rnd-beta",
            "state": "TERMINATED",
            "spark_version": "15.3.x-scala2.12",
        },
    ]
}


@pytest.fixture()
def service() -> CliClusterService:
    return CliClusterService()


def test_list_clusters_parses_json(service: CliClusterService) -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = json.dumps(_TWO_CLUSTERS)
        mock_run.return_value.stderr = ""

        clusters = service.list_clusters("dev")

    assert clusters[0] == Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")


def test_resolve_cluster_exact_match(service: CliClusterService) -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = json.dumps(_TWO_CLUSTERS)
        mock_run.return_value.stderr = ""

        cluster = service.resolve_cluster("rnd-alpha", "dev")

    assert cluster.cluster_name == "rnd-alpha"


def test_resolve_cluster_ambiguous_raises(service: CliClusterService) -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = json.dumps(_TWO_CLUSTERS)
        mock_run.return_value.stderr = ""

        with pytest.raises(ClusterError, match="ambiguous"):
            service.resolve_cluster("rnd", "dev")
