"""Databricks cluster operations — list, resolve, and check availability.

Exposes a protocol-based API so callers can swap in test doubles.
The default implementation shells out to the ``databricks`` CLI.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from typing import Protocol

# Databricks cluster IDs match this pattern: MMDD-HHMMSS-xxxxxxxx
_CLUSTER_ID_RE = re.compile(r"^\d{4}-\d{6}-[a-z0-9]{8}$")


@dataclass(frozen=True)
class Cluster:
    """Immutable snapshot of a Databricks cluster's identity and state."""

    cluster_id: str
    cluster_name: str
    state: str
    spark_version: str


class ClusterError(Exception):
    """User-friendly cluster operation error."""


class ClusterService(Protocol):
    """Minimal interface for cluster discovery."""

    def list_clusters(self, profile: str) -> list[Cluster]: ...

    def resolve_cluster(self, name: str, profile: str) -> Cluster: ...

    def check_available(self) -> bool: ...


class CliClusterService:
    """Implementation backed by the ``databricks`` CLI subprocess."""

    def list_clusters(self, profile: str) -> list[Cluster]:
        """List all clusters visible to *profile*.

        Runs ``databricks clusters list --profile <profile> --output json``
        and parses the result into :class:`Cluster` objects.
        """
        try:
            result = subprocess.run(
                ["databricks", "clusters", "list", "--profile", profile, "--output", "json"],
                capture_output=True,
                text=True,
                timeout=120,
            )
        except FileNotFoundError:
            raise ClusterError("databricks CLI not found on PATH")

        if result.returncode != 0:
            msg = result.stderr.strip() or f"databricks clusters list failed (exit {result.returncode})"
            raise ClusterError(msg)

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ClusterError(f"failed to parse CLI output: {exc}")

        raw_clusters = data.get("clusters", [])
        return [
            Cluster(
                cluster_id=c["cluster_id"],
                cluster_name=c["cluster_name"],
                state=c["state"],
                spark_version=c["spark_version"],
            )
            for c in raw_clusters
        ]

    def resolve_cluster(self, name: str, profile: str) -> Cluster:
        """Find a cluster by *name* or *cluster ID*.

        If *name* matches the Databricks cluster ID format (``MMDD-HHMMSS-xxxxxxxx``),
        it is returned directly as a :class:`Cluster` without calling ``list_clusters``.
        Otherwise, searches by exact name match first, then substring.

        Raises :class:`ClusterError` if no match is found or if a substring
        search produces multiple candidates (ambiguous).
        """
        # Short-circuit: if it's already a cluster ID, skip the API call.
        if _CLUSTER_ID_RE.match(name):
            return Cluster(
                cluster_id=name,
                cluster_name=name,
                state="UNKNOWN",
                spark_version="UNKNOWN",
            )

        clusters = self.list_clusters(profile)

        # Exact match takes priority.
        for c in clusters:
            if c.cluster_name == name:
                return c

        # Fall back to substring match.
        matches = [c for c in clusters if name in c.cluster_name]

        if len(matches) == 1:
            return matches[0]

        if len(matches) > 1:
            names = ", ".join(c.cluster_name for c in matches)
            raise ClusterError(f"ambiguous cluster name '{name}' — matches: {names}")

        raise ClusterError(f"no cluster matching '{name}'")

    def check_available(self) -> bool:
        """Return ``True`` if the ``databricks`` CLI is on PATH."""
        return shutil.which("databricks") is not None


def default_service() -> ClusterService:
    """Return the standard CLI-backed cluster service."""
    return CliClusterService()
