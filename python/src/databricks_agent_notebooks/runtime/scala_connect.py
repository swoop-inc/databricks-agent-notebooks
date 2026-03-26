"""Scala Databricks Connect version resolution and coursier pre-fetch.

Resolves the correct ``databricks-connect`` Maven artifact version for a
cluster's DBR line and pre-downloads it via coursier so the Almond kernel's
``$ivy`` import is a cache hit.

Supports **Scala 2.12** (DBR <= 16.x) and **Scala 2.13** (DBR 17+).  The
:func:`resolve_scala_connect` function returns both the connect line and the
matching :class:`~databricks_agent_notebooks._constants.ScalaVariant`.
"""

from __future__ import annotations

import re
import subprocess
from typing import Callable

from databricks_agent_notebooks._constants import ScalaVariant, scala_variant_for_dbr
from databricks_agent_notebooks.integrations.databricks.clusters import (
    Cluster,
    resolve_cluster_databricks_line,
)
from databricks_agent_notebooks.runtime.kernel import find_coursier

ARTIFACT_VERSION_RE = re.compile(
    r"/databricks-connect(?:_2\.13)?-(?P<version>\d+\.\d+\.\d+)\.jar$"
)


def resolve_scala_connect(cluster: Cluster) -> tuple[str, ScalaVariant]:
    """Derive the Databricks Connect line and Scala variant from a cluster.

    Returns a ``(dbr_line, scala_variant)`` tuple where *dbr_line* is the
    ``major.minor`` connect line (e.g. ``"16.4"``) and *scala_variant*
    captures Scala-version-specific configuration.
    """
    dbr_line = resolve_cluster_databricks_line(cluster)
    major = int(dbr_line.split(".")[0])
    return dbr_line, scala_variant_for_dbr(major)


# Backward-compat alias — callers that only need the line can still use it.
def resolve_scala_connect_line(cluster: Cluster) -> str:
    """Derive the Databricks Connect line for Scala from a cluster.

    Returns the ``major.minor`` connect line (e.g. ``"16.4"``).

    .. deprecated::
        Prefer :func:`resolve_scala_connect` which also returns the
        :class:`ScalaVariant`.
    """
    line, _variant = resolve_scala_connect(cluster)
    return line


def prefetch_scala_connect(
    connect_line: str,
    variant: ScalaVariant | None = None,
    *,
    subprocess_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    find_coursier_fn: Callable[[], str | None] = find_coursier,
) -> str:
    """Pre-download Databricks Connect JARs and return the resolved version.

    Runs ``coursier fetch`` to populate the local cache so that the Almond
    kernel's ``import $ivy`` resolves instantly.  Parses the fetched artifact
    paths to determine the exact version that was resolved.

    Parameters
    ----------
    connect_line:
        The ``major.minor`` connect line (e.g. ``"16.4"``).
    variant:
        The Scala variant to use for Maven coordinates.  Defaults to
        Scala 2.12 behavior when *None* (backward compat).
    subprocess_run:
        Injectable subprocess runner (for testing).
    find_coursier_fn:
        Injectable coursier locator (for testing).

    Returns
    -------
    str
        The exact resolved version (e.g. ``"16.4.7"``).

    Raises
    ------
    RuntimeError
        If coursier is not installed or the artifact cannot be resolved.
    """
    coursier_bin = find_coursier_fn()
    if coursier_bin is None:
        raise RuntimeError(
            "coursier is required for Scala Databricks Connect resolution. "
            "Install via: brew install coursier/formulas/coursier"
        )

    if variant is not None:
        artifact = variant.maven_artifact
    else:
        artifact = "databricks-connect"

    coordinate = f"com.databricks:{artifact}:{connect_line}.+"
    result = subprocess_run(
        [coursier_bin, "fetch", coordinate],
        capture_output=True,
        text=True,
        check=True,
    )

    for line in result.stdout.strip().splitlines():
        match = ARTIFACT_VERSION_RE.search(line)
        if match:
            return match.group("version")

    raise RuntimeError(
        f"Could not determine resolved version from coursier fetch output "
        f"for {coordinate}"
    )
