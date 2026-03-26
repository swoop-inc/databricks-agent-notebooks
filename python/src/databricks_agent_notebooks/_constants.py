"""Shared constants for the databricks_agent_notebooks library."""

from __future__ import annotations

from dataclasses import dataclass

# Kernel metadata per language — single source of truth.
# Used by _converter.py, _dbr_source.py, and cli.py to set/read kernel metadata.
KERNELSPECS: dict[str, dict[str, str]] = {
    "python": {
        "name": "python3",
        "display_name": "Python 3",
        "language": "python",
    },
    "scala": {
        "name": "scala212-dbr-connect",
        "display_name": "Scala 2.12 (Databricks Connect)",
        "language": "scala",
    },
    "sql": {
        "name": "python3",
        "display_name": "Python 3 (SQL wrapper)",
        "language": "sql",
    },
}

# Fallback Databricks Connect version for Scala injection when no cluster is
# specified (e.g. serverless).  When a cluster IS specified, the version is
# resolved dynamically from the cluster's DBR line via scala_connect.py.
DATABRICKS_CONNECT_VERSION = "16.4.7"
DATABRICKS_CONNECT_LINE = "16.4"

# Scala 2.13 / DBR 17+ fallback constants (parallel to the 2.12 ones above).
DATABRICKS_CONNECT_213_VERSION = "17.3.4"
DATABRICKS_CONNECT_213_LINE = "17.3"


# ---------------------------------------------------------------------------
# ScalaVariant — Scala-version-specific configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScalaVariant:
    """Encapsulates differences between Scala 2.12 and 2.13 pipelines.

    Fields
    ------
    scala_version:
        ``"2.12"`` or ``"2.13"``.
    kernel_id:
        Jupyter kernel identifier (directory name).
    kernel_display_name:
        Human-readable kernel display name.
    maven_artifact:
        Maven artifact name for Databricks Connect (no group prefix).
        ``"databricks-connect"`` for 2.12, ``"databricks-connect_2.13"`` for 2.13.
    ivy_separator:
        Separator between group and artifact in Ammonite ``$ivy`` imports.
        Single ``:`` for 2.12 (literal artifact), double ``::`` for 2.13
        (Scala-suffixed).
    min_jdk:
        Minimum JDK major version required.
    """

    scala_version: str
    kernel_id: str
    kernel_display_name: str
    maven_artifact: str
    ivy_separator: str
    min_jdk: int


SCALA_212 = ScalaVariant(
    scala_version="2.12",
    kernel_id="scala212-dbr-connect",
    kernel_display_name="Scala 2.12 (Databricks Connect)",
    maven_artifact="databricks-connect",
    ivy_separator=":",
    min_jdk=11,
)

SCALA_213 = ScalaVariant(
    scala_version="2.13",
    kernel_id="scala213-dbr-connect",
    kernel_display_name="Scala 2.13 (Databricks Connect)",
    maven_artifact="databricks-connect_2.13",
    ivy_separator="::",
    min_jdk=17,
)

SCALA_VARIANTS: dict[str, ScalaVariant] = {
    "2.12": SCALA_212,
    "2.13": SCALA_213,
}

# Default variant for serverless / no-cluster fallback (current LTS is DBR 17.3).
DEFAULT_SCALA_VARIANT = SCALA_213


def scala_variant_for_dbr(major: int) -> ScalaVariant:
    """Return the appropriate :class:`ScalaVariant` for a DBR major version."""
    if major >= 17:
        return SCALA_213
    return SCALA_212
