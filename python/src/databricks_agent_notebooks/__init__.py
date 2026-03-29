"""Standalone Databricks notebook conversion and execution tooling."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("databricks-agent-notebooks")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

__all__ = ["__version__"]
