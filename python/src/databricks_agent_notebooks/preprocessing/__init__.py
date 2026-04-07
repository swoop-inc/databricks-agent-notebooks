"""Text-based preprocessing pipeline for notebook source files.

Expands directives (e.g., ``{! include("path") !}``) in raw file content
before the notebook parsing step.  If no directives are present, the file
passes through unchanged with zero overhead.
"""

from databricks_agent_notebooks.preprocessing.engine import preprocess_text

__all__ = ["preprocess_text"]
