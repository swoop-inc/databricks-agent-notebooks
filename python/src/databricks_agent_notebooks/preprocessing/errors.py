"""Error types for the preprocessing pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks_agent_notebooks.preprocessing.plugins import PreprocessorPlugin


class PreprocessorError(Exception):
    """Base error for preprocessing failures.

    Wraps plugin identity and structured detail so callers can produce
    actionable diagnostics without introspecting the exception chain.
    """

    def __init__(
        self,
        plugin: PreprocessorPlugin | None,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.plugin = plugin
        self.plugin_name = plugin.plugin_metadata().name if plugin else "engine"
        self.detail = detail or {}
        super().__init__(f"[preprocessing:{self.plugin_name}] {message}")
