"""Plugin interface for the preprocessing pipeline.

Plugins are Python classes that Jinja2 calls directly as template globals.
The ABC ensures every plugin is self-describing and follows the lifecycle
contract (construct with context, use, cleanup).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PluginMetadata:
    """Describes a plugin for discovery, help text, and documentation generation."""

    name: str  # ^[a-z][a-z0-9_]*$
    description: str  # short, for CLI help
    markdown_docs: str | None = None  # long-form docs, H3+ only, no top heading


@dataclass(frozen=True)
class PluginContext:
    """Immutable context passed to every plugin at construction time.

    Provides the notebook path so plugins can resolve relative references
    (e.g., include paths) against the notebook's location on disk.
    """

    notebook_path: Path
    params: dict[str, str] = field(default_factory=dict)

    @property
    def notebook_dir(self) -> Path:
        """Directory containing the notebook being preprocessed."""
        return self.notebook_path.parent


class PreprocessorPlugin(ABC):
    """Base class for preprocessing plugins.

    Subclasses register as Jinja2 template globals under their metadata name.
    Jinja2 resolves ``{! name("arg") !}`` as ``globals["name"]("arg")``, so
    the plugin's ``__call__`` method is the directive implementation.
    """

    @classmethod
    @abstractmethod
    def plugin_metadata(cls) -> PluginMetadata:
        """Return static metadata describing this plugin."""
        ...

    @abstractmethod
    def __init__(self, context: PluginContext) -> None:
        """Initialize with the current preprocessing context."""
        ...

    def cleanup(self) -> None:
        """Release resources. Called in a finally block -- must not raise."""
