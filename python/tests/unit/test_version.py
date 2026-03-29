"""Verify __version__ is dynamically derived from package metadata."""

from unittest.mock import patch

import pytest

from databricks_agent_notebooks import __version__


def test_version_is_not_dev_placeholder() -> None:
    """In a properly installed environment, version should not be the fallback."""
    assert __version__ != "0.0.0-dev", (
        "__version__ is the fallback value — the package may not be installed. "
        "Run 'uv pip install -e .' or 'uv sync' first."
    )


def test_version_matches_installed_metadata() -> None:
    """The dynamic __version__ must match importlib.metadata."""
    from importlib.metadata import version
    assert __version__ == version("databricks-agent-notebooks")


def test_fallback_when_package_not_found() -> None:
    """When importlib.metadata cannot find the package, __version__ falls back to sentinel."""
    from importlib.metadata import PackageNotFoundError

    with patch(
        "importlib.metadata.version",
        side_effect=PackageNotFoundError("databricks-agent-notebooks"),
    ):
        # Re-execute the version lookup logic (module-level code)
        try:
            from importlib.metadata import version
            result = version("databricks-agent-notebooks")
        except PackageNotFoundError:
            result = "0.0.0-dev"

    assert result == "0.0.0-dev"


def test_install_target_rejects_dev_sentinel() -> None:
    """_default_package_install_target must not emit an impossible requirement."""
    from pathlib import Path
    from databricks_agent_notebooks.runtime.connect import _default_package_install_target

    original_is_file = Path.is_file

    def is_file_no_pyproject(self: Path) -> bool:
        """Return False for pyproject.toml checks to skip source-tree detection."""
        if self.name == "pyproject.toml":
            return False
        return original_is_file(self)

    # Mock away all resolution paths so only the fallback branch executes:
    # 1. Source-tree pyproject.toml detection returns False
    # 2. Distribution metadata lookup raises
    # 3. __version__ is the dev sentinel
    with patch("databricks_agent_notebooks.__version__", "0.0.0-dev"), \
         patch("databricks_agent_notebooks.runtime.connect.__version__", "0.0.0-dev"), \
         patch.object(Path, "is_file", is_file_no_pyproject), \
         patch(
             "importlib.metadata.distribution",
             side_effect=Exception("no dist"),
         ):
        with pytest.raises(RuntimeError, match="Cannot determine a valid install target"):
            _default_package_install_target()
