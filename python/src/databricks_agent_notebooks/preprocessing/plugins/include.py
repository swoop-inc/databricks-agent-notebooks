"""Built-in include plugin -- inline file content at preprocessing time.

Directive syntax: ``{! include("path") !}``

Relative paths are resolved from the notebook's directory.  Absolute paths
are used as-is.  No containment boundary is enforced -- the execution
environment (sandbox, OS permissions) is the access-control layer.
"""

from __future__ import annotations

from pathlib import Path

from databricks_agent_notebooks.preprocessing.errors import PreprocessorError
from databricks_agent_notebooks.preprocessing.plugins import (
    PluginContext,
    PluginMetadata,
    PreprocessorPlugin,
)

_DOCS = """\
### Syntax

```
{! include("path/to/file") !}
```

### Behavior

- Relative paths are resolved from the directory containing the notebook.
- Absolute paths are used as-is.
- The included file's content is inserted verbatim -- no further directive
  expansion occurs within included content.

### Examples

Include a shared SQL fragment (same directory):

```
{! include("fragments/common_ctes.sql") !}
```

Include a Python utility from a sibling directory:

```python
{! include("../../python/src/shared/spark_helpers.py") !}
```

Include by absolute path:

```python
{! include("/opt/shared-notebooks/common_setup.py") !}
```
"""


class IncludePlugin(PreprocessorPlugin):
    """Include file content as-is. Resolves relative to notebook directory."""

    _METADATA = PluginMetadata(
        name="include",
        description="Include file content verbatim",
        markdown_docs=_DOCS,
    )

    @classmethod
    def plugin_metadata(cls) -> PluginMetadata:
        return cls._METADATA

    def __init__(self, context: PluginContext) -> None:
        self._notebook_dir = context.notebook_dir.resolve()

    def __call__(self, path: str) -> str:
        """Read and return the content of the file at *path*.

        Raises PreprocessorError if the file is not found or cannot be read.
        """
        resolved = (self._notebook_dir / path).resolve()

        try:
            exists = resolved.is_file()
        except OSError as exc:
            raise PreprocessorError(
                self,
                f"Cannot access {path!r} (resolved to {resolved}): {exc}",
                detail={"path": path, "resolved": str(resolved)},
            ) from exc

        if not exists:
            raise PreprocessorError(
                self,
                f"File not found: {path!r} (resolved to {resolved})",
                detail={"path": path, "resolved": str(resolved)},
            )

        try:
            return resolved.read_text()
        except OSError as exc:
            raise PreprocessorError(
                self,
                f"Cannot read {path!r} (resolved to {resolved}): {exc}",
                detail={"path": path, "resolved": str(resolved)},
            ) from exc
