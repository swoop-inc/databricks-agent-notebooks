"""Built-in param plugin -- CLI-driven parameterization of notebooks.

Directive syntax: ``{! param("name") !}``

Values are supplied via ``--param name=value`` on the CLI and resolved at
preprocessing time, before the notebook enters the parsing pipeline.
"""

from __future__ import annotations

import re

from databricks_agent_notebooks.preprocessing.errors import PreprocessorError
from databricks_agent_notebooks.preprocessing.plugins import (
    PluginContext,
    PluginMetadata,
    PreprocessorPlugin,
)

_DOCS = """\
### Syntax

```
{! param("name") !}
{! param("name").with_default("fallback") !}
{! param("name").with_default("fallback").validate(required=True, regex=r"^\\w+$").get() !}
```

### Resolution order

CLI value (``--param name=value``) > default (``with_default()``) > empty
string ``""`` (if not required) or error (if required and missing).

### Fluent API

| Method | Returns | Behavior |
|--------|---------|----------|
| ``param("name")`` | ``ParamHandle`` | Create handle, look up CLI value |
| ``.with_default(value)`` | ``ParamHandle`` | Set fallback value, chainable |
| ``.validate(*, required, regex)`` | ``ParamHandle`` | Validate immediately, raise on failure |
| ``.get()`` | ``str`` | Return resolved value |
| ``str()`` | ``str`` | Same as ``.get()`` -- bare ``{! param("x") !}`` works |

### CLI usage

```bash
agent-notebook run notebook.md --param table_name=users --param limit=100
```

### Edge cases

| Input | Behavior |
|-------|----------|
| ``--param key=value`` | Normal |
| ``--param key=`` | Value is ``""`` (explicitly provided empty string) |
| ``--param key=a=b`` | Value is ``a=b`` (split on first ``=`` only) |
| Duplicate ``--param key=X --param key=Y`` | Last wins (Y) |

### Examples

Simple substitution:

```python
table = "{! param('table_name') !}"
```

With a default value:

```python
table = "{! param('table_name').with_default('default_table') !}"
```

With validation:

```python
table = "{! param('table_name').validate(required=True, regex=r'^\\w+$').get() !}"
```
"""


class ParamHandle:
    """Fluent builder for resolving a single preprocessing parameter.

    Instances are created by ``ParamPlugin.__call__`` and returned into the
    Jinja2 template context.  ``__str__`` delegates to ``get()`` so that a
    bare ``{! param("x") !}`` directive resolves without an explicit
    ``.get()`` call.
    """

    def __init__(self, name: str, value: str | None, plugin: ParamPlugin) -> None:
        self._name = name
        self._value = value
        self._plugin = plugin
        self._default: str | None = None
        self._required = False

    def with_default(self, value: str) -> ParamHandle:
        """Set a fallback value used when no CLI value is provided."""
        self._default = value
        return self

    def validate(
        self, *, required: bool = False, regex: str | None = None,
    ) -> ParamHandle:
        """Validate the resolved value immediately.

        Raises ``PreprocessorError`` if *required* is true and no value is
        available, or if *regex* is provided and the non-empty resolved value
        does not match.
        """
        self._required = required
        resolved = self._resolve()
        self._check_required()

        if regex is not None and resolved != "":
            if not re.fullmatch(regex, resolved):
                raise PreprocessorError(
                    self._plugin,
                    f"Parameter {self._name!r} value {resolved!r} "
                    f"does not match pattern {regex!r}",
                    detail={"param": self._name, "value": resolved, "regex": regex},
                )

        return self

    def get(self) -> str:
        """Return the resolved value.

        Resolution order: CLI value > default > empty string (or raise if
        the parameter was marked required and no value is available).
        """
        self._check_required()
        return self._resolve()

    def _check_required(self) -> None:
        """Raise if the parameter is required but has no value."""
        if self._required and self._value is None and self._default is None:
            raise PreprocessorError(
                self._plugin,
                f"Required parameter {self._name!r} has no value and no default",
                detail={"param": self._name},
            )

    def _resolve(self) -> str:
        if self._value is not None:
            return self._value
        if self._default is not None:
            return self._default
        return ""

    def __repr__(self) -> str:
        return f"ParamHandle({self._name!r}, value={self._value!r}, default={self._default!r})"

    def __str__(self) -> str:
        return self.get()


class ParamPlugin(PreprocessorPlugin):
    """Resolve CLI-supplied parameters into notebook content."""

    _METADATA = PluginMetadata(
        name="param",
        description="CLI-driven notebook parameterization",
        markdown_docs=_DOCS,
    )

    @classmethod
    def plugin_metadata(cls) -> PluginMetadata:
        return cls._METADATA

    def __init__(self, context: PluginContext) -> None:
        self._params = context.params

    def __call__(self, name: str) -> ParamHandle:
        """Create a handle for the named parameter.

        The handle looks up *name* in the CLI-supplied params dict and
        provides a fluent API for defaults and validation.
        """
        value = self._params.get(name)
        return ParamHandle(name, value, self)
