"""Documentation-only plugin -- cross-plugin patterns and best practices.

This plugin has no ``__call__`` method and no runtime behavior.  Its sole
purpose is to surface composability patterns in the auto-generated
``plugins.md`` documentation that ships with the package.
"""

from __future__ import annotations

from databricks_agent_notebooks.preprocessing.plugins import (
    PluginContext,
    PluginMetadata,
    PreprocessorPlugin,
)

_DOCS = """\
### Built-in filters and globals

The preprocessing environment includes filters and globals from the Python
standard library.  These are available in all directives alongside the
`include` and `param` plugins.

#### Filters

| Filter | Wraps | Behavior |
|--------|-------|----------|
| `fromjson` | [`json.loads`](https://docs.python.org/3/library/json.html#json.loads) | Parse a JSON string into a Python object |
| `split` | [`str.split`](https://docs.python.org/3/library/stdtypes.html#str.split) | Split string by separator (default: whitespace) |
| `basename` | [`os.path.basename`](https://docs.python.org/3/library/os.path.html#os.path.basename) | Extract filename from a path |
| `dirname` | [`os.path.dirname`](https://docs.python.org/3/library/os.path.html#os.path.dirname) | Extract directory from a path |
| `regex_search` | [`re.search`](https://docs.python.org/3/library/re.html#re.search) | First capture group if any, else full match; no match returns `""` |
| `regex_replace` | [`re.sub`](https://docs.python.org/3/library/re.html#re.sub) | Replace all occurrences of a pattern |

#### Globals

| Global | Wraps | Behavior |
|--------|-------|----------|
| `env` | [`os.environ.get`](https://docs.python.org/3/library/os.html#os.environ) | `env("KEY")` returns value or `""`;  `env("KEY", "default")` returns value or `"default"` |
| `now` | [`datetime.datetime.now`](https://docs.python.org/3/library/datetime.html#datetime.datetime.now) | Returns a `datetime` object -- use `.strftime()` for formatting |

#### Block syntax

Use `{!% %!}` delimiters for control flow (loops, conditionals):

```
{!% for item in param("list").get() | fromjson %!}
process("{! item !}")
{!% endfor %!}
```

### Dynamic includes

The `include` and `param` plugins compose naturally.  `param` resolves a
CLI-supplied value; `include` reads the file at that path.  Together they
enable notebooks that adapt to different environments without code changes.

#### Syntax

```
{! include(param("snippet_path").get()) !}
```

#### CLI usage

```bash
agent-notebook run notebook.md --param snippet_path=path/to/file.py
```

Different runs can inject different files into the same notebook cell:

```bash
# dev environment
agent-notebook run notebook.md --param snippet_path=snippets/dev_setup.py

# production environment
agent-notebook run notebook.md --param snippet_path=snippets/prod_setup.py
```

#### How it works

1. `param("snippet_path")` returns a handle bound to the CLI value.
2. `.get()` resolves to the string value (e.g., `"snippets/dev_setup.py"`).
3. `include(...)` reads the file at that path relative to the notebook
   directory and inserts its content verbatim.

#### Optional default

Use `with_default` so the notebook still works without `--param`:

```
{! include(param("snippet_path").with_default("snippets/default.py").get()) !}
```

### Multi-file dynamic includes

Pass a JSON array of file paths via `--param` and loop over them with the
`fromjson` filter and `{!% for %!}` block syntax:

#### Syntax

```
{!% for f in param("sources").get() | fromjson %!}
{! include(f) !}
{!% endfor %!}
```

#### CLI usage

```bash
agent-notebook run notebook.md \\
  --param 'sources=["lib/utils.py","lib/transforms.py","lib/io.py"]'
```

#### How it works

1. `param("sources").get()` resolves to the raw JSON string.
2. `| fromjson` parses it into a Python list.
3. `{!% for f in ... %!}` iterates over each path.
4. `include(f)` reads each file and inserts its content verbatim.

#### With path display

Combine with the `basename` filter to label each included file:

```
{!% for f in param("sources").get() | fromjson %!}
# --- {! f | basename !} ---
{! include(f) !}
{!% endfor %!}
```

### Pipeline patterns

The `pipeline` module provides reusable building blocks for single-notebook
data pipelines with selective execution and recompute.  Import everything:

```python
from databricks_agent_notebooks.pipeline import *
```

#### Parameterized pipeline context

Use `param` to feed pipeline configuration from CLI arguments.  The
`context` param provides a JSON object of defaults; individual params
override specific keys:

```python
ctx = Context(
    \"\"\"{! param('context').with_default('{}').get() !}\"\"\",
    defaults={"table_prefix": "default.demo"})
ctx.overlay_params({
    "steps": \"\"\"{! param('steps').with_default('').get() !}\"\"\",
    "recompute": \"\"\"{! param('recompute').with_default('').get() !}\"\"\",
})
```

```bash
agent-notebook run pipeline.md --cluster "local[*]" \\
  --param 'steps=["ingest","clean"]' \\
  --param 'recompute=["clean"]'
```

#### Key components

- `read_or_compute_table` -- cached computed value for Spark tables (read
  from cache or compute and save)
- `StepRunner` -- step orchestration with configurable on/off and recompute
- `Context` -- mutable dict-like bag with auto-resolution via `contextvars`

See `advanced_pipeline_concepts.md` for the full conceptual guide and
`examples/pipeline/pipeline_demo.md` for a working 4-step demo.
"""


class BestPracticesPlugin(PreprocessorPlugin):
    """Documentation-only plugin for cross-plugin patterns."""

    _METADATA = PluginMetadata(
        name="best_practices",
        description="Cross-plugin patterns and best practices (documentation only)",
        markdown_docs=_DOCS,
    )

    @classmethod
    def plugin_metadata(cls) -> PluginMetadata:
        return cls._METADATA

    def __init__(self, context: PluginContext) -> None:
        pass
