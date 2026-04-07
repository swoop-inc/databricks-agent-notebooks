# databricks-agent-notebooks

`databricks-agent-notebooks` is a Python package and CLI for executing Databricks-like notebooks from a local development environment. It makes automated notebook execution possible outside a workspace UI and IDE extensions, and is specifically optimized for coding agents such as Claude Code and Codex.

## Install

```bash
# Remote execution (serverless and cluster via Databricks Connect)
uv tool install databricks-agent-notebooks

# Full functionality including LOCAL_SPARK (bundles standalone pyspark)
uv tool install "databricks-agent-notebooks[local-spark]"
```

Or with pip:

```bash
pip install databricks-agent-notebooks                      # remote-only
pip install "databricks-agent-notebooks[local-spark]"       # with LOCAL_SPARK
```

Then, give your agent:

```
Run `agent-notebook help` and follow the agent README and agent doctor instructions
```

## Requirements

- Configured [Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth) profile in `~/.databrickscfg` or `DATABRICKS_CONFIG_FILE`
- For Scala notebooks: Java 11+ with `coursier` or `cs` on `PATH`

## Configuration

Execution settings come from four levels, each overriding the previous:

1. **`pyproject.toml`** -- `[tool.agent-notebook]` in the nearest `pyproject.toml` (walks up to `.git` boundary)
2. **Environment variables** -- `AGENT_NOTEBOOK_*` env vars
3. **Notebook frontmatter** -- YAML block at the top of markdown notebooks under the `agent-notebook:` key (the legacy `databricks:` key is also supported)
4. **CLI flags** -- always win

All resolved config and user-defined params are available at runtime as `agent_notebook_parameters` -- a Python dict injected into every Python and SQL notebook before execution (Scala support planned). Use `hooks.python.prologue_cells` in config to inject setup code (catalog selection, helper imports) between session creation and user content.

See the [agent guide](python/src/databricks_agent_notebooks/for_agents/README.md#configuration) for the full precedence model, supported keys, and examples. See [runtime parameters](python/src/databricks_agent_notebooks/for_agents/README.md#runtime-parameters-agent_notebook_parameters) and [hooks](python/src/databricks_agent_notebooks/for_agents/README.md#hooks-prologue-cells) for the lifecycle extensibility features.

## Supported Notebook Formats

Executing a notebook requires a **language** and an execution target. The target is set via the `cluster` field: a Databricks cluster name/ID, `SERVERLESS`, or a local master URL like `local[*]`. An optional `profile` selects the Databricks auth profile. These values can be provided via `pyproject.toml`, notebook frontmatter, or CLI flags.

**Markdown** is the recommended authoring format. A YAML frontmatter block can embed all values so the notebook is self-contained. The frontmatter key is `agent-notebook:`:

````markdown
---
agent-notebook:
  language: python
  profile: my-workspace
  cluster: my-cluster-name
---

# Exploratory analysis

```python
df = spark.sql("SELECT current_catalog(), current_schema()")
display(df)
```
````

Scala notebooks work the same way (use `cluster: SERVERLESS` for explicit serverless, or omit for implicit serverless):

````markdown
---
agent-notebook:
  language: scala
  profile: my-workspace
  cluster: SERVERLESS
---

```scala
spark.sql("SELECT 1").show()
```
````

Scala notebooks have additional restrictions. See [Scala development](python/src/databricks_agent_notebooks/for_agents/scala_development.md) for details.

Local Spark notebooks skip Databricks entirely -- useful for CI, testing, and development without credentials:

````markdown
---
agent-notebook:
  language: python
  cluster: "local[*]"
---

```python
result = spark.range(10).count()
print(f"count={result}")
```
````

Each markdown notebook must use a single language -- you cannot mix Python and Scala cells.

### Other supported formats

- [Databricks Source Format](https://docs.databricks.com/aws/en/notebooks/notebook-format) (`.py`, `.scala`, `.sql`)
- Jupyter notebooks (`.ipynb`)

These formats have no frontmatter mechanism, so profile and cluster must come from `pyproject.toml` or CLI flags.

## Quick Usage

```bash
# Check installation
agent-notebook doctor

# Run a notebook (profile/cluster from frontmatter or pyproject.toml)
agent-notebook run path/to/notebook.md

# Override profile and cluster on the CLI
agent-notebook run path/to/notebook.md --profile my-workspace --cluster my-cluster-id

# Explicit serverless execution
agent-notebook run path/to/notebook.md --cluster SERVERLESS

# Local execution -- no Databricks credentials needed
agent-notebook run path/to/notebook.md --cluster "local[*]"
agent-notebook run path/to/notebook.md --cluster "local[4]"   # 4 threads
```

Output files are written to `path/to/notebook_output/`:

- `notebook.executed.ipynb` (executed notebook)
- `notebook.executed.md` (Markdown)
- `notebook.executed.html` (HTML)

Use `--output-dir` to change the parent directory, or `--format all` / `--format md` / `--format html` to control rendered output.

## CLI Quick Reference (`run`)

| Flag | Description |
|---|---|
| `--profile` | Databricks auth profile (see note on `LOCAL_SPARK` below) |
| `--cluster` | Execution target: cluster name/ID, `SERVERLESS`, or `local[N]` |
| `--language` | Override notebook language (python, scala) |
| `--format` | Output format: `all` (default), `md`, `html` |
| `--output-dir` | Output directory (default: input file's parent) |
| `--timeout` | Per-cell timeout in seconds (default: unset) |
| `--allow-errors` | Continue execution on cell errors |
| `--no-inject-session` | Skip Databricks Connect session injection |
| `--no-preprocess` | Skip preprocessing directive expansion |
| `--param NAME=VALUE` | Set a preprocessing parameter (repeatable) |
| `--library PATH` | Add a Python library path to sys.path (repeatable) |
| `--clean` | Remove and recreate the output directory before running |

## Tips

- `--allow-errors` is useful when a notebook contains independent commands, e.g., a series of summary queries.
- Passing a cluster ID is the deterministic path for cluster-based execution. Name resolution is best-effort with a configurable `cluster_list_timeout` (default: 120s).
- `--clean` removes and recreates the output directory -- useful for deterministic re-runs.

## Execution Targets

The `--cluster` flag is a unified execution target selector:

| Value | Execution mode |
|---|---|
| `SERVERLESS` | Explicit serverless (Databricks Connect) |
| `local[*]`, `local[4]`, etc. | Local Spark session (no Databricks) |
| `my-cluster` or cluster ID | Databricks cluster-backed execution |
| *(omitted)* | Serverless (implicit default) |

`SERVERLESS` is case-insensitive. Local master patterns are case-sensitive (lowercase only), following Spark conventions (`local`, `local[*]`, `local[N]`, `local[N,M]`).

### Local Execution

`--cluster "local[*]"` runs notebooks against a local Spark session with no Databricks credentials needed.

- Python requires pyspark, included with the `[local-spark]` extra. Scala uses `$ivy` imports (self-contained)
- `--no-inject-session` can be combined with local execution if you want to skip the injected session
- `--profile LOCAL_SPARK` still works for backward compatibility but is deprecated. Use `--cluster "local[*]"` instead.

See the [agent guide's local execution section](python/src/databricks_agent_notebooks/for_agents/README.md#local-execution-no-databricks) for environment variable tuning, Scala restrictions, and configuration behavior details.

## Agent Defaults

Built-in defaults that apply when no explicit configuration overrides them.
Override via environment definitions, `pyproject.toml`, env var, or frontmatter.

```yaml
# agent-notebook built-in defaults
cluster_list_timeout: 120  # seconds -- budget for cluster listing and name resolution
                           # env var: AGENT_NOTEBOOK_CLUSTER_LIST_TIMEOUT
```

## Deeper Documentation

- [Agent guide](python/src/databricks_agent_notebooks/for_agents/README.md) -- comprehensive reference for agent and automated use
- [Runtime parameters](python/src/databricks_agent_notebooks/for_agents/README.md#runtime-parameters-agent_notebook_parameters) -- accessing resolved config at runtime
- [Hooks (prologue cells)](python/src/databricks_agent_notebooks/for_agents/README.md#hooks-prologue-cells) -- injecting setup code before notebook content
- [First-time setup](python/src/databricks_agent_notebooks/for_agents/agent_doctor.md) -- readiness checks
- [Scala development](python/src/databricks_agent_notebooks/for_agents/scala_development.md) -- Scala-specific tips and restrictions
- [Runtime-home layout](docs/runtime-home.md)
- [Release and publishing notes](docs/release.md)

## Contributing

External contributions are welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

This project was originally created by [Simeon Simeonov](https://github.com/ssimeonov) with support from [Swoop](https://github.com/swoop-inc) and is available under the [MIT License](LICENSE).
