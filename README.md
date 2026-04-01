# databricks-agent-notebooks

`databricks-agent-notebooks` is a Python package and CLI for working with Databricks-like notebooks from a local development environment. It exists to make automated notebook execution possible outside a workspace UI and IDE extensions.

The CLI is specifically optimized for use by coding agents such as Claude Code and Codex:

- Command output is designed for agent use
- Long-running Databricks operation monitoring is optimized to protect agent session context and minimize token use
- Markdown is a first-class format for notebook creation and capturing execution outputs
- Complex operations such as kernel and [Databrick Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect) version management are fully automated based on the Databricks workspace/cluster configuration
- `agent-notebooks help` includes a [README](python/src/databricks_agent_notebooks/for_agents/README.md) for agents and a [first-time setup flow](python/src/databricks_agent_notebooks/for_agents/agent_doctor.md)
- Agents benefit from Databricks-optimized [Scala development tips](python/src/databricks_agent_notebooks/for_agents/scala_development.md)


## Install From PyPI

```bash
uv tool install databricks-agent-notebooks
```

Or with pip:

```bash
pip install databricks-agent-notebooks
```

Then, give your agent the following prompt:

```
Run `agent-notebook help` and follow the agent README and agent doctor instructions
```

## Requirements

- Configured [Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth) profile in `~/.databrickscfg` or `DATABRICKS_CONFIG_FILE`
- For Scala notebooks, Java 11 or newer is required with `coursier` or `cs` on `PATH`

## Supported Notebook Formats

Executing a notebook on Databricks requires three things:

- **Language** — Python, Scala, or SQL
- **Profile** — a [Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth) profile identifying the workspace
- **Cluster** (optional) — a cluster name or ID; omit for serverless execution

**Markdown** is the recommended authoring format. A YAML frontmatter block can embed all three values so the notebook is self-contained:

````markdown
---
databricks:
  language: python
  profile: my-workspace
  cluster: my-cluster-name
---

# Exploratory analysis

```python
df = spark.sql("SELECT current_catalog(), current_schema()")
display(df)
```

```python
df.printSchema()
```
````

Scala notebooks work the same way. This one omits `cluster`, so it runs serverless:

````markdown
---
databricks:
  language: scala
  profile: my-workspace
---

# Scala smoke test

```scala
spark.sql("SELECT 1").show()
```
````

Local Spark notebooks skip Databricks entirely — useful for CI, testing, and development without credentials:

````markdown
---
databricks:
  language: python
  profile: LOCAL_SPARK
---

# Local test

```python
result = spark.range(10).count()
print(f"count={result}")
```
````

CLI flags (`--profile`, `--cluster`, `--language`) override frontmatter values. When frontmatter is not provided, these values must be passed explicitly on the `run` command.

Each markdown notebook must use a single language — you cannot mix Python and Scala cells the way you can in a Databricks workspace notebook, because execution runs through Databricks Connect rather than a workspace interpreter.

### Other supported notebook formats

- [Databricks Source Format](https://docs.databricks.com/aws/en/notebooks/notebook-format) (`.py`, `.scala`, `.sql`) — notebooks exported from a Databricks workspace with header and cell delimiters. Note that `.py` notebooks cannot refer to code in other `.py` notebooks and certain Databricks specific cell types such as `%run` are unsupported.
- Jupyter notebooks (`.ipynb`) — executed as-is with existing kernel metadata

These formats have no frontmatter mechanism, so `--profile` and optionally `--cluster` must always be provided as CLI flags.

## Useful Commands

Check the installation:

```bash
agent-notebook doctor
```

Execute a notebook's cells on Databricks to generate IPython, Markdown and HTML outputs:

```bash
# profile and, optionally, cluster specified in frontmatter 
agent-notebook run path/to/notebook.md

# with a specific profile and cluster
agent-notebook run path/to/notebook.md --profile <profile_name> --cluster <cluster-name-or-id>

# local execution — no Databricks credentials needed
agent-notebook run path/to/notebook.md --profile LOCAL_SPARK
```

Output files are written to `path/to/notebook_output/`: 

- `notebook.executed.ipynb` (executed notebook)
- `notebook.executed.md` (Markdown)
- `notebook.executed.html` (HTML)

Use `--output-dir` to change the parent directory, or `--format md` / `--format html` to emit only one rendered format. 

### Tips

- Do not add `--timeout` unless you know the cell-level upper bound you want to enforce and understand the effects of external factors such as cluster startup, shared resource availability, autoscaling, possible stage failure or spot instance loss, and the like.
- `--allow-errors` will continue execution on cell errors. This is useful when a notebook contains independent commands, e.g., a series of summary queries -- `display(spark.sql(...))`.
- Passing a cluster ID is the deterministic path for cluster-based execution
- A cluster name will be resolved to a cluster ID in a best-effort manner, within a 30 second timeout window. In case of no resolution, helpful fuzzy matches will be returned to help with typos.
- `--no-inject-session` bypasses managed Databricks Connect runtime selection and session injection for both cluster-backed and serverless runs when your notebook handles its own Spark session
- For Python serverless execution, the policy tries a conservative version first, validates it, falls back to older supported lines if needed, and caches the first workspace/profile success under runtime-home for reuse. `DATABRICKS_AGENT_NOTEBOOKS_SERVERLESS_CONNECT_LINE` is a Python-only escape hatch for forcing an explicit serverless Connect line in the unlikely event you need to override the cached/default policy.

### Local Execution

`--profile LOCAL_SPARK` runs notebooks against a local Spark session with no Databricks credentials needed.

- `--profile LOCAL_SPARK --cluster foo` is an error — the two are mutually exclusive
- `SPARK_HOME` is not needed — Scala uses `$ivy` imports (self-contained). Python requires pyspark installed in the active Python environment (`pip install pyspark` or `uv pip install pyspark`)

`--no-inject-session` is a separate concern: it skips session injection entirely so the notebook can manage its own Spark session. It can be combined with `--profile LOCAL_SPARK` if you want local execution without the injected session.

Environment variables for tuning (all optional):

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_NOTEBOOK_LOCAL_SPARK_MASTER` | `local[*]` | Spark master URL |
| `AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY` | _(Spark default)_ | Driver memory, e.g., `2g` |
| `AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY` | _(Spark default)_ | e.g., `2g` — Python only (see Scala restrictions below) |
| `AGENT_NOTEBOOK_LOCAL_SPARK_VERSION` | `3.5.4` | Scala only (Python uses pip-installed PySpark); Spark version for `$ivy` import |

> **Scala:** The CLI validates master URLs and memory configuration at startup.
> See [Scala local mode restrictions](python/src/databricks_agent_notebooks/for_agents/scala_development.md#local-mode-restrictions)
> for details.

## Deeper Documentation

- [Runtime-home layout](docs/runtime-home.md)
- [Release and publishing notes](docs/release.md)

## Contributing

External contributions are welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

This project was originally created by [Simeon Simeonov](https://github.com/ssimeonov) with support from [Swoop](https://github.com/swoop-inc) and is available under the [MIT License](LICENSE).
