# Notebook Utilities

Reusable utilities for dual-environment (local Spark + Databricks) notebook
development. Three functions:

- **`is_databricks(spark=None)`** -- detect whether the active Spark session
  is backed by Databricks
- **`resolve_repo_root()`** -- find the repository root directory when
  `__file__` is unavailable (because `include()` inlined the code)
- **`set_query_execution_timeout(seconds=9000, spark=None)`** -- set the
  per-query execution timeout on Databricks (no-op locally)

## Why these exist

Agents use a local-first workflow: test locally (fast, free), then run on
Databricks (expensive). Notebooks need to branch behavior between environments
-- local metastore vs Unity Catalog, local parquet vs Delta tables, local
scratch paths vs volume paths.

`resolve_repo_root()` exists because `__file__` may be unavailable in some
notebook execution contexts (e.g. after `include()` inlining of repo-local
code). It provides an explicit filesystem anchor via `REPO_ROOT` env var
(primary) or `.git` directory walk (fallback).

## Usage

### Import (works in all execution modes)

Direct import works in LOCAL_SPARK, serverless, and cluster modes because
`agent-notebook` always executes notebooks in a Python environment where the
package is installed:

```python
from databricks_agent_notebooks.notebook_utils import *
```

No `include()` or `_includes/` symlinks are needed. Use `include()` for
**repo-local code** (your project's transforms, domain logic), not for
utilities that ship with `agent-notebook`.

## Environment detection

```python
if is_databricks():
    # Databricks: use Unity Catalog tables
    df.write.saveAsTable("catalog.schema.table")
else:
    # Local: use parquet files
    df.write.parquet(f"{resolve_repo_root()}/tmp/scratch/table")
```

When called without arguments, `is_databricks()` resolves the Spark session in
order:

1. Caller's `spark` global (frame inspection -- works whether the module was
   imported or inlined via `include()`)
2. `SparkSession.getActiveSession()` as fallback
3. If no session is found, returns `False`

Detection checks whether any Spark configuration key contains `.databricks.`
(dot-bounded). This is reliable across serverless, cluster-attached, and
Databricks Connect sessions.

You can also pass a session explicitly: `is_databricks(spark=my_session)`.

## Execution timeout

Long-running Databricks queries (large table scans, heavy joins, full-graph
traversals) may exceed the default per-query timeout of 9000 s (2.5 h).
`set_query_execution_timeout` wraps `spark.databricks.execution.timeout` and
is a no-op outside Databricks, so it's safe to call unconditionally:

```python
from databricks_agent_notebooks.notebook_utils import set_query_execution_timeout

set_query_execution_timeout(86400)  # 24 hours for large transforms
```

The default (9000) matches the Databricks platform default. Pass a higher
value only when you know the workload needs it.

## Repo root resolution

```python
root = resolve_repo_root()
fixture_path = f"{root}/python/tests/fixtures/sample.csv"
scratch_dir = f"{root}/tmp/scratch"
```

Resolution order:

1. `REPO_ROOT` environment variable -- set by run scripts
2. Walk up from cwd until a `.git` entry is found
3. Fall back to cwd

### Interaction with run scripts

If your project uses a run script wrapper for `agent-notebook run`, export
`REPO_ROOT` in that script so notebooks always have a reliable anchor:

```bash
export REPO_ROOT="$(git rev-parse --show-toplevel)"
agent-notebook run notebook.md --cluster "local[*]"
```

On Databricks, the repo root is typically irrelevant (Delta tables replace local
paths), but the function still returns a value so callers that unconditionally
reference a repo root do not need environment-specific construction logic.
