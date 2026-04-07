# Pipeline Primitives

Reusable building blocks for single-notebook data pipelines with selective
execution and selective refresh.

Two layers:

1. **Cached computed value** -- get a value from cache or build it
2. **Step orchestration** -- arrange named computations with configurable
   execution (on/off) and cache busting (recompute)

Import everything:

```python
from databricks_agent_notebooks.pipeline import *
```

## Cached computed value

The concept: getting a value from cache (cheap) or by computing it
(expensive). Two operations:

- **read** -- get it from cache. For Spark tables, `spark.table(name)` is
  atomic read + existence check in one call.
- **compute** -- build it. Returns a DataFrame or DataFrameWriter. Expensive.

### Why a wrapper, not an if/else

The wrapper controls invocation of `read` and `compute`. This shape supports
future lifecycle hooks (timing, validation, stats) without changing call
sites. The wrapper decides when to call which operation.

### `read_or_compute_table`

The Spark implementation of the cached computed value concept.

```python
def read_or_compute_table(
    read: str | Callable,        # table name or callable returning table name
    compute: Callable[..., Any], # returns DataFrame or DataFrameWriter
    *,
    id: str | None = None,       # value name for recompute matching
    refresh: bool = False,       # bust cache: skip read, go straight to compute
    context: Context | None = None,  # override auto-resolved context
) -> DataFrame:
```

Behavior:

- **`read` as string:** Used as the table name directly. The caller controls
  naming -- the framework does not construct or prefix table names.
- **`read` as callable:** Inspected for arity. 0-arg: called directly.
  1-arg: receives context. Useful for dynamic table name resolution.
- **`id`:** Name for recompute matching. Defaults to the resolved table name
  when `read` is a string.
- **`refresh`:** If True, skip read and compute directly. Also auto-triggered
  if `id` appears in context's recompute set.
- **`compute` result:** DataFrame is written via `.write.saveAsTable()`.
  DataFrameWriter is written via `.saveAsTable()` directly. Table is dropped
  first for idempotency, then read back to return a clean DataFrame.

Usage examples:

```python
# Explicit table name
df = read_or_compute_table(
    read="default.demo_ingest",
    compute=lambda: spark.range(1000).toDF("id"))

# Callable read, context-aware (user-controlled naming)
df = read_or_compute_table(
    read=lambda c: f"{c['catalog']}.{c['schema']}.ingest",
    compute=lambda: spark.range(1000).toDF("id"))

# User-defined prefix convention (not framework behavior)
T = "default.demo"
df = read_or_compute_table(
    read=f"{T}_ingest",
    compute=lambda: spark.range(1000).toDF("id"))
```

## Context

A mutable dict-like bag that any pipeline layer can populate. Auto-resolved
via `contextvars` -- single-threaded notebook execution is a perfect fit.

- `read_or_compute_table` picks up context automatically
- `StepRunner` is wired in via `ctx.set_runner()`
- No explicit passing needed unless overriding

### Parameter lifecycle

One reserved param name: `context`. Its JSON object value becomes the
starting context. Every other `--param` overwrites into context.

```python
ctx = Context(
    param("context").get(),           # starting context (JSON object or empty)
    defaults={                        # per-key defaults
        "table_prefix": "default.demo",
        "steps": {},
        "recompute": [],
    })
ctx.overlay_params({                  # individual --param values overlay
    "steps": param("steps").get(),
    "recompute": param("recompute").get(),
})
```

CLI examples:

```bash
# Individual params
agent-notebook run pipeline.md --cluster "local[*]" \
  --param 'steps=["ingest","clean"]' \
  --param 'recompute=["clean"]'

# Everything in a context JSON
agent-notebook run pipeline.md --cluster "local[*]" \
  --param 'context={"table_prefix":"default.demo","steps":["ingest"]}'

# Context defaults + individual override
agent-notebook run pipeline.md --cluster "local[*]" \
  --param 'context={"table_prefix":"default.demo"}' \
  --param 'steps=["ingest"]'
```

## Step orchestration

### StepRunner

Arranges named computations with configurable execution and cache busting.
Wired into context via `ctx.set_runner()`. The notebook author controls
step ordering by cell placement.

```python
ctx.set_runner(StepRunner(steps=["ingest", "clean", "enrich", "publish"]))
```

### Flexible input surface

Steps and recompute accept multiple input shapes:

| Input shape | Example | Interpretation |
|-------------|---------|----------------|
| Comma-delimited | `"ingest,clean"` | These steps on (others use default) |
| JSON array | `'["ingest","clean"]'` | Same |
| JSON object | `'{"ingest":true,"clean":false}'` | Explicit on/off |
| `"ALL"` | `"ALL"` | All known steps |
| Empty string | `""` | All defaults |

### Data-flow safety

When a step is disabled:

- If the table exists: serves from cache (downstream steps still get data)
- If the table does not exist: raises an error (prevents silent failures)

This ensures disabled steps never break the data flow.

### `ctx.run()`

The primary step execution method:

```python
df_ingest = ctx.run("ingest", table="default.demo_ingest",
                     compute=compute_ingest)
```

Parameters:
- **`step`:** Step name for orchestration (should_run / should_recompute)
- **`table`:** Fully qualified table name. The caller controls naming.
- **`compute`:** Callable returning a DataFrame or DataFrameWriter.

What it does:
1. Checks `ctx.should_run("ingest")` -- handles defaults, overrides, "ALL"
2. If enabled: `read_or_compute_table(read=table, compute=..., id=step, refresh=ctx.should_recompute(step))`
3. If disabled + table exists: reads from cache
4. If disabled + no table: raises

### Workflow pattern (skip entirely)

For steps that produce side effects beyond a table (e.g., file uploads),
use `ctx.should_run()` directly:

```python
if ctx.should_run("export"):
    upload_results(df_final)
```

## Recompute

Named values can be selectively recomputed via context:

```python
ctx["recompute"] = ["clean"]  # or via --param 'recompute=["clean"]'
```

Both `ctx.run()` and standalone `read_or_compute_table()` check the
recompute set automatically. The `id` parameter controls which name is
matched:

```python
# id defaults to the resolved table name for string read
read_or_compute_table(read="ingest", compute=..., id="my_custom_id")
```

## Full notebook wiring

See `examples/pipeline/pipeline_demo.md` for a working 4-step demo.

The construction sequence (shown as post-preprocessing Python -- in the
actual notebook, wrap `param(...)` calls in `{! ... !}` directives inside
triple-quoted strings to avoid quote collision with JSON values, e.g.,
`"""{! param('context').with_default('{}').get() !}"""`):

```python
from databricks_agent_notebooks.pipeline import *

# 1. Create context from params (post-preprocessing view)
ctx = Context(
    param("context").get(),
    defaults={"table_prefix": "default.pipeline_demo"})
ctx.overlay_params({
    "steps": param("steps").get(),
    "recompute": param("recompute").get(),
})

# 2. Wire in the runner
ctx.set_runner(StepRunner(steps=["ingest", "clean", "enrich", "publish"]))
set_context(ctx)

# 3. Execute steps -- table names are explicit, user-controlled
T = ctx["table_prefix"]  # e.g., "default.pipeline_demo"
df_ingest = ctx.run("ingest", table=f"{T}_ingest", compute=compute_ingest)
df_clean = ctx.run("clean", table=f"{T}_clean",
                    compute=lambda: transform(df_ingest))
```

## The shape generalizes

`read_or_compute_table` is the Spark implementation of a general pattern.
The same concept applies to other sources:

- `csv_from_url(read=local_path, compute=lambda: download(url))` -- cache a
  downloaded CSV locally
- `parquet_from_s3(read=local_path, compute=lambda: fetch(s3_uri))` -- cache
  S3 data

These are not implemented now, but the wrapper shape (read vs compute,
controlled invocation, future hooks) holds across implementations.

## Compute functions

For non-trivial transforms, use named functions instead of lambdas. They can
be unit tested independently and included from source files:

```python
# In transforms.py
def compute_enrich(df_clean):
    return (
        df_clean
        .withColumn("doubled", f.col("id") * 2)
        .withColumn("label", f.when(f.col("id") < 250, "low").otherwise("high"))
    )

# In the notebook
df_enrich = ctx.run("enrich", table=f"{T}_enrich",
                     compute=lambda: compute_enrich(df_clean))
```

## Cell design for step-gated notebooks

Jupyter executes every cell top-to-bottom unconditionally. `ctx.run()` gates
the compute function inside the cell -- it does not skip the cell itself.
Any code at cell scope runs regardless of which steps are selected.

**Rule:** A cell containing `ctx.run("X", ...)` must have no cell-scope code
that fails when step X is off. Everything step-specific goes inside the
compute function.

Correct -- import inside the compute function:

```python
def _compute_humanize():
    from my_project.transforms import HumanizeMapper
    return HumanizeMapper(df_source).run()

df = ctx.run("humanize", table=f"{T}_humanize", compute=_compute_humanize)
```

Incorrect -- import at cell scope crashes before `ctx.run()` can gate:

```python
from my_project.transforms import HumanizeMapper  # runs always

def _compute_humanize():
    return HumanizeMapper(df_source).run()

df = ctx.run("humanize", table=f"{T}_humanize", compute=_compute_humanize)
```

**Safe at cell scope:** `def` statements, the `ctx.run()` call, references to
objects always available (spark, context, table name constants).

**Must go inside the compute function:** imports that may not be installed,
setup that only makes sense when the step runs (temp files, network calls,
cluster config), code depending on outputs of steps that may not have run.

`--allow-errors` is not a workaround. It suppresses all cell errors
indiscriminately -- it cannot distinguish skipped-step failures from real
errors. This applies to `read_or_compute_table`, `ctx.should_run()` guards,
and any other gating pattern.
