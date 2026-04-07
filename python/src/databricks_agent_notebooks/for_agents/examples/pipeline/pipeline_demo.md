---
agent-notebook:
  language: python
---

# Pipeline Demo

A 4-step pipeline demonstrating `read_or_compute_table`, `StepRunner`, and
`Context` with selective execution and recompute.

CLI usage (run all / selective / recompute):

    agent-notebook run pipeline_demo.md --cluster "local[*]"
    agent-notebook run pipeline_demo.md --cluster "local[*]" --param 'steps=["ingest","clean"]'
    agent-notebook run pipeline_demo.md --cluster "local[*]" --param 'recompute=["clean"]'

## Setup

```python
import logging
from databricks_agent_notebooks.pipeline import (
    Context, StepRunner, set_context,
)
import pyspark.sql.functions as f

logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
```

## Pipeline Configuration

```python
ctx = Context(
    """{! param('context').with_default('{}').get() !}""",
    defaults={
        "table_prefix": "default.pipeline_demo",
        "steps": {},
        "recompute": [],
    })
ctx.overlay_params({
    "steps": """{! param('steps').with_default('').get() !}""",
    "recompute": """{! param('recompute').with_default('').get() !}""",
})
ctx["spark"] = spark
ctx.set_runner(StepRunner(steps=["ingest", "clean", "enrich", "publish"]))
set_context(ctx)
ctx.print_config()
```

## Table Naming

Define your own naming convention. The framework does not construct table
names -- the caller controls naming.

```python
T = ctx["table_prefix"]  # "default.pipeline_demo" from defaults
```

## Step 1: Ingest

```python
df_ingest = ctx.run("ingest", table=f"{T}_ingest",
                     compute=lambda: spark.range(1000).toDF("id"))
print(f"ingest: {df_ingest.count()} rows")
```

## Step 2: Clean

```python
def compute_clean():
    return (
        df_ingest
        .filter("id % 2 = 0")
        .withColumn("source", f.lit("demo"))
    )

df_clean = ctx.run("clean", table=f"{T}_clean", compute=compute_clean)
print(f"clean: {df_clean.count()} rows")
```

## Step 3: Enrich

```python
def compute_enrich():
    return (
        df_clean
        .withColumn("doubled", f.col("id") * 2)
        .withColumn("label", f.when(f.col("id") < 250, "low").otherwise("high"))
    )

df_enrich = ctx.run("enrich", table=f"{T}_enrich", compute=compute_enrich)
print(f"enrich: {df_enrich.count()} rows")
```

## Step 4: Publish

```python
def compute_publish():
    return df_enrich.select("id", "doubled", "label")

df_publish = ctx.run("publish", table=f"{T}_publish", compute=compute_publish)
print(f"publish: {df_publish.count()} rows")
```

## Summary

```python
for step in ["ingest", "clean", "enrich", "publish"]:
    table = f"{T}_{step}"
    try:
        count = spark.table(table).count()
        print(f"  {step}: {count} rows")
    except Exception:
        print(f"  {step}: not materialized")
```
