---
databricks:
  language: python
---

# Python Select One

Use this as a minimal non-mutating notebook run.

Supply the Databricks execution context yourself, either through frontmatter
you add or through explicit CLI flags.

```python
display(spark.sql("select 1"))
```
