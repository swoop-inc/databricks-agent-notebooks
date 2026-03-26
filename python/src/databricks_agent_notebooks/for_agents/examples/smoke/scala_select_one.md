---
databricks:
  language: scala
---

# Scala Select One

Use this as a minimal non-mutating notebook run.

Supply the Databricks execution context yourself, either through frontmatter
you add or through explicit CLI flags.

```scala
spark.sql("select 1").show()
```
