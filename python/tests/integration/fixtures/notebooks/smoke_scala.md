# Smoke Test — Scala

Minimal Scala notebook for Databricks serverless integration test.

```scala
val result = spark.range(10).count()
println(s"count=$result")
```
