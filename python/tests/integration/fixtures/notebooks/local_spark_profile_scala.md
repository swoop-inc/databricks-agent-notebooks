# Local Spark Profile Test — Scala

Relies on the injected LOCAL_SPARK session cell — does NOT create its own SparkSession.

```scala
val result = spark.range(10).count()
println(s"count=$result")
spark.stop()
```
