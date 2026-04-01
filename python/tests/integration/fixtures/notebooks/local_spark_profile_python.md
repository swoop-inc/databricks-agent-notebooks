# Local Spark Profile Test — Python

Relies on the injected LOCAL_SPARK session cell — does NOT create its own SparkSession.

```python
result = spark.range(10).count()
print(f"count={result}")
spark.stop()
```
