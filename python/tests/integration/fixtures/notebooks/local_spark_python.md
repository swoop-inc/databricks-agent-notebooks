# Local Spark Test — Python

Self-contained notebook that creates its own local SparkSession.
No Databricks imports or connectivity required.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("integration-test").getOrCreate()
result = spark.range(10).count()
print(f"count={result}")
spark.stop()
```
