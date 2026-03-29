# Multi-Cell Test — Python

Tests sequential cell execution.

```python
x = 10
print(f"x={x}")
```

```python
y = x * 2
print(f"y={y}")
```

```python
result = spark.range(y).count()
print(f"count={result}")
```
