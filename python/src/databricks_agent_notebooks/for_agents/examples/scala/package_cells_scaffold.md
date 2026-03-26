---
databricks:
  language: scala
---

# Scala Package Cell Scaffold

This scaffold shows the ideal stable-interface plus run-unique-implementation shape at notebook scope.

```scala
// PACKAGE CELL 1
// Source: examples/scala/src/api/stable/Api.scala
package api.stable

trait Api {
  def compute(value: Double): Double
}
```

```scala
// PACKAGE CELL 2
// Source: examples/scala/src/api/implementation/Api.scala
package api.implementation.pkg_<uuid_without_dashes>

object Api extends api.stable.Api {
  def compute(value: Double): Double = 2 * value
}
```

```scala
// Context-local binding cell
val api = api.implementation.pkg_<uuid_without_dashes>.Api
```

```scala
// Downstream execution cell
val result = api.compute(10)
println(s"RESULT=$result")
```
