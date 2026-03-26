# Scala Development For Agents

**Note:** Both Scala 2.12 (DBR ≤ 16.x) and Scala 2.13 (DBR 17+) are supported. Kernel selection is automatic based on the cluster's DBR version. Serverless Scala defaults to the 2.13 kernel (current LTS).

## When to use this guide

Read this file when any of the following are true:

- your notebook is written in Scala
- you need to bring repo-managed Scala code into notebook cells
- you expect to hand Scala notebook work to another agent

If you only need the mechanical package cell rewrite procedure, jump to
`package_cell_instructions.md`.

## Scala REPL implications

Both Scala and Python cell execution happens in a REPL environment in Databricks on the driver node. 

Since Scala is a compiled language, the Scala REPL relies on just-in-time compilation of Scala snippets. This creates two types of problems with Scala types:

- variable fully-qualified names
- cluster-level JVM side effects

### Variable fully-qualified names

Since Scala is a JVM language, each snippet has to live in a package. To manage closure visibility correctly, the Scala REPL wraps each new snippet to execute in an anonymous nested package. Therefore, the fully-qualified name of `case class X(x: Int)` in a notebook cell is unknown and a redefinition of `X` in another cell would have a different fully qualified name, occluding the previously defined `X` via closure visibility rules.

The implication of this is error messages whose summaries, along the lines of "X was expected but I got X" sometimes don't make sense until one realizes that if `X` was defined in a notebook cell its fully qualified name is not `X`. If `X` is perhaps defined twice in a notebook or, more accurately, defined twice *in the same execution context* (Scala REPL session on the Spark driver node) You may get this type of error when code that saw the first definition of `X` receives an instance of the second definition of `X`. 

### Cluster-level JVM side effects

Databricks supports Scala [package cells](https://docs.databricks.com/en/notebooks/package-cells). These are special notebook cells that compile at the JVM level. This means they're available across all Scala sessions on the same cluster. 

```scala
package com.databricks.example

case class TestKey(id: Long, str: String)
```

**IMPORTANT:** The *initial version* of the types defined in package cells will become "frozen" in the JVM until the cluster is restarted.

This can be useful if you want to take repo-managed Scala files and use them on Databricks without packaging them into a JAR, reconfiguring and restarting a cluster. Simply put the files in package cells and run the notebook. However, the same behavior can become a problem if the code in package cells needs to change: you have to restart the cluster on every change. This can slow down work substantially. 

## Notebooks with repo-managed Scala code in active development

If you want to easily use repo-managed Scala code under active development in notebooks without having to restart a cluster with every change, there is pattern you can use that leverages variable package names.

This pattern is valuable when:

- cluster restart costs are too high for fast edit/test loops
- JAR publishing is too slow for exploratory or debugging work
- multiple code changes from repo-managed Scala need to be tested quickly on remote compute

This pattern avoids the bad tradeoff of maintaining a second handwritten notebook-only Scala codebase while leveraging the standardization of repo-managed Scala development, including test suite access.

The core idea is:

- keep the source of truth in repo-managed Scala code
- generate notebook package cells mechanically from that code
- give each generated package cell a run-unique UUID-without-dashes package suffix to avoid the "initial version becomes frozen" in the JVM issue
- rewrite repo-local imports and fully qualified repo-local references that point
  into the actively changing implementation package so they also use the
  run-unique suffix
- expose a context-local stable interface value so downstream notebook cells do not need to know the run-unique package name

### Ideal recommended shape

Use four logical cell types:

1. Stable interface cell
2. Run-unique package cell generated from library code
3. Context-local binding cell
4. Downstream execution cells

#### 1. Stable interface cell

Assume you have an interface (trait) in a repo, e.g., `Api.scala`:

```scala
package api.stable

trait Api {
  def compute(value: Double): Double
}
```

Use that Scala file without changes as the first notebook cell. 

**Note:** changing this requires a cluster restart. The code should be stable. 

In some cases, the interface trait might already be available in a JAR that is attached to the cluster. In that case, you don't need to add the first cell type, and you can just use the interface directly in the following cell. 

#### 2. Run-unique package cell

Assume you have an implementation of the trait as `Api.scala`:

```scala
package api.implementation

object Api extends api.stable.Api {
  // We want to changing this implementation many times without restarting a cluster
  def compute(value: Double): Double = 2 * value
}
```

Create an implementation notebook cell by adding a unique sub-package name *per notebook run*.


```scala
package api.implementation.pkg_8f14e45fceea467a9d2f8d3c1b0a4d55

object Api extends api.stable.Api {
  def compute(value: Double): Double = 2 * value
}
```

#### 3. Context-local binding cell

Bind the run-specific implementation to a context local value for easy access through the rest of the notebook.

```scala
// Deliberately use a notebook-context-local val here.
// Do not assign through a global mutable singleton, because that would
// leak state across parallel sessions sharing the same cluster JVM.
val api = api.implementation.pkg_8f14e45fceea467a9d2f8d3c1b0a4d55.Api
```

#### 4. Downstream execution cell(s)

From this point on, notebook cells can use the run-specific implementation without changes:

```scala
val result = api.compute(10)
println(s"RESULT=$result")
```

### If there is no interface trait

There is no interface trait, work directly with companion objects using the same pattern.

First, a package cell with a run-unique package name:

```scala
package example.pkg_8f14e45fceea467a9d2f8d3c1b0a4d55

object Example {
  def compute(value: Double): Double = 2 * value
}
```

Then, a binding cell:

```scala
val example = example.pkg_8f14e45fceea467a9d2f8d3c1b0a4d55.Example
```

### What This Buys Us

- core Scala code is managed in a normal repo/project with packages, tests, CI, etc.
- stable portions of Scala code, if needed, could be added as a JAR or package cells to the cluster across restarts
- small portions under active development can be used on Databricks without cluster restarts using a mechanical rewrite procedure of two notebook cells
- downstream notebook cells use a fixed local handle name
- the implementation package can change on every run
- the run-unique package avoids restart-sensitive package collisions
- the active handle remains scoped to the current notebook context instead of mutating cluster-global JVM state

### Transformation Procedure

This is a repeatable pattern.

If the actively changing implementation spans multiple repo-managed Scala files,
rewrite repo-local imports and fully qualified repo-local references so they
point at the same run-unique implementation package.

For example:

```scala
import api.implementation.Api
```

becomes:

```scala
import api.implementation.pkg_8f14e45fceea467a9d2f8d3c1b0a4d55.Api
```

Keep external imports unchanged.

To make it easier to follow the full procedure, you may direct agent sessions
to the instructions in `package_cell_instructions.md`.

## What this repo provides

The packaged examples under `examples/scala/` show:

- small repo-style `.scala` source inputs
- a notebook-markdown scaffold with rewritten package cells
- a separate binding and execution cell

These examples are teaching artifacts, not a product support promise. They are
meant to save an agent from reconstructing the pattern from scratch.
