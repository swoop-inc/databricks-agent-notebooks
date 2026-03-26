# Package Cell Instructions

Use this file when an agent needs a mechanical procedure for turning repo-style
Scala source files into notebook cells that follow the recommended shape in
`scala_development.md`.

It is deliberately narrow so you can hand it to a sub-agent as an execution
contract.

## Goal

Use evolving repo-managed Scala code in notebook development without forcing a cluster restart after every code change.

## Recommended shape

Use four logical cell types:

1. Stable interface cell
2. Run-unique implementation package cell
3. Context-local binding cell
4. Downstream execution cells

The stable interface cell is optional when the interface already comes from a
cluster-attached JAR or when there is no interface. 

## Core rule

Mint one fresh suffix per notebook run:

- `pkg_<uuid_without_dashes>`

DO use that suffix for the actively changing implementation package cell(s).

DO NOT rewrite stable interface code just because you are using the pattern.

Keep external imports unchanged.

## Procedure when you have a stable interface

1. Identify the stable interface Scala file, for example:

   ```scala
   package api.stable

   trait Api {
     def compute(value: Double): Double
   }
   ```

2. Put that stable interface in its own package cell without changes, unless
   the interface is already available from a JAR attached to the cluster or there is no interface.
3. Mint one fresh `pkg_<uuid_without_dashes>` value for this notebook run.
4. Take the actively changing implementation file, for example:

   ```scala
   package api.implementation

   object Api extends api.stable.Api {
     def compute(value: Double): Double = 2 * value
   }
   ```

5. Rewrite only the implementation package to append the run-unique suffix:

   ```scala
   package api.implementation.pkg_<uuid_without_dashes>

   object Api extends api.stable.Api {
     def compute(value: Double): Double = 2 * value
   }
   ```

   Note that this change works whether there is a stable interface or not. In other words, you can use the pattern with a companion object that does not extend any trait.

6. If the actively changing implementation spans multiple repo-managed Scala
   files, rewrite repo-local imports and fully qualified repo-local references
   that point into that implementation package so they use the same suffixed
   package path.

   Example:

   ```scala
   import api.implementation.Api
   ```

   becomes:

   ```scala
   import api.implementation.pkg_<uuid_without_dashes>.Api
   ```

   Keep external imports unchanged.

7. Add a plain Scala binding cell after the package cells:

   ```scala
   // Deliberately use a notebook-context-local val here.
   // Do not assign through a global mutable singleton, because that would
   // leak state across parallel sessions sharing the same cluster JVM.
   val api = api.implementation.pkg_<uuid_without_dashes>.Api
   ```

8. Keep later notebook cells dependent only on that context-local binding:

   ```scala
   val result = api.compute(10)
   println(s"RESULT=$result")
   ```

## Procedure when there is no interface trait

1. Mint one fresh `pkg_<uuid_without_dashes>` value for this notebook run.
2. Rewrite the package declaration of the actively changing object or companion
   object to include that suffix.
3. Add a plain Scala binding cell that exposes a stable notebook-local handle.

Example:

```scala
package example.pkg_<uuid_without_dashes>

object Example {
  def compute(value: Double): Double = 2 * value
}
```

```scala
val example = example.pkg_<uuid_without_dashes>.Example
```

## Important constraints

- Keep package cells definition-only.
- Do not mix notebook setup or execution logic into package cells.
- The stable interface cell should stay stable. Changing it requires a
  cluster restart.
- The run-unique package suffix exists to avoid the JVM-level package freezing
  behavior of Scala package cells on a shared cluster.
- The binding cell should use notebook-context-local `val`s rather than
  cluster-global mutable state.

## Examples in this package

See `examples/scala/` for:

- repo-style Scala source inputs under `src/`
- a notebook-markdown scaffold that shows:
  - stable interface cell
  - run-unique implementation package cell
  - context-local binding cell
  - downstream execution cell

Treat the examples as mechanical guidance artifacts. Adjust them to your
user's codebase rather than copying them blindly.
