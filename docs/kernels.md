# Kernels

Kernel installation and launch should stay generated and machine-addressable.

## Principles

- kernelspec directories are identified by stable machine ids
- display names are user-facing labels, not primary identities
- `kernel.json` should stay thin and point at a launcher/bootstrap boundary
- Spark and Databricks wiring belongs in launcher code, not handwritten kernelspec env blocks

## Intended CLI Surface

The standalone CLI should grow toward:

- `agent-notebook kernels install`
- `agent-notebook kernels list`
- `agent-notebook kernels remove`
- `agent-notebook kernels doctor`

## Current Tranche

The standalone runtime now installs managed Scala kernels through a generated launcher boundary:

- `agent-notebook kernels install` still uses Almond at install time, but rewrites the installed `kernel.json` so `argv` points at the packaged `databricks_agent_notebooks.runtime.launcher` entrypoint
- the launcher contract artifact is authoritative for launcher path and bootstrap argv metadata
- the launcher clears Spark-specific env at launch time instead of baking Databricks wiring into handwritten kernelspec env blocks
- the kernel receipt records the generated install directory and its launcher contract path for repair and doctor flows
