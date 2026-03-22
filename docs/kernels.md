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
- the launcher contract artifact is authoritative for launcher path, bootstrap argv metadata, and the runtime receipt it expects to launch against
- the launcher clears Spark-specific env at launch time instead of baking Databricks wiring into handwritten kernelspec env blocks
- the kernel receipt records the generated install directory, runtime id, runtime receipt path, and launcher contract path for repair and doctor flows

## Runtime Identity

Kernel ids and runtime ids are now distinct:

- `kernel_id` remains the stable kernelspec directory name
- `runtime_id` is keyed from compatibility metadata, currently the Databricks line plus the active Python line
- launcher contracts and kernel receipts both carry `runtime_id` and `runtime_receipt_path`
- `agent-notebook kernels list` surfaces the resolved runtime id for each discovered kernel

For the first runtime-manager tranche, runtime installs are still metadata-only and offline-verifiable. `agent-notebook runtimes list` and `agent-notebook runtimes doctor` expose the managed runtime inventory without adding download, repair, prune, or GC flows yet.
