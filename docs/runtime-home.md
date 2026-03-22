# Runtime Home

Managed runtime assets live outside caller repositories.

Resolution order:

1. `DATABRICKS_AGENT_NOTEBOOKS_HOME`
2. platformdirs user data path for `databricks-agent-notebooks`

Layout:

```text
<home>/
  cache/
  data/
    runtimes/
    kernels/
  state/
    installations/
    links/
    logs/
  bin/
  config/
```

This separation keeps durable installs distinct from disposable cache state and gives kernel/runtime receipts a stable home.

## Receipt Placement

- managed runtime receipts live under `data/runtimes/<runtime-id>/runtime-receipt.json`
- generated kernelspecs still live under `data/kernels/`
- kernel install receipts live under `state/installations/kernels/<kernel-id>.json`

This means runtime identity is now rooted in a runtime receipt under `data/runtimes/`, while kernel artifacts reference that receipt instead of treating `kernel_id` as a runtime identifier.

## Read-Only Inventory Surface

- `agent-notebook runtimes list` shows the runtime ids recorded under runtime-home
- `agent-notebook runtimes doctor` validates receipt shape and `install_root` coherence without downloading or mutating runtimes
