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
      dbr-16.4-python-3.12/
        runtime-receipt.json
        venv/
    kernels/
  state/
    installations/
    links/
    logs/
  bin/
  config/
    serverless-runtime-policy-cache.json
```

This separation keeps durable installs distinct from disposable cache state and gives kernel/runtime receipts a stable home.

## Receipt Placement

- managed runtime receipts live under `data/runtimes/<runtime-id>/runtime-receipt.json`
- generated kernelspecs still live under `data/kernels/`
- kernel install receipts live under `state/installations/kernels/<kernel-id>.json`

This means runtime identity is now rooted in a runtime receipt under `data/runtimes/`, while kernel artifacts reference that receipt instead of treating `kernel_id` as a runtime identifier.

## Managed Connect Runtimes

Cluster-backed execution now materializes managed Python runtimes under `data/runtimes/`.

Behavior:

- runtime ids remain stable as `dbr-<major.minor>-python-<major.minor>`
- each runtime owns its own `venv` alongside the runtime receipt
- on first use for an injected Python cluster-backed run, `agent-notebook run --cluster ...` creates the environment and installs `databricks-connect==<line>.*`
- subsequent runs on the same DBR line reuse the existing runtime only when both the runtime receipt and `venv` are present; incomplete first-time materializations are repaired on retry instead of being treated as healthy

## Serverless Policy Cache

Python serverless execution caches the first validated Connect line per workspace/profile in `config/serverless-runtime-policy-cache.json`. The cache maps a `profile:<name>|host:<host>` key to the Connect line that passed validation, so subsequent serverless runs skip the candidate trial loop. The `DATABRICKS_AGENT_NOTEBOOKS_SERVERLESS_CONNECT_LINE` env var bypasses both the cache and the candidate list.
