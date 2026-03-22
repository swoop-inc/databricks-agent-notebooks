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
