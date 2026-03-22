# databricks-agent-notebooks

Standalone notebook conversion and local/offline runtime tooling, structured as a language-neutral repository from day one.

## Repository Layout

- `python/`: active v1 package, CLI, and test surface
- `jvm/`: reserved lane for future Scala and JVM runtime work
- `contracts/`: machine-readable contracts shared across runtime and launcher boundaries
- `docs/`: repository and runtime design notes

## V1 Scope

The current extraction intentionally ships one active Python distribution:

- distribution: `databricks-agent-notebooks`
- import package: `databricks_agent_notebooks`
- CLI: `agent-notebook`

Verified local and offline surfaces today:

- notebook format conversion helpers
- narrow local execution and rendering helpers
- runtime-home and manifest primitives for tool-owned managed assets
- read-only runtime inventory commands via `agent-notebook runtimes list|doctor`

Databricks compute-mode support is not yet claimed. See [`docs/databricks-support-matrix.md`](docs/databricks-support-matrix.md) for the split between verified local/offline surfaces and unverified compute-mode surfaces.

## Local Development

```bash
cd python
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/pytest tests/unit
.venv/bin/agent-notebook help
```

## Design Constraints

- no dependence on a caller repo's virtualenv layout
- strict inward dependency direction from CLI and integrations to core/runtime layers
- generated runtime and kernel assets should live in a tool-owned home, not in host repositories
- future JVM work should fit under the existing root layout without repo surgery

See [`docs/repo-layout.md`](docs/repo-layout.md) and [`docs/runtime-home.md`](docs/runtime-home.md) for the current repository and runtime model.

Additional planning docs live in [`docs/kernels.md`](docs/kernels.md), [`docs/databricks-support-matrix.md`](docs/databricks-support-matrix.md), and [`docs/release.md`](docs/release.md).
