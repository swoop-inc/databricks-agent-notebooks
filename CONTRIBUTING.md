# Contributing

Pull requests are welcome.

## Development Setup

The public contributor path is intentionally simple:

- Python 3.11 or newer is required for package development
- Java 11+, `coursier`/`cs`, and the Databricks CLI are only needed if you are working on kernel installation, doctor checks, or Databricks-aware flows
- For remote execution work, use a Python interpreter compatible with the target `databricks-connect` line

Set up a local editable environment from the repository root:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e './python[dev]'
python -m databricks_agent_notebooks help
agent-notebook help
```

## Before Opening A PR

Run the core verification commands from the repository root:

```bash
python -m pytest python/tests/unit
python -m databricks_agent_notebooks help
agent-notebook help
agent-notebook render --help
agent-notebook kernels install --help
agent-notebook doctor --help
agent-notebook runtimes doctor --help
```

If you change packaging or installation behavior, also run a fresh local install in a clean virtual environment and confirm the CLI still starts.

## Notes

- Keep documentation and examples public-facing and repo-relative.
- Keep support claims aligned with [`docs/databricks-support-matrix.md`](docs/databricks-support-matrix.md).
