# databricks-agent-notebooks

`databricks-agent-notebooks` is a Python package and CLI for working with Databricks-oriented notebooks from a normal local development environment.

It exists to make notebook workflows easier to package, inspect, and run outside a workspace UI. The project focuses on three things:

- turning notebook-like sources into standard notebook artifacts
- managing the local kernel and runtime metadata needed for notebook execution
- rendering executed notebooks into shareable Markdown or HTML output

The repository is intentionally structured for long-term growth, but the currently published artifact is the Python package in [`python/`](python/).

## Install From PyPI

`databricks-agent-notebooks` is published on PyPI:

```bash
python3 -m pip install databricks-agent-notebooks
```

The package requires Python 3.11 or newer. For Databricks Connect-oriented execution, use a Python version compatible with the target `databricks-connect` line.

## Prerequisites By Workflow

Some commands are pure local/offline operations. Others depend on external tools you need to install separately.

### Base package and local/offline flows

These are enough for installation, CLI discovery, runtime-home inspection, notebook rendering, and Python notebook execution:

- Python 3.11 or newer
- `pip`

The PyPI package installs its Python dependencies for you, including `ipykernel`, `jupytext`, `nbconvert`, `nbformat`, `platformdirs`, and `PyYAML`.

### Kernel installation and Databricks Connect-oriented flows

These are required when you want to install or validate the managed Scala kernel:

- Java 11 or newer
- `coursier` or `cs` on `PATH`
- a Jupyter client such as JupyterLab, Jupyter Notebook, or an editor that can use Jupyter kernels

### Databricks-aware commands

These are required for commands that inspect Databricks configuration or resolve clusters:

- the Databricks CLI on `PATH`
- a configured profile in `~/.databrickscfg` or `DATABRICKS_CONFIG_FILE`

The current package uses the Databricks CLI for cluster discovery and profile validation helpers. That is a package requirement for those code paths, not a universal Databricks Connect requirement.

## For Agents And Sandboxed Runners

If you are running `agent-notebook` from an agent framework or a sandboxed execution environment, separate local/offline commands from live Databricks commands.

Usually safe in sandbox:

- `agent-notebook help`
- `agent-notebook render ...`
- `agent-notebook kernels install ...`
- `agent-notebook kernels doctor ...`
- `agent-notebook runtimes list`
- `agent-notebook runtimes doctor`

Commands that may need an unsandboxed execution path:

- `agent-notebook run ...` when it must talk to Databricks
- `agent-notebook clusters ...`
- any workflow that depends on live Databricks CLI network access

Observed failure mode in restricted sandboxes: Databricks-facing commands can fail with DNS, TLS, or certificate verification errors even when the same command works from the host shell. If that happens, treat it as an execution-environment constraint first, not as proof that `agent-notebook` itself is misconfigured.

Recommended agent policy:

- keep file reads, edits, and local/offline checks sandboxed
- run only the Databricks-facing command outside the sandbox
- do not broaden the unsandboxed exception to unrelated commands

## Databricks Connect Versioning

The current release can resolve target cluster identity, but it does not auto-install or auto-switch `databricks-connect` in your local Python environment.

Use a dedicated environment for notebook execution and install a `databricks-connect` line compatible with the target compute you intend to use. For cluster-backed runs, cluster metadata can inform that choice. For serverless flows, validate with `databricks-connect test` in the environment that will execute notebooks.

## Verified Install Quickstart

The repository currently verifies local/offline packaging and CLI surfaces first. A conservative quickstart after installing from PyPI is:

```bash
python3 -m pip install databricks-agent-notebooks
agent-notebook help
agent-notebook render --help
agent-notebook install-kernel --help
agent-notebook kernels install --help
agent-notebook doctor --help
agent-notebook kernels doctor --help
agent-notebook runtimes list
agent-notebook runtimes doctor --help
```

## First Useful Commands

Render an already-executed notebook:

```bash
agent-notebook render path/to/notebook.executed.ipynb --format md
```

Install and inspect the managed Scala kernel:

```bash
agent-notebook kernels install --user
agent-notebook kernels list
agent-notebook kernels doctor --profile DEFAULT
```

Check runtime-home state without mutating anything:

```bash
agent-notebook runtimes list
agent-notebook runtimes doctor
```

Run a notebook through the local execution pipeline after the Databricks prerequisites are in place:

```bash
agent-notebook run path/to/notebook.md --profile DEFAULT --cluster <cluster-name-or-id>
```

## What The Project Supports Today

The current release is intentionally conservative about support claims. Verified local/offline surfaces today include:

- Python packaging and installation from built artifacts
- notebook conversion, rendering, and runtime-home helpers
- managed kernel install, list, remove, and doctor flows
- read-only runtime inventory commands under the tool-managed home
- Python notebook execution from the installed package, including a local `python3` kernel preflight backed by the packaged `ipykernel` dependency

Managed assets live under a dedicated runtime home instead of being written into the caller's repository. See [`docs/runtime-home.md`](docs/runtime-home.md) for the layout and the `DATABRICKS_AGENT_NOTEBOOKS_HOME` override.

## Support Boundaries And Non-Goals

Databricks compute-mode support is not yet claimed.

In practice, that means:

- this project is local/offline first, even when it exposes Databricks-aware commands
- it is not a general Databricks workspace sync, deployment, or job-orchestration tool
- runtime inventory is read-only today; download, repair, prune, and garbage-collection flows are not part of the current surface
- the `jvm/` tree is repository scaffolding for future work, not a published JVM distribution today

For the precise release posture, see [`docs/databricks-support-matrix.md`](docs/databricks-support-matrix.md).

## Deeper Documentation

- [Databricks support matrix](docs/databricks-support-matrix.md)
- [Kernel installation and runtime model](docs/kernels.md)
- [Runtime-home layout](docs/runtime-home.md)
- [Repository structure](docs/repo-layout.md)
- [Release and publishing notes](docs/release.md)

## Contributing

External contributions are welcome. Start with [`CONTRIBUTING.md`](CONTRIBUTING.md) for contributor setup and the verification commands to run before opening a pull request.

If you want a quick editable install from the repository root:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e './python[dev]'
```

## License

This project is available under the [MIT License](LICENSE).
