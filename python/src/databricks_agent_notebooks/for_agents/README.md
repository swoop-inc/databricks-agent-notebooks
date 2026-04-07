# Agent Notebook Guide

This guide is the shortest useful entrypoint for agents and sandboxed runners
that use `agent-notebook` as an execution tool.

Use it to decide what is safe to run immediately, what needs more setup, and
which deeper guide to read next.

## First encounter

This document describes what `agent-notebook` can do. It does not verify that
your environment can use it. If this is the first time you are encountering
`agent-notebook`, after reading this guide, you MUST continue to
`agent_doctor.md` — it walks through readiness checks you should complete before
your first real use.

## Safe local and offline commands

Usually safe in a sandbox:

- `agent-notebook help`
- `agent-notebook render ...`
- `agent-notebook doctor`
- `agent-notebook runtimes list`

Usually safe only when outbound artifact downloads are allowed:

- `agent-notebook kernels install ...`

These commands are good first probes when you only need local packaging or
runtime-home confirmation.

## Execution targets

The `--cluster` flag is a unified execution target selector:

| `--cluster` value | Execution mode |
|---|---|
| `SERVERLESS` | Explicit serverless (Databricks Connect) |
| `local[*]`, `local[4]`, etc. | Local Spark session (no Databricks) |
| cluster name or ID | Databricks cluster-backed execution |
| *(omitted)* | Serverless (implicit default) |

`SERVERLESS` is case-insensitive. This also works in notebook frontmatter
and pyproject.toml:

```yaml
agent-notebook:
  cluster: SERVERLESS
```

## Local execution (no Databricks)

`--cluster "local[*]"` runs notebooks against a local Spark session with no
Databricks credentials or `SPARK_HOME` needed.

- `agent-notebook run notebook.md --cluster "local[*]"` — notebook with injected local SparkSession (language from frontmatter or `--language`)
- `agent-notebook run notebook.md --cluster "local[4]"` — local Spark with 4 threads

`--profile LOCAL_SPARK` still works for backward compatibility but is
deprecated. Use `--cluster "local[*]"` instead.

### Prerequisites by language

**Python local Spark** -- needs `dangerouslyDisableSandbox: true` (PySpark's JVM creates temp dirs in `/var/folders/` which the Claude Code sandbox blocks).
- **Prerequisite:** `pyspark` must be available. It is included automatically
  when the package is installed with the `[local-spark]` extra (e.g.,
  `uv tool install "databricks-agent-notebooks[local-spark]"`). If installed
  without the extra, add pyspark manually (`pip install pyspark` or
  `uv pip install pyspark`). Run `agent-notebook doctor` to verify -- the
  pyspark check reports the installed version and whether it comes from
  standalone pyspark or databricks-connect. The check verifies that pyspark
  can actually be imported, not just that a package directory exists -- partial
  or broken installs are detected and reported. If pyspark is missing,
  `agent-notebook run --cluster "local[*]"` will fail fast with an actionable
  error message before attempting notebook execution.

**Scala local Spark** -- requires sandbox bypass (the Almond kernel writes to
`~/Library/Caches/Almond/` on macOS). Use `dangerouslyDisableSandbox: true` in
Claude Code or equivalent in other sandboxed environments.
- **Prerequisite:** a matching Almond kernel — Scala 2.12 for Spark 3.x, Scala
  2.13 for Spark 4.x. See `scala_development.md` for kernel install and
  version pairing details.

### Environment variables for tuning (all optional)

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_NOTEBOOK_LOCAL_SPARK_MASTER` | `local[*]` | Fallback master URL when `--cluster` does not specify one. Overridden by `--cluster "local[N]"`. Scala has restrictions — see [Scala development](scala_development.md#local-mode-restrictions) |
| `AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY` | _(Spark default)_ | e.g., `4g` — works for both Python (SparkConf) and Scala (injected as `-Xmx` JVM flag) |
| `AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY` | _(Spark default)_ | e.g., `2g` — Python `local-cluster` only, rejected for Scala |
| `AGENT_NOTEBOOK_LOCAL_SPARK_VERSION` | `3.5.4` | Scala only (Python uses pip-installed PySpark); Spark version for `$ivy` import |

`--cluster SERVERLESS --profile LOCAL_SPARK` is an error — contradictory targets.

### Configuration behavior

Understanding how Spark master modes interact with memory configuration
avoids silent misconfiguration:

- **`local[N]` / `local[*]`** — single-JVM mode. All execution happens in the
  driver process. `local[N]` controls thread-level parallelism; `local[*]` uses
  all available cores. There are no separate executor processes — the driver IS
  the executor.
- **Driver memory** is the only meaningful memory knob in `local[*]` mode:
  - **Python:** `AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY=4g` is injected as
    `spark.driver.memory` in SparkConf before session creation.
  - **Scala:** the same env var is injected as `-Xmx4g` into `JDK_JAVA_OPTIONS`
    at JVM startup. The SparkConf property is also generated but is decorative
    for Scala — the JVM heap is already sized by the time `SparkSession.builder`
    reads configuration.
- **`spark.executor.memory`**, **`spark.executor.instances`**, and
  **`spark.executor.cores`** are silently ignored in `local[*]` mode — there
  are no executor processes to configure.
- **Scala restrictions:** The CLI validates master URLs, executor memory, and
  driver memory configuration for Scala at startup, with actionable error
  messages. See [Scala development — local mode restrictions](scala_development.md#local-mode-restrictions)
  for the full specification.
- **`local-cluster[N,C,M]`** spawns separate executor JVMs and works for
  **Python** notebooks only. It is incompatible with Scala — see the Scala
  restrictions link above.

Use `--no-inject-session` if you want to skip session injection entirely and
manage your own Spark session in the notebook.

## User-provided libraries

The `--library` flag adds local Python source directories to `sys.path` in the
injected setup cell.  Notebooks can then import from those directories without
pip-installing anything.  This is the primary mechanism for active development
workflows where you want notebook code to use modules you are actively editing.

### Why sys.path, not pip install

`pip install -e` rebuilds the editable wheel on every invocation (~1.2s even
when nothing changed).  For a test suite running multiple notebooks, that adds
up fast.  Path injection costs zero -- no install step, no caching, no
invalidation.  Source changes are picked up immediately.

### Basic usage

The `--library` flag is repeatable.  Paths can also be declared in frontmatter
or `pyproject.toml`.  Libraries follow last-writer-wins: when multiple config
levels specify libraries, only the highest-priority level's list is used.
Lower-priority libraries are replaced entirely, not merged.

Given a notebook with this frontmatter:

```yaml
---
agent-notebook:
  profile: dev
  libraries:
    - ../libs/transforms
    - ../libs/metrics
---
```

Run with CLI libraries:

```bash
agent-notebook run notebook.md --library ../libs/test_helpers --library ../libs/overrides
```

The effective library list comes entirely from CLI (the highest-priority
level that sets libraries).  The `sys.path` order (highest priority first) is:

1. `../libs/overrides` (CLI, last)
2. `../libs/test_helpers` (CLI, first)

The frontmatter libraries (`../libs/transforms`, `../libs/metrics`) are
replaced entirely.  To use all four, list them all in the CLI invocation.

If two directories contain a module with the same name, the higher-priority
directory wins the import.

### Path resolution

- Relative paths are resolved against the notebook file's directory.
- If a path points to a directory containing both `pyproject.toml` and a `src/`
  subdirectory, it auto-resolves to the `src/` subdirectory (standard Python
  src-layout convention).  You can point at either the project root or `src/`
  directly -- both work.
- Non-existent paths produce a warning but are still included (the directory
  may be created at execution time or exist only in certain environments).

### Active development workflows

The notebook runs in its own Python process.  Your shell's `PYTHONPATH`, your
test runner's `sys.path`, and your IDE's configured interpreter are all
irrelevant -- the notebook does not inherit any of them.  `--library` is how
you make local source directories visible inside the notebook.

**Flow 1: Shared utility code.**  You have a directory with Python modules you
want to import.  Point `--library` at it.

```
repo/
  helpers/
    data_cleaning.py
    feature_utils.py
  notebooks/
    analysis.md        # wants to `from data_cleaning import ...`
```

```bash
# Path is relative to the notebook file, not the working directory
agent-notebook run notebooks/analysis.md --library ../helpers
```

Or in the notebook's frontmatter:

```yaml
---
agent-notebook:
  libraries:
    - ../helpers
---
```

**Flow 2: Project with src/tests layout.**  You are developing a library with
a standard `src` layout and a test suite.  The notebook needs the production
source, and sometimes also test utilities (fixture builders, mock factories,
assertion helpers).

```
my_lib/
  pyproject.toml
  src/my_lib/           # production code
  tests/
    helpers/            # test utilities
      fixtures.py
      assertions.py
```

For production code only, point at the project root -- src-layout auto-detection
resolves to `src/`:

```bash
agent-notebook run nb.md --library ./my_lib
```

For production code plus test helpers, add both:

```bash
agent-notebook run nb.md --library ./my_lib --library ./my_lib/tests
```

### Using --library from a test harness

A test runner that executes notebooks via `agent-notebook run` as a subprocess
should pass `--library` flags on the command line.  The test runner's own
`sys.path` does not propagate to the notebook process.

The natural pattern is:

- **Frontmatter** declares what the notebook always needs (production source).
- **CLI** adds what the caller needs for this particular run (test utilities,
  overrides).

If the test runner also imports from the same library (e.g., to build fixtures
or check return types), that is a separate concern -- the test runner uses
normal Python packaging (`uv sync`, `pip install -e`, or its own `sys.path`
setup), and the notebook uses `--library`.

### Limitations

- **Python only.**  Scala notebooks ignore `--library` with a warning.  Scala
  library paths require a different mechanism (classpath / `$ivy`) and are not
  supported.
- **`--no-inject-session`** skips all session injection including library paths.
  If you use this flag, you manage your own imports.
- **Not sufficient for compiled code.**  Libraries with C extensions or complex
  dependency trees that need a build step require `pip install` into the
  execution environment.
- **Driver-side only.**  `sys.path` changes affect the driver process where
  your notebook code runs.  UDFs that Spark serializes and sends to workers
  will not see these paths.  For worker-side code, use `spark.addPyFile()` or
  install the library as a cluster library.

## Databricks-facing commands

These commands depend on live Databricks access and may need different sandbox
or permission handling:

- `agent-notebook run ...` — the primary execution command. Its `--cluster`
  flag accepts a cluster **name** or **ID** (auto-resolved), `SERVERLESS` for
  explicit serverless, or a local master URL like `local[*]` for local Spark.
  You do NOT need to look up a cluster ID before calling `run`.
  **Exception:** `--cluster "local[*]"` is a local command
  that does not contact Databricks (see "Local execution" above).
- `agent-notebook clusters ...` — a connectivity probe for initial setup only
  (see `agent_doctor.md`). Do not use it as a pre-step to `run`. On some
  workspaces, it can generate more than a gigabyte of output.

### Do not pre-list clusters when you already have a cluster name

`agent-notebook run --cluster <cluster-name>` resolves the name automatically.

**Anti-pattern — do NOT do this:**

```bash
# WRONG: listing clusters just to get an ID you do not need
agent-notebook clusters --profile prod
# ... parse output to find cluster ID ...
agent-notebook run notebook.md --profile prod --cluster 0123-456789-abcdef
```

**Correct pattern:**

```bash
# RIGHT: pass the cluster name directly
agent-notebook run notebook.md --profile prod --cluster my-cluster
```

If the `run` command cannot resolve a cluster name, its error message will
include helpful fuzzy matches for clusters you can present to the user.

On large workspaces with many clusters, the `cluster_list_timeout` (default:
120s, configurable via `AGENT_NOTEBOOK_CLUSTER_LIST_TIMEOUT` env var or config)
may expire before an exact match is found. When this happens, `agent-notebook`
returns fuzzy name suggestions based on partial results received before the
timeout. Inspect these suggestions -- the full cluster name (e.g.,
`"rnd-alpha [engineering]"` instead of `"rnd-alpha"`) usually resolves the
issue. Passing a cluster ID is the deterministic path for cluster-based
execution and is never subject to timeouts.

The only time `agent-notebook clusters` is appropriate is during the initial
`agent_doctor.md` readiness flow to verify live Databricks connectivity. Once
you know the cluster name, pass it to `--cluster` and move on.

### Permissions

If they fail with DNS, TLS, certificate, or similar transport errors, treat the
execution environment as the first suspect rather than the tool itself.

Recommended agent policy:

- keep file reads, edits, and local/offline checks sandboxed
- run only the Databricks-facing command outside the sandbox when needed
- if sandboxed notebook startup fails around IPython or Jupyter runtime state,
  check whether the sandbox needs write access to `~/.ipython`
- for Scala notebooks, the Almond kernel requires write access to
  `~/Library/Caches/Almond/` on macOS

## Configuration

Any `run` CLI argument (except the positional `file`) can be defaulted in
`pyproject.toml`, notebook frontmatter, or environment variables. These four
config surfaces use the same vocabulary; higher-priority levels override
lower ones.

### Source levels (lowest to highest priority)

```
pyproject.toml  -->  env vars  -->  frontmatter  -->  CLI args
  (repo)          (session)       (notebook)       (invocation)
```

For scalars and booleans, the higher-priority level wins when non-null.
Libraries (list values) follow last-writer-wins: the highest-priority level that sets libraries replaces all lower levels.
Params merge across levels (higher-priority keys overwrite lower-priority keys).
The resolved environment name (`env`) is always present in the output.

### Environment variables

Parameters can be set via `AGENT_NOTEBOOK_<UPPER_KEY>` env vars:

```bash
export AGENT_NOTEBOOK_PROFILE=nonhealth-prod
export AGENT_NOTEBOOK_TIMEOUT=600
export AGENT_NOTEBOOK_ENV=staging
```

The `AGENT_NOTEBOOK_LOCAL_SPARK_*` vars are excluded (they belong to the
LOCAL_SPARK subsystem).

### Named environments

Define per-environment defaults in `pyproject.toml`:

```toml
[tool.agent-notebook]
profile = "nonhealth-prod"
libraries = ["python/src"]

[tool.agent-notebook.environments.staging]
cluster = "staging-cluster"
timeout = 300

[tool.agent-notebook.environments.production]
cluster = "prod-cluster"
timeout = 600
```

Select an environment with `--env`:

```bash
agent-notebook run nb.md --env staging
# Resolves to: profile=nonhealth-prod, cluster=staging-cluster, timeout=300
```

Environment values are defaults -- explicit params from any source override them.

**Default environment:** If `[tool.agent-notebook.environments.default]` exists
and no `--env` is specified, its `env` key determines which environment to select.
If no environments are defined, the resolved `env` is silently `"default"`.

**Comma-separated specs:** `--env staging,extra` merges `staging` then `extra`
(later environments in the list override earlier ones).

**Error on unknown:** If `--env` names an environment that does not exist,
`agent-notebook` exits with an error. The only exception is `env="default"` with
no `default` environment defined -- this silently produces empty env defaults.

### Discovery

`pyproject.toml` is discovered by walking up from the notebook file's parent
directory to the nearest `.git` boundary. The first `pyproject.toml` containing
a `[tool.agent-notebook]` section wins.

### Examples

**pyproject.toml** (repo-wide defaults):

```toml
[tool.agent-notebook]
libraries = ["python/src"]
profile = "nonhealth-prod"
format = "all"
timeout = 300
inject-session = false

[tool.agent-notebook.params]
region = "us-east-1"

[tool.agent-notebook.environments.staging]
cluster = "staging-cluster"

[tool.agent-notebook.environments.production]
cluster = "prod-cluster"
timeout = 600
```

**Notebook frontmatter** (per-notebook defaults):

```yaml
---
agent-notebook:
  profile: nonhealth-prod
  libraries:
    - python/src
  format: all
  timeout: 300
  inject-session: false
  params:
    region: us-east-1
---
```

**CLI** (per-invocation overrides):

```bash
agent-notebook run nb.md --profile nonhealth-prod --library python/src \
  --format all --timeout 300 --no-inject-session --param region=us-east-1

# With named environment:
agent-notebook run nb.md --env staging

# With JSON params:
agent-notebook run nb.md --params '{"region": "eu-west-1", "debug": "true"}'
```

### Frontmatter key

The frontmatter key is `agent-notebook:`.

### Path resolution

Relative paths for `libraries` and `output-dir` resolve differently depending
on where they are specified:

| Source | Relative paths resolve from | Why |
|--------|----------------------------|-----|
| `pyproject.toml` | pyproject.toml's parent dir (project root) | Config is in a fixed location; notebooks can be anywhere in the repo. `libraries = ["python/src"]` means "python/src in this project" regardless of which notebook runs. |
| Frontmatter | Notebook file's parent dir | Frontmatter travels with the notebook. `libraries: [../libs]` means "relative to where this notebook lives." |
| CLI | Notebook file's parent dir | You are running a specific notebook, so `--library ../libs` is relative to that notebook -- same as frontmatter. |

### Caution

`--allow-errors` and `--clean` have no CLI "off" counterpart. Once set `true`
in `pyproject.toml` or frontmatter, they apply to every run of that notebook
(or every notebook in the repo). Only set these for settings that genuinely
apply to all runs.

### Best practice: check config before adding CLI flags

Before constructing a `run` command, check `pyproject.toml` (look for
`[tool.agent-notebook]`) and the notebook's frontmatter. In a well-configured
repo, all required settings may already be supplied:

```bash
# If pyproject.toml has profile and the notebook has language in frontmatter,
# this is a complete, valid invocation -- no flags needed:
agent-notebook run notebook.md
```

Only add CLI flags to override or supplement what the config layers provide.
Redundant flags are harmless but noisy; missing config is an error. When in
doubt, run without flags first -- the error message will say exactly what is
missing.

## Progress model

Current progress model:

- `agent-notebook run` emits compact progress lines on `stderr` for `prepare`,
  `clean`, `compute`, `execute-start`, `cell-start`, `executing`, `render`,
  `done`, and `failed`
- The `compute` phase includes a `mode` value: `local-spark` (local execution
  via `--cluster "local[N]"` or deprecated `--profile LOCAL_SPARK`), `cluster`
  (cluster-backed), or `serverless` (explicit `--cluster SERVERLESS` or no
  cluster specified)
- `cell-start` identifies the current executing cell with `cell_index` plus
  safe generic descriptors such as `[code cell]` or
  `[AGENT-NOTEBOOK:INJECTED] Session setup`
- `executing` emits coarse heartbeats tied to the same safe descriptor set
- percent complete is intentionally not inferred; repeated heartbeats on one
  long-running cell are normal for remote Databricks work

## Timeouts

### Notebook cell timeout (`--timeout`)

Only set `--timeout` if you have a high-confidence real upper bound for the cell
or notebook you are running. Otherwise, you risk losing valuable work due to
factors you cannot control: cluster startup/resizing, node availability, task
failure and re-execution, resource contention from other jobs.

`--timeout` is per-cell, not per-notebook.

### Agent environment timeout

Your agent environment's shell tool has its own timeout — the maximum duration a
single foreground command can run before being killed. In Claude Code, this is
600 seconds (10 minutes). If a foreground `agent-notebook run` exceeds this
ceiling, the process is killed and the work is lost.

This is why long-running execution patterns (below) default to non-blocking: they
decouple the notebook run from the shell tool's timeout.

### Databricks server-side execution timeout

On serverless compute, Databricks Connect enforces a default per-query timeout
of **9000 seconds (2.5 hours)**. If a query exceeds this limit, it is canceled
with `QUERY_EXECUTION_TIMEOUT_EXCEEDED`. The timeout is configurable via
`spark.databricks.execution.timeout`.

Override it in a notebook config cell when you expect long-running queries:

```python
spark.conf.set("spark.databricks.execution.timeout", "86400")  # 24 hours
```

**If you expect multi-hour transforms** (large tables, heavy joins, full-graph
traversals), discuss the timeout with the user before running. The 2.5-hour
default is a common surprise for workloads at scale.

### Orphaned queries after client disconnect

When `agent-notebook`'s `--timeout` fires or the local process is killed, the
server-side Spark query does not stop immediately. Databricks detects the broken
Spark Connect session and cancels the orphaned query, but with a delay of
approximately 5-6 minutes. During this window the query continues consuming
serverless compute.

This cleanup is automatic. For typical agent workflows, the cost of a few
minutes of orphan compute is negligible and does not require special handling.

## Long-running runs

Unless you have a concrete reason to expect a short run (e.g., a familiar
serverless smoke notebook you have run before), treat every notebook run as
potentially long. Cluster startup, serverless warmup, queueing, autoscaling,
dependency setup, and shared compute contention can all stretch runtimes
unpredictably.

### Execution helper script

A parameterizable wrapper script ships with this package at
`for_agents/scripts/agent-nb-run.sh`. It handles path computation (log path,
rendered output path, stem), output directory creation, early validation, and
tee-to-log — so you do not need to reconstruct these details from examples.

**Important:** This script is NOT on `PATH`. You must resolve its absolute path
from the installed package before use.

#### Resolving the script path

```bash
# Resolve once and reuse:
AGENT_NB_RUN="$(python3 -c "from importlib.resources import files; print(files('databricks_agent_notebooks').joinpath('for_agents', 'scripts', 'agent-nb-run.sh'))")"
```

Cache this in `AGENT_NB_RUN` and reuse the variable throughout your session. All
examples below assume this variable is set.

Use it as the command in any of the patterns below. It forwards all arguments to
`agent-notebook run` unchanged.

```bash
"$AGENT_NB_RUN" <notebook> --profile <profile> [--output-dir <dir>] [--cluster <name>] [--format md] [...]
```

### Quick reference: which pattern to use

| Environment | Default pattern | Use foreground only when |
|-------------|----------------|------------------------|
| Claude Code | `run_in_background` | You know the run will finish in under 5 minutes |
| Codex | PTY session | N/A — PTY has no fixed timeout |
| Standard shell | `nohup` detached | You will wait interactively |

### Claude Code

Two non-blocking patterns, in preference order.

**Pattern 1: `run_in_background` (preferred)**

Non-blocking — the session stays interactive and Claude Code notifies on
completion. No fixed timeout ceiling; the run continues until it finishes.

Use the Bash tool with `run_in_background: true`:

```bash
# Use with Bash tool parameter: run_in_background: true
"$AGENT_NB_RUN" path/to/notebook.md \
  --profile <profile> \
  --format md \
  --output-dir tmp/run-output
```

The script emits the log path and output directory to stderr at startup. Read the
log file to check progress while the run is active.

**Pattern 2: `nohup` detached**

Best for fire-and-forget — runs survive session termination. Useful when
operating as a sub-agent or in a short-lived session.

```bash
nohup "$AGENT_NB_RUN" path/to/notebook.md \
  --profile <profile> \
  --format md \
  --output-dir tmp/run-output \
  > /dev/null 2>&1 &
echo "PID: $!"
```

The script tees output to its own log file, so redirecting to `/dev/null` is
safe — nothing is lost.

**Note on foreground:** Foreground execution blocks the session and is subject to
the 10-minute timeout ceiling. Only use foreground when you have strong
confidence the run will finish in under 5 minutes (e.g., a known-quick
serverless smoke notebook you have run before).

### Codex

`nohup` detachment is unreliable in Codex — background processes may be killed
after the tool call returns. Use a persistent PTY session instead.

Start the command in a PTY session (`tty: true`), then poll with empty
`write_stdin` calls to check progress:

```bash
"$AGENT_NB_RUN" path/to/notebook.md \
  --profile <profile> \
  --format md \
  --output-dir tmp/run-output
```

The PTY session has no fixed timeout — the command runs until completion.

### Standard shell / non-agent environments

Use the `nohup` detached pattern:

```bash
nohup "$AGENT_NB_RUN" path/to/notebook.md \
  --profile <profile> \
  --format md \
  --output-dir tmp/run-output \
  > /dev/null 2>&1 &
echo "PID: $!"
```

Add `--cluster <cluster-name-or-id>` when the user context requires cluster
compute. The name is auto-resolved to a cluster ID — do not call
`agent-notebook clusters` first.

### Monitoring (all environments)

After launching a non-blocking run, use these to check status:

- **Process liveness:** `ps -p <pid> -o pid=,command=` or
  `pgrep -lf "agent-notebook run"` (macOS) /
  `pgrep -af "agent-notebook run"` (Linux)
- **Progress:** `tail -f <log-path>` (the execution helper reports the log path
  at startup)
- **Completion:** the rendered output file (e.g., `<stem>.executed.md`) appears
  in the output directory when the run finishes successfully
- **Side effects:** if earlier cells have already written durable side effects
  elsewhere, you can often inspect those while the notebook is still running

### Intermediate progress monitoring

`agent-notebook` is only one part of a broader Databricks toolchain. When a cell
produces durable side effects such as tables or file assets, consider whether
the user already has other Databricks tooling available for cheap follow-up
work before the notebook fully completes:

- the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli) is
  useful for lightweight inspection and retrieval flows
- [Databricks MCP](https://docs.databricks.com/aws/en/generative-ai/mcp) can
  expose Databricks tools to an MCP-aware agent environment

If the user has no such tooling, follow-up work can be performed using a new 
focused, fast running notebook.

Good follow-up checks are cheap operations such as schema inspection, row-count
validation, and retrieval of small file artifacts. Discuss policy with the user
before using serverless or clusters for more expensive operations.

### Sub-agent patterns

This pattern also works if you want a notebook run to survive an agent session, for example when you are operating as a sub-agent or in a short-lived session.

In that case, generate a handoff artifact for future agent sessions that records the rendered output path and run log path.

If the notebook has already produced durable side effects elsewhere, optionally record those locations too, although good notebook design should usually make those locations visible in notebook output.

## Artifact outputs

There are four common artifact outputs: tables, files, display summaries and diagnostic output.

What notebooks should emit when creating new artifacts:

- **tables:** table names
- **files:** canonical retrieval-ready paths
- **display() summaries:** directly in notebook output

Avoid:

- browser-only URLs
- large inline notebook payloads
- presigned URLs as the default durable contract

Good outputs are canonical volume paths, workspace file paths, table names, or
other machine-usable locations an agent can pass to the next step.

### Display summaries

The output of `display()` can be convenient. Important caveats:

- truncation limits for `display()`: it works best for small summary tables and "for example" outputs.
- displaying columns with complex data types: they may render poorly; often better shown as JSONlines diagnostic output

### Diagnostic output

Printing to STDOUT appears in cell output. Important caveats:

- truncation limits for STDOUT: it works best for constrained print output. For bigger output, generate files.
- STDOUT output is hidden when `display()` is used in the same cell: if you want to see STDOUT, put `display()` in a separate cell.

### Tables

Prefer Unity Catalog tables for tabular outputs. Agree with the user on where
tables should be created. If a table name is dynamically determined, include it in diagnostic output.

### Files

Use file assets when they are the right shape for the result, such as logs,
diagnostics, exported support files, or small verification artifacts.

For file-native outputs, prefer Unity Catalog volumes as the default side
channel.

Canonical path forms:

- inside notebooks or runtime code: `/Volumes/<catalog>/<schema>/<volume>/...`
- for CLI retrieval: `dbfs:/Volumes/<catalog>/<schema>/<volume>/...`

Workspace files are reasonable for small development-oriented artifacts, but do
not use them as the default large-result storage surface.

DBFS root is a legacy-only fallback. Do not design new workflows around DBFS
root or mounts unless compatibility forces it.

When the real artifact is a query result, prefer direct table or query-result
retrieval over turning that result into an ad hoc file.

## Run artifact management

Decide, where appropriate with user input, where executed notebooks, rendered outputs, run logs, and similar byproducts should be stored in the file system. Also decide whether they should be tracked in git, ignored locally, or cleaned up after the run.

Transient run logs and rendered outputs should usually stay out of git unless the user explicitly wants them preserved as artifacts.

On success, keep the smallest useful output surface and clean up bulky transient files when they no longer help with handoff or verification.

On failure, bias toward keeping the run log, rendered notebook output if present, and any pointers to partial durable side effects, at least while these artifacts may be useful for root cause analysis without rerunning immediately.

### Cleaning output before re-runs (`--clean`)

`--clean` removes and recreates the output directory (`{stem}_output/`) before
the pipeline writes any files. Use it when re-running a notebook and you want a
deterministic output directory with no stale artifacts from a prior run.

```bash
agent-notebook run notebook.md --profile prod --clean
```

Without `--clean`, new outputs overwrite same-named files but stale files from
previous runs (e.g., renamed assets, old rendered formats) remain.

## Preprocessing

Notebook source files are preprocessed before parsing. Directives using
`{! plugin("args") !}` syntax are expanded at the text level, before
`to_notebook()` sees the content. Preprocessing applies to all text-based
notebook formats (`.md`, `.py`, `.scala`, `.sql`) -- only `.ipynb` files are
excluded (they are JSON, not plain text).

- If no `{!` appears in the source, preprocessing is a no-op (zero overhead).
- Use `--no-preprocess` to skip preprocessing entirely.

Built-in plugins:

- `include` -- inline file content relative to the notebook directory
- `param` -- CLI-driven parameterization via `--param name=value`

Parameterization example:

```bash
agent-notebook run notebook.md --param table_name=users --param limit=100
```

In the notebook:

```python
table = "{! param('table_name').with_default('default_table') !}"
```

Read `preprocessing/plugins.md` for the full plugin reference with syntax
and examples.

## Runtime parameters (`agent_notebook_parameters`)

All resolved configuration and user-defined params are exposed to notebook
code at runtime via the `agent_notebook_parameters` dict. This is injected
automatically as a hidden cell before the session setup -- no opt-in needed.

```python
# Available in every Python/SQL notebook cell after injection:
print(agent_notebook_parameters["profile"])      # "nonhealth-prod"
print(agent_notebook_parameters["timeout"])       # 300 (int, not string)
print(agent_notebook_parameters["my_param"])      # from --param or config
```

The dict includes:

- **Config fields** with native types: `profile` (str), `cluster` (str),
  `language` (str), `timeout` (int), `allow_errors` (bool), `output_dir` (str),
  `libraries` (list of str).
- **User-defined params** from frontmatter `params:` or `pyproject.toml`
  `[tool.agent-notebook.params]`, with their original types preserved (int,
  bool, list, etc.). CLI params (`--param key=value` and `--params '...'`)
  are coerced to strings at parse time.

Framework-internal fields (`inject_session`, `preprocess`, `clean`, `format`,
`hooks`, `params`) are excluded -- they control tool behavior, not notebook logic.

Scala notebooks do not yet receive `agent_notebook_parameters` (planned).

### Relationship to preprocessing

Preprocessing (`{! param("name") !}`) is text-time substitution -- it runs
before the notebook is parsed. `agent_notebook_parameters` is runtime access --
it is a Python dict available in cells during execution. Use preprocessing when
you need values baked into source code (string literals, SQL table names). Use
`agent_notebook_parameters` when you need typed values at runtime (ints, bools,
dicts) or when you want to branch on config programmatically.

## Hooks (prologue cells)

Hooks let you inject setup code between session creation and user content.
Configure them under `hooks.<language>` in `pyproject.toml` or frontmatter.

### Cell sequence

A generated notebook has this cell order:

```
1. parameters_setup    -- agent_notebook_parameters (hidden, automatic)
2. session_setup       -- SparkSession/DatabricksSession (hidden, unless --no-inject-session)
3. prologue cells      -- user-defined setup (from hooks)
4. content cells       -- from the notebook source file
```

### Configuration

**pyproject.toml** (repo-wide):

```toml
[tool.agent-notebook.hooks.python]
prologue_cells = [
    "spark.sql('USE CATALOG my_catalog')",
    "spark.sql('USE SCHEMA my_schema')",
]
```

**Frontmatter** (per-notebook):

```yaml
---
agent-notebook:
  hooks:
    python:
      prologue_cells:
        - "from my_helpers import setup; setup(spark)"
---
```

### Cell type rules

Prologue cell strings are typed by their content:

| Content pattern | Cell type | Visibility |
|-----------------|-----------|------------|
| Plain code (no fence) | Code cell | Hidden (stripped from rendered output unless error) |
| `` ```python `` ... `` ``` `` | Code cell | Visible (shown in rendered output with output) |
| `` ```markdown `` ... `` ``` `` | Markdown cell | Visible (shown in rendered output) |

The opening fence can use more than three backticks (e.g. ` `````` `) to allow
the content itself to contain code fences. The fence language tag must match the
notebook language for visible code cells. A fenced block with an unrecognized
language tag (e.g. `` ```scala `` in a Python notebook) is treated as hidden
code with the fence markers stripped.

### Preprocessing in prologue cells

Prologue cells are Jinja-preprocessed with the same params available to the
notebook source. This means `{! param("name") !}` works in prologue cells:

```toml
[tool.agent-notebook.hooks.python]
prologue_cells = [
    "spark.sql('USE CATALOG {! param(\"catalog\").with_default(\"dev\") !}')",
]
```

### `-FILE` convention

For longer setup code, use the `-FILE` suffix to read cell content from files:

```toml
[tool.agent-notebook.hooks.python]
prologue_cells-FILE = ["./hooks/setup_catalog.py", "./hooks/import_helpers.py"]
```

Relative file paths are resolved from the directory containing `pyproject.toml`.
Absolute paths are used as-is. The `-FILE` convention is a `pyproject.toml`-only
feature -- it is not supported in frontmatter YAML.

### Error surfacing

Prologue cells that are hidden (plain code, no fence) follow the same rule as
the session setup cell: stripped from rendered output on success, but included
if the cell produces an error. This lets users debug prologue failures without
spelunking in raw `.ipynb` files.

### Merge semantics

Hooks use override (last-writer-wins) semantics, like `libraries`. If both
`pyproject.toml` and frontmatter define `hooks.python.prologue_cells`, the
frontmatter value replaces the project-level value entirely.

## Scala development 

Read `scala_development.md` when the task involves Scala notebook work.

The packaged examples live under `examples/` beside these docs. They are small
reference artifacts you can reuse directly instead of recreating notebooks or
pattern files from memory.

## Notebook utilities (environment detection and repo root)

The `notebook_utils` module provides three functions for dual-environment notebook
development:

- `is_databricks(spark=None)` -- detect Databricks vs local Spark
- `resolve_repo_root()` -- find the repo root when `__file__` is unavailable
- `set_query_execution_timeout(seconds=9000, spark=None)` -- set per-query
  timeout on Databricks (no-op locally)

Import them directly -- this works in all execution modes (LOCAL_SPARK,
serverless, cluster) because `agent-notebook` always executes notebooks in a
Python environment where the package is installed:

```python
from databricks_agent_notebooks.notebook_utils import *
```

No `include()` or `_includes/` symlinks are needed for these utilities.
`include()` is for inlining **repo-local code** (your project's transforms,
domain logic, helper modules) -- not for utilities that ship with
`agent-notebook`.

Read `notebook_utils_guide.md` for the full reference -- usage patterns,
resolution order, and interaction with run scripts.

### Inline detection (Scala or one-off use)

For Scala notebooks or one-off checks where importing the module is not
practical, the reliable approach -- working on both clusters and serverless --
is to check for any Spark configuration key containing `.databricks.`:

**Python:**

```python
conf = spark.conf.getAll
if callable(conf):
    conf = conf()
is_dbx = any(".databricks." in k for k in conf.keys())
```

**Scala:**

```scala
val isDatabricks = spark.conf.getAll.keys.exists(_.contains(".databricks."))
```

## Persistence and version checking

You MUST persist key facts about `agent-notebook` to your memory system. The
specific items to persist are listed in `agent_doctor.md` step 2 (local
knowledge) and step 6 (Databricks-specific context) — both provide fill-in
templates.

If you already have `agent-notebook` in memory or instructions, run
`agent-notebook --version` and compare against your stored version. If versions
differ, re-run the agent_doctor flow to refresh. Check both your memory system
and any persistent instructions (CLAUDE.md, AGENTS.md, etc.) — stale
instructions are as problematic as stale memory.

In a non-interactive session where you cannot prompt the user, refresh local
knowledge (doctor steps 1-2) and skip steps that require user input (workspace
selection, smoke tests).

This is not optional. Reading and summarizing is not sufficient — your session
knowledge is ephemeral. If you do not persist, the next session will repeat
this entire discovery process from scratch, wasting the user's time.

## DECISION REQUIRED: agent doctor

Check your memory system and persistent instructions for `agent-notebook`
context. Then:

1. **No stored context:** this is your first encounter — you MUST continue
   reading `agent_doctor.md`
2. **Stored context exists:** run `agent-notebook --version` and compare against
   your stored version. If versions differ, or stored paths no longer exist,
   continue reading `agent_doctor.md` to refresh
3. **Version matches and paths exist:** you are current — skip the doctor

When continuing to `agent_doctor.md`:
- In an interactive session, complete the full flow
- In a non-interactive session, complete steps 1-2 (local checks and persistence
  refresh) and skip steps that require user input
- DO NOT stop to complete the conversation turn
- DO NOT return to the user with a summary of this README
