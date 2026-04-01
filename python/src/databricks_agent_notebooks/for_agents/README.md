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

## Local execution (no Databricks)

`--profile LOCAL_SPARK` runs notebooks against a local Spark session with no
Databricks credentials or `SPARK_HOME` needed.

- `agent-notebook run notebook.md --profile LOCAL_SPARK` — notebook with injected local SparkSession (language from frontmatter or `--language`)

### Prerequisites by language

**Python LOCAL_SPARK** — sandbox-safe, no special permissions needed.
- **Prerequisite:** `pyspark` must be installed in the active Python environment
  (`pip install pyspark` or `uv pip install pyspark`). Run `agent-notebook doctor`
  to verify — the pyspark check reports the installed version and whether it
  comes from standalone pyspark or databricks-connect. The check verifies that
  pyspark can actually be imported, not just that a package directory exists —
  partial or broken installs are detected and reported. If pyspark is missing,
  `agent-notebook run --profile LOCAL_SPARK` will fail fast with an actionable
  error message before attempting notebook execution.

**Scala LOCAL_SPARK** — requires sandbox bypass (the Almond kernel writes to
`~/Library/Caches/Almond/` on macOS). Use `dangerouslyDisableSandbox: true` in
Claude Code or equivalent in other sandboxed environments.
- **Prerequisite:** a matching Almond kernel — Scala 2.12 for Spark 3.x, Scala
  2.13 for Spark 4.x. See `scala_development.md` for kernel install and
  version pairing details.

### Environment variables for tuning (all optional)

| Variable | Default | Purpose |
|---|---|---|
| `AGENT_NOTEBOOK_LOCAL_SPARK_MASTER` | `local[*]` | Spark master URL. Scala has restrictions — see [Scala development](scala_development.md#local-mode-restrictions) |
| `AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY` | _(Spark default)_ | e.g., `4g` — works for both Python (SparkConf) and Scala (injected as `-Xmx` JVM flag) |
| `AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY` | _(Spark default)_ | e.g., `2g` — Python `local-cluster` only, rejected for Scala |
| `AGENT_NOTEBOOK_LOCAL_SPARK_VERSION` | `3.5.4` | Scala only (Python uses pip-installed PySpark); Spark version for `$ivy` import |

`--profile LOCAL_SPARK --cluster foo` is an error — the two are mutually exclusive.

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

## Databricks-facing commands

These commands depend on live Databricks access and may need different sandbox
or permission handling:

- `agent-notebook run ...` — the primary execution command. Its `--cluster`
  flag accepts a cluster **name** or **ID** and auto-resolves the name to an ID
  internally. You do NOT need to look up a cluster ID before calling `run`.
  **Exception:** `agent-notebook run --profile LOCAL_SPARK` is a local command
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

On large workspaces with many clusters, the 30-second cluster-listing timeout
may expire before an exact match is found. When this happens, `agent-notebook`
returns fuzzy name suggestions based on partial results received before the
timeout. Inspect these suggestions — the full cluster name (e.g.,
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

## Progress model

Current progress model:

- `agent-notebook run` emits compact progress lines on `stderr` for `prepare`,
  `compute`, `execute-start`, `cell-start`, `executing`, `render`, `done`, and
  `failed`
- The `compute` phase includes a `mode` value: `local-spark` (LOCAL_SPARK
  profile), `cluster` (cluster-backed), or `serverless` (no cluster)
- `cell-start` identifies the current executing cell with `cell_index` plus
  safe generic descriptors such as `[code cell]` or
  `[AGENT-NOTEBOOK:INJECTED] Databricks session setup`
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

## Scala development 

Read `scala_development.md` when the task involves Scala notebook work.

The packaged examples live under `examples/` beside these docs. They are small
reference artifacts you can reuse directly instead of recreating notebooks or
pattern files from memory.

## Detecting the Databricks environment

Code running on Databricks sometimes needs to detect that it is on Databricks
rather than a local Spark session. The reliable approach — working on both
clusters and serverless — is to check for the presence of any Spark session
configuration key containing `.databricks.`:

**Python:**

```python
is_databricks = any(".databricks." in k for k in spark.conf.getAll().keys())
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
