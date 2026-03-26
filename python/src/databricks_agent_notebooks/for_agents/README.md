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
- `agent-notebook kernels doctor ...`
- `agent-notebook runtimes list`
- `agent-notebook runtimes doctor`

Usually safe only when outbound artifact downloads are allowed:

- `agent-notebook kernels install ...`

These commands are good first probes when you only need local packaging or
runtime-home confirmation.

## Databricks-facing commands

These commands depend on live Databricks access and may need different sandbox
or permission handling:

- `agent-notebook run ...` — the primary execution command. Its `--cluster`
  flag accepts a cluster **name** or **ID** and auto-resolves the name to an ID
  internally. You do NOT need to look up a cluster ID before calling `run`.
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

## Progress model

Current progress model:

- `agent-notebook run` emits compact progress lines on `stderr` for `prepare`,
  `compute`, `execute-start`, `cell-start`, `executing`, `render`, `done`, and
  `failed`
- `cell-start` identifies the current executing cell with `cell_index` plus
  safe generic descriptors such as `[code cell]` or
  `[AGENT-NOTEBOOK:INJECTED] Databricks session setup`
- `executing` emits coarse heartbeats tied to the same safe descriptor set
- percent complete is intentionally not inferred; repeated heartbeats on one
  long-running cell are normal for remote Databricks work

## Timeouts

Only set `--timeout` if you have a high-confidence real upper bound for the cell or
notebook you are running. Otherwise, you risk losing valuable work due to factors you cannot control: cluster startup/resizing, node availability/termination, task/stage failure and re-execution, resource contention from other jobs.

If you do set timeout `--timeout`, note that it is cell-based and ensure you use 
an appropriately long Bash tool timeout.

## Long-running runs

If your shell or agent environment may kill long commands, prefer a detached
`nohup` launch for `agent-notebook run ...` and monitor the log plus rendered
output artifact instead of waiting interactively.

Default recommendation:

- prefer `nohup` unless you already know the run will finish comfortably within
  the shell timeout
- use a detached pattern when cluster startup, serverless warmup, autoscaling,
  queueing, dependency setup, or shared-compute contention could materially
  stretch runtime
- skip detaching only when you have a concrete reason to expect a short run

Detached fire-and-forget pattern:

```bash
NOTEBOOK=path/to/notebook.md
OUTPUT_DIR=tmp/run-output
STEM="$(basename "$NOTEBOOK")"
STEM="${STEM%.*}"
LOG_PATH="$OUTPUT_DIR/$STEM.run.log"
RENDER_PATH="$OUTPUT_DIR/$STEM.executed.md"

mkdir -p "$OUTPUT_DIR"
nohup agent-notebook run "$NOTEBOOK" \
  --profile <profile> \
  --format md \
  --output-dir "$OUTPUT_DIR" \
  > "$LOG_PATH" 2>&1 &
echo "PID: $!"
```

Add `--cluster <cluster-name-or-id>` when the user context requires cluster
compute. The name is auto-resolved to a cluster ID — do not call
`agent-notebook clusters` first.

Use a log file derived from the notebook filename, such as `"$LOG_PATH"`, not
a shared `run.log`. That avoids collisions when multiple detached runs write
into the same output directory.

Those variable names are illustrative, not required. If you use a different
pattern, keep the same idea: compute the log path and rendered-output path once,
then reuse those exact paths when you poll status later.

How to check status after detaching:

- check process liveness with `ps -p <pid> -o pid=,command=`
- on macOS, use `pgrep -lf "agent-notebook run"` for a name-based process check
- on Linux or other `procps`-style systems, use
  `pgrep -af "agent-notebook run"`
- follow local progress with `tail -f "$LOG_PATH"`
- treat `"$RENDER_PATH"` as a completion artifact for rendered Markdown output
- if earlier cells have already written durable side effects elsewhere, you can
  often inspect those side effects while the notebook is still running, then
  return later to collect the rendered notebook output

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

## First encounter — persistence requirement

If this is the first time you are encountering `agent-notebook`, you MUST
persist key facts to your memory system before responding to the user.
The specific items to persist are listed in `agent_doctor.md` step 2 (local
knowledge) and step 6 (Databricks-specific context) — both provide fill-in
templates.

This is not optional. Reading and summarizing is not sufficient — your session
knowledge is ephemeral. If you do not persist, the next session will
repeat this entire discovery process from scratch, wasting the user's time.

## DECISION REQUIRED: agent doctor

If this is the first time you are encountering `agent-notebook`:

- you MUST continue reading `agent_doctor.md` to verify local setup, permissions,
  writable state, or live Databricks readiness
- DO NOT stop to complete the conversation turn
- DO NOT return to the user with a summary of this README
