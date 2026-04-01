# Agent Doctor

Use this guide whenever you need to answer one of these questions:

- Is `agent-notebook` installed and usable here?
- Do I have the local permissions and writable state this workflow needs?
- Can I reach live Databricks from this execution environment?
- Should I detach a run with `nohup` even from a long-lived agent session?
- What facts from this check are worth remembering for later?

This is a repeatable readiness flow, not a one-time install checklist.

## 1. Confirm local CLI visibility

Intent: prove the installed tool is present and that at least one real local validation path runs before you spend time on live Databricks setup.

Useful probes:

```bash
agent-notebook doctor
```

Interpretation:

- Success means the packaged CLI is reachable and the bundled local doctor flow
  can execute.
- `agent-notebook doctor` runs real local checks such as coursier, managed
  kernel presence, kernel semantics, `SPARK_HOME`, and Java, and it should be
  the default local readiness command an agent starts
  with.
- A missing command or import failure means you should stop and fix the local
  installation first.

## 2. Persist local knowledge

If the README version check (above) already confirmed your stored version
matches the installed version and stored paths exist, skip to step 3.

Otherwise — whether this is a fresh install, a version mismatch, or missing
memory — persist using the template below.

The template below is the authoritative schema. Your stored memory and
instructions are a cache. If you have existing agent-notebook context, compare
each field below against your stored values: update changed values, add fields
present here but missing, and remove fields you have that this template no
longer includes. Check both your memory system and any persistent instructions
(CLAUDE.md, AGENTS.md, etc.) — stale instructions are as problematic as stale
memory.

Do this NOW, before proceeding to Databricks probes — those may require user
input, and agents frequently stop at interaction gates without persisting.

Save a memory containing at minimum:

- **Tool:** `agent-notebook` at `<which agent-notebook path>`
- **Version:** `<output of agent-notebook --version>`
- **Agent docs:** `<this directory's absolute path>`
- **Execution helper:** `<absolute path to for_agents/scripts/agent-nb-run.sh>` — resolve with:
  `python3 -c "from importlib.resources import files; print(files('databricks_agent_notebooks').joinpath('for_agents', 'scripts', 'agent-nb-run.sh'))"`
  This script is NOT on PATH — always use the full resolved path or a cached variable.
- **Local readiness:** doctor check pass/fail per check, versions discovered (runtimes, kernel, Java)
- **Scala support:** both cluster-backed and serverless; serverless defaults to Scala 2.13
- **Operational pattern:** non-blocking execution for cluster-targeted runs (see README for environment-specific pattern)

Then proceed to step 3.

## 3. Probe Databricks configuration first, then live access

Intent: separate profile-backed local preflight from a real live Databricks probe.

Useful probes:

```bash
agent-notebook doctor --profile <profile>
agent-notebook clusters --profile <profile>
```

Interpretation:

- Use profile names, environment names, and cluster names only when they come
  from strong agent context or directly from the user.
- Do not invent workspace names, profile names, cluster names, or other
  environment defaults.
- `agent-notebook doctor --profile <profile>` is the profile-backed form of the
  same umbrella doctor command. It helps confirm profile and tool
  configuration, but it is not proof of live Databricks connectivity.
- `agent-notebook clusters --profile <profile>` is the real lightweight live
  access probe in the current CLI surface. **Use it here for connectivity
  verification only.** After this doctor flow, do NOT run `clusters` again as a
  pre-step to `agent-notebook run` — the `run` command's `--cluster` flag
  auto-resolves cluster names to IDs internally. Remember this!
- In the current implementation, `agent-notebook clusters --profile <profile>`
  uses a 30-second cluster-listing timeout budget rather than waiting
  indefinitely for a slow workspace response. Note that listing clusters in
  large workspaces can generate more than a gigabyte of output.
- On large workspaces, the 30-second timeout may expire before an exact cluster
  name match is found during `run --cluster <name>`. When this happens, the
  tool returns fuzzy name suggestions from partial results. Inspect these
  suggestions — the full cluster name (e.g., `"rnd-alpha [engineering]"` instead
  of `"rnd-alpha"`) usually resolves the issue. Passing a cluster ID instead of
  a name is the deterministic path and is never subject to timeouts.
- If networking or certificate failures appear only inside the agent sandbox,
  notify the user that the command likely needs a narrower unsandboxed path.

## 4. Present findings and align with the user

Intent: surface what you learned in steps 1 and 3 and get explicit user direction before running smoke tests.

First, assess internally:

- What level of functionality has already been verified: local doctor only, profile-backed preflight, live Databricks access, or a real notebook run?
- Have any permission or sandbox issues already been identified?
- Does your shell or agent environment impose hard command time limits that make `nohup` the safer default?
- Will the smoke run likely wait on cluster start, cluster resize, cluster queueing, or busy shared compute?

Then present a brief summary of what you found (which checks passed, which failed, which workspaces responded) and ask the user the following questions. DO NOT proceed to step 5 until the user has answered:

- Which workspace(s) need smoke-run verification? List the profiles you discovered (from `~/.databrickscfg` and any profiles probed in step 3) as options. Do not ask the user to recall profile names from memory.
- Is Scala notebook work expected?
- Should smoke runs target serverless, a specific cluster, or both? Serverless is available on most workspaces by default (requires Unity Catalog and a supported region) but is not universal. Present serverless as the default fast option and cluster compute as the alternative. If the user is unsure whether their workspace supports serverless, a serverless smoke run is the cheapest way to find out.

## 5. Run a non-mutating smoke notebook

Intent: prove a real notebook run succeeds for each workspace the user selected in step 4. Every verified workspace needs at least one successful smoke run — serverless or cluster-targeted.

Ensure you have resolved `AGENT_NB_RUN` as shown in the README's "Execution helper script" section before running these examples.

Packaged examples live under `examples/smoke/`:

- `python_select_one.md`
- `scala_select_one.md`

Both are intentionally small and non-mutating. They already encode the
language, so you do not need to create new `.py` or `.scala` files first.

### Serverless Python

This is the default Python smoke path because serverless should usually start
quickly and have available resources for a quick notebook. If this is your first
run or you are unsure of timing, use the long-running pattern from the README
instead of foreground.

```bash
"$AGENT_NB_RUN" <installed-for_agents>/examples/smoke/python_select_one.md \
  --profile <profile> \
  --output-dir <writable-output-root> \
  --format md
```

### Cluster-targeted Python

If the user requested cluster-targeted Python execution in step 4, use a
non-blocking run. Cluster startup can add minutes — always use your
environment's long-running pattern (see the README "Long-running runs" section).

Use the execution helper script (resolved via `$AGENT_NB_RUN`) with your
environment's non-blocking pattern. Example using `nohup` (Claude Code, standard
shells):

```bash
nohup "$AGENT_NB_RUN" \
  <installed-for_agents>/examples/smoke/python_select_one.md \
  --profile <profile> \
  --cluster <cluster-name-or-id-from-strong-context-or-user> \
  --output-dir <writable-output-root> \
  --format md \
  > /dev/null 2>&1 &
echo "PID: $!"
```

For Claude Code, `run_in_background: true` is preferred over `nohup`. For Codex,
use a PTY session instead — `nohup` is unreliable in Codex.

### Scala

If the user confirmed Scala notebook work in step 4, verify Scala execution
using the same non-blocking pattern as cluster-targeted Python:

```bash
nohup "$AGENT_NB_RUN" \
  <installed-for_agents>/examples/smoke/scala_select_one.md \
  --profile <profile> \
  --cluster <cluster-name-or-id-from-strong-context-or-user> \
  --output-dir <writable-output-root> \
  --format md \
  > /dev/null 2>&1 &
echo "PID: $!"
```

For Claude Code, use `run_in_background: true`. For Codex, use a PTY session.

### Rules

- DO NOT modify the installed example notebooks!
- DO NOT generate output in the installed package path!

## 6. Persist Databricks-specific context

Intent: update the memory you saved in step 2 with Databricks-specific facts
that could only be determined after probing and smoke testing.

Update your earlier memory to include:

- **Profiles verified:** `<list profiles that passed smoke test>`
- **Default compute:** `<serverless if available, else cluster name/id>`
- **Non-blocking execution needed:** `<default yes; only skip for runs with predictably short duration such as known-quick serverless smoke notebooks or lightweight metadata queries>`
- **Workspace constraints:** `<any discovered limitations>`
- **Cluster name resolution:** cluster names can be passed directly to `--cluster`
  without calling `agent-notebook clusters` first — the `run` command auto-resolves names
