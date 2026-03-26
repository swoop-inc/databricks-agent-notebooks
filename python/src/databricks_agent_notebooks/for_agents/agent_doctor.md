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

You now have enough information to persist to memory. Do this NOW, before
proceeding to Databricks probes — those may require user input, and agents
frequently stop at interaction gates without persisting.

Save a memory containing at minimum:

- **Tool:** `agent-notebook` at `<which agent-notebook path>`
- **Agent docs:** `<this directory's absolute path>`
- **Local readiness:** doctor check pass/fail per check, versions discovered (runtimes, kernel, Java)
- **Scala support:** both cluster-backed and serverless; serverless defaults to Scala 2.13
- **Operational pattern:** detached `nohup` execution for cluster-targeted runs

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

Packaged examples live under `examples/smoke/`:

- `python_select_one.md`
- `scala_select_one.md`

Both are intentionally small and non-mutating. They already encode the
language, so you do not need to create new `.py` or `.scala` files first.

### Serverless Python

This is the default Python smoke path because serverless should usually start quickly and have available resources for a quick notebook.

```bash
agent-notebook run <installed-for_agents>/examples/smoke/python_select_one.md \
  --profile <profile> \
  --output-dir <writable-output-root> \
  --format md
```

### Cluster-targeted Python

If the user requested cluster-targeted Python execution in step 4, use a detached run:

```bash
NOTEBOOK=<installed-for_agents>/examples/smoke/python_select_one.md
OUTPUT_DIR=<writable-output-root>
STEM="$(basename "$NOTEBOOK")"
STEM="${STEM%.*}"

mkdir -p "$OUTPUT_DIR"
nohup agent-notebook run "$NOTEBOOK" \
  --profile <profile> \
  --cluster <cluster-name-or-id-from-strong-context-or-user> \
  --output-dir "$OUTPUT_DIR" \
  --format md \
  > "$OUTPUT_DIR/$STEM.run.log" 2>&1 &
echo "PID: $!"
```

### Scala

If the user confirmed Scala notebook work in step 4, verify Scala execution:

```bash
NOTEBOOK=<installed-for_agents>/examples/smoke/scala_select_one.md
OUTPUT_DIR=<writable-output-root>
STEM="$(basename "$NOTEBOOK")"
STEM="${STEM%.*}"

mkdir -p "$OUTPUT_DIR"
nohup agent-notebook run "$NOTEBOOK" \
  --profile <profile> \
  --cluster <cluster-name-or-id-from-strong-context-or-user> \
  --no-inject-session \
  --output-dir "$OUTPUT_DIR" \
  --format md \
  > "$OUTPUT_DIR/$STEM.run.log" 2>&1 &
echo "PID: $!"
```

### Rules

- DO NOT modify the installed example notebooks!
- DO NOT generate output in the installed package path!

## 6. Persist Databricks-specific context

Intent: update the memory you saved in step 2 with Databricks-specific facts
that could only be determined after probing and smoke testing.

Update your earlier memory to include:

- **Profiles verified:** `<list profiles that passed smoke test>`
- **Default compute:** `<serverless if available, else cluster name/id>`
- **Detached execution needed:** `<yes/no, based on whether cluster compute is in use>`
- **Workspace constraints:** `<any discovered limitations>`
- **Cluster name resolution:** cluster names can be passed directly to `--cluster`
  without calling `agent-notebook clusters` first — the `run` command auto-resolves names
