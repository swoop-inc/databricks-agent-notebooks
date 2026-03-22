# Agent Notebook Readiness Integration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Integrate the launcher bootstrap path fixes onto the runtime-receipts branch and close the symlinked-prefix verification regression without regressing receipt validation.

**Architecture:** Keep the current runtime receipt model from `71f3fcd`, port only the launcher-fix behaviors that still apply here, and fix verification by comparing filesystem identity rather than raw path strings where installs can be reached through symlinks. Drive the work test-first with targeted unit coverage for the relative install metadata path behavior and the symlink-safe verification path.

**Tech Stack:** Python, pytest, JSON schema contracts, pathlib

---

### Task 1: Add failing integration tests

**Files:**
- Modify: `python/tests/unit/test_runtime_kernel.py`
- Modify: `python/tests/unit/test_repo_layout.py`

**Step 1: Write the failing tests**

Add focused tests that cover:
- `install_kernel()` storing absolute contract and receipt metadata when `prefix` is relative
- `install_kernel()` preserving non-`SPARK_HOME` env entries in both `kernel.json` and launcher contract
- `verify_kernel()` accepting a valid install reached through a symlinked prefix when receipt and contract paths are canonicalized
- schema coverage asserting `bootstrap_argv` remains required

**Step 2: Run tests to verify they fail**

Run: `python3 -m pytest python/tests/unit/test_runtime_kernel.py -k "relative or symlink or preserves_non_spark or schema" -v`

Expected: failures showing the missing launcher-fix integration behavior and the current lexical path comparison bug in `verify_kernel()`.

### Task 2: Implement the minimal runtime fix

**Files:**
- Modify: `python/src/databricks_agent_notebooks/runtime/kernel.py`
- Modify: `contracts/launcher-kernel-contract.schema.json`

**Step 1: Port the launcher-fix behaviors that still apply**

Update kernel installation and patching so that:
- relative install inputs serialize absolute launcher contract and receipt paths
- rewritten `kernel.json` preserves non-`SPARK_HOME` env entries
- launcher contract env mirrors the rewritten `kernel.json`

**Step 2: Fix symlink-safe verification**

Replace lexical path comparisons in `verify_kernel()` with a shared helper that compares filesystem identity for existing paths and falls back safely when needed.

**Step 3: Run focused tests to verify they pass**

Run: `python3 -m pytest python/tests/unit/test_runtime_kernel.py python/tests/unit/test_repo_layout.py -v`

Expected: all targeted tests pass.

### Task 3: Full verification and integration

**Files:**
- Modify: working tree as needed from prior tasks only

**Step 1: Run unit tests**

Run: `python3 -m pytest python/tests/unit`

Expected: full unit suite passes.

**Step 2: Run CLI smoke check**

Run: `PYTHONPATH=python/src python3 -m databricks_agent_notebooks help`

Expected: help output renders successfully with exit code 0.

**Step 3: Commit and push**

Run:
- `git add contracts/launcher-kernel-contract.schema.json python/src/databricks_agent_notebooks/runtime/kernel.py python/tests/unit/test_repo_layout.py python/tests/unit/test_runtime_kernel.py docs/plans/2026-03-22-agent-notebook-readiness-integration.md`
- `git commit -m "fix: integrate launcher bootstrap path handling"`
- `git push origin codex/agent-notebook-readiness-integration`

**Step 4: Confirm CI**

Watch the branch workflow run and record the GitHub Actions run id and final status before reporting completion.
