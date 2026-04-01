# Future Work

## Primary (for agents)

- LOCAL_SPARK managed PySpark version: materialize a venv with a specific `pyspark` version for Python notebooks (like Databricks Connect runtime management), controlled via `AGENT_NOTEBOOK_LOCAL_SPARK_VERSION`
- SKILL
- Re-design doctor output for agents -> to go into memory
- include cell termination signals in run output to allow agents to do follow-up tasks in parallel
- support for %run magic command (local-side includes)
- streaming nbconvert output
- use standard nbformat git coordinates once [our PR](https://github.com/jupyter/nbformat/pull/427) is accepted
- **Fold `agent-nb-run.sh` into `agent-notebook run`**: the execution helper ships inside the package and is not on PATH, forcing agents to resolve its path via brittle recipes. The script exists because `run` lacks three features: tee-to-log (`--log-dir`), machine-readable status block (log path, output dir, stem on stderr at startup), and early validation (notebook exists, profile provided). Adding these to `run` directly eliminates the separate script and its discoverability problem. Backgrounding stays the agent's responsibility (environment-specific: `run_in_background`, `nohup`, PTY) — a separate `run-async` subcommand would conflate missing `run` features with agent execution patterns.
- **pyspark doctor false-positive resilience**: `check_pyspark()` now tries an actual import (not just `find_spec`), but `importlib.metadata.version()` still returns "unknown" when pyspark is installed in an isolated venv (e.g., `uv tool`). Investigate whether version detection should fall back to `pyspark.__version__` after a successful import.

## LOCAL_SPARK Scala hardening

- **CLI unit tests for LOCAL_SPARK Scala path**: kernelspec selection (Spark 3.x → 2.12, 4.x → 2.13), `JAVA_TOOL_OPTIONS` conditional (set for 3.x, not for 4.x). Currently integration-test-only, requiring Java + coursier + Almond to detect regressions. Use `monkeypatch` for env cleanup.
- **`JAVA_TOOL_OPTIONS` process-global mutation**: `cli.py` mutates `os.environ` directly and never restores. Safe today (CLI runs as standalone process) but blocks clean CLI unit tests and any embedded/library use. Fix: pass modified env via `env=` parameter to subprocess, or save/restore after execution.

## Secondary (for humans)

- support SQL cells in Python & Scala notebooks (rewrite as `display(spark.sql("...")))`)
