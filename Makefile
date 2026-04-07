.PHONY: install test test.unit test.integration.fast test.integration.slow test.integration.spark \
        test.integration.spark.scala test.integration.databricks test.all help

## Install globally from this worktree (always rebuilds from source)
install:
	uv tool install --force --reinstall --from "./python[local-spark]" databricks-agent-notebooks

## Default: unit + fast integration (no side effects, no downloads)
test:
	cd python && uv run --extra dev pytest tests/unit tests/integration -m "not slow"

## Unit tests only
test.unit:
	cd python && uv run --extra dev pytest tests/unit

## Fast integration: CLI smoke, doctor, render (seconds, no side effects)
test.integration.fast:
	cd python && uv run --extra dev pytest tests/integration -m "integration and not slow"

## Slow integration: kernel lifecycle, global install (minutes, downloads artifacts)
## Locally uses isolated temp dir by default. Set AGENT_NOTEBOOK_TEST_MACHINE_INSTALL=1 for real location.
## Agents: run outside sandbox (dangerouslyDisableSandbox) — writes to ~/Library/Application Support/, ~/Library/Caches/
test.integration.slow:
	cd python && uv run --extra dev pytest tests/integration -m "slow and not spark and not databricks"

## Local Spark: notebook execution with local PySpark (requires pyspark installed)
## Agents: run outside sandbox (dangerouslyDisableSandbox) — executes notebooks via agent-notebook run
test.integration.spark:
	cd python && uv run --extra dev pytest tests/integration -m "spark and not scala"

## Scala local Spark: Scala notebook execution with local Spark via $ivy (requires Java, coursier, Almond kernel)
## Agents: run outside sandbox (dangerouslyDisableSandbox) — executes notebooks via agent-notebook run
test.integration.spark.scala:
	cd python && uv run --extra dev pytest tests/integration -m "spark and scala"

## Live Databricks: notebook execution against real workspace (requires credentials)
## Locally: uses DEFAULT profile. CI: uses DATABRICKS_HOST/DATABRICKS_TOKEN secrets.
## Agents: run outside sandbox (dangerouslyDisableSandbox) — executes notebooks via agent-notebook run
test.integration.databricks:
	cd python && uv run --extra dev pytest tests/integration -m databricks

## Everything (unit + all integration tiers)
## Agents: run outside sandbox (dangerouslyDisableSandbox) — includes Databricks-facing tests
test.all:
	cd python && uv run --extra dev pytest

## Show available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'
	@echo ""
	@grep -E '^[a-z].*:' Makefile | sed 's/:.*//; s/^/  make /'
