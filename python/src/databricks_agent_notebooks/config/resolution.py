"""Parameter resolution engine with named environment support.

Merges parameters from multiple source maps (TOML, env vars, frontmatter,
CLI) into a single flat dict.  Named environments provide defaults that
explicit params override; the resolved environment name is always pinned
on top.

The algorithm follows param-layering.md:

1. **Split** -- extract ``environments`` into a registry, promote nested
   ``params`` from each source map.
2. **Merge** -- merge the flat param maps left-to-right (later wins).
3. **Resolve env** -- determine the environment spec from merged params,
   the ``default`` environment, or fall back to ``"default"``.
4. **Build env params** -- look up each name in the comma-separated spec.
5. **Final merge** -- ``env_params < merged_params < {env: spec}``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


class EnvironmentNotFoundError(Exception):
    """Raised when ``--env`` names an environment not in the registry."""


# ---------------------------------------------------------------------------
# Parameter key constants
# ---------------------------------------------------------------------------

PARAM_CLUSTER_LIST_TIMEOUT = "cluster_list_timeout"

# Agent-level defaults -- the floor beneath all config layers.
# Override via environment, pyproject.toml, env var, or frontmatter.
AGENT_DEFAULTS: dict[str, Any] = {
    PARAM_CLUSTER_LIST_TIMEOUT: 120.0,  # seconds
}


# Keys that belong to the tool's configuration (not notebook params).
# AgentNotebookConfig extracts the ones it knows; the rest are
# agent-level keys (like cluster_list_timeout) that services consume
# directly from the resolved dict.
CONFIG_KEYS: frozenset[str] = frozenset({
    "profile", "cluster", "language", "libraries", "format",
    "timeout", "output_dir", "allow_errors", "inject_session",
    "preprocess", "clean", "hooks",
    PARAM_CLUSTER_LIST_TIMEOUT,
})

# Env var prefix for the general param convention.
_ENV_VAR_PREFIX = "AGENT_NOTEBOOK_"
# Env vars with this prefix belong to the existing LOCAL_SPARK subsystem
# and are NOT collected by collect_env_vars().
_LOCAL_SPARK_PREFIX = "AGENT_NOTEBOOK_LOCAL_SPARK_"


def resolve_params(sources: list[dict[str, Any]]) -> dict[str, Any]:
    """Resolve a sequence of source maps into a flat parameter dict.

    Each source map may contain:

    - ``environments`` -- a dict of named environment configs.
    - ``params`` -- a nested param dict whose values take priority over
      sibling keys in the same source.
    - Any other keys -- treated as explicit params.

    Returns a flat dict with the ``env`` key always set to the resolved
    environment spec string.

    Raises :class:`EnvironmentNotFoundError` when the env spec names an
    environment that does not exist in the registry, *unless* the missing
    name is ``"default"`` (which silently resolves to empty env params).
    """
    # 1. Split environments from params
    environments: dict[str, dict[str, Any]] = {}
    params_without_env: list[dict[str, Any]] = []

    for m in sources:
        if "environments" in m:
            env = m["environments"]
            if isinstance(env, dict):
                environments.update(env)

        rest = {k: v for k, v in m.items() if k != "environments"}
        nested = rest.pop("params", {})
        if not isinstance(nested, dict):
            nested = {}
        params_without_env.append({**rest, **nested})

    # 2. Merge all explicit params (later wins for all types)
    merged_params: dict[str, Any] = {}
    for p in params_without_env:
        merged_params.update(p)

    # 3. Resolve environment selection
    env_spec = (
        merged_params.get("env")
        or environments.get("default", {}).get("env")
        or "default"
    )

    # 4. Build merged environment params from "env1,env2" spec
    env_params: dict[str, Any] = {}
    for name in env_spec.split(","):
        name = name.strip()
        if not name:
            continue
        if name in environments:
            env_def = dict(environments[name])
            nested = env_def.pop("params", {})
            if isinstance(nested, dict):
                env_def.update(nested)
            env_params.update(env_def)
        elif name != "default":
            raise EnvironmentNotFoundError(name)

    # 5. Final layered merge: agent defaults < env defaults < explicit params < resolved env key
    return {
        **AGENT_DEFAULTS,
        **{k: v for k, v in env_params.items() if k != "env"},
        **merged_params,
        "env": env_spec,
    }


def collect_env_vars() -> dict[str, Any]:
    """Read ``AGENT_NOTEBOOK_*`` env vars into a flat param dict.

    Convention: ``AGENT_NOTEBOOK_<UPPER_KEY>`` maps to ``<lower_key>``.
    Skips the existing ``AGENT_NOTEBOOK_LOCAL_SPARK_*`` vars (they have
    their own subsystem) and the bare ``AGENT_NOTEBOOK`` var if present.

    Values are always strings -- type coercion happens downstream.
    """
    result: dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith(_ENV_VAR_PREFIX):
            continue
        if key.startswith(_LOCAL_SPARK_PREFIX):
            continue
        suffix = key[len(_ENV_VAR_PREFIX):]
        if not suffix:
            continue
        result[suffix.lower()] = value
    return result


def resolve_from_environment(
    start_dir: Path | None = None,
    extra_sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Resolve params from project config, env vars, and optional extras."""
    from databricks_agent_notebooks.config.project import load_project_source_map

    toml_source, _ = load_project_source_map(start_dir or Path.cwd())
    sources: list[dict[str, Any]] = [toml_source, collect_env_vars()]
    if extra_sources:
        sources.extend(extra_sources)
    return resolve_params(sources)
