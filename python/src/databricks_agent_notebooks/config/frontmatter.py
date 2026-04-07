"""Notebook configuration: dataclass, frontmatter parsing, and config merging.

Provides :class:`AgentNotebookConfig` -- the unified configuration object used
across all three config surfaces (pyproject.toml, notebook frontmatter, CLI).

Frontmatter parsing reads both ``databricks:`` (legacy) and ``agent-notebook:``
keys.  When both are present, ``agent-notebook:`` values are merged over
``databricks:`` values using the same semantics as any other config layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

import yaml

from databricks_agent_notebooks._constants import LOCAL_MASTER_RE, LOCAL_SPARK_PROFILE, SERVERLESS_CLUSTER


@dataclass(frozen=True)
class AgentNotebookConfig:
    """Immutable notebook execution configuration.

    All fields default to None, meaning "not specified at this level."
    Hardcoded defaults are applied only after the full config merge.
    """

    # Connection
    profile: str | None = None
    cluster: str | None = None
    language: str | None = None

    # Libraries
    libraries: tuple[str, ...] | None = None

    # Run settings
    format: str | None = None
    timeout: int | None = None
    output_dir: str | None = None
    allow_errors: bool | None = None
    inject_session: bool | None = None
    preprocess: bool | None = None
    clean: bool | None = None
    params: dict[str, str] | None = None

    # Lifecycle hooks
    hooks: dict | None = None

    @classmethod
    def from_resolved_params(
        cls,
        resolved: dict[str, Any],
    ) -> tuple[AgentNotebookConfig, dict[str, Any]]:
        """Build a config from a flat resolved-params dict.

        Extracts keys belonging to :data:`CONFIG_KEYS`, coerces their
        types following the same rules as :func:`_parse_config_block`,
        and returns ``(config, notebook_params)`` where *notebook_params*
        is everything not in CONFIG_KEYS (minus ``environments``).

        Path resolution is NOT performed here.  TOML-sourced paths are
        resolved at parse time in :func:`load_project_source_map`;
        CLI/frontmatter paths are resolved at point of use in ``_cmd_run``.
        """
        from databricks_agent_notebooks.config.resolution import CONFIG_KEYS  # noqa: PLC0415

        config_kwargs: dict[str, Any] = {}
        notebook_params: dict[str, Any] = {}

        skip_keys = CONFIG_KEYS | {"environments"}

        for key, value in resolved.items():
            if key in CONFIG_KEYS:
                # Coerce types
                if key in _SCALAR_FIELDS:
                    if isinstance(value, str):
                        config_kwargs[key] = value
                    elif isinstance(value, (int, float)) and not isinstance(value, bool):
                        config_kwargs[key] = str(value)

                elif key == "timeout":
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        config_kwargs[key] = int(value)
                    elif isinstance(value, str):
                        try:
                            config_kwargs[key] = int(value)
                        except (ValueError, TypeError):
                            try:
                                config_kwargs[key] = int(float(value))
                            except (ValueError, TypeError):
                                pass

                elif key in ("allow_errors", "inject_session", "preprocess", "clean"):
                    if isinstance(value, bool):
                        config_kwargs[key] = value
                    elif isinstance(value, int):
                        config_kwargs[key] = bool(value)
                    elif isinstance(value, str):
                        if value.lower() in ("true", "1"):
                            config_kwargs[key] = True
                        elif value.lower() in ("false", "0"):
                            config_kwargs[key] = False

                elif key == "libraries":
                    if isinstance(value, (list, tuple)):
                        config_kwargs[key] = tuple(str(v) for v in value)
                    elif isinstance(value, str):
                        config_kwargs[key] = tuple(
                            s.strip() for s in value.split(",") if s.strip()
                        )

                elif key == "hooks":
                    if isinstance(value, dict):
                        config_kwargs[key] = value

            elif key not in skip_keys:
                notebook_params[key] = value

        return cls(**config_kwargs), notebook_params

    def with_defaults(self, **defaults: Any) -> AgentNotebookConfig:
        """Return a new instance with None fields filled from *defaults*."""
        changes = {}
        for key, value in defaults.items():
            if getattr(self, key) is None:
                changes[key] = value
        return replace(self, **changes) if changes else self


# Backward-compatible alias so existing imports keep working during migration.
DatabricksConfig = AgentNotebookConfig


_FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---", re.DOTALL)

# Fields parsed as scalars (str or None).
_SCALAR_FIELDS = {"profile", "cluster", "language", "format", "output_dir"}
# Note: "output-dir" in config files maps to "output_dir" on the dataclass.
_KEY_TO_FIELD = {"output-dir": "output_dir", "inject-session": "inject_session",
                 "allow-errors": "allow_errors"}

# All valid AgentNotebookConfig field names (used to skip unknown keys silently).
_VALID_FIELDS = frozenset(f.name for f in fields(AgentNotebookConfig))


def _parse_config_block(block: dict[str, Any]) -> AgentNotebookConfig:
    """Parse a config dict (from frontmatter YAML or TOML) into AgentNotebookConfig.

    Handles type checking for all field types:
    - Strings: profile, cluster, language, format, output-dir
    - Integer: timeout
    - Booleans: allow-errors, inject-session, preprocess, clean
    - List of strings: libraries
    - Dict of strings: params

    Unknown keys are silently ignored.  Hyphenated keys (``inject-session``)
    are mapped to their underscored dataclass field names.
    """
    kwargs: dict[str, Any] = {}

    for raw_key, value in block.items():
        field_name = _KEY_TO_FIELD.get(raw_key, raw_key.replace("-", "_"))
        if field_name not in _VALID_FIELDS:
            continue

        if field_name in _SCALAR_FIELDS:
            if isinstance(value, str):
                kwargs[field_name] = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                # TOML numbers parsed as int/float -- coerce to str for format/profile
                kwargs[field_name] = str(value)

        elif field_name == "timeout":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                kwargs[field_name] = int(value)

        elif field_name in ("allow_errors", "inject_session", "preprocess", "clean"):
            if isinstance(value, bool):
                kwargs[field_name] = value

        elif field_name == "libraries":
            if isinstance(value, list) and all(isinstance(item, str) for item in value):
                kwargs[field_name] = tuple(value)

        elif field_name == "params":
            if isinstance(value, dict) and all(
                isinstance(k, str) and isinstance(v, str) for k, v in value.items()
            ):
                kwargs[field_name] = dict(value)

        elif field_name == "hooks":
            if isinstance(value, dict) and all(
                isinstance(k, str) and isinstance(v, dict) for k, v in value.items()
            ):
                # Normalize hyphenated sub-keys (e.g. prologue-cells -> prologue_cells)
                kwargs[field_name] = {
                    lang: {sk.replace("-", "_"): sv for sk, sv in lang_dict.items()}
                    for lang, lang_dict in value.items()
                }

    return AgentNotebookConfig(**kwargs)


def parse_frontmatter(path: Path) -> AgentNotebookConfig:
    """Parse YAML frontmatter and extract notebook config.

    Reads both ``databricks:`` (legacy) and ``agent-notebook:`` keys.
    When both are present, ``agent-notebook:`` values are merged over
    ``databricks:`` values.

    Returns a default (all-None) config when:
    - The file has no frontmatter block
    - The frontmatter contains neither recognized key
    - The recognized key's value is not a mapping
    """
    text = path.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(text)
    if match is None:
        return AgentNotebookConfig()

    try:
        parsed: Any = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return AgentNotebookConfig()

    if not isinstance(parsed, dict):
        return AgentNotebookConfig()

    db_block = parsed.get("databricks")
    an_block = parsed.get("agent-notebook")

    db_config = _parse_config_block(db_block) if isinstance(db_block, dict) else AgentNotebookConfig()
    an_config = _parse_config_block(an_block) if isinstance(an_block, dict) else AgentNotebookConfig()

    return merge_config(db_config, an_config)


def _normalize_source_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Normalize hyphenated config keys to underscored in a source map.

    Top-level keys and keys inside ``environments`` dicts are normalized.
    Keys inside ``params`` are preserved as-is (user-defined names).
    """
    result: dict[str, Any] = {}
    for k, v in d.items():
        nk = k.replace("-", "_")
        if nk == "environments" and isinstance(v, dict):
            result[nk] = {
                env_name: _normalize_source_keys(env_dict) if isinstance(env_dict, dict) else env_dict
                for env_name, env_dict in v.items()
            }
        elif nk == "hooks" and isinstance(v, dict):
            # Normalize hook keys within each language block
            result[nk] = {
                lang: _normalize_source_keys(lang_dict) if isinstance(lang_dict, dict) else lang_dict
                for lang, lang_dict in v.items()
            }
        elif nk == "params":
            result[nk] = v  # preserve user-defined param keys
        else:
            result[nk] = v
    return result


def load_frontmatter_source_map(path: Path) -> dict[str, Any]:
    """Parse YAML frontmatter and return a raw source map dict.

    Reads both ``databricks:`` (legacy) and ``agent-notebook:`` keys.
    When both are present, ``agent-notebook:`` values are merged over
    ``databricks:`` values (raw dict merge, not parsed config merge).

    Returns ``{}`` when no usable frontmatter is found.
    """
    text = path.read_text(encoding="utf-8")
    match = _FRONTMATTER_RE.match(text)
    if match is None:
        return {}

    try:
        parsed: Any = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return {}

    if not isinstance(parsed, dict):
        return {}

    db_block = parsed.get("databricks")
    an_block = parsed.get("agent-notebook")

    if not isinstance(db_block, dict) and not isinstance(an_block, dict):
        return {}

    result: dict[str, Any] = {}
    if isinstance(db_block, dict):
        result.update(db_block)
    if isinstance(an_block, dict):
        result.update(an_block)
    return _normalize_source_keys(result)


def merge_config(
    base: AgentNotebookConfig,
    override: AgentNotebookConfig,
) -> AgentNotebookConfig:
    """Merge two config layers.  *override* wins for scalars/booleans.

    Merge semantics per field type:

    - **Scalars/booleans:** override wins if non-None.
    - **Libraries:** override wins if non-None (like scalars).
    - **Params:** dict merge (override keys overwrite base keys).
    """
    # Libraries: override wins (like scalars)
    if override.libraries is not None:
        merged_libraries: tuple[str, ...] | None = override.libraries
    else:
        merged_libraries = base.libraries

    # Params: dict merge
    if base.params is not None or override.params is not None:
        merged_params: dict[str, str] | None = {
            **(base.params or {}),
            **(override.params or {}),
        }
    else:
        merged_params = None

    # Hooks: override wins (like libraries)
    if override.hooks is not None:
        merged_hooks: dict | None = override.hooks
    else:
        merged_hooks = base.hooks

    return AgentNotebookConfig(
        profile=override.profile if override.profile is not None else base.profile,
        cluster=override.cluster if override.cluster is not None else base.cluster,
        language=override.language if override.language is not None else base.language,
        libraries=merged_libraries,
        format=override.format if override.format is not None else base.format,
        timeout=override.timeout if override.timeout is not None else base.timeout,
        output_dir=override.output_dir if override.output_dir is not None else base.output_dir,
        allow_errors=override.allow_errors if override.allow_errors is not None else base.allow_errors,
        inject_session=override.inject_session if override.inject_session is not None else base.inject_session,
        preprocess=override.preprocess if override.preprocess is not None else base.preprocess,
        clean=override.clean if override.clean is not None else base.clean,
        params=merged_params,
        hooks=merged_hooks,
    )


def is_local_spark(config: AgentNotebookConfig) -> bool:
    """Detect whether *config* specifies the reserved LOCAL_SPARK profile.

    Case-insensitive: ``local_spark``, ``Local_Spark``, ``LOCAL_SPARK``
    all match.
    """
    return config.profile is not None and config.profile.upper() == LOCAL_SPARK_PROFILE


def is_local_master(cluster: str | None) -> bool:
    """Return True if *cluster* is a Spark local master URL.

    Matches ``local``, ``local[*]``, ``local[4]``, ``local[4,2]``.
    Returns False for None or non-matching strings.
    """
    return cluster is not None and LOCAL_MASTER_RE.match(cluster) is not None


def is_serverless_cluster(cluster: str | None) -> bool:
    """Return True if *cluster* is the reserved SERVERLESS sentinel.

    Case-insensitive: ``serverless``, ``SERVERLESS``, ``Serverless``
    all match.
    """
    return cluster is not None and cluster.upper() == SERVERLESS_CLUSTER
