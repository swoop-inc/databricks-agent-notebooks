"""Databricks Connect runtime modeling and materialization helpers."""

from __future__ import annotations

import configparser
import json
import os
import subprocess
import re
from dataclasses import dataclass
from pathlib import Path
import sys
from collections.abc import Mapping

from databricks_agent_notebooks import __version__
from databricks_agent_notebooks.integrations.databricks.clusters import Cluster, resolve_cluster_databricks_line
from databricks_agent_notebooks.runtime.home import RuntimeHome, ensure_runtime_home
from databricks_agent_notebooks.runtime.inventory import (
    current_python_line,
    materialize_runtime_installation,
    runtime_receipt_path,
    runtime_id_for,
)

_LINE_RE = re.compile(r"(?P<major>\d+)\.(?P<minor>\d+)")
SERVERLESS_CONNECT_OVERRIDE_ENV_VAR = "DATABRICKS_AGENT_NOTEBOOKS_SERVERLESS_CONNECT_LINE"
SERVERLESS_RUNTIME_CACHE_FILENAME = "serverless-runtime-policy-cache.json"
_SERVERLESS_RUNTIME_CACHE_VERSION = "1"
_DEFAULT_SERVERLESS_CONNECT_LINES = ("16.4",)


def _normalize_major_minor_line(value: str, *, label: str) -> str:
    match = _LINE_RE.search(value)
    if match is None:
        raise ValueError(f"Unable to determine {label} line from {value!r}")
    return f"{int(match.group('major'))}.{int(match.group('minor'))}"


def normalize_databricks_runtime_line(value: str) -> str:
    """Normalize a DBR line or spark_version string to ``major.minor``."""
    return _normalize_major_minor_line(value, label="Databricks Runtime")


def normalize_connect_line(value: str) -> str:
    """Normalize a Databricks Connect version or line to ``major.minor``."""
    return _normalize_major_minor_line(value, label="Databricks Connect")


@dataclass(frozen=True)
class ConnectRuntimeSpec:
    """Identity for a managed Databricks Connect runtime."""

    databricks_line: str
    connect_line: str
    python_line: str
    runtime_id: str

    @classmethod
    def for_cluster(cls, *, databricks_line: str, python_line: str) -> "ConnectRuntimeSpec":
        normalized_dbr_line = normalize_databricks_runtime_line(databricks_line)
        normalized_connect_line = normalize_connect_line(normalized_dbr_line)
        return cls(
            databricks_line=normalized_dbr_line,
            connect_line=normalized_connect_line,
            python_line=python_line,
            runtime_id=runtime_id_for(normalized_connect_line, python_line),
        )

    @classmethod
    def for_serverless(cls, *, connect_line: str, python_line: str) -> "ConnectRuntimeSpec":
        normalized_connect_line = normalize_connect_line(connect_line)
        return cls(
            databricks_line=normalized_connect_line,
            connect_line=normalized_connect_line,
            python_line=python_line,
            runtime_id=runtime_id_for(normalized_connect_line, python_line),
        )


@dataclass(frozen=True)
class ManagedConnectRuntime:
    """Materialized runtime location for Databricks Connect execution."""

    runtime_id: str
    databricks_line: str
    connect_line: str
    python_line: str
    install_root: Path
    python_executable: Path


class ServerlessRuntimeValidationError(RuntimeError):
    """Raised when a serverless runtime validates as incompatible for the target workspace."""


def _runtime_install_root(home: RuntimeHome, runtime_id: str) -> Path:
    return home.runtimes_dir / runtime_id


def _runtime_python_executable(install_root: Path) -> Path:
    if sys.platform == "win32":
        return install_root / "venv" / "Scripts" / "python.exe"
    return install_root / "venv" / "bin" / "python"


def _default_package_install_target() -> list[str]:
    # Development install: editable from source tree
    package_root = Path(__file__).resolve().parents[3]
    if (package_root / "pyproject.toml").is_file():
        return ["-e", str(package_root)]

    # Global/tool install: find original source via distribution metadata
    try:
        from importlib.metadata import distribution
        dist = distribution("databricks-agent-notebooks")
        direct_url_text = dist.read_text("direct_url.json")
        if direct_url_text:
            url_info = json.loads(direct_url_text)
            url = url_info.get("url", "")
            if url.startswith("file://"):
                source_path = Path(url.removeprefix("file://"))
                if (source_path / "pyproject.toml").is_file():
                    return [str(source_path)]
    except Exception:
        pass

    if __version__ == "0.0.0-dev":
        raise RuntimeError(
            "Cannot determine a valid install target for databricks-agent-notebooks: "
            "package metadata is unavailable and no source tree was found. "
            "Install the package first ('uv pip install -e .') or set PYTHONPATH to the source tree."
        )
    return [f"databricks-agent-notebooks=={__version__}"]


def _resolve_environ(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    env_map = dict(os.environ)
    if environ is not None:
        env_map.update(environ)
    return env_map


def _resolve_databricks_cfg_path(environ: Mapping[str, str] | None = None) -> Path:
    env_map = _resolve_environ(environ)
    override = env_map.get("DATABRICKS_CONFIG_FILE")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".databrickscfg"


def _normalize_workspace_host(value: str | None) -> str:
    if not value:
        return "unknown"
    return value.strip().rstrip("/").lower() or "unknown"


def _resolve_profile_name(profile: str | None, *, environ: Mapping[str, str] | None = None) -> str | None:
    env_map = _resolve_environ(environ)
    return profile or env_map.get("DATABRICKS_CONFIG_PROFILE") or None


def _resolve_workspace_host(profile: str | None, *, environ: Mapping[str, str] | None = None) -> str:
    env_map = _resolve_environ(environ)
    host_override = env_map.get("DATABRICKS_HOST")
    if host_override:
        return _normalize_workspace_host(host_override)

    cfg_path = _resolve_databricks_cfg_path(env_map)
    if not cfg_path.is_file():
        return "unknown"

    config = configparser.ConfigParser()
    config.read(cfg_path, encoding="utf-8")
    resolved_profile = _resolve_profile_name(profile, environ=env_map)
    if not resolved_profile or resolved_profile == config.default_section:
        return _normalize_workspace_host(config.defaults().get("host"))
    if config.has_section(resolved_profile):
        return _normalize_workspace_host(config[resolved_profile].get("host"))
    return "unknown"


def _serverless_cache_key(profile: str | None, *, environ: Mapping[str, str] | None = None) -> str:
    profile_name = _resolve_profile_name(profile, environ=environ) or "DEFAULT"
    host = _resolve_workspace_host(profile, environ=environ)
    return f"profile:{profile_name}|host:{host}"


def _serverless_cache_path(home: RuntimeHome) -> Path:
    return home.config_dir / SERVERLESS_RUNTIME_CACHE_FILENAME


def _load_serverless_runtime_cache(home: RuntimeHome) -> dict[str, str]:
    path = _serverless_cache_path(home)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if payload.get("version") != _SERVERLESS_RUNTIME_CACHE_VERSION:
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}
    return {
        str(key): normalize_connect_line(str(value))
        for key, value in entries.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def _write_serverless_runtime_cache(home: RuntimeHome, entries: Mapping[str, str]) -> Path:
    path = _serverless_cache_path(home)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": _SERVERLESS_RUNTIME_CACHE_VERSION,
        "entries": dict(sorted(entries.items())),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _serverless_connect_candidates(
    profile: str | None,
    *,
    home: RuntimeHome,
    environ: Mapping[str, str] | None = None,
) -> tuple[list[str], str | None]:
    env_map = _resolve_environ(environ)
    override = env_map.get(SERVERLESS_CONNECT_OVERRIDE_ENV_VAR)
    if override:
        return [normalize_connect_line(override)], None

    cache = _load_serverless_runtime_cache(home)
    cached_line = cache.get(_serverless_cache_key(profile, environ=env_map))
    candidates: list[str] = []
    if cached_line is not None:
        candidates.append(cached_line)
    for connect_line in _DEFAULT_SERVERLESS_CONNECT_LINES:
        normalized_line = normalize_connect_line(connect_line)
        if normalized_line not in candidates:
            candidates.append(normalized_line)
    return candidates, cached_line


def validate_serverless_runtime(
    runtime: ManagedConnectRuntime,
    *,
    profile: str | None,
    subprocess_run=subprocess.run,
) -> None:
    """Validate that the materialized runtime can open a serverless session."""
    builder = "DatabricksSession.builder"
    if profile:
        builder += f".profile({profile!r})"

    code = "\n".join(
        [
            "from databricks.connect import DatabricksSession",
            f"spark = {builder}.serverless().getOrCreate()",
            "spark.range(1).count()",
        ]
    )
    try:
        subprocess_run(
            [str(runtime.python_executable), "-c", code],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        message = f"serverless validation failed for Databricks Connect {runtime.connect_line}"
        if detail:
            message += f": {detail}"
        raise ServerlessRuntimeValidationError(message) from exc


def materialize_managed_runtime(
    home: RuntimeHome,
    *,
    spec: ConnectRuntimeSpec,
    subprocess_run=subprocess.run,
    package_install_target: list[str] | None = None,
) -> ManagedConnectRuntime:
    """Create or reuse the Python environment for a managed Connect line."""
    resolved_home = ensure_runtime_home(home)
    install_root = _runtime_install_root(resolved_home, spec.runtime_id)
    python_executable = _runtime_python_executable(install_root)
    receipt_path = runtime_receipt_path(resolved_home, spec.runtime_id)

    venv_just_created = False
    if not python_executable.is_file():
        venv_dir = python_executable.parent.parent
        install_root.mkdir(parents=True, exist_ok=True)
        subprocess_run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
        venv_just_created = True

    if venv_just_created or not receipt_path.is_file():
        subprocess_run([str(python_executable), "-m", "pip", "install", "--upgrade", "pip"], check=True)
        subprocess_run(
            [
                str(python_executable),
                "-m",
                "pip",
                "install",
                *(package_install_target or _default_package_install_target()),
                f"databricks-connect=={spec.connect_line}.*",
            ],
            check=True,
        )

    materialize_runtime_installation(
        resolved_home,
        databricks_line=spec.databricks_line,
        python_line=spec.python_line,
    )
    return ManagedConnectRuntime(
        runtime_id=spec.runtime_id,
        databricks_line=spec.databricks_line,
        connect_line=spec.connect_line,
        python_line=spec.python_line,
        install_root=install_root,
        python_executable=python_executable,
    )


def ensure_serverless_runtime(
    *,
    profile: str | None,
    home: RuntimeHome,
    python_line: str | None = None,
    environ: Mapping[str, str] | None = None,
    subprocess_run=subprocess.run,
    package_install_target: list[str] | None = None,
    materialize_runtime=materialize_managed_runtime,
    validate_runtime=validate_serverless_runtime,
) -> ManagedConnectRuntime:
    """Resolve a conservative serverless runtime, validate it once, and cache the winner."""
    resolved_home = ensure_runtime_home(home)
    env_map = _resolve_environ(environ)
    effective_profile = _resolve_profile_name(profile, environ=env_map)
    candidates, cached_line = _serverless_connect_candidates(effective_profile, home=resolved_home, environ=env_map)
    cache_key = _serverless_cache_key(effective_profile, environ=env_map)
    cache = _load_serverless_runtime_cache(resolved_home)
    attempted: list[str] = []
    errors: list[str] = []
    override_active = SERVERLESS_CONNECT_OVERRIDE_ENV_VAR in env_map

    for connect_line in candidates:
        attempted.append(connect_line)
        runtime = materialize_runtime(
            resolved_home,
            spec=ConnectRuntimeSpec.for_serverless(
                connect_line=connect_line,
                python_line=python_line or current_python_line(),
            ),
            subprocess_run=subprocess_run,
            package_install_target=package_install_target,
        )
        if cached_line == connect_line and not override_active:
            return runtime
        try:
            validate_runtime(runtime, profile=effective_profile, subprocess_run=subprocess_run)
        except ServerlessRuntimeValidationError as exc:
            errors.append(str(exc))
            if override_active:
                break
            continue

        if not override_active:
            cache[cache_key] = connect_line
            _write_serverless_runtime_cache(resolved_home, cache)
        return runtime

    attempted_lines = ", ".join(attempted) or "none"
    message = f"unable to validate a serverless Databricks Connect runtime from candidate lines: {attempted_lines}"
    if errors:
        message += f" ({errors[-1]})"
    if override_active:
        message += f"; set by {SERVERLESS_CONNECT_OVERRIDE_ENV_VAR}"
    raise RuntimeError(message)


def ensure_cluster_runtime(
    cluster: Cluster,
    *,
    home: RuntimeHome,
    python_line: str | None = None,
    subprocess_run=subprocess.run,
    package_install_target: list[str] | None = None,
) -> ManagedConnectRuntime:
    """Resolve and materialize the managed runtime required by a cluster."""
    spec = ConnectRuntimeSpec.for_cluster(
        databricks_line=resolve_cluster_databricks_line(cluster),
        python_line=python_line or current_python_line(),
    )
    return materialize_managed_runtime(
        home,
        spec=spec,
        subprocess_run=subprocess_run,
        package_install_target=package_install_target,
    )
