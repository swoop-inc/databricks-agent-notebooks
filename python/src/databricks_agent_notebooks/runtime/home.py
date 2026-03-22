"""Runtime-home resolution for managed notebook runtimes and kernels."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from platformdirs import PlatformDirs

HOME_ENV_VAR = "DATABRICKS_AGENT_NOTEBOOKS_HOME"


@dataclass(frozen=True)
class RuntimeHome:
    """Resolved runtime-home paths for durable and disposable assets."""

    root: Path
    cache_dir: Path
    runtimes_dir: Path
    kernels_dir: Path
    installations_dir: Path
    links_dir: Path
    logs_dir: Path
    bin_dir: Path
    config_dir: Path


def resolve_runtime_home(env: Mapping[str, str] | None = None) -> RuntimeHome:
    """Resolve the managed runtime home using env override or platformdirs."""
    env_map = dict(env or {})
    override = env_map.get(HOME_ENV_VAR)
    if override:
        root = Path(override)
    else:
        root = Path(PlatformDirs("databricks-agent-notebooks", "swoop").user_data_path)

    return RuntimeHome(
        root=root,
        cache_dir=root / "cache",
        runtimes_dir=root / "data" / "runtimes",
        kernels_dir=root / "data" / "kernels",
        installations_dir=root / "state" / "installations",
        links_dir=root / "state" / "links",
        logs_dir=root / "state" / "logs",
        bin_dir=root / "bin",
        config_dir=root / "config",
    )


def ensure_runtime_home(home: RuntimeHome) -> RuntimeHome:
    """Create the managed runtime-home directory structure."""
    for path in (
        home.cache_dir,
        home.runtimes_dir,
        home.kernels_dir,
        home.installations_dir,
        home.links_dir,
        home.logs_dir,
        home.bin_dir,
        home.config_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return home
