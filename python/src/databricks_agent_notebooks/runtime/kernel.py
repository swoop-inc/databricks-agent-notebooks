"""Managed Almond kernel installation and verification."""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from databricks_agent_notebooks.runtime.home import ensure_runtime_home, resolve_runtime_home

KERNEL_ID = "scala212-dbr-connect"
KERNEL_DISPLAY_NAME = "Scala 2.12 (Databricks Connect)"
ADD_OPENS_FLAG = "--add-opens=java.base/java.nio=ALL-UNNAMED"


@dataclass(frozen=True)
class InstalledKernel:
    """A kernelspec discovered in the managed runtime home or an override dir."""

    name: str
    directory: Path
    source: str


def find_coursier() -> str | None:
    """Return the absolute path to ``coursier`` or ``cs``."""
    path = shutil.which("coursier")
    if path is not None:
        return path
    return shutil.which("cs")


def resolve_kernels_dir(
    kernels_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Resolve the kernels directory, defaulting to the managed runtime home."""
    if kernels_dir is not None:
        return kernels_dir

    home = ensure_runtime_home(resolve_runtime_home(env))
    return home.kernels_dir


def _list_search_dirs(
    kernels_dirs: list[Path] | None = None,
    env: Mapping[str, str] | None = None,
) -> list[tuple[Path, str]]:
    runtime_kernels_dir = resolve_runtime_home(env).kernels_dir
    search_dirs: list[tuple[Path, str]] = [(runtime_kernels_dir, "runtime-home")]

    for kernels_dir in kernels_dirs or []:
        search_dirs.append((kernels_dir, str(kernels_dir)))

    deduped: list[tuple[Path, str]] = []
    seen: set[Path] = set()
    for path, source in search_dirs:
        if path in seen:
            continue
        deduped.append((path, source))
        seen.add(path)
    return deduped


def install_kernel(
    kernels_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Install the managed Almond kernel and patch its ``kernel.json``."""
    target_dir = resolve_kernels_dir(kernels_dir=kernels_dir, env=env)
    coursier_bin = find_coursier()
    if coursier_bin is None:
        msg = "coursier is required. Install via: brew install coursier/formulas/coursier"
        raise RuntimeError(msg)

    target_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            coursier_bin,
            "launch",
            "--fork",
            "almond",
            "--scala",
            "2.12",
            "--",
            "--install",
            "--force",
            "--id",
            KERNEL_ID,
            "--display-name",
            KERNEL_DISPLAY_NAME,
            "--jupyter-path",
            str(target_dir),
        ],
        check=True,
    )

    kernel_dir = target_dir / KERNEL_ID
    patch_kernel_json(kernel_dir)
    return kernel_dir


def list_installed_kernels(
    kernels_dirs: list[Path] | None = None,
    env: Mapping[str, str] | None = None,
) -> list[InstalledKernel]:
    """Return installed kernels from the managed runtime home and override dirs."""
    kernels: list[InstalledKernel] = []

    for kernels_dir, source in _list_search_dirs(kernels_dirs=kernels_dirs, env=env):
        if not kernels_dir.is_dir():
            continue

        for child in sorted(kernels_dir.iterdir(), key=lambda path: path.name):
            if not child.is_dir():
                continue
            if not (child / "kernel.json").is_file():
                continue
            kernels.append(InstalledKernel(name=child.name, directory=child, source=source))

    return kernels


def remove_kernel(
    name: str,
    kernels_dirs: list[Path] | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Remove a named installed kernel from runtime-home or an override dir."""
    if not name or name in {".", ".."} or Path(name).name != name or any(sep in name for sep in ("/", "\\")):
        raise ValueError("kernel name must be a simple directory name")

    matches: list[Path] = []
    for kernels_dir, _source in _list_search_dirs(kernels_dirs=kernels_dirs, env=env):
        candidate = kernels_dir / name
        if not candidate.is_dir():
            continue
        if not (candidate / "kernel.json").is_file():
            continue
        matches.append(candidate)

    if not matches:
        raise FileNotFoundError(f"kernel not found: {name}")
    if len(matches) > 1:
        joined = ", ".join(str(match) for match in matches)
        raise RuntimeError(f"kernel '{name}' found in multiple directories: {joined}")

    shutil.rmtree(matches[0])
    return matches[0]


def patch_kernel_json(kernel_dir: Path) -> None:
    """Ensure the installed kernelspec has Databricks Connect-safe semantics."""
    kernel_json_path = kernel_dir / "kernel.json"
    data = json.loads(kernel_json_path.read_text(encoding="utf-8"))

    argv = data.setdefault("argv", [])
    if ADD_OPENS_FLAG not in argv:
        argv.insert(1, ADD_OPENS_FLAG)

    env = data.setdefault("env", {})
    env["SPARK_HOME"] = ""

    kernel_json_path.write_text(json.dumps(data, indent=1) + "\n", encoding="utf-8")


def verify_kernel(
    kernels_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    """Return validation issues for the managed kernel."""
    target_dir = resolve_kernels_dir(kernels_dir=kernels_dir, env=env)
    kernel_dir = target_dir / KERNEL_ID
    issues: list[str] = []

    if not kernel_dir.is_dir():
        issues.append(f"Kernel directory does not exist: {kernel_dir}")
        return issues

    kernel_json_path = kernel_dir / "kernel.json"
    if not kernel_json_path.is_file():
        issues.append(f"kernel.json not found in {kernel_dir}")
        return issues

    data = json.loads(kernel_json_path.read_text(encoding="utf-8"))
    argv = data.get("argv", [])
    if ADD_OPENS_FLAG not in argv:
        issues.append(f"Required JVM flag missing from argv: {ADD_OPENS_FLAG}")

    kernel_env = data.get("env", {})
    if kernel_env.get("SPARK_HOME") != "":
        issues.append("SPARK_HOME not set to empty string in kernel env")

    return issues
