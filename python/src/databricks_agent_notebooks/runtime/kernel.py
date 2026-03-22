"""Managed Almond kernel installation and verification."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from databricks_agent_notebooks.runtime.home import ensure_runtime_home, resolve_runtime_home
from databricks_agent_notebooks.runtime.manifest import (
    KernelArtifactReceipt,
    LauncherKernelContract,
    read_json_record,
    write_json_record,
)

KERNEL_ID = "scala212-dbr-connect"
KERNEL_DISPLAY_NAME = "Scala 2.12 (Databricks Connect)"
ADD_OPENS_FLAG = "--add-opens=java.base/java.nio=ALL-UNNAMED"
CONTRACT_FILENAME = "launcher-contract.json"
_TOOL_METADATA_KEY = "databricks_agent_notebooks"
_CONTRACT_VERSION = "1"
_RECEIPT_VERSION = "1"


@dataclass(frozen=True)
class InstalledKernel:
    """A kernelspec discovered in the managed runtime home or an override dir."""

    name: str
    directory: Path
    source: str
    launcher_contract_path: Path | None = None
    receipt_path: Path | None = None


def _kernel_metadata(data: dict[str, object]) -> dict[str, str]:
    metadata = data.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata

    tool_metadata = metadata.setdefault(_TOOL_METADATA_KEY, {})
    if not isinstance(tool_metadata, dict):
        tool_metadata = {}
        metadata[_TOOL_METADATA_KEY] = tool_metadata
    return tool_metadata


def _kernel_metadata_paths(data: dict[str, object]) -> tuple[Path | None, Path | None]:
    metadata = data.get("metadata", {})
    if not isinstance(metadata, dict):
        return None, None
    tool_metadata = metadata.get(_TOOL_METADATA_KEY, {})
    if not isinstance(tool_metadata, dict):
        return None, None

    contract_path = tool_metadata.get("launcher_contract_path")
    receipt_path = tool_metadata.get("receipt_path")
    return (
        Path(contract_path) if isinstance(contract_path, str) and contract_path else None,
        Path(receipt_path) if isinstance(receipt_path, str) and receipt_path else None,
    )


def _read_kernel_json(kernel_dir: Path) -> dict[str, object]:
    return json.loads((kernel_dir / "kernel.json").read_text(encoding="utf-8"))


def _user_kernels_dir() -> Path:
    home = Path.home()
    if sys.platform == "darwin":
        return home / "Library" / "Jupyter" / "kernels"
    return home / ".local" / "share" / "jupyter" / "kernels"


def _receipt_path(home_root: Path, kernel_id: str) -> Path:
    return home_root / "state" / "installations" / "kernels" / f"{kernel_id}.json"


def _installed_at_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def find_coursier() -> str | None:
    """Return the absolute path to ``coursier`` or ``cs``."""
    path = shutil.which("coursier")
    if path is not None:
        return path
    return shutil.which("cs")


def resolve_kernels_dir(
    kernels_dir: Path | None = None,
    *,
    user: bool = False,
    prefix: Path | None = None,
    sys_prefix: bool = False,
    jupyter_path: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Resolve the kernels directory, defaulting to the managed runtime home."""
    selected = sum(
        (
            kernels_dir is not None,
            user,
            prefix is not None,
            sys_prefix,
            jupyter_path is not None,
        )
    )
    if selected > 1:
        raise ValueError("select only one install location flag")
    if kernels_dir is not None:
        return kernels_dir
    if jupyter_path is not None:
        return jupyter_path
    if prefix is not None:
        return prefix / "share" / "jupyter" / "kernels"
    if sys_prefix:
        return Path(sys.prefix) / "share" / "jupyter" / "kernels"
    if user:
        return _user_kernels_dir()

    return resolve_runtime_home(env).kernels_dir


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
    kernel_id: str = KERNEL_ID,
    display_name: str = KERNEL_DISPLAY_NAME,
    kernels_dir: Path | None = None,
    *,
    user: bool = False,
    prefix: Path | None = None,
    sys_prefix: bool = False,
    jupyter_path: Path | None = None,
    force: bool = False,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Install the managed Almond kernel and patch its ``kernel.json``."""
    home = ensure_runtime_home(resolve_runtime_home(env))
    target_dir = resolve_kernels_dir(
        kernels_dir=kernels_dir,
        user=user,
        prefix=prefix,
        sys_prefix=sys_prefix,
        jupyter_path=jupyter_path,
        env=env,
    )
    coursier_bin = find_coursier()
    if coursier_bin is None:
        msg = "coursier is required. Install via: brew install coursier/formulas/coursier"
        raise RuntimeError(msg)

    target_dir.mkdir(parents=True, exist_ok=True)
    command = [
        coursier_bin,
        "launch",
        "--fork",
        "almond",
        "--scala",
        "2.12",
        "--",
        "--install",
    ]
    if force:
        command.append("--force")
    command.extend(
        [
            "--id",
            kernel_id,
            "--display-name",
            display_name,
            "--jupyter-path",
            str(target_dir),
        ]
    )

    subprocess.run(command, check=True)

    kernel_dir = target_dir / kernel_id
    contract_path = kernel_dir / CONTRACT_FILENAME
    receipt_path = _receipt_path(home.root, kernel_id)
    patch_kernel_json(kernel_dir, contract_path=contract_path, receipt_path=receipt_path)
    data = _read_kernel_json(kernel_dir)
    installed_at = _installed_at_timestamp()

    write_json_record(
        contract_path,
        LauncherKernelContract(
            contract_version=_CONTRACT_VERSION,
            kernel_id=kernel_id,
            display_name=str(data.get("display_name", display_name)),
            language=str(data.get("language", "scala")),
            argv=[str(part) for part in data.get("argv", [])],
            env={str(key): str(value) for key, value in dict(data.get("env", {})).items()},
            runtime_id=kernel_id,
            launcher_path=str(data.get("argv", [""])[0]) if data.get("argv") else "",
        ),
    )
    write_json_record(
        receipt_path,
        KernelArtifactReceipt(
            receipt_version=_RECEIPT_VERSION,
            kernel_id=kernel_id,
            display_name=str(data.get("display_name", display_name)),
            language=str(data.get("language", "scala")),
            install_dir=str(kernel_dir),
            launcher_contract_path=str(contract_path),
            installed_at=installed_at,
        ),
    )
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
            data = _read_kernel_json(child)
            contract_path, receipt_path = _kernel_metadata_paths(data)
            kernels.append(
                InstalledKernel(
                    name=child.name,
                    directory=child,
                    source=source,
                    launcher_contract_path=contract_path,
                    receipt_path=receipt_path,
                )
            )

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


def patch_kernel_json(
    kernel_dir: Path,
    contract_path: Path | None = None,
    receipt_path: Path | None = None,
) -> None:
    """Ensure the installed kernelspec has Databricks Connect-safe semantics."""
    kernel_json_path = kernel_dir / "kernel.json"
    data = json.loads(kernel_json_path.read_text(encoding="utf-8"))

    argv = data.setdefault("argv", [])
    if ADD_OPENS_FLAG not in argv:
        argv.insert(1, ADD_OPENS_FLAG)

    env = data.setdefault("env", {})
    env["SPARK_HOME"] = ""

    if contract_path is not None or receipt_path is not None:
        metadata = _kernel_metadata(data)
        if contract_path is not None:
            metadata["launcher_contract_path"] = str(contract_path)
        if receipt_path is not None:
            metadata["receipt_path"] = str(receipt_path)

    kernel_json_path.write_text(json.dumps(data, indent=1) + "\n", encoding="utf-8")


def verify_kernel(
    kernels_dir: Path | None = None,
    *,
    kernel_id: str = KERNEL_ID,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    """Return validation issues for the managed kernel."""
    target_dir = resolve_kernels_dir(kernels_dir=kernels_dir, env=env)
    kernel_dir = target_dir / kernel_id
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

    contract_path, receipt_path = _kernel_metadata_paths(data)
    if contract_path is None:
        issues.append("launcher contract metadata missing from kernel.json")
    elif not contract_path.is_file():
        issues.append(f"launcher contract not found: {contract_path}")
    else:
        try:
            contract = read_json_record(contract_path, LauncherKernelContract)
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            issues.append(f"launcher contract invalid: {exc}")
        else:
            if contract.kernel_id != kernel_id:
                issues.append(
                    f"launcher contract kernel_id mismatch: expected {kernel_id}, found {contract.kernel_id}"
                )
            if contract.argv != [str(part) for part in argv]:
                issues.append("launcher contract argv does not match kernel.json")
            if contract.env != {str(key): str(value) for key, value in dict(kernel_env).items()}:
                issues.append("launcher contract env does not match kernel.json")

    if receipt_path is None:
        issues.append("kernel receipt metadata missing from kernel.json")
    elif not receipt_path.is_file():
        issues.append(f"kernel receipt not found: {receipt_path}")
    else:
        try:
            receipt = read_json_record(receipt_path, KernelArtifactReceipt)
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            issues.append(f"kernel receipt invalid: {exc}")
        else:
            if receipt.kernel_id != kernel_id:
                issues.append(
                    f"kernel receipt kernel_id mismatch: expected {kernel_id}, found {receipt.kernel_id}"
                )
            if receipt.install_dir != str(kernel_dir):
                issues.append("kernel receipt install_dir does not match kernel directory")
            if contract_path is not None and receipt.launcher_contract_path != str(contract_path):
                issues.append("kernel receipt launcher_contract_path does not match kernel metadata")

    return issues
