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
from databricks_agent_notebooks.runtime.inventory import (
    materialize_runtime_installation,
    runtime_receipt_path as managed_runtime_receipt_path,
)
from databricks_agent_notebooks.runtime.launcher import LAUNCHER_MODULE, build_launcher_argv
from databricks_agent_notebooks.runtime.manifest import (
    KernelArtifactReceipt,
    LauncherKernelContract,
    RuntimeInstallReceipt,
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
    launcher_path: str | None = None
    runtime_id: str | None = None
    runtime_receipt_path: Path | None = None


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


def _normalize_bootstrap_argv(argv: object) -> list[str]:
    normalized = [str(part) for part in argv] if isinstance(argv, list) else []
    if not normalized:
        raise RuntimeError("installed kernel.json did not contain a bootstrap argv")
    if ADD_OPENS_FLAG not in normalized:
        normalized.insert(1, ADD_OPENS_FLAG)
    return normalized


def _normalize_kernel_env(env: object) -> dict[str, str]:
    if not isinstance(env, dict):
        return {}
    return {str(key): str(value) for key, value in env.items() if str(key) != "SPARK_HOME"}


def _paths_refer_to_same_location(left: Path | str, right: Path | str) -> bool:
    left_path = Path(left)
    right_path = Path(right)
    try:
        if left_path.exists() and right_path.exists():
            return left_path.samefile(right_path)
    except OSError:
        pass
    return left_path.resolve() == right_path.resolve()


def _contract_for_kernel(
    contract_path: Path,
    *,
    kernel_id: str,
    display_name: str,
    language: str,
    env: dict[str, str],
    bootstrap_argv: list[str],
    runtime_receipt: RuntimeInstallReceipt,
) -> LauncherKernelContract:
    launcher_argv = build_launcher_argv(contract_path)
    return LauncherKernelContract(
        contract_version=_CONTRACT_VERSION,
        kernel_id=kernel_id,
        display_name=display_name,
        language=language,
        argv=launcher_argv,
        env=env,
        runtime_id=runtime_receipt.runtime_id,
        runtime_receipt_path=str(Path(runtime_receipt.install_root) / "runtime-receipt.json"),
        launcher_path=launcher_argv[0],
        bootstrap_argv=bootstrap_argv,
    )


def _write_kernel_json(
    kernel_dir: Path,
    *,
    contract_path: Path,
    receipt_path: Path,
) -> dict[str, object]:
    kernel_json_path = kernel_dir / "kernel.json"
    data = json.loads(kernel_json_path.read_text(encoding="utf-8"))
    normalized_env = _normalize_kernel_env(data.get("env"))
    data["argv"] = build_launcher_argv(contract_path)
    if normalized_env:
        data["env"] = normalized_env
    else:
        data.pop("env", None)

    metadata = _kernel_metadata(data)
    metadata["launcher_contract_path"] = str(contract_path)
    metadata["receipt_path"] = str(receipt_path)

    kernel_json_path.write_text(json.dumps(data, indent=1) + "\n", encoding="utf-8")
    return data


def _launcher_path_from_contract(contract_path: Path | None) -> str | None:
    if contract_path is None or not contract_path.is_file():
        return None
    try:
        return read_json_record(contract_path, LauncherKernelContract).launcher_path
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None


def _runtime_identity_from_artifacts(
    contract_path: Path | None,
    receipt_path: Path | None,
) -> tuple[str | None, Path | None]:
    runtime_id: str | None = None
    runtime_receipt_path: Path | None = None

    if contract_path is not None and contract_path.is_file():
        try:
            contract = read_json_record(contract_path, LauncherKernelContract)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
        else:
            runtime_id = contract.runtime_id
            runtime_receipt_path = Path(contract.runtime_receipt_path)

    if receipt_path is not None and receipt_path.is_file():
        try:
            receipt = read_json_record(receipt_path, KernelArtifactReceipt)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
        else:
            runtime_id = runtime_id or receipt.runtime_id
            if runtime_receipt_path is None:
                runtime_receipt_path = Path(receipt.runtime_receipt_path)

    return runtime_id, runtime_receipt_path


def load_launcher_contract(kernel_dir: Path) -> LauncherKernelContract | None:
    """Load the launcher contract referenced by a generated kernelspec."""
    contract_path, _receipt_path = _kernel_metadata_paths(_read_kernel_json(kernel_dir))
    if contract_path is None or not contract_path.is_file():
        return None
    try:
        return read_json_record(contract_path, LauncherKernelContract)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None


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
    """Install the managed Almond kernel and rewrite its ``kernel.json``."""
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

    kernel_dir = (target_dir / kernel_id).resolve()
    contract_path = (kernel_dir / CONTRACT_FILENAME).resolve()
    receipt_path = _receipt_path(home.root, kernel_id).resolve()
    runtime_receipt = materialize_runtime_installation(home)
    runtime_receipt_path = managed_runtime_receipt_path(home, runtime_receipt.runtime_id)
    existing_data = _read_kernel_json(kernel_dir)
    bootstrap_argv = _normalize_bootstrap_argv(existing_data.get("argv"))
    kernel_env = _normalize_kernel_env(existing_data.get("env"))
    display_name_value = str(existing_data.get("display_name", display_name))
    language_value = str(existing_data.get("language", "scala"))
    data = patch_kernel_json(kernel_dir, contract_path=contract_path, receipt_path=receipt_path)
    installed_at = _installed_at_timestamp()
    contract = _contract_for_kernel(
        contract_path,
        kernel_id=kernel_id,
        display_name=display_name_value,
        language=language_value,
        env=kernel_env,
        bootstrap_argv=bootstrap_argv,
        runtime_receipt=runtime_receipt,
    )

    write_json_record(
        contract_path,
        contract,
    )
    write_json_record(
        receipt_path,
        KernelArtifactReceipt(
            receipt_version=_RECEIPT_VERSION,
            kernel_id=kernel_id,
            display_name=str(data.get("display_name", display_name_value)),
            language=str(data.get("language", language_value)),
            install_dir=str(kernel_dir),
            runtime_id=runtime_receipt.runtime_id,
            runtime_receipt_path=str(runtime_receipt_path),
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
            runtime_id, runtime_receipt_path = _runtime_identity_from_artifacts(contract_path, receipt_path)
            kernels.append(
                InstalledKernel(
                    name=child.name,
                    directory=child,
                    source=source,
                    launcher_contract_path=contract_path,
                    receipt_path=receipt_path,
                    launcher_path=_launcher_path_from_contract(contract_path),
                    runtime_id=runtime_id,
                    runtime_receipt_path=runtime_receipt_path,
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
) -> dict[str, object]:
    """Rewrite an installed kernelspec onto the managed launcher boundary."""
    kernel_dir = kernel_dir.resolve()
    resolved_contract_path = (contract_path or (kernel_dir / CONTRACT_FILENAME)).resolve()
    metadata_contract_path, metadata_receipt_path = _kernel_metadata_paths(_read_kernel_json(kernel_dir))
    if metadata_contract_path is not None:
        metadata_contract_path = metadata_contract_path.resolve()
    if metadata_receipt_path is not None:
        metadata_receipt_path = metadata_receipt_path.resolve()
    resolved_receipt_path = receipt_path or metadata_receipt_path
    if resolved_receipt_path is None:
        raise ValueError("receipt_path is required when rewriting kernel.json")
    resolved_receipt_path = resolved_receipt_path.resolve()
    return _write_kernel_json(
        kernel_dir,
        contract_path=resolved_contract_path or metadata_contract_path or (kernel_dir / CONTRACT_FILENAME).resolve(),
        receipt_path=resolved_receipt_path,
    )


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
    argv = [str(part) for part in data.get("argv", [])]
    if len(argv) < 3 or argv[1:3] != ["-m", LAUNCHER_MODULE]:
        issues.append("kernel.json argv does not point at the managed launcher boundary")

    kernel_env = data.get("env", {})
    if isinstance(kernel_env, dict) and "SPARK_HOME" in kernel_env:
        issues.append("SPARK_HOME should not be set in kernel.json env; launcher owns runtime env")

    contract_path, receipt_path = _kernel_metadata_paths(data)
    contract: LauncherKernelContract | None = None
    runtime_receipt_path: Path | None = None
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
            if contract.launcher_path != argv[0]:
                issues.append("launcher contract launcher_path does not match kernel.json argv[0]")
            runtime_receipt_path = Path(contract.runtime_receipt_path)
            if not contract.bootstrap_argv:
                issues.append("launcher contract bootstrap_argv missing")
            elif ADD_OPENS_FLAG not in contract.bootstrap_argv:
                issues.append(f"Required JVM flag missing from launcher bootstrap argv: {ADD_OPENS_FLAG}")
            if not contract.runtime_id:
                issues.append("launcher contract runtime_id missing")
            if not contract.runtime_receipt_path:
                issues.append("launcher contract runtime_receipt_path missing")
            elif not runtime_receipt_path.is_file():
                issues.append(f"runtime receipt not found: {runtime_receipt_path}")

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
            if not _paths_refer_to_same_location(kernel_dir, receipt.install_dir):
                issues.append("kernel receipt install_dir does not match kernel directory")
            if contract is not None and receipt.runtime_id != contract.runtime_id:
                issues.append("kernel receipt runtime_id does not match launcher contract")
            if runtime_receipt_path is not None and not _paths_refer_to_same_location(
                runtime_receipt_path,
                receipt.runtime_receipt_path,
            ):
                issues.append("kernel receipt runtime_receipt_path does not match launcher contract")
            if contract_path is not None and not _paths_refer_to_same_location(
                contract_path,
                receipt.launcher_contract_path,
            ):
                issues.append("kernel receipt launcher_contract_path does not match kernel metadata")

    if runtime_receipt_path is not None and runtime_receipt_path.is_file():
        try:
            runtime_receipt = read_json_record(runtime_receipt_path, RuntimeInstallReceipt)
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            issues.append(f"runtime receipt invalid: {exc}")
        else:
            if contract is not None and runtime_receipt.runtime_id != contract.runtime_id:
                issues.append("runtime receipt runtime_id does not match launcher contract")
            expected_runtime_root = runtime_receipt_path.parent
            if not _paths_refer_to_same_location(expected_runtime_root, runtime_receipt.install_root):
                issues.append("runtime receipt install_root does not match runtime directory")

    return issues
