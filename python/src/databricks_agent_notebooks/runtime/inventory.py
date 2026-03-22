"""Managed runtime receipt materialization, inventory, and validation."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from databricks_agent_notebooks._constants import DATABRICKS_CONNECT_LINE
from databricks_agent_notebooks.runtime.home import RuntimeHome, ensure_runtime_home, resolve_runtime_home
from databricks_agent_notebooks.runtime.manifest import RuntimeInstallReceipt, read_json_record, write_json_record

RUNTIME_RECEIPT_FILENAME = "runtime-receipt.json"
_RUNTIME_KIND = "databricks-connect"
_RECEIPT_VERSION = "1"


@dataclass(frozen=True)
class InstalledRuntime:
    """A managed runtime receipt discovered under runtime-home."""

    runtime_id: str
    status: str
    databricks_line: str
    python_line: str
    receipt_path: Path
    install_root: Path


@dataclass(frozen=True)
class RuntimeInventoryCheck:
    """Result of validating one runtime inventory entry."""

    name: str
    status: str
    message: str


def runtime_id_for(databricks_line: str, python_line: str) -> str:
    """Build the stable runtime identity used under runtime-home."""
    return f"dbr-{databricks_line}-python-{python_line}"


def current_python_line() -> str:
    """Return the current interpreter's major.minor Python line."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def runtime_receipt_path(home: RuntimeHome, runtime_id: str) -> Path:
    """Return the canonical runtime receipt path for a managed runtime."""
    return home.runtimes_dir / runtime_id / RUNTIME_RECEIPT_FILENAME


def _installed_at_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def materialize_runtime_installation(
    home: RuntimeHome,
    *,
    databricks_line: str = DATABRICKS_CONNECT_LINE,
    python_line: str | None = None,
    runtime_kind: str = _RUNTIME_KIND,
    status: str = "materialized",
) -> RuntimeInstallReceipt:
    """Ensure a managed runtime receipt exists under ``data/runtimes``."""
    resolved_home = ensure_runtime_home(home)
    resolved_python_line = python_line or current_python_line()
    runtime_id = runtime_id_for(databricks_line, resolved_python_line)
    receipt_path = runtime_receipt_path(resolved_home, runtime_id)
    install_root = receipt_path.parent

    if receipt_path.is_file():
        try:
            return read_json_record(receipt_path, RuntimeInstallReceipt)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    install_root.mkdir(parents=True, exist_ok=True)
    receipt = RuntimeInstallReceipt(
        receipt_version=_RECEIPT_VERSION,
        runtime_id=runtime_id,
        runtime_kind=runtime_kind,
        databricks_line=databricks_line,
        python_line=resolved_python_line,
        install_root=str(install_root),
        installed_at=_installed_at_timestamp(),
        status=status,
    )
    write_json_record(receipt_path, receipt)
    return receipt


def _scan_runtime_receipts(home: RuntimeHome) -> list[tuple[Path, RuntimeInstallReceipt | None, Exception | None]]:
    if not home.runtimes_dir.is_dir():
        return []

    results: list[tuple[Path, RuntimeInstallReceipt | None, Exception | None]] = []
    for receipt_path in sorted(home.runtimes_dir.glob(f"*/{RUNTIME_RECEIPT_FILENAME}")):
        try:
            receipt = read_json_record(receipt_path, RuntimeInstallReceipt)
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            results.append((receipt_path, None, exc))
        else:
            results.append((receipt_path, receipt, None))
    return results


def list_installed_runtimes(
    *,
    home: RuntimeHome | None = None,
    env: Mapping[str, str] | None = None,
) -> list[InstalledRuntime]:
    """Return valid managed runtimes recorded under runtime-home."""
    resolved_home = home or resolve_runtime_home(env)
    runtimes: list[InstalledRuntime] = []

    for receipt_path, receipt, error in _scan_runtime_receipts(resolved_home):
        if error is not None or receipt is None:
            continue
        runtimes.append(
            InstalledRuntime(
                runtime_id=receipt.runtime_id,
                status=receipt.status,
                databricks_line=receipt.databricks_line,
                python_line=receipt.python_line,
                receipt_path=receipt_path,
                install_root=Path(receipt.install_root),
            )
        )

    return runtimes


def doctor_installed_runtimes(
    *,
    home: RuntimeHome | None = None,
    env: Mapping[str, str] | None = None,
) -> list[RuntimeInventoryCheck]:
    """Validate runtime receipts rooted under runtime-home."""
    resolved_home = home or resolve_runtime_home(env)
    scanned = _scan_runtime_receipts(resolved_home)
    if not scanned:
        return [
            RuntimeInventoryCheck(
                "inventory",
                "fail",
                f"no managed runtimes recorded under {resolved_home.runtimes_dir}",
            )
        ]

    checks: list[RuntimeInventoryCheck] = []
    for receipt_path, receipt, error in scanned:
        runtime_name = receipt_path.parent.name
        if error is not None or receipt is None:
            checks.append(RuntimeInventoryCheck(runtime_name, "fail", f"invalid runtime receipt: {error}"))
            continue

        expected_root = resolved_home.runtimes_dir / receipt.runtime_id
        if receipt.runtime_id != runtime_name:
            checks.append(
                RuntimeInventoryCheck(
                    receipt.runtime_id,
                    "fail",
                    f"receipt path runtime id mismatch: expected {runtime_name}, found {receipt.runtime_id}",
                )
            )
            continue
        if Path(receipt.install_root) != expected_root:
            checks.append(
                RuntimeInventoryCheck(
                    receipt.runtime_id,
                    "fail",
                    f"install_root mismatch: expected {expected_root}, found {receipt.install_root}",
                )
            )
            continue
        if not expected_root.is_dir():
            checks.append(RuntimeInventoryCheck(receipt.runtime_id, "fail", f"install_root missing: {expected_root}"))
            continue

        checks.append(
            RuntimeInventoryCheck(
                receipt.runtime_id,
                "ok",
                f"materialized runtime: {expected_root}",
            )
        )

    return checks
