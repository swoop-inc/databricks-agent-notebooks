"""JSON-serializable runtime and kernel receipt models."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class LauncherKernelContract:
    contract_version: str
    kernel_id: str
    display_name: str
    language: str
    argv: list[str]
    env: dict[str, str]
    runtime_id: str
    launcher_path: str
    bootstrap_argv: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LauncherKernelContract":
        return cls(**data)


@dataclass(frozen=True)
class RuntimeInstallReceipt:
    receipt_version: str
    runtime_id: str
    runtime_kind: str
    databricks_line: str
    python_line: str
    install_root: str
    launcher_contract_path: str
    installed_at: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RuntimeInstallReceipt":
        return cls(**data)


@dataclass(frozen=True)
class KernelArtifactReceipt:
    receipt_version: str
    kernel_id: str
    display_name: str
    language: str
    install_dir: str
    launcher_contract_path: str
    installed_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KernelArtifactReceipt":
        return cls(**data)


def write_json_record(path: Path, record: Any) -> Path:
    """Persist a dataclass-style record as stable JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = record.to_dict() if hasattr(record, "to_dict") else asdict(record)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def read_json_record(path: Path, record_type: type[T]) -> T:
    """Load a JSON record from disk into the target record type."""
    data = json.loads(path.read_text(encoding="utf-8"))
    from_dict = getattr(record_type, "from_dict")
    return from_dict(data)
