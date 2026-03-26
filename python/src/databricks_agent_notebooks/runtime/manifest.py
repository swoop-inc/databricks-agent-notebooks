"""JSON-serializable runtime and kernel receipt models."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")


def _normalized_dataclass_payload(
    record_type: type[Any],
    data: dict[str, Any],
    *,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    field_names = {field.name for field in fields(record_type)}
    normalized = {key: value for key, value in data.items() if key in field_names}
    for key, value in (defaults or {}).items():
        normalized.setdefault(key, value)
    return normalized


@dataclass(frozen=True)
class LauncherKernelContract:
    contract_version: str
    kernel_id: str
    display_name: str
    language: str
    argv: list[str]
    env: dict[str, str]
    runtime_id: str
    runtime_receipt_path: str
    launcher_path: str
    bootstrap_argv: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        source_path: Path | None = None,
    ) -> "LauncherKernelContract":
        del source_path
        return cls(
            **_normalized_dataclass_payload(
                cls,
                data,
                defaults={"runtime_receipt_path": ""},
            )
        )


@dataclass(frozen=True)
class RuntimeInstallReceipt:
    receipt_version: str
    runtime_id: str
    runtime_kind: str
    databricks_line: str
    python_line: str
    install_root: str
    installed_at: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        source_path: Path | None = None,
    ) -> "RuntimeInstallReceipt":
        del source_path
        return cls(**_normalized_dataclass_payload(cls, data))


@dataclass(frozen=True)
class KernelArtifactReceipt:
    receipt_version: str
    kernel_id: str
    display_name: str
    language: str
    install_dir: str
    runtime_id: str
    runtime_receipt_path: str
    launcher_contract_path: str
    installed_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        source_path: Path | None = None,
    ) -> "KernelArtifactReceipt":
        del source_path
        return cls(
            **_normalized_dataclass_payload(
                cls,
                data,
                defaults={
                    "runtime_id": "",
                    "runtime_receipt_path": "",
                },
            )
        )


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
    return from_dict(data, source_path=path)
