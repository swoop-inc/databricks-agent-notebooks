from __future__ import annotations

import json
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_repo_has_monorepo_ready_root_layout() -> None:
    assert (REPO_ROOT / "python").is_dir()
    assert (REPO_ROOT / "jvm").is_dir()
    assert (REPO_ROOT / "contracts").is_dir()
    assert (REPO_ROOT / "docs").is_dir()
    assert (REPO_ROOT / ".github" / "workflows").is_dir()


def test_launcher_contract_schema_exists() -> None:
    schema_path = REPO_ROOT / "contracts" / "launcher-kernel-contract.schema.json"
    data = json.loads(schema_path.read_text(encoding="utf-8"))

    assert data["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert data["title"] == "LauncherKernelContract"
    assert "contract_version" in data["required"]
    assert "kernel_id" in data["required"]
    assert "language" in data["required"]
    assert "argv" in data["required"]
    assert "runtime_id" in data["required"]


def test_python_distribution_declares_nbconvert_dependency() -> None:
    pyproject_path = REPO_ROOT / "python" / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    dependencies = pyproject["project"]["dependencies"]
    assert any(dep.startswith("nbconvert") for dep in dependencies)
