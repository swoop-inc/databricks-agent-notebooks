from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path


def _load_proof_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "installed_artifact_installer_proof.py"
    spec = importlib.util.spec_from_file_location("installed_artifact_installer_proof", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_write_coursier_shim_installs_kernel_json(tmp_path: Path) -> None:
    module = _load_proof_module()
    shim_path = tmp_path / "coursier"
    kernels_dir = tmp_path / "jupyter-kernels"

    module.write_coursier_shim(shim_path)

    subprocess.run(
        [
            str(shim_path),
            "launch",
            "--fork",
            "almond",
            "--scala",
            "2.12",
            "--",
            "--install",
            "--id",
            "installed-artifact-proof",
            "--display-name",
            "Installed Artifact Proof",
            "--jupyter-path",
            str(kernels_dir),
        ],
        check=True,
    )

    kernel_json_path = kernels_dir / "installed-artifact-proof" / "kernel.json"
    kernel_json = json.loads(kernel_json_path.read_text(encoding="utf-8"))
    assert kernel_json["display_name"] == "Installed Artifact Proof"
    assert kernel_json["language"] == "scala"
    assert kernel_json["argv"] == [
        "/usr/bin/java",
        "coursier",
        "--connection-file",
        "{connection_file}",
    ]
    assert kernel_json["env"] == {"SPARK_HOME": "/opt/spark"}


def test_build_proof_env_isolates_runtime_home_and_fake_bin(tmp_path: Path) -> None:
    module = _load_proof_module()
    fake_bin = tmp_path / "fake-bin"
    runtime_home = tmp_path / "runtime-home"
    home_dir = tmp_path / "home"

    env = module.build_proof_env(fake_bin=fake_bin, runtime_home=runtime_home, home_dir=home_dir)

    assert env[module.RUNTIME_HOME_ENV_VAR] == str(runtime_home)
    assert env["HOME"] == str(home_dir)
    assert env["PATH"].split(os.pathsep)[0] == str(fake_bin)
    assert "PYTHONPATH" not in env


def test_write_python_run_smoke_input_creates_python_markdown(tmp_path: Path) -> None:
    module = _load_proof_module()

    notebook_path = module.write_python_run_smoke_input(tmp_path)

    assert notebook_path == tmp_path / "python-run-smoke.md"
    content = notebook_path.read_text(encoding="utf-8")
    assert "```python" in content
    assert "print(\"artifact smoke\")" in content


def test_validate_generated_artifacts_checks_cross_references(tmp_path: Path) -> None:
    module = _load_proof_module()
    runtime_home = tmp_path / "runtime-home"
    kernels_dir = tmp_path / "jupyter-kernels"
    kernel_id = "installed-artifact-proof"
    runtime_id = "dbr-16.4-python-3.12"
    kernel_dir = kernels_dir / kernel_id
    kernel_dir.mkdir(parents=True)

    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = runtime_home / "state" / "installations" / "kernels" / f"{kernel_id}.json"
    runtime_receipt_path = runtime_home / "data" / "runtimes" / runtime_id / "runtime-receipt.json"
    receipt_path.parent.mkdir(parents=True)
    runtime_receipt_path.parent.mkdir(parents=True)

    kernel_json = {
        "argv": [
            "/tmp/venv/bin/python",
            "-m",
            "databricks_agent_notebooks.runtime.launcher",
            "--launcher-contract",
            str(contract_path),
            "--connection-file",
            "{connection_file}",
        ],
        "display_name": "Installed Artifact Proof",
        "language": "scala",
        "metadata": {
            "databricks_agent_notebooks": {
                "launcher_contract_path": str(contract_path),
                "receipt_path": str(receipt_path),
            }
        },
    }
    contract = {
        "contract_version": "1",
        "kernel_id": kernel_id,
        "display_name": "Installed Artifact Proof",
        "language": "scala",
        "argv": kernel_json["argv"],
        "env": {},
        "runtime_id": runtime_id,
        "runtime_receipt_path": str(runtime_receipt_path),
        "launcher_path": "/tmp/venv/bin/python",
        "bootstrap_argv": [
            "/usr/bin/java",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "coursier",
            "--connection-file",
            "{connection_file}",
        ],
    }
    receipt = {
        "receipt_version": "1",
        "kernel_id": kernel_id,
        "display_name": "Installed Artifact Proof",
        "language": "scala",
        "install_dir": str(kernel_dir),
        "runtime_id": runtime_id,
        "runtime_receipt_path": str(runtime_receipt_path),
        "launcher_contract_path": str(contract_path),
        "installed_at": "2026-03-22T00:00:00+00:00",
    }
    runtime_receipt = {
        "receipt_version": "1",
        "runtime_id": runtime_id,
        "runtime_kind": "databricks-connect",
        "databricks_line": "16.4",
        "python_line": "3.12",
        "install_root": str(runtime_receipt_path.parent),
        "installed_at": "2026-03-22T00:00:00+00:00",
        "status": "materialized",
    }

    (kernel_dir / "kernel.json").write_text(json.dumps(kernel_json), encoding="utf-8")
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    runtime_receipt_path.write_text(json.dumps(runtime_receipt), encoding="utf-8")

    artifacts = module.validate_generated_artifacts(
        runtime_home=runtime_home,
        kernels_dir=kernels_dir,
        kernel_id=kernel_id,
    )

    assert artifacts.kernel_dir == kernel_dir
    assert artifacts.contract_path == contract_path
    assert artifacts.receipt_path == receipt_path
    assert artifacts.runtime_receipt_path == runtime_receipt_path
    assert artifacts.runtime_id == runtime_id


def test_validate_generated_artifacts_accepts_equivalent_runtime_install_root(tmp_path: Path) -> None:
    module = _load_proof_module()
    alias_root = tmp_path / "alias-root"
    real_root = tmp_path / "real-root"
    alias_root.mkdir()
    runtime_home = alias_root / "runtime-home"
    kernels_dir = alias_root / "jupyter-kernels"
    kernel_id = "installed-artifact-proof"
    runtime_id = "dbr-16.4-python-3.12"
    runtime_dir = real_root / "runtime-home" / "data" / "runtimes" / runtime_id
    kernel_dir = kernels_dir / kernel_id
    kernel_dir.mkdir(parents=True)
    runtime_dir.parent.mkdir(parents=True)
    (alias_root / "runtime-home").symlink_to(real_root / "runtime-home")

    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = runtime_home / "state" / "installations" / "kernels" / f"{kernel_id}.json"
    runtime_receipt_path = runtime_dir / "runtime-receipt.json"
    receipt_path.parent.mkdir(parents=True)
    runtime_receipt_path.parent.mkdir(parents=True)

    kernel_json = {
        "argv": [
            "/tmp/venv/bin/python",
            "-m",
            "databricks_agent_notebooks.runtime.launcher",
            "--launcher-contract",
            str(contract_path),
            "--connection-file",
            "{connection_file}",
        ],
        "display_name": "Installed Artifact Proof",
        "language": "scala",
        "metadata": {
            "databricks_agent_notebooks": {
                "launcher_contract_path": str(contract_path),
                "receipt_path": str(receipt_path),
            }
        },
    }
    contract = {
        "contract_version": "1",
        "kernel_id": kernel_id,
        "display_name": "Installed Artifact Proof",
        "language": "scala",
        "argv": kernel_json["argv"],
        "env": {},
        "runtime_id": runtime_id,
        "runtime_receipt_path": str((runtime_home / "data" / "runtimes" / runtime_id / "runtime-receipt.json")),
        "launcher_path": "/tmp/venv/bin/python",
        "bootstrap_argv": [
            "/usr/bin/java",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "coursier",
            "--connection-file",
            "{connection_file}",
        ],
    }
    receipt = {
        "receipt_version": "1",
        "kernel_id": kernel_id,
        "display_name": "Installed Artifact Proof",
        "language": "scala",
        "install_dir": str(kernel_dir),
        "runtime_id": runtime_id,
        "runtime_receipt_path": contract["runtime_receipt_path"],
        "launcher_contract_path": str(contract_path),
        "installed_at": "2026-03-22T00:00:00+00:00",
    }
    runtime_receipt = {
        "receipt_version": "1",
        "runtime_id": runtime_id,
        "runtime_kind": "databricks-connect",
        "databricks_line": "16.4",
        "python_line": "3.12",
        "install_root": str(runtime_dir),
        "installed_at": "2026-03-22T00:00:00+00:00",
        "status": "materialized",
    }

    (kernel_dir / "kernel.json").write_text(json.dumps(kernel_json), encoding="utf-8")
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    runtime_receipt_path.write_text(json.dumps(runtime_receipt), encoding="utf-8")

    artifacts = module.validate_generated_artifacts(
        runtime_home=runtime_home,
        kernels_dir=kernels_dir,
        kernel_id=kernel_id,
    )

    assert artifacts.runtime_receipt_path == runtime_receipt_path.resolve()


def test_agent_notebook_bin_prefers_unresolved_venv_sibling(tmp_path: Path, monkeypatch) -> None:
    module = _load_proof_module()
    framework_bin = tmp_path / "framework" / "bin"
    venv_bin = tmp_path / ".venv" / "bin"
    python_bin = venv_bin / "python"
    agent_notebook = venv_bin / "agent-notebook"
    framework_python = framework_bin / "python"
    framework_python.parent.mkdir(parents=True)
    venv_bin.mkdir(parents=True)
    framework_python.write_text("", encoding="utf-8")
    python_bin.symlink_to(framework_python)
    agent_notebook.write_text("", encoding="utf-8")

    monkeypatch.setattr(module.sys, "executable", str(python_bin))
    monkeypatch.setattr(module.shutil, "which", lambda name: None)

    assert module._agent_notebook_bin() == agent_notebook
