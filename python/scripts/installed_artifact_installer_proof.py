#!/usr/bin/env python3
"""Prove an installed wheel can execute offline installer and doctor flows."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

RUNTIME_HOME_ENV_VAR = "DATABRICKS_AGENT_NOTEBOOKS_HOME"
KERNEL_ID = "installed-artifact-proof"
DISPLAY_NAME = "Installed Artifact Proof"
CONTRACT_FILENAME = "launcher-contract.json"
RUNTIME_RECEIPT_FILENAME = "runtime-receipt.json"
_TOOL_METADATA_KEY = "databricks_agent_notebooks"


@dataclass(frozen=True)
class ProofLayout:
    root: Path
    runtime_home: Path
    kernels_dir: Path
    fake_bin: Path
    home_dir: Path


@dataclass(frozen=True)
class GeneratedArtifacts:
    kernel_dir: Path
    contract_path: Path
    receipt_path: Path
    runtime_receipt_path: Path
    runtime_id: str
    kernel_json: dict[str, object]
    contract: dict[str, object]
    receipt: dict[str, object]
    runtime_receipt: dict[str, object]


def make_layout(root: Path) -> ProofLayout:
    layout = ProofLayout(
        root=root,
        runtime_home=root / "runtime-home",
        kernels_dir=root / "jupyter-kernels",
        fake_bin=root / "fake-bin",
        home_dir=root / "home",
    )
    for path in (layout.runtime_home, layout.kernels_dir, layout.fake_bin, layout.home_dir):
        path.mkdir(parents=True, exist_ok=True)
    return layout


def build_proof_env(
    *,
    fake_bin: Path,
    runtime_home: Path,
    home_dir: Path,
    base_env: dict[str, str] | None = None,
) -> dict[str, str]:
    env = dict(base_env or os.environ)
    env.pop("PYTHONPATH", None)
    env[RUNTIME_HOME_ENV_VAR] = str(runtime_home)
    env["HOME"] = str(home_dir)
    existing_path = env.get("PATH", "")
    env["PATH"] = os.pathsep.join([str(fake_bin), existing_path]) if existing_path else str(fake_bin)
    return env


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    current_mode = path.stat().st_mode
    path.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def write_coursier_shim(path: Path) -> None:
    _write_executable(
        path,
        """#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def _value(args, flag):
    try:
        index = args.index(flag)
    except ValueError as exc:
        raise SystemExit(f"missing {flag}") from exc
    try:
        return args[index + 1]
    except IndexError as exc:
        raise SystemExit(f"missing value for {flag}") from exc


args = sys.argv[1:]
if "--" in args:
    args = args[args.index("--") + 1 :]
if "--install" not in args:
    raise SystemExit("expected --install")

kernel_id = _value(args, "--id")
display_name = _value(args, "--display-name")
jupyter_path = Path(_value(args, "--jupyter-path"))
kernel_dir = jupyter_path / kernel_id
kernel_dir.mkdir(parents=True, exist_ok=True)
kernel_json = {
    "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
    "display_name": display_name,
    "language": "scala",
    "env": {"SPARK_HOME": "/opt/spark"},
}
(kernel_dir / "kernel.json").write_text(json.dumps(kernel_json, indent=1) + "\\n", encoding="utf-8")
""",
    )


def write_java_shim(path: Path) -> None:
    _write_executable(
        path,
        """#!/bin/sh
if [ "$1" = "-version" ]; then
  echo 'openjdk version "17.0.10"' 1>&2
  exit 0
fi
exit 0
""",
    )


def write_databricks_shim(path: Path) -> None:
    _write_executable(
        path,
        """#!/bin/sh
exit 0
""",
    )


def install_fake_tooling(fake_bin: Path) -> None:
    write_coursier_shim(fake_bin / "coursier")
    write_java_shim(fake_bin / "java")
    write_databricks_shim(fake_bin / "databricks")


def _agent_notebook_bin() -> Path:
    sibling = Path(sys.executable).with_name("agent-notebook")
    if sibling.is_file():
        return sibling

    resolved_sibling = Path(sys.executable).resolve().with_name("agent-notebook")
    if resolved_sibling.is_file():
        return resolved_sibling

    fallback = shutil.which("agent-notebook")
    if fallback is not None:
        return Path(fallback)

    raise RuntimeError(f"agent-notebook entrypoint not found next to {sys.executable}")


def _run(command: list[str], *, env: dict[str, str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, check=False)


def _assert_success(result: subprocess.CompletedProcess[str], description: str) -> None:
    if result.returncode == 0:
        return
    raise RuntimeError(
        f"{description} failed with exit code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _same_path(left: Path, right: Path) -> bool:
    return left.resolve() == right.resolve()


def _find_single_runtime_receipt(runtime_home: Path) -> Path:
    matches = sorted(runtime_home.glob(f"data/runtimes/*/{RUNTIME_RECEIPT_FILENAME}"))
    _expect(len(matches) == 1, f"expected one runtime receipt under {runtime_home / 'data' / 'runtimes'}, found {matches}")
    return matches[0].resolve()


def validate_generated_artifacts(
    *,
    runtime_home: Path,
    kernels_dir: Path,
    kernel_id: str,
) -> GeneratedArtifacts:
    kernel_dir = (kernels_dir / kernel_id).resolve()
    contract_path = (kernel_dir / CONTRACT_FILENAME).resolve()
    receipt_path = (runtime_home / "state" / "installations" / "kernels" / f"{kernel_id}.json").resolve()
    runtime_receipt_path = _find_single_runtime_receipt(runtime_home)

    _expect(kernel_dir.is_dir(), f"kernel directory missing: {kernel_dir}")
    _expect(contract_path.is_file(), f"launcher contract missing: {contract_path}")
    _expect(receipt_path.is_file(), f"kernel receipt missing: {receipt_path}")
    _expect(runtime_receipt_path.is_file(), f"runtime receipt missing: {runtime_receipt_path}")

    kernel_json = _read_json(kernel_dir / "kernel.json")
    contract = _read_json(contract_path)
    receipt = _read_json(receipt_path)
    runtime_receipt = _read_json(runtime_receipt_path)

    metadata = kernel_json.get("metadata", {})
    _expect(isinstance(metadata, dict), "kernel.json metadata missing")
    tool_metadata = metadata.get(_TOOL_METADATA_KEY, {})
    _expect(isinstance(tool_metadata, dict), "kernel.json tool metadata missing")

    metadata_contract_path = Path(str(tool_metadata.get("launcher_contract_path", "")))
    metadata_receipt_path = Path(str(tool_metadata.get("receipt_path", "")))
    _expect(metadata_contract_path.is_absolute(), "kernel.json launcher_contract_path must be absolute")
    _expect(metadata_receipt_path.is_absolute(), "kernel.json receipt_path must be absolute")
    _expect(_same_path(metadata_contract_path, contract_path), "kernel.json launcher_contract_path mismatch")
    _expect(_same_path(metadata_receipt_path, receipt_path), "kernel.json receipt_path mismatch")

    runtime_id = str(contract.get("runtime_id", ""))
    _expect(runtime_id, "launcher contract runtime_id missing")
    _expect(runtime_id == str(receipt.get("runtime_id", "")), "kernel receipt runtime_id mismatch")
    _expect(runtime_id == str(runtime_receipt.get("runtime_id", "")), "runtime receipt runtime_id mismatch")

    contract_runtime_receipt_path = Path(str(contract.get("runtime_receipt_path", "")))
    receipt_runtime_receipt_path = Path(str(receipt.get("runtime_receipt_path", "")))
    receipt_contract_path = Path(str(receipt.get("launcher_contract_path", "")))
    _expect(contract_runtime_receipt_path.is_absolute(), "launcher contract runtime_receipt_path must be absolute")
    _expect(receipt_runtime_receipt_path.is_absolute(), "kernel receipt runtime_receipt_path must be absolute")
    _expect(receipt_contract_path.is_absolute(), "kernel receipt launcher_contract_path must be absolute")
    _expect(contract_runtime_receipt_path.is_file(), "launcher contract runtime_receipt_path missing on disk")
    _expect(receipt_runtime_receipt_path.is_file(), "kernel receipt runtime_receipt_path missing on disk")
    _expect(receipt_contract_path.is_file(), "kernel receipt launcher_contract_path missing on disk")
    _expect(_same_path(contract_runtime_receipt_path, runtime_receipt_path), "launcher contract runtime receipt mismatch")
    _expect(_same_path(receipt_runtime_receipt_path, runtime_receipt_path), "kernel receipt runtime receipt mismatch")
    _expect(_same_path(receipt_contract_path, contract_path), "kernel receipt launcher contract mismatch")

    bootstrap_argv = contract.get("bootstrap_argv")
    _expect(isinstance(bootstrap_argv, list) and bool(bootstrap_argv), "launcher contract bootstrap_argv missing")
    runtime_install_root = Path(str(runtime_receipt.get("install_root", "")))
    _expect(runtime_install_root.is_absolute(), "runtime receipt install_root must be absolute")
    _expect(_same_path(runtime_install_root, runtime_receipt_path.parent), "runtime receipt install_root mismatch")

    return GeneratedArtifacts(
        kernel_dir=kernel_dir,
        contract_path=contract_path,
        receipt_path=receipt_path,
        runtime_receipt_path=runtime_receipt_path,
        runtime_id=runtime_id,
        kernel_json=kernel_json,
        contract=contract,
        receipt=receipt,
        runtime_receipt=runtime_receipt,
    )


def _snapshot_tree(root: Path) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    for path in sorted(child for child in root.rglob("*") if child.is_file()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        snapshot[str(path.relative_to(root))] = digest
    return snapshot


def run_proof() -> None:
    with tempfile.TemporaryDirectory(prefix="installed-artifact-proof-") as temp_dir:
        layout = make_layout(Path(temp_dir))
        install_fake_tooling(layout.fake_bin)
        env = build_proof_env(fake_bin=layout.fake_bin, runtime_home=layout.runtime_home, home_dir=layout.home_dir)
        agent_notebook = _agent_notebook_bin()

        install_result = _run(
            [
                str(agent_notebook),
                "kernels",
                "install",
                "--id",
                KERNEL_ID,
                "--display-name",
                DISPLAY_NAME,
                "--jupyter-path",
                str(layout.kernels_dir),
                "--force",
            ],
            env=env,
            cwd=layout.root,
        )
        _assert_success(install_result, "agent-notebook kernels install")

        artifacts = validate_generated_artifacts(
            runtime_home=layout.runtime_home,
            kernels_dir=layout.kernels_dir,
            kernel_id=KERNEL_ID,
        )
        pre_doctor_snapshot = _snapshot_tree(layout.root)

        kernels_doctor_result = _run(
            [
                str(agent_notebook),
                "kernels",
                "doctor",
                "--id",
                KERNEL_ID,
                "--jupyter-path",
                str(layout.kernels_dir),
            ],
            env=env,
            cwd=layout.root,
        )
        _assert_success(kernels_doctor_result, "agent-notebook kernels doctor")
        _expect(
            "kernel semantics verified via launcher contract" in kernels_doctor_result.stdout,
            "kernels doctor did not report launcher-contract-backed verification",
        )

        runtimes_doctor_result = _run(
            [str(agent_notebook), "runtimes", "doctor"],
            env=env,
            cwd=layout.root,
        )
        _assert_success(runtimes_doctor_result, "agent-notebook runtimes doctor")
        _expect(
            "materialized runtime" in runtimes_doctor_result.stdout,
            "runtimes doctor did not report a materialized runtime",
        )

        post_doctor_snapshot = _snapshot_tree(layout.root)
        _expect(post_doctor_snapshot == pre_doctor_snapshot, "doctor commands mutated proof-root artifacts")

        print(f"Installed artifact proof succeeded for kernel '{KERNEL_ID}'.")
        print(f"Kernel dir: {artifacts.kernel_dir}")
        print(f"Runtime id: {artifacts.runtime_id}")
        print(f"Runtime receipt: {artifacts.runtime_receipt_path}")


def main() -> int:
    run_proof()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
