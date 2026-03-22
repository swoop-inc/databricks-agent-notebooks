from __future__ import annotations

from pathlib import Path

from databricks_agent_notebooks.runtime.manifest import (
    KernelArtifactReceipt,
    LauncherKernelContract,
    RuntimeInstallReceipt,
    read_json_record,
    write_json_record,
)


def test_launcher_kernel_contract_round_trips() -> None:
    contract = LauncherKernelContract(
        contract_version="1",
        kernel_id="scala212-dbr-connect",
        display_name="Scala 2.12 (Databricks Connect)",
        language="scala",
        argv=["python", "-m", "databricks_agent_notebooks"],
        env={"SPARK_HOME": ""},
        runtime_id="dbr-16.4-python-3.12",
        runtime_receipt_path="data/runtimes/dbr-16.4-python-3.12/runtime-receipt.json",
        launcher_path="bin/scala-kernel-launcher",
        bootstrap_argv=["java", "--connection-file", "{connection_file}"],
    )

    restored = LauncherKernelContract.from_dict(contract.to_dict())

    assert restored == contract


def test_runtime_install_receipt_round_trips_on_disk(tmp_path: Path) -> None:
    receipt = RuntimeInstallReceipt(
        receipt_version="1",
        runtime_id="dbr-16.4-python-3.12",
        runtime_kind="databricks-connect",
        databricks_line="16.4",
        python_line="3.12",
        install_root="data/runtimes/dbr-16.4-python-3.12",
        installed_at="2026-03-22T12:00:00+00:00",
        status="materialized",
    )
    receipt_path = tmp_path / "runtime.json"

    write_json_record(receipt_path, receipt)
    restored = read_json_record(receipt_path, RuntimeInstallReceipt)

    assert restored == receipt


def test_kernel_artifact_receipt_round_trips_on_disk(tmp_path: Path) -> None:
    receipt = KernelArtifactReceipt(
        receipt_version="1",
        kernel_id="scala212-dbr-connect",
        display_name="Scala 2.12 (Databricks Connect)",
        language="scala",
        install_dir="data/kernels/specs/scala212-dbr-connect",
        runtime_id="dbr-16.4-python-3.12",
        runtime_receipt_path="data/runtimes/dbr-16.4-python-3.12/runtime-receipt.json",
        launcher_contract_path="contracts/scala212-dbr-connect.json",
        installed_at="2026-03-22T12:00:00+00:00",
    )
    receipt_path = tmp_path / "kernel.json"

    write_json_record(receipt_path, receipt)
    restored = read_json_record(receipt_path, KernelArtifactReceipt)

    assert restored == receipt


def test_json_record_writer_creates_parent_directories(tmp_path: Path) -> None:
    receipt = KernelArtifactReceipt(
        receipt_version="1",
        kernel_id="python3-dbr-connect",
        display_name="Python 3",
        language="python",
        install_dir="data/kernels/specs/python3-dbr-connect",
        runtime_id="dbr-16.4-python-3.12",
        runtime_receipt_path="data/runtimes/dbr-16.4-python-3.12/runtime-receipt.json",
        launcher_contract_path="contracts/python3-dbr-connect.json",
        installed_at="2026-03-22T12:00:00+00:00",
    )
    path = tmp_path / "nested" / "receipt.json"

    write_json_record(path, receipt)

    assert path.is_file()
    assert path.parent.is_dir()
