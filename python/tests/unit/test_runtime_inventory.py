from __future__ import annotations

import json
from pathlib import Path

from databricks_agent_notebooks.runtime.home import RuntimeHome


def _make_runtime_home(root: Path) -> RuntimeHome:
    return RuntimeHome(
        root=root,
        cache_dir=root / "cache",
        runtimes_dir=root / "data" / "runtimes",
        kernels_dir=root / "data" / "kernels",
        installations_dir=root / "state" / "installations",
        links_dir=root / "state" / "links",
        logs_dir=root / "state" / "logs",
        bin_dir=root / "bin",
        config_dir=root / "config",
    )


def test_materialize_runtime_installation_writes_receipt_under_runtime_root(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.inventory import (
        RUNTIME_RECEIPT_FILENAME,
        materialize_runtime_installation,
        runtime_id_for,
    )

    home = _make_runtime_home(tmp_path / "runtime-home")

    receipt = materialize_runtime_installation(
        home,
        databricks_line="16.4",
        python_line="3.12",
    )

    runtime_id = runtime_id_for("16.4", "3.12")
    runtime_root = home.runtimes_dir / runtime_id
    assert receipt.runtime_id == runtime_id
    assert receipt.runtime_kind == "databricks-connect"
    assert receipt.install_root == str(runtime_root)
    assert receipt.status == "materialized"
    assert (runtime_root / RUNTIME_RECEIPT_FILENAME).is_file()


def test_list_installed_runtimes_reads_runtime_receipts(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.inventory import list_installed_runtimes, materialize_runtime_installation

    home = _make_runtime_home(tmp_path / "runtime-home")
    materialize_runtime_installation(home, databricks_line="16.4", python_line="3.11")
    materialize_runtime_installation(home, databricks_line="16.4", python_line="3.12")

    runtimes = list_installed_runtimes(home=home)

    assert [runtime.runtime_id for runtime in runtimes] == [
        "dbr-16.4-python-3.11",
        "dbr-16.4-python-3.12",
    ]
    assert [runtime.status for runtime in runtimes] == ["materialized", "materialized"]


def test_doctor_installed_runtimes_reports_install_root_mismatch(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.inventory import doctor_installed_runtimes, materialize_runtime_installation

    home = _make_runtime_home(tmp_path / "runtime-home")
    receipt = materialize_runtime_installation(home, databricks_line="16.4", python_line="3.12")
    receipt_path = Path(receipt.install_root) / "runtime-receipt.json"
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    payload["install_root"] = str(Path(receipt.install_root).parent / "missing-runtime-root")
    receipt_path.write_text(json.dumps(payload), encoding="utf-8")

    checks = doctor_installed_runtimes(home=home)

    assert len(checks) == 1
    assert checks[0].status == "fail"
    assert receipt.runtime_id in checks[0].name
    assert "install_root mismatch" in checks[0].message


def test_runtime_inventory_accepts_legacy_runtime_receipt_shape(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.inventory import doctor_installed_runtimes, list_installed_runtimes

    home = _make_runtime_home(tmp_path / "runtime-home")
    runtime_id = "dbr-16.4-python-3.12"
    receipt_path = home.runtimes_dir / runtime_id / "runtime-receipt.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(
        json.dumps(
            {
                "receipt_version": "1",
                "runtime_id": runtime_id,
                "runtime_kind": "databricks-connect",
                "databricks_line": "16.4",
                "python_line": "3.12",
                "install_root": str(receipt_path.parent),
                "launcher_contract_path": str(home.kernels_dir / "scala212-dbr-connect" / "launcher-contract.json"),
                "installed_at": "2026-03-22T00:00:00+00:00",
                "status": "materialized",
            }
        ),
        encoding="utf-8",
    )

    runtimes = list_installed_runtimes(home=home)
    checks = doctor_installed_runtimes(home=home)

    assert [runtime.runtime_id for runtime in runtimes] == [runtime_id]
    assert [(check.name, check.status) for check in checks] == [(runtime_id, "ok")]
