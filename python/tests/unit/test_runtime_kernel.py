from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

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


def test_patch_kernel_json_adds_flag_and_clears_spark_home(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import CONTRACT_FILENAME, patch_kernel_json

    kernel_dir = tmp_path / "scala212-dbr-connect"
    kernel_dir.mkdir()
    kernel_json = kernel_dir / "kernel.json"
    contract_path = kernel_dir / CONTRACT_FILENAME
    receipt_path = tmp_path / "state" / "installations" / "kernels" / "scala212-dbr-connect.json"
    kernel_json.write_text(
        json.dumps(
            {
                "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                "env": {"EXISTING": "1", "SPARK_HOME": "/opt/spark"},
            }
        ),
        encoding="utf-8",
    )

    patch_kernel_json(kernel_dir, contract_path=contract_path, receipt_path=receipt_path)
    patch_kernel_json(kernel_dir, contract_path=contract_path, receipt_path=receipt_path)

    data = json.loads(kernel_json.read_text(encoding="utf-8"))
    assert data["argv"] == [
        sys.executable,
        "-m",
        "databricks_agent_notebooks.runtime.launcher",
        "--launcher-contract",
        str(contract_path),
        "--connection-file",
        "{connection_file}",
    ]
    assert data.get("env", {}) == {}
    metadata = data["metadata"]["databricks_agent_notebooks"]
    assert metadata["launcher_contract_path"] == str(contract_path)
    assert metadata["receipt_path"] == str(receipt_path)


def test_patch_kernel_json_records_contract_and_receipt_metadata(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import patch_kernel_json

    kernel_dir = tmp_path / "scala212-dbr-connect"
    kernel_dir.mkdir()
    kernel_json = kernel_dir / "kernel.json"
    kernel_json.write_text(
        json.dumps(
            {
                "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                "env": {"SPARK_HOME": "/opt/spark"},
            }
        ),
        encoding="utf-8",
    )

    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = tmp_path / "state" / "installations" / "kernels" / "scala212-dbr-connect.json"

    patch_kernel_json(
        kernel_dir,
        contract_path=contract_path,
        receipt_path=receipt_path,
    )

    data = json.loads(kernel_json.read_text(encoding="utf-8"))
    metadata = data["metadata"]["databricks_agent_notebooks"]
    assert metadata["launcher_contract_path"] == str(contract_path)
    assert metadata["receipt_path"] == str(receipt_path)
    assert data["argv"][0] == sys.executable


def test_install_kernel_uses_runtime_home_by_default(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import (
        ADD_OPENS_FLAG,
        KERNEL_DISPLAY_NAME,
        KERNEL_ID,
        CONTRACT_FILENAME,
        install_kernel,
    )
    from databricks_agent_notebooks.runtime.inventory import RUNTIME_RECEIPT_FILENAME, runtime_id_for

    home = _make_runtime_home(tmp_path / "runtime-home")
    kernel_dir = home.kernels_dir / KERNEL_ID
    receipt_path = home.installations_dir / "kernels" / f"{KERNEL_ID}.json"
    runtime_id = runtime_id_for("16.4", f"{sys.version_info.major}.{sys.version_info.minor}")
    runtime_receipt_path = home.runtimes_dir / runtime_id / RUNTIME_RECEIPT_FILENAME

    def fake_run(*_args, **_kwargs) -> None:
        kernel_dir.mkdir(parents=True)
        (kernel_dir / "kernel.json").write_text(
            json.dumps(
                {
                    "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                    "display_name": KERNEL_DISPLAY_NAME,
                    "language": "scala",
                    "env": {"SPARK_HOME": "/opt/spark"},
                }
            ),
            encoding="utf-8",
        )

    with (
        patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.kernel.ensure_runtime_home", return_value=home) as ensure_home,
        patch("databricks_agent_notebooks.runtime.kernel.find_coursier", return_value="/opt/bin/coursier"),
        patch("databricks_agent_notebooks.runtime.kernel.subprocess.run", side_effect=fake_run) as run,
    ):
        kernel_dir = install_kernel()

    assert kernel_dir == home.kernels_dir / KERNEL_ID
    ensure_home.assert_called_once_with(home)
    run.assert_called_once_with(
        [
            "/opt/bin/coursier",
            "launch",
            "--fork",
            "almond",
            "--scala",
            "2.12",
            "--",
            "--install",
            "--id",
            KERNEL_ID,
            "--display-name",
            KERNEL_DISPLAY_NAME,
            "--jupyter-path",
            str(home.kernels_dir),
        ],
        check=True,
    )
    contract = json.loads((kernel_dir / CONTRACT_FILENAME).read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    runtime_receipt = json.loads(runtime_receipt_path.read_text(encoding="utf-8"))

    assert contract["kernel_id"] == KERNEL_ID
    assert contract["display_name"] == KERNEL_DISPLAY_NAME
    assert contract["argv"] == [
        sys.executable,
        "-m",
        "databricks_agent_notebooks.runtime.launcher",
        "--launcher-contract",
        str(kernel_dir / CONTRACT_FILENAME),
        "--connection-file",
        "{connection_file}",
    ]
    assert contract["env"] == {}
    assert contract["runtime_id"] == runtime_id
    assert contract["runtime_receipt_path"] == str(runtime_receipt_path)
    assert contract["launcher_path"] == sys.executable
    assert contract["bootstrap_argv"] == [
        "/usr/bin/java",
        ADD_OPENS_FLAG,
        "coursier",
        "--connection-file",
        "{connection_file}",
    ]
    assert receipt["kernel_id"] == KERNEL_ID
    assert receipt["runtime_id"] == runtime_id
    assert receipt["runtime_receipt_path"] == str(runtime_receipt_path)
    assert receipt["launcher_contract_path"] == str(kernel_dir / CONTRACT_FILENAME)
    assert receipt["install_dir"] == str(kernel_dir)
    assert runtime_receipt["runtime_id"] == runtime_id
    assert runtime_receipt["install_root"] == str(home.runtimes_dir / runtime_id)
    assert runtime_receipt["status"] == "materialized"
    kernel_json = json.loads((kernel_dir / "kernel.json").read_text(encoding="utf-8"))
    assert kernel_json["argv"] == contract["argv"]
    assert kernel_json.get("env", {}) == {}


def test_install_kernel_requires_coursier(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import install_kernel

    home = _make_runtime_home(tmp_path / "runtime-home")

    with (
        patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.kernel.ensure_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.kernel.find_coursier", return_value=None),
    ):
        with pytest.raises(RuntimeError, match="coursier is required"):
            install_kernel()


def test_install_kernel_accepts_contract_flags(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import install_kernel

    target_dir = tmp_path / "share" / "jupyter" / "kernels"

    def fake_run(*_args, **_kwargs) -> None:
        kernel_dir = target_dir / "custom-scala"
        kernel_dir.mkdir(parents=True)
        (kernel_dir / "kernel.json").write_text(
            json.dumps(
                {
                    "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                    "display_name": "Custom Scala",
                    "language": "scala",
                    "env": {"SPARK_HOME": "/opt/spark"},
                }
            ),
            encoding="utf-8",
        )

    with (
        patch("databricks_agent_notebooks.runtime.kernel.find_coursier", return_value="/opt/bin/coursier"),
        patch("databricks_agent_notebooks.runtime.kernel.subprocess.run", side_effect=fake_run) as run,
    ):
        install_kernel(
            kernel_id="custom-scala",
            display_name="Custom Scala",
            prefix=tmp_path,
            force=True,
        )

    run.assert_called_once_with(
        [
            "/opt/bin/coursier",
            "launch",
            "--fork",
            "almond",
            "--scala",
            "2.12",
            "--",
            "--install",
            "--force",
            "--id",
            "custom-scala",
            "--display-name",
            "Custom Scala",
            "--jupyter-path",
            str(target_dir),
        ],
        check=True,
    )


def test_verify_kernel_reports_missing_launcher_semantics(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import KERNEL_ID, verify_kernel

    kernel_dir = tmp_path / KERNEL_ID
    kernel_dir.mkdir()
    (kernel_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                "env": {"SPARK_HOME": "/opt/spark"},
            }
        ),
        encoding="utf-8",
    )

    issues = verify_kernel(tmp_path)

    assert any("launcher" in issue.lower() for issue in issues)
    assert any("SPARK_HOME" in issue for issue in issues)


def test_verify_kernel_reports_missing_contract_artifacts(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import CONTRACT_FILENAME, KERNEL_DISPLAY_NAME, KERNEL_ID, verify_kernel

    kernel_dir = tmp_path / KERNEL_ID
    kernel_dir.mkdir()
    (kernel_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": [
                    sys.executable,
                    "-m",
                    "databricks_agent_notebooks.runtime.launcher",
                    "--launcher-contract",
                    str(kernel_dir / CONTRACT_FILENAME),
                    "--connection-file",
                    "{connection_file}",
                ],
                "display_name": KERNEL_DISPLAY_NAME,
                "language": "scala",
                "env": {},
            }
        ),
        encoding="utf-8",
    )

    issues = verify_kernel(tmp_path)

    assert any("launcher contract" in issue.lower() for issue in issues)
    assert any("receipt" in issue.lower() for issue in issues)


def test_verify_kernel_supports_custom_kernel_id(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import verify_kernel

    kernel_id = "custom-scala"
    runtime_id = "dbr-16.4-python-3.12"
    kernel_dir = tmp_path / kernel_id
    kernel_dir.mkdir()
    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = tmp_path / "state" / "installations" / "kernels" / f"{kernel_id}.json"
    runtime_receipt_path = tmp_path / "data" / "runtimes" / runtime_id / "runtime-receipt.json"
    receipt_path.parent.mkdir(parents=True)
    runtime_receipt_path.parent.mkdir(parents=True)

    kernel_json = {
        "argv": [
            sys.executable,
            "-m",
            "databricks_agent_notebooks.runtime.launcher",
            "--launcher-contract",
            str(contract_path),
            "--connection-file",
            "{connection_file}",
        ],
        "display_name": "Custom Scala",
        "language": "scala",
        "env": {},
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
        "display_name": "Custom Scala",
        "language": "scala",
        "argv": kernel_json["argv"],
        "env": {},
        "runtime_id": runtime_id,
        "runtime_receipt_path": str(runtime_receipt_path),
        "launcher_path": sys.executable,
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
        "display_name": "Custom Scala",
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
        "install_root": str(tmp_path / "data" / "runtimes" / runtime_id),
        "installed_at": "2026-03-22T00:00:00+00:00",
        "status": "materialized",
    }

    (kernel_dir / "kernel.json").write_text(json.dumps(kernel_json), encoding="utf-8")
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    runtime_receipt_path.write_text(json.dumps(runtime_receipt), encoding="utf-8")

    assert verify_kernel(tmp_path, kernel_id=kernel_id) == []


def test_verify_kernel_reports_missing_runtime_receipt(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import verify_kernel

    kernel_id = "custom-scala"
    runtime_id = "dbr-16.4-python-3.12"
    kernel_dir = tmp_path / kernel_id
    kernel_dir.mkdir()
    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = tmp_path / "state" / "installations" / "kernels" / f"{kernel_id}.json"
    runtime_receipt_path = tmp_path / "data" / "runtimes" / runtime_id / "runtime-receipt.json"
    receipt_path.parent.mkdir(parents=True)

    (kernel_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": [
                    sys.executable,
                    "-m",
                    "databricks_agent_notebooks.runtime.launcher",
                    "--launcher-contract",
                    str(contract_path),
                    "--connection-file",
                    "{connection_file}",
                ],
                "display_name": "Custom Scala",
                "language": "scala",
                "env": {},
                "metadata": {
                    "databricks_agent_notebooks": {
                        "launcher_contract_path": str(contract_path),
                        "receipt_path": str(receipt_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    contract_path.write_text(
        json.dumps(
            {
                "contract_version": "1",
                "kernel_id": kernel_id,
                "display_name": "Custom Scala",
                "language": "scala",
                "argv": [
                    sys.executable,
                    "-m",
                    "databricks_agent_notebooks.runtime.launcher",
                    "--launcher-contract",
                    str(contract_path),
                    "--connection-file",
                    "{connection_file}",
                ],
                "env": {},
                "runtime_id": runtime_id,
                "runtime_receipt_path": str(runtime_receipt_path),
                "launcher_path": sys.executable,
                "bootstrap_argv": [
                    "/usr/bin/java",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "coursier",
                    "--connection-file",
                    "{connection_file}",
                ],
            }
        ),
        encoding="utf-8",
    )
    receipt_path.write_text(
        json.dumps(
            {
                "receipt_version": "1",
                "kernel_id": kernel_id,
                "display_name": "Custom Scala",
                "language": "scala",
                "install_dir": str(kernel_dir),
                "runtime_id": runtime_id,
                "runtime_receipt_path": str(runtime_receipt_path),
                "launcher_contract_path": str(contract_path),
                "installed_at": "2026-03-22T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    issues = verify_kernel(tmp_path, kernel_id=kernel_id)

    assert any("runtime receipt" in issue.lower() for issue in issues)


def test_list_installed_kernels_reports_runtime_home_and_overrides(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import list_installed_kernels

    runtime_home = _make_runtime_home(tmp_path / "runtime-home")
    runtime_id = "dbr-16.4-python-3.12"
    runtime_kernel = runtime_home.kernels_dir / "scala212-dbr-connect"
    runtime_kernel.mkdir(parents=True)
    runtime_contract = runtime_kernel / "launcher-contract.json"
    runtime_receipt = runtime_home.installations_dir / "kernels" / "scala212-dbr-connect.json"
    managed_runtime_receipt = runtime_home.runtimes_dir / runtime_id / "runtime-receipt.json"
    (runtime_kernel / "kernel.json").write_text(
        json.dumps(
            {
                "display_name": "Scala 2.12 (Databricks Connect)",
                "metadata": {
                    "databricks_agent_notebooks": {
                        "launcher_contract_path": str(runtime_contract),
                        "receipt_path": str(runtime_receipt),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    runtime_contract.write_text(
        json.dumps(
            {
                "contract_version": "1",
                "kernel_id": "scala212-dbr-connect",
                "display_name": "Scala 2.12 (Databricks Connect)",
                "language": "scala",
                "argv": [
                    sys.executable,
                    "-m",
                    "databricks_agent_notebooks.runtime.launcher",
                    "--launcher-contract",
                    str(runtime_contract),
                    "--connection-file",
                    "{connection_file}",
                ],
                "env": {},
                "runtime_id": runtime_id,
                "runtime_receipt_path": str(managed_runtime_receipt),
                "launcher_path": sys.executable,
                "bootstrap_argv": [
                    "/usr/bin/java",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "coursier",
                    "--connection-file",
                    "{connection_file}",
                ],
            }
        ),
        encoding="utf-8",
    )
    runtime_receipt.parent.mkdir(parents=True)
    managed_runtime_receipt.parent.mkdir(parents=True)
    runtime_receipt.write_text(
        json.dumps(
            {
                "receipt_version": "1",
                "kernel_id": "scala212-dbr-connect",
                "display_name": "Scala 2.12 (Databricks Connect)",
                "language": "scala",
                "install_dir": str(runtime_kernel),
                "runtime_id": runtime_id,
                "runtime_receipt_path": str(managed_runtime_receipt),
                "launcher_contract_path": str(runtime_contract),
                "installed_at": "2026-03-22T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    managed_runtime_receipt.write_text(
        json.dumps(
            {
                "receipt_version": "1",
                "runtime_id": runtime_id,
                "runtime_kind": "databricks-connect",
                "databricks_line": "16.4",
                "python_line": "3.12",
                "install_root": str(runtime_home.runtimes_dir / runtime_id),
                "installed_at": "2026-03-22T00:00:00+00:00",
                "status": "materialized",
            }
        ),
        encoding="utf-8",
    )

    override_dir = tmp_path / "custom-kernels"
    override_kernel = override_dir / "python3"
    override_kernel.mkdir(parents=True)
    (override_kernel / "kernel.json").write_text(
        json.dumps(
            {
                "display_name": "Python 3",
                "metadata": {
                    "databricks_agent_notebooks": {
                        "launcher_contract_path": str(override_kernel / "launcher-contract.json"),
                        "receipt_path": str(override_kernel / "install-receipt.json"),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (override_kernel / "launcher-contract.json").write_text(
        json.dumps(
            {
                "contract_version": "1",
                "kernel_id": "python3",
                "display_name": "Python 3",
                "language": "python",
                "argv": [
                    "/usr/bin/python3",
                    "-m",
                    "ipykernel_launcher",
                    "-f",
                    "{connection_file}",
                ],
                "env": {},
                "runtime_id": "local-python3",
                "runtime_receipt_path": str(override_kernel / "runtime-receipt.json"),
                "launcher_path": "/usr/bin/python3",
                "bootstrap_argv": [
                    "/usr/bin/python3",
                    "-m",
                    "ipykernel_launcher",
                    "-f",
                    "{connection_file}",
                ],
            }
        ),
        encoding="utf-8",
    )

    with patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=runtime_home):
        kernels = list_installed_kernels(kernels_dirs=[override_dir])

    assert [(kernel.name, kernel.source) for kernel in kernels] == [
        ("scala212-dbr-connect", "runtime-home"),
        ("python3", str(override_dir)),
    ]
    assert [kernel.directory for kernel in kernels] == [runtime_kernel, override_kernel]
    assert kernels[0].launcher_contract_path == runtime_contract
    assert kernels[0].receipt_path == runtime_receipt
    assert kernels[0].launcher_path == sys.executable
    assert kernels[0].runtime_id == runtime_id
    assert kernels[1].launcher_contract_path == override_kernel / "launcher-contract.json"
    assert kernels[1].receipt_path == override_kernel / "install-receipt.json"
    assert kernels[1].launcher_path == "/usr/bin/python3"
    assert kernels[1].runtime_id == "local-python3"


def test_remove_kernel_deletes_named_kernel_from_runtime_home(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import remove_kernel

    runtime_home = _make_runtime_home(tmp_path / "runtime-home")
    kernel_dir = runtime_home.kernels_dir / "scala212-dbr-connect"
    kernel_dir.mkdir(parents=True)
    (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")

    with patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=runtime_home):
        removed = remove_kernel("scala212-dbr-connect")

    assert removed == kernel_dir
    assert not kernel_dir.exists()


def test_remove_kernel_rejects_path_like_names(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.kernel import remove_kernel

    runtime_home = _make_runtime_home(tmp_path / "runtime-home")

    with patch("databricks_agent_notebooks.runtime.kernel.resolve_runtime_home", return_value=runtime_home):
        with pytest.raises(ValueError, match="kernel name"):
            remove_kernel("../scala212-dbr-connect")
