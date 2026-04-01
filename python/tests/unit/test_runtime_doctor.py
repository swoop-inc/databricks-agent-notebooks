from __future__ import annotations

import builtins
import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch

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


def test_kernel_search_dirs_prioritize_runtime_home(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import kernel_search_dirs

    home = _make_runtime_home(tmp_path / "runtime-home")

    with patch("databricks_agent_notebooks.runtime.doctor.Path.home", return_value=tmp_path / "home"):
        dirs = kernel_search_dirs(home)

    assert dirs[0] == home.kernels_dir
    assert dirs[1:] == [
        tmp_path / "home" / "Library" / "Jupyter" / "kernels",
        tmp_path / "home" / ".local" / "share" / "jupyter" / "kernels",
    ]


def test_run_checks_reports_kernel_semantics_warning_and_optional_profile(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import run_checks
    from databricks_agent_notebooks.runtime.kernel import KERNEL_ID

    home = _make_runtime_home(tmp_path / "runtime-home")
    kernel_dir = home.kernels_dir / KERNEL_ID
    kernel_dir.mkdir(parents=True)
    (kernel_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": ["/usr/bin/java", "coursier", "--connection-file", "{connection_file}"],
                "env": {"SPARK_HOME": "/opt/spark"},
            }
        ),
        encoding="utf-8",
    )

    java_result = Mock(stdout="", stderr='openjdk version "17.0.10"\n')

    with (
        patch("databricks_agent_notebooks.runtime.doctor.resolve_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.doctor.shutil.which", side_effect=lambda name: f"/usr/bin/{name}"),
        patch("databricks_agent_notebooks.runtime.doctor.subprocess.run", return_value=java_result),
        patch("databricks_agent_notebooks.runtime.doctor.Path.home", return_value=tmp_path / "home"),
        patch("databricks_agent_notebooks.runtime.doctor.os.environ", {"SPARK_HOME": "/opt/spark"}, create=True),
    ):
        cfg = tmp_path / "home" / ".databrickscfg"
        cfg.parent.mkdir(parents=True)
        cfg.write_text("[DEFAULT]\nhost = https://example.com\n", encoding="utf-8")
        checks = run_checks(profile="DEFAULT")

    statuses = {check.name: check.status for check in checks}

    assert statuses["coursier"] == "ok"
    assert statuses["kernel"] == "ok"
    assert statuses["kernel_semantics"] == "fail"
    assert "spark_home" not in statuses
    assert statuses["java"] == "ok"
    assert "pyspark" in statuses
    assert statuses["profile"] == "ok"


def test_run_checks_reports_missing_java_and_kernel(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import run_checks

    home = _make_runtime_home(tmp_path / "runtime-home")

    def which(name: str) -> str | None:
        return None

    with (
        patch("databricks_agent_notebooks.runtime.doctor.resolve_runtime_home", return_value=home),
        patch("databricks_agent_notebooks.runtime.doctor.shutil.which", side_effect=which),
        patch("databricks_agent_notebooks.runtime.doctor.Path.home", return_value=tmp_path / "home"),
        patch("databricks_agent_notebooks.runtime.doctor.os.environ", {}, create=True),
    ):
        checks = run_checks()

    messages = {check.name: check.message for check in checks}
    statuses = {check.name: check.status for check in checks}

    assert statuses["coursier"] == "fail"
    assert statuses["kernel"] == "fail"
    assert statuses["kernel_semantics"] == "fail"
    assert statuses["java"] == "fail"
    assert "pyspark" in statuses
    assert "kernels install" in messages["kernel"]


def test_check_kernel_semantics_requires_launcher_contract(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import check_kernel_semantics
    from databricks_agent_notebooks.runtime.kernel import CONTRACT_FILENAME, KERNEL_DISPLAY_NAME, KERNEL_ID

    home = _make_runtime_home(tmp_path / "runtime-home")
    kernel_dir = home.kernels_dir / KERNEL_ID
    kernel_dir.mkdir(parents=True)
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

    check = check_kernel_semantics(home=home)

    assert check.status == "fail"
    assert "launcher contract" in check.message.lower()
    assert "kernels install --force" in check.message


def test_check_kernel_semantics_supports_custom_kernel_id(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import check_kernel_semantics
    from databricks_agent_notebooks.runtime.kernel import ADD_OPENS_FLAG

    home = _make_runtime_home(tmp_path / "runtime-home")
    kernel_id = "custom-scala"
    runtime_id = "dbr-16.4-python-3.12"
    kernel_dir = home.kernels_dir / kernel_id
    kernel_dir.mkdir(parents=True)
    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = home.installations_dir / "kernels" / f"{kernel_id}.json"
    runtime_receipt_path = home.runtimes_dir / runtime_id / "runtime-receipt.json"
    receipt_path.parent.mkdir(parents=True)
    runtime_receipt_path.parent.mkdir(parents=True)
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
                    ADD_OPENS_FLAG,
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
    runtime_receipt_path.write_text(
        json.dumps(
            {
                "receipt_version": "1",
                "runtime_id": runtime_id,
                "runtime_kind": "databricks-connect",
                "databricks_line": "16.4",
                "python_line": "3.12",
                "install_root": str(home.runtimes_dir / runtime_id),
                "installed_at": "2026-03-22T00:00:00+00:00",
                "status": "materialized",
            }
        ),
        encoding="utf-8",
    )

    check = check_kernel_semantics(home=home, kernel_id=kernel_id)

    assert check.status == "ok"
    assert sys.executable in check.message


def test_check_kernel_semantics_accepts_legacy_artifacts(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import check_kernel_semantics

    home = _make_runtime_home(tmp_path / "runtime-home")
    kernel_id = "custom-scala"
    kernel_dir = home.kernels_dir / kernel_id
    kernel_dir.mkdir(parents=True)
    contract_path = kernel_dir / "launcher-contract.json"
    receipt_path = home.installations_dir / "kernels" / f"{kernel_id}.json"
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
                "runtime_id": kernel_id,
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
                "launcher_contract_path": str(contract_path),
                "installed_at": "2026-03-22T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    check = check_kernel_semantics(home=home, kernel_id=kernel_id)

    assert check.status == "ok"
    assert sys.executable in check.message


def test_java_warns_below_17() -> None:
    from databricks_agent_notebooks.runtime.doctor import check_java

    java_result = Mock(stdout="", stderr='openjdk version "11.0.20"\n')

    with (
        patch("databricks_agent_notebooks.runtime.doctor.shutil.which", return_value="/usr/bin/java"),
        patch("databricks_agent_notebooks.runtime.doctor.subprocess.run", return_value=java_result),
    ):
        check = check_java()

    assert check.status == "warn"
    assert "11" in check.message
    assert "17" in check.message


def test_java_ok_at_17() -> None:
    from databricks_agent_notebooks.runtime.doctor import check_java

    java_result = Mock(stdout="", stderr='openjdk version "17.0.10"\n')

    with (
        patch("databricks_agent_notebooks.runtime.doctor.shutil.which", return_value="/usr/bin/java"),
        patch("databricks_agent_notebooks.runtime.doctor.subprocess.run", return_value=java_result),
    ):
        check = check_java()

    assert check.status == "ok"
    assert "17" in check.message


def _pyspark_import_passthrough():
    """Return an __import__ replacement that allows ``import pyspark`` to succeed with a mock."""
    original_import = builtins.__import__
    fake_pyspark = Mock()

    def mock_import(name, *args, **kwargs):
        if name == "pyspark":
            return fake_pyspark
        return original_import(name, *args, **kwargs)

    return mock_import


def test_check_pyspark_found_standalone() -> None:
    from databricks_agent_notebooks.runtime.doctor import check_pyspark

    spec = Mock(origin="/usr/lib/python3.12/site-packages/pyspark/__init__.py")
    with (
        patch("importlib.util.find_spec", return_value=spec),
        patch("importlib.metadata.version", return_value="3.5.4"),
        patch("builtins.__import__", side_effect=_pyspark_import_passthrough()),
    ):
        check = check_pyspark()

    assert check.name == "pyspark"
    assert check.status == "ok"
    assert "3.5.4" in check.message
    assert "standalone" in check.message


def test_check_pyspark_found_databricks_connect() -> None:
    from databricks_agent_notebooks.runtime.doctor import check_pyspark

    spec = Mock(origin="/usr/lib/python3.12/site-packages/databricks/connect/pyspark/__init__.py")
    with (
        patch("importlib.util.find_spec", return_value=spec),
        patch("importlib.metadata.version", return_value="16.4.0"),
        patch("builtins.__import__", side_effect=_pyspark_import_passthrough()),
    ):
        check = check_pyspark()

    assert check.name == "pyspark"
    assert check.status == "ok"
    assert "16.4.0" in check.message
    assert "databricks-connect" in check.message


def test_check_pyspark_found_unknown_version() -> None:
    import importlib.metadata

    from databricks_agent_notebooks.runtime.doctor import check_pyspark

    spec = Mock(origin="/some/path/pyspark/__init__.py")
    with (
        patch("importlib.util.find_spec", return_value=spec),
        patch("importlib.metadata.version", side_effect=importlib.metadata.PackageNotFoundError("pyspark")),
        patch("builtins.__import__", side_effect=_pyspark_import_passthrough()),
    ):
        check = check_pyspark()

    assert check.name == "pyspark"
    assert check.status == "ok"
    assert "unknown" in check.message
    assert "standalone" in check.message


def test_check_pyspark_not_found() -> None:
    from databricks_agent_notebooks.runtime.doctor import check_pyspark

    with patch("importlib.util.find_spec", return_value=None):
        check = check_pyspark()

    assert check.name == "pyspark"
    assert check.status == "warn"
    assert "not found" in check.message
    assert "LOCAL_SPARK" in check.message


def test_check_pyspark_stump_directory_detected() -> None:
    """find_spec returns truthy but actual import fails — should warn, not ok."""
    from databricks_agent_notebooks.runtime.doctor import check_pyspark

    spec = Mock(origin="/some/path/pyspark/__init__.py")

    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "pyspark":
            raise ImportError("No module named 'pyspark'")
        return original_import(name, *args, **kwargs)

    # Ensure pyspark is not cached in sys.modules (otherwise import bypasses __import__ mock)
    _saved = {k: sys.modules.pop(k) for k in [k for k in sys.modules if k == "pyspark" or k.startswith("pyspark.")]}
    with (
        patch("importlib.util.find_spec", return_value=spec),
        patch("builtins.__import__", side_effect=mock_import),
    ):
        check = check_pyspark()
    sys.modules.update(_saved)

    assert check.name == "pyspark"
    assert check.status == "warn"
    assert "not importable" in check.message
    assert "reinstall" in check.message


def test_check_profile_default_requires_real_default_entries(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import check_profile

    home = tmp_path / "home"
    cfg = home / ".databrickscfg"
    cfg.parent.mkdir(parents=True)
    cfg.write_text("[DEV]\nhost = https://example.com\ntoken = abc\n", encoding="utf-8")

    with patch("databricks_agent_notebooks.runtime.doctor.Path.home", return_value=home):
        check = check_profile("DEFAULT")

    assert check.status == "fail"
    assert "DEFAULT" in check.message


def test_check_profile_honors_databricks_config_file(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import check_profile

    cfg = tmp_path / "custom-databricks.cfg"
    cfg.write_text("[PROD]\nhost = https://example.com\ntoken = abc\n", encoding="utf-8")

    with (
        patch("databricks_agent_notebooks.runtime.doctor.Path.home", return_value=tmp_path / "home"),
        patch.dict("databricks_agent_notebooks.runtime.doctor.os.environ", {"DATABRICKS_CONFIG_FILE": str(cfg)}, clear=True),
    ):
        check = check_profile("PROD")

    assert check.status == "ok"
    assert "PROD" in check.message


# ---------------------------------------------------------------------------
# Scala Connect cache readiness checks
# ---------------------------------------------------------------------------


def test_check_scala_connect_cached_ok() -> None:
    from databricks_agent_notebooks._constants import SCALA_212
    from databricks_agent_notebooks.runtime.doctor import check_scala_connect_cached

    fake_result = Mock(
        returncode=0,
        stdout="/cache/com/databricks/databricks-connect-16.4.7.jar\n",
    )
    fake_run = Mock(return_value=fake_result)

    check = check_scala_connect_cached("16.4", SCALA_212, "/usr/bin/cs", subprocess_run=fake_run)

    assert check.status == "ok"
    assert "16.4.7" in check.message
    assert "cached" in check.message
    fake_run.assert_called_once()
    args = fake_run.call_args
    assert args[0][0] == ["/usr/bin/cs", "fetch", "--mode", "offline", "com.databricks:databricks-connect:16.4.+"]


def test_check_scala_connect_cached_timeout() -> None:
    import subprocess as _subprocess

    from databricks_agent_notebooks._constants import SCALA_212
    from databricks_agent_notebooks.runtime.doctor import check_scala_connect_cached

    fake_run = Mock(side_effect=_subprocess.TimeoutExpired(cmd="cs", timeout=15))

    check = check_scala_connect_cached("16.4", SCALA_212, "/usr/bin/cs", subprocess_run=fake_run)

    assert check.status == "warn"
    assert "timed out" in check.message


def test_check_scala_connect_cached_warn() -> None:
    from databricks_agent_notebooks._constants import SCALA_212
    from databricks_agent_notebooks.runtime.doctor import check_scala_connect_cached

    fake_result = Mock(returncode=1, stdout="")
    fake_run = Mock(return_value=fake_result)

    check = check_scala_connect_cached("13.3", SCALA_212, "/usr/bin/cs", subprocess_run=fake_run)

    assert check.status == "warn"
    assert "not cached" in check.message
    assert "will download" in check.message


def test_scala_readiness_skips_no_coursier() -> None:
    from databricks_agent_notebooks.runtime.doctor import doctor_scala_connect_readiness

    with patch("databricks_agent_notebooks.runtime.doctor.find_coursier", return_value=None):
        checks = doctor_scala_connect_readiness()

    assert checks == []


def test_scala_readiness_skips_no_runtimes() -> None:
    from databricks_agent_notebooks.runtime.doctor import doctor_scala_connect_readiness

    with (
        patch("databricks_agent_notebooks.runtime.doctor.find_coursier", return_value="/usr/bin/cs"),
        patch("databricks_agent_notebooks.runtime.doctor.list_installed_runtimes", return_value=[]),
    ):
        checks = doctor_scala_connect_readiness()

    assert checks == []


def test_scala_readiness_deduplicates() -> None:
    from databricks_agent_notebooks.runtime.doctor import doctor_scala_connect_readiness
    from databricks_agent_notebooks.runtime.inventory import InstalledRuntime

    runtimes = [
        InstalledRuntime("dbr-16.4-python-3.12", "materialized", "16.4", "3.12", Path("/r1"), Path("/r1")),
        InstalledRuntime("dbr-16.4-python-3.14", "materialized", "16.4", "3.14", Path("/r2"), Path("/r2")),
    ]
    fake_result = Mock(returncode=0, stdout="/cache/databricks-connect-16.4.7.jar\n")
    fake_run = Mock(return_value=fake_result)

    with (
        patch("databricks_agent_notebooks.runtime.doctor.find_coursier", return_value="/usr/bin/cs"),
        patch("databricks_agent_notebooks.runtime.doctor.list_installed_runtimes", return_value=runtimes),
    ):
        checks = doctor_scala_connect_readiness(subprocess_run=fake_run)

    assert len(checks) == 1
    assert "16.4" in checks[0].name


def test_scala_readiness_mixed_212_213() -> None:
    from databricks_agent_notebooks.runtime.doctor import doctor_scala_connect_readiness
    from databricks_agent_notebooks.runtime.inventory import InstalledRuntime

    runtimes = [
        InstalledRuntime("dbr-16.4-python-3.12", "materialized", "16.4", "3.12", Path("/r1"), Path("/r1")),
        InstalledRuntime("dbr-17.3-python-3.12", "materialized", "17.3", "3.12", Path("/r2"), Path("/r2")),
    ]
    fake_result = Mock(returncode=0, stdout="/cache/databricks-connect-16.4.7.jar\n")
    fake_run = Mock(return_value=fake_result)

    with (
        patch("databricks_agent_notebooks.runtime.doctor.find_coursier", return_value="/usr/bin/cs"),
        patch("databricks_agent_notebooks.runtime.doctor.list_installed_runtimes", return_value=runtimes),
    ):
        checks = doctor_scala_connect_readiness(subprocess_run=fake_run)

    assert len(checks) == 2
    names = {c.name for c in checks}
    assert "scala-connect(16.4, 2.12)" in names
    assert "scala-connect(17.3, 2.13)" in names


def test_scala_readiness_correct_213_artifact() -> None:
    from databricks_agent_notebooks.runtime.doctor import doctor_scala_connect_readiness
    from databricks_agent_notebooks.runtime.inventory import InstalledRuntime

    runtimes = [
        InstalledRuntime("dbr-17.3-python-3.12", "materialized", "17.3", "3.12", Path("/r1"), Path("/r1")),
    ]
    fake_result = Mock(
        returncode=0,
        stdout="/cache/com/databricks/databricks-connect_2.13-17.3.4.jar\n",
    )
    fake_run = Mock(return_value=fake_result)

    with (
        patch("databricks_agent_notebooks.runtime.doctor.find_coursier", return_value="/usr/bin/cs"),
        patch("databricks_agent_notebooks.runtime.doctor.list_installed_runtimes", return_value=runtimes),
    ):
        checks = doctor_scala_connect_readiness(subprocess_run=fake_run)

    assert len(checks) == 1
    assert checks[0].name == "scala-connect(17.3, 2.13)"
    # Verify the command used the 2.13 artifact
    args = fake_run.call_args[0][0]
    assert "databricks-connect_2.13" in args[-1]
