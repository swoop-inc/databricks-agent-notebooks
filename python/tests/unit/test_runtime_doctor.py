from __future__ import annotations

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
    assert statuses["spark_home"] == "warn"
    assert statuses["databricks_cli"] == "ok"
    assert statuses["java"] == "ok"
    assert statuses["profile"] == "ok"


def test_run_checks_reports_missing_java_and_kernel(tmp_path: Path) -> None:
    from databricks_agent_notebooks.runtime.doctor import run_checks

    home = _make_runtime_home(tmp_path / "runtime-home")

    def which(name: str) -> str | None:
        if name == "databricks":
            return None
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
    assert statuses["databricks_cli"] == "fail"
    assert statuses["java"] == "fail"
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
                "launcher_contract_path": str(contract_path),
                "installed_at": "2026-03-22T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    check = check_kernel_semantics(home=home, kernel_id=kernel_id)

    assert check.status == "ok"
    assert sys.executable in check.message


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
