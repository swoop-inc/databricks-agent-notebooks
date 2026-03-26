from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from databricks_agent_notebooks.runtime.connect import (
    ConnectRuntimeSpec,
    SERVERLESS_CONNECT_OVERRIDE_ENV_VAR,
    SERVERLESS_RUNTIME_CACHE_FILENAME,
    ServerlessRuntimeValidationError,
    ensure_serverless_runtime,
    materialize_managed_runtime,
    normalize_connect_line,
    normalize_databricks_runtime_line,
)
from databricks_agent_notebooks.runtime.home import RuntimeHome
from databricks_agent_notebooks.runtime.inventory import materialize_runtime_installation


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


def _write_databricks_cfg(tmp_path: Path, *, profile: str, host: str) -> Path:
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text(f"[{profile}]\nhost = {host}\ntoken = abc\n", encoding="utf-8")
    return cfg


def _write_databricks_cfg_profiles(tmp_path: Path, profiles: dict[str, str]) -> Path:
    cfg = tmp_path / ".databrickscfg"
    lines: list[str] = []
    for profile, host in profiles.items():
        lines.extend([f"[{profile}]", f"host = {host}", "token = abc", ""])
    cfg.write_text("\n".join(lines), encoding="utf-8")
    return cfg


def test_normalize_databricks_runtime_line_accepts_cluster_spark_version() -> None:
    assert normalize_databricks_runtime_line("16.4.x-photon-scala2.12") == "16.4"


def test_normalize_connect_line_trims_patch_version() -> None:
    assert normalize_connect_line("16.4.7") == "16.4"


def test_connect_runtime_spec_builds_stable_runtime_identity() -> None:
    spec = ConnectRuntimeSpec.for_cluster(databricks_line="16.4.x-scala2.12", python_line="3.12")

    assert spec.databricks_line == "16.4"
    assert spec.connect_line == "16.4"
    assert spec.python_line == "3.12"
    assert spec.runtime_id == "dbr-16.4-python-3.12"


def test_materialize_managed_runtime_creates_virtualenv_and_installs_requested_connect_line(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    spec = ConnectRuntimeSpec.for_cluster(databricks_line="16.4.x-scala2.12", python_line="3.12")
    commands: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool) -> None:
        commands.append(command)
        if command[:3] == [sys.executable, "-m", "venv"]:
            runtime_python = home.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python"
            runtime_python.parent.mkdir(parents=True, exist_ok=True)
            runtime_python.write_text("", encoding="utf-8")

    runtime = materialize_managed_runtime(
        home,
        spec=spec,
        subprocess_run=fake_run,
        package_install_target=["-e", "/tmp/repo/python"],
    )

    assert runtime.runtime_id == "dbr-16.4-python-3.12"
    assert runtime.python_executable == home.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python"
    assert commands == [
        [sys.executable, "-m", "venv", str(home.runtimes_dir / spec.runtime_id / "venv")],
        [str(runtime.python_executable), "-m", "pip", "install", "--upgrade", "pip"],
        [
            str(runtime.python_executable),
            "-m",
            "pip",
            "install",
            "-e",
            "/tmp/repo/python",
            "databricks-connect==16.4.*",
        ],
    ]


def test_materialize_managed_runtime_reuses_existing_python_runtime(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    spec = ConnectRuntimeSpec.for_cluster(databricks_line="16.4.x-scala2.12", python_line="3.12")
    runtime_python = home.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python"
    runtime_python.parent.mkdir(parents=True, exist_ok=True)
    runtime_python.write_text("", encoding="utf-8")
    materialize_runtime_installation(home, databricks_line=spec.databricks_line, python_line=spec.python_line)

    called = False

    def fake_run(command: list[str], *, check: bool) -> None:
        del command, check
        nonlocal called
        called = True

    runtime = materialize_managed_runtime(
        home,
        spec=spec,
        subprocess_run=fake_run,
        package_install_target=["-e", "/tmp/repo/python"],
    )

    assert runtime.python_executable == runtime_python
    assert called is False


def test_materialize_managed_runtime_repairs_partial_runtime_without_receipt(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    spec = ConnectRuntimeSpec.for_cluster(databricks_line="16.4.x-scala2.12", python_line="3.12")
    runtime_python = home.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python"
    runtime_python.parent.mkdir(parents=True, exist_ok=True)
    runtime_python.write_text("", encoding="utf-8")
    commands: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool) -> None:
        del check
        commands.append(command)

    runtime = materialize_managed_runtime(
        home,
        spec=spec,
        subprocess_run=fake_run,
        package_install_target=["-e", "/tmp/repo/python"],
    )

    assert runtime.python_executable == runtime_python
    assert commands == [
        [str(runtime_python), "-m", "pip", "install", "--upgrade", "pip"],
        [
            str(runtime_python),
            "-m",
            "pip",
            "install",
            "-e",
            "/tmp/repo/python",
            "databricks-connect==16.4.*",
        ],
    ]


def test_ensure_serverless_runtime_uses_conservative_default_and_caches_workspace_profile_success(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    cfg = _write_databricks_cfg(tmp_path, profile="prod", host="https://workspace.example")
    materialized: list[str] = []
    validated: list[tuple[str, str | None]] = []

    def fake_materialize(
        home_arg: RuntimeHome,
        *,
        spec: ConnectRuntimeSpec,
        subprocess_run,
        package_install_target,
    ) -> SimpleNamespace:
        del subprocess_run, package_install_target
        materialized.append(spec.connect_line)
        return SimpleNamespace(
            runtime_id=spec.runtime_id,
            databricks_line=spec.databricks_line,
            connect_line=spec.connect_line,
            python_line=spec.python_line,
            install_root=home_arg.runtimes_dir / spec.runtime_id,
            python_executable=home_arg.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python",
        )

    def fake_validate(runtime, *, profile: str | None, subprocess_run) -> None:
        del subprocess_run
        validated.append((runtime.connect_line, profile))

    runtime = ensure_serverless_runtime(
        profile="prod",
        home=home,
        environ={"DATABRICKS_CONFIG_FILE": str(cfg)},
        materialize_runtime=fake_materialize,
        validate_runtime=fake_validate,
    )

    assert runtime.connect_line == "16.4"
    assert materialized == ["16.4"]
    assert validated == [("16.4", "prod")]
    cache_path = home.config_dir / SERVERLESS_RUNTIME_CACHE_FILENAME
    assert json.loads(cache_path.read_text(encoding="utf-8")) == {
        "entries": {
            "profile:prod|host:https://workspace.example": "16.4",
        },
        "version": "1",
    }


def test_ensure_serverless_runtime_falls_back_to_older_supported_line_when_validation_fails(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    cfg = _write_databricks_cfg(tmp_path, profile="prod", host="https://workspace.example")
    materialized: list[str] = []
    validated: list[str] = []

    def fake_materialize(
        home_arg: RuntimeHome,
        *,
        spec: ConnectRuntimeSpec,
        subprocess_run,
        package_install_target,
    ) -> SimpleNamespace:
        del subprocess_run, package_install_target
        materialized.append(spec.connect_line)
        return SimpleNamespace(
            runtime_id=spec.runtime_id,
            databricks_line=spec.databricks_line,
            connect_line=spec.connect_line,
            python_line=spec.python_line,
            install_root=home_arg.runtimes_dir / spec.runtime_id,
            python_executable=home_arg.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python",
        )

    def fake_validate(runtime, *, profile: str | None, subprocess_run) -> None:
        del profile, subprocess_run
        validated.append(runtime.connect_line)
        if runtime.connect_line == "16.4":
            raise ServerlessRuntimeValidationError("16.4 is incompatible")

    runtime = ensure_serverless_runtime(
        profile="prod",
        home=home,
        environ={"DATABRICKS_CONFIG_FILE": str(cfg)},
        materialize_runtime=fake_materialize,
        validate_runtime=fake_validate,
    )

    assert runtime.connect_line == "15.4"
    assert materialized == ["16.4", "15.4"]
    assert validated == ["16.4", "15.4"]


def test_ensure_serverless_runtime_reuses_cached_workspace_profile_line_without_revalidation(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    cfg = _write_databricks_cfg(tmp_path, profile="prod", host="https://workspace.example")
    cache_path = home.config_dir / SERVERLESS_RUNTIME_CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "version": "1",
                "entries": {
                    "profile:prod|host:https://workspace.example": "15.4",
                },
            }
        ),
        encoding="utf-8",
    )
    materialized: list[str] = []

    def fake_materialize(
        home_arg: RuntimeHome,
        *,
        spec: ConnectRuntimeSpec,
        subprocess_run,
        package_install_target,
    ) -> SimpleNamespace:
        del subprocess_run, package_install_target
        materialized.append(spec.connect_line)
        return SimpleNamespace(
            runtime_id=spec.runtime_id,
            databricks_line=spec.databricks_line,
            connect_line=spec.connect_line,
            python_line=spec.python_line,
            install_root=home_arg.runtimes_dir / spec.runtime_id,
            python_executable=home_arg.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python",
        )

    def fail_validate(runtime, *, profile: str | None, subprocess_run) -> None:
        del runtime, profile, subprocess_run
        raise AssertionError("cached serverless line should not be revalidated")

    runtime = ensure_serverless_runtime(
        profile="prod",
        home=home,
        environ={"DATABRICKS_CONFIG_FILE": str(cfg)},
        materialize_runtime=fake_materialize,
        validate_runtime=fail_validate,
    )

    assert runtime.connect_line == "15.4"
    assert materialized == ["15.4"]


def test_ensure_serverless_runtime_env_override_takes_precedence_over_cached_policy(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    cfg = _write_databricks_cfg(tmp_path, profile="prod", host="https://workspace.example")
    cache_path = home.config_dir / SERVERLESS_RUNTIME_CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "version": "1",
                "entries": {
                    "profile:prod|host:https://workspace.example": "15.4",
                },
            }
        ),
        encoding="utf-8",
    )
    materialized: list[str] = []
    validated: list[str] = []

    def fake_materialize(
        home_arg: RuntimeHome,
        *,
        spec: ConnectRuntimeSpec,
        subprocess_run,
        package_install_target,
    ) -> SimpleNamespace:
        del subprocess_run, package_install_target
        materialized.append(spec.connect_line)
        return SimpleNamespace(
            runtime_id=spec.runtime_id,
            databricks_line=spec.databricks_line,
            connect_line=spec.connect_line,
            python_line=spec.python_line,
            install_root=home_arg.runtimes_dir / spec.runtime_id,
            python_executable=home_arg.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python",
        )

    def fake_validate(runtime, *, profile: str | None, subprocess_run) -> None:
        del profile, subprocess_run
        validated.append(runtime.connect_line)

    runtime = ensure_serverless_runtime(
        profile="prod",
        home=home,
        environ={
            "DATABRICKS_CONFIG_FILE": str(cfg),
            SERVERLESS_CONNECT_OVERRIDE_ENV_VAR: "16.4.15",
        },
        materialize_runtime=fake_materialize,
        validate_runtime=fake_validate,
    )

    assert runtime.connect_line == "16.4"
    assert materialized == ["16.4"]
    assert validated == ["16.4"]
    assert json.loads(cache_path.read_text(encoding="utf-8"))["entries"] == {
        "profile:prod|host:https://workspace.example": "15.4"
    }


def test_ensure_serverless_runtime_writes_distinct_cache_entries_for_env_selected_profiles(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    cfg = _write_databricks_cfg_profiles(
        tmp_path,
        {
            "dev": "https://dev.workspace.example",
            "prod": "https://prod.workspace.example",
        },
    )
    validated: list[tuple[str, str | None]] = []

    def fake_materialize(
        home_arg: RuntimeHome,
        *,
        spec: ConnectRuntimeSpec,
        subprocess_run,
        package_install_target,
    ) -> SimpleNamespace:
        del subprocess_run, package_install_target
        return SimpleNamespace(
            runtime_id=spec.runtime_id,
            databricks_line=spec.databricks_line,
            connect_line=spec.connect_line,
            python_line=spec.python_line,
            install_root=home_arg.runtimes_dir / spec.runtime_id,
            python_executable=home_arg.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python",
        )

    def fake_validate(runtime, *, profile: str | None, subprocess_run) -> None:
        del subprocess_run
        validated.append((runtime.connect_line, profile))

    ensure_serverless_runtime(
        profile=None,
        home=home,
        environ={
            "DATABRICKS_CONFIG_FILE": str(cfg),
            "DATABRICKS_CONFIG_PROFILE": "dev",
        },
        materialize_runtime=fake_materialize,
        validate_runtime=fake_validate,
    )
    ensure_serverless_runtime(
        profile=None,
        home=home,
        environ={
            "DATABRICKS_CONFIG_FILE": str(cfg),
            "DATABRICKS_CONFIG_PROFILE": "prod",
        },
        materialize_runtime=fake_materialize,
        validate_runtime=fake_validate,
    )

    assert validated == [("16.4", "dev"), ("16.4", "prod")]
    cache_path = home.config_dir / SERVERLESS_RUNTIME_CACHE_FILENAME
    assert json.loads(cache_path.read_text(encoding="utf-8"))["entries"] == {
        "profile:dev|host:https://dev.workspace.example": "16.4",
        "profile:prod|host:https://prod.workspace.example": "16.4",
    }


def test_ensure_serverless_runtime_reuses_cached_line_for_env_selected_profile(tmp_path: Path) -> None:
    home = _make_runtime_home(tmp_path / "runtime-home")
    cfg = _write_databricks_cfg_profiles(
        tmp_path,
        {
            "dev": "https://dev.workspace.example",
            "prod": "https://prod.workspace.example",
        },
    )
    cache_path = home.config_dir / SERVERLESS_RUNTIME_CACHE_FILENAME
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "version": "1",
                "entries": {
                    "profile:dev|host:https://dev.workspace.example": "15.4",
                    "profile:prod|host:https://prod.workspace.example": "16.4",
                },
            }
        ),
        encoding="utf-8",
    )
    materialized: list[str] = []

    def fake_materialize(
        home_arg: RuntimeHome,
        *,
        spec: ConnectRuntimeSpec,
        subprocess_run,
        package_install_target,
    ) -> SimpleNamespace:
        del subprocess_run, package_install_target
        materialized.append(spec.connect_line)
        return SimpleNamespace(
            runtime_id=spec.runtime_id,
            databricks_line=spec.databricks_line,
            connect_line=spec.connect_line,
            python_line=spec.python_line,
            install_root=home_arg.runtimes_dir / spec.runtime_id,
            python_executable=home_arg.runtimes_dir / spec.runtime_id / "venv" / "bin" / "python",
        )

    def fail_validate(runtime, *, profile: str | None, subprocess_run) -> None:
        del runtime, profile, subprocess_run
        raise AssertionError("env-selected cached serverless line should not be revalidated")

    dev_runtime = ensure_serverless_runtime(
        profile=None,
        home=home,
        environ={
            "DATABRICKS_CONFIG_FILE": str(cfg),
            "DATABRICKS_CONFIG_PROFILE": "dev",
        },
        materialize_runtime=fake_materialize,
        validate_runtime=fail_validate,
    )
    prod_runtime = ensure_serverless_runtime(
        profile=None,
        home=home,
        environ={
            "DATABRICKS_CONFIG_FILE": str(cfg),
            "DATABRICKS_CONFIG_PROFILE": "prod",
        },
        materialize_runtime=fake_materialize,
        validate_runtime=fail_validate,
    )

    assert dev_runtime.connect_line == "15.4"
    assert prod_runtime.connect_line == "16.4"
    assert materialized == ["15.4", "16.4"]
