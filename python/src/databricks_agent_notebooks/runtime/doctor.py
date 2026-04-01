"""Offline environment checks for the managed Databricks notebook runtime."""

from __future__ import annotations

import configparser
import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from databricks_agent_notebooks._constants import ScalaVariant, scala_variant_for_dbr
from databricks_agent_notebooks.runtime.home import RuntimeHome, resolve_runtime_home
from databricks_agent_notebooks.runtime.inventory import list_installed_runtimes
from databricks_agent_notebooks.runtime.kernel import KERNEL_ID, find_coursier, load_launcher_contract, verify_kernel
from databricks_agent_notebooks.runtime.scala_connect import ARTIFACT_VERSION_RE


@dataclass(frozen=True)
class Check:
    """Result of a single doctor check."""

    name: str
    status: str
    message: str


def kernel_search_dirs(
    home: RuntimeHome | None = None,
    kernels_dir: Path | None = None,
) -> list[Path]:
    """Return candidate kernelspec roots in priority order."""
    if kernels_dir is not None:
        return [kernels_dir]

    resolved_home = home or resolve_runtime_home()
    dirs = [
        resolved_home.kernels_dir,
        Path.home() / "Library" / "Jupyter" / "kernels",
        Path.home() / ".local" / "share" / "jupyter" / "kernels",
    ]

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in dirs:
        if path not in seen:
            deduped.append(path)
            seen.add(path)
    return deduped


def _install_command(kernel_id: str, *, force: bool = False) -> str:
    parts = ["agent-notebook", "kernels", "install"]
    if force:
        parts.append("--force")
    if kernel_id != KERNEL_ID:
        parts.extend(["--id", kernel_id])
    return shlex.join(parts)


def _find_managed_kernel_dir(search_dirs: list[Path], kernel_id: str = KERNEL_ID) -> Path | None:
    for kernels_dir in search_dirs:
        kernel_dir = kernels_dir / kernel_id
        if kernel_dir.is_dir():
            return kernel_dir
    return None


def _find_fallback_scala_kernel(search_dirs: list[Path]) -> str | None:
    for kernels_dir in search_dirs:
        if not kernels_dir.is_dir():
            continue
        for child in kernels_dir.iterdir():
            if not child.is_dir():
                continue
            if not (child / "kernel.json").is_file():
                continue
            lower_name = child.name.lower()
            if "scala" in lower_name or "almond" in lower_name:
                return child.name
    return None


def check_coursier() -> Check:
    if shutil.which("coursier") or shutil.which("cs"):
        return Check("coursier", "ok", "coursier found on PATH")
    return Check("coursier", "fail", "coursier not found on PATH (need 'coursier' or 'cs')")


def check_kernel(
    home: RuntimeHome | None = None,
    kernels_dir: Path | None = None,
    kernel_id: str = KERNEL_ID,
) -> Check:
    search_dirs = kernel_search_dirs(home=home, kernels_dir=kernels_dir)
    kernel_dir = _find_managed_kernel_dir(search_dirs, kernel_id=kernel_id)
    if kernel_dir is not None:
        return Check("kernel", "ok", f"Managed kernel found: {kernel_dir}")

    fallback_name = _find_fallback_scala_kernel(search_dirs)
    if fallback_name is not None:
        return Check(
            "kernel",
            "warn",
            f"found '{fallback_name}' but not '{kernel_id}' — run '{_install_command(kernel_id)}'",
        )

    return Check("kernel", "fail", f"managed kernel '{kernel_id}' not found — run '{_install_command(kernel_id)}'")


def check_kernel_semantics(
    home: RuntimeHome | None = None,
    kernels_dir: Path | None = None,
    kernel_id: str = KERNEL_ID,
) -> Check:
    search_dirs = kernel_search_dirs(home=home, kernels_dir=kernels_dir)
    kernel_dir = _find_managed_kernel_dir(search_dirs, kernel_id=kernel_id)
    if kernel_dir is None:
        return Check(
            "kernel_semantics",
            "fail",
            f"managed kernel '{kernel_id}' not found — run '{_install_command(kernel_id)}'",
        )

    issues = verify_kernel(kernel_dir.parent, kernel_id=kernel_id)
    if not issues:
        contract = load_launcher_contract(kernel_dir)
        launcher_identity = contract.launcher_path if contract is not None else "unknown launcher"
        return Check(
            "kernel_semantics",
            "ok",
            f"kernel semantics verified via launcher contract: {launcher_identity}",
        )

    return Check(
        "kernel_semantics",
        "fail",
        f"{'; '.join(issues)} — run '{_install_command(kernel_id, force=True)}'",
    )




def check_pyspark() -> Check:
    """Check whether pyspark is importable (needed for Python LOCAL_SPARK).

    Uses a two-phase check: ``find_spec`` as a fast negative, then an actual
    import to catch stump directories or broken installs where ``find_spec``
    returns a truthy ModuleSpec but the package cannot actually be loaded.
    """
    import importlib.metadata  # noqa: PLC0415
    import importlib.util  # noqa: PLC0415

    spec = importlib.util.find_spec("pyspark")
    if spec is None:
        return Check(
            "pyspark",
            "warn",
            "pyspark not found — required for Python LOCAL_SPARK "
            "(pip install pyspark or uv pip install pyspark)",
        )

    # find_spec returned truthy — verify the package actually loads.
    try:
        import pyspark  # noqa: PLC0415
    except ImportError as exc:
        return Check(
            "pyspark",
            "warn",
            f"pyspark directory found but not importable ({exc}) — "
            "reinstall with: pip install pyspark (or uv pip install pyspark)",
        )

    # Determine version
    try:
        version = importlib.metadata.version("pyspark")
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"

    # Determine source: standalone pyspark vs databricks-connect
    origin = getattr(spec, "origin", None) or ""
    source = "databricks-connect" if "databricks" in origin.lower() else "standalone"

    return Check("pyspark", "ok", f"pyspark {version} ({source})")


def _resolve_databricks_cfg_path(environ: Mapping[str, str] | None = None) -> Path:
    env_map = dict(os.environ)
    if environ is not None:
        env_map.update(environ)
    override = env_map.get("DATABRICKS_CONFIG_FILE")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".databrickscfg"


def check_profile(profile: str, environ: Mapping[str, str] | None = None) -> Check:
    cfg_path = _resolve_databricks_cfg_path(environ)
    if not cfg_path.is_file():
        return Check("profile", "fail", f"{cfg_path} not found — cannot verify profile '{profile}'")

    config = configparser.ConfigParser()
    config.read(cfg_path, encoding="utf-8")

    if profile == config.default_section:
        if config.defaults():
            return Check("profile", "ok", f"profile '{profile}' found in {cfg_path}")
        return Check("profile", "fail", f"profile '{profile}' not found in {cfg_path}")
    if config.has_section(profile):
        return Check("profile", "ok", f"profile '{profile}' found in {cfg_path}")
    return Check("profile", "fail", f"profile '{profile}' not found in {cfg_path}")


def _parse_java_major_version(output: str) -> int | None:
    match = re.search(r'version "([^"]+)"', output)
    if not match:
        return None

    version = match.group(1)
    if version.startswith("1."):
        legacy_match = re.match(r"1\.(\d+)", version)
        if legacy_match:
            return int(legacy_match.group(1))
        return None

    major = version.split(".", 1)[0]
    if major.isdigit():
        return int(major)
    return None


def check_java() -> Check:
    if shutil.which("java") is None:
        return Check("java", "fail", "java not found on PATH")

    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        return Check("java", "fail", "java not found on PATH")

    output = (result.stderr or "") + (result.stdout or "")
    major = _parse_java_major_version(output)
    if major is None:
        return Check("java", "fail", f"could not parse Java version from: {output.strip()}")
    if major >= 17:
        return Check("java", "ok", f"Java {major} (>= 17, supports all DBR versions)")
    if major >= 11:
        return Check("java", "warn", f"Java {major} (>= 11 but < 17; DBR 17+ requires JDK 17)")
    return Check("java", "fail", f"Java {major} is below minimum (11)")


def check_scala_connect_cached(
    connect_line: str,
    variant: ScalaVariant,
    coursier_bin: str,
    subprocess_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Check:
    """Check whether Databricks Connect JARs are cached locally for a given Connect line."""
    coordinate = f"com.databricks:{variant.maven_artifact}:{connect_line}.+"
    check_name = f"scala-connect({connect_line}, {variant.scala_version})"
    try:
        result = subprocess_run(
            [coursier_bin, "fetch", "--mode", "offline", coordinate],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        return Check(check_name, "warn", f"not cached ({coordinate}) — timed out checking cache")

    if result.returncode == 0:
        for line in result.stdout.strip().splitlines():
            match = ARTIFACT_VERSION_RE.search(line)
            if match:
                version = match.group("version")
                return Check(check_name, "ok", f"cached ({variant.maven_artifact} {version})")
        return Check(check_name, "ok", f"cached ({coordinate})")

    return Check(check_name, "warn", f"not cached ({coordinate}) — will download on first notebook launch")


def doctor_scala_connect_readiness(
    home: RuntimeHome | None = None,
    env: Mapping[str, str] | None = None,
    subprocess_run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> list[Check]:
    """Report coursier cache status for each materialized Scala Connect line."""
    coursier_bin = find_coursier()
    if coursier_bin is None:
        return []

    runtimes = list_installed_runtimes(home=home, env=env)
    if not runtimes:
        return []

    seen: set[tuple[str, str]] = set()
    checks: list[Check] = []
    for runtime in runtimes:
        try:
            major = int(runtime.databricks_line.split(".")[0])
        except (ValueError, IndexError):
            continue
        variant = scala_variant_for_dbr(major)
        key = (runtime.databricks_line, variant.scala_version)
        if key in seen:
            continue
        seen.add(key)
        checks.append(
            check_scala_connect_cached(
                runtime.databricks_line,
                variant,
                coursier_bin,
                subprocess_run=subprocess_run,
            )
        )

    return checks


def run_checks(
    profile: str | None = None,
    kernels_dir: Path | None = None,
    kernel_id: str = KERNEL_ID,
    env: Mapping[str, str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[Check]:
    home = resolve_runtime_home(env)
    checks = [
        check_coursier(),
        check_kernel(home=home, kernels_dir=kernels_dir, kernel_id=kernel_id),
        check_kernel_semantics(home=home, kernels_dir=kernels_dir, kernel_id=kernel_id),
        check_java(),
        check_pyspark(),
    ]
    if profile is not None:
        checks.append(check_profile(profile, environ=environ))
    return checks
