"""Offline environment checks for the managed Databricks notebook runtime."""

from __future__ import annotations

import configparser
import os
import re
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from databricks_agent_notebooks.runtime.home import RuntimeHome, resolve_runtime_home
from databricks_agent_notebooks.runtime.kernel import KERNEL_ID, verify_kernel


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


def _find_managed_kernel_dir(search_dirs: list[Path]) -> Path | None:
    for kernels_dir in search_dirs:
        kernel_dir = kernels_dir / KERNEL_ID
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
) -> Check:
    search_dirs = kernel_search_dirs(home=home, kernels_dir=kernels_dir)
    kernel_dir = _find_managed_kernel_dir(search_dirs)
    if kernel_dir is not None:
        return Check("kernel", "ok", f"Managed kernel found: {kernel_dir}")

    fallback_name = _find_fallback_scala_kernel(search_dirs)
    if fallback_name is not None:
        return Check(
            "kernel",
            "warn",
            f"found '{fallback_name}' but not '{KERNEL_ID}' — run 'agent-notebook install-kernel'",
        )

    return Check("kernel", "fail", f"managed kernel '{KERNEL_ID}' not found — run 'agent-notebook install-kernel'")


def check_kernel_semantics(
    home: RuntimeHome | None = None,
    kernels_dir: Path | None = None,
) -> Check:
    search_dirs = kernel_search_dirs(home=home, kernels_dir=kernels_dir)
    kernel_dir = _find_managed_kernel_dir(search_dirs)
    if kernel_dir is None:
        return Check(
            "kernel_semantics",
            "fail",
            f"managed kernel '{KERNEL_ID}' not found — run 'agent-notebook install-kernel'",
        )

    issues = verify_kernel(kernel_dir.parent)
    if not issues:
        return Check(
            "kernel_semantics",
            "ok",
            "kernel semantics verified: required JVM flag present and SPARK_HOME cleared",
        )

    return Check("kernel_semantics", "fail", "; ".join(issues))


def check_spark_home(environ: Mapping[str, str] | None = None) -> Check:
    env_map = dict(environ or os.environ)
    if env_map.get("SPARK_HOME"):
        return Check("spark_home", "warn", "SPARK_HOME is set — should be unset for Databricks Connect")
    return Check("spark_home", "ok", "SPARK_HOME is not set")


def check_databricks_cli() -> Check:
    if shutil.which("databricks"):
        return Check("databricks_cli", "ok", "databricks CLI found on PATH")
    return Check("databricks_cli", "fail", "databricks CLI not found on PATH")


def check_profile(profile: str) -> Check:
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.is_file():
        return Check("profile", "fail", f"~/.databrickscfg not found — cannot verify profile '{profile}'")

    config = configparser.ConfigParser()
    config.read(cfg_path, encoding="utf-8")

    if profile == config.default_section:
        return Check("profile", "ok", f"profile '{profile}' found in ~/.databrickscfg")
    if config.has_section(profile):
        return Check("profile", "ok", f"profile '{profile}' found in ~/.databrickscfg")
    return Check("profile", "fail", f"profile '{profile}' not found in ~/.databrickscfg")


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
    if major >= 11:
        return Check("java", "ok", f"Java {major} (>= 11)")
    return Check("java", "fail", f"Java {major} is below minimum (11)")


def run_checks(
    profile: str | None = None,
    kernels_dir: Path | None = None,
    env: Mapping[str, str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[Check]:
    home = resolve_runtime_home(env)
    checks = [
        check_coursier(),
        check_kernel(home=home, kernels_dir=kernels_dir),
        check_kernel_semantics(home=home, kernels_dir=kernels_dir),
        check_spark_home(environ=environ),
        check_databricks_cli(),
        check_java(),
    ]
    if profile is not None:
        checks.append(check_profile(profile))
    return checks
