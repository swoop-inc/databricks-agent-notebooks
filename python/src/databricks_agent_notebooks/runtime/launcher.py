"""Managed launcher/bootstrap boundary for generated Scala kernelspecs."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from databricks_agent_notebooks.runtime.manifest import LauncherKernelContract, read_json_record

LAUNCHER_MODULE = "databricks_agent_notebooks.runtime.launcher"
CONNECTION_FILE_TOKEN = "{connection_file}"


def build_launcher_argv(contract_path: Path) -> list[str]:
    """Return the generated kernelspec argv for the managed launcher boundary."""
    return [
        sys.executable,
        "-m",
        LAUNCHER_MODULE,
        "--launcher-contract",
        str(contract_path),
        "--connection-file",
        CONNECTION_FILE_TOKEN,
    ]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="agent-notebook-kernel-launcher")
    parser.add_argument("--launcher-contract", required=True, help="Path to launcher-contract.json")
    parser.add_argument("--connection-file", required=True, help="Kernel connection file provided by Jupyter")
    return parser.parse_args(argv)


def _resolve_bootstrap_argv(contract: LauncherKernelContract, connection_file: str) -> list[str]:
    if not contract.bootstrap_argv:
        raise ValueError("launcher contract missing bootstrap_argv")
    return [connection_file if part == CONNECTION_FILE_TOKEN else part for part in contract.bootstrap_argv]


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    contract = read_json_record(Path(args.launcher_contract), LauncherKernelContract)
    bootstrap_argv = _resolve_bootstrap_argv(contract, args.connection_file)
    env = dict(os.environ)
    env["SPARK_HOME"] = ""
    os.execvpe(bootstrap_argv[0], bootstrap_argv, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
