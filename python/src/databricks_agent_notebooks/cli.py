"""CLI entry point for agent-notebook — normalize, inject, execute, and render notebooks.

Provides subcommands for the full notebook pipeline (run), standalone rendering,
cluster discovery, kernel installation, and environment validation.  Follows the
same argparse/subparsers/dispatch-table pattern as ``libs.continuum.cli``.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from databricks_agent_notebooks.config.frontmatter import DatabricksConfig, merge_config
from databricks_agent_notebooks.execution.executor import execute_notebook
from databricks_agent_notebooks.execution.injection import inject_cells
from databricks_agent_notebooks.execution.rendering import render
from databricks_agent_notebooks.formats.conversion import to_notebook, validate_single_language
from databricks_agent_notebooks.integrations.databricks.clusters import ClusterError, default_service
from databricks_agent_notebooks.runtime.doctor import Check, run_checks
from databricks_agent_notebooks.runtime.kernel import install_kernel, list_installed_kernels, remove_kernel

import nbformat


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-notebook",
        description="Databricks notebook execution: normalize, inject, execute, render.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # -- run --
    run = subparsers.add_parser("run", help="Normalize, inject, execute, and render a notebook")
    run.add_argument("file", help="Input file (markdown, ipynb, or Databricks source)")
    run.add_argument("--cluster", default=None, help="Cluster name or ID")
    run.add_argument("--profile", default=None, help="Databricks CLI profile")
    run.add_argument("--format", default="all", choices=["all", "md", "html"], dest="fmt", help="Output format (default: all)")
    run.add_argument("--output-dir", default=None, help="Output directory (default: input file's parent)")
    run.add_argument("--timeout", type=int, default=600, help="Per-cell timeout in seconds (default: 600)")
    run.add_argument("--allow-errors", action="store_true", help="Continue execution on cell errors")
    run.add_argument("--no-inject-session", action="store_true", help="Skip Databricks Connect session injection")
    run.add_argument("--language", default=None, help="Override notebook language (python, scala)")

    # -- clusters --
    clusters = subparsers.add_parser("clusters", help="List Databricks clusters")
    clusters.add_argument("--profile", required=True, help="Databricks CLI profile")

    # -- install-kernel --
    ik = subparsers.add_parser("install-kernel", help="Install the Databricks Connect Almond kernel")
    ik.add_argument("--kernels-dir", default=None, help="Jupyter kernels directory")

    # -- kernels --
    kernels = subparsers.add_parser("kernels", help="Manage installed Databricks kernels")
    kernel_subparsers = kernels.add_subparsers(dest="kernels_command", required=True)

    kernels_install = kernel_subparsers.add_parser("install", help="Install the Databricks Connect Almond kernel")
    kernels_install.add_argument("--kernels-dir", default=None, help="Jupyter kernels directory")

    kernels_list = kernel_subparsers.add_parser("list", help="List installed kernels under runtime-home and overrides")
    kernels_list.add_argument(
        "--kernels-dir",
        action="append",
        default=[],
        help="Additional kernels directory to inspect (can be passed multiple times)",
    )

    kernels_remove = kernel_subparsers.add_parser("remove", help="Remove a named installed kernel")
    kernels_remove.add_argument("name", help="Kernel directory name to remove")
    kernels_remove.add_argument(
        "--kernels-dir",
        action="append",
        default=[],
        help="Additional kernels directory to search (can be passed multiple times)",
    )

    kernels_doctor = kernel_subparsers.add_parser("doctor", help="Validate kernel installation and environment")
    kernels_doctor.add_argument("--profile", default=None, help="Databricks CLI profile to validate")

    # -- render --
    rnd = subparsers.add_parser("render", help="Render an already-executed notebook")
    rnd.add_argument("file", help="Path to executed .ipynb notebook")
    rnd.add_argument("--format", default="all", choices=["all", "md", "html"], dest="fmt", help="Output format (default: all)")
    rnd.add_argument("--output-dir", default=None, help="Output directory (default: input file's parent)")

    # -- doctor --
    doctor = subparsers.add_parser("doctor", help="Validate environment for Databricks Connect")
    doctor.add_argument("--profile", default=None, help="Databricks CLI profile to validate")

    # -- help --
    subparsers.add_parser("help", help="Show usage information")

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> int:
    """Full pipeline: normalize -> merge config -> inject -> execute -> render."""
    input_path = Path(args.file).resolve()
    if not input_path.is_file():
        print(f"error: file not found: {args.file}", file=sys.stderr)
        return 1

    # Step 1: Normalize to notebook
    notebook, frontmatter_config = to_notebook(input_path)

    # Step 1b: Validate single language (fail fast on mixed-language notebooks)
    try:
        validate_single_language(notebook)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # Step 2: Merge config
    config = merge_config(
        frontmatter_config or DatabricksConfig(),
        cli_profile=args.profile,
        cli_cluster=args.cluster,
        cli_language=args.language,
    )

    # Step 3: Resolve cluster
    service = default_service()
    if config.cluster:
        try:
            cluster = service.resolve_cluster(config.cluster, config.profile or "DEFAULT")
            config = DatabricksConfig(
                profile=config.profile,
                cluster=cluster.cluster_id,
                language=config.language,
            )
        except ClusterError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    else:
        print(
            "No cluster specified — using serverless compute "
            "(Beta for Scala, some limitations apply).",
            file=sys.stderr,
        )

    # Step 4: Inject session setup
    if not args.no_inject_session:
        notebook = inject_cells(notebook, config, input_path)

    # Step 5: Set up output directory
    stem = input_path.stem
    output_dir = Path(args.output_dir).resolve() if args.output_dir else input_path.parent
    run_output_dir = output_dir / f"{stem}_output"
    run_output_dir.mkdir(parents=True, exist_ok=True)

    # Step 6: Write notebook to temp file for execution
    with tempfile.NamedTemporaryFile(
        suffix=".ipynb", dir=str(run_output_dir), delete=False, mode="w"
    ) as tmp:
        nbformat.write(notebook, tmp)
        temp_notebook = Path(tmp.name)

    # Step 7: If input was not .ipynb, save pre-execution notebook
    if input_path.suffix.lower() != ".ipynb":
        pre_exec_path = run_output_dir / f"{stem}.ipynb"
        shutil.copy2(temp_notebook, pre_exec_path)

    # Step 8: Execute — use the kernel from the notebook's own metadata
    kernel_name = notebook.metadata.get("kernelspec", {}).get("name", "scala212-dbr-connect")
    result = execute_notebook(
        temp_notebook,
        kernel=kernel_name,
        timeout=args.timeout,
        allow_errors=args.allow_errors,
    )

    # Step 9: Copy executed notebook to output dir
    executed_path = run_output_dir / f"{stem}.executed.ipynb"
    if result.output_path and result.output_path.is_file():
        shutil.copy2(result.output_path, executed_path)
    elif result.success:
        # output_path might be the same as the temp file
        shutil.copy2(temp_notebook, executed_path)

    # Step 10: Render
    if executed_path.is_file():
        render_paths = render(executed_path, run_output_dir, args.fmt)
    else:
        render_paths = {}

    # Step 11: Clean up temp file
    try:
        temp_notebook.unlink(missing_ok=True)
        # Also clean up the default executed output if it exists alongside temp
        default_executed = temp_notebook.with_suffix(".executed.ipynb")
        if default_executed.is_file() and default_executed != executed_path:
            default_executed.unlink(missing_ok=True)
    except OSError:
        pass

    # Step 12: Print summary
    if result.success:
        print(f"Execution succeeded ({result.duration_seconds:.1f}s)")
    else:
        print(f"Execution failed ({result.duration_seconds:.1f}s): {result.error}", file=sys.stderr)

    print(f"Output directory: {run_output_dir}")
    for fmt_name, path in render_paths.items():
        print(f"  {fmt_name}: {path}")

    return 0 if result.success else 1


def _cmd_clusters(args: argparse.Namespace) -> int:
    """List available Databricks clusters."""
    service = default_service()
    try:
        clusters = service.list_clusters(args.profile)
    except ClusterError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not clusters:
        print("No clusters found.", file=sys.stderr)
        return 0

    # Header
    print(f"{'NAME':<40} {'STATE':<12} {'ID'}")
    print("-" * 80)
    for c in clusters:
        print(f"{c.cluster_name:<40} {c.state:<12} {c.cluster_id}")

    return 0


def _cmd_install_kernel(args: argparse.Namespace) -> int:
    return _cmd_kernels_install(args)


def _resolve_kernel_dir_args(args: argparse.Namespace) -> list[Path]:
    raw_dirs = getattr(args, "kernels_dir", None)
    if raw_dirs is None:
        return []
    if isinstance(raw_dirs, list):
        return [Path(kernels_dir) for kernels_dir in raw_dirs]
    return [Path(raw_dirs)]


def _cmd_kernels_install(args: argparse.Namespace) -> int:
    """Install the Databricks Connect Almond kernel."""
    kernel_dirs = _resolve_kernel_dir_args(args)
    kernels_dir = kernel_dirs[0] if kernel_dirs else None
    try:
        kernel_dir = install_kernel(kernels_dir=kernels_dir)
        print(f"Kernel installed: {kernel_dir}")
        return 0
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _cmd_kernels_list(args: argparse.Namespace) -> int:
    """List installed kernels from runtime-home and any explicit override dirs."""
    kernels = list_installed_kernels(kernels_dirs=_resolve_kernel_dir_args(args))
    if not kernels:
        print("No kernels installed.")
        return 0

    print(f"{'NAME':<24} {'SOURCE':<20} DIRECTORY")
    for kernel in kernels:
        print(f"{kernel.name:<24} {kernel.source:<20} {kernel.directory}")
    return 0


def _cmd_kernels_remove(args: argparse.Namespace) -> int:
    """Remove a named installed kernel safely."""
    try:
        removed_dir = remove_kernel(args.name, kernels_dirs=_resolve_kernel_dir_args(args))
        print(f"Kernel removed: {removed_dir}")
        return 0
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def _cmd_render(args: argparse.Namespace) -> int:
    """Render an already-executed notebook."""
    input_path = Path(args.file).resolve()
    if not input_path.is_file():
        print(f"error: file not found: {args.file}", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir).resolve() if args.output_dir else input_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        render_paths = render(input_path, output_dir, args.fmt)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    for fmt_name, path in render_paths.items():
        print(f"{fmt_name}: {path}")

    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    return _cmd_kernels_doctor(args)


def _cmd_kernels_doctor(args: argparse.Namespace) -> int:
    """Run environment validation checks."""
    checks = run_checks(profile=args.profile)

    status_symbols = {"ok": "[ok]", "warn": "[!!]", "fail": "[FAIL]"}

    for check in checks:
        symbol = status_symbols.get(check.status, "[??]")
        print(f"  {symbol} {check.name}: {check.message}")

    failures = [c for c in checks if c.status == "fail"]
    if failures:
        print(f"\n{len(failures)} check(s) failed.", file=sys.stderr)
        return 1

    print("\nAll checks passed.")
    return 0


def _cmd_kernels(args: argparse.Namespace) -> int:
    handler = _KERNEL_HANDLERS.get(args.kernels_command)
    if handler is None:
        print(f"error: unknown kernels command: {args.kernels_command}", file=sys.stderr)
        return 1
    return handler(args)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------


_HANDLERS = {
    "run": _cmd_run,
    "clusters": _cmd_clusters,
    "install-kernel": _cmd_install_kernel,
    "kernels": _cmd_kernels,
    "render": _cmd_render,
    "doctor": _cmd_doctor,
}

_KERNEL_HANDLERS = {
    "install": _cmd_kernels_install,
    "list": _cmd_kernels_list,
    "remove": _cmd_kernels_remove,
    "doctor": _cmd_kernels_doctor,
}


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m databricks_agent_notebooks`` and ``bin/agent-notebook``."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None or args.command == "help":
        parser.print_help()
        return 0

    handler = _HANDLERS.get(args.command)
    if handler is None:
        print(f"error: unknown command: {args.command}", file=sys.stderr)
        return 1

    return handler(args)
