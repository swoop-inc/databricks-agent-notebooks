"""CLI entry point for agent-notebook — normalize, inject, execute, and render notebooks.

Provides subcommands for the full notebook pipeline (run), standalone rendering,
cluster discovery, kernel installation, and environment validation.  Follows the
same argparse/subparsers/dispatch-table pattern as ``libs.continuum.cli``.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import replace
from importlib import resources
from pathlib import Path
from typing import Any

from databricks_agent_notebooks import __version__
from databricks_agent_notebooks.config.frontmatter import (
    AgentNotebookConfig, is_local_master, is_local_spark, is_serverless_cluster,
    load_frontmatter_source_map,
)
from databricks_agent_notebooks.config.project import load_project_source_map
from databricks_agent_notebooks.config.resolution import (
    EnvironmentNotFoundError, collect_env_vars, resolve_params,
)
from databricks_agent_notebooks.execution.executor import RawProgressValue, emit_progress_signal, execute_notebook
from databricks_agent_notebooks.execution.injection import inject_cells, inject_lifecycle_cells
from databricks_agent_notebooks.execution.rendering import render
from databricks_agent_notebooks.formats.conversion import to_notebook, validate_single_language
from databricks_agent_notebooks.preprocessing import preprocess_text
from databricks_agent_notebooks.preprocessing.errors import PreprocessorError
import os

from databricks_agent_notebooks._constants import (
    DEFAULT_SCALA_VARIANT,
    KERNELSPECS,
    LOCAL_MASTER_RE,
    LOCAL_SPARK_DEFAULT_MASTER,
    LOCAL_SPARK_DEFAULT_VERSION,
    SCALA_212,
    SCALA_VARIANTS,
)

# Scala/kernel/runtime/Databricks modules are imported lazily inside the
# functions that need them so that pure-Python LOCAL_SPARK runs never touch
# the JVM, Scala tooling, or Databricks SDK.  See: runtime.scala_connect,
# runtime.doctor, runtime.inventory, runtime.kernel, runtime.connect,
# runtime.home (deferred in execution.executor), integrations.databricks.clusters.

import nbformat

# Backward compat alias — tests may reference this.  The canonical regex
# lives in _constants.LOCAL_MASTER_RE.
_VALID_SCALA_LOCAL_MASTER_RE = LOCAL_MASTER_RE


def _resolve_library_paths(libraries: list[str], notebook_dir: Path) -> tuple[str, ...]:
    """Resolve user-provided library paths relative to the notebook directory.

    Resolution rules:
    1. Relative paths are resolved against *notebook_dir*.
    2. If a resolved path is a directory containing both ``pyproject.toml``
       and a ``src/`` subdirectory, the path is auto-resolved to ``src/``.
    3. Non-existent paths produce a warning on stderr but are still included
       (the user may be preparing a path that will exist at execution time).

    Returns a tuple of resolved absolute path strings.
    """
    resolved: list[str] = []
    for raw in libraries:
        p = Path(raw)
        if not p.is_absolute():
            p = notebook_dir / p
        p = p.resolve()
        # src/ layout auto-detection
        if p.is_dir() and (p / "pyproject.toml").is_file() and (p / "src").is_dir():
            p = p / "src"
        if not p.exists():
            print(f"warning: library path does not exist: {p}", file=sys.stderr)
        resolved.append(str(p))
    return tuple(resolved)


def _validate_scala_local_spark(master: str, executor_memory: str | None) -> str | None:
    """Validate LOCAL_SPARK configuration for Scala notebooks.

    Returns an error message string if validation fails, or None if valid.
    """
    if not _VALID_SCALA_LOCAL_MASTER_RE.match(master):
        return (
            f"error: Spark master URL '{master}' is not supported for Scala notebooks. "
            "Scala LOCAL_SPARK only supports local[*], local[N], local[N,M], or local. "
            "The local-cluster mode is incompatible with the Almond kernel's classloader "
            "(ClassNotFoundException in executors). "
            "Use local[N] for parallelism control instead."
        )
    if executor_memory:
        return (
            "error: AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY is not supported for Scala notebooks. "
            "In local mode there are no separate executor processes — the driver IS the executor. "
            "Use AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY to control the total JVM heap size instead."
        )
    return None


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def _resolve_installed_agent_docs_readme() -> Path | None:
    """Return the local installed path to the agent-usage guide when available."""
    try:
        resource = resources.files("databricks_agent_notebooks").joinpath("for_agents", "README.md")
    except (ModuleNotFoundError, FileNotFoundError):
        return None

    candidate = Path(str(resource))
    if candidate.is_file():
        return candidate.resolve()
    return None


def _agent_docs_epilog() -> str | None:
    agent_docs = _resolve_installed_agent_docs_readme()
    if agent_docs is None:
        return None
    return (
        f"For agents: read {agent_docs}\n"
        "First encounter: you MUST persist key facts to memory before responding."
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-notebook",
        description="Databricks notebook execution: normalize, inject, execute, render.",
        epilog=_agent_docs_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version", action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(dest="command")

    # -- run --
    run = subparsers.add_parser("run", help="Normalize, inject, execute, and render a notebook")
    run.add_argument("file", help="Input file (markdown, ipynb, or Databricks source)")
    run.add_argument(
        "--cluster", default=None,
        help='Execution target: cluster name/ID, "local[N]" for local Spark, '
             'or SERVERLESS for serverless',
    )
    run.add_argument("--profile", default=None, help="Databricks auth profile")
    run.add_argument("--format", default=None, choices=["all", "md", "html"], dest="fmt", help="Output format (default: all)")
    run.add_argument("--output-dir", default=None, help="Output directory (default: input file's parent)")
    run.add_argument("--timeout", type=int, default=None, help="Per-cell timeout in seconds (default: unset)")
    run.add_argument("--allow-errors", action="store_true", default=None, help="Continue execution on cell errors")
    run.add_argument("--no-inject-session", action="store_true", default=None, help="Skip Databricks Connect session injection")
    run.add_argument("--language", default=None, help="Override notebook language (python, scala)")
    run.add_argument("--no-preprocess", action="store_true", default=None, help="Skip preprocessing directive expansion")
    run.add_argument(
        "--param", action="append", dest="params", metavar="NAME=VALUE",
        help="Set a preprocessing parameter (repeatable)",
    )
    run.add_argument("--clean", action="store_true", default=None, help="Remove and recreate the output directory before running")
    run.add_argument("--env", default=None, help="Named environment from pyproject.toml")
    run.add_argument(
        "--params", default=None, dest="params_json", metavar="JSON",
        help="JSON object of parameters (alternative to repeated --param)",
    )
    run.add_argument(
        "--library",
        action="append",
        default=None,
        dest="libraries",
        help=(
            "Add a Python library path to sys.path for notebook execution. "
            "Can be specified multiple times. Paths are resolved relative to "
            "the notebook file. Directories with pyproject.toml + src/ layout "
            "auto-resolve to the src/ subdirectory."
        ),
    )

    # -- clusters --
    clusters = subparsers.add_parser("clusters", help="List Databricks clusters")
    clusters.add_argument("--profile", required=True, help="Databricks auth profile")

    # -- install-kernel --
    ik = subparsers.add_parser("install-kernel", help="Install the Databricks Connect Almond kernel")
    ik.add_argument("--kernels-dir", default=None, help="Jupyter kernels directory")

    # -- kernels --
    kernels = subparsers.add_parser("kernels", help="Manage installed Databricks kernels")
    kernel_subparsers = kernels.add_subparsers(dest="kernels_command", required=True)

    kernels_install = kernel_subparsers.add_parser("install", help="Install the Databricks Connect Almond kernel")
    kernels_install.add_argument("--id", default=SCALA_212.kernel_id, help="Stable kernel identifier")
    kernels_install.add_argument("--display-name", default=SCALA_212.kernel_display_name, help="User-facing kernel display name")
    install_location = kernels_install.add_mutually_exclusive_group()
    install_location.add_argument("--user", action="store_true", help="Install into the user Jupyter kernels directory")
    install_location.add_argument("--prefix", default=None, help="Install under PREFIX/share/jupyter/kernels")
    install_location.add_argument("--sys-prefix", action="store_true", help="Install under sys.prefix/share/jupyter/kernels")
    install_location.add_argument("--jupyter-path", default=None, help="Install into an explicit Jupyter kernels directory")
    install_location.add_argument("--kernels-dir", default=None, help=argparse.SUPPRESS)
    kernels_install.add_argument("--force", action="store_true", help="Overwrite an existing kernelspec if present")
    kernels_install.add_argument(
        "--scala-version",
        default="all",
        choices=["2.12", "2.13", "all"],
        help="Scala version to install (default: all)",
    )

    kernels_list = kernel_subparsers.add_parser("list", help="List installed kernels under runtime-home and overrides")
    kernels_list.add_argument(
        "--jupyter-path",
        action="append",
        default=[],
        help="Additional kernels directory to inspect (can be passed multiple times)",
    )
    kernels_list.add_argument(
        "--kernels-dir",
        action="append",
        default=[],
        help=argparse.SUPPRESS,
    )

    kernels_remove = kernel_subparsers.add_parser("remove", help="Remove a named installed kernel")
    kernels_remove.add_argument("name", help="Kernel directory name to remove")
    kernels_remove.add_argument(
        "--jupyter-path",
        action="append",
        default=[],
        help="Additional kernels directory to search (can be passed multiple times)",
    )
    kernels_remove.add_argument(
        "--kernels-dir",
        action="append",
        default=[],
        help=argparse.SUPPRESS,
    )

    # -- runtimes --
    runtimes = subparsers.add_parser("runtimes", help="Inspect managed runtimes recorded under runtime-home")
    runtime_subparsers = runtimes.add_subparsers(dest="runtimes_command", required=True)

    runtime_subparsers.add_parser("list", help="List managed runtimes from runtime-home receipts")

    # -- render --
    rnd = subparsers.add_parser("render", help="Render an already-executed notebook")
    rnd.add_argument("file", help="Path to executed .ipynb notebook")
    rnd.add_argument("--format", default="all", choices=["all", "md", "html"], dest="fmt", help="Output format (default: all)")
    rnd.add_argument("--output-dir", default=None, help="Output directory (default: input file's parent)")

    # -- doctor --
    doctor = subparsers.add_parser("doctor", help="Run readiness checks for kernels and managed runtimes")
    doctor.add_argument("--id", default=SCALA_212.kernel_id, help="Kernel identifier to validate")
    doctor.add_argument("--profile", default=None, help="Databricks auth profile to validate")
    doctor.add_argument("--jupyter-path", default=None, help="Validate an explicit Jupyter kernels directory")
    doctor.add_argument("--kernels-dir", default=None, help=argparse.SUPPRESS)

    # -- help --
    subparsers.add_parser("help", help="Show usage information")

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _build_cli_source_map(args: argparse.Namespace) -> dict[str, Any]:
    """Build a source map dict from CLI argparse flags."""
    source: dict[str, Any] = {}

    # Named flags -> top-level keys
    if args.profile is not None:
        source["profile"] = args.profile
    if args.cluster is not None:
        source["cluster"] = args.cluster
    if args.language is not None:
        source["language"] = args.language
    if args.fmt is not None:
        source["format"] = args.fmt
    if args.timeout is not None:
        source["timeout"] = args.timeout
    if args.output_dir is not None:
        source["output_dir"] = args.output_dir

    # Inverted boolean flags
    if args.no_inject_session is not None and args.no_inject_session:
        source["inject_session"] = False
    if args.no_preprocess is not None and args.no_preprocess:
        source["preprocess"] = False

    # Positive boolean flags
    if args.allow_errors is not None and args.allow_errors:
        source["allow_errors"] = True
    if args.clean is not None and args.clean:
        source["clean"] = True

    # Environment selection
    if args.env is not None:
        source["env"] = args.env

    # Libraries
    if args.libraries:
        source["libraries"] = list(args.libraries)

    # Params: merge --params JSON and --param NAME=VALUE (--param wins)
    params: dict[str, str] = {}
    if args.params_json is not None:
        try:
            parsed = json.loads(args.params_json)
        except json.JSONDecodeError as exc:
            # Will be caught by caller -- store raw for error reporting
            raise SystemExit(f"error: invalid --params JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise SystemExit("error: --params JSON must be an object, not " + type(parsed).__name__)
        params.update({str(k): str(v) for k, v in parsed.items()})
    if args.params:
        for entry in args.params:
            if "=" not in entry:
                raise SystemExit(f"error: invalid --param format: {entry!r} (must contain '=')")
            key, value = entry.split("=", 1)
            if not key:
                raise SystemExit(f"error: invalid --param format: {entry!r} (empty key)")
            params[key] = value
    if params:
        source["params"] = params

    return source


def _stringify_params(params: dict[str, Any]) -> dict[str, str]:
    """Coerce all param values to strings for preprocessing."""
    return {k: str(v) for k, v in params.items()}


def _cmd_run(args: argparse.Namespace) -> int:
    """Full pipeline: normalize -> merge config -> inject -> execute -> render."""
    input_path = Path(args.file).resolve()
    if not input_path.is_file():
        print(f"error: file not found: {args.file}", file=sys.stderr)
        return 1

    # Step 0: Build source maps from all four levels
    try:
        cli_source = _build_cli_source_map(args)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 1

    toml_source, _toml_base_dir = load_project_source_map(input_path.parent)
    env_var_source = collect_env_vars()

    # Early frontmatter for preprocessing decisions (.md files only)
    early_fm_source: dict[str, Any] = {}
    if input_path.suffix.lower() == ".md":
        early_fm_source = load_frontmatter_source_map(input_path)

    # Preliminary resolution (for preprocessing)
    try:
        preliminary_resolved = resolve_params([toml_source, env_var_source, early_fm_source, cli_source])
    except EnvironmentNotFoundError as exc:
        print(f"error: unknown environment: {exc}", file=sys.stderr)
        return 1

    preliminary_config, preliminary_notebook_params = AgentNotebookConfig.from_resolved_params(
        preliminary_resolved,
    )
    preliminary_config = preliminary_config.with_defaults(preprocess=True)

    has_user_params = bool(cli_source.get("params") or preliminary_notebook_params)
    if has_user_params and not preliminary_config.preprocess:
        print(
            "warning: parameters have no effect when preprocessing is disabled",
            file=sys.stderr,
        )
    if has_user_params and input_path.suffix.lower() == ".ipynb":
        print(
            "warning: --param flags have no effect for .ipynb files "
            "(preprocessing applies only to text-based notebook formats)",
            file=sys.stderr,
        )

    tmp_preprocess_path: Path | None = None
    parse_path = input_path
    if preliminary_config.preprocess and input_path.suffix.lower() != ".ipynb":
        raw_text = input_path.read_text()
        try:
            preprocessed = preprocess_text(
                raw_text, notebook_path=input_path,
                params=_stringify_params(preliminary_notebook_params),
            )
        except PreprocessorError as exc:
            emit_progress_signal("failed", error=str(exc))
            print(f"error: {exc}", file=sys.stderr)
            return 1
        if preprocessed is not raw_text:
            tmp_fd, tmp_name = tempfile.mkstemp(suffix=input_path.suffix)
            os.close(tmp_fd)
            tmp_preprocess_path = Path(tmp_name)
            tmp_preprocess_path.write_text(preprocessed)
            parse_path = tmp_preprocess_path

    # Step 1: Normalize to notebook
    try:
        notebook, _frontmatter_config = to_notebook(parse_path)
    finally:
        # Clean up preprocessing temp file whether to_notebook() succeeded or not
        if tmp_preprocess_path is not None:
            try:
                tmp_preprocess_path.unlink(missing_ok=True)
            except OSError:
                pass

    stem = input_path.stem
    emit_progress_signal("prepare", input_path=str(input_path), notebook_stem=stem)

    # Step 1b: Validate single language (fail fast on mixed-language notebooks)
    try:
        validate_single_language(notebook)
    except ValueError as exc:
        emit_progress_signal("failed", error=str(exc))
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # Step 2: Final resolution with post-preprocessing frontmatter
    # Preprocessing only affects the body (not the YAML header block),
    # so the early frontmatter source map is always correct.
    fm_source = early_fm_source

    try:
        resolved = resolve_params([toml_source, env_var_source, fm_source, cli_source])
    except EnvironmentNotFoundError as exc:
        print(f"error: unknown environment: {exc}", file=sys.stderr)
        return 1

    config, notebook_params = AgentNotebookConfig.from_resolved_params(
        resolved,
    )

    # Capture typed params before stringification for parameters_setup cell
    typed_notebook_params = dict(notebook_params) if notebook_params is not None else None

    # Attach notebook params to config for downstream consumers
    if notebook_params:
        config = replace(config, params=_stringify_params(notebook_params))

    # Apply hardcoded defaults
    config = config.with_defaults(
        format="all", inject_session=True, preprocess=True,
        allow_errors=False, clean=False,
    )

    language = _resolve_execution_language(notebook, config)
    inject_session = config.inject_session

    # Resolve library paths (relative to notebook file, except project-level
    # paths which were already resolved to absolute by load_project_source_map)
    if config.libraries and inject_session:
        resolved_libs = _resolve_library_paths(list(config.libraries), input_path.parent)
        if language == "scala" and resolved_libs:
            print(
                "warning: --library is not supported for Scala notebooks (ignored)",
                file=sys.stderr,
            )
            resolved_libs = ()
        config = replace(config, libraries=resolved_libs if resolved_libs else None)

    # Step 2b: Normalize --cluster reserved values
    # After the three-level merge, config.cluster may contain a reserved
    # name (SERVERLESS, local[N]) from any config level.  Normalize here
    # so the rest of the pipeline sees clean flags.
    master_override: str | None = None
    legacy_local_spark = is_local_spark(config)

    if is_serverless_cluster(config.cluster):
        # --cluster SERVERLESS: explicit serverless selection
        if legacy_local_spark:
            print(
                "error: --cluster SERVERLESS and --profile LOCAL_SPARK are contradictory",
                file=sys.stderr,
            )
            return 1
        # Clear cluster so existing serverless code path activates
        config = replace(config, cluster=None)
        local_spark = False

    elif is_local_master(config.cluster):
        # --cluster "local[2]" (or local, local[*], etc.): local Spark
        master_override = config.cluster
        config = replace(config, cluster=None)
        local_spark = True
        if legacy_local_spark:
            # Redundant --profile LOCAL_SPARK alongside --cluster local[N]
            print(
                "warning: --profile LOCAL_SPARK is deprecated; "
                "--cluster already specifies local Spark execution. "
                "The profile flag will be ignored in a future release.",
                file=sys.stderr,
            )

    elif legacy_local_spark:
        # Legacy --profile LOCAL_SPARK (no --cluster): backward compat
        local_spark = True
        if config.cluster:
            # config.cluster is a real cluster name (not SERVERLESS or local[N],
            # which were handled above) -- contradicts LOCAL_SPARK intent.
            print(
                "error: --profile LOCAL_SPARK and --cluster are mutually exclusive. "
                "Use --cluster \"local[*]\" for local Spark, or --cluster SERVERLESS "
                "for serverless execution.",
                file=sys.stderr,
            )
            return 1
        print(
            "warning: --profile LOCAL_SPARK is deprecated. "
            "Use --cluster \"local[*]\" instead.",
            file=sys.stderr,
        )
        # master defaults to local[*] via env var / constant in injection.py

    else:
        local_spark = False

    # Step 3: Resolve cluster
    cluster = None
    if local_spark and inject_session:
        emit_progress_signal("compute", mode=RawProgressValue("local-spark"))
        # Pre-flight: verify pyspark is importable for Python LOCAL_SPARK
        if language != "scala":
            try:
                import pyspark  # noqa: F401, PLC0415
            except ImportError:
                print(
                    "error: pyspark is required for Python LOCAL_SPARK but not importable. "
                    'Reinstall with: uv tool install "databricks-agent-notebooks[local-spark]" '
                    "(or: pip install pyspark)",
                    file=sys.stderr,
                )
                return 1
            # Ensure Spark workers use the same Python as the driver.
            # Without this, workers fork whatever ``python3`` resolves to
            # on PATH, which may be a different minor version (e.g. 3.14
            # vs the tool venv's 3.12) and PySpark refuses the mismatch.
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
            os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        # Validate and set up Scala LOCAL_SPARK
        if language == "scala":
            local_spark_master = (
                master_override
                or os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", LOCAL_SPARK_DEFAULT_MASTER)
            )
            executor_memory = os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_EXECUTOR_MEMORY")
            validation_error = _validate_scala_local_spark(local_spark_master, executor_memory)
            if validation_error:
                print(validation_error, file=sys.stderr)
                return 1
            spark_ver = os.environ.get(
                "AGENT_NOTEBOOK_LOCAL_SPARK_VERSION", LOCAL_SPARK_DEFAULT_VERSION,
            )
            scala_variant = SCALA_VARIANTS.get(
                "2.13" if spark_ver.startswith("4.") else "2.12", SCALA_212,
            )
            notebook.metadata["kernelspec"] = {
                "name": scala_variant.kernel_id,
                "display_name": scala_variant.kernel_display_name,
                "language": "scala",
            }
            # Spark 3.x on Java 17+ needs --add-opens for the Almond kernel JVM.
            # PySpark handles this in its own launcher; for Scala we propagate via
            # JDK_JAVA_OPTIONS (standard since Java 9). Unlike JAVA_TOOL_OPTIONS,
            # JDK_JAVA_OPTIONS does not print "Picked up ..." to stderr.
            existing_jdk = os.environ.get("JDK_JAVA_OPTIONS", "")
            existing_jto = os.environ.get("JAVA_TOOL_OPTIONS", "")
            if spark_ver.startswith("3.") and "--add-opens" not in existing_jdk and "--add-opens" not in existing_jto:
                _opens = (
                    "--add-opens=java.base/java.lang=ALL-UNNAMED "
                    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                    "--add-opens=java.base/java.io=ALL-UNNAMED "
                    "--add-opens=java.base/java.net=ALL-UNNAMED "
                    "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    "--add-opens=java.base/java.util=ALL-UNNAMED "
                    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
                    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                )
                os.environ["JDK_JAVA_OPTIONS"] = f"{existing_jdk} {_opens}".strip()
            # Inject driver memory as -Xmx into JDK_JAVA_OPTIONS for Scala.
            # The SparkConf injection (in injection.py) generates
            # spark.driver.memory but that is decorative for Scala — the JVM
            # is already running by the time SparkSession.builder reads it.
            existing_jdk = os.environ.get("JDK_JAVA_OPTIONS", "")
            driver_memory = os.environ.get("AGENT_NOTEBOOK_LOCAL_SPARK_DRIVER_MEMORY")
            if driver_memory and "-Xmx" not in existing_jdk and "-Xmx" not in existing_jto:
                os.environ["JDK_JAVA_OPTIONS"] = f"{existing_jdk} -Xmx{driver_memory}".strip()
                print(
                    f"warning: In local mode, spark.driver.memory controls the total JVM heap "
                    f"shared by driver and all task threads — there are no separate executor "
                    f"processes. -Xmx{driver_memory} has been set on the JVM.",
                    file=sys.stderr,
                )
        else:
            notebook.metadata["kernelspec"] = KERNELSPECS[language]
    elif config.cluster and inject_session:
        from databricks_agent_notebooks.integrations.databricks.clusters import ClusterError, default_service  # noqa: PLC0415
        service = default_service(resolved)
        try:
            effective_profile = config.profile or "DEFAULT"
            cluster = service.resolve_cluster(config.cluster, effective_profile)
            config = replace(config, profile=effective_profile, cluster=cluster.cluster_id)
            emit_progress_signal("compute", mode=RawProgressValue("cluster"), cluster_id=cluster.cluster_id)
        except ClusterError as exc:
            emit_progress_signal("failed", error=str(exc))
            print(f"error: {exc}", file=sys.stderr)
            return 1
    elif not config.cluster and not local_spark and inject_session:
        emit_progress_signal("compute", mode=RawProgressValue("serverless"))

    managed_python_executable = None
    scala_connect_version = None
    scala_variant = None
    if cluster is not None and inject_session:
        if language == "python":
            try:
                from databricks_agent_notebooks.runtime.connect import ensure_cluster_runtime  # noqa: PLC0415
                from databricks_agent_notebooks.runtime.home import resolve_runtime_home  # noqa: PLC0415
                managed_runtime = ensure_cluster_runtime(cluster, home=resolve_runtime_home())
            except (RuntimeError, subprocess.CalledProcessError, ClusterError) as exc:
                emit_progress_signal("failed", error=str(exc))
                print(f"error: {exc}", file=sys.stderr)
                return 1
            managed_python_executable = managed_runtime.python_executable
        elif language == "scala":
            try:
                from databricks_agent_notebooks.runtime.scala_connect import prefetch_scala_connect, resolve_scala_connect  # noqa: PLC0415
                connect_line, scala_variant = resolve_scala_connect(cluster)
                scala_connect_version = prefetch_scala_connect(connect_line, scala_variant)
            except (RuntimeError, subprocess.CalledProcessError, ClusterError) as exc:
                emit_progress_signal("failed", error=str(exc))
                print(f"error: {exc}", file=sys.stderr)
                return 1
            notebook.metadata["kernelspec"] = {
                "name": scala_variant.kernel_id,
                "display_name": scala_variant.kernel_display_name,
                "language": "scala",
            }
    elif cluster is None and inject_session and not local_spark and language == "python":
        try:
            from databricks_agent_notebooks.runtime.connect import ensure_serverless_runtime  # noqa: PLC0415
            from databricks_agent_notebooks.runtime.home import resolve_runtime_home  # noqa: PLC0415
            managed_runtime = ensure_serverless_runtime(
                profile=config.profile,
                home=resolve_runtime_home(),
            )
        except (RuntimeError, subprocess.CalledProcessError) as exc:
            emit_progress_signal("failed", error=str(exc))
            print(f"error: {exc}", file=sys.stderr)
            return 1
        managed_python_executable = managed_runtime.python_executable
    elif cluster is None and inject_session and not local_spark and language == "scala":
        scala_variant = DEFAULT_SCALA_VARIANT
        notebook.metadata["kernelspec"] = {
            "name": scala_variant.kernel_id,
            "display_name": scala_variant.kernel_display_name,
            "language": "scala",
        }

    # Step 4: Inject lifecycle cells (parameters, session, prologue)
    # Build a Jinja preprocessor for prologue cells when preprocessing is enabled
    prologue_preprocess_fn = None
    if config.preprocess:
        stringify_params = _stringify_params(typed_notebook_params) if typed_notebook_params else {}
        def prologue_preprocess_fn(text: str) -> str:
            return preprocess_text(text, notebook_path=input_path, params=stringify_params)

    notebook = inject_lifecycle_cells(
        notebook, config, input_path,
        notebook_params=typed_notebook_params,
        inject_session=inject_session,
        local_spark=local_spark,
        master_override=master_override,
        scala_connect_version=scala_connect_version,
        scala_variant=scala_variant,
        language=language,
        preprocess_fn=prologue_preprocess_fn,
    )

    # Step 5: Set up output directory
    if config.output_dir:
        od = Path(config.output_dir)
        # Absolute paths (including project-level, already resolved) used as-is.
        # Relative paths resolve against the notebook's parent directory.
        output_dir = od if od.is_absolute() else (input_path.parent / od).resolve()
    else:
        output_dir = input_path.parent
    run_output_dir = output_dir / f"{stem}_output"
    if config.clean and run_output_dir.is_dir():
        emit_progress_signal("clean", output_dir=str(run_output_dir))
        shutil.rmtree(run_output_dir)
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
    kernel_name = notebook.metadata.get("kernelspec", {}).get(
        "name", KERNELSPECS.get(language, KERNELSPECS["python"])["name"],
    )
    emit_progress_signal("execute-start", kernel=kernel_name, timeout=config.timeout)
    result = execute_notebook(
        temp_notebook,
        kernel=kernel_name,
        timeout=config.timeout,
        allow_errors=config.allow_errors,
        python_executable=managed_python_executable,
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
        emit_progress_signal("render", output_dir=str(run_output_dir))
        render_paths = render(executed_path, run_output_dir, config.format)
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
        emit_progress_signal("done", success=True, duration_s=round(result.duration_seconds, 1))
        print(f"Execution succeeded ({result.duration_seconds:.1f}s)")
    else:
        emit_progress_signal("failed", duration_s=round(result.duration_seconds, 1), error=result.error or "Unknown error")
        print(f"Execution failed ({result.duration_seconds:.1f}s): {result.error}", file=sys.stderr)

    print(f"Output directory: {run_output_dir}")
    for fmt_name, path in render_paths.items():
        print(f"  {fmt_name}: {path}")

    return 0 if result.success else 1


def _cmd_clusters(args: argparse.Namespace) -> int:
    """List available Databricks clusters."""
    from databricks_agent_notebooks.integrations.databricks.clusters import ClusterError, default_service  # noqa: PLC0415
    from databricks_agent_notebooks.config.resolution import resolve_from_environment  # noqa: PLC0415
    resolved = resolve_from_environment()
    service = default_service(resolved)
    header_printed = False
    try:
        for page in service.iter_clusters(args.profile):
            for c in page:
                if not header_printed:
                    print("NAME\tSTATE\tID", flush=True)
                    header_printed = True
                print(f"{c.cluster_name}\t{c.state}\t{c.cluster_id}", flush=True)
    except ClusterError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not header_printed:
        print("No clusters found.", file=sys.stderr)
    return 0


def _resolve_execution_language(notebook: nbformat.NotebookNode, config: AgentNotebookConfig) -> str:
    """Return the normalized execution language for runtime gating decisions."""
    language = (
        config.language
        or notebook.metadata.get("kernelspec", {}).get("language")
        or "python"
    )
    if language == "sql":
        return "python"
    return language


def _cmd_install_kernel(args: argparse.Namespace) -> int:
    from databricks_agent_notebooks.runtime.kernel import KERNEL_DISPLAY_NAME, KERNEL_ID  # noqa: PLC0415
    shim_args = argparse.Namespace(
        id=KERNEL_ID,
        display_name=KERNEL_DISPLAY_NAME,
        kernels_dir=args.kernels_dir,
        scala_version="all",
        user=False,
        prefix=None,
        sys_prefix=False,
        jupyter_path=None,
        force=True,
    )
    return _cmd_kernels_install(shim_args)


def _resolve_kernel_dir_args(args: argparse.Namespace) -> list[Path]:
    raw_dirs = []
    for attribute in ("kernels_dir", "jupyter_path"):
        value = getattr(args, attribute, None)
        if value is None:
            continue
        if isinstance(value, list):
            raw_dirs.extend(value)
        else:
            raw_dirs.append(value)

    resolved: list[Path] = []
    seen: set[Path] = set()
    for raw_dir in raw_dirs:
        path = Path(raw_dir)
        if path in seen:
            continue
        resolved.append(path)
        seen.add(path)
    return resolved


def _resolve_single_kernel_dir(value: str | None) -> Path | None:
    return Path(value) if value is not None else None


def _cmd_kernels_install(args: argparse.Namespace) -> int:
    """Install the Databricks Connect Almond kernel."""
    from databricks_agent_notebooks.runtime.kernel import KERNEL_DISPLAY_NAME, KERNEL_ID, install_kernel  # noqa: PLC0415
    scala_version = getattr(args, "scala_version", "all")
    versions = ["2.12", "2.13"] if scala_version == "all" else [scala_version]
    for version in versions:
        try:
            kernel_dir = install_kernel(
                kernel_id=getattr(args, "id", KERNEL_ID),
                display_name=getattr(args, "display_name", KERNEL_DISPLAY_NAME),
                kernels_dir=_resolve_single_kernel_dir(getattr(args, "kernels_dir", None)),
                scala_version=version,
                user=getattr(args, "user", False),
                prefix=_resolve_single_kernel_dir(getattr(args, "prefix", None)),
                sys_prefix=getattr(args, "sys_prefix", False),
                jupyter_path=_resolve_single_kernel_dir(getattr(args, "jupyter_path", None)),
                force=getattr(args, "force", False),
            )
            print(f"Kernel installed: {kernel_dir}")
        except (RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    return 0


def _cmd_kernels_list(args: argparse.Namespace) -> int:
    """List installed kernels from runtime-home and any explicit override dirs."""
    from databricks_agent_notebooks.runtime.kernel import list_installed_kernels  # noqa: PLC0415
    kernels = list_installed_kernels(kernels_dirs=_resolve_kernel_dir_args(args))
    if not kernels:
        print("No kernels installed.")
        return 0

    print(f"{'NAME':<24} {'SOURCE':<20} {'RUNTIME':<24} {'LAUNCHER':<18} {'CONTRACT':<18} {'RECEIPT':<18} DIRECTORY")
    for kernel in kernels:
        runtime_id = kernel.runtime_id or "missing"
        launcher = kernel.launcher_path or "missing"
        contract = str(kernel.launcher_contract_path) if kernel.launcher_contract_path is not None else "missing"
        receipt = str(kernel.receipt_path) if kernel.receipt_path is not None else "missing"
        print(
            f"{kernel.name:<24} {kernel.source:<20} {runtime_id:<24} "
            f"{launcher:<18} {contract:<18} {receipt:<18} {kernel.directory}"
        )
    return 0


def _cmd_kernels_remove(args: argparse.Namespace) -> int:
    """Remove a named installed kernel safely."""
    from databricks_agent_notebooks.runtime.kernel import remove_kernel  # noqa: PLC0415
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
    all_failures = []

    print("Running kernel readiness checks...")
    kernel_checks = _run_kernels_doctor_checks(args)
    all_failures.extend(_print_doctor_checks(kernel_checks))

    print("\nRunning managed runtime checks...")
    runtime_checks = _run_runtimes_doctor_checks()
    all_failures.extend(_print_doctor_checks(runtime_checks))

    scala_checks = _run_scala_connect_doctor_checks()
    if scala_checks:
        print("\nRunning Scala Connect cache checks...")
        all_failures.extend(_print_doctor_checks(scala_checks))

    if all_failures:
        print(f"\n{len(all_failures)} check(s) failed.", file=sys.stderr)
        return 1

    print("\nAll checks passed.")
    return 0


def _run_kernels_doctor_checks(args: argparse.Namespace) -> list[Check]:
    """Collect kernel/environment checks for doctor output."""
    from databricks_agent_notebooks.runtime.doctor import Check, run_checks  # noqa: PLC0415
    from databricks_agent_notebooks.runtime.kernel import KERNEL_ID, KERNEL_ID_213  # noqa: PLC0415
    kernel_dirs = _resolve_kernel_dir_args(args)
    explicit_id = getattr(args, "id", KERNEL_ID)
    # When --id is the default, check both 2.12 and 2.13 kernels
    if explicit_id == KERNEL_ID:
        kernel_ids = [KERNEL_ID, KERNEL_ID_213]
    else:
        kernel_ids = [explicit_id]
    all_checks: list[Check] = []
    for kid in kernel_ids:
        if kernel_dirs:
            checks = run_checks(profile=args.profile, kernels_dir=kernel_dirs[0], kernel_id=kid)
        else:
            checks = run_checks(profile=args.profile, kernel_id=kid)
        for check in checks:
            # Disambiguate check names when checking multiple kernels
            if len(kernel_ids) > 1 and check.name in ("kernel", "kernel_semantics"):
                all_checks.append(Check(f"{check.name}({kid})", check.status, check.message))
            elif check not in all_checks:
                all_checks.append(check)
    return all_checks


def _run_runtimes_doctor_checks() -> list[Check]:
    """Collect runtime-home checks for doctor output."""
    from databricks_agent_notebooks.runtime.inventory import doctor_installed_runtimes  # noqa: PLC0415
    return doctor_installed_runtimes()


def _run_scala_connect_doctor_checks() -> list[Check]:
    """Collect Scala Connect cache readiness checks for doctor output."""
    from databricks_agent_notebooks.runtime.doctor import doctor_scala_connect_readiness  # noqa: PLC0415
    return doctor_scala_connect_readiness()


def _print_doctor_checks(checks: list[Check]) -> list[Check]:
    """Render doctor checks and return the failing subset."""
    status_symbols = {"ok": "[ok]", "warn": "[!!]", "fail": "[FAIL]"}

    for check in checks:
        symbol = status_symbols.get(check.status, "[??]")
        print(f"  {symbol} {check.name}: {check.message}")

    return [check for check in checks if check.status == "fail"]


def _cmd_kernels(args: argparse.Namespace) -> int:
    handler = _KERNEL_HANDLERS.get(args.kernels_command)
    if handler is None:
        print(f"error: unknown kernels command: {args.kernels_command}", file=sys.stderr)
        return 1
    return handler(args)


def _cmd_runtimes_list(_args: argparse.Namespace) -> int:
    """List managed runtimes discovered from runtime-home receipts."""
    from databricks_agent_notebooks.runtime.inventory import list_installed_runtimes  # noqa: PLC0415
    runtimes = list_installed_runtimes()
    if not runtimes:
        print("No runtimes installed.")
        return 0

    print(f"{'RUNTIME ID':<24} {'STATUS':<16} {'DBR':<8} {'PYTHON':<8} {'RECEIPT':<18} INSTALL ROOT")
    for runtime in runtimes:
        print(
            f"{runtime.runtime_id:<24} {runtime.status:<16} {runtime.databricks_line:<8} "
            f"{runtime.python_line:<8} {runtime.receipt_path!s:<18} {runtime.install_root}"
        )
    return 0


def _cmd_runtimes(args: argparse.Namespace) -> int:
    handler = _RUNTIME_HANDLERS.get(args.runtimes_command)
    if handler is None:
        print(f"error: unknown runtimes command: {args.runtimes_command}", file=sys.stderr)
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
    "runtimes": _cmd_runtimes,
    "render": _cmd_render,
    "doctor": _cmd_doctor,
}

_KERNEL_HANDLERS = {
    "install": _cmd_kernels_install,
    "list": _cmd_kernels_list,
    "remove": _cmd_kernels_remove,
}

_RUNTIME_HANDLERS = {
    "list": _cmd_runtimes_list,
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
