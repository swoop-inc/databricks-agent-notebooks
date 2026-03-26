from __future__ import annotations

import json
import subprocess
import tomllib
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_pyproject() -> dict[str, object]:
    pyproject_path = REPO_ROOT / "python" / "pyproject.toml"
    return tomllib.loads(read_text(pyproject_path))


def load_workflow(path: Path) -> dict[str, object]:
    return yaml.load(read_text(path), Loader=yaml.BaseLoader)


def _tracked_files(*paths: str) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "--", *paths],
        check=True,
        capture_output=True,
        text=True,
    )
    return [
        repo_path
        for line in result.stdout.splitlines()
        if line and (repo_path := REPO_ROOT / line).exists()
    ]


def test_repo_has_monorepo_ready_root_layout() -> None:
    assert (REPO_ROOT / "python").is_dir()
    assert (REPO_ROOT / "contracts").is_dir()
    assert (REPO_ROOT / "docs").is_dir()
    assert (REPO_ROOT / ".github" / "workflows").is_dir()


def test_launcher_contract_schema_exists() -> None:
    schema_path = REPO_ROOT / "contracts" / "launcher-kernel-contract.schema.json"
    data = json.loads(schema_path.read_text(encoding="utf-8"))

    assert data["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert data["title"] == "LauncherKernelContract"
    assert "contract_version" in data["required"]
    assert "kernel_id" in data["required"]
    assert "language" in data["required"]
    assert "argv" in data["required"]
    assert "runtime_id" in data["required"]
    assert "bootstrap_argv" in data["required"]


def test_python_distribution_declares_nbconvert_dependency() -> None:
    pyproject = load_pyproject()

    dependencies = pyproject["project"]["dependencies"]
    assert any(dep.startswith("nbconvert") for dep in dependencies)


def test_python_distribution_includes_agent_docs_package_data() -> None:
    pyproject = load_pyproject()

    package_data = pyproject["tool"]["setuptools"]["package-data"]
    patterns = package_data["databricks_agent_notebooks"]

    assert "for_agents/*.md" in patterns
    assert "for_agents/**/*.md" in patterns
    assert "for_agents/**/*" in patterns


def test_python_distribution_declares_release_metadata() -> None:
    pyproject = load_pyproject()
    project = pyproject["project"]

    assert project["description"] == (
        "Standalone notebook conversion and local runtime tooling for the "
        "databricks-agent-notebooks repository."
    )
    assert project["license"] == "MIT"
    assert project["license-files"] == ["LICENSE"]
    assert project["authors"] == [{"name": "Swoop"}]
    assert sorted(project["keywords"]) == [
        "databricks",
        "jupyter",
        "notebooks",
    ]

    classifiers = project["classifiers"]
    assert "Intended Audience :: Developers" in classifiers
    assert "Operating System :: OS Independent" in classifiers
    assert "Programming Language :: Python :: 3" in classifiers
    assert "Programming Language :: Python :: 3.11" in classifiers
    assert "Programming Language :: Python :: 3.12" in classifiers
    assert "Topic :: Software Development :: Libraries :: Python Modules" in classifiers

    assert project["urls"] == {
        "Homepage": "https://github.com/swoop-inc/databricks-agent-notebooks",
        "Repository": "https://github.com/swoop-inc/databricks-agent-notebooks",
        "Issues": "https://github.com/swoop-inc/databricks-agent-notebooks/issues",
    }

    license_path = REPO_ROOT / "python" / project["license-files"][0]
    assert license_path.read_text(encoding="utf-8").startswith("MIT License")


def test_python_ci_validates_built_artifacts() -> None:
    workflow = read_text(REPO_ROOT / ".github" / "workflows" / "python-ci.yml")

    assert "Build distribution artifacts" in workflow
    assert "uv build" in workflow
    assert "twine check --strict dist/*" in workflow
    assert "tarfile.open" in workflow
    assert "zipfile.ZipFile" in workflow
    assert "licenses/LICENSE" in workflow
    assert "entry_points.txt" in workflow
    assert "uv pip install --python .artifact-venv/bin/python dist/*.whl" in workflow
    assert "scripts/installed_artifact_installer_proof.py" in workflow
    assert "databricks_agent_notebooks/for_agents/README.md" in workflow
    assert "upload-artifact" in workflow


def test_publish_workflow_uses_trusted_publishing_scaffolding() -> None:
    workflow = load_workflow(REPO_ROOT / ".github" / "workflows" / "publish.yml")

    assert workflow["on"] == {
        "workflow_dispatch": "",
        "push": {"tags": ["v*"]},
    }

    jobs = workflow["jobs"]
    build_job = jobs["build"]
    testpypi_job = jobs["publish-testpypi"]
    pypi_job = jobs["publish-pypi"]

    build_steps = build_job["steps"]
    assert build_steps[0]["uses"] == (
        "actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5"
    )
    assert build_steps[1]["uses"] == (
        "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065"
    )
    assert build_steps[1]["with"] == {"python-version": "3.12"}
    assert build_steps[2]["uses"] == (
        "astral-sh/setup-uv@d4b2f3b6ecc6e67c4457f6d3e41ec42d3d0fcb86"
    )
    assert build_steps[-1]["uses"] == (
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
    )
    assert build_steps[-1]["with"] == {
        "name": "dist-artifacts",
        "path": "python/dist/*",
    }

    tag_version_step = next(
        step
        for step in build_steps
        if step.get("name") == "Verify tag matches package version"
    )
    assert tag_version_step["if"] == (
        "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/')"
    )
    assert "github.ref_name" in tag_version_step["run"]
    assert "pyproject.toml" in tag_version_step["run"]
    assert "project" in tag_version_step["run"]
    assert "version" in tag_version_step["run"]

    assert testpypi_job["if"] == "github.event_name == 'workflow_dispatch'"
    assert testpypi_job["permissions"] == {"id-token": "write"}
    assert testpypi_job["environment"] == "testpypi"
    assert testpypi_job["steps"][0]["uses"] == (
        "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093"
    )
    assert testpypi_job["steps"][1]["uses"] == (
        "pypa/gh-action-pypi-publish@ed0c53931b1dc9bd32cbe73a98c7f6766f8a527e"
    )
    assert testpypi_job["steps"][1]["with"] == {
        "repository-url": "https://test.pypi.org/legacy/"
    }

    assert pypi_job["if"] == (
        "github.event_name == 'push' && startsWith(github.ref, 'refs/tags/')"
    )
    assert pypi_job["permissions"] == {
        "id-token": "write",
        "attestations": "write",
        "contents": "read",
    }
    assert pypi_job["environment"] == "pypi"
    assert pypi_job["steps"][0]["uses"] == (
        "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093"
    )
    assert pypi_job["steps"][1]["uses"] == (
        "pypa/gh-action-pypi-publish@ed0c53931b1dc9bd32cbe73a98c7f6766f8a527e"
    )


def test_release_docs_only_claim_verified_local_offline_scope() -> None:
    readme = read_text(REPO_ROOT / "README.md")
    agent_readme = read_text(
        REPO_ROOT / "python" / "src" / "databricks_agent_notebooks" / "for_agents" / "README.md"
    )
    release = read_text(REPO_ROOT / "docs" / "release.md")

    assert "## For Agents And Sandboxed Runners" not in readme
    assert "python/src/databricks_agent_notebooks/for_agents/README.md" in readme
    assert "agent-notebook help" in readme
    assert "agent-notebook kernels doctor --help" not in readme
    assert "agent-notebook runtimes doctor --help" not in readme
    assert "# Agent Notebook Guide" in agent_readme
    assert "nohup agent-notebook run" in agent_readme
    assert 'LOG_PATH="$OUTPUT_DIR/$STEM.run.log"' in agent_readme
    assert 'RENDER_PATH="$OUTPUT_DIR/$STEM.executed.md"' in agent_readme
    assert "~/.ipython" in agent_readme
    assert 'pgrep -lf "agent-notebook run"' in agent_readme
    assert 'pgrep -af "agent-notebook run"' in agent_readme
    assert "Databricks CLI" in agent_readme
    assert "Databricks MCP" in agent_readme
    assert "/Volumes/<catalog>/<schema>/<volume>" in agent_readme
    assert "browser-only URLs" in agent_readme
    assert "DBFS root is a legacy-only fallback" in agent_readme

    assert ".github/workflows/publish.yml" in release
    assert "`testpypi`" in release
    assert "`pypi`" in release
    assert "Trusted Publishing" in release
    assert "manual approval" in release

    support_matrix_path = REPO_ROOT / "docs" / "databricks-support-matrix.md"
    if support_matrix_path.exists():
        support_matrix = read_text(support_matrix_path)
        assert "Verified Local And Offline Surfaces" in support_matrix
        assert "offline artifact verification" in support_matrix
        assert "launcher contract, kernel receipt, runtime receipt" in support_matrix
        assert "Unverified Compute-Mode Surfaces" in support_matrix
        assert "classic cluster" in support_matrix
        assert "serverless" in support_matrix
        assert "not yet claimed" in support_matrix
        assert "planned" not in support_matrix.lower()


def test_top_level_readme_documents_unified_doctor_surface() -> None:
    readme = read_text(REPO_ROOT / "README.md")

    assert "agent-notebook kernels doctor --help" not in readme
    assert "agent-notebook runtimes doctor --help" not in readme
    assert "agent-notebook doctor" in readme
def test_public_docs_exclude_private_plans_and_machine_specific_paths() -> None:
    assert _tracked_files("docs/plans") == []

    tracked_public_docs = [
        path
        for path in _tracked_files(
            "README.md",
            "docs",
            "python/src/databricks_agent_notebooks/for_agents",
        )
        if path.suffix == ".md" and "docs/plans/" not in path.as_posix()
    ]
    forbidden_markers = ("/Users/", "worktrees/databricks-agent-notebooks")
    offenders: list[str] = []

    for path in tracked_public_docs:
        text = path.read_text(encoding="utf-8")
        rel_path = path.relative_to(REPO_ROOT)
        offenders.extend(
            f"{rel_path}: {marker}" for marker in forbidden_markers if marker in text
        )

    assert offenders == []
