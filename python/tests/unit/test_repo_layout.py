from __future__ import annotations

import json
import subprocess
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_pyproject() -> dict[str, object]:
    pyproject_path = REPO_ROOT / "python" / "pyproject.toml"
    return tomllib.loads(read_text(pyproject_path))


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
    assert (REPO_ROOT / "jvm").is_dir()
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
    assert "python -m build" in workflow
    assert "twine check --strict dist/*" in workflow
    assert "tarfile.open" in workflow
    assert "zipfile.ZipFile" in workflow
    assert "licenses/LICENSE" in workflow
    assert "entry_points.txt" in workflow
    assert ".artifact-venv/bin/pip install dist/*.whl" in workflow
    assert "scripts/installed_artifact_installer_proof.py" in workflow
    assert "upload-artifact" in workflow


def test_publish_workflow_uses_trusted_publishing_scaffolding() -> None:
    workflow = read_text(REPO_ROOT / ".github" / "workflows" / "publish.yml")

    assert "workflow_dispatch:" in workflow
    assert "push:" in workflow
    assert "tags:" in workflow
    assert '      - "v*"' in workflow
    assert "upload-artifact@v4" in workflow
    assert "download-artifact@v4" in workflow
    assert "environment: testpypi" in workflow
    assert "environment: pypi" in workflow
    assert "id-token: write" in workflow
    assert "repository-url: https://test.pypi.org/legacy/" in workflow
    assert "if: github.event_name == 'workflow_dispatch'" in workflow
    assert "if: startsWith(github.ref, 'refs/tags/')" in workflow
    assert "python -m build" in workflow
    assert "dist-artifacts" in workflow


def test_release_docs_only_claim_verified_local_offline_scope() -> None:
    readme = read_text(REPO_ROOT / "README.md")
    release = read_text(REPO_ROOT / "docs" / "release.md")
    support_matrix = read_text(REPO_ROOT / "docs" / "databricks-support-matrix.md")

    assert "local/offline" in readme
    assert "Databricks compute-mode support is not yet claimed." in readme
    assert "Verified Install Quickstart" in readme
    assert "python -m pip install -e './python[dev]'" in readme
    assert "agent-notebook help" in readme
    assert "agent-notebook install-kernel --help" in readme
    assert "agent-notebook kernels install --help" in readme
    assert "agent-notebook doctor --help" in readme
    assert "agent-notebook kernels doctor --help" in readme
    assert "agent-notebook runtimes doctor --help" in readme

    assert "Current Verified Evidence" in release
    assert ".github/workflows/publish.yml" in release
    assert "`testpypi`" in release
    assert "`pypi`" in release
    assert "Trusted Publishing" in release
    assert "manual approval" in release
    assert "Actual uploads still depend on GitHub environment protection rules and PyPI/TestPyPI trusted publisher configuration outside this repository." in release
    assert "avoid support or compatibility claims that are not covered by the repository evidence" in release

    assert "Verified Local And Offline Surfaces" in support_matrix
    assert "offline artifact verification" in support_matrix
    assert "launcher contract, kernel receipt, runtime receipt" in support_matrix
    assert "Unverified Compute-Mode Surfaces" in support_matrix
    assert "classic cluster" in support_matrix
    assert "serverless" in support_matrix
    assert "not yet claimed" in support_matrix
    assert "planned" not in support_matrix.lower()


def test_public_docs_exclude_private_plans_and_machine_specific_paths() -> None:
    assert _tracked_files("docs/plans") == []

    tracked_public_docs = [
        path
        for path in _tracked_files("README.md", "docs")
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
