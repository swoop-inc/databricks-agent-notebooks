from __future__ import annotations

import json
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


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
    pyproject_path = REPO_ROOT / "python" / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    dependencies = pyproject["project"]["dependencies"]
    assert any(dep.startswith("nbconvert") for dep in dependencies)


def test_python_distribution_declares_public_release_metadata() -> None:
    pyproject_path = REPO_ROOT / "python" / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = pyproject["project"]

    assert project["description"] == (
        "Standalone notebook conversion and local runtime tooling for the "
        "databricks-agent-notebooks repository."
    )
    assert project["license"] == "MIT"
    assert project["license-files"] == ["LICENSE"]
    assert project["authors"] == [{"name": "Swoop"}]
    assert set(project["keywords"]) >= {"databricks", "jupyter", "notebook"}

    classifiers = set(project["classifiers"])
    assert "Programming Language :: Python :: 3" in classifiers
    assert "Programming Language :: Python :: 3.11" in classifiers
    assert "Programming Language :: Python :: 3.12" in classifiers

    assert project["urls"] == {
        "Homepage": "https://github.com/swoop-inc/databricks-agent-notebooks",
        "Repository": "https://github.com/swoop-inc/databricks-agent-notebooks",
        "Issues": "https://github.com/swoop-inc/databricks-agent-notebooks/issues",
    }

    license_path = REPO_ROOT / "python" / project["license-files"][0]
    assert license_path.read_text(encoding="utf-8").startswith("MIT License")


def test_python_ci_validates_built_artifacts() -> None:
    workflow_text = (
        REPO_ROOT / ".github" / "workflows" / "python-ci.yml"
    ).read_text(encoding="utf-8")

    assert "python -m twine check --strict dist/*" in workflow_text
    assert "Validate sdist and wheel contents" in workflow_text
    assert "dist-info/licenses/LICENSE" in workflow_text
    assert ".artifact-venv/bin/pip install dist/*.whl" in workflow_text
    assert "scripts/installed_artifact_installer_proof.py" in workflow_text
    assert "upload-artifact" in workflow_text


def test_release_docs_only_make_verified_support_claims() -> None:
    readme_text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    release_text = (REPO_ROOT / "docs" / "release.md").read_text(encoding="utf-8")
    matrix_text = (
        REPO_ROOT / "docs" / "databricks-support-matrix.md"
    ).read_text(encoding="utf-8")

    assert "compute-mode support is not yet claimed" in readme_text
    assert "Databricks integration helpers" not in readme_text

    assert "Verified Today" in release_text
    assert "sdist and wheel builds" in release_text
    assert "twine metadata validation" in release_text
    assert "agent-notebook kernels install" in release_text
    assert "isolated local runtime-home" in release_text
    assert "publishing" in release_text.lower()
    assert "deferred" in release_text.lower()
    assert "ci validates artifacts" in release_text.lower()

    assert "Verified Local And Offline Surfaces" in matrix_text
    assert "offline installer proof" in matrix_text
    assert "launcher contract, kernel receipt, runtime receipt" in matrix_text
    assert "Unverified Compute-Mode Surfaces" in matrix_text
    assert "classic cluster" in matrix_text
    assert "serverless" in matrix_text
    assert "not yet claimed" in matrix_text
    assert "planned" not in matrix_text.lower()
