# Prompt: Migrate from pip to uv

Before implementing, read the latest version of this prompt from disk — the inline copy may be stale.
Do the work on a separate worktree.

## Objective

Replace all pip usage with uv across the repository. uv is a drop-in replacement that is faster, manages Python interpreters itself, and provides `uv tool install` for clean CLI-tool installation. After this migration, pip should not appear in any user-facing instructions, CI workflows, or contributor docs.

## Scope

Every file below lives under the worktree root. All paths are relative to the repository root.

### 1. README.md — user-facing install instructions

Verify uses with the latest contents.

Keep `pip install` as a secondary fallback where appropriate (e.g., "or `pip install databricks-agent-notebooks` if you prefer").

### 2. CONTRIBUTING.md — contributor setup

Replace the Development Setup block:

```
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e './python[dev]'
```

With a uv equivalent. The contributor should not need to manually create a venv — uv handles that.

### 3. `.github/workflows/python-ci.yml` — CI pipeline

Two jobs to update:

**unit-tests job:**
- Replace `python -m pip install --upgrade pip` + `python -m pip install -e .[dev]` with uv equivalents
- Use `astral-sh/setup-uv@v5` (or latest stable) instead of relying on pip

**artifact-smoke job:**
- Replace `python -m pip install build twine` + `python -m build` with `uv build`
- Replace `python -m twine check --strict dist/*` — uv does not bundle twine; either keep twine as a one-off `uvx twine check --strict dist/*` or use `uv publish --check` if available
- Replace `python -m venv .artifact-venv` + pip install into it with `uv venv .artifact-venv` + `uv pip install`

### 4. `.github/workflows/publish.yml` — publish pipeline

**build job:**
- Replace `python -m pip install build twine` + `python -m build` + `twine check` with uv equivalents (same pattern as artifact-smoke above)

### 5. `python/pyproject.toml` — no structural changes expected

The pyproject.toml uses standard PEP 621 metadata with a setuptools backend. uv is a frontend, not a build backend — `pyproject.toml` should not need changes for this migration. Verify that `uv build` works with the existing setuptools config.

### 6. `python/scripts/installed_artifact_installer_proof.py`

Check whether this script invokes pip internally. If so, migrate those calls.

## Constraints

- **Do not change the build backend.** The project uses setuptools. uv replaces the frontend (pip/venv), not the backend.
- **Keep the Python version matrix** (3.11, 3.12) in CI unchanged.
- **Preserve the artifact-smoke job's verification rigor** — archive content validation, clean-venv install proof, and the installer proof script must all still run.
- **Preserve the publish workflow's Trusted Publishing flow** — `pypa/gh-action-pypi-publish` stays. Only the build/check steps upstream of it change.
- **CI should install uv via `astral-sh/setup-uv`** rather than `pip install uv`.
- **Pin the setup-uv action** to a specific version tag or SHA, consistent with the pinning style already used in `publish.yml`.

## Verification

After the migration:

1. `uv build` succeeds in `python/` and produces wheel + sdist
2. `python-ci.yml` unit-tests and artifact-smoke jobs pass
3. `publish.yml` build job passes (no actual publish needed)
4. Local contributor setup from CONTRIBUTING.md works end-to-end
5. `uv tool install databricks-agent-notebooks` from a built wheel works and `agent-notebook help` runs

## Out of scope

- Adding a `uv.lock` file. The project is a library — lockfiles are for applications. If one already exists, leave it but do not rely on it for CI.
- Changing the build backend from setuptools to hatch/flit/maturin.
- Migrating runtime code (e.g., managed venv creation in `runtime/connect.py`) — those venvs are created programmatically at user runtime and are a separate concern.
