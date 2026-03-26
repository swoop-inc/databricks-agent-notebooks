# Release

Publishing uses PyPI Trusted Publishing via `.github/workflows/publish.yml`.

## How it works

- **TestPyPI:** trigger manually via `workflow_dispatch`
- **PyPI:** push a tag matching `v<project.version>` from `python/pyproject.toml`

The `pypi` GitHub environment should require manual approval.

## External setup

These steps live outside the repository and cannot be derived from code:

1. Create `testpypi` and `pypi` GitHub environments in repository settings
2. Configure each PyPI/TestPyPI trusted publisher to trust this repository, the `publish.yml` workflow, and the matching GitHub environment name
3. Keep PyPI/TestPyPI project names aligned with the package name in `pyproject.toml`
