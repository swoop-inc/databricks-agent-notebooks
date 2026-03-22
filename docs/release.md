# Release

The standalone repository should only make release claims that are verified by the repository as it exists today.

## CI Policy

- run validation on every push and pull request
- keep one stable gate job suitable for branch protection
- build sdists and wheels in CI
- validate distribution metadata and archive structure before installation
- prove installed wheels can execute offline installer and doctor flows in a clean environment, not only editable installs

## Current Verified Evidence

- the Python CI workflow runs `pytest` against `python/tests/unit`
- CI builds both wheel and sdist artifacts, runs `twine check --strict`, and inspects the built archives
- CI installs the built wheel into a clean virtualenv, verifies the installed CLI entry point, and runs the offline installer proof against isolated local paths
- release/support messaging should avoid support or compatibility claims that are not covered by the repository evidence

## Publishing Direction

- separate validation workflows from publishing workflows
- keep release metadata and support claims aligned with the documented Databricks support matrix
- publishing is deferred until runtime and kernel support claims are ready to publish

## Trusted Publishing Scaffold

The publication scaffold lives in `.github/workflows/publish.yml`.

- manual `workflow_dispatch` runs are reserved for TestPyPI-style publishing only
- `push` handling is tag-only (`v*`), so ordinary branch pushes do not publish
- the workflow builds distributions once, uploads them as `dist-artifacts`, and only then hands them to the publish jobs
- the publish jobs use the GitHub environments `testpypi` and `pypi`
- each publish job requests `permissions: id-token: write` for Trusted Publishing

Trusted Publishing prerequisites:

- create the `testpypi` and `pypi` GitHub environments in repository settings
- configure PyPI and TestPyPI trusted publishers to trust this repository, the `.github/workflows/publish.yml` workflow, and the matching GitHub environment
- keep the PyPI/TestPyPI project names and repository metadata aligned with the package being uploaded

The `pypi` environment should require manual approval before the tag-driven publish job can proceed.

Actual uploads still depend on GitHub environment protection rules and PyPI/TestPyPI trusted publisher configuration outside this repository.

## Current Tranche

The repository now has a Python CI workflow that runs unit tests from the source tree, validates built artifacts, proves the installed wheel can execute local/offline installer flows, and includes Trusted Publishing scaffolding for TestPyPI and PyPI. Actual publishing remains gated on external GitHub and package-index configuration, and release/support claims should stay limited to the local/offline surface that the repository can verify today.
