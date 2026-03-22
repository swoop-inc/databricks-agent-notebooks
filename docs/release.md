# Release

The standalone repository should only make release claims that are verified by the repository as it exists today.

## CI Policy

- run validation on every push and pull request
- keep one stable gate job suitable for branch protection
- build sdists and wheels in CI
- smoke test installed artifacts in a clean environment, not only editable installs

## Verified Today

- CI validates artifacts with sdist and wheel builds
- CI validates artifacts with twine metadata validation
- CI validates artifacts with a clean-wheel smoke install
- CI validates artifacts by uploading the built distribution files for inspection

## Publishing Direction

- separate validation workflows from publishing workflows
- keep release metadata and support claims aligned with the documented Databricks support matrix
- publishing is deferred until runtime and kernel support claims are ready to publish

## Current Tranche

The repository now has a Python CI workflow that runs unit tests from the source tree and validates artifacts before making any publishing claims. Publishing is deferred until the runtime and kernel surfaces settle.
