# Release

The standalone repository should validate built artifacts before making release claims.

## CI Policy

- run validation on every push and pull request
- keep one stable gate job suitable for branch protection
- build sdists and wheels in CI
- smoke test installed artifacts in a clean environment, not only editable installs

## Publishing Direction

- separate validation workflows from publishing workflows
- prefer Trusted Publishing for PyPI when release automation is added
- keep release metadata and support claims aligned with the documented Databricks support matrix

## Current Tranche

The repository now has a Python CI workflow that runs unit tests from the source tree and performs a clean-wheel smoke install. Release publishing automation is intentionally deferred until the runtime and kernel surfaces settle.
