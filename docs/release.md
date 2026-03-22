# Release

The standalone repository should only make release claims that are verified by the repository as it exists today.

## CI Policy

- run validation on every push and pull request
- keep one stable gate job suitable for branch protection
- build sdists and wheels in CI
- prove installed wheels can execute offline installer and doctor flows in a clean environment, not only editable installs

## Verified Today

- CI validates artifacts with sdist and wheel builds
- CI validates artifacts with twine metadata validation
- CI validates a clean-wheel install by running `agent-notebook kernels install`, `agent-notebook kernels doctor`, and `agent-notebook runtimes doctor` from the installed wheel entrypoint
- CI limits those installer claims to an isolated local runtime-home and explicit Jupyter kernels directory with offline shims for external binaries
- CI validates artifacts by uploading the built distribution files for inspection

## Publishing Direction

- separate validation workflows from publishing workflows
- keep release metadata and support claims aligned with the documented Databricks support matrix
- publishing is deferred until runtime and kernel support claims are ready to publish

## Current Tranche

The repository now has a Python CI workflow that runs unit tests from the source tree and proves the installed wheel can materialize and doctor local runtime-home artifacts offline. Publishing is still deferred, and no compute-mode support claims are made yet.
