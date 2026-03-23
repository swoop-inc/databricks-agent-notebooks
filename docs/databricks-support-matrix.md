# Databricks Support Matrix

Support claims for this tool should be compute-aware.

## Verified Local And Offline Surfaces

| Surface | Status | Evidence |
|---------|--------|----------|
| Python packaging metadata | verified locally | `python/pyproject.toml` and CI artifact validation |
| sdist and wheel builds | verified locally | `python-ci` builds both artifacts on every push and pull request |
| clean-wheel install and offline installer proof | verified locally | `python-ci` installs the built wheel into a clean virtualenv and runs `scripts/installed_artifact_installer_proof.py` against an isolated runtime-home, explicit kernels dir, and a real `agent-notebook run` smoke for a Python notebook |
| managed kernel and runtime receipt cross-references | verified locally | installed-wheel proof validates launcher contract, kernel receipt, runtime receipt, and kernelspec metadata from the built artifact context |
| notebook conversion, rendering, runtime inventory, and Python kernel preflight helpers | verified locally | `python/tests/unit` exercises these local and offline paths |

These claims remain limited to offline artifact verification. They do not claim live Databricks connectivity or any compute-mode behavior.

## Unverified Compute-Mode Surfaces

Compute-mode support is not yet claimed.

| Compute mode | Status | Notes |
|--------------|--------|-------|
| classic cluster | not yet claimed | no release claim today |
| serverless | not yet claimed | no release claim today |
