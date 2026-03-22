# Databricks Support Matrix

Support claims for this tool should be compute-aware.

## Verified Local And Offline Surfaces

| Surface | Status | Evidence |
|---------|--------|----------|
| Python packaging metadata | verified locally | `python/pyproject.toml` and CI artifact validation |
| sdist and wheel builds | verified locally | `python-ci` builds both artifacts on every push and pull request |
| clean-wheel install and CLI smoke checks | verified locally | `python-ci` installs the built wheel into a clean virtualenv |
| notebook conversion, rendering, and runtime inventory helpers | verified locally | `python/tests/unit` exercises these local and offline paths |

## Unverified Compute-Mode Surfaces

Compute-mode support is not yet claimed.

| Compute mode | Status | Notes |
|--------------|--------|-------|
| classic cluster | not yet claimed | no release claim today |
| serverless | not yet claimed | no release claim today |
