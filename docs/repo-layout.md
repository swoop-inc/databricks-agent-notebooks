# Repository Layout

This repository is monorepo-ready from day one.

- `python/`: active v1 package, CLI, and tests
- `jvm/`: reserved lane for future JVM and Scala work
- `contracts/`: machine-readable launcher/runtime contracts
- `docs/`: architecture and operational docs
- `.github/workflows/`: CI for artifact-first validation

The current tranche keeps v1 to one active Python distribution and leaves the JVM lane as reserved topology rather than a live artifact.
