# Kernels

Kernel installation and launch should stay generated and machine-addressable.

## Principles

- kernelspec directories are identified by stable machine ids
- display names are user-facing labels, not primary identities
- `kernel.json` should stay thin and point at a launcher/bootstrap boundary
- Spark and Databricks wiring belongs in launcher code, not handwritten kernelspec env blocks

## Intended CLI Surface

The standalone CLI should grow toward:

- `agent-notebook kernels install`
- `agent-notebook kernels list`
- `agent-notebook kernels remove`
- `agent-notebook kernels doctor`

## Current Tranche

This extraction only establishes the repository lane, runtime-home layout, and shared launcher contract schema. Real kernel installation logic still needs to move from the current stubbed `install-kernel` path to a generated launcher-driven implementation.
