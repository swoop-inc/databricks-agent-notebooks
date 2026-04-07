# Contributing

Pull requests are welcome.

## Development Setup

Set up a local editable environment from the repository root:

```bash
uv venv
source .venv/bin/activate
uv pip install -e './python[dev,local-spark]'
python -m databricks_agent_notebooks help
agent-notebook help
```

## Before Opening A PR

- Ensure local tests are green
- Ensure CI is green
- Follow the installation instructions in a clean (virtual) environment, including the agent prompt
- Verify functionality against a live Databricks environment, covering the serverless/cluster-based and Python/Scala matrix, as needed (see below)

## Databricks Integration Tests

The CI suite includes integration tests that run notebooks against a live Databricks workspace. These tests run automatically on pushes and pull requests when repository secrets are configured.

We encourage contributors to run these tests on their own forks to catch integration issues before submitting a PR.

### Setting Up Secrets on Your Fork

You need two secrets — your Databricks workspace URL and a personal access token. Using the [GitHub CLI](https://cli.github.com/):

```bash
gh secret set DATABRICKS_HOST --body "https://your-workspace.cloud.databricks.com"
gh secret set DATABRICKS_TOKEN --body "dapi..."
```

Run these from your fork's local clone (or pass `--repo your-username/databricks-agent-notebooks`).

### Triggering the Tests

The integration test workflow runs on all pushes and pull requests. If `DATABRICKS_TOKEN` is set, the Databricks job runs automatically alongside the other integration jobs (kernel lifecycle and local Spark tests, which don't require credentials).

When secrets are unavailable — for example, a PR from a fork to the upstream repo — the Databricks job is skipped gracefully. The other jobs still run.

## Notes

- Keep documentation and examples public-facing and repo-relative.
