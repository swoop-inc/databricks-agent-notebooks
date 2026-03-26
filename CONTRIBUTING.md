# Contributing

Pull requests are welcome.

## Development Setup

Set up a local editable environment from the repository root:

```bash
uv venv
source .venv/bin/activate
uv pip install -e './python[dev]'
python -m databricks_agent_notebooks help
agent-notebook help
```

## Before Opening A PR

- Ensure local tests are green
- Ensure CI is green
- Follow the installation instructions in a clean (virtual) environment, including the agent prompt
- Verify functionality against a live Databricks environment, covering the serverless/cluster-based and Python/Scala matrix, as needed

## Notes

- Keep documentation and examples public-facing and repo-relative.
