"""Development script for regenerating plugin documentation.

Run from the python/ directory:

    uv run python -m databricks_agent_notebooks._build

Regenerates for_agents/preprocessing/plugins.md from live plugin metadata.
The output is checked into the repository so it ships with the installed
package without requiring a custom build hook.
"""

from __future__ import annotations

from pathlib import Path


def regenerate_plugin_docs() -> Path:
    """Regenerate the plugin reference markdown and return the output path."""
    from databricks_agent_notebooks.preprocessing.docs import generate_plugin_docs

    out_dir = Path(__file__).resolve().parent / "for_agents" / "preprocessing"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "plugins.md"
    out_path.write_text(generate_plugin_docs())
    return out_path


if __name__ == "__main__":
    path = regenerate_plugin_docs()
    print(f"Regenerated {path}")
