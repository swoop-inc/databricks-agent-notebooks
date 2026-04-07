"""Tests for notebook lifecycle extensibility: hooks, parameters_setup, prologue cells."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import nbformat
import pytest

from databricks_agent_notebooks.config.frontmatter import (
    AgentNotebookConfig,
    _parse_config_block,
    merge_config,
)
from databricks_agent_notebooks.config.resolution import CONFIG_KEYS
from databricks_agent_notebooks.execution.injection import (
    _build_parameters_dict,
    _generate_parameters_code,
    _make_parameters_cell,
    inject_lifecycle_cells,
    is_injected_cell,
    parse_cell_spec,
)
from databricks_agent_notebooks.execution.executor import _build_cell_label
from databricks_agent_notebooks.execution.lineage import ExecutionLineage
from databricks_agent_notebooks.execution.rendering import (
    _cell_has_error,
    render_markdown,
)


def _make_notebook(language: str = "python") -> nbformat.NotebookNode:
    notebook = nbformat.v4.new_notebook()
    notebook.cells = [nbformat.v4.new_code_cell("x = 1" if language == "python" else "val x = 1")]
    if language == "scala":
        notebook.metadata["kernelspec"] = {"name": "scala212-dbr-connect", "language": "scala"}
    else:
        notebook.metadata["kernelspec"] = {"name": "python3", "language": "python"}
    return notebook


def _make_lineage() -> ExecutionLineage:
    return ExecutionLineage(
        source_path="/tmp/test.md",
        timestamp="2025-01-15T10:30:00+00:00",
        git_branch="main",
        git_commit="abc1234",
    )


# ---------------------------------------------------------------------------
# Config layer: hooks field
# ---------------------------------------------------------------------------


class TestHooksConfig:
    def test_hooks_in_config_keys(self) -> None:
        assert "hooks" in CONFIG_KEYS

    def test_parse_config_block_with_hooks(self) -> None:
        block = {
            "profile": "dev",
            "hooks": {
                "python": {
                    "prologue_cells": ["print('hello')"],
                },
            },
        }
        config = _parse_config_block(block)
        assert config.hooks == {"python": {"prologue_cells": ["print('hello')"]}}

    def test_parse_config_block_rejects_non_dict_hooks(self) -> None:
        block = {"hooks": "not-a-dict"}
        config = _parse_config_block(block)
        assert config.hooks is None

    def test_parse_config_block_rejects_hooks_with_non_dict_values(self) -> None:
        block = {"hooks": {"python": "not-a-dict"}}
        config = _parse_config_block(block)
        assert config.hooks is None

    def test_merge_config_hooks_override(self) -> None:
        base = AgentNotebookConfig(hooks={"python": {"prologue_cells": ["a"]}})
        override = AgentNotebookConfig(hooks={"python": {"prologue_cells": ["b"]}})
        merged = merge_config(base, override)
        assert merged.hooks == {"python": {"prologue_cells": ["b"]}}

    def test_merge_config_hooks_none_override_keeps_base(self) -> None:
        base = AgentNotebookConfig(hooks={"python": {"prologue_cells": ["a"]}})
        override = AgentNotebookConfig()
        merged = merge_config(base, override)
        assert merged.hooks == {"python": {"prologue_cells": ["a"]}}

    def test_merge_config_hooks_both_none(self) -> None:
        merged = merge_config(AgentNotebookConfig(), AgentNotebookConfig())
        assert merged.hooks is None

    def test_parse_config_block_normalizes_hyphenated_hook_keys(self) -> None:
        block = {
            "hooks": {
                "python": {
                    "prologue-cells": ["print('hello')"],
                    "parameters-setup": "custom()",
                },
            },
        }
        config = _parse_config_block(block)
        assert "prologue_cells" in config.hooks["python"]
        assert "parameters_setup" in config.hooks["python"]

    def test_from_resolved_params_extracts_hooks(self) -> None:
        resolved = {
            "profile": "dev",
            "hooks": {"python": {"prologue_cells": ["setup()"]}},
            "env": "default",
        }
        config, params = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.hooks == {"python": {"prologue_cells": ["setup()"]}}
        assert "hooks" not in params


# ---------------------------------------------------------------------------
# Parameters setup cell
# ---------------------------------------------------------------------------


class TestParametersSetup:
    def test_build_parameters_dict_config_fields(self) -> None:
        config = AgentNotebookConfig(
            profile="dev-sp",
            timeout=300,
            allow_errors=False,
            inject_session=True,
            preprocess=True,
            clean=False,
            format="all",
            libraries=("/my/lib", "/other/lib"),
        )
        result = _build_parameters_dict(config, None)
        assert result["profile"] == "dev-sp"
        assert result["timeout"] == 300
        assert result["allow_errors"] is False
        assert result["libraries"] == ["/my/lib", "/other/lib"]
        # Framework-internal fields excluded
        assert "params" not in result
        assert "hooks" not in result
        assert "inject_session" not in result
        assert "preprocess" not in result
        assert "clean" not in result
        assert "format" not in result

    def test_build_parameters_dict_with_notebook_params(self) -> None:
        config = AgentNotebookConfig(profile="dev")
        params = {"batch_size": 100, "table_name": "my_table", "enabled": True}
        result = _build_parameters_dict(config, params)
        assert result["profile"] == "dev"
        assert result["batch_size"] == 100
        assert result["table_name"] == "my_table"
        assert result["enabled"] is True

    def test_build_parameters_dict_excludes_none_fields(self) -> None:
        config = AgentNotebookConfig(profile="dev")
        result = _build_parameters_dict(config, None)
        assert "cluster" not in result
        assert "timeout" not in result

    def test_generate_parameters_code(self) -> None:
        code = _generate_parameters_code({"profile": "dev", "timeout": 300})
        assert "import json as _json" in code
        assert "agent_notebook_parameters = _json.loads(" in code
        assert "del _json" in code
        # Execute the code to verify it produces a valid dict
        ns: dict = {}
        exec(code, ns)
        assert ns["agent_notebook_parameters"]["profile"] == "dev"
        assert ns["agent_notebook_parameters"]["timeout"] == 300

    def test_make_parameters_cell_metadata(self) -> None:
        config = AgentNotebookConfig(profile="dev")
        cell = _make_parameters_cell(config, None)
        assert cell.metadata["agent_notebook_injected"] is True
        assert cell.metadata["agent_notebook_cell_role"] == "parameters"
        assert cell.cell_type == "code"

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_injects_parameters_cell_first(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(profile="dev", inject_session=True)

        inject_lifecycle_cells(notebook, config, inject_session=True)

        assert notebook.cells[0].metadata["agent_notebook_cell_role"] == "parameters"
        assert notebook.cells[1].metadata["agent_notebook_cell_role"] == "session"
        assert notebook.cells[2].source == "x = 1"

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_parameters_without_session(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(profile="dev")

        inject_lifecycle_cells(notebook, config, inject_session=False)

        assert len(notebook.cells) == 2  # parameters + content
        assert notebook.cells[0].metadata["agent_notebook_cell_role"] == "parameters"
        assert notebook.cells[1].source == "x = 1"

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_parameters_typed_values(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(profile="dev", timeout=300)
        typed_params = {"batch_size": 100, "enabled": True}

        inject_lifecycle_cells(
            notebook, config,
            inject_session=False,
            notebook_params=typed_params,
        )

        code = notebook.cells[0].source
        assert "agent_notebook_parameters" in code
        # Execute the code to verify it produces a valid dict
        ns: dict = {}
        exec(code, ns)
        params = ns["agent_notebook_parameters"]
        assert params["profile"] == "dev"
        assert params["timeout"] == 300
        assert params["batch_size"] == 100
        assert params["enabled"] is True

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_sql_maps_to_python(self, mock_capture) -> None:
        """SQL notebooks get parameters cell and Python-language prologue."""
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        notebook.metadata["kernelspec"]["language"] = "sql"
        config = AgentNotebookConfig(
            profile="dev",
            hooks={"python": {"prologue_cells": ["setup()"]}},
        )

        inject_lifecycle_cells(notebook, config, inject_session=False, language="sql")

        assert notebook.cells[0].metadata["agent_notebook_cell_role"] == "parameters"
        assert notebook.cells[1].source == "setup()"

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_scala_skips_parameters_cell(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook(language="scala")
        config = AgentNotebookConfig(profile="dev")

        inject_lifecycle_cells(notebook, config, inject_session=True, language="scala")

        # First cell should be session, not parameters (Scala deferred)
        assert notebook.cells[0].metadata["agent_notebook_cell_role"] == "session"


# ---------------------------------------------------------------------------
# Prologue cells: cell type detection
# ---------------------------------------------------------------------------


class TestPrologueCells:
    def test_plain_code_is_hidden(self) -> None:
        cell = parse_cell_spec("print('hello')", "python")
        assert cell.cell_type == "code"
        assert cell.source == "print('hello')"
        assert cell.metadata["agent_notebook_injected"] is True
        assert cell.metadata["agent_notebook_cell_role"] == "prologue"

    def test_fenced_markdown_is_visible(self) -> None:
        spec = "```markdown\n# Setup Notes\nThis runs before content.\n```"
        cell = parse_cell_spec(spec, "python")
        assert cell.cell_type == "markdown"
        assert cell.source == "# Setup Notes\nThis runs before content."
        assert cell.metadata.get("agent_notebook_injected") is not True
        assert cell.metadata["agent_notebook_cell_role"] == "prologue"

    def test_fenced_code_is_visible(self) -> None:
        spec = "```python\nprint('visible')\n```"
        cell = parse_cell_spec(spec, "python")
        assert cell.cell_type == "code"
        assert cell.source == "print('visible')"
        assert cell.metadata.get("agent_notebook_injected") is not True
        assert cell.metadata["agent_notebook_cell_role"] == "prologue"

    def test_extended_fence_allows_nested_fences(self) -> None:
        spec = "``````markdown\n# Title\n```python\ncode\n```\n``````"
        cell = parse_cell_spec(spec, "python")
        assert cell.cell_type == "markdown"
        assert "```python" in cell.source
        assert "code" in cell.source

    def test_fenced_code_language_case_insensitive(self) -> None:
        spec = "```Python\nprint('hi')\n```"
        cell = parse_cell_spec(spec, "python")
        assert cell.cell_type == "code"
        assert cell.metadata.get("agent_notebook_injected") is not True

    def test_fenced_wrong_language_is_hidden_with_fences_stripped(self) -> None:
        spec = "```scala\nval x = 1\n```"
        cell = parse_cell_spec(spec, "python")
        # Fence tag doesn't match notebook language -> hidden code, fences stripped
        assert cell.cell_type == "code"
        assert cell.metadata["agent_notebook_injected"] is True
        assert cell.source == "val x = 1"
        assert "```" not in cell.source

    def test_multiline_hidden_code(self) -> None:
        spec = "import os\nos.environ['KEY'] = 'value'\nprint('setup done')"
        cell = parse_cell_spec(spec, "python")
        assert cell.cell_type == "code"
        assert cell.metadata["agent_notebook_injected"] is True
        assert "import os" in cell.source

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_injects_prologue_after_session(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(
            profile="dev",
            hooks={"python": {"prologue_cells": ["print('setup')"]}},
        )

        inject_lifecycle_cells(notebook, config, inject_session=True)

        assert notebook.cells[0].metadata["agent_notebook_cell_role"] == "parameters"
        assert notebook.cells[1].metadata["agent_notebook_cell_role"] == "session"
        assert notebook.cells[2].metadata["agent_notebook_cell_role"] == "prologue"
        assert notebook.cells[2].source == "print('setup')"
        assert notebook.cells[3].source == "x = 1"

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_multiple_prologue_cells(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(
            profile="dev",
            hooks={"python": {"prologue_cells": [
                "import os",
                "```markdown\n# Notes\n```",
                "```python\nprint('visible')\n```",
            ]}},
        )

        inject_lifecycle_cells(notebook, config, inject_session=False)

        # parameters + 3 prologue + 1 content = 5
        assert len(notebook.cells) == 5
        assert notebook.cells[1].source == "import os"
        assert notebook.cells[1].metadata["agent_notebook_injected"] is True
        assert notebook.cells[2].cell_type == "markdown"
        assert notebook.cells[3].cell_type == "code"
        assert notebook.cells[3].metadata.get("agent_notebook_injected") is not True

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_prologue_jinja_preprocessing(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(
            profile="dev",
            hooks={"python": {"prologue_cells": ["x = 'REPLACED'"]}},
        )

        def mock_preprocess(text: str) -> str:
            return text.replace("REPLACED", "hello")

        inject_lifecycle_cells(
            notebook, config,
            inject_session=False,
            preprocess_fn=mock_preprocess,
        )

        assert notebook.cells[1].source == "x = 'hello'"

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_empty_prologue_specs_skipped(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(
            profile="dev",
            hooks={"python": {"prologue_cells": ["", "  ", "print('ok')"]}},
        )

        inject_lifecycle_cells(notebook, config, inject_session=False)

        # parameters + 1 non-empty prologue + 1 content = 3
        assert len(notebook.cells) == 3

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_no_hooks_no_prologue(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(profile="dev")

        inject_lifecycle_cells(notebook, config, inject_session=False)

        # parameters + content only
        assert len(notebook.cells) == 2


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestLifecycleIdempotency:
    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_removes_old_injected_cells(self, mock_capture) -> None:
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(profile="dev")

        inject_lifecycle_cells(notebook, config, inject_session=True)
        first_count = len(notebook.cells)

        inject_lifecycle_cells(notebook, config, inject_session=True)
        second_count = len(notebook.cells)

        assert first_count == second_count

    @patch("databricks_agent_notebooks.execution.injection.capture_pre_execution")
    def test_lifecycle_idempotent_with_visible_prologue(self, mock_capture) -> None:
        """Visible prologue cells must not accumulate on repeated calls."""
        mock_capture.return_value = _make_lineage()
        notebook = _make_notebook()
        config = AgentNotebookConfig(
            profile="dev",
            hooks={"python": {"prologue_cells": [
                "```python\nprint('visible')\n```",
                "```markdown\n# Notes\n```",
                "hidden_setup()",
            ]}},
        )

        inject_lifecycle_cells(notebook, config, inject_session=True)
        first_count = len(notebook.cells)

        inject_lifecycle_cells(notebook, config, inject_session=True)
        second_count = len(notebook.cells)

        assert first_count == second_count


# ---------------------------------------------------------------------------
# Error-aware rendering
# ---------------------------------------------------------------------------


class TestErrorAwareRendering:
    def test_cell_has_error_true(self) -> None:
        cell = nbformat.v4.new_code_cell("bad_code")
        cell.outputs = [nbformat.v4.new_output("error", ename="NameError", evalue="x")]
        assert _cell_has_error(cell) is True

    def test_cell_has_error_false_no_outputs(self) -> None:
        cell = nbformat.v4.new_code_cell("good_code")
        assert _cell_has_error(cell) is False

    def test_cell_has_error_false_stream_output(self) -> None:
        cell = nbformat.v4.new_code_cell("print('hi')")
        cell.outputs = [nbformat.v4.new_output("stream", text="hi\n")]
        assert _cell_has_error(cell) is False

    def test_render_markdown_shows_failed_injected_cell(self, tmp_path: Path) -> None:
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {"language": "python"}

        # Injected cell WITH error
        injected = nbformat.v4.new_code_cell("raise Exception('boom')")
        injected.metadata["agent_notebook_injected"] = True
        injected.outputs = [
            nbformat.v4.new_output("error", ename="Exception", evalue="boom"),
        ]

        content = nbformat.v4.new_code_cell("x = 1")
        notebook.cells = [injected, content]

        path = tmp_path / "test.ipynb"
        nbformat.write(notebook, str(path))
        out = tmp_path / "test.md"
        render_markdown(path, out)

        md = out.read_text(encoding="utf-8")
        assert "raise Exception('boom')" in md
        assert "Exception: boom" in md

    def test_render_markdown_hides_successful_injected_cell(self, tmp_path: Path) -> None:
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {"language": "python"}

        injected = nbformat.v4.new_code_cell("setup_code()")
        injected.metadata["agent_notebook_injected"] = True
        injected.outputs = []

        content = nbformat.v4.new_code_cell("x = 1")
        notebook.cells = [injected, content]

        path = tmp_path / "test.ipynb"
        nbformat.write(notebook, str(path))
        out = tmp_path / "test.md"
        render_markdown(path, out)

        md = out.read_text(encoding="utf-8")
        assert "setup_code()" not in md
        assert "x = 1" in md


# ---------------------------------------------------------------------------
# Executor labels
# ---------------------------------------------------------------------------


class TestExecutorLabels:
    def test_parameters_role_label(self) -> None:
        cell = nbformat.v4.new_code_cell("params code")
        cell.metadata["agent_notebook_cell_role"] = "parameters"
        cell.metadata["agent_notebook_injected"] = True
        label = _build_cell_label(cell, ["params code"])
        assert "Parameters setup" in label

    def test_session_role_label(self) -> None:
        cell = nbformat.v4.new_code_cell("session code")
        cell.metadata["agent_notebook_cell_role"] = "session"
        cell.metadata["agent_notebook_injected"] = True
        label = _build_cell_label(cell, ["session code"])
        assert "Session setup" in label

    def test_prologue_role_label(self) -> None:
        cell = nbformat.v4.new_code_cell("prologue code")
        cell.metadata["agent_notebook_cell_role"] = "prologue"
        cell.metadata["agent_notebook_injected"] = True
        label = _build_cell_label(cell, ["prologue code"])
        assert "Prologue" in label

    def test_epilogue_role_label(self) -> None:
        cell = nbformat.v4.new_code_cell("epilogue code")
        cell.metadata["agent_notebook_cell_role"] = "epilogue"
        cell.metadata["agent_notebook_injected"] = True
        label = _build_cell_label(cell, ["epilogue code"])
        assert "Epilogue" in label

    def test_injected_without_role_fallback(self) -> None:
        cell = nbformat.v4.new_code_cell("old injected")
        cell.metadata["agent_notebook_injected"] = True
        label = _build_cell_label(cell, ["old injected"])
        assert "Setup" in label

    def test_normal_cell_label(self) -> None:
        cell = nbformat.v4.new_code_cell("x = 1")
        label = _build_cell_label(cell, ["x = 1"])
        assert label == "[code cell]"


# ---------------------------------------------------------------------------
# -FILE with hooks config
# ---------------------------------------------------------------------------


class TestFileRefsWithHooks:
    def test_file_refs_resolve_prologue_cells(self, tmp_path: Path) -> None:
        from databricks_agent_notebooks.config.file_refs import resolve_file_refs

        setup_file = tmp_path / "setup.py"
        setup_file.write_text("import os\nos.environ['KEY'] = 'value'")

        data = {
            "hooks": {
                "python": {
                    "prologue_cells-FILE": [str(setup_file)],
                },
            },
        }

        result = resolve_file_refs(data, tmp_path)
        assert "prologue_cells" in result["hooks"]["python"]
        cells = result["hooks"]["python"]["prologue_cells"]
        assert len(cells) == 1
        assert "import os" in cells[0]

    def test_file_refs_resolve_single_file_hook(self, tmp_path: Path) -> None:
        from databricks_agent_notebooks.config.file_refs import resolve_file_refs

        setup_file = tmp_path / "params_setup.py"
        setup_file.write_text("custom_params_setup()")

        data = {
            "hooks": {
                "python": {
                    "parameters_setup-FILE": str(setup_file),
                },
            },
        }

        result = resolve_file_refs(data, tmp_path)
        assert result["hooks"]["python"]["parameters_setup"] == "custom_params_setup()"

    def test_file_refs_through_project_config(self, tmp_path: Path) -> None:
        """End-to-end: -FILE keys in pyproject.toml flow through load_project_source_map."""
        from databricks_agent_notebooks.config.project import load_project_source_map

        # Create a hook file
        hooks_dir = tmp_path / "hooks"
        hooks_dir.mkdir()
        (hooks_dir / "setup.py").write_text("spark.sql('USE CATALOG dev')")

        # Create pyproject.toml with -FILE reference
        (tmp_path / "pyproject.toml").write_text(
            '[tool.agent-notebook]\n'
            'profile = "test"\n'
            '\n'
            '[tool.agent-notebook.hooks.python]\n'
            'prologue_cells-FILE = ["./hooks/setup.py"]\n'
        )
        # load_project_source_map walks up to .git boundary
        (tmp_path / ".git").mkdir()

        source_map, base_dir = load_project_source_map(tmp_path)

        assert base_dir == tmp_path
        # The -FILE key should be resolved to file contents
        assert "hooks" in source_map
        hooks = source_map["hooks"]
        assert "python" in hooks
        prologue = hooks["python"]["prologue_cells"]
        assert len(prologue) == 1
        assert prologue[0] == "spark.sql('USE CATALOG dev')"
        # The -FILE key itself should be gone
        assert "prologue_cells_FILE" not in hooks["python"]
        assert "prologue_cells-FILE" not in hooks["python"]
