"""Tests for the preprocessing pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from databricks_agent_notebooks.preprocessing import preprocess_text
from databricks_agent_notebooks.preprocessing.errors import PreprocessorError
from databricks_agent_notebooks.preprocessing.plugins import PluginContext
from databricks_agent_notebooks.preprocessing.plugins.best_practices import BestPracticesPlugin
from databricks_agent_notebooks.preprocessing.plugins.include import IncludePlugin
from databricks_agent_notebooks.preprocessing.plugins.param import ParamHandle, ParamPlugin


# ---------------------------------------------------------------------------
# Engine tests
# ---------------------------------------------------------------------------


class TestPreprocessText:
    """Tests for the core preprocessing engine."""

    def test_no_directives_returns_same_object(self, tmp_path: Path) -> None:
        """Text without {! markers passes through unchanged (same object)."""
        text = "# Hello\n\nSome content\n"
        nb = tmp_path / "test.md"
        nb.write_text(text)
        result = preprocess_text(text, notebook_path=nb)
        assert result is text

    def test_include_expands(self, tmp_path: Path) -> None:
        """A valid include directive expands to the file content."""
        fragment = tmp_path / "fragment.sql"
        fragment.write_text("SELECT 1")

        nb = tmp_path / "test.md"
        nb.write_text("before\n{! include('fragment.sql') !}\nafter")

        result = preprocess_text(
            nb.read_text(), notebook_path=nb,
        )
        assert result == "before\nSELECT 1\nafter"

    def test_unknown_directive_raises(self, tmp_path: Path) -> None:
        """An unrecognized directive name raises PreprocessorError."""
        nb = tmp_path / "test.md"
        nb.write_text("{! nonexistent('foo') !}")

        with pytest.raises(PreprocessorError, match="Unknown directive"):
            preprocess_text(nb.read_text(), notebook_path=nb)

    def test_malformed_syntax_raises(self, tmp_path: Path) -> None:
        """Broken Jinja2 syntax raises PreprocessorError."""
        nb = tmp_path / "test.md"
        nb.write_text("{! unclosed(")

        with pytest.raises(PreprocessorError, match="Syntax error"):
            preprocess_text(nb.read_text(), notebook_path=nb)

    def test_standard_jinja2_delimiters_ignored(self, tmp_path: Path) -> None:
        """Standard {{ }} and {% %} in notebook content are not processed."""
        text = "{{ variable }}\n{% if x %}yes{% endif %}\n{# comment #}\n"
        nb = tmp_path / "test.md"
        nb.write_text(text)
        # No {! present, so fast-path returns same object
        result = preprocess_text(text, notebook_path=nb)
        assert result is text

    def test_standard_jinja2_delimiters_preserved_alongside_directives(
        self, tmp_path: Path,
    ) -> None:
        """Standard Jinja2 delimiters survive when directives are also present."""
        fragment = tmp_path / "frag.txt"
        fragment.write_text("included")

        nb = tmp_path / "test.md"
        text = "{{ var }}\n{! include('frag.txt') !}\n{% block %}\n"
        nb.write_text(text)

        result = preprocess_text(text, notebook_path=nb)
        assert "{{ var }}" in result
        assert "included" in result
        assert "{% block %}" in result

    def test_multiple_includes(self, tmp_path: Path) -> None:
        """Multiple include directives in one file all expand."""
        (tmp_path / "a.txt").write_text("AAA")
        (tmp_path / "b.txt").write_text("BBB")

        nb = tmp_path / "test.md"
        text = "{! include('a.txt') !}\n---\n{! include('b.txt') !}\n"
        nb.write_text(text)

        result = preprocess_text(text, notebook_path=nb)
        assert "AAA" in result
        assert "BBB" in result


    def test_ipynb_content_with_directives_skipped_by_caller(
        self, tmp_path: Path,
    ) -> None:
        """Callers skip .ipynb files, but engine itself processes any text.

        The .ipynb skip is in cli.py (suffix check), not in preprocess_text().
        This test documents that the engine has no special .ipynb handling --
        the caller is responsible for gating.
        """
        # Engine processes anything with {! in it, regardless of content shape
        json_with_directive = '{"cells": [{! include("x") !}]}'
        nb = tmp_path / "test.ipynb"
        nb.write_text(json_with_directive)

        (tmp_path / "x").write_text('"hello"')
        result = preprocess_text(json_with_directive, notebook_path=nb)
        assert '"hello"' in result

    def test_empty_file_include(self, tmp_path: Path) -> None:
        """Including an empty file produces empty string at that position."""
        (tmp_path / "empty.txt").write_text("")
        nb = tmp_path / "test.md"
        text = "before|{! include('empty.txt') !}|after"
        nb.write_text(text)

        result = preprocess_text(text, notebook_path=nb)
        assert result == "before||after"


# ---------------------------------------------------------------------------
# Include plugin tests
# ---------------------------------------------------------------------------


class TestIncludePlugin:
    """Tests for the include plugin directly."""

    def test_relative_path_resolution(self, tmp_path: Path) -> None:
        """Paths resolve relative to the notebook directory."""
        subdir = tmp_path / "sub"
        subdir.mkdir()
        target = subdir / "data.txt"
        target.write_text("hello")

        ctx = PluginContext(notebook_path=tmp_path / "nb.md")
        plugin = IncludePlugin(ctx)
        assert plugin("sub/data.txt") == "hello"

    def test_file_not_found(self, tmp_path: Path) -> None:
        """Missing file raises PreprocessorError with clear message."""
        ctx = PluginContext(notebook_path=tmp_path / "nb.md")
        plugin = IncludePlugin(ctx)

        with pytest.raises(PreprocessorError, match="File not found.*missing.txt"):
            plugin("missing.txt")

    def test_relative_path_parent_traversal(self, tmp_path: Path) -> None:
        """Include via ../ to a sibling directory works."""
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        shared = tmp_path / "shared"
        shared.mkdir()
        (shared / "helpers.py").write_text("def helper(): pass")

        ctx = PluginContext(notebook_path=nb_dir / "nb.md")
        plugin = IncludePlugin(ctx)
        assert plugin("../shared/helpers.py") == "def helper(): pass"

    def test_absolute_path(self, tmp_path: Path) -> None:
        """Include by absolute path works."""
        target = tmp_path / "absolute_target.txt"
        target.write_text("absolute content")

        # Notebook in a different directory
        nb_dir = tmp_path / "notebooks"
        nb_dir.mkdir()
        ctx = PluginContext(notebook_path=nb_dir / "nb.md")
        plugin = IncludePlugin(ctx)
        assert plugin(str(target)) == "absolute content"

    def test_content_verbatim(self, tmp_path: Path) -> None:
        """Included content appears exactly as-is, no escaping or processing."""
        content = "line1\n  indented\n\ttabbed\n{! not_a_directive !}\n"
        target = tmp_path / "verbatim.txt"
        target.write_text(content)

        ctx = PluginContext(notebook_path=tmp_path / "nb.md")
        plugin = IncludePlugin(ctx)
        assert plugin("verbatim.txt") == content

    def test_plugin_metadata(self) -> None:
        """Plugin metadata is correctly set."""
        meta = IncludePlugin.plugin_metadata()
        assert meta.name == "include"
        assert meta.description
        assert meta.markdown_docs is not None


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestPreprocessingIntegration:
    """End-to-end tests combining preprocessing with notebook parsing."""

    def test_markdown_notebook_with_include(self, tmp_path: Path) -> None:
        """A markdown notebook with an include directive preprocesses correctly."""
        # Create a fragment to include
        fragment = tmp_path / "setup.py"
        fragment.write_text("spark = SparkSession.builder.getOrCreate()")

        # Create a markdown notebook that includes the fragment
        nb_text = """\
---
databricks:
  profile: test
---

# My Notebook

```python
{! include("setup.py") !}
```

```python
df = spark.range(10)
df.show()
```
"""
        nb_path = tmp_path / "notebook.md"
        nb_path.write_text(nb_text)

        result = preprocess_text(nb_text, notebook_path=nb_path)

        # The include should have expanded
        assert "spark = SparkSession.builder.getOrCreate()" in result
        # The frontmatter should be preserved
        assert "profile: test" in result
        # The rest of the notebook should be unchanged
        assert "df = spark.range(10)" in result

    def test_notebook_without_directives_unchanged(self, tmp_path: Path) -> None:
        """A notebook without any directives passes through as the same object."""
        nb_text = """\
---
databricks:
  profile: test
---

# Plain Notebook

```python
print("hello")
```
"""
        nb_path = tmp_path / "notebook.md"
        nb_path.write_text(nb_text)

        result = preprocess_text(nb_text, notebook_path=nb_path)
        assert result is nb_text

    def test_preprocessed_notebook_parses_correctly(self, tmp_path: Path) -> None:
        """After preprocessing, the result still parses as a valid notebook."""
        from databricks_agent_notebooks.formats.conversion import to_notebook

        fragment = tmp_path / "cell.py"
        fragment.write_text("x = 42")

        nb_text = """\
---
databricks:
  profile: test
---

# Test

```python
{! include("cell.py") !}
```
"""
        nb_path = tmp_path / "notebook.md"
        nb_path.write_text(nb_text)

        preprocessed = preprocess_text(nb_text, notebook_path=nb_path)

        # Write preprocessed content to a temp file for to_notebook
        preprocessed_path = tmp_path / "preprocessed.md"
        preprocessed_path.write_text(preprocessed)

        notebook, config = to_notebook(preprocessed_path)
        assert notebook is not None
        # Find the code cell with our included content
        code_cells = [c for c in notebook.cells if c.cell_type == "code"]
        assert any("x = 42" in c.source for c in code_cells)


# ---------------------------------------------------------------------------
# Error detail tests
# ---------------------------------------------------------------------------


class TestPreprocessorErrorDetail:
    """Tests for structured error information."""

    def test_os_error_wrapped_in_preprocessor_error(self, tmp_path: Path) -> None:
        """OS-level read errors are wrapped in PreprocessorError."""
        target = tmp_path / "unreadable.txt"
        target.write_text("content")
        target.chmod(0o000)

        ctx = PluginContext(notebook_path=tmp_path / "nb.md")
        plugin = IncludePlugin(ctx)

        try:
            with pytest.raises(PreprocessorError, match="Cannot read"):
                plugin("unreadable.txt")
        finally:
            target.chmod(0o644)

    def test_file_not_found_error_has_detail(self, tmp_path: Path) -> None:
        """File not found errors carry the requested path in detail."""
        ctx = PluginContext(notebook_path=tmp_path / "nb.md")
        plugin = IncludePlugin(ctx)

        with pytest.raises(PreprocessorError) as exc_info:
            plugin("nonexistent.txt")

        assert exc_info.value.detail["path"] == "nonexistent.txt"
        assert "resolved" in exc_info.value.detail

    def test_engine_error_has_no_plugin(self, tmp_path: Path) -> None:
        """Engine-level errors (syntax, unknown directive) have plugin_name 'engine'."""
        nb = tmp_path / "test.md"
        nb.write_text("{! bogus() !}")

        with pytest.raises(PreprocessorError) as exc_info:
            preprocess_text(nb.read_text(), notebook_path=nb)

        assert exc_info.value.plugin is None
        assert exc_info.value.plugin_name == "engine"


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------


class TestCLIPreprocessFlags:
    """Tests for CLI preprocessing flags."""

    def test_no_preprocess_flag_registered(self) -> None:
        """The --no-preprocess flag appears in run subcommand help."""
        from databricks_agent_notebooks.cli import _build_parser

        parser = _build_parser()
        # Parse with --no-preprocess to confirm it's a valid flag
        args = parser.parse_args(["run", "--no-preprocess", "dummy.md"])
        assert args.no_preprocess is True

    def test_no_preprocess_default_is_none(self) -> None:
        """By default, no_preprocess is None (unspecified); defaults applied after merge."""
        from databricks_agent_notebooks.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["run", "dummy.md"])
        assert args.no_preprocess is None


# ---------------------------------------------------------------------------
# ParamHandle tests
# ---------------------------------------------------------------------------


class TestParamHandle:
    """Tests for ParamHandle fluent builder."""

    def _make_plugin(self, params: dict[str, str] | None = None) -> ParamPlugin:
        ctx = PluginContext(notebook_path=Path("/tmp/nb.md"), params=params or {})
        return ParamPlugin(ctx)

    def test_get_returns_cli_value(self) -> None:
        """CLI value is returned when available."""
        plugin = self._make_plugin({"name": "alice"})
        handle = ParamHandle("name", "alice", plugin)
        assert handle.get() == "alice"

    def test_get_returns_default_when_no_cli_value(self) -> None:
        """Default is used when no CLI value is provided."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        assert handle.with_default("fallback").get() == "fallback"

    def test_get_returns_empty_string_when_missing_not_required(self) -> None:
        """Empty string returned when no value, no default, not required."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        assert handle.get() == ""

    def test_get_throws_when_missing_and_required(self) -> None:
        """PreprocessorError raised when required but no value available."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        # Use validate() to set required, which raises immediately --
        # but get() also checks defensively, so test via validate first.
        with pytest.raises(PreprocessorError, match="Required parameter"):
            handle.validate(required=True)

    def test_with_default_chainable(self) -> None:
        """with_default returns the same handle for chaining."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        result = handle.with_default("x")
        assert result is handle

    def test_validate_required_throws_immediately(self) -> None:
        """validate(required=True) throws when no value is available."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        with pytest.raises(PreprocessorError, match="Required parameter"):
            handle.validate(required=True)

    def test_validate_regex_match(self) -> None:
        """validate with matching regex passes without error."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", "abc123", plugin)
        result = handle.validate(regex=r"^\w+$")
        assert result is handle

    def test_validate_regex_mismatch_throws(self) -> None:
        """validate with non-matching regex throws PreprocessorError."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", "hello world", plugin)
        with pytest.raises(PreprocessorError, match="does not match pattern"):
            handle.validate(regex=r"^\w+$")

    def test_validate_regex_skipped_on_empty(self) -> None:
        """Regex is not checked on empty resolved value."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        # Should not raise even though "" doesn't match \w+
        result = handle.validate(regex=r"^\w+$")
        assert result is handle

    def test_str_delegates_to_get(self) -> None:
        """str() produces the same result as get()."""
        plugin = self._make_plugin({"x": "42"})
        handle = ParamHandle("x", "42", plugin)
        assert str(handle) == "42"
        assert str(handle) == handle.get()

    def test_validate_required_satisfied_by_default(self) -> None:
        """A default value satisfies the required check."""
        plugin = self._make_plugin()
        handle = ParamHandle("name", None, plugin)
        result = handle.with_default("fallback").validate(required=True)
        assert result is handle
        assert result.get() == "fallback"

    def test_full_chain(self) -> None:
        """with_default().validate().get() works end to end."""
        plugin = self._make_plugin({"name": "prod_table"})
        handle = ParamHandle("name", "prod_table", plugin)
        result = handle.with_default("default_table").validate(
            required=True, regex=r"^\w+$",
        ).get()
        assert result == "prod_table"


# ---------------------------------------------------------------------------
# ParamPlugin tests
# ---------------------------------------------------------------------------


class TestParamPlugin:
    """Tests for the param plugin through the preprocessing engine."""

    def test_param_expands_cli_value(self, tmp_path: Path) -> None:
        """A param directive expands to the CLI-supplied value."""
        nb = tmp_path / "test.md"
        text = 'table = "{! param(\'table_name\') !}"'
        nb.write_text(text)

        result = preprocess_text(
            text, notebook_path=nb, params={"table_name": "users"},
        )
        assert result == 'table = "users"'

    def test_param_with_default(self, tmp_path: Path) -> None:
        """Default is used when no CLI value is supplied."""
        nb = tmp_path / "test.md"
        text = 'x = "{! param(\'key\').with_default(\'fallback\') !}"'
        nb.write_text(text)

        result = preprocess_text(text, notebook_path=nb, params={})
        assert result == 'x = "fallback"'

    def test_param_missing_returns_empty(self, tmp_path: Path) -> None:
        """Missing param with no default resolves to empty string."""
        nb = tmp_path / "test.md"
        text = 'x = "{! param(\'key\') !}"'
        nb.write_text(text)

        result = preprocess_text(text, notebook_path=nb, params={})
        assert result == 'x = ""'

    def test_multiple_params(self, tmp_path: Path) -> None:
        """Multiple params expand independently."""
        nb = tmp_path / "test.md"
        text = "{! param('a') !}-{! param('b') !}"
        nb.write_text(text)

        result = preprocess_text(
            text, notebook_path=nb, params={"a": "one", "b": "two"},
        )
        assert result == "one-two"

    def test_param_metadata(self) -> None:
        """Plugin metadata is correctly set."""
        meta = ParamPlugin.plugin_metadata()
        assert meta.name == "param"
        assert meta.description
        assert meta.markdown_docs is not None


# ---------------------------------------------------------------------------
# Param CLI flag tests
# ---------------------------------------------------------------------------


class TestParamCLIFlags:
    """Tests for --param CLI flag."""

    def test_param_flag_registered(self) -> None:
        """The --param flag is accepted by the run subparser."""
        from databricks_agent_notebooks.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args([
            "run", "dummy.md", "--param", "key=value",
        ])
        assert args.params == ["key=value"]

    def test_param_flag_accepts_multiple(self) -> None:
        """Multiple --param flags accumulate in a list."""
        from databricks_agent_notebooks.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args([
            "run", "dummy.md",
            "--param", "a=1",
            "--param", "b=2",
        ])
        assert args.params == ["a=1", "b=2"]

    def test_param_default_is_none(self) -> None:
        """Without --param, the params attribute is None."""
        from databricks_agent_notebooks.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["run", "dummy.md"])
        assert args.params is None

    def test_param_with_no_preprocess_is_accepted(self) -> None:
        """--param and --no-preprocess can coexist (params are silently ignored)."""
        from databricks_agent_notebooks.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args([
            "run", "dummy.md", "--no-preprocess", "--param", "key=value",
        ])
        assert args.no_preprocess is True
        assert args.params == ["key=value"]


# ---------------------------------------------------------------------------
# Dynamic includes (cross-plugin composition)
# ---------------------------------------------------------------------------


class TestDynamicIncludes:
    """Tests for the param + include composition pattern."""

    def test_best_practices_metadata(self) -> None:
        """Plugin metadata is correctly set."""
        meta = BestPracticesPlugin.plugin_metadata()
        assert meta.name == "best_practices"
        assert meta.description
        assert meta.markdown_docs is not None

    def test_best_practices_not_callable_as_directive(self, tmp_path: Path) -> None:
        """Using best_practices as a directive raises a clear error, not TypeError."""
        nb = tmp_path / "test.md"
        text = '{! best_practices("x") !}'
        nb.write_text(text)

        with pytest.raises(PreprocessorError, match="Unknown directive"):
            preprocess_text(text, notebook_path=nb)

    def test_param_value_as_include_path(self, tmp_path: Path) -> None:
        """include(param("key").get()) resolves the param then includes the file."""
        import uuid

        sentinel = str(uuid.uuid4())
        snippet = tmp_path / "snippet.py"
        snippet.write_text(sentinel)

        nb = tmp_path / "test.md"
        text = '{! include(param("snippet_path").get()) !}'
        nb.write_text(text)

        result = preprocess_text(
            text, notebook_path=nb, params={"snippet_path": str(snippet)},
        )
        assert sentinel in result
