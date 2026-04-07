"""Tests for built-in Jinja2 filters and globals."""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest

from databricks_agent_notebooks.preprocessing import preprocess_text
from databricks_agent_notebooks.preprocessing.errors import PreprocessorError


@pytest.fixture()
def notebook_path(tmp_path: Path) -> Path:
    """Dummy notebook path for preprocessing calls."""
    p = tmp_path / "test.md"
    p.write_text("")
    return p


class TestBuiltinsInstalled:
    """Verify builtins are available in the preprocessing environment."""

    def test_fromjson_filter_available(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! \'["a"]\' | fromjson | join(",") !}',
            notebook_path=notebook_path,
        )
        assert result == "a"


class TestFromjsonFilter:
    """Tests for the fromjson filter."""

    def test_parse_array(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! \'["a","b","c"]\' | fromjson | join(",") !}',
            notebook_path=notebook_path,
        )
        assert result == "a,b,c"

    def test_parse_object(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! (\'{"k":"v"}\' | fromjson)["k"] !}',
            notebook_path=notebook_path,
        )
        assert result == "v"

    def test_with_param(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! param("items").get() | fromjson | join(":") !}',
            notebook_path=notebook_path,
            params={"items": '["x","y"]'},
        )
        assert result == "x:y"

    def test_invalid_json_raises(self, notebook_path: Path) -> None:
        with pytest.raises(PreprocessorError, match="Invalid JSON"):
            preprocess_text(
                "{! 'not json' | fromjson !}",
                notebook_path=notebook_path,
            )


class TestEnvGlobal:
    """Tests for the env() global function."""

    def test_existing_var(self, notebook_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("_TEST_BUILTIN_VAR", "hello")
        result = preprocess_text(
            '{! env("_TEST_BUILTIN_VAR") !}',
            notebook_path=notebook_path,
        )
        assert result == "hello"

    def test_missing_var_returns_empty(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! env("_NONEXISTENT_VAR_12345") !}',
            notebook_path=notebook_path,
        )
        assert result == ""

    def test_missing_var_with_default(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! env("_NONEXISTENT_VAR_12345", "fallback") !}',
            notebook_path=notebook_path,
        )
        assert result == "fallback"

    def test_existing_var_ignores_default(
        self, notebook_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("_TEST_BUILTIN_VAR", "real")
        result = preprocess_text(
            '{! env("_TEST_BUILTIN_VAR", "fallback") !}',
            notebook_path=notebook_path,
        )
        assert result == "real"


class TestPathFilters:
    """Tests for basename and dirname filters."""

    def test_basename(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "/a/b/file.py" | basename !}',
            notebook_path=notebook_path,
        )
        assert result == "file.py"

    def test_dirname(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "/a/b/file.py" | dirname !}',
            notebook_path=notebook_path,
        )
        assert result == "/a/b"

    def test_basename_no_directory(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "file.py" | basename !}',
            notebook_path=notebook_path,
        )
        assert result == "file.py"

    def test_dirname_no_directory(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "file.py" | dirname !}',
            notebook_path=notebook_path,
        )
        assert result == ""

    def test_basename_in_loop_with_fromjson(self, notebook_path: Path) -> None:
        text = '{!% for f in \'["/a/x.py","/b/y.py"]\' | fromjson %!}{! f | basename !}\n{!% endfor %!}'
        result = preprocess_text(text, notebook_path=notebook_path)
        assert result == "x.py\ny.py\n"


class TestSplitFilter:
    """Tests for the split filter."""

    def test_split_comma(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "a,b,c" | split(",") | join(":") !}',
            notebook_path=notebook_path,
        )
        assert result == "a:b:c"

    def test_split_default_whitespace(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "a b  c" | split | join(",") !}',
            notebook_path=notebook_path,
        )
        assert result == "a,b,c"

    def test_split_with_param(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! param("tables").get() | split("|") | join(",") !}',
            notebook_path=notebook_path,
            params={"tables": "t1|t2|t3"},
        )
        assert result == "t1,t2,t3"


class TestRegexFilters:
    """Tests for regex_search and regex_replace filters."""

    def test_search_full_match(self, notebook_path: Path) -> None:
        result = preprocess_text(
            r'{! "spark-3.5.2" | regex_search("\d+\.\d+\.\d+") !}',
            notebook_path=notebook_path,
        )
        assert result == "3.5.2"

    def test_search_capture_group(self, notebook_path: Path) -> None:
        result = preprocess_text(
            r'{! "spark-3.5.2" | regex_search("spark-(\d+\.\d+)") !}',
            notebook_path=notebook_path,
        )
        assert result == "3.5"

    def test_search_no_match(self, notebook_path: Path) -> None:
        result = preprocess_text(
            r'{! "hello" | regex_search("\d+") !}',
            notebook_path=notebook_path,
        )
        assert result == ""

    def test_replace(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! "hello world" | regex_replace("world", "there") !}',
            notebook_path=notebook_path,
        )
        assert result == "hello there"

    def test_replace_with_pattern(self, notebook_path: Path) -> None:
        # Double backslash needed: Jinja2 string literals process escapes
        result = preprocess_text(
            r'{! "v3.5.2" | regex_replace("v(\d+)", "version-\\1") !}',
            notebook_path=notebook_path,
        )
        assert result == "version-3.5.2"

    def test_search_invalid_pattern_raises(self, notebook_path: Path) -> None:
        with pytest.raises(PreprocessorError, match="Invalid regex pattern"):
            preprocess_text(
                '{! "hello" | regex_search("[invalid") !}',
                notebook_path=notebook_path,
            )

    def test_replace_invalid_pattern_raises(self, notebook_path: Path) -> None:
        with pytest.raises(PreprocessorError, match="Invalid regex pattern"):
            preprocess_text(
                '{! "hello" | regex_replace("[invalid", "x") !}',
                notebook_path=notebook_path,
            )


class TestNowGlobal:
    """Tests for the now() global function."""

    def test_returns_datetime(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! now().strftime("%Y") !}',
            notebook_path=notebook_path,
        )
        assert result == str(datetime.datetime.now().year)

    def test_date_formatting(self, notebook_path: Path) -> None:
        result = preprocess_text(
            '{! now().strftime("%Y-%m-%d") !}',
            notebook_path=notebook_path,
        )
        # Just verify it's a valid date format
        datetime.datetime.strptime(result, "%Y-%m-%d")

    def test_usable_in_expression(self, notebook_path: Path) -> None:
        result = preprocess_text(
            'run_{! now().strftime("%Y%m%d") !}',
            notebook_path=notebook_path,
        )
        assert result.startswith("run_20")


class TestMultiFileDynamicInclude:
    """End-to-end test for the multi-file dynamic include pattern."""

    def test_json_param_loop_include(self, tmp_path: Path) -> None:
        # Create source files to include
        lib = tmp_path / "lib"
        lib.mkdir()
        (lib / "utils.py").write_text("def util(): pass\n")
        (lib / "helpers.py").write_text("def helper(): pass\n")

        # Create notebook
        notebook = tmp_path / "notebook.md"
        notebook.write_text("")

        text = (
            "# Setup\n"
            "```python\n"
            '{!% for f in param("sources").get() | fromjson %!}'
            "{! include(f) !}"
            "{!% endfor %!}"
            "```\n"
        )
        result = preprocess_text(
            text,
            notebook_path=notebook,
            params={"sources": '["lib/utils.py","lib/helpers.py"]'},
        )
        assert "def util(): pass" in result
        assert "def helper(): pass" in result

    def test_loop_with_basename(self, tmp_path: Path) -> None:
        notebook = tmp_path / "notebook.md"
        notebook.write_text("")

        text = (
            '{!% for f in param("files").get() | fromjson %!}'
            "# {! f | basename !}\n"
            "{!% endfor %!}"
        )
        result = preprocess_text(
            text,
            notebook_path=notebook,
            params={"files": '["/a/one.py","/b/two.py"]'},
        )
        assert result == "# one.py\n# two.py\n"
