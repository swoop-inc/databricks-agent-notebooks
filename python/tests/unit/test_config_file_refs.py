"""Tests for the -FILE key resolution utility."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from databricks_agent_notebooks.config.file_refs import (
    FileRefError,
    resolve_file_refs,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(path: Path, content: str | bytes) -> Path:
    """Write *content* to *path*, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# 1. Basic resolution
# ---------------------------------------------------------------------------


class TestBasicResolution:
    def test_single_string_resolved(self, tmp_path: Path) -> None:
        _write(tmp_path / "greeting.txt", "hello world")
        data = {"msg-FILE": "greeting.txt"}
        result = resolve_file_refs(data, tmp_path)
        assert result == {"msg": "hello world"}

    def test_list_of_paths_resolved(self, tmp_path: Path) -> None:
        _write(tmp_path / "a.txt", "aaa")
        _write(tmp_path / "b.txt", "bbb")
        data = {"cells-FILE": ["a.txt", "b.txt"]}
        result = resolve_file_refs(data, tmp_path)
        assert result == {"cells": ["aaa", "bbb"]}

    def test_file_key_removed(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "content")
        result = resolve_file_refs({"x-FILE": "f.txt"}, tmp_path)
        assert "x-FILE" not in result
        assert "x" in result

    def test_non_file_keys_preserved(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "content")
        data = {"x-FILE": "f.txt", "keep": 42, "also": "yes"}
        result = resolve_file_refs(data, tmp_path)
        assert result["keep"] == 42
        assert result["also"] == "yes"
        assert result["x"] == "content"


# ---------------------------------------------------------------------------
# 2. Path resolution
# ---------------------------------------------------------------------------


class TestPathResolution:
    def test_relative_path(self, tmp_path: Path) -> None:
        _write(tmp_path / "sub" / "f.txt", "relative")
        result = resolve_file_refs({"x-FILE": "sub/f.txt"}, tmp_path)
        assert result["x"] == "relative"

    def test_absolute_path(self, tmp_path: Path) -> None:
        target = _write(tmp_path / "abs.txt", "absolute")
        result = resolve_file_refs({"x-FILE": str(target)}, tmp_path / "other")
        assert result["x"] == "absolute"

    def test_dot_relative_path(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "dot-relative")
        result = resolve_file_refs({"x-FILE": "./f.txt"}, tmp_path)
        assert result["x"] == "dot-relative"

    def test_parent_relative_path(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "parent")
        base = tmp_path / "sub"
        base.mkdir()
        result = resolve_file_refs({"x-FILE": "../f.txt"}, base)
        assert result["x"] == "parent"


# ---------------------------------------------------------------------------
# 3. Nested dicts
# ---------------------------------------------------------------------------


class TestNestedDicts:
    def test_nested_file_key(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "nested")
        data: dict[str, Any] = {"outer": {"inner-FILE": "f.txt"}}
        result = resolve_file_refs(data, tmp_path)
        assert result == {"outer": {"inner": "nested"}}

    def test_deeply_nested(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "deep")
        data: dict[str, Any] = {"a": {"b": {"c": {"d-FILE": "f.txt"}}}}
        result = resolve_file_refs(data, tmp_path)
        assert result["a"]["b"]["c"]["d"] == "deep"

    def test_multiple_file_keys_at_different_levels(self, tmp_path: Path) -> None:
        _write(tmp_path / "top.txt", "top-content")
        _write(tmp_path / "nested.txt", "nested-content")
        data: dict[str, Any] = {
            "a-FILE": "top.txt",
            "sub": {"b-FILE": "nested.txt"},
        }
        result = resolve_file_refs(data, tmp_path)
        assert result["a"] == "top-content"
        assert result["sub"]["b"] == "nested-content"

    def test_nested_non_dict_values_unchanged(self, tmp_path: Path) -> None:
        data: dict[str, Any] = {"sub": {"keep": [1, 2, 3], "flag": True}}
        result = resolve_file_refs(data, tmp_path)
        assert result == {"sub": {"keep": [1, 2, 3], "flag": True}}


# ---------------------------------------------------------------------------
# 4. Errors
# ---------------------------------------------------------------------------


class TestErrors:
    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(FileRefError) as exc_info:
            resolve_file_refs({"x-FILE": "nope.txt"}, tmp_path)
        err = exc_info.value
        assert err.key == "x"
        assert err.file_path is not None
        assert "does not exist" in err.reason

    def test_binary_file(self, tmp_path: Path) -> None:
        _write(tmp_path / "bin.dat", b"\x00\x01\x02")
        with pytest.raises(FileRefError, match="binary"):
            resolve_file_refs({"x-FILE": "bin.dat"}, tmp_path)

    def test_non_utf8_file(self, tmp_path: Path) -> None:
        _write(tmp_path / "latin.txt", b"\xc0\xc1\xfe\xff")
        with pytest.raises(FileRefError, match="not valid UTF-8"):
            resolve_file_refs({"x-FILE": "latin.txt"}, tmp_path)

    def test_wrong_value_type_int(self, tmp_path: Path) -> None:
        with pytest.raises(FileRefError, match="got int"):
            resolve_file_refs({"x-FILE": 42}, tmp_path)

    def test_wrong_value_type_bool(self, tmp_path: Path) -> None:
        with pytest.raises(FileRefError, match="got bool"):
            resolve_file_refs({"x-FILE": True}, tmp_path)

    def test_wrong_value_type_dict(self, tmp_path: Path) -> None:
        with pytest.raises(FileRefError, match="got dict"):
            resolve_file_refs({"x-FILE": {"nested": "bad"}}, tmp_path)

    def test_list_with_non_string_element(self, tmp_path: Path) -> None:
        _write(tmp_path / "ok.txt", "fine")
        with pytest.raises(FileRefError, match="index 1.*got int"):
            resolve_file_refs({"x-FILE": ["ok.txt", 99]}, tmp_path)

    def test_ambiguous_keys(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "content")
        data = {"foo": "bar", "foo-FILE": "f.txt"}
        with pytest.raises(FileRefError, match="ambiguous.*foo.*foo-FILE"):
            resolve_file_refs(data, tmp_path)

    def test_ambiguous_keys_nested(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "content")
        data: dict[str, Any] = {
            "outer": {"bar": "val", "bar-FILE": "f.txt"},
        }
        with pytest.raises(FileRefError, match="ambiguous"):
            resolve_file_refs(data, tmp_path)

    def test_directory_path(self, tmp_path: Path) -> None:
        subdir = tmp_path / "adir"
        subdir.mkdir()
        with pytest.raises(FileRefError, match="directory"):
            resolve_file_refs({"x-FILE": "adir"}, tmp_path)

    def test_error_includes_dotted_key_path(self, tmp_path: Path) -> None:
        data: dict[str, Any] = {"a": {"b": {"c-FILE": "missing.txt"}}}
        with pytest.raises(FileRefError) as exc_info:
            resolve_file_refs(data, tmp_path)
        assert exc_info.value.key == "a.b.c"

    def test_permission_error_propagates_unwrapped(self, tmp_path: Path) -> None:
        """PermissionError from unreadable files propagates directly."""
        p = _write(tmp_path / "secret.txt", "content")
        p.chmod(0o000)
        try:
            with pytest.raises(PermissionError):
                resolve_file_refs({"x-FILE": "secret.txt"}, tmp_path)
        finally:
            p.chmod(0o644)

    def test_list_error_includes_element_index(self, tmp_path: Path) -> None:
        _write(tmp_path / "a.txt", "ok")
        data = {"cells-FILE": ["a.txt", "missing.txt"]}
        with pytest.raises(FileRefError) as exc_info:
            resolve_file_refs(data, tmp_path)
        assert "[1]" in exc_info.value.key


# ---------------------------------------------------------------------------
# 5. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_dict(self, tmp_path: Path) -> None:
        assert resolve_file_refs({}, tmp_path) == {}

    def test_no_file_keys(self, tmp_path: Path) -> None:
        data = {"a": 1, "b": "two", "c": [3]}
        result = resolve_file_refs(data, tmp_path)
        assert result == data

    def test_empty_list_stays_empty(self, tmp_path: Path) -> None:
        result = resolve_file_refs({"x-FILE": []}, tmp_path)
        assert result == {"x": []}

    def test_empty_file_produces_empty_string(self, tmp_path: Path) -> None:
        _write(tmp_path / "empty.txt", "")
        result = resolve_file_refs({"x-FILE": "empty.txt"}, tmp_path)
        assert result["x"] == ""

    def test_whitespace_only_preserved(self, tmp_path: Path) -> None:
        _write(tmp_path / "ws.txt", "   \n\t  \n")
        result = resolve_file_refs({"x-FILE": "ws.txt"}, tmp_path)
        assert result["x"] == "   \n\t  \n"

    def test_trailing_newline_not_stripped(self, tmp_path: Path) -> None:
        _write(tmp_path / "nl.txt", "hello\n")
        result = resolve_file_refs({"x-FILE": "nl.txt"}, tmp_path)
        assert result["x"] == "hello\n"

    def test_unicode_preserved(self, tmp_path: Path) -> None:
        content = "cafe\u0301 \u2603 \U0001f600"
        _write(tmp_path / "uni.txt", content)
        result = resolve_file_refs({"x-FILE": "uni.txt"}, tmp_path)
        assert result["x"] == content

    def test_input_not_mutated(self, tmp_path: Path) -> None:
        _write(tmp_path / "f.txt", "content")
        data: dict[str, Any] = {"x-FILE": "f.txt", "nested": {"y-FILE": "f.txt"}}
        original = copy.deepcopy(data)
        resolve_file_refs(data, tmp_path)
        assert data == original

    def test_lowercase_file_suffix_not_resolved(self, tmp_path: Path) -> None:
        data = {"x-file": "something.txt"}
        result = resolve_file_refs(data, tmp_path)
        assert result == {"x-file": "something.txt"}

    def test_no_dash_suffix_not_resolved(self, tmp_path: Path) -> None:
        data = {"xFILE": "something.txt"}
        result = resolve_file_refs(data, tmp_path)
        assert result == {"xFILE": "something.txt"}

    def test_bare_file_key_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileRefError, match="no base key name"):
            resolve_file_refs({"-FILE": "f.txt"}, tmp_path)

    def test_null_byte_beyond_8k_not_detected_as_binary(self, tmp_path: Path) -> None:
        """Null bytes past the 8192-byte detection window are not flagged."""
        content = b"a" * 8192 + b"\x00"
        _write(tmp_path / "sneaky.dat", content)
        result = resolve_file_refs({"x-FILE": "sneaky.dat"}, tmp_path)
        assert "\x00" in result["x"]


# ---------------------------------------------------------------------------
# 6. Integration-style
# ---------------------------------------------------------------------------


class TestIntegrationStyle:
    def test_realistic_toml_structure(self, tmp_path: Path) -> None:
        """Mimics a real ``[tool.agent-notebook]`` TOML section."""
        _write(tmp_path / "setup.py", "spark.conf.set('k', 'v')")
        _write(tmp_path / "cells" / "transform.py", "df = spark.sql('SELECT 1')")
        _write(tmp_path / "cells" / "validate.py", "assert df.count() > 0")

        data: dict[str, Any] = {
            "profile": "DEFAULT",
            "language": "python",
            "add_cell-FILE": "setup.py",
            "environments": {
                "staging": {
                    "cluster": "staging-cluster",
                    "add_cell-FILE": ["cells/transform.py", "cells/validate.py"],
                },
                "production": {
                    "cluster": "prod-cluster",
                    "timeout": 3600,
                },
            },
            "params": {
                "batch_date": "2026-01-01",
            },
        }

        result = resolve_file_refs(data, tmp_path)

        # Top-level -FILE resolved
        assert result["add_cell"] == "spark.conf.set('k', 'v')"
        assert "add_cell-FILE" not in result

        # Nested -FILE resolved
        staging = result["environments"]["staging"]
        assert staging["add_cell"] == [
            "df = spark.sql('SELECT 1')",
            "assert df.count() > 0",
        ]
        assert "add_cell-FILE" not in staging

        # Non-FILE keys preserved
        assert result["profile"] == "DEFAULT"
        assert result["language"] == "python"
        assert result["environments"]["production"]["cluster"] == "prod-cluster"
        assert result["environments"]["production"]["timeout"] == 3600
        assert result["params"]["batch_date"] == "2026-01-01"
