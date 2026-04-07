"""Tests for databricks_agent_notebooks.notebook_utils.

Pure Python tests -- no pyspark dependency.  Spark interactions are mocked.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks_agent_notebooks.notebook_utils import (
    is_databricks,
    resolve_repo_root,
    set_query_execution_timeout,
)


# ===========================================================================
# is_databricks
# ===========================================================================


def _make_spark(keys):
    """Create a mock SparkSession with given config keys.

    Mocks PySpark 3.x RuntimeConf.getAll() which returns a dict.
    """
    spark = MagicMock()
    spark.conf.getAll.return_value = dict.fromkeys(keys, "value")
    return spark


def _make_spark_property(keys):
    """Create a mock SparkSession where getAll is a property (dict), not a method.

    On Databricks, spark.conf.getAll is a dict property -- calling it
    with parens raises TypeError.  This simulates that environment.
    """
    spark = MagicMock()
    spark.conf.getAll = dict.fromkeys(keys, "value")
    return spark


class TestIsDatabricksExplicit:
    """Tests with an explicit spark argument."""

    def test_databricks_keys_returns_true(self):
        spark = _make_spark(["spark.databricks.clusterUsageTags.orgId", "spark.app.id"])
        assert is_databricks(spark) is True

    def test_no_databricks_keys_returns_false(self):
        spark = _make_spark(["spark.app.id", "spark.master"])
        assert is_databricks(spark) is False

    def test_empty_keys_returns_false(self):
        spark = _make_spark([])
        assert is_databricks(spark) is False

    def test_attribute_error_returns_false(self):
        spark = MagicMock()
        spark.conf.getAll.side_effect = AttributeError("no getAll")
        assert is_databricks(spark) is False

    def test_type_error_returns_false(self):
        spark = MagicMock()
        spark.conf.getAll.side_effect = TypeError("bad call")
        assert is_databricks(spark) is False

    def test_partial_match_not_fooled(self):
        """Keys like 'spark.notdatabricks.foo' should not match."""
        spark = _make_spark(["spark.notdatabricks.foo"])
        assert is_databricks(spark) is False

    def test_dot_bounded_match(self):
        """The match requires dots around 'databricks'."""
        spark = _make_spark(["spark.sql.databricks.connectorVersion"])
        assert is_databricks(spark) is True


class TestIsDatabricksPropertyStyle:
    """Tests where spark.conf.getAll is a property (dict), not a callable.

    On Databricks, getAll is a dict property.  Calling getAll() raises
    TypeError('dict object is not callable').  The function must handle
    both forms.
    """

    def test_property_databricks_keys_returns_true(self):
        spark = _make_spark_property(
            ["spark.databricks.clusterUsageTags.orgId", "spark.app.id"]
        )
        assert is_databricks(spark) is True

    def test_property_no_databricks_keys_returns_false(self):
        spark = _make_spark_property(["spark.app.id", "spark.master"])
        assert is_databricks(spark) is False

    def test_property_empty_keys_returns_false(self):
        spark = _make_spark_property([])
        assert is_databricks(spark) is False


class TestIsDatabricksFrameInspection:
    """Tests where spark=None and the function uses frame inspection.

    Frame inspection reads f_back.f_globals (module-level globals), not
    local variables.  We temporarily inject 'spark' into this module's
    globals so the function finds it.
    """

    def test_caller_global_with_databricks_keys(self):
        globals()["spark"] = _make_spark(["spark.databricks.workspaceId"])
        try:
            assert is_databricks() is True
        finally:
            del globals()["spark"]

    def test_caller_global_without_databricks_keys(self):
        globals()["spark"] = _make_spark(["spark.app.id"])
        try:
            assert is_databricks() is False
        finally:
            del globals()["spark"]


class TestIsDatabricksFallback:
    """Tests where spark=None and no caller global exists."""

    def test_active_session_with_databricks_keys(self):
        mock_session = _make_spark(["spark.databricks.workspaceId"])
        mock_module = MagicMock()
        mock_module.SparkSession.getActiveSession.return_value = mock_session
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": mock_module}):
            result = is_databricks()
        assert result is True

    def test_no_active_session_returns_false(self):
        mock_module = MagicMock()
        mock_module.SparkSession.getActiveSession.return_value = None
        with patch.dict("sys.modules", {"pyspark": MagicMock(), "pyspark.sql": mock_module}):
            result = is_databricks()
        assert result is False

    def test_no_pyspark_returns_false(self):
        """When pyspark is not installed and no caller global, returns False."""
        with patch.dict("sys.modules", {"pyspark": None, "pyspark.sql": None}):
            assert is_databricks() is False


# ===========================================================================
# set_query_execution_timeout
# ===========================================================================


class TestSetQueryExecutionTimeout:
    """Tests for set_query_execution_timeout with explicit spark argument."""

    def test_sets_timeout_on_databricks(self):
        spark = _make_spark(["spark.databricks.clusterUsageTags.orgId"])
        set_query_execution_timeout(86400, spark=spark)
        spark.conf.set.assert_called_once_with(
            "spark.databricks.execution.timeout", "86400"
        )

    def test_noop_outside_databricks(self):
        spark = _make_spark(["spark.app.id", "spark.master"])
        set_query_execution_timeout(86400, spark=spark)
        spark.conf.set.assert_not_called()

    def test_custom_seconds(self):
        spark = _make_spark(["spark.databricks.workspaceId"])
        set_query_execution_timeout(3600, spark=spark)
        spark.conf.set.assert_called_once_with(
            "spark.databricks.execution.timeout", "3600"
        )

    def test_default_is_9000(self):
        spark = _make_spark(["spark.databricks.workspaceId"])
        set_query_execution_timeout(spark=spark)
        spark.conf.set.assert_called_once_with(
            "spark.databricks.execution.timeout", "9000"
        )


class TestSetQueryExecutionTimeoutFrameInspection:
    """Tests where spark=None and frame inspection resolves the session."""

    def test_sets_timeout_via_caller_global(self):
        globals()["spark"] = _make_spark(["spark.databricks.workspaceId"])
        try:
            set_query_execution_timeout(86400)
            globals()["spark"].conf.set.assert_called_once_with(
                "spark.databricks.execution.timeout", "86400"
            )
        finally:
            del globals()["spark"]

    def test_noop_via_caller_global_non_databricks(self):
        globals()["spark"] = _make_spark(["spark.app.id"])
        try:
            set_query_execution_timeout(86400)
            globals()["spark"].conf.set.assert_not_called()
        finally:
            del globals()["spark"]


# ===========================================================================
# resolve_repo_root
# ===========================================================================


class TestResolveRepoRootEnvVar:
    """Tests for the REPO_ROOT environment variable path."""

    def test_env_var_set(self, monkeypatch):
        monkeypatch.setenv("REPO_ROOT", "/custom/repo")
        assert resolve_repo_root() == "/custom/repo"

    def test_env_var_empty_string_falls_through(self, monkeypatch, tmp_path):
        monkeypatch.setenv("REPO_ROOT", "")
        monkeypatch.chdir(tmp_path)
        # No .git anywhere, so falls back to cwd
        assert resolve_repo_root() == str(tmp_path)


class TestResolveRepoRootGitWalk:
    """Tests for the .git directory walk."""

    def test_git_directory_in_cwd(self, monkeypatch, tmp_path):
        monkeypatch.delenv("REPO_ROOT", raising=False)
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)
        assert resolve_repo_root() == str(tmp_path)

    def test_git_directory_in_ancestor(self, monkeypatch, tmp_path):
        monkeypatch.delenv("REPO_ROOT", raising=False)
        (tmp_path / ".git").mkdir()
        nested = tmp_path / "a" / "b" / "c"
        nested.mkdir(parents=True)
        monkeypatch.chdir(nested)
        assert resolve_repo_root() == str(tmp_path)

    def test_git_file_in_ancestor(self, monkeypatch, tmp_path):
        """Worktrees use a .git file (not directory) pointing to the main repo."""
        monkeypatch.delenv("REPO_ROOT", raising=False)
        (tmp_path / ".git").write_text("gitdir: /somewhere/else/.git/worktrees/dev")
        nested = tmp_path / "src"
        nested.mkdir()
        monkeypatch.chdir(nested)
        assert resolve_repo_root() == str(tmp_path)

    def test_deeply_nested(self, monkeypatch, tmp_path):
        monkeypatch.delenv("REPO_ROOT", raising=False)
        (tmp_path / ".git").mkdir()
        deep = tmp_path / "a" / "b" / "c" / "d" / "e"
        deep.mkdir(parents=True)
        monkeypatch.chdir(deep)
        assert resolve_repo_root() == str(tmp_path)

    def test_no_git_anywhere_falls_back_to_cwd(self, monkeypatch, tmp_path):
        monkeypatch.delenv("REPO_ROOT", raising=False)
        monkeypatch.chdir(tmp_path)
        assert resolve_repo_root() == str(tmp_path)
