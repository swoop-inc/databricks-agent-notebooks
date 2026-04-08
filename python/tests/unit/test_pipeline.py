"""Tests for databricks_agent_notebooks.pipeline.

Pure Python tests -- no pyspark dependency.  Spark interactions are mocked.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from databricks_agent_notebooks.pipeline import (
    Context,
    MissingDataFrameError,
    MissingTableError,
    OptionalDataFrame,
    StepRunner,
    _ensure_schema_exists,
    _is_spark_connect,
    _materialize_and_read,
    _parse_flexible_set,
    _parse_step_config,
    _table_exists,
    _validate_table_name,
    read_or_compute_table_step,
    cluster_cores,
    core_based_parallelism,
    get_context,
    read_or_compute_table,
    set_context,
    spark_conf,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_context():
    """Reset the context var before and after each test."""
    from databricks_agent_notebooks.pipeline import _current_context

    token = _current_context.set(None)
    yield
    _current_context.reset(token)


@pytest.fixture
def mock_spark():
    """A mock SparkSession with table() and sql() methods."""
    spark = MagicMock()
    # Mock module path for classic Spark detection
    spark.__class__.__module__ = "pyspark.sql.session"
    # catalog.tableExists for Spark Connect path
    spark.catalog.tableExists.return_value = True
    # spark.table returns a mock DataFrame by default
    mock_df = MagicMock()
    mock_df.count.return_value = 42
    spark.table.return_value = mock_df
    return spark


@pytest.fixture
def ctx_with_spark(mock_spark):
    """A Context pre-loaded with a mock spark session."""
    ctx = Context(defaults={"table_prefix": "default.demo"})
    ctx["spark"] = mock_spark
    return ctx


# ===========================================================================
# Context tests
# ===========================================================================


class TestContextInit:
    def test_empty_context(self):
        ctx = Context()
        assert ctx.get("table_prefix") is None

    def test_defaults(self):
        ctx = Context(defaults={"table_prefix": "default.demo", "steps": {}})
        assert ctx["table_prefix"] == "default.demo"
        assert ctx["steps"] == {}

    def test_initial_json_string(self):
        initial = json.dumps({"table_prefix": "cat.schema", "custom": 123})
        ctx = Context(initial, defaults={"table_prefix": "default.demo"})
        assert ctx["table_prefix"] == "cat.schema"
        assert ctx["custom"] == 123

    def test_initial_dict(self):
        ctx = Context({"table_prefix": "from_dict"})
        assert ctx["table_prefix"] == "from_dict"

    def test_initial_overrides_defaults(self):
        ctx = Context(
            {"table_prefix": "override"},
            defaults={"table_prefix": "default_value", "extra": True},
        )
        assert ctx["table_prefix"] == "override"
        assert ctx["extra"] is True

    def test_initial_empty_string(self):
        ctx = Context("", defaults={"table_prefix": "stays"})
        assert ctx["table_prefix"] == "stays"

    def test_initial_none(self):
        ctx = Context(None, defaults={"table_prefix": "stays"})
        assert ctx["table_prefix"] == "stays"


class TestContextOverlay:
    def test_overlay_json_value(self):
        ctx = Context(defaults={"steps": {}})
        ctx.overlay_params({"steps": '["ingest","clean"]'})
        assert ctx["steps"] == ["ingest", "clean"]

    def test_overlay_plain_string(self):
        ctx = Context()
        ctx.overlay_params({"table_prefix": "my.prefix"})
        assert ctx["table_prefix"] == "my.prefix"

    def test_overlay_skips_empty(self):
        ctx = Context(defaults={"steps": {}})
        ctx.overlay_params({"steps": ""})
        assert ctx["steps"] == {}

    def test_overlay_skips_none(self):
        ctx = Context(defaults={"steps": {}})
        ctx.overlay_params({"steps": None})
        assert ctx["steps"] == {}

    def test_overlay_json_object(self):
        ctx = Context()
        ctx.overlay_params({"steps": '{"ingest": true, "clean": false}'})
        assert ctx["steps"] == {"ingest": True, "clean": False}


class TestContextDictAccess:
    def test_getitem_setitem(self):
        ctx = Context()
        ctx["key"] = "value"
        assert ctx["key"] == "value"

    def test_contains(self):
        ctx = Context(defaults={"a": 1})
        assert "a" in ctx
        assert "b" not in ctx

    def test_get_with_default(self):
        ctx = Context()
        assert ctx.get("missing", "fallback") == "fallback"


class TestContextRunner:
    def test_should_run_without_runner_raises(self):
        ctx = Context()
        with pytest.raises(RuntimeError, match="No StepRunner configured"):
            ctx.should_run("ingest")

    def test_run_without_runner_raises(self):
        ctx = Context()
        with pytest.raises(RuntimeError, match="No StepRunner configured"):
            ctx.run("ingest", table="default.demo_ingest", compute=lambda: None)

    def test_should_run_delegates_to_runner(self):
        ctx = Context(defaults={"steps": '["ingest"]'})
        ctx.overlay_params({"steps": '["ingest"]'})
        runner = StepRunner(steps=["ingest", "clean"])
        ctx.set_runner(runner)
        assert ctx.should_run("ingest") is True

    def test_run_enabled_step(self, mock_spark):
        ctx = Context()
        ctx["spark"] = mock_spark
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)
        set_context(ctx)

        result = ctx.run("ingest", table="default.demo_ingest",
                         compute=lambda: mock_spark.range(10))
        # Should have called read_or_compute_table internally
        assert result is not None

    def test_run_disabled_step_with_cache(self, mock_spark):
        ctx = Context()
        ctx["spark"] = mock_spark
        ctx.overlay_params({"steps": '{"ingest": false}'})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)

        result = ctx.run("ingest", table="default.demo_ingest", compute=lambda: None)
        mock_spark.table.assert_called_with("default.demo_ingest")
        assert result is not None

    def test_run_disabled_step_no_cache_raises(self, mock_spark):
        mock_spark.table.side_effect = type("AnalysisException", (Exception,), {})("TABLE_OR_VIEW_NOT_FOUND")
        ctx = Context()
        ctx["spark"] = mock_spark
        ctx.overlay_params({"steps": '{"ingest": false}'})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)

        with pytest.raises(RuntimeError, match="disabled but table.*does not exist"):
            ctx.run("ingest", table="default.demo_ingest", compute=lambda: None)

    def test_run_disabled_step_non_table_error_propagates(self, mock_spark):
        """Non-table-not-found errors propagate, not wrapped as 'does not exist'."""
        mock_spark.table.side_effect = ConnectionError("network failure")
        ctx = Context()
        ctx["spark"] = mock_spark
        ctx.overlay_params({"steps": '{"ingest": false}'})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)

        with pytest.raises(ConnectionError, match="network failure"):
            ctx.run("ingest", table="default.demo_ingest", compute=lambda: None)

    def test_run_with_recompute(self, mock_spark):
        ctx = Context()
        ctx["spark"] = mock_spark
        ctx["recompute"] = ["ingest"]
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)
        set_context(ctx)

        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back

        result = ctx.run("ingest", table="default.demo_ingest",
                         compute=lambda: compute_df)
        # Should trigger compute even though cache exists
        compute_df.write.mode.return_value.saveAsTable.assert_called_once()


class TestContextRecompute:
    def test_should_recompute_empty(self):
        ctx = Context()
        assert ctx.should_recompute("ingest") is False

    def test_should_recompute_list(self):
        ctx = Context()
        ctx["recompute"] = ["ingest", "clean"]
        assert ctx.should_recompute("ingest") is True
        assert ctx.should_recompute("enrich") is False

    def test_should_recompute_csv_string(self):
        ctx = Context()
        ctx["recompute"] = "ingest,clean"
        assert ctx.should_recompute("ingest") is True
        assert ctx.should_recompute("publish") is False

    def test_should_recompute_json_array_string(self):
        ctx = Context()
        ctx["recompute"] = '["clean"]'
        assert ctx.should_recompute("clean") is True
        assert ctx.should_recompute("ingest") is False


class TestContextPrintConfig:
    def test_print_config_runs(self, capsys):
        ctx = Context(defaults={"table_prefix": "default.demo"})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)
        ctx.print_config()
        captured = capsys.readouterr()
        assert "Pipeline configuration:" in captured.out
        assert "table_prefix" in captured.out
        assert "runner steps" in captured.out


# ===========================================================================
# StepRunner tests
# ===========================================================================


class TestStepRunnerParsing:
    def test_csv_input(self):
        runner = StepRunner(steps=["ingest", "clean", "enrich"])
        assert runner.should_run("ingest", "ingest,clean") is True
        assert runner.should_run("clean", "ingest,clean") is True
        assert runner.should_run("enrich", "ingest,clean") is True  # default=True

    def test_json_array_input(self):
        runner = StepRunner(steps=["ingest", "clean"])
        assert runner.should_run("ingest", '["ingest"]') is True
        assert runner.should_run("clean", '["ingest"]') is True  # default=True

    def test_json_object_input(self):
        runner = StepRunner(steps=["ingest", "clean"])
        assert runner.should_run("ingest", '{"ingest": true, "clean": false}') is True
        assert runner.should_run("clean", '{"ingest": true, "clean": false}') is False

    def test_all_keyword(self):
        runner = StepRunner(steps=["ingest", "clean", "enrich"])
        assert runner.should_run("ingest", "ALL") is True
        assert runner.should_run("clean", "ALL") is True
        assert runner.should_run("enrich", "ALL") is True

    def test_empty_string_uses_defaults(self):
        runner = StepRunner(
            steps=["ingest", "clean"], defaults={"ingest": True, "clean": False}
        )
        assert runner.should_run("ingest", "") is True
        assert runner.should_run("clean", "") is False

    def test_none_uses_defaults(self):
        runner = StepRunner(
            steps=["ingest", "clean"], defaults={"ingest": False, "clean": True}
        )
        assert runner.should_run("ingest", None) is False
        assert runner.should_run("clean", None) is True

    def test_unknown_step_raises(self):
        runner = StepRunner(steps=["ingest"])
        with pytest.raises(ValueError, match="Unknown step 'bogus'"):
            runner.should_run("bogus")

    def test_list_input(self):
        runner = StepRunner(steps=["ingest", "clean"])
        assert runner.should_run("ingest", ["ingest"]) is True

    def test_dict_input(self):
        runner = StepRunner(steps=["ingest", "clean"])
        assert runner.should_run("ingest", {"ingest": True, "clean": False}) is True
        assert runner.should_run("clean", {"ingest": True, "clean": False}) is False


class TestStepRunnerDefaults:
    def test_default_all_true(self):
        runner = StepRunner(steps=["a", "b"])
        assert runner.defaults == {"a": True, "b": True}

    def test_custom_defaults(self):
        runner = StepRunner(steps=["a", "b"], defaults={"a": True, "b": False})
        assert runner.should_run("a") is True
        assert runner.should_run("b") is False


# ===========================================================================
# read_or_compute_table tests
# ===========================================================================


class TestReadOrComputeTable:
    def test_cache_hit(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        result = read_or_compute_table(
            read="default.demo_ingest",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        mock_spark.table.assert_called_with("default.demo_ingest")
        assert result is mock_spark.table.return_value

    def test_cache_miss_triggers_compute(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.table.side_effect = [
            AnalysisException("TABLE_OR_VIEW_NOT_FOUND"),  # cache miss
            MagicMock(),  # read-back after write
        ]
        compute_df = MagicMock()  # DataFrame with .write
        compute_df.write = MagicMock()

        result = read_or_compute_table(
            read="default.demo_ingest", compute=lambda: compute_df
        )
        compute_df.write.mode.assert_called_once_with("overwrite")
        compute_df.write.mode.return_value.saveAsTable.assert_called_once_with("default.demo_ingest")

    def test_refresh_skips_cache(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        compute_df = MagicMock()
        compute_df.write = MagicMock()
        # Set up table call for read-back after write
        read_back_df = MagicMock()
        mock_spark.table.return_value = read_back_df

        result = read_or_compute_table(
            read="default.demo_ingest", compute=lambda: compute_df, refresh=True
        )
        # table() should only be called once for read-back, not for cache check
        compute_df.write.mode.return_value.saveAsTable.assert_called_once_with("default.demo_ingest")

    def test_compute_error_propagates(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.table.side_effect = AnalysisException("TABLE_OR_VIEW_NOT_FOUND")

        def bad_compute():
            raise ValueError("compute failed")

        with pytest.raises(ValueError, match="compute failed"):
            read_or_compute_table(
                read="default.demo_ingest",
                compute=bad_compute,
            )

    def test_string_read_bare_name(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        read_or_compute_table(
            read="ingest",
            compute=lambda: pytest.fail("should not compute"),
        )
        # Bare names are used as-is -- no auto-prefixing
        mock_spark.table.assert_called_with("ingest")

    def test_string_read_qualified_name(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        read_or_compute_table(
            read="catalog.schema.my_table",
            compute=lambda: pytest.fail("should not compute"),
        )
        mock_spark.table.assert_called_with("catalog.schema.my_table")

    def test_callable_read_no_args(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        read_or_compute_table(
            read=lambda: "my.custom.table",
            compute=lambda: pytest.fail("should not compute"),
        )
        mock_spark.table.assert_called_with("my.custom.table")

    def test_callable_read_with_context(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        read_or_compute_table(
            read=lambda c: f"{c['table_prefix']}_lookup",
            compute=lambda: pytest.fail("should not compute"),
        )
        mock_spark.table.assert_called_with("default.demo_lookup")

    def test_id_recompute_matching(self, ctx_with_spark, mock_spark):
        ctx_with_spark["recompute"] = ["my_value"]
        set_context(ctx_with_spark)

        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back

        result = read_or_compute_table(
            read="default.demo_ingest",
            compute=lambda: compute_df,
            id="my_value",
        )
        # Should have triggered refresh via recompute
        compute_df.write.mode.return_value.saveAsTable.assert_called_once()

    def test_dataframe_writer_result(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        # A DataFrameWriter has saveAsTable but no collect
        writer = MagicMock(spec=["saveAsTable", "mode", "format"])
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.table.side_effect = [
            AnalysisException("TABLE_OR_VIEW_NOT_FOUND"),  # cache miss
            MagicMock(),  # read-back
        ]
        read_or_compute_table(
            read="default.demo_output", compute=lambda: writer
        )
        writer.saveAsTable.assert_called_once_with("default.demo_output")

    def test_context_auto_resolution(self, ctx_with_spark, mock_spark):
        set_context(ctx_with_spark)
        # Call without explicit context -- should auto-resolve
        result = read_or_compute_table(
            read="ingest",
            compute=lambda: pytest.fail("should not compute"),
        )
        assert result is not None

    def test_explicit_context_overrides_auto(self, mock_spark):
        auto_ctx = Context(defaults={"table_prefix": "auto"})
        auto_ctx["spark"] = mock_spark
        set_context(auto_ctx)

        explicit_ctx = Context(defaults={"table_prefix": "explicit"})
        explicit_ctx["spark"] = mock_spark

        read_or_compute_table(
            read="my.explicit.table",
            compute=lambda: pytest.fail("should not compute"),
            context=explicit_ctx,
        )
        # Explicit context used for spark session; table name as-is
        mock_spark.table.assert_called_with("my.explicit.table")

    def test_cache_check_non_table_error_propagates(self, ctx_with_spark, mock_spark):
        """Non-table-not-found errors during cache check propagate, don't trigger compute."""
        set_context(ctx_with_spark)
        mock_spark.table.side_effect = ConnectionError("network failure")

        with pytest.raises(ConnectionError, match="network failure"):
            read_or_compute_table(
                read="default.demo_ingest",
                compute=lambda: pytest.fail("compute should not be called"),
            )

    def test_callable_read_without_context_raises(self):
        # No context set, callable requires one arg -- should get clear error
        with pytest.raises(RuntimeError, match="requires context"):
            read_or_compute_table(
                read=lambda c: f"{c['prefix']}_table",
                compute=lambda: None,
            )


# ===========================================================================
# Context auto-resolution tests
# ===========================================================================


class TestContextVar:
    def test_get_set_context(self):
        assert get_context() is None
        ctx = Context()
        set_context(ctx)
        assert get_context() is ctx


# ===========================================================================
# Parsing helper tests
# ===========================================================================


class TestParseFlexibleSet:
    def test_list(self):
        assert _parse_flexible_set(["a", "b"]) == {"a", "b"}

    def test_set(self):
        assert _parse_flexible_set({"x"}) == {"x"}

    def test_csv_string(self):
        assert _parse_flexible_set("a, b, c") == {"a", "b", "c"}

    def test_json_array_string(self):
        assert _parse_flexible_set('["x","y"]') == {"x", "y"}

    def test_empty_string(self):
        assert _parse_flexible_set("") == set()

    def test_single_name(self):
        assert _parse_flexible_set("ingest") == {"ingest"}


class TestParseStepConfig:
    def test_all_keyword(self):
        known = {"a", "b", "c"}
        result = _parse_step_config("ALL", known)
        assert result == {"a": True, "b": True, "c": True}

    def test_json_object(self):
        result = _parse_step_config('{"a": true, "b": false}', set())
        assert result == {"a": True, "b": False}

    def test_json_array(self):
        result = _parse_step_config('["a", "b"]', set())
        assert result == {"a": True, "b": True}

    def test_csv(self):
        result = _parse_step_config("a,b", set())
        assert result == {"a": True, "b": True}

    def test_empty_string(self):
        assert _parse_step_config("", set()) == {}

    def test_dict_input(self):
        result = _parse_step_config({"a": True, "b": 0}, set())
        assert result == {"a": True, "b": False}

    def test_list_input(self):
        result = _parse_step_config(["x", "y"], set())
        assert result == {"x": True, "y": True}


# ===========================================================================
# Table name validation tests
# ===========================================================================


class TestValidateTableName:
    def test_valid_simple(self):
        _validate_table_name("my_table")

    def test_valid_two_part(self):
        _validate_table_name("default.my_table")

    def test_valid_three_part(self):
        _validate_table_name("catalog.schema.table")

    def test_invalid_sql_injection(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            _validate_table_name("table; DROP TABLE users--")

    def test_invalid_spaces(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            _validate_table_name("table name")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid table name"):
            _validate_table_name("")


# ===========================================================================
# Spark Connect dual-path tests
# ===========================================================================


class TestSparkConnectDualPath:
    """Tests for _is_spark_connect and dual-path cache detection."""

    def test_is_spark_connect_classic(self):
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.session"
        assert _is_spark_connect(spark) is False

    def test_is_spark_connect_connect(self):
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.connect.session"
        assert _is_spark_connect(spark) is True

    def test_is_spark_connect_non_pyspark_connect(self):
        """Module containing 'connect' but not pyspark.sql.connect is not Connect."""
        spark = MagicMock()
        spark.__class__.__module__ = "some.other.connect.session"
        assert _is_spark_connect(spark) is False

    def test_cache_hit_spark_connect(self, mock_spark):
        """Spark Connect path: tableExists=True -> returns cached df."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        mock_spark.catalog.tableExists.return_value = True
        ctx = Context()
        ctx["spark"] = mock_spark
        set_context(ctx)

        result = read_or_compute_table(
            read="default.test_table",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        mock_spark.catalog.tableExists.assert_called_with("default.test_table")
        mock_spark.table.assert_called_with("default.test_table")

    def test_cache_miss_spark_connect(self, mock_spark):
        """Spark Connect path: tableExists=False -> triggers compute."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        mock_spark.catalog.tableExists.return_value = False
        ctx = Context()
        ctx["spark"] = mock_spark
        set_context(ctx)

        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back

        result = read_or_compute_table(
            read="default.test_table",
            compute=lambda: compute_df,
        )
        compute_df.write.mode.return_value.saveAsTable.assert_called_once_with("default.test_table")

    def test_disabled_step_cache_hit_spark_connect(self, mock_spark):
        """Spark Connect: disabled step with existing table serves from cache."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        mock_spark.catalog.tableExists.return_value = True
        cached_df = MagicMock()
        mock_spark.table.return_value = cached_df

        ctx = Context()
        ctx["spark"] = mock_spark
        ctx.overlay_params({"steps": '{"ingest": false}'})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)

        result = ctx.run("ingest", table="default.test_table", compute=lambda: None)
        assert result is cached_df

    def test_disabled_step_cache_miss_spark_connect(self, mock_spark):
        """Spark Connect: disabled step without table raises RuntimeError."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        mock_spark.catalog.tableExists.return_value = False

        ctx = Context()
        ctx["spark"] = mock_spark
        ctx.overlay_params({"steps": '{"ingest": false}'})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)

        with pytest.raises(RuntimeError, match="disabled but table.*does not exist"):
            ctx.run("ingest", table="default.test_table", compute=lambda: None)

    def test_cache_hit_classic_spark(self, mock_spark):
        """Classic Spark path: spark.table() succeeds -> cache hit."""
        # mock_spark default module is not "connect", so classic path
        ctx = Context()
        ctx["spark"] = mock_spark
        set_context(ctx)

        result = read_or_compute_table(
            read="default.test_table",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        mock_spark.table.assert_called_with("default.test_table")
        # catalog.tableExists should NOT be called for classic Spark
        mock_spark.catalog.tableExists.assert_not_called()

    def test_cache_miss_classic_spark(self, mock_spark):
        """Classic Spark path: spark.table() raises -> cache miss, compute."""
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.table.side_effect = [
            AnalysisException("TABLE_OR_VIEW_NOT_FOUND"),
            MagicMock(),  # read-back
        ]
        ctx = Context()
        ctx["spark"] = mock_spark
        set_context(ctx)

        compute_df = MagicMock()
        compute_df.write = MagicMock()

        result = read_or_compute_table(
            read="default.test_table",
            compute=lambda: compute_df,
        )
        compute_df.write.mode.return_value.saveAsTable.assert_called_once_with("default.test_table")
        # catalog.tableExists should NOT be called for classic Spark
        mock_spark.catalog.tableExists.assert_not_called()


# ===========================================================================
# Error type tests
# ===========================================================================


class TestMissingDataFrameError:
    def test_default_message_with_step(self):
        err = MissingDataFrameError(step="enrich")
        assert "step 'enrich'" in str(err)
        assert err.step == "enrich"

    def test_default_message_with_step_and_id(self):
        err = MissingDataFrameError(step="enrich", id="enrich_v2")
        assert "step 'enrich'" in str(err)
        assert "id 'enrich_v2'" in str(err)

    def test_id_same_as_step_not_duplicated(self):
        err = MissingDataFrameError(step="enrich", id="enrich")
        msg = str(err)
        assert "step 'enrich'" in msg
        assert "id 'enrich'" not in msg  # id omitted when same as step

    def test_custom_message(self):
        err = MissingDataFrameError(step="x", message="custom error text")
        assert str(err) == "custom error text"

    def test_no_metadata(self):
        err = MissingDataFrameError()
        assert "unknown source" in str(err)


class TestMissingTableError:
    def test_message_with_all_fields(self):
        err = MissingTableError(step="enrich", id="enrich", table_name="db.enriched")
        msg = str(err)
        assert "table 'db.enriched'" in msg
        assert "step 'enrich'" in msg
        assert err.table_name == "db.enriched"

    def test_is_missing_dataframe_error(self):
        err = MissingTableError(step="x")
        assert isinstance(err, MissingDataFrameError)

    def test_no_step(self):
        err = MissingTableError(table_name="db.t")
        msg = str(err)
        assert "table 'db.t'" in msg


# ===========================================================================
# OptionalDataFrame tests
# ===========================================================================


class TestOptionalDataFrame:
    def test_get_with_dataframe(self):
        df = MagicMock()
        opt = OptionalDataFrame(step="x", _df=df)
        assert opt.get() is df

    def test_get_without_dataframe_raises(self):
        opt = OptionalDataFrame(step="enrich", id="enrich")
        with pytest.raises(MissingDataFrameError, match="step 'enrich'"):
            opt.get()

    def test_get_with_deferred_error(self):
        err = MissingTableError(step="enrich", table_name="db.enriched")
        opt = OptionalDataFrame(step="enrich", _error=err)
        with pytest.raises(MissingTableError, match="Table not found"):
            opt.get()

    def test_get_or_else_with_dataframe(self):
        df = MagicMock()
        opt = OptionalDataFrame(_df=df)
        assert opt.get_or_else("fallback") is df

    def test_get_or_else_with_value_default(self):
        opt = OptionalDataFrame()
        assert opt.get_or_else("fallback") == "fallback"

    def test_get_or_else_with_callable_default(self):
        opt = OptionalDataFrame()
        result = opt.get_or_else(lambda: "computed")
        assert result == "computed"

    def test_get_or_else_callable_not_invoked_when_present(self):
        df = MagicMock()
        opt = OptionalDataFrame(_df=df)
        result = opt.get_or_else(lambda: pytest.fail("should not be called"))
        assert result is df

    def test_bool_true_when_present(self):
        opt = OptionalDataFrame(_df=MagicMock())
        assert bool(opt) is True

    def test_bool_false_when_absent(self):
        opt = OptionalDataFrame()
        assert bool(opt) is False

    def test_dataframe_property(self):
        df = MagicMock()
        opt = OptionalDataFrame(_df=df)
        assert opt.dataframe is df

    def test_dataframe_property_none(self):
        opt = OptionalDataFrame()
        assert opt.dataframe is None

    def test_frozen(self):
        opt = OptionalDataFrame(step="x")
        with pytest.raises(AttributeError):
            opt.step = "y"


# ===========================================================================
# read_or_compute_table_step tests
# ===========================================================================


class TestCachedOrComputedTableStep:
    """Tests for read_or_compute_table_step."""

    def _make_ctx(self, mock_spark, steps_config=None, recompute=None):
        """Create a Context with runner and optional step/recompute config."""
        ctx = Context()
        ctx["spark"] = mock_spark
        runner = StepRunner(steps=["ingest", "enrich", "publish"])
        ctx.set_runner(runner)
        if steps_config is not None:
            ctx.overlay_params({"steps": steps_config})
        if recompute is not None:
            ctx["recompute"] = recompute
        set_context(ctx)
        return ctx

    def test_enabled_step_delegates_to_read_or_compute(self, mock_spark):
        """Enabled step returns OptionalDataFrame wrapping the computed result."""
        ctx = self._make_ctx(mock_spark)
        compute_df = MagicMock()

        result = read_or_compute_table_step(
            "ingest",
            read="db.ingest_table",
            compute=lambda: compute_df,
        )
        assert isinstance(result, OptionalDataFrame)
        assert bool(result) is True
        assert result.step == "ingest"
        assert result.table_name == "db.ingest_table"

    def test_disabled_step_cache_hit_classic(self, mock_spark):
        """Disabled step with existing cache -- classic Spark path."""
        cached_df = MagicMock()
        mock_spark.table.return_value = cached_df
        ctx = self._make_ctx(mock_spark, steps_config='{"enrich": false}')

        result = read_or_compute_table_step(
            "enrich",
            read="db.enriched",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        assert isinstance(result, OptionalDataFrame)
        assert bool(result) is True
        assert result.get() is cached_df
        assert result.table_name == "db.enriched"

    def test_disabled_step_cache_miss_classic(self, mock_spark):
        """Disabled step without cache -- returns deferred MissingTableError."""
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.table.side_effect = AnalysisException("TABLE_OR_VIEW_NOT_FOUND")
        ctx = self._make_ctx(mock_spark, steps_config='{"enrich": false}')

        result = read_or_compute_table_step(
            "enrich",
            read="db.enriched",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        assert isinstance(result, OptionalDataFrame)
        assert bool(result) is False
        with pytest.raises(MissingTableError, match="Table not found"):
            result.get()

    def test_disabled_step_cache_hit_spark_connect(self, mock_spark):
        """Disabled step with cache -- Spark Connect path."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        mock_spark.catalog.tableExists.return_value = True
        cached_df = MagicMock()
        mock_spark.table.return_value = cached_df
        ctx = self._make_ctx(mock_spark, steps_config='{"enrich": false}')

        result = read_or_compute_table_step(
            "enrich",
            read="db.enriched",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        assert bool(result) is True
        assert result.get() is cached_df
        mock_spark.catalog.tableExists.assert_called_with("db.enriched")

    def test_disabled_step_cache_miss_spark_connect(self, mock_spark):
        """Disabled step without cache -- Spark Connect deferred error."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        mock_spark.catalog.tableExists.return_value = False
        ctx = self._make_ctx(mock_spark, steps_config='{"enrich": false}')

        result = read_or_compute_table_step(
            "enrich",
            read="db.enriched",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        assert bool(result) is False
        with pytest.raises(MissingTableError, match="Table not found"):
            result.get()

    def test_recompute_triggers_refresh(self, mock_spark):
        """should_recompute drives refresh on enabled steps."""
        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back
        ctx = self._make_ctx(mock_spark, recompute=["ingest"])

        result = read_or_compute_table_step(
            "ingest",
            read="db.ingest_table",
            compute=lambda: compute_df,
        )
        # Refresh means compute is called even though cache exists
        compute_df.write.mode.return_value.saveAsTable.assert_called_once()

    def test_explicit_refresh(self, mock_spark):
        """refresh=True forces recompute on enabled step."""
        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back
        ctx = self._make_ctx(mock_spark)

        result = read_or_compute_table_step(
            "ingest",
            read="db.ingest_table",
            compute=lambda: compute_df,
            refresh=True,
        )
        compute_df.write.mode.return_value.saveAsTable.assert_called_once()

    def test_custom_id(self, mock_spark):
        """Custom id= overrides step name for recompute matching."""
        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back
        ctx = self._make_ctx(mock_spark, recompute=["custom_id"])

        result = read_or_compute_table_step(
            "ingest",
            read="db.ingest_table",
            compute=lambda: compute_df,
            id="custom_id",
        )
        assert result.id == "custom_id"
        compute_df.write.mode.return_value.saveAsTable.assert_called_once()

    def test_no_context_raises(self):
        """No context set and no explicit context raises RuntimeError."""
        with pytest.raises(RuntimeError, match="No pipeline context"):
            read_or_compute_table_step(
                "ingest",
                read="db.table",
                compute=lambda: None,
            )

    def test_callable_read(self, mock_spark):
        """Read argument can be a callable."""
        ctx = self._make_ctx(mock_spark)

        result = read_or_compute_table_step(
            "ingest",
            read=lambda c: f"{c.get('prefix', 'db')}.ingest_table",
            compute=lambda: MagicMock(),
        )
        assert result.table_name == "db.ingest_table"

    def test_disabled_step_non_table_error_propagates_classic(self, mock_spark):
        """Non-table errors on disabled step cache check propagate."""
        mock_spark.table.side_effect = ConnectionError("network failure")
        ctx = self._make_ctx(mock_spark, steps_config='{"enrich": false}')

        with pytest.raises(ConnectionError, match="network failure"):
            read_or_compute_table_step(
                "enrich",
                read="db.enriched",
                compute=lambda: pytest.fail("compute should not be called"),
            )


# ===========================================================================
# spark_conf tests
# ===========================================================================


class TestSparkConf:
    """Tests for spark_conf context manager."""

    def _make_conf_spark(self):
        """Create a mock SparkSession with conf.get/set/unset."""
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.session"
        store = {}

        def conf_get(key):
            if key in store:
                return store[key]
            raise Exception(f"Key not found: {key}")

        def conf_set(key, value):
            store[key] = value

        def conf_unset(key):
            store.pop(key, None)

        spark.conf.get = MagicMock(side_effect=conf_get)
        spark.conf.set = MagicMock(side_effect=conf_set)
        spark.conf.unset = MagicMock(side_effect=conf_unset)
        return spark, store

    def test_basic_override_and_restore(self):
        spark, store = self._make_conf_spark()
        store["spark.sql.shuffle.partitions"] = "200"

        with spark_conf({"spark.sql.shuffle.partitions": "1000"}, spark=spark):
            assert store["spark.sql.shuffle.partitions"] == "1000"

        assert store["spark.sql.shuffle.partitions"] == "200"

    def test_restore_on_exception(self):
        spark, store = self._make_conf_spark()
        store["spark.sql.shuffle.partitions"] = "200"

        with pytest.raises(ValueError, match="boom"):
            with spark_conf({"spark.sql.shuffle.partitions": "1000"}, spark=spark):
                assert store["spark.sql.shuffle.partitions"] == "1000"
                raise ValueError("boom")

        assert store["spark.sql.shuffle.partitions"] == "200"

    def test_nesting(self):
        spark, store = self._make_conf_spark()
        store["spark.sql.shuffle.partitions"] = "200"

        with spark_conf({"spark.sql.shuffle.partitions": "500"}, spark=spark):
            assert store["spark.sql.shuffle.partitions"] == "500"
            with spark_conf({"spark.sql.shuffle.partitions": "1000"}, spark=spark):
                assert store["spark.sql.shuffle.partitions"] == "1000"
            assert store["spark.sql.shuffle.partitions"] == "500"

        assert store["spark.sql.shuffle.partitions"] == "200"

    def test_previously_unset_key(self):
        spark, store = self._make_conf_spark()

        with spark_conf({"spark.test.new_key": "value"}, spark=spark):
            assert store["spark.test.new_key"] == "value"

        assert "spark.test.new_key" not in store

    def test_resolution_via_resolve_spark(self, mock_spark):
        """spark_conf with no explicit spark arg uses _resolve_spark."""
        # Set up the mock with a conf store
        store = {"spark.sql.shuffle.partitions": "200"}
        mock_spark.conf.get.side_effect = lambda k: store.get(k) or (_ for _ in ()).throw(Exception(f"not found: {k}"))
        mock_spark.conf.set.side_effect = lambda k, v: store.__setitem__(k, v)
        mock_spark.conf.unset.side_effect = lambda k: store.pop(k, None)

        ctx = Context()
        ctx["spark"] = mock_spark
        set_context(ctx)

        with spark_conf({"spark.sql.shuffle.partitions": "1000"}):
            assert store["spark.sql.shuffle.partitions"] == "1000"

        assert store["spark.sql.shuffle.partitions"] == "200"

    def test_strict_mode_propagates_error(self):
        spark, store = self._make_conf_spark()
        original_set = spark.conf.set.side_effect

        def failing_set(key, value):
            if key == "spark.bad.key":
                raise Exception("CONFIG_NOT_AVAILABLE")
            original_set(key, value)

        spark.conf.set = MagicMock(side_effect=failing_set)

        with pytest.raises(Exception, match="CONFIG_NOT_AVAILABLE"):
            with spark_conf({"spark.bad.key": "value"}, spark=spark):
                pass

    def test_lenient_mode_skips_bad_keys(self):
        spark, store = self._make_conf_spark()
        store["spark.sql.shuffle.partitions"] = "200"
        original_set = spark.conf.set.side_effect

        def failing_set(key, value):
            if key == "spark.bad.key":
                raise Exception("CONFIG_NOT_AVAILABLE")
            original_set(key, value)

        spark.conf.set = MagicMock(side_effect=failing_set)

        with spark_conf(
            {"spark.sql.shuffle.partitions": "1000", "spark.bad.key": "rejected"},
            spark=spark,
            lenient=True,
        ):
            assert store["spark.sql.shuffle.partitions"] == "1000"
            assert "spark.bad.key" not in store

        # Good key was restored, bad key was never set so not restored
        assert store["spark.sql.shuffle.partitions"] == "200"


# ===========================================================================
# cluster_cores and core_based_parallelism tests
# ===========================================================================


class TestClusterCores:
    """Tests for cluster_cores discovery."""

    def test_databricks_cluster_with_cores_per_worker(self):
        """Databricks cluster with cores_per_worker provided -- skip UDF probe."""
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.session"
        spark.conf.get.return_value = "10"  # 10 workers

        result = cluster_cores(spark=spark, cores_per_worker=16)
        assert result == 160
        spark.conf.get.assert_called_with(
            "spark.databricks.clusterUsageTags.clusterWorkers"
        )

    def test_databricks_cluster_with_udf_probe(self):
        """Databricks cluster without cores_per_worker -- uses UDF probe."""
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.session"
        spark.conf.get.return_value = "200"  # 200 workers

        # Mock the UDF probe chain: spark.range(1).select(...).collect()[0][0]
        mock_row = MagicMock()
        mock_row.__getitem__ = MagicMock(return_value=16)
        mock_collect = [mock_row]
        spark.range.return_value.select.return_value.collect.return_value = mock_collect

        # Mock pyspark imports (not installed in test env)
        mock_udf = MagicMock(side_effect=lambda **kw: lambda fn: fn)
        mock_pyspark_functions = MagicMock(udf=mock_udf)
        mock_pyspark_types = MagicMock()
        with patch.dict("sys.modules", {
            "pyspark": MagicMock(),
            "pyspark.sql": MagicMock(),
            "pyspark.sql.functions": mock_pyspark_functions,
            "pyspark.sql.types": mock_pyspark_types,
        }):
            result = cluster_cores(spark=spark)
        assert result == 3200

    def test_local_spark_fallback(self):
        """No Databricks tags -- falls back to os.cpu_count()."""
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.session"
        spark.conf.get.side_effect = Exception("key not found")

        with patch("os.cpu_count", return_value=8):
            result = cluster_cores(spark=spark)
        assert result == 8

    def test_cluster_cores_connect_no_tags_raises(self):
        """Spark Connect without cluster tags raises, doesn't fall back to os.cpu_count."""
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.connect.session"
        spark.conf.get.side_effect = Exception("key not found")

        with pytest.raises(RuntimeError, match="Spark Connect.*cluster tags"):
            cluster_cores(spark=spark)


class TestCoreBasedParallelism:
    """Tests for core_based_parallelism scaling."""

    def test_basic_scaling(self):
        result = core_based_parallelism(
            min_parallelism=200, max_parallelism=10000,
            compute=lambda cores: cores * 3,
            cores=100,
        )
        assert result == 300

    def test_clamp_to_min(self):
        result = core_based_parallelism(
            min_parallelism=200, max_parallelism=10000,
            compute=lambda cores: cores * 1,  # 50 < 200
            cores=50,
        )
        assert result == 200

    def test_clamp_to_max(self):
        result = core_based_parallelism(
            min_parallelism=200, max_parallelism=10000,
            compute=lambda cores: cores * 10,  # 50000 > 10000
            cores=5000,
        )
        assert result == 10000

    def test_with_cores_arg_skips_discovery(self):
        """Pre-computed cores argument skips cluster_cores entirely."""
        result = core_based_parallelism(
            min_parallelism=100, max_parallelism=5000,
            compute=lambda c: c * 2,
            cores=500,
        )
        assert result == 1000

    def test_discovery_integration(self):
        """Without cores= arg, calls cluster_cores for discovery."""
        spark = MagicMock()
        spark.__class__.__module__ = "pyspark.sql.session"
        spark.conf.get.side_effect = Exception("not databricks")

        with patch("os.cpu_count", return_value=4):
            result = core_based_parallelism(
                min_parallelism=10, max_parallelism=1000,
                compute=lambda c: c * 5,
                spark=spark,
            )
        assert result == 20


# ===========================================================================
# _table_exists tests
# ===========================================================================


class TestTableExists:
    """Tests for _table_exists helper -- SCHEMA_NOT_FOUND tolerance."""

    def test_table_exists_returns_true(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        assert _table_exists(spark, "cat.schema.table") is True
        spark.catalog.tableExists.assert_called_once_with("cat.schema.table")

    def test_table_does_not_exist_returns_false(self):
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        assert _table_exists(spark, "cat.schema.table") is False

    def test_schema_not_found_returns_false(self):
        """SCHEMA_NOT_FOUND from tableExists should return False, not raise."""
        AnalysisException = type("AnalysisException", (Exception,), {})
        spark = MagicMock()
        spark.catalog.tableExists.side_effect = AnalysisException(
            "[SCHEMA_NOT_FOUND] The schema `swoop_dev.__testing_abc` cannot be found."
        )
        assert _table_exists(spark, "swoop_dev.__testing_abc.my_table") is False

    def test_table_or_view_not_found_returns_false(self):
        """TABLE_OR_VIEW_NOT_FOUND from tableExists should return False."""
        AnalysisException = type("AnalysisException", (Exception,), {})
        spark = MagicMock()
        spark.catalog.tableExists.side_effect = AnalysisException(
            "[TABLE_OR_VIEW_NOT_FOUND] table not found"
        )
        assert _table_exists(spark, "cat.schema.table") is False

    def test_non_analysis_exception_propagates(self):
        """Non-AnalysisException errors should propagate."""
        spark = MagicMock()
        spark.catalog.tableExists.side_effect = ConnectionError("network failure")
        with pytest.raises(ConnectionError, match="network failure"):
            _table_exists(spark, "cat.schema.table")

    def test_other_analysis_exception_propagates(self):
        """AnalysisException with non-not-found error class should propagate."""
        AnalysisException = type("AnalysisException", (Exception,), {})
        spark = MagicMock()
        spark.catalog.tableExists.side_effect = AnalysisException(
            "[INSUFFICIENT_PERMISSIONS] User does not have access."
        )
        with pytest.raises(AnalysisException, match="INSUFFICIENT_PERMISSIONS"):
            _table_exists(spark, "cat.schema.table")

    def test_spark_connect_schema_not_found_cache_check(self, mock_spark):
        """End-to-end: Spark Connect with SCHEMA_NOT_FOUND triggers compute."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.catalog.tableExists.side_effect = AnalysisException(
            "[SCHEMA_NOT_FOUND] The schema `swoop_dev.__testing_abc` cannot be found."
        )

        compute_df = MagicMock()
        compute_df.write = MagicMock()
        read_back = MagicMock()
        mock_spark.table.return_value = read_back

        ctx = Context()
        ctx["spark"] = mock_spark
        set_context(ctx)

        result = read_or_compute_table(
            read="swoop_dev.__testing_abc.my_table",
            compute=lambda: compute_df,
        )
        # Should have gone through compute path, not raised
        compute_df.write.mode.return_value.saveAsTable.assert_called_once_with(
            "swoop_dev.__testing_abc.my_table"
        )

    def test_disabled_step_schema_not_found_spark_connect(self, mock_spark):
        """Disabled step + SCHEMA_NOT_FOUND on Spark Connect = deferred error, not crash."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.catalog.tableExists.side_effect = AnalysisException(
            "[SCHEMA_NOT_FOUND] The schema `swoop_dev.__testing_abc` cannot be found."
        )

        ctx = Context()
        ctx["spark"] = mock_spark
        ctx.overlay_params({"steps": '{"ingest": false}'})
        runner = StepRunner(steps=["ingest"])
        ctx.set_runner(runner)
        set_context(ctx)

        # ctx.run should raise RuntimeError (disabled + no cache), not AnalysisException
        with pytest.raises(RuntimeError, match="disabled but table.*does not exist"):
            ctx.run(
                "ingest",
                table="swoop_dev.__testing_abc.my_table",
                compute=lambda: None,
            )

    def test_disabled_step_schema_not_found_optional(self, mock_spark):
        """read_or_compute_table_step with SCHEMA_NOT_FOUND returns deferred error."""
        mock_spark.__class__.__module__ = "pyspark.sql.connect.session"
        AnalysisException = type("AnalysisException", (Exception,), {})
        mock_spark.catalog.tableExists.side_effect = AnalysisException(
            "[SCHEMA_NOT_FOUND] The schema `swoop_dev.__testing_abc` cannot be found."
        )

        ctx = Context()
        ctx["spark"] = mock_spark
        runner = StepRunner(steps=["enrich"])
        ctx.set_runner(runner)
        ctx.overlay_params({"steps": '{"enrich": false}'})
        set_context(ctx)

        result = read_or_compute_table_step(
            "enrich",
            read="swoop_dev.__testing_abc.enriched",
            compute=lambda: pytest.fail("compute should not be called"),
        )
        assert bool(result) is False
        with pytest.raises(MissingTableError):
            result.get()


# ===========================================================================
# _ensure_schema_exists tests
# ===========================================================================


class TestEnsureSchemaExists:
    """Tests for _ensure_schema_exists helper."""

    def test_three_part_name_creates_schema(self):
        spark = MagicMock()
        _ensure_schema_exists(spark, "swoop_dev.__testing_abc.my_table")
        spark.sql.assert_called_once_with(
            "CREATE SCHEMA IF NOT EXISTS swoop_dev.__testing_abc"
        )

    def test_two_part_name_is_noop(self):
        spark = MagicMock()
        _ensure_schema_exists(spark, "schema.my_table")
        spark.sql.assert_not_called()

    def test_one_part_name_is_noop(self):
        spark = MagicMock()
        _ensure_schema_exists(spark, "my_table")
        spark.sql.assert_not_called()


# ===========================================================================
# _materialize_and_read SCHEMA_NOT_FOUND retry tests
# ===========================================================================


class TestMaterializeAndReadRetry:
    """Tests for SCHEMA_NOT_FOUND auto-create and retry in _materialize_and_read."""

    def test_no_error_no_retry(self):
        """Normal write path -- no retry needed."""
        spark = MagicMock()
        df = MagicMock()
        df.write = MagicMock()
        read_back = MagicMock()
        spark.table.return_value = read_back

        result = _materialize_and_read(spark, "cat.schema.table", df)
        df.write.mode.assert_called_once_with("overwrite")
        df.write.mode.return_value.saveAsTable.assert_called_once_with("cat.schema.table")
        assert result is read_back

    def test_schema_not_found_triggers_create_and_retry(self):
        """SCHEMA_NOT_FOUND on first saveAsTable -> create schema -> retry."""
        spark = MagicMock()
        df = MagicMock()
        df.write = MagicMock()
        read_back = MagicMock()
        spark.table.return_value = read_back

        # First saveAsTable raises SCHEMA_NOT_FOUND, second succeeds
        save_mock = df.write.mode.return_value.saveAsTable
        AnalysisException = type("AnalysisException", (Exception,), {})
        save_mock.side_effect = [
            AnalysisException("[SCHEMA_NOT_FOUND] The schema `cat.schema` cannot be found."),
            None,
        ]

        result = _materialize_and_read(spark, "cat.schema.table", df)

        # Schema should have been created
        spark.sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS cat.schema")
        # saveAsTable called twice (original + retry)
        assert save_mock.call_count == 2
        assert result is read_back

    def test_non_schema_error_propagates(self):
        """Non-SCHEMA_NOT_FOUND errors propagate without retry."""
        spark = MagicMock()
        df = MagicMock()
        df.write = MagicMock()

        save_mock = df.write.mode.return_value.saveAsTable
        save_mock.side_effect = RuntimeError("disk full")

        with pytest.raises(RuntimeError, match="disk full"):
            _materialize_and_read(spark, "cat.schema.table", df)

        # Should NOT have tried to create schema
        spark.sql.assert_not_called()

    def test_dataframe_writer_retry(self):
        """DataFrameWriter path retries on SCHEMA_NOT_FOUND with warning."""
        spark = MagicMock()
        # A DataFrameWriter has saveAsTable but no collect
        writer = MagicMock(spec=["saveAsTable", "mode", "format"])
        read_back = MagicMock()
        spark.table.return_value = read_back

        AnalysisException = type("AnalysisException", (Exception,), {})
        writer.saveAsTable.side_effect = [
            AnalysisException("[SCHEMA_NOT_FOUND] The schema `cat.schema` cannot be found."),
            None,
        ]

        result = _materialize_and_read(spark, "cat.schema.table", writer)

        spark.sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS cat.schema")
        assert writer.saveAsTable.call_count == 2
        assert result is read_back

    def test_two_part_name_no_schema_create(self):
        """SCHEMA_NOT_FOUND with two-part name -- _ensure_schema_exists is a no-op."""
        spark = MagicMock()
        df = MagicMock()
        df.write = MagicMock()

        save_mock = df.write.mode.return_value.saveAsTable
        AnalysisException = type("AnalysisException", (Exception,), {})
        # First call raises, retry also raises (schema not created for 2-part name)
        save_mock.side_effect = AnalysisException(
            "[SCHEMA_NOT_FOUND] The schema `schema` cannot be found."
        )

        # _ensure_schema_exists is a no-op for 2-part names, so the retry
        # will raise the same error
        with pytest.raises(AnalysisException):
            _materialize_and_read(spark, "schema.table", df)

        # _ensure_schema_exists should NOT have issued CREATE SCHEMA
        spark.sql.assert_not_called()

    def test_non_analysis_exception_with_schema_not_found_propagates(self):
        """Non-AnalysisException mentioning SCHEMA_NOT_FOUND should NOT retry."""
        spark = MagicMock()
        df = MagicMock()
        df.write = MagicMock()

        save_mock = df.write.mode.return_value.saveAsTable
        save_mock.side_effect = RuntimeError(
            "Wrapped: SCHEMA_NOT_FOUND in some context"
        )

        with pytest.raises(RuntimeError, match="SCHEMA_NOT_FOUND"):
            _materialize_and_read(spark, "cat.schema.table", df)

        # Should NOT have tried to create schema (wrong exception type)
        spark.sql.assert_not_called()

    def test_invalid_table_name_still_rejected(self):
        """Table name validation still runs before any write attempt."""
        spark = MagicMock()
        df = MagicMock()

        with pytest.raises(ValueError, match="Invalid table name"):
            _materialize_and_read(spark, "table; DROP TABLE users--", df)
