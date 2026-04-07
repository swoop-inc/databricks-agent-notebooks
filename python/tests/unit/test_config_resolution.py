"""Comprehensive tests for the unified parameter resolution engine."""

from __future__ import annotations

from typing import Any

import pytest

from databricks_agent_notebooks.config.resolution import (
    AGENT_DEFAULTS,
    CONFIG_KEYS,
    EnvironmentNotFoundError,
    PARAM_CLUSTER_LIST_TIMEOUT,
    collect_env_vars,
    resolve_from_environment,
    resolve_params,
)
from databricks_agent_notebooks.config.frontmatter import AgentNotebookConfig


# ---------------------------------------------------------------------------
# 1. Basic resolution (no environments)
# ---------------------------------------------------------------------------


class TestBasicResolution:
    def test_single_source_flat_params(self) -> None:
        result = resolve_params([{"profile": "prod", "cluster": "c1"}])
        assert result["profile"] == "prod"
        assert result["cluster"] == "c1"
        assert result["env"] == "default"

    def test_multiple_sources_later_wins(self) -> None:
        result = resolve_params([
            {"profile": "dev", "timeout": 100},
            {"profile": "prod"},
        ])
        assert result["profile"] == "prod"
        assert result["timeout"] == 100

    def test_empty_sources(self) -> None:
        result = resolve_params([])
        assert result["env"] == "default"
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 120.0

    def test_empty_source_dicts(self) -> None:
        result = resolve_params([{}, {}, {}])
        assert result["env"] == "default"
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 120.0

    def test_backward_compat_no_environments(self) -> None:
        """Without environments, the result is a flat merge plus env=default."""
        result = resolve_params([
            {"profile": "dev", "timeout": 300},
            {"timeout": 600, "cluster": "c1"},
        ])
        assert result == {
            **AGENT_DEFAULTS,
            "profile": "dev",
            "timeout": 600,
            "cluster": "c1",
            "env": "default",
        }


# ---------------------------------------------------------------------------
# 2. Params promotion
# ---------------------------------------------------------------------------


class TestParamsPromotion:
    def test_nested_params_override_siblings(self) -> None:
        result = resolve_params([{
            "region": "us-east-1",
            "params": {"region": "eu-west-1"},
        }])
        assert result["region"] == "eu-west-1"

    def test_empty_params_no_op(self) -> None:
        result = resolve_params([{"profile": "dev", "params": {}}])
        assert result["profile"] == "dev"

    def test_non_dict_params_ignored(self) -> None:
        result = resolve_params([{"profile": "dev", "params": "invalid"}])
        assert result["profile"] == "dev"

    def test_params_from_multiple_sources(self) -> None:
        result = resolve_params([
            {"params": {"a": "1", "b": "2"}},
            {"params": {"b": "3", "c": "4"}},
        ])
        assert result["a"] == "1"
        assert result["b"] == "3"
        assert result["c"] == "4"


# ---------------------------------------------------------------------------
# 3. Environment resolution
# ---------------------------------------------------------------------------


class TestEnvironmentResolution:
    def test_env_flag_selects_environment(self) -> None:
        result = resolve_params([{
            "environments": {
                "staging": {"cluster": "staging-cluster", "timeout": 300},
            },
            "env": "staging",
        }])
        assert result["cluster"] == "staging-cluster"
        assert result["timeout"] == 300
        assert result["env"] == "staging"

    def test_default_env_redirect(self) -> None:
        result = resolve_params([{
            "environments": {
                "default": {"env": "staging"},
                "staging": {"cluster": "staging-cluster"},
            },
        }])
        assert result["cluster"] == "staging-cluster"
        assert result["env"] == "staging"

    def test_no_env_resolves_to_default(self) -> None:
        result = resolve_params([{"profile": "dev"}])
        assert result["env"] == "default"

    def test_comma_separated_env_spec(self) -> None:
        result = resolve_params([{
            "environments": {
                "base": {"timeout": 100, "profile": "base"},
                "overlay": {"profile": "overlay"},
            },
            "env": "base,overlay",
        }])
        assert result["timeout"] == 100
        assert result["profile"] == "overlay"
        assert result["env"] == "base,overlay"

    def test_environment_not_found_error(self) -> None:
        with pytest.raises(EnvironmentNotFoundError, match="nonexistent"):
            resolve_params([{"env": "nonexistent"}])

    def test_env_default_with_no_default_is_silent(self) -> None:
        """env='default' with no default env defined is OK (resolves to empty)."""
        result = resolve_params([{"profile": "dev"}])
        assert result["env"] == "default"
        assert result["profile"] == "dev"

    def test_explicit_env_default_no_default_env(self) -> None:
        """Explicitly setting env='default' when no default env exists is OK."""
        result = resolve_params([{"env": "default", "profile": "dev"}])
        assert result["env"] == "default"
        assert result["profile"] == "dev"


# ---------------------------------------------------------------------------
# 4. Environment registry
# ---------------------------------------------------------------------------


class TestEnvironmentRegistry:
    def test_later_source_replaces_same_name_entirely(self) -> None:
        result = resolve_params([
            {"environments": {"staging": {"cluster": "old", "timeout": 100}}},
            {"environments": {"staging": {"cluster": "new"}}},
            {"env": "staging"},
        ])
        assert result["cluster"] == "new"
        # timeout from old staging is gone (replaced, not merged)
        assert "timeout" not in result

    def test_env_nested_params_promoted(self) -> None:
        """Nested params inside environment definitions are promoted to top level."""
        result = resolve_params([
            {"environments": {"staging": {"cluster": "c1", "params": {"region": "us-east-1"}}}},
            {"env": "staging"},
        ])
        assert result["region"] == "us-east-1"
        assert result["cluster"] == "c1"
        assert "params" not in result or not isinstance(result.get("params"), dict)

    def test_env_values_are_defaults(self) -> None:
        """Explicit params override environment values."""
        result = resolve_params([{
            "environments": {"staging": {"cluster": "env-cluster", "timeout": 300}},
            "env": "staging",
            "cluster": "explicit-cluster",
        }])
        assert result["cluster"] == "explicit-cluster"
        assert result["timeout"] == 300

    def test_env_values_from_later_source_override(self) -> None:
        """A later explicit param overrides env default."""
        result = resolve_params([
            {
                "environments": {"staging": {"timeout": 300}},
                "env": "staging",
            },
            {"timeout": 600},
        ])
        assert result["timeout"] == 600


# ---------------------------------------------------------------------------
# 5. Priority ordering
# ---------------------------------------------------------------------------


class TestPriorityOrdering:
    def test_env_params_lower_than_merged_params(self) -> None:
        result = resolve_params([{
            "environments": {"staging": {"profile": "env-profile"}},
            "env": "staging",
            "profile": "explicit-profile",
        }])
        assert result["profile"] == "explicit-profile"

    def test_pinned_env_key(self) -> None:
        """The env key is always the resolved spec, not overridden by env params."""
        result = resolve_params([{
            "environments": {"staging": {"env": "should-be-ignored"}},
            "env": "staging",
        }])
        assert result["env"] == "staging"

    def test_toml_lt_env_vars_lt_frontmatter_lt_cli(self) -> None:
        """Four source levels in priority order."""
        result = resolve_params([
            {"profile": "toml", "cluster": "toml", "timeout": 100, "language": "toml"},
            {"cluster": "env-var", "language": "env-var"},
            {"language": "frontmatter"},
            {},  # CLI with no overrides
        ])
        assert result["profile"] == "toml"
        assert result["cluster"] == "env-var"
        assert result["language"] == "frontmatter"
        assert result["timeout"] == 100


# ---------------------------------------------------------------------------
# 6. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_source_list_returns_env_default(self) -> None:
        result = resolve_params([])
        assert result["env"] == "default"
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 120.0

    def test_source_with_only_environments(self) -> None:
        result = resolve_params([{
            "environments": {"staging": {"cluster": "c1"}},
        }])
        assert result["env"] == "default"
        assert "cluster" not in result

    def test_env_that_sets_env_key_is_stripped(self) -> None:
        """The env key inside environment params is stripped."""
        result = resolve_params([{
            "environments": {"staging": {"env": "recursive", "cluster": "c1"}},
            "env": "staging",
        }])
        assert result["env"] == "staging"
        assert result["cluster"] == "c1"

    def test_list_values_overwrite(self) -> None:
        """List values from later sources overwrite earlier ones."""
        result = resolve_params([
            {"libraries": ["a", "b"]},
            {"libraries": ["c"]},
        ])
        assert result["libraries"] == ["c"]

    def test_list_overwrite_with_env_defaults(self) -> None:
        """Explicit list params overwrite env default lists entirely."""
        result = resolve_params([{
            "environments": {"staging": {"libraries": ["env-lib"]}},
            "env": "staging",
            "libraries": ["explicit-lib"],
        }])
        assert result["libraries"] == ["explicit-lib"]

    def test_env_list_used_when_no_explicit(self) -> None:
        """Env-provided list values apply when no explicit override exists."""
        result = resolve_params([{
            "environments": {"staging": {"libraries": ["env-lib"]}},
            "env": "staging",
        }])
        assert result["libraries"] == ["env-lib"]

    def test_scalar_values_overwrite(self) -> None:
        """Scalar values from later sources overwrite earlier ones."""
        result = resolve_params([
            {"profile": "first"},
            {"profile": "second"},
        ])
        assert result["profile"] == "second"


# ---------------------------------------------------------------------------
# 7. collect_env_vars
# ---------------------------------------------------------------------------


class TestCollectEnvVars:
    def test_agent_notebook_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK_PROFILE", "from-env")
        result = collect_env_vars()
        assert result["profile"] == "from-env"

    def test_skips_local_spark_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK_LOCAL_SPARK_MASTER", "local[4]")
        result = collect_env_vars()
        assert "local_spark_master" not in result

    def test_skips_non_matching(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTHER_VAR", "value")
        result = collect_env_vars()
        assert "other_var" not in result

    def test_skips_bare_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK", "bare")
        result = collect_env_vars()
        assert len(result) == 0 or "agent_notebook" not in result

    def test_multiple_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK_PROFILE", "prod")
        monkeypatch.setenv("AGENT_NOTEBOOK_TIMEOUT", "600")
        monkeypatch.setenv("AGENT_NOTEBOOK_ENV", "staging")
        result = collect_env_vars()
        assert result["profile"] == "prod"
        assert result["timeout"] == "600"  # always string
        assert result["env"] == "staging"

    def test_values_are_strings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK_TIMEOUT", "300")
        result = collect_env_vars()
        assert isinstance(result["timeout"], str)


# ---------------------------------------------------------------------------
# 8. from_resolved_params
# ---------------------------------------------------------------------------


class TestFromResolvedParams:
    def test_extracts_config_keys(self) -> None:
        resolved = {
            "profile": "prod",
            "cluster": "c1",
            "language": "python",
            "timeout": 300,
            "env": "default",
        }
        config, params = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.profile == "prod"
        assert config.cluster == "c1"
        assert config.language == "python"
        assert config.timeout == 300
        assert params == {"env": "default"}

    def test_returns_remaining_as_notebook_params(self) -> None:
        resolved = {
            "profile": "prod",
            "region": "us-east-1",
            "debug": "true",
            "env": "staging",
        }
        config, params = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.profile == "prod"
        assert params == {"region": "us-east-1", "debug": "true", "env": "staging"}

    def test_excludes_environments_from_notebook_params(self) -> None:
        resolved = {
            "env": "staging",
            "environments": {"staging": {}},
            "region": "us-east-1",
        }
        config, params = AgentNotebookConfig.from_resolved_params(resolved)
        assert "env" in params
        assert params["env"] == "staging"
        assert "environments" not in params
        assert params == {"region": "us-east-1", "env": "staging"}

    def test_coerces_timeout_from_string(self) -> None:
        resolved = {"timeout": "600", "env": "default"}
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.timeout == 600

    def test_coerces_timeout_from_float(self) -> None:
        resolved = {"timeout": 300.5, "env": "default"}
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.timeout == 300

    def test_coerces_bool_from_string(self) -> None:
        resolved = {
            "allow_errors": "true",
            "inject_session": "false",
            "preprocess": "True",
            "clean": "False",
            "env": "default",
        }
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.allow_errors is True
        assert config.inject_session is False
        assert config.preprocess is True
        assert config.clean is False

    def test_coerces_libraries_from_list(self) -> None:
        resolved = {"libraries": ["a", "b"], "env": "default"}
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.libraries == ("a", "b")

    def test_coerces_libraries_from_comma_string(self) -> None:
        resolved = {"libraries": "a,b,c", "env": "default"}
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.libraries == ("a", "b", "c")

    def test_does_not_resolve_relative_paths(self) -> None:
        """from_resolved_params no longer resolves paths -- that happens upstream."""
        resolved = {
            "libraries": ["relative/lib"],
            "output_dir": "relative/output",
            "env": "default",
        }
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        # Paths are passed through as-is (no resolution)
        assert config.libraries == ("relative/lib",)
        assert config.output_dir == "relative/output"

    def test_absolute_paths_unchanged(self) -> None:
        resolved = {
            "libraries": ["/absolute/lib"],
            "output_dir": "/absolute/output",
            "env": "default",
        }
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.libraries == ("/absolute/lib",)
        assert config.output_dir == "/absolute/output"

    def test_coerces_timeout_from_float_string(self) -> None:
        """String timeout '300.5' should be accepted via float fallback."""
        resolved = {"timeout": "300.5", "env": "default"}
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.timeout == 300

    def test_coerces_bool_from_int(self) -> None:
        """TOML integer 1/0 accepted for boolean fields."""
        resolved = {
            "allow_errors": 1,
            "inject_session": 0,
            "env": "default",
        }
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.allow_errors is True
        assert config.inject_session is False

    def test_coerces_bool_from_string_digits(self) -> None:
        """Env var string "1"/"0" accepted for boolean fields."""
        resolved = {
            "allow_errors": "1",
            "inject_session": "0",
            "env": "default",
        }
        config, _ = AgentNotebookConfig.from_resolved_params(resolved)
        assert config.allow_errors is True
        assert config.inject_session is False


# ---------------------------------------------------------------------------
# 9. Agent defaults
# ---------------------------------------------------------------------------


class TestAgentDefaults:
    def test_cluster_list_timeout_present_in_empty_resolution(self) -> None:
        result = resolve_params([])
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 120.0

    def test_agent_default_overridden_by_explicit_param(self) -> None:
        result = resolve_params([{PARAM_CLUSTER_LIST_TIMEOUT: 10}])
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 10

    def test_agent_default_overridden_by_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK_CLUSTER_LIST_TIMEOUT", "5")
        result = resolve_params([collect_env_vars()])
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == "5"

    def test_agent_default_overridden_by_environment(self) -> None:
        result = resolve_params([{
            "environments": {"fast": {PARAM_CLUSTER_LIST_TIMEOUT: 2}},
            "env": "fast",
        }])
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 2

    def test_explicit_param_beats_environment(self) -> None:
        result = resolve_params([{
            "environments": {"fast": {PARAM_CLUSTER_LIST_TIMEOUT: 2}},
            "env": "fast",
            PARAM_CLUSTER_LIST_TIMEOUT: 15,
        }])
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 15


# ---------------------------------------------------------------------------
# 10. resolve_from_environment
# ---------------------------------------------------------------------------


class TestResolveFromEnvironment:
    def test_returns_agent_defaults_with_no_project(self, tmp_path) -> None:
        result = resolve_from_environment(start_dir=tmp_path)
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 120.0

    def test_extra_sources_override(self, tmp_path) -> None:
        result = resolve_from_environment(
            start_dir=tmp_path,
            extra_sources=[{PARAM_CLUSTER_LIST_TIMEOUT: 5}],
        )
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == 5

    def test_env_var_override(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AGENT_NOTEBOOK_CLUSTER_LIST_TIMEOUT", "10")
        result = resolve_from_environment(start_dir=tmp_path)
        assert result[PARAM_CLUSTER_LIST_TIMEOUT] == "10"
