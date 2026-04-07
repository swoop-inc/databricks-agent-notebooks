"""Jinja2-based preprocessing engine.

Expands directives written as ``{! plugin("arg") !}`` in notebook source text
before the file enters the notebook parsing pipeline.  Custom delimiters
prevent conflicts with Python f-strings, Jinja2/dbt templates, and all other
common notebook content.
"""

from __future__ import annotations

import logging
from pathlib import Path

import jinja2

_log = logging.getLogger(__name__)

from databricks_agent_notebooks.preprocessing.builtins import install_builtins
from databricks_agent_notebooks.preprocessing.errors import PreprocessorError
from databricks_agent_notebooks.preprocessing.plugins import (
    PreprocessorPlugin,
    PluginContext,
)
from databricks_agent_notebooks.preprocessing.plugins.best_practices import BestPracticesPlugin
from databricks_agent_notebooks.preprocessing.plugins.include import IncludePlugin
from databricks_agent_notebooks.preprocessing.plugins.param import ParamPlugin

_BUILTIN_PLUGINS: list[type[PreprocessorPlugin]] = [BestPracticesPlugin, IncludePlugin, ParamPlugin]


def _create_environment() -> jinja2.Environment:
    """Build a Jinja2 environment with custom delimiters.

    The ``{! !}`` variable delimiters never collide with Python, Jinja2/dbt,
    LaTeX, bash, SQL, or markdown.  Block and comment delimiters are set to
    equally unusual sequences so Jinja2 ignores ``{% %}`` and ``{# #}`` in
    notebook content.
    """
    return jinja2.Environment(
        variable_start_string="{!",
        variable_end_string="!}",
        block_start_string="{!%",
        block_end_string="%!}",
        comment_start_string="{!#",
        comment_end_string="#!}",
        autoescape=False,
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
        finalize=lambda x: "" if x is None else x,
    )


def preprocess_text(text: str, *, notebook_path: Path, params: dict[str, str] | None = None) -> str:
    """Expand preprocessing directives in source text.

    Returns the text unchanged (same object) if no directives are present,
    enabling a fast-path identity check (``result is text``) for callers
    that want to skip temp-file creation.
    """
    if "{!" not in text:
        return text

    context = PluginContext(notebook_path=notebook_path, params=params or {})
    plugins = [cls(context) for cls in _BUILTIN_PLUGINS]

    env = _create_environment()
    install_builtins(env)
    template_globals = {
        p.plugin_metadata().name: p for p in plugins if callable(p)
    }

    try:
        template = env.from_string(text)
        return template.render(template_globals)
    except jinja2.TemplateSyntaxError as exc:
        raise PreprocessorError(None, f"Syntax error: {exc}") from exc
    except jinja2.UndefinedError as exc:
        raise PreprocessorError(None, f"Unknown directive: {exc}") from exc
    finally:
        for p in plugins:
            try:
                p.cleanup()
            except Exception:
                _log.debug(
                    "cleanup failed for plugin %s",
                    p.plugin_metadata().name,
                    exc_info=True,
                )
