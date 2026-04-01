"""Tests for notebook rendering."""

from __future__ import annotations

import base64
import sys
from pathlib import Path
from unittest.mock import patch

import nbformat

from databricks_agent_notebooks.execution.rendering import (
    _is_almond_repl_echo,
    render,
    render_all,
    render_html,
    render_markdown,
)


def _sample_notebook(tmp_path: Path) -> Path:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {
        "name": "scala212-dbr-connect",
        "display_name": "Scala 2.12 (Databricks Connect)",
        "language": "scala",
    }
    injected = nbformat.v4.new_code_cell("// setup code")
    injected.metadata["agent_notebook_injected"] = True
    markdown = nbformat.v4.new_markdown_cell("# Hello")
    code = nbformat.v4.new_code_cell("val x = 1")
    code.outputs = [nbformat.v4.new_output("stream", text="2\n")]
    notebook.cells = [injected, markdown, code]

    path = tmp_path / "sample.ipynb"
    nbformat.write(notebook, str(path))
    return path


def test_render_markdown_hides_injected_cell(tmp_path: Path) -> None:
    notebook_path = _sample_notebook(tmp_path)
    output_path = tmp_path / "out.md"

    render_markdown(notebook_path, output_path)
    content = output_path.read_text(encoding="utf-8")

    assert "// setup code" not in content
    assert "# Hello" in content
    assert "```scala" in content
    assert "```output" in content


def test_render_markdown_saves_images(tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {"language": "scala"}
    png_data = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
        b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00"
        b"\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00"
        b"\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    b64_png = base64.b64encode(png_data).decode("ascii")
    code = nbformat.v4.new_code_cell("plot()")
    code.outputs = [nbformat.v4.new_output("display_data", data={"image/png": b64_png, "text/plain": "<Figure>"})]
    notebook.cells = [code]

    notebook_path = tmp_path / "with_image.ipynb"
    nbformat.write(notebook, str(notebook_path))
    output_path = tmp_path / "rendered.md"

    render_markdown(notebook_path, output_path)

    assert (tmp_path / "rendered_assets").is_dir()
    assert "rendered_assets/" in output_path.read_text(encoding="utf-8")


@patch("databricks_agent_notebooks.execution.rendering.subprocess.run")
def test_render_html_calls_nbconvert(mock_run, tmp_path: Path) -> None:
    mock_run.return_value = type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    notebook_path = _sample_notebook(tmp_path)
    output_path = tmp_path / "out.html"

    render_html(notebook_path, output_path)

    cmd = mock_run.call_args[0][0]
    assert cmd[:3] == [sys.executable, "-m", "jupyter"]


def test_render_dispatch_all_returns_both_formats(tmp_path: Path) -> None:
    notebook_path = _sample_notebook(tmp_path)
    with patch("databricks_agent_notebooks.execution.rendering.render_html", return_value=tmp_path / "sample.html"):
        result = render_all(notebook_path, tmp_path)

    assert "md" in result
    assert "html" in result


def test_render_dispatch_md_only(tmp_path: Path) -> None:
    notebook_path = _sample_notebook(tmp_path)
    result = render(notebook_path, tmp_path, fmt="md")
    assert list(result) == ["md"]


def test_render_markdown_strips_ansi_from_stream(tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {"language": "python"}
    code = nbformat.v4.new_code_cell("print('hi')")
    code.outputs = [
        nbformat.v4.new_output("stream", text="\x1b[32mgreen\x1b[0m and \x1b[1;31mbold red\x1b[0m\n"),
    ]
    notebook.cells = [code]

    path = tmp_path / "ansi_stream.ipynb"
    nbformat.write(notebook, str(path))
    output_path = tmp_path / "ansi_stream.md"

    render_markdown(path, output_path)
    content = output_path.read_text(encoding="utf-8")

    assert "\x1b" not in content
    assert "green and bold red" in content


def test_render_markdown_strips_osc_sequences(tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {"language": "python"}
    code = nbformat.v4.new_code_cell("rich.print()")
    code.outputs = [
        nbformat.v4.new_output(
            "stream",
            text="\x1b]8;;https://example.com\x1b\\link text\x1b]8;;\x1b\\\n",
        ),
    ]
    notebook.cells = [code]

    path = tmp_path / "ansi_osc.ipynb"
    nbformat.write(notebook, str(path))
    output_path = tmp_path / "ansi_osc.md"

    render_markdown(path, output_path)
    content = output_path.read_text(encoding="utf-8")

    assert "\x1b" not in content
    assert "link text" in content


def test_render_markdown_strips_ansi_from_error_evalue(tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {"language": "python"}
    code = nbformat.v4.new_code_cell("raise ValueError('bad')")
    error_output = nbformat.v4.new_output("error", ename="ValueError", evalue="\x1b[1;31mbad value\x1b[0m")
    code.outputs = [error_output]
    notebook.cells = [code]

    path = tmp_path / "ansi_error.ipynb"
    nbformat.write(notebook, str(path))
    output_path = tmp_path / "ansi_error.md"

    render_markdown(path, output_path)
    content = output_path.read_text(encoding="utf-8")

    assert "\x1b" not in content
    assert "ValueError: bad value" in content


def test_render_markdown_strips_ansi_from_text_plain(tmp_path: Path) -> None:
    notebook = nbformat.v4.new_notebook()
    notebook.metadata["kernelspec"] = {"language": "python"}
    code = nbformat.v4.new_code_cell("df")
    code.outputs = [
        nbformat.v4.new_output(
            "execute_result",
            data={"text/plain": "\x1b[34mDataFrame\x1b[0m[count: \x1b[33m10\x1b[0m]"},
        ),
    ]
    notebook.cells = [code]

    path = tmp_path / "ansi_plain.ipynb"
    nbformat.write(notebook, str(path))
    output_path = tmp_path / "ansi_plain.md"

    render_markdown(path, output_path)
    content = output_path.read_text(encoding="utf-8")

    assert "\x1b" not in content
    assert "DataFrame[count: 10]" in content


# --- Almond REPL echo suppression tests ---


class TestIsAlmondReplEcho:
    """Unit tests for _is_almond_repl_echo detection."""

    def test_simple_val_binding(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {"text/plain": "result: Long = 10L"},
        }
        assert _is_almond_repl_echo(output, "scala") is True

    def test_val_binding_with_ansi(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {
                "text/plain": (
                    "\x1b[36mresult\x1b[39m: \x1b[32mLong\x1b[39m = \x1b[32m10L\x1b[39m"
                )
            },
        }
        assert _is_almond_repl_echo(output, "scala") is True

    def test_qualified_type(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {
                "text/plain": "spark: org.apache.spark.sql.connect.SparkSession = org.apache.spark.sql.connect.SparkSession@5c50e74b"
            },
        }
        assert _is_almond_repl_echo(output, "scala") is True

    def test_import_echo(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {"text/plain": "import com.databricks.connect.DatabricksSession"},
        }
        assert _is_almond_repl_echo(output, "scala") is True

    def test_multi_line_imports_and_val(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {
                "text/plain": (
                    "import $ivy.$\n"
                    "import com.databricks.connect.DatabricksSession\n"
                    "import com.databricks.sdk.core.DatabricksConfig\n"
                    "spark: org.apache.spark.sql.connect.SparkSession = org.apache.spark.sql.connect.SparkSession@5c50e74b"
                )
            },
        }
        assert _is_almond_repl_echo(output, "scala") is True

    def test_not_triggered_for_python(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {"text/plain": "result: Long = 10L"},
        }
        assert _is_almond_repl_echo(output, "python") is False

    def test_not_triggered_for_display_data(self) -> None:
        output = {
            "output_type": "display_data",
            "data": {"text/plain": "result: Long = 10L"},
        }
        assert _is_almond_repl_echo(output, "scala") is False

    def test_not_triggered_when_html_present(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {
                "text/html": "<table>...</table>",
                "text/plain": "result: DataFrame = [id: bigint]",
            },
        }
        assert _is_almond_repl_echo(output, "scala") is False

    def test_not_triggered_when_image_present(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {
                "image/png": "iVBOR...",
                "text/plain": "result: Long = 10L",
            },
        }
        assert _is_almond_repl_echo(output, "scala") is False

    def test_non_repl_text_plain(self) -> None:
        """Arbitrary text that does not match REPL patterns is kept."""
        output = {
            "output_type": "execute_result",
            "data": {"text/plain": "Hello, world!"},
        }
        assert _is_almond_repl_echo(output, "scala") is False

    def test_empty_text_plain(self) -> None:
        output = {
            "output_type": "execute_result",
            "data": {"text/plain": ""},
        }
        assert _is_almond_repl_echo(output, "scala") is False

    def test_res_variable(self) -> None:
        """Almond auto-generated variable names like res0, res1."""
        output = {
            "output_type": "execute_result",
            "data": {"text/plain": "res0: Long = 42L"},
        }
        assert _is_almond_repl_echo(output, "scala") is True


class TestRenderMarkdownScalaReplSuppression:
    """Integration tests for REPL echo suppression in rendered markdown."""

    def test_repl_echo_suppressed_stream_kept(self, tmp_path: Path) -> None:
        """Stream output (println) is kept; REPL echo is suppressed."""
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {"language": "scala"}
        code = nbformat.v4.new_code_cell(
            'val result = spark.range(10).count()\nprintln(s"count=$result")'
        )
        code.outputs = [
            nbformat.v4.new_output("stream", text="count=10\n"),
            nbformat.v4.new_output(
                "execute_result",
                data={
                    "text/plain": "\x1b[36mresult\x1b[39m: \x1b[32mLong\x1b[39m = \x1b[32m10L\x1b[39m"
                },
            ),
        ]
        notebook.cells = [code]

        path = tmp_path / "scala_repl.ipynb"
        nbformat.write(notebook, str(path))
        output_path = tmp_path / "scala_repl.md"

        render_markdown(path, output_path)
        content = output_path.read_text(encoding="utf-8")

        # The println output should be present
        assert "count=10" in content
        # The REPL echo should NOT be present
        assert "result: Long" not in content
        assert "10L" not in content
        # There should be exactly one output block
        assert content.count("```output") == 1

    def test_repl_echo_suppressed_no_other_output(self, tmp_path: Path) -> None:
        """When the only output is a REPL echo, no output block is rendered."""
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {"language": "scala"}
        code = nbformat.v4.new_code_cell("val x = 42")
        code.outputs = [
            nbformat.v4.new_output(
                "execute_result",
                data={"text/plain": "x: Int = 42"},
            ),
        ]
        notebook.cells = [code]

        path = tmp_path / "scala_val.ipynb"
        nbformat.write(notebook, str(path))
        output_path = tmp_path / "scala_val.md"

        render_markdown(path, output_path)
        content = output_path.read_text(encoding="utf-8")

        assert "```output" not in content
        assert "x: Int" not in content

    def test_html_execute_result_preserved_in_scala(self, tmp_path: Path) -> None:
        """execute_result with text/html is kept even in Scala notebooks."""
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {"language": "scala"}
        code = nbformat.v4.new_code_cell("df.show()")
        code.outputs = [
            nbformat.v4.new_output(
                "execute_result",
                data={
                    "text/html": "<table><tr><td>1</td></tr></table>",
                    "text/plain": "df: DataFrame = [id: bigint]",
                },
            ),
        ]
        notebook.cells = [code]

        path = tmp_path / "scala_html.ipynb"
        nbformat.write(notebook, str(path))
        output_path = tmp_path / "scala_html.md"

        render_markdown(path, output_path)
        content = output_path.read_text(encoding="utf-8")

        assert "```output" in content
        assert "<table>" in content

    def test_python_execute_result_not_affected(self, tmp_path: Path) -> None:
        """Python notebooks are not affected by Scala REPL suppression."""
        notebook = nbformat.v4.new_notebook()
        notebook.metadata["kernelspec"] = {"language": "python"}
        code = nbformat.v4.new_code_cell("x = 42\nx")
        code.outputs = [
            nbformat.v4.new_output(
                "execute_result",
                data={"text/plain": "result: Long = 42"},
            ),
        ]
        notebook.cells = [code]

        path = tmp_path / "python_result.ipynb"
        nbformat.write(notebook, str(path))
        output_path = tmp_path / "python_result.md"

        render_markdown(path, output_path)
        content = output_path.read_text(encoding="utf-8")

        assert "```output" in content
        assert "result: Long = 42" in content
