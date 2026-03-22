"""Tests for notebook rendering."""

from __future__ import annotations

import base64
import sys
from pathlib import Path
from unittest.mock import patch

import nbformat

from databricks_agent_notebooks.execution.rendering import render, render_all, render_html, render_markdown


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
