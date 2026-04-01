"""Render executed notebooks to Markdown and/or HTML.

Converts ``.ipynb`` notebooks into human-readable output formats.
Markdown rendering is done in-process by walking the notebook cell
structure; HTML rendering delegates to ``jupyter nbconvert --to html``.

Cells tagged with ``agent_notebook_injected`` metadata are silently
omitted from rendered output so that framework-generated setup code
does not clutter the final document.
"""

from __future__ import annotations

import base64
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import nbformat

_ANSI_RE = re.compile(
    r"\x1b"
    r"(?:"
    r"\[[0-9;]*[a-zA-Z]"       # CSI sequences: ESC [ ... letter
    r"|"
    r"\][^\x07\x1b]*"          # OSC sequences: ESC ] ... (terminated by BEL or ST)
    r"(?:\x07|\x1b\\)"         # ... BEL or ESC \ terminator
    r"|"
    r"\([A-Z]"                 # Character set selection: ESC ( letter
    r")"
)

# Almond (Scala REPL) echoes every expression result as lines like:
#   res0: Long = 10L
#   result: org.apache.spark.sql.DataFrame = [id: bigint]
#   import com.databricks.connect.DatabricksSession
# These are noise in rendered output — the user's intentional output goes
# through println (stream) or rich display (display_data).
_ALMOND_REPL_LINE_RE = re.compile(
    r"^(?:"
    r"\w[\w.]*\s*:\s*.+=\s*.+"    # identifier: Type = value
    r"|"
    r"import\s+.+"                # import statement echo
    r")$"
)


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences (CSI codes) from *text*."""
    return _ANSI_RE.sub("", text)


def _is_almond_repl_echo(output: dict, language: str) -> bool:
    """Return True if *output* is an Almond REPL value echo to suppress.

    The Almond Scala kernel attaches an ``execute_result`` to every cell
    containing the REPL-style echo of variable bindings and import
    statements (e.g. ``result: Long = 10L``).  When the output carries
    only ``text/plain`` (no ``text/html`` or images), and every line
    matches the Almond echo pattern, we treat it as noise and skip it
    during rendering.
    """
    if language != "scala":
        return False
    if output.get("output_type") != "execute_result":
        return False
    data = output.get("data", {})
    # If richer formats are present, the result is intentional display output.
    if "text/html" in data or "image/png" in data:
        return False
    plain = data.get("text/plain", "")
    if not plain:
        return False
    cleaned = _strip_ansi(plain).strip()
    if not cleaned:
        return False
    return all(
        _ALMOND_REPL_LINE_RE.match(line) for line in cleaned.splitlines()
    )


def _detect_language(nb: nbformat.NotebookNode) -> str:
    """Extract the code language from notebook kernel metadata."""
    kernelspec = nb.metadata.get("kernelspec", {})
    lang = kernelspec.get("language", "")
    if lang:
        return lang
    # Fall back to language_info if kernelspec doesn't have it
    lang_info = nb.metadata.get("language_info", {})
    return lang_info.get("name", "python")


def render(
    notebook_path: Path,
    output_dir: Path,
    fmt: str = "all",
) -> dict[str, Path]:
    """Render a notebook to one or more output formats.

    Parameters
    ----------
    notebook_path:
        Path to the executed ``.ipynb`` file.
    output_dir:
        Directory where rendered output files are written.
    fmt:
        One of ``"all"``, ``"md"``, or ``"html"``.

    Returns
    -------
    dict[str, Path]
        Mapping of format name to output file path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = notebook_path.stem

    if fmt == "all":
        return render_all(notebook_path, output_dir)
    if fmt == "md":
        md_path = output_dir / f"{stem}.md"
        return {"md": render_markdown(notebook_path, md_path)}
    if fmt == "html":
        html_path = output_dir / f"{stem}.html"
        return {"html": render_html(notebook_path, html_path)}

    msg = f"Unknown format: {fmt!r} (expected 'all', 'md', or 'html')"
    raise ValueError(msg)


def render_all(
    notebook_path: Path,
    output_dir: Path,
) -> dict[str, Path]:
    """Render a notebook to both Markdown and HTML.

    Parameters
    ----------
    notebook_path:
        Path to the executed ``.ipynb`` file.
    output_dir:
        Directory where rendered output files are written.

    Returns
    -------
    dict[str, Path]
        ``{"md": <md_path>, "html": <html_path>}``
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = notebook_path.stem

    md_path = output_dir / f"{stem}.md"
    html_path = output_dir / f"{stem}.html"

    return {
        "md": render_markdown(notebook_path, md_path),
        "html": render_html(notebook_path, html_path),
    }


def render_markdown(notebook_path: Path, output_path: Path) -> Path:
    """Render a notebook to Markdown.

    Walks the cell structure and emits fenced code blocks for code cells,
    raw text for markdown cells, and formatted output blocks.  Cells with
    ``agent_notebook_injected`` metadata are silently skipped.

    Parameters
    ----------
    notebook_path:
        Path to the ``.ipynb`` file.
    output_path:
        Where to write the Markdown output.

    Returns
    -------
    Path
        The ``output_path``, for chaining convenience.
    """
    nb = nbformat.read(str(notebook_path), as_version=4)
    language = _detect_language(nb)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Asset directory for embedded images
    assets_dir = output_path.parent / f"{output_path.stem}_assets"

    parts: list[str] = []

    for cell_idx, cell in enumerate(nb.cells):
        # Skip injected cells
        if cell.metadata.get("agent_notebook_injected", False):
            continue

        if cell.cell_type == "markdown":
            parts.append(cell.source)
            parts.append("")  # blank line separator

        elif cell.cell_type == "code":
            # Code source
            parts.append(f"```{language}")
            parts.append(cell.source)
            parts.append("```")
            parts.append("")

            # Cell outputs — accumulate consecutive stream outputs into
            # a single fenced block to avoid cluttered output from
            # multiple print() calls.
            pending_stream: list[str] = []

            def _flush_stream() -> None:
                if pending_stream:
                    merged = "".join(pending_stream).rstrip("\n")
                    parts.append("```output")
                    parts.append(merged)
                    parts.append("```")
                    parts.append("")
                    pending_stream.clear()

            for out_idx, output in enumerate(cell.get("outputs", [])):
                output_type = output.get("output_type", "")

                if output_type == "stream":
                    pending_stream.append(_strip_ansi(output.get("text", "")))

                elif output_type in ("execute_result", "display_data"):
                    _flush_stream()
                    # Skip Almond REPL value echoes in Scala notebooks
                    if _is_almond_repl_echo(output, language):
                        continue
                    # Text output
                    text = None
                    data = output.get("data", {})
                    if "image/png" in data:
                        # Save image
                        assets_dir.mkdir(parents=True, exist_ok=True)
                        img_name = f"cell_{cell_idx}_output_{out_idx}.png"
                        img_path = assets_dir / img_name
                        img_bytes = base64.b64decode(data["image/png"])
                        img_path.write_bytes(img_bytes)
                        rel_path = f"{assets_dir.name}/{img_name}"
                        parts.append(f"![output]({rel_path})")
                        parts.append("")
                        continue
                    elif "text/html" in data:
                        text = data["text/html"]
                    elif "text/plain" in data:
                        text = _strip_ansi(data["text/plain"])

                    if text is not None:
                        parts.append("```output")
                        parts.append(text.rstrip("\n"))
                        parts.append("```")
                        parts.append("")

                elif output_type == "error":
                    _flush_stream()
                    ename = _strip_ansi(output.get("ename", "Error"))
                    evalue = _strip_ansi(output.get("evalue", ""))
                    parts.append("```error")
                    parts.append(f"{ename}: {evalue}")
                    parts.append("```")
                    parts.append("")

            _flush_stream()

    output_path.write_text("\n".join(parts), encoding="utf-8")
    return output_path


def render_html(notebook_path: Path, output_path: Path) -> Path:
    """Render a notebook to HTML via ``jupyter nbconvert``.

    Injected cells (tagged with ``agent_notebook_injected``) are stripped
    before conversion so framework setup code does not appear in the output.

    Parameters
    ----------
    notebook_path:
        Path to the ``.ipynb`` file.
    output_path:
        Where to write the HTML output.

    Returns
    -------
    Path
        The ``output_path``.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Strip injected cells before passing to nbconvert
    nb = nbformat.read(str(notebook_path), as_version=4)
    nb.cells = [c for c in nb.cells if not c.metadata.get("agent_notebook_injected", False)]

    with tempfile.TemporaryDirectory() as tmp_dir:
        filtered_path = Path(tmp_dir) / f"{output_path.stem}.ipynb"
        nbformat.write(nb, str(filtered_path))

        subprocess.run(  # noqa: S603
            [
                sys.executable,
                "-m",
                "jupyter",
                "nbconvert",
                "--to",
                "html",
                f"--output={output_path}",
                str(filtered_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    return output_path
