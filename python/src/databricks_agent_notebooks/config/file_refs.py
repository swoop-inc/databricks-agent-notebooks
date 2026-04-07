"""Resolve ``-FILE`` key conventions in parsed TOML dicts.

Walks a nested dict (typically from ``pyproject.toml``) and replaces keys
ending in ``-FILE`` with the contents of the referenced files.  For example,
``add_cell-FILE = "cell.py"`` becomes ``add_cell = "<contents of cell.py>"``.

This runs as a pre-processing step *before* key normalization, since
normalization converts hyphens to underscores and would destroy the
``-FILE`` suffix pattern.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

__all__ = ["FileRefError", "resolve_file_refs"]

_FILE_SUFFIX = "-FILE"


class FileRefError(Exception):
    """Raised when a ``-FILE`` key cannot be resolved.

    Attributes:
        key: Dotted path to the offending key
             (e.g. ``"environments.staging.add_cell-FILE"``).
        file_path: Resolved filesystem path, if applicable.
        reason: Human-readable description of the problem.
    """

    def __init__(
        self,
        *,
        key: str,
        file_path: Path | None = None,
        reason: str,
    ) -> None:
        self.key = key
        self.file_path = file_path
        self.reason = reason
        parts = [f"{key}: {reason}"]
        if file_path is not None:
            parts.append(f"({file_path})")
        super().__init__(" ".join(parts))


def _resolve_single_file(path: Path, key_path: str) -> str:
    """Read *path*, validate it is a text file, and return its contents.

    Binary detection checks for null bytes in the first 8192 bytes,
    matching the heuristic used by git.  OS-level errors (e.g.
    ``PermissionError``) propagate unwrapped.
    """
    if not path.exists():
        raise FileRefError(key=key_path, file_path=path, reason="file does not exist")
    if path.is_dir():
        raise FileRefError(
            key=key_path, file_path=path, reason="path is a directory, not a file"
        )
    raw = path.read_bytes()
    if b"\x00" in raw[:8192]:
        raise FileRefError(
            key=key_path,
            file_path=path,
            reason="file appears to be binary (contains null bytes)",
        )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        raise FileRefError(
            key=key_path, file_path=path, reason="file is not valid UTF-8 text"
        )


def _walk(node: dict[str, Any], base_dir: Path, key_path: str) -> dict[str, Any]:
    """Recursively resolve ``-FILE`` keys in *node*."""
    # -- Ambiguity detection: scan all keys before processing any ----------
    file_keys = {k for k in node if k.endswith(_FILE_SUFFIX)}
    for fk in file_keys:
        base_key = fk[: -len(_FILE_SUFFIX)]
        if not base_key:
            dotted = f"{key_path}.{fk}" if key_path else fk
            raise FileRefError(
                key=dotted, reason="key '-FILE' has no base key name"
            )
        if base_key in node:
            dotted = f"{key_path}.{base_key}" if key_path else base_key
            raise FileRefError(
                key=dotted,
                reason=(
                    f"ambiguous: both '{base_key}' and '{base_key}{_FILE_SUFFIX}' "
                    f"exist at the same level"
                ),
            )

    result: dict[str, Any] = {}
    for key, value in node.items():
        dotted = f"{key_path}.{key}" if key_path else key

        if key in file_keys:
            base_key = key[: -len(_FILE_SUFFIX)]
            resolved_key = f"{key_path}.{base_key}" if key_path else base_key

            if isinstance(value, str):
                path = base_dir / value if not Path(value).is_absolute() else Path(value)
                result[base_key] = _resolve_single_file(path.resolve(), resolved_key)
            elif isinstance(value, list):
                contents: list[str] = []
                for i, elem in enumerate(value):
                    if not isinstance(elem, str):
                        raise FileRefError(
                            key=dotted,
                            reason=(
                                f"list element at index {i} is not a string, "
                                f"got {type(elem).__name__}"
                            ),
                        )
                    path = (
                        base_dir / elem
                        if not Path(elem).is_absolute()
                        else Path(elem)
                    )
                    contents.append(
                        _resolve_single_file(
                            path.resolve(), f"{resolved_key}[{i}]"
                        )
                    )
                result[base_key] = contents
            else:
                raise FileRefError(
                    key=dotted,
                    reason=(
                        f"value must be a string or list of strings, "
                        f"got {type(value).__name__}"
                    ),
                )
        elif isinstance(value, dict):
            result[key] = _walk(value, base_dir, dotted)
        else:
            result[key] = value

    return result


def resolve_file_refs(data: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Resolve all ``-FILE`` keys in *data*, reading referenced files.

    *base_dir* is the anchor for relative paths -- typically the directory
    containing the config file (``pyproject.toml`` or notebook) that
    produced *data*.

    Walks *data* recursively.  For any key ending in ``-FILE``
    (case-sensitive, literal dash required):

    1. Validates the value is ``str`` or ``list[str]``.
    2. Resolves each path relative to *base_dir* (absolute paths used as-is).
    3. Reads the file -- must exist and be valid UTF-8 text (no null bytes).
    4. Stores the contents under the key with ``-FILE`` stripped.
    5. Removes the original ``-FILE`` key.

    Returns a new dict; the input is not mutated.
    """
    return _walk(data, base_dir, "")
