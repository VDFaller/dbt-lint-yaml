"""Shared YAML utility helpers for dbt-lint-yaml ruamel.yaml scripts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

try:
    from ruamel.yaml import YAML
    from ruamel.yaml.comments import CommentedMap
except ImportError:  # pragma: no cover - surface dependency error clearly
    print(
        "ruamel.yaml is required to apply YAML updates. Install it with `pip install ruamel.yaml`.",
        file=sys.stderr,
    )
    raise


class YamlHelperError(Exception):
    """Raised when the YAML structure does not match expectations."""


def load_request() -> Dict[str, Any]:
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError as exc:  # pragma: no cover - payload contract violation
        raise YamlHelperError(f"Invalid JSON payload: {exc}") from exc


def init_yaml() -> YAML:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def load_document(path: Path, yaml: YAML) -> CommentedMap:
    """Load a YAML file into a CommentedMap, returning empty map if file doesn't exist."""
    if not path.exists():
        return CommentedMap()

    with path.open("r", encoding="utf-8") as handle:
        document = yaml.load(handle) or CommentedMap()

    if not isinstance(document, CommentedMap):
        raise YamlHelperError(f"YAML document `{path}` is not a mapping")

    return document


def document_is_empty(doc: CommentedMap) -> bool:
    """True when no models or sources remain."""
    return not doc.get("models") and not doc.get("sources")


def write_or_remove(path: Path, yaml: YAML, doc: CommentedMap) -> None:
    """Write doc to path, or delete path if the document is empty."""
    if document_is_empty(doc):
        if path.exists():
            path.unlink()
        return

    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        yaml.dump(doc, handle)
