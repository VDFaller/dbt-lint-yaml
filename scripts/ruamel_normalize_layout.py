#!/usr/bin/env python3
"""Normalize dbt model properties layout while preserving YAML formatting."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

try:
    from ruamel.yaml import YAML
    from ruamel.yaml.comments import CommentedMap, CommentedSeq
except ImportError:  # pragma: no cover - surface dependency error clearly
    print(
        "ruamel.yaml is required to apply YAML updates. Install it with `pip install ruamel.yaml`.",
        file=sys.stderr,
    )
    raise


class LayoutError(Exception):
    """Raised when the YAML structure does not match expectations."""


def load_request() -> Dict[str, Any]:
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError as exc:  # pragma: no cover - payload contract violation
        raise LayoutError(f"Invalid JSON payload: {exc}") from exc


def init_yaml() -> YAML:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def load_document(path: Path, yaml: YAML) -> CommentedMap:
    if not path.exists():
        return CommentedMap()

    with path.open("r", encoding="utf-8") as handle:
        document = yaml.load(handle) or CommentedMap()

    if not isinstance(document, CommentedMap):
        raise LayoutError(f"YAML document `{path}` is not a mapping")

    return document


def ensure_model_sequence(doc: CommentedMap) -> CommentedSeq:
    models = doc.get("models")
    if models is None:
        models = CommentedSeq()
        doc["models"] = models
    elif not isinstance(models, list):
        raise LayoutError("Expected `models` key to contain a sequence")
    return models  # type: ignore[return-value]


def remove_model(doc: CommentedMap, model_name: str, source_path: Path) -> CommentedMap:
    models = ensure_model_sequence(doc)
    for index, entry in enumerate(models):
        if isinstance(entry, dict) and entry.get("name") == model_name:
            model = models.pop(index)
            if not models:
                doc.pop("models", None)
            if not isinstance(model, CommentedMap):
                model = CommentedMap(model)
            return model
    raise LayoutError(f"Model `{model_name}` not found in `{source_path}`")


def upsert_model(doc: CommentedMap, model: CommentedMap, model_name: str) -> None:
    models = ensure_model_sequence(doc)
    for index, entry in enumerate(models):
        if isinstance(entry, dict) and entry.get("name") == model_name:
            models[index] = model
            break
    else:
        models.append(model)


def document_is_empty(doc: CommentedMap) -> bool:
    models = doc.get("models")
    sources = doc.get("sources")
    other_keys = [key for key in doc.keys() if key not in {"models", "sources"}]

    models_empty = not models
    sources_empty = not sources

    return models_empty and sources_empty and not other_keys


def write_or_remove(path: Path, yaml: YAML, doc: CommentedMap) -> None:
    if document_is_empty(doc):
        if path.exists():
            path.unlink()
        return

    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        yaml.dump(doc, handle)


def normalize_to_directory(
    yaml: YAML,
    current_path: Path,
    expected_path: Path,
    model_name: str,
) -> bool:
    if not current_path.exists():
        raise LayoutError(f"YAML file `{current_path}` not found")

    if current_path == expected_path:
        return False

    source_doc = load_document(current_path, yaml)
    model = remove_model(source_doc, model_name, current_path)

    target_doc = load_document(expected_path, yaml)
    upsert_model(target_doc, model, model_name)

    write_or_remove(current_path, yaml, source_doc)
    write_or_remove(expected_path, yaml, target_doc)
    return True


def main() -> int:
    try:
        payload = load_request()
        yaml = init_yaml()

        current_path = Path(payload["current_patch"])
        expected_path = Path(payload["expected_patch"])
        model_name = payload["model_name"]

        mutated = normalize_to_directory(yaml, current_path, expected_path, model_name)

    except LayoutError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except FileNotFoundError as exc:  # pragma: no cover - bubbled up for clarity
        print(str(exc), file=sys.stderr)
        return 1

    json.dump({"mutated": mutated}, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
