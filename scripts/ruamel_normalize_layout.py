#!/usr/bin/env python3
"""Normalize dbt model properties layout while preserving YAML formatting."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from yaml_utils import (  # noqa: E402
    YamlHelperError,
    init_yaml,
    load_document,
    load_request,
    write_or_remove,
)

try:
    from ruamel.yaml import YAML
    from ruamel.yaml.comments import CommentedMap, CommentedSeq
except ImportError:  # pragma: no cover - surface dependency error clearly
    print(
        "ruamel.yaml is required to apply YAML updates. Install it with `pip install ruamel.yaml`.",
        file=sys.stderr,
    )
    raise


def ensure_model_sequence(doc: CommentedMap) -> CommentedSeq:
    models = doc.get("models")
    if models is None:
        models = CommentedSeq()
        doc["models"] = models
    elif not isinstance(models, list):
        raise YamlHelperError("Expected `models` key to contain a sequence")
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
    raise YamlHelperError(f"Model `{model_name}` not found in `{source_path}`")


def upsert_model(doc: CommentedMap, model: CommentedMap, model_name: str) -> None:
    models = ensure_model_sequence(doc)
    for index, entry in enumerate(models):
        if isinstance(entry, dict) and entry.get("name") == model_name:
            models[index] = model
            break
    else:
        models.append(model)


def normalize_to_directory(
    yaml: YAML,
    current_path: Path,
    expected_path: Path,
    model_name: str,
) -> bool:
    if not current_path.exists():
        raise YamlHelperError(f"YAML file `{current_path}` not found")

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

    except YamlHelperError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except FileNotFoundError as exc:  # pragma: no cover - bubbled up for clarity
        print(str(exc), file=sys.stderr)
        return 1

    json.dump({"mutated": mutated}, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
