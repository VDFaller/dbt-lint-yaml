#!/usr/bin/env python3
"""Apply dbt model column description updates using ruamel.yaml."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

try:
    from ruamel.yaml import YAML
except ImportError:  # pragma: no cover - import error should bubble up clearly
    print(
        "ruamel.yaml is required to apply YAML updates. Install it with `pip install ruamel.yaml`.",
        file=sys.stderr,
    )
    raise


class PatchError(Exception):
    """Raised when the YAML structure does not match expectations."""


def load_payload() -> Dict[str, Any]:
    try:
        return json.load(sys.stdin)
    except json.JSONDecodeError as exc:  # pragma: no cover - input contract violation
        raise PatchError(f"Invalid JSON payload: {exc}") from exc


def ensure_sequence(value: Any, name: str) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    raise PatchError(f"Expected `{name}` to be a sequence, found {type(value).__name__}")


def find_model(models: List[Any], model_name: str) -> Dict[str, Any]:
    for entry in models:
        if isinstance(entry, dict) and entry.get("name") == model_name:
            return entry
    raise PatchError(f"Model `{model_name}` not found in YAML")


def ensure_column(columns: List[Any], column_name: str) -> Dict[str, Any]:
    for entry in columns:
        if isinstance(entry, dict) and entry.get("name") == column_name:
            return entry
    new_column: Dict[str, Any] = {"name": column_name}
    columns.append(new_column)
    return new_column


def apply_updates(payload: Dict[str, Any]) -> List[str]:
    patch_path = Path(payload["patch_path"])
    model_name = payload["model_name"]
    column_changes = payload.get("column_changes", [])
    model_description = payload.get("model_description") if "model_description" in payload else None
    if not column_changes and model_description is None:
        return []

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)

    if not patch_path.exists():
        raise PatchError(f"YAML file `{patch_path}` not found")

    with patch_path.open("r", encoding="utf-8") as handle:
        document = yaml.load(handle) or {}

    models = ensure_sequence(document.get("models"), "models")
    model = find_model(models, model_name)
    columns = ensure_sequence(model.get("columns"), "columns")
    model["columns"] = columns

    updated_columns: List[str] = []
    model_mutated = False

    if "model_description" in payload:
        if model_description is None:
            if "description" in model:
                model.pop("description", None)
                model_mutated = True
        else:
            if model.get("description") != model_description:
                model["description"] = model_description
                model_mutated = True

    for change in column_changes:
        column_name = change.get("column_name")
        if not column_name:
            continue
        column = ensure_column(columns, column_name)
        new_description = change.get("new_description")
        current_description = column.get("description")
        if new_description is None:
            if "description" in column:
                column.pop("description", None)
                updated_columns.append(column_name)
        elif current_description != new_description:
            column["description"] = new_description
            updated_columns.append(column_name)

    if updated_columns or model_mutated:
        with patch_path.open("w", encoding="utf-8") as handle:
            yaml.dump(document, handle)

    return updated_columns


def main() -> int:
    try:
        payload = load_payload()
        updated_columns = apply_updates(payload)
    except PatchError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except FileNotFoundError as exc:  # pragma: no cover - propagates to stderr
        print(str(exc), file=sys.stderr)
        return 1

    json.dump({"updated_columns": updated_columns}, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
