#!/usr/bin/env python3
"""Apply dbt model column description updates using ruamel.yaml.

Supports batch processing: multiple models in a single file can be updated
in one call, reducing process overhead. The payload should contain:
- patch_path: Path to the YAML file
- models: List of model updates, each with:
  - model_name: Name of the model
  - column_changes: List of column updates
  - model_description: Optional model description update
"""

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


def apply_model_update(
    yaml_document: Dict[str, Any], model_update: Dict[str, Any]
) -> List[str]:
    """Apply updates for a single model within an already-loaded YAML document.
    
    Args:
        yaml_document: The loaded YAML document (e.g., {"models": [...]})
        model_update: Update spec with model_name, column_changes, model_description
    
    Returns:
        List of updated column names
    """
    model_name = model_update.get("model_name")
    column_changes = model_update.get("column_changes", [])
    model_description = model_update.get("model_description")

    if not column_changes and "model_description" not in model_update:
        return []

    models = ensure_sequence(yaml_document.get("models"), "models")
    model = find_model(models, model_name)
    columns = ensure_sequence(model.get("columns"), "columns")
    model["columns"] = columns

    updated_columns: List[str] = []

    # Apply model description change if present
    if "model_description" in model_update:
        if model_description is None:
            if "description" in model:
                model.pop("description", None)
        else:
            if model.get("description") != model_description:
                model["description"] = model_description

    # Apply column changes
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

    return updated_columns


def apply_updates(payload: Dict[str, Any]) -> Dict[str, List[str]]:
    """Apply batch updates to a single YAML file.
    
    Args:
        payload: Dict with:
            - patch_path: Path to YAML file
            - models: List of model updates
    
    Returns:
        Dict mapping model_name to list of updated column names
    """
    patch_path = Path(payload["patch_path"])
    model_updates = payload.get("models", [])

    if not model_updates:
        return {}

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)

    if not patch_path.exists():
        raise PatchError(f"YAML file `{patch_path}` not found")

    with patch_path.open("r", encoding="utf-8") as handle:
        document = yaml.load(handle) or {}

    results: Dict[str, List[str]] = {}
    file_mutated = False

    # Process all models in a single pass through the document
    for model_update in model_updates:
        updated_cols = apply_model_update(document, model_update)
        if updated_cols:
            file_mutated = True
            results[model_update.get("model_name", "")] = updated_cols

    # Write once if anything changed
    if file_mutated:
        with patch_path.open("w", encoding="utf-8") as handle:
            yaml.dump(document, handle)

    return results


def main() -> int:
    try:
        payload = load_payload()
        results = apply_updates(payload)
    except PatchError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except FileNotFoundError as exc:  # pragma: no cover - propagates to stderr
        print(str(exc), file=sys.stderr)
        return 1

    json.dump({"results": results}, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
