#!/usr/bin/env python3
"""Move a single dbt source block between YAML files using ruamel.yaml.

Preserves comments and formatting in both the source and destination files.

Input (stdin JSON):
  {
    "old_patch_path": "<absolute path>",
    "source_name":    "<source name to extract>",
    "new_patch_path": "<absolute path to write to>"
  }

Output (stdout JSON):
  {"applied": true}   -- source block was moved
  {"applied": false}  -- source not found or paths are identical
"""

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


def remove_source(doc: CommentedMap, source_name: str) -> CommentedMap | None:
    """Remove and return the named source block from doc, or None if not found."""
    sources = doc.get("sources")
    if not isinstance(sources, list):
        return None

    for index, entry in enumerate(sources):
        if isinstance(entry, dict) and entry.get("name") == source_name:
            source = sources.pop(index)
            if not sources:
                doc.pop("sources", None)
            if not isinstance(source, CommentedMap):
                source = CommentedMap(source)
            return source

    return None


def upsert_source(doc: CommentedMap, source: CommentedMap, source_name: str) -> None:
    """Insert or replace the named source block in doc."""
    sources = doc.get("sources")
    if sources is None:
        sources = CommentedSeq()
        doc["sources"] = sources

    for index, entry in enumerate(sources):
        if isinstance(entry, dict) and entry.get("name") == source_name:
            sources[index] = source
            return

    sources.append(source)


def move_source(
    yaml: YAML,
    old_path: Path,
    source_name: str,
    new_path: Path,
) -> bool:
    if not old_path.exists():
        raise YamlHelperError(f"YAML file `{old_path}` not found")

    if old_path == new_path:
        return False

    old_doc = load_document(old_path, yaml)
    source = remove_source(old_doc, source_name)
    if source is None:
        return False

    new_doc = load_document(new_path, yaml)
    upsert_source(new_doc, source, source_name)

    write_or_remove(old_path, yaml, old_doc)
    write_or_remove(new_path, yaml, new_doc)
    return True


def main() -> int:
    try:
        payload = load_request()
        yaml = init_yaml()

        old_patch_path = Path(payload["old_patch_path"])
        source_name = payload["source_name"]
        new_patch_path = Path(payload["new_patch_path"])

        applied = move_source(yaml, old_patch_path, source_name, new_patch_path)

    except YamlHelperError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except FileNotFoundError as exc:  # pragma: no cover - bubbled up for clarity
        print(str(exc), file=sys.stderr)
        return 1

    json.dump({"applied": applied}, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
