# Source directories

**rule_id**: `source_directories`

## Summary
- A source YAML definition is not in the subdirectory that matches its source name.

## What it checks
- Walks upward from each source's YAML patch path to find the configured `staging_directory`.
- The immediate subdirectory of `staging_directory` that contains the YAML file must equal the source's name (e.g. `staging/stripe/` for a source named `stripe`).
- Sources not located under `staging_directory` at all are skipped.

## Why this matters
- Keeping each source's YAML in its own `staging/<source_name>/` directory makes it easy to find source definitions and matches the dbt Labs project structure convention. A source defined in a sibling directory (or loose in `staging/`) is easy to lose and hard to maintain.

## Default
- Not enabled by default. Add `source_directories` to your `select` list to turn it on.

## Autofixable with `--fix`?
- Yes. The autofix extracts the source block from its current YAML file and writes it to `<staging_directory>/<source_name>/_<source_name>__sources.yml`, creating the directory if needed. If the original file contained only that source block, it is deleted; otherwise the remaining sources stay in place.

**Example — two sources sharing a file at the staging root:**

```
models/
  staging/
    __sources.yml   ← contains both "ecom" and "analytics" source blocks
```

After `--fix`:

```
models/
  staging/
    ecom/
      _ecom__sources.yml      ← ecom block extracted here
    analytics/
      _analytics__sources.yml ← analytics block extracted here
    # __sources.yml deleted (now empty)
```

**Example — source defined under the wrong sibling directory:**

```
models/
  staging/
    source_1/
      _source_1__sources.yml   ← also contains "source_2" definition (wrong)
```

After `--fix`:

```
models/
  staging/
    source_1/
      _source_1__sources.yml   ← only source_1 remains
    source_2/
      _source_2__sources.yml   ← source_2 extracted here
```

## Configs that affect this rule

| Config key | Default | Description |
|---|---|---|
| `staging_directory` | `"staging"` | Directory under which source YAMLs must live |

## Notes
- Tests live alongside the source YAML they belong to, so moving a source YAML automatically moves its tests with it.

## Implementation (for contributors)
- Source: [src/check/sources.rs](/src/check/sources.rs) → `check_source_directories`
- Writeback: [src/writeback/mod.rs](/src/writeback/mod.rs) → `apply_source_changes`

## See also
- [model_directories](model_directories.md) — the companion check for model SQL files
- General configuration: [docs/configuration.md](/docs/configuration.md)
