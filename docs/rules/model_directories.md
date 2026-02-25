# Model directories

**rule_id**: `model_directories`

## Summary
- A model is not in the subdirectory that matches its name prefix.

## What it checks
- Walks upward from each model's SQL file and finds the nearest directory whose name matches any configured tier (`staging_directory`, `intermediate_directory`, or `mart_directory`).
- If that nearest tier directory does not equal the model's expected directory (determined by its name prefix), the check fails.
- Models whose names don't match any configured prefix are classified as `Unknown` and skipped entirely.

## Why this matters
- Consistent directory structure makes it easy to locate models and understand the data flow at a glance. A staging model buried inside `marts/` violates the layered architecture convention and can confuse downstream consumers.

## Default
- Not enabled by default. Add `model_directories` to your `select` list to turn it on.

## Autofixable with `--fix`?
- Yes. The autofix moves the `.sql` file (and its co-located YAML properties file, if any) to the correct tier directory.

### Autofix algorithm
1. Walk upward from the file's directory looking for a tier directory.
2. If the **expected** tier directory is found in the walk, place the file directly under it (preserving any subdirectory nesting below it).
3. Otherwise, replace the nearest **wrong** tier directory with the expected one, preserving any subdirectory structure below it.

**Example — staging model nested under a source subdirectory in the wrong tier:**

```
models/
  marts/
    stripe/
      stg_stripe__orders.sql   ← wrong, nearest tier dir is "marts"
```

After `--fix`:

```
models/
  staging/
    stripe/
      stg_stripe__orders.sql   ← correct, stripe/ subdir preserved
```

## Configs that affect this rule

| Config key | Default | Description |
|---|---|---|
| `staging_directory` | `"staging"` | Expected directory name for staging models |
| `staging_prefixes` | `["stg_", "base_"]` | Prefixes that identify staging models |
| `intermediate_directory` | `"intermediate"` | Expected directory name for intermediate models |
| `intermediate_prefixes` | `["int_"]` | Prefixes that identify intermediate models |
| `mart_directory` | `"marts"` | Expected directory name for mart models |
| `mart_prefixes` | `["dim_", "fct_"]` | Prefixes that identify mart models |

Prefix matching uses **longest-match wins**: if a model name matches prefixes from multiple lists, the longest matching prefix wins. This lets you override a broad prefix with a more specific one (e.g. adding `stg_stripe_` to `mart_prefixes` overrides the shorter `stg_` in `staging_prefixes`).

## Notes
- Classification is always by prefix — the model's current directory is never used to infer its type.
- This check runs before `model_properties_layout` in the pipeline so that the layout check computes YAML paths based on the corrected SQL location.

## Implementation (for contributors)
- Source: [src/check/models.rs](/src/check/models.rs) → `check_model_directories`

## See also
- [source_directories](source_directories.md) — the companion check for source YAML files
- General configuration: [docs/configuration.md](/docs/configuration.md)
