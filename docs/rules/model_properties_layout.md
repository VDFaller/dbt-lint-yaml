# Model properties layout

**rule_id**: model_properties_layout

## Summary
- Ensures model properties YAML files follow the configured layout (per-model files or a single per-directory file).

## What it checks
- Compares the manifest patch path for each model against the expected path based on `model_properties_layout`.
- When the layout is `per_directory`, all models in the same directory must share a single `_directory__models.yml` file.
- When the layout is `per_model`, each model must have its own `<model_name>.yml` file.

## Default
- Disabled. Set `model_properties_layout` and include the selector to enable it.

## Autofixable with `--fix`?
- Yes. When `--fix` is enabled and the selector is fixable, properties files are rewritten to the expected locations and merged or split as needed.

## Implementation (for contributors)
- Source: [src/check/models.rs](/src/check/models.rs). The check reads and writes YAML using the Rust writeback helpers.

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)
- Default configuration: [docs/default-dbt-lint.toml](/docs/default-dbt-lint.toml)
