# Root models

**rule_id**: root_models

## Summary
- Identifies models that are root-level (no upstream models) and likely represent ingestion/staging.

## What it checks
- Flags models with no model upstream dependencies.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](/src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)