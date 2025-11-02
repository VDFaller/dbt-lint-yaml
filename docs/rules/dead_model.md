# Dead model

**rule_id**: dead_model

## Summary
- Detects models that have no downstream model dependencies (i.e., "dead" models).

## What it checks
- Flags models that are not referenced by any other model in the project's DAG.
- Tests and unit tests do not count as downstream model dependencies.

## Default
- Disabled by default.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](/src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)
