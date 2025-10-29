# Unused sources

**rule_id**: unused_sources

## Summary
- Detects sources defined in YAML that are not referenced by any model.

## What it checks
- Flags source definitions that are not used anywhere in the project.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/sources.rs](/src/check/sources.rs)

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)