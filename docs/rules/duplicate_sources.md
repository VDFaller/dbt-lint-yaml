# Duplicate sources

**rule_id**: duplicate_sources

## Summary
- Detects duplicate source declarations that map to the same logical relation.

## What it checks
- Flags when two or more source declarations would refer to the same database/schema/name combination.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/sources.rs](/src/check/sources.rs)

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)