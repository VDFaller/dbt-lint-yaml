# Source fanout

**rule_id**: source_fanout

## Summary
- Flags source tables that are used widely across many models (high fanout).

## What it checks
- Counts how many models reference a source and flags those above an implicit threshold.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/sources.rs](src/check/sources.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
