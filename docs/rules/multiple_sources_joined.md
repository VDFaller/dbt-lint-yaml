# Multiple sources joined

**rule_id**: multiple_sources_joined

## Summary
- Detects models that join multiple distinct sources together.

## What it checks
- Flags SQL that merges rows from two or more different `source()` relations into a single model.

## Why this matters
- Joining many unrelated sources in one model can increase coupling and complicate lineage and ownership.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
