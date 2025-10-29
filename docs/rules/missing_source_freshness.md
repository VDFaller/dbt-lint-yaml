# Missing source freshness

**rule_id**: missing_source_freshness

## Summary
- A source is missing `freshness` configuration or required freshness metadata.

## What it checks
- Ensures sources include freshness metadata where policy expects it.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/sources.rs](src/check/sources.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
