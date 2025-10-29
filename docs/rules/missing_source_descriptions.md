# Missing source descriptions

**rule_id**: missing_source_descriptions

## Summary
- A source (or source table) is missing a `description` in its YAML.

## What it checks
- Detects missing descriptions for source declarations in your `sources:` YAML files.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/sources.rs](src/check/sources.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
