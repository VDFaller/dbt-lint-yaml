# Direct join to source

**rule_id**: direct_join_to_source

## Summary
- Detects when a model directly joins to a source relation instead of joining via an upstream model.

## What it checks
- Flags SQL that performs joins directly against `source()` relations rather than leveraging curated upstream models.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
