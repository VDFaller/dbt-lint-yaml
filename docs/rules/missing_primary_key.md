# Missing primary key

**rule_id**: missing_primary_key

## Summary
- A model is missing a primary-key-like declaration or tests asserting uniqueness.

## What it checks
- Flags models that don't expose a clear primary key column or lack expected uniqueness tests.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](/src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)