# Public models without contract

**rule_id**: public_models_without_contract

## Summary
- Flags public-facing models that lack a contract or expected documentation.

## What it checks
- Detects models intended for public use that don't declare a contract or required metadata.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
