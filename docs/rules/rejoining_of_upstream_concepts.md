# Rejoining of upstream concepts

**rule_id**: rejoining_of_upstream_concepts

## Summary
- Detects when previously separated upstream concepts are rejoined downstream, which can indicate leaky abstractions.

## What it checks
- Flags models that re-join sets of upstream transformations that were intended to stay separate.

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
