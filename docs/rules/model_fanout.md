# Model fanout

**rule_id**: model_fanout

## Summary
- Flags models that have an unusually large number of downstream dependents.

## What it checks
- Counts downstream (children) models and flags those over `model_fanout_threshold`.

## Default
- Enabled. Default threshold: `3`.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs) -> `model_fanout`

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
