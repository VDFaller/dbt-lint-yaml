# Missing model descriptions

**rule_id**: missing_model_descriptions

## Summary
- A model is missing a top-level description in its properties YAML.

## What it checks
- Ensures each model has a brief description in the model's properties file (the YAML dbt uses for model docs).

## Default
- Enabled.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs)

## See also
- General configuration: [docs/configuration.md](docs/configuration.md)
