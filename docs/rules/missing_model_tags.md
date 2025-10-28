Title: Missing model tags

rule_id: missing_model_tags

Summary
- A model is missing tags in its properties YAML.

What it checks
- Flags models that don't have a `tags:` entry in the properties file.

Default
- Enabled.

Autofixable with `--fix`?
- No.

Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs)

See also
- General configuration: [docs/configuration.md](docs/configuration.md)
