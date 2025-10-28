Title: Missing properties file

rule_id: missing_properties_file

Summary
- A model does not have an associated properties YAML file (e.g., `model_name.yml`).

What it checks
- Checks whether a model has a properties YAML file that declares model- and column-level metadata. If missing, the rule reports it.

Default
- Enabled.

Autofixable with `--fix`?
- Yes. When `--fix` is enabled, the tool can create a minimal properties YAML for the model. Review generated files before committing.

Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs) (creates a `ModelChange` descriptor for writeback)

See also
- General configuration: [docs/configuration.md](docs/configuration.md)
