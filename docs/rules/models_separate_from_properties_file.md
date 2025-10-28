Title: Models separate from properties file

rule_id: models_separate_from_properties_file

Summary
- A model's properties are stored separately from the model file (e.g., in a central properties file) which may complicate tooling.

What it checks
- Flags models whose properties live in a different path than the model file.

Default
- Enabled.

Autofixable with `--fix`?
- Not currently reliably fixable in all cases. The selector is marked unfixable by default in code comments.

Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs) (note: writeback for this selector is disabled in some situations)

See also
- General configuration: [docs/configuration.md](docs/configuration.md)
