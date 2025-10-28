Title: Missing source table descriptions

rule_id: missing_source_table_descriptions

Summary
- A table inside a source is missing descriptive metadata (table- or column-level).

What it checks
- Ensures tables declared under `sources:` include `description` text and, where appropriate, column descriptions.

Default
- Enabled.

Autofixable with `--fix`?
- No.

Implementation (for contributors)
- Source: [src/check/sources.rs](src/check/sources.rs)

See also
- General configuration: [docs/configuration.md](docs/configuration.md)
