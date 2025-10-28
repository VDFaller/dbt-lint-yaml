Title: Missing required tests

rule_id: missing_required_tests

Summary
- Ensures models include the tests your project declares as required.

What it checks
- Flags models that are missing tests listed in the project's `required_tests` configuration.

Default
- Enabled if `required_tests` is populated in config; otherwise depends on your project defaults.

Autofixable with `--fix`?
- No. The tool will surface missing tests but will not add test definitions automatically.

Implementation (for contributors)
- Source: [src/check/models.rs](src/check/models.rs) (uses `Config.required_tests` to drive checks)

See also
- General configuration: [docs/configuration.md](docs/configuration.md)
