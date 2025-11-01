# Missing source column descriptions

**rule_id**: missing_source_column_descriptions

## Summary
- One or more columns declared for a source table are missing a description.

## What it checks
- For each `source` -> `tables` entry, validates that each declared column has a non-empty `description`.

## Default
- Enabled by default (included in the example `dbt-lint.toml` in this repo).

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: `src/check/sources.rs`

## See also
- `missing_source_table_descriptions` — checks for table-level source descriptions and general source-table documentation.
- General configuration: `docs/configuration.md`
