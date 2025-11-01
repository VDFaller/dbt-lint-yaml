# Missing column descriptions

**rule_id**: missing_column_descriptions

## Summary
- Column(s) in a model are missing descriptions in the model's properties YAML.

## What it checks
- Detects columns defined by your model that do not have a description entry in that model's properties file (the YAML dbt uses for column docs).

## Why this matters
- Column descriptions power catalog docs and help downstream users understand your data. Missing descriptions reduce documentation quality and increase onboarding friction.

## Default
- Enabled.

## Autofixable with `--fix`?
- Sometimes — when helpful upstream descriptions exist.
- When you run the tool with `--fix`, it will look for matching column names in upstream models, seeds, and sources. If it finds a description, it will record a change and the writeback step will insert that description into the model's properties YAML. If no good match is found, the column is left unchanged.

## Configs that affects this rule
- render_descriptions (project-wide): controls whether descriptions are rendered inline or as doc blocks. See the general configuration docs for details (link below). This rule does not require per-rule enablement; use the selector `missing_column_descriptions` in your `select`/`exclude` lists to enable/disable it.
- invalid_descriptions (project-wide): list of placeholder strings considered invalid for descriptions (default: `["TBD", "FILL ME OUT"]`). Columns with descriptions that match these markers (case-insensitive, trimmed) are treated as missing and may be auto-filled from upstream.

## Notes
- This rule only inserts descriptions when a confident upstream match exists. It will not invent free-text descriptions.

## Implementation (for contributors)
- Source: [src/check/models.rs](/src/check/models.rs) -> `check_model_column`

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)
