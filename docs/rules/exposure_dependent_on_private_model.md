
```markdown
# Exposure depends on private model

**rule_id**: exposure_dependent_on_private_model

## Summary
- An exposure depends on one or more models marked with `access: private` in the manifest.

## What it checks
- For each exposure, inspects `depends_on.nodes` and finds referenced models.
- If any referenced model has `access: private`, the exposure is reported as failing and the private model ids are listed.
- The check only considers manifest nodes whose unique id starts with `model`.

## Default
- Enabled when the selector `ExposureDependentOnPrivateModel` is selected.

## Autofixable with `--fix`?
- No.

## Implementation (for contributors)
- Source: [src/check/exposures.rs](/src/check/exposures.rs)

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)

## Configs that affect this rule
- `ExposureDependentOnPrivateModel` — selector to enable this specific check.
