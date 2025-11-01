# Exposure Parents Materializations

**rule_id**: exposure_parents_materializations

## Summary
- An exposure depends on one or more parent models that are not materialized as `table` or `incremental` (for example: `view` or `ephemeral`).

## Default
- Enabled when the selector `ExposureParentsMaterializations` is selected.

## Autofixable with `--fix`?
- No.

## Reason to Flag

Exposures should depend on the business logic you encoded into your dbt project (e.g. models or metrics) rather than raw untransformed sources. Additionally, models that are referenced by an exposure are likely to be used heavily in downstream systems, and therefore need to be performant when queried.

## How to Remediate

If you have a source parent of an exposure, you should incorporate that raw data into your project in some way, then update the exposure to point to that model.

If necessary, update the `materialized` configuration on the models returned to either `table` or `incremental`. This can be done in individual model files using a config block, or for groups of models in your dbt_project.yml file. See the docs on model configurations for more info!

## Implementation (for contributors)
- Source: [`src/check/exposures.rs`](/src/check/exposures.rs)
- See the in-code reference to the dbt Project Evaluator guidance: `https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/performance/#exposure-parents-materializations`

## See also
- General configuration: [docs/configuration.md](/docs/configuration.md)
