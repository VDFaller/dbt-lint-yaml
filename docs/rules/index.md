
## dbt-lint-yaml rules

This folder documents the user-facing rules you can enable or disable in `dbt-lint.toml`. Each file describes:


- [missing_column_descriptions](missing_column_descriptions.md) — Missing column descriptions
- [missing_model_descriptions](missing_model_descriptions.md) — Missing model descriptions
- [missing_model_tags](missing_model_tags.md) — Missing model tags
- [missing_source_descriptions](missing_source_descriptions.md) — Missing source descriptions
- [missing_source_table_descriptions](missing_source_table_descriptions.md) — Missing source table descriptions
- [missing_source_column_descriptions](missing_source_column_descriptions.md) — Missing source column descriptions
- [direct_join_to_source](direct_join_to_source.md) — Direct join to source
- [missing_properties_file](missing_properties_file.md) — Missing properties file
- [duplicate_sources](duplicate_sources.md) — Duplicate sources
- [model_fanout](model_fanout.md) — Model fanout
- [root_models](root_models.md) — Root models
- [unused_sources](unused_sources.md) — Unused sources
- [missing_primary_key](missing_primary_key.md) — Missing primary key
- [missing_source_freshness](missing_source_freshness.md) — Missing source freshness
- [multiple_sources_joined](multiple_sources_joined.md) — Multiple sources joined
- [rejoining_of_upstream_concepts](rejoining_of_upstream_concepts.md) — Rejoining of upstream concepts
- [source_fanout](source_fanout.md) — Source fanout
- [public_models_without_contract](public_models_without_contract.md) — Public models without contract
- [models_separate_from_properties_file](models_separate_from_properties_file.md) — Models separate from properties file
- [missing_required_tests](missing_required_tests.md) — Missing required tests (config-driven)
- [exposure_dependent_on_private_model](exposure_dependent_on_private_model.md) — Exposure depends on private models
- [exposure_parents_materializations](exposure_parents_materializations.md) — Exposure depends on non-materialized parent models
- [dead_model](dead_model.md) — Dead model (has no downstream dependencies)
