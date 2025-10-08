use crate::{
    config::{Config, Selector},
    osmosis::get_upstream_col_desc,
};
use dbt_dag::deps_mgmt::topological_sort;
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode, ManifestModel, ManifestSource};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::path::PathBuf;

#[derive(Default, Debug)]
pub struct ModelFailure {
    pub model_id: String,
    pub description_missing: bool,
    pub tags_missing: bool,
    pub column_failures: BTreeMap<String, ColumnFailure>,
    pub is_direct_join_to_source: bool,
    pub is_missing_properties_file: bool,
    pub is_model_fanout: bool,
}

impl Display for ModelFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ModelFailure: {}", self.model_id)?;
        if self.description_missing {
            writeln!(f, "  - Missing Description")?;
        }
        if self.tags_missing {
            writeln!(f, "  - Missing Tags")?;
        }
        if self.is_direct_join_to_source {
            writeln!(f, "  - Direct join to source detected")?;
        }
        if self.is_missing_properties_file {
            writeln!(f, "  - Missing properties file")?;
        }
        if self.is_model_fanout {
            writeln!(f, "  - Model fanout exceeds threshold")?;
        }
        for column_failure in self.column_failures.values() {
            write!(f, "{}", column_failure)?;
        }
        Ok(())
    }
}

#[derive(Default, Debug, Clone)]
pub struct ColumnFailure {
    pub column_name: String,
    pub description_missing: bool,
}

impl Display for ColumnFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  ColumnFailure: {}", self.column_name)?;
        if self.description_missing {
            writeln!(f, "    - Missing Description")?;
        }
        Ok(())
    }
}
// TODO: Change ModelChanges to pull from an enum of possible changes
#[derive(Default, Debug)]
pub struct ModelChanges {
    pub model_id: String,
    pub patch_path: Option<PathBuf>,
    pub column_changes: BTreeMap<String, BTreeSet<ColumnChanges>>,
}
#[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnChanges {
    pub column_name: String,
    pub old_description: Option<String>,
    pub new_description: Option<String>,
}

#[derive(Default, Debug)]
pub struct SourceFailure {
    pub source_id: String,
    pub description_missing: bool,
    pub duplicate_id: Option<String>,
}

impl Display for SourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SourceFailure: {}", self.source_id)?;
        if self.description_missing {
            writeln!(f, "  - Missing Description")?;
        }
        if let Some(duplicate_id) = &self.duplicate_id {
            writeln!(f, "  - Duplicate Source Definition: {}", duplicate_id)?;
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct Failures {
    pub models: BTreeMap<String, ModelFailure>,
    pub sources: BTreeMap<String, SourceFailure>,
}

impl Display for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Failures:")?;
        for model_failure in self.models.values() {
            write!(f, "{}", model_failure)?;
        }
        for source_failure in self.sources.values() {
            write!(f, "{}", source_failure)?;
        }
        Ok(())
    }
}

impl Failures {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty() && self.sources.is_empty()
    }
}

#[derive(Default, Debug)]
pub struct CheckResult {
    pub failures: Failures,
    pub model_changes: BTreeMap<String, ModelChanges>,
}

#[derive(Default, Debug)]
struct ColumnCheckResult {
    failures: BTreeMap<String, ColumnFailure>,
    column_changes: BTreeMap<String, BTreeSet<ColumnChanges>>,
}

// TODO: This should just be the full DAG, not just models
fn models_in_dag_order(manifest: &DbtManifestV12) -> Vec<String> {
    let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for (node_id, node) in &manifest.nodes {
        if let DbtNode::Model(model) = node {
            let upstream_models = model
                .__base_attr__
                .depends_on
                .nodes
                .iter()
                .filter(|upstream_id| {
                    matches!(manifest.nodes.get(*upstream_id), Some(DbtNode::Model(_)))
                })
                .cloned()
                .collect::<BTreeSet<_>>();

            deps.insert(node_id.clone(), upstream_models);
        }
    }

    topological_sort(&deps)
}

pub fn check_all(manifest: &DbtManifestV12, config: &Config) -> CheckResult {
    let mut result = CheckResult::default();
    let sorted_nodes = models_in_dag_order(manifest);

    for model_id in sorted_nodes {
        let (model_failure, model_changes) =
            check_model(manifest, &model_id, &result.model_changes, config);

        if let Some(failure) = model_failure {
            result
                .failures
                .models
                .insert(failure.model_id.clone(), failure);
        }

        if let Some(changes) = model_changes {
            result
                .model_changes
                .insert(changes.model_id.clone(), changes);
        }
    }

    for source in manifest.sources.values() {
        if let Some(source_failure) = check_source(manifest, source, config) {
            result
                .failures
                .sources
                .insert(source_failure.source_id.clone(), source_failure);
        }
    }

    result
}

fn check_model(
    manifest: &DbtManifestV12,
    model_id: &str,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> (Option<ModelFailure>, Option<ModelChanges>) {
    let Some(node @ DbtNode::Model(model_meta)) = manifest.nodes.get(model_id) else {
        return (None, None);
    };

    let unique_id = model_meta.__common_attr__.unique_id.clone();
    let patch_path = model_meta.__common_attr__.patch_path.clone();
    let description_missing = config.select.contains(&Selector::MissingModelDescriptions)
        && model_meta.__common_attr__.description.is_none();
    let tags_missing =
        config.select.contains(&Selector::MissingModelTags) && model_meta.config.tags.is_none();

    let is_direct_join_to_source =
        config.select.contains(&Selector::DirectJoinToSource) && direct_join_to_source(model_meta);
    let is_missing_properties_file =
        config.select.contains(&Selector::MissingPropertiesFile) && missing_properties_file(node);
    let is_model_fanout = model_fanout(manifest, model_id, config);

    let ColumnCheckResult {
        failures: column_failures,
        column_changes,
    } = check_model_columns(manifest, model_id, prior_changes, &config);

    let has_column_failures = !column_failures.is_empty();

    let model_failure = if description_missing
        || tags_missing
        || has_column_failures
        || is_direct_join_to_source
        || is_missing_properties_file
        || is_model_fanout
    {
        Some(ModelFailure {
            model_id: unique_id.clone(),
            description_missing,
            tags_missing,
            column_failures,
            is_direct_join_to_source,
            is_missing_properties_file,
            is_model_fanout,
        })
    } else {
        None
    };

    let model_changes = (!column_changes.is_empty()).then_some(ModelChanges {
        model_id: unique_id,
        patch_path,
        column_changes,
    });

    (model_failure, model_changes)
}

fn direct_join_to_source(model: &ManifestModel) -> bool {
    let depends_on = &model.__base_attr__.depends_on.nodes;
    if depends_on.len() < 2 {
        return false;
    }
    depends_on
        .iter()
        .any(|upstream_id| upstream_id.starts_with("source."))
}

fn missing_properties_file(node: &DbtNode) -> bool {
    match node {
        DbtNode::Model(model) => model.__common_attr__.patch_path.is_none(),
        DbtNode::Seed(seed) => seed.__common_attr__.patch_path.is_none(),
        DbtNode::Snapshot(snap) => snap.__common_attr__.patch_path.is_none(),
        _ => false,
    }
}

fn model_fanout(manifest: &DbtManifestV12, model_id: &str, config: &Config) -> bool {
    if !config.select.contains(&Selector::ModelFanout) {
        return false;
    }
    let downstream_models = manifest
        .child_map
        .get(model_id)
        .into_iter()
        .flatten()
        .filter(|id| id.starts_with("model."))
        .count();

    return downstream_models > config.model_fanout_threshold;
}

fn check_model_columns(
    manifest: &DbtManifestV12,
    model_id: &str,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> ColumnCheckResult {
    let mut result = ColumnCheckResult::default();
    if !config.select.contains(&Selector::MissingColumnDescriptions) {
        return result;
    }

    let (missing_columns, previous_descriptions) = {
        let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) else {
            return result;
        };

        let missing_columns: Vec<String> = model
            .__base_attr__
            .columns
            .values()
            .filter(|col| col.description.is_none())
            .map(|col| col.name.clone())
            .collect();

        if missing_columns.is_empty() {
            return result;
        }

        let mut previous_descriptions: BTreeMap<String, Option<String>> = BTreeMap::new();
        for col_name in &missing_columns {
            let description = model
                .__base_attr__
                .columns
                .get(col_name)
                .and_then(|col| col.description.clone());
            previous_descriptions.insert(col_name.clone(), description);
        }

        (missing_columns, previous_descriptions)
    };

    for col_name in &missing_columns {
        if !config.pull_column_desc_from_upstream {
            result.failures.insert(
                col_name.clone(),
                ColumnFailure {
                    column_name: col_name.clone(),
                    description_missing: true,
                },
            );
            continue;
        }
        match get_upstream_col_desc(manifest, Some(prior_changes), model_id, col_name) {
            Some(desc) => {
                let old_description = previous_descriptions.get(col_name).cloned().unwrap_or(None);
                let new_description = Some(desc);

                if old_description != new_description {
                    result
                        .column_changes
                        .entry(col_name.clone())
                        .or_default()
                        .insert(ColumnChanges {
                            column_name: col_name.clone(),
                            old_description,
                            new_description,
                        });
                }
            }
            None => {
                result.failures.insert(
                    col_name.clone(),
                    ColumnFailure {
                        column_name: col_name.clone(),
                        description_missing: true,
                    },
                );
            }
        }
    }

    result
}

fn check_source(
    manifest: &DbtManifestV12,
    source: &ManifestSource,
    config: &Config,
) -> Option<SourceFailure> {
    let description_missing = config.select.contains(&Selector::MissingSourceDescriptions)
        && source.__common_attr__.description.is_none();
    let duplicate_id = config
        .select
        .contains(&Selector::DuplicateSources)
        .then(|| duplicate_source(manifest, source))
        .flatten();

    (description_missing || duplicate_id.is_some()).then(|| SourceFailure {
        source_id: source.__common_attr__.unique_id.clone(),
        description_missing,
        duplicate_id,
    })
}

fn duplicate_source(manifest: &DbtManifestV12, source: &ManifestSource) -> Option<String> {
    if source.__common_attr__.name == source.identifier {
        return None;
    }
    // TODO: look into performance of this search in a larger project
    manifest
        .sources
        .values()
        .find(|s| {
            // there technically could be more than one dupe, but do I care?
            s.identifier == source.identifier
                && s.source_name == source.source_name
                && s.__common_attr__.unique_id != source.__common_attr__.unique_id
        })
        .map(|s| s.__common_attr__.unique_id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbt_schemas::schemas::{dbt_column::DbtColumn, manifest::DbtNode};
    use std::sync::Arc;

    fn manifest_with_inheritable_column() -> DbtManifestV12 {
        let mut manifest = DbtManifestV12::default();

        manifest.nodes.insert(
            "model.test.upstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.test.downstream".to_string(),
            DbtNode::Model(Default::default()),
        );

        if let Some(DbtNode::Model(upstream)) = manifest.nodes.get_mut("model.test.upstream") {
            upstream.__common_attr__.unique_id = "model.test.upstream".to_string();
            upstream
                .__base_attr__
                .columns
                .insert("customer_id".to_string(), {
                    let mut column = DbtColumn::default();
                    column.name = "customer_id".to_string();
                    column.description = Some("Upstream description".to_string());
                    Arc::new(column)
                });
        }

        if let Some(DbtNode::Model(downstream)) = manifest.nodes.get_mut("model.test.downstream") {
            downstream.__common_attr__.unique_id = "model.test.downstream".to_string();
            downstream.__base_attr__.depends_on.nodes = vec!["model.test.upstream".to_string()];
            downstream
                .__base_attr__
                .columns
                .insert("customer_id".to_string(), {
                    let mut column = DbtColumn::default();
                    column.name = "customer_id".to_string();
                    column.description = None;
                    Arc::new(column)
                });
        }

        manifest
    }

    #[test]
    fn check_model_returns_column_changes() {
        let manifest = manifest_with_inheritable_column();
        let prior_changes = std::collections::BTreeMap::<String, ModelChanges>::new();

        let (model_failure, model_changes) = check_model(
            &manifest,
            "model.test.downstream",
            &prior_changes,
            &Config::default(),
        );

        let failure = model_failure.expect("expected model failure to be recorded");
        assert!(failure.column_failures.is_empty());
        assert!(failure.description_missing);
        let changes = model_changes.expect("expected column changes to be recorded");
        assert_eq!(changes.model_id, "model.test.downstream");
        let column_set = changes
            .column_changes
            .get("customer_id")
            .expect("customer_id column should be present");
        let change = column_set.iter().next().expect("change entry should exist");
        assert_eq!(
            change.new_description.as_deref(),
            Some("Upstream description")
        );
    }

    #[test]
    fn check_all_collects_model_changes() {
        let manifest = manifest_with_inheritable_column();

        let result = check_all(&manifest, &Config::default());

        assert_eq!(result.model_changes.len(), 1);
        assert!(result.model_changes.contains_key("model.test.downstream"));
        let failure = result
            .failures
            .models
            .get("model.test.downstream")
            .expect("model failure should be tracked");
        assert!(failure.description_missing);
    }

    #[test]
    fn test_direct_join_to_source() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec![
            "model.test.upstream".to_string(),
            "source.test.raw_layer.orders".to_string(),
        ];
        assert!(direct_join_to_source(&model));
    }

    #[test]
    fn test_direct_join_to_source_single_dependency() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec!["source.test.raw_layer.orders".to_string()];
        assert!(!direct_join_to_source(&model));
    }

    #[test]
    fn test_direct_join_to_source_no_sources() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec![
            "model.test.upstream".to_string(),
            "model.test.another_upstream".to_string(),
        ];
        assert!(!direct_join_to_source(&model));
    }

    #[test]
    fn test_model_fanout() {
        let mut manifest = DbtManifestV12::default();

        manifest.child_map.insert(
            "model.test.one_model".to_string(),
            vec!["model.test.downstream_0".to_string()],
        );
        manifest.child_map.insert(
            "model.test.lots_of_tests".to_string(),
            vec![
                "model.jaffle_shop.orders".to_string(),
                "test.jaffle_shop.not_null_stg_products".to_string(),
                "unit_test.jaffle_shop.order_items.test_supply_costs_sum_correctly".to_string(),
            ],
        );
        manifest.child_map.insert(
            "model.test.four_models".to_string(),
            vec![
                "model.test.downstream_0".to_string(),
                "model.test.downstream_1".to_string(),
                "model.test.downstream_2".to_string(),
                "model.test.downstream_3".to_string(),
            ],
        );

        let config = Config {
            model_fanout_threshold: 1,
            ..Default::default()
        };

        assert!(!model_fanout(&manifest, "model.test.one_model", &config));
        assert!(!model_fanout(
            &manifest,
            "model.test.lots_of_tests",
            &config
        ));
        assert!(model_fanout(&manifest, "model.test.four_models", &config));
    }
}
