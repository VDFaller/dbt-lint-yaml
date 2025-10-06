use crate::osmosis::inherit_column_descriptions;
use dbt_dag::deps_mgmt::topological_sort;
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode, ManifestSource};
use std::collections::{BTreeMap, BTreeSet};
pub mod osmosis;
use std::path::PathBuf;

#[derive(Default, Debug)]
pub struct ModelFailure {
    pub model_id: String,
    pub description_missing: bool,
    pub tags_missing: bool,
    pub column_failures: BTreeMap<String, ColumnFailure>,
}

#[derive(Default, Debug, Clone)]
pub struct ColumnFailure {
    pub column_name: String,
    pub description_missing: bool,
}

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
}

#[derive(Default, Debug)]
pub struct Failures {
    pub models: BTreeMap<String, ModelFailure>,
    pub sources: BTreeMap<String, SourceFailure>,
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

pub fn check_all(manifest: &mut DbtManifestV12) -> CheckResult {
    let mut result = CheckResult::default();
    let sorted_nodes = models_in_dag_order(manifest);
    println!("Model processing order: {:?}", sorted_nodes);

    for model_id in sorted_nodes {
        let (model_failure, model_changes) = check_model(manifest, &model_id);

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
        if let Some(source_failure) = check_source(source) {
            result
                .failures
                .sources
                .insert(source_failure.source_id.clone(), source_failure);
        }
    }

    result
}

fn check_model(
    manifest: &mut DbtManifestV12,
    model_id: &str,
) -> (Option<ModelFailure>, Option<ModelChanges>) {
    let Some(DbtNode::Model(model_meta)) = manifest.nodes.get(model_id) else {
        return (None, None);
    };

    let unique_id = model_meta.__common_attr__.unique_id.clone();
    let patch_path = model_meta.__common_attr__.patch_path.clone();
    let description_missing = model_meta.__common_attr__.description.is_none();
    let tags_missing = model_meta.config.tags.is_none();

    let ColumnCheckResult {
        failures: column_failures,
        column_changes,
    } = check_model_columns(manifest, model_id);

    let has_column_failures = !column_failures.is_empty();

    let model_failure = if description_missing || tags_missing || has_column_failures {
        Some(ModelFailure {
            model_id: unique_id.clone(),
            description_missing,
            tags_missing,
            column_failures,
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

fn check_model_columns(manifest: &mut DbtManifestV12, model_id: &str) -> ColumnCheckResult {
    let mut result = ColumnCheckResult::default();

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
        let _ = inherit_column_descriptions(manifest, model_id, col_name);
    }

    let Some(DbtNode::Model(updated_model)) = manifest.nodes.get(model_id) else {
        return result;
    };

    let unresolved: Vec<String> = missing_columns
        .iter()
        .filter_map(|col_name| {
            updated_model
                .__base_attr__
                .columns
                .get(col_name)
                .and_then(|col| col.description.is_none().then(|| col.name.clone()))
        })
        .collect();

    for col_name in unresolved {
        result.failures.insert(
            col_name.clone(),
            ColumnFailure {
                column_name: col_name,
                description_missing: true,
            },
        );
    }

    for col_name in &missing_columns {
        let old_description = previous_descriptions.get(col_name).cloned().unwrap_or(None);
        let new_description = updated_model
            .__base_attr__
            .columns
            .get(col_name)
            .and_then(|col| col.description.clone());

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

    result
}

fn check_source(source: &ManifestSource) -> Option<SourceFailure> {
    let description_missing = source.__common_attr__.description.is_none();

    description_missing.then(|| SourceFailure {
        source_id: source.__common_attr__.unique_id.clone(),
        description_missing,
    })
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
        let mut manifest = manifest_with_inheritable_column();

        let (model_failure, model_changes) = check_model(&mut manifest, "model.test.downstream");

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
        let mut manifest = manifest_with_inheritable_column();

        let result = check_all(&mut manifest);

        assert_eq!(result.model_changes.len(), 1);
        assert!(result.model_changes.contains_key("model.test.downstream"));
        let failure = result
            .failures
            .models
            .get("model.test.downstream")
            .expect("model failure should be tracked");
        assert!(failure.description_missing);
    }
}
