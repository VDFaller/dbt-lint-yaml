use crate::osmosis::inherit_column_descriptions;
use dbt_dag::deps_mgmt::topological_sort;
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode, ManifestSource};
use std::collections::{BTreeMap, BTreeSet};
pub mod osmosis;
use std::path::PathBuf;

#[derive(Default, Debug)]
pub struct ModelFailures {
    pub no_descriptions: Vec<String>,
    pub no_tags: Vec<String>,
    pub column_failures: Vec<ColumnFailures>,
}

#[derive(Default, Debug)]
pub struct ColumnFailures {
    pub model: String,
    pub no_descriptions: Vec<String>,
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
pub struct SourceFailures {
    pub no_descriptions: Vec<String>,
}

#[derive(Default, Debug)]
pub struct Failures {
    pub model_failures: ModelFailures,
    pub source_failures: SourceFailures,
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

pub fn check_all(manifest: &mut DbtManifestV12) -> Failures {
    let mut failures = Failures::default();
    let sorted_nodes = models_in_dag_order(manifest);
    println!("Model processing order: {:?}", sorted_nodes);

    for model_id in sorted_nodes {
        check_model(manifest, &model_id, &mut failures.model_failures);
    }

    for source in manifest.sources.values() {
        check_source(source, &mut failures.source_failures);
    }

    failures
}

fn check_model(manifest: &mut DbtManifestV12, model_id: &str, failures: &mut ModelFailures) {
    if let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) {
        if model.__common_attr__.description.is_none() {
            failures
                .no_descriptions
                .push(model.__common_attr__.unique_id.clone());
        }

        if model.config.tags.is_none() {
            failures
                .no_tags
                .push(model.__common_attr__.unique_id.clone());
        }
    } else {
        return;
    }

    if let Some(column_failures) = check_model_columns(manifest, model_id) {
        failures.column_failures.push(column_failures);
    }
}

fn check_model_columns(manifest: &mut DbtManifestV12, model_id: &str) -> Option<ColumnFailures> {
    let missing_columns: Vec<String> = {
        let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) else {
            return None;
        };
        model
            .__base_attr__
            .columns
            .values()
            .filter(|col| col.description.is_none())
            .map(|col| col.name.clone())
            .collect()
    };

    if missing_columns.is_empty() {
        return None;
    }

    for col_name in &missing_columns {
        let _ = inherit_column_descriptions(manifest, model_id, col_name);
    }

    let Some(DbtNode::Model(model)) = manifest.nodes.get(model_id) else {
        return None;
    };

    let unresolved: Vec<String> = missing_columns
        .iter()
        .filter_map(|col_name| {
            model
                .__base_attr__
                .columns
                .get(col_name)
                .and_then(|col| col.description.is_none().then(|| col.name.clone()))
        })
        .collect();

    if unresolved.is_empty() {
        None
    } else {
        Some(ColumnFailures {
            model: model.__common_attr__.unique_id.clone(),
            no_descriptions: unresolved,
        })
    }
}

fn check_source(source: &ManifestSource, failures: &mut SourceFailures) {
    if source.__common_attr__.description.is_none() {
        failures
            .no_descriptions
            .push(source.__common_attr__.unique_id.clone());
    }
}
