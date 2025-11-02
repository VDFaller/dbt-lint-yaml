use crate::config::Config;
use dbt_dag::deps_mgmt::topological_sort;
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode};
use std::collections::{BTreeMap, BTreeSet};

mod columns;
mod exposures;
mod models;
mod sources;

use exposures::check_exposures;
use models::check_model;
use sources::check_source;

pub use crate::change_descriptors::ColumnChange;
pub use crate::change_descriptors::{ModelChange, ModelChanges};
pub use columns::{ColumnFailure, ColumnResult};
pub use exposures::{ExposureChange, ExposureFailure, ExposureResult};
pub use models::{ModelFailure, ModelResult};
pub use sources::{SourceFailure, SourceResult};

#[derive(Debug, Clone, Default)]
pub struct CheckResult {
    pub models: BTreeMap<String, ModelResult>,
    pub sources: BTreeMap<String, SourceResult>,
    pub exposures: BTreeMap<String, ExposureResult>,
    pub model_changes: BTreeMap<String, ModelChanges>,
}

impl CheckResult {
    pub fn has_failures(&self) -> bool {
        self.models.values().any(ModelResult::is_failure)
            || self.sources.values().any(SourceResult::is_failure)
            || self.exposures.values().any(|r| !r.failures.is_empty())
    }

    pub fn model_failures(&self) -> impl Iterator<Item = &ModelResult> {
        self.models.values().filter(|result| result.is_failure())
    }

    pub fn source_failures(&self) -> impl Iterator<Item = &SourceResult> {
        self.sources.values().filter(|result| result.is_failure())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CheckEvent<'a> {
    Model(&'a ModelResult),
    Source(&'a SourceResult),
    Exposure(&'a ExposureResult),
}

pub fn check_all(manifest: &DbtManifestV12, config: &Config) -> CheckResult {
    check_all_with_report(manifest, config, |_| {})
}

pub fn check_all_with_report<F>(
    manifest: &DbtManifestV12,
    config: &Config,
    mut reporter: F,
) -> CheckResult
where
    F: FnMut(CheckEvent<'_>),
{
    let mut result = CheckResult::default();
    let mut accumulated_changes: BTreeMap<String, ModelChanges> = BTreeMap::new();
    let sorted_nodes = nodes_in_dag_order(manifest);

    for node_id in sorted_nodes {
        let Some(DbtNode::Model(_)) = manifest.nodes.get(&node_id) else {
            continue;
        };

        let model_result = check_model(manifest, &node_id, &accumulated_changes, config);

        if let Some(changes) = model_result.changes() {
            accumulated_changes.insert(changes.model_id.clone(), changes.clone());
            result
                .model_changes
                .insert(changes.model_id.clone(), changes.clone());
        }

        reporter(CheckEvent::Model(&model_result));

        let model_key = model_result.model_id().to_string();
        result.models.insert(model_key, model_result);
    }

    for source in manifest.sources.values() {
        let source_result = check_source(manifest, source, config);

        reporter(CheckEvent::Source(&source_result));

        let source_key = source_result.source_id().to_string();
        result.sources.insert(source_key, source_result);
    }

    // run exposure checks
    for exposure_result in check_exposures(manifest, config) {
        reporter(CheckEvent::Exposure(&exposure_result));
        let exposure_key = exposure_result.exposure_id.to_string();
        result.exposures.insert(exposure_key, exposure_result);
    }

    result
}

// TODO: this still feels a bit off because it doesn't have sources.
fn nodes_in_dag_order(manifest: &DbtManifestV12) -> Vec<String> {
    let mut deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for (node_id, node) in &manifest.nodes {
        let upstream_nodes = match node {
            DbtNode::Model(model) => Some(&model.__base_attr__.depends_on.nodes),
            DbtNode::Seed(seed) => Some(&seed.__base_attr__.depends_on.nodes),
            DbtNode::Snapshot(snapshot) => Some(&snapshot.__base_attr__.depends_on.nodes),
            DbtNode::Analysis(analysis) => Some(&analysis.__base_attr__.depends_on.nodes),
            _ => None,
        };

        if let Some(upstream_nodes) = upstream_nodes {
            let upstream = upstream_nodes
                .iter()
                .filter(|upstream_id| {
                    matches!(
                        manifest.nodes.get(*upstream_id),
                        Some(
                            DbtNode::Model(_)
                                | DbtNode::Seed(_)
                                | DbtNode::Snapshot(_)
                                | DbtNode::Analysis(_)
                        )
                    )
                })
                .cloned()
                .collect::<BTreeSet<_>>();

            deps.insert(node_id.clone(), upstream);
        }
    }

    topological_sort(&deps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Selector};
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
            upstream.__base_attr__.columns.push({
                let column = DbtColumn {
                    name: "customer_id".to_string(),
                    description: Some("Upstream description".to_string()),
                    ..Default::default()
                };
                Arc::new(column)
            });
        }

        if let Some(DbtNode::Model(downstream)) = manifest.nodes.get_mut("model.test.downstream") {
            downstream.__common_attr__.unique_id = "model.test.downstream".to_string();
            downstream.__base_attr__.depends_on.nodes = vec!["model.test.upstream".to_string()];
            downstream.__base_attr__.columns.push({
                let column = DbtColumn {
                    name: "customer_id".to_string(),
                    description: None,
                    ..Default::default()
                };
                Arc::new(column)
            });
        }

        manifest
    }

    #[test]
    fn check_all_collects_model_changes() {
        let manifest = manifest_with_inheritable_column();

        let config = Config {
            select: vec![
                Selector::MissingModelDescriptions,
                Selector::MissingColumnDescriptions,
            ],
            ..Default::default()
        }
        .with_fix(true);
        let result = check_all(&manifest, &config);

        assert_eq!(result.model_changes.len(), 2);
        assert!(result.model_changes.contains_key("model.test.downstream"));
        assert!(result.model_changes.contains_key("model.test.upstream"));
        let model_result = result
            .models
            .get("model.test.downstream")
            .expect("model result should be tracked");
        assert!(model_result.is_pass(), "downstream model should be fixed");
        assert!(
            model_result
                .changes()
                .expect("changes should be recorded")
                .column_changes
                .contains_key("customer_id"),
            "column change should be present"
        );
    }
}
