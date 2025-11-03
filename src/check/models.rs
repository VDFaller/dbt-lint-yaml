use super::columns::ColumnResult;
use crate::change_descriptors::{ColumnChange, ModelChange, ModelChanges};
use crate::codegen::write_generated_model;
use crate::{
    config::{Config, Selector},
    writeback::properties::model_property_from_manifest_differences,
};
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode, ManifestModel};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::path::PathBuf;

use strum::AsRefStr;

#[derive(Debug, Clone, AsRefStr, PartialEq, Eq)]
pub enum ModelFailure {
    DescriptionMissing,
    TagsMissing(Vec<String>),
    DirectJoinToSource(Vec<String>),
    MissingPropertiesFile,
    ModelFanout(usize),
    MissingRequiredTests(Vec<String>),
    RootModel,
    MissingPrimaryKey,
    MultipleSourcesJoined(Vec<String>),
    RejoiningOfUpstreamConcepts(Vec<String>),
    PublicModelWithoutContract,
    DeadModel,
    ModelSeparateFromPropertiesFile {
        patch_path: PathBuf,
        original_file_path: PathBuf,
    },
}

impl Display for ModelFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let extra_info = match self {
            ModelFailure::TagsMissing(tags) => format!(" (tags: {})", tags.join(",")),
            ModelFailure::DirectJoinToSource(sources) => {
                format!(" (sources: {})", sources.join(","))
            }
            ModelFailure::MissingRequiredTests(tests) => format!(" (tests: {})", tests.join(",")),
            ModelFailure::MultipleSourcesJoined(sources) => {
                format!(" (sources: {})", sources.join(","))
            }
            ModelFailure::RejoiningOfUpstreamConcepts(concepts) => {
                format!(" (concepts: {})", concepts.join(","))
            }
            ModelFailure::ModelSeparateFromPropertiesFile {
                patch_path,
                original_file_path,
            } => {
                format!(
                    " (patch_path: {}, original_file_path: {})",
                    patch_path.display(),
                    original_file_path.display()
                )
            }
            _ => String::new(),
        };
        write!(f, "{}{}", self.as_ref(), extra_info)
    }
}

// ModelChange and ModelChanges are defined in `crate::change_descriptors`.

#[derive(Debug, Clone, Default)]
pub struct ModelResult {
    pub model_id: String,
    pub failures: Vec<ModelFailure>,
    pub column_results: BTreeMap<String, ColumnResult>, // kind of hate this, but...
    pub changes: Option<ModelChanges>,
}

impl ModelResult {
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub fn changes(&self) -> Option<&ModelChanges> {
        self.changes.as_ref()
    }

    pub fn failures(&self) -> &[ModelFailure] {
        &self.failures
    }

    pub fn has_column_failures(&self) -> bool {
        self.column_results.values().any(ColumnResult::is_failure)
    }

    pub fn is_pass(&self) -> bool {
        self.failures.is_empty() && !self.has_column_failures()
    }

    pub fn is_failure(&self) -> bool {
        !self.is_pass()
    }

    pub fn failure_reasons(&self) -> Vec<String> {
        let mut reasons: Vec<String> = self.failures.iter().map(ToString::to_string).collect();
        for column_result in self.column_results.values() {
            reasons.extend(column_result.failure_reasons());
        }
        reasons
    }
}

impl Display for ModelResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_pass() {
            write!(f, "ModelResult: Pass:{}", self.model_id)
        } else {
            writeln!(f, "ModelResult: Fail:{}", self.model_id)?;
            for reason in self.failure_reasons() {
                writeln!(f, "    {reason}")?;
            }
            Ok(())
        }
    }
}

pub(crate) fn check_model(
    manifest: &DbtManifestV12,
    model_id: &str,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> ModelResult {
    let Some(node @ DbtNode::Model(original_model)) = manifest.nodes.get(model_id) else {
        return ModelResult {
            model_id: model_id.to_string(),
            ..Default::default()
        };
    };
    let mut working_model = original_model.clone();

    let model_unique_id = working_model.__common_attr__.unique_id.clone();
    let model_name = model_unique_id
        .rsplit('.')
        .next()
        .unwrap_or(&model_unique_id)
        .to_string();
    let _model_type = model_type(original_model); // currently unused

    let mut failures: Vec<ModelFailure> = Vec::new();
    let mut model_level_changes: Vec<ModelChange> = Vec::new();
    let mut property_change_required = false;

    if let Some(f) = missing_properties_file(node, config) {
        failures.push(f)
    }

    if config.fix
        && config.is_selected(Selector::MissingPropertiesFile)
        && config.is_fixable(Selector::MissingPropertiesFile)
        && working_model.__common_attr__.patch_path.is_none()
    {
        let generated_patch = working_model
            .__common_attr__
            .original_file_path
            .with_extension("yml");
        working_model.__common_attr__.patch_path = Some(generated_patch);
    }

    match missing_model_description(&mut working_model, config) {
        Ok(Some(change)) => {
            if matches!(change, ModelChange::ChangePropertiesFile { .. }) {
                property_change_required = true;
            }
            model_level_changes.push(change);
        }
        Ok(None) => {}
        Err(failure) => failures.push(failure),
    }
    if let Err(failure) = missing_model_tags(&working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = missing_required_tests(manifest, &working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = missing_primary_key(&working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = public_model_without_contract(&working_model, config) {
        failures.push(failure)
    }

    if let Err(failure) = direct_join_to_source(&working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = model_fanout(manifest, model_id, config) {
        failures.push(failure)
    }
    if let Err(failure) = root_model(&working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = multiple_sources_joined(&working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = rejoining_of_upstream_concepts(manifest, &working_model, config) {
        failures.push(failure)
    }
    if let Err(failure) = dead_model(&working_model, manifest, config) {
        failures.push(failure)
    }

    match model_separate_from_properties_file(node, config) {
        Ok(Some(change)) => model_level_changes.push(change),
        Ok(None) => {}
        Err(failure) => failures.push(failure),
    }

    let column_results = crate::check::columns::check_model_columns(
        manifest,
        original_model,
        &mut working_model,
        prior_changes,
        config,
    );

    let mut column_changes: BTreeMap<String, BTreeSet<ColumnChange>> = BTreeMap::new();
    for (column_name, column_result) in &column_results {
        if !column_result.changes().is_empty() {
            property_change_required = true;
        }
        for change in column_result.changes() {
            column_changes
                .entry(column_name.clone())
                .or_default()
                .insert(change.clone());
        }
    }

    let patch_path = working_model.__common_attr__.patch_path.clone();

    if config.fix && property_change_required {
        if let Some(property) =
            model_property_from_manifest_differences(original_model, &working_model)
        {
            let mut applied = false;
            for change in model_level_changes.iter_mut() {
                if let ModelChange::ChangePropertiesFile { property: slot, .. } = change {
                    *slot = Some(property.clone());
                    applied = true;
                }
            }
            if !applied {
                model_level_changes.push(ModelChange::ChangePropertiesFile {
                    model_id: model_unique_id.clone(),
                    model_name: model_name.clone(),
                    patch_path: patch_path.clone(),
                    property: Some(property),
                });
            }
        }
    }

    let has_model_changes = !model_level_changes.is_empty();
    let has_column_changes = !column_changes.is_empty();

    let changes = if !has_model_changes && !has_column_changes {
        None
    } else {
        Some(ModelChanges {
            model_id: model_unique_id.clone(),
            patch_path,
            changes: model_level_changes,
            column_changes,
        })
    };

    ModelResult {
        model_id: model_unique_id,
        failures,
        column_results,
        changes,
    }
}

// Column checking (moved to `src/check/columns.rs`)

/// Check if a model is missing a description.
/// A description is considered missing if it is:
/// - None
/// - An empty string (after trimming)
/// - Matches any of the configured invalid descriptions (case-insensitive, after trimming)
fn missing_model_description(
    model: &mut ManifestModel,
    config: &Config,
) -> Result<Option<ModelChange>, ModelFailure> {
    if !config.is_selected(Selector::MissingModelDescriptions) {
        return Ok(None);
    }

    let is_missing = match model.__common_attr__.description.as_ref() {
        None => true,
        Some(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                true
            } else {
                config
                    .invalid_descriptions
                    .iter()
                    .any(|bad| bad.eq_ignore_ascii_case(trimmed))
            }
        }
    };

    if !is_missing {
        return Ok(None);
    }

    if config.is_fixable(Selector::MissingModelDescriptions) {
        // placeholder description until smarter rendering is implemented
        model.__common_attr__.description = Some("Auto-generated description".to_string());
        let model_id = model.__common_attr__.unique_id.clone();
        let model_name = model_id.rsplit('.').next().unwrap_or(&model_id).to_string();
        let patch_path = model.__common_attr__.patch_path.clone();
        Ok(Some(ModelChange::ChangePropertiesFile {
            model_id,
            model_name,
            patch_path,
            property: None,
        }))
    } else {
        Err(ModelFailure::DescriptionMissing)
    }
}

fn missing_model_tags(model: &ManifestModel, config: &Config) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::MissingModelTags) {
        return Ok(());
    }
    if model.config.tags.is_none() {
        Err(ModelFailure::TagsMissing(Vec::new()))
    } else {
        Ok(())
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#multiple-sources-joined
fn multiple_sources_joined(model: &ManifestModel, config: &Config) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::MultipleSourcesJoined) {
        return Ok(());
    }
    let sources: Vec<String> = model
        .__base_attr__
        .depends_on
        .nodes
        .iter()
        .filter(|upstream_id| upstream_id.starts_with("source."))
        .cloned()
        .collect();
    if sources.len() > 1 {
        Err(ModelFailure::MultipleSourcesJoined(sources))
    } else {
        Ok(())
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#direct-join-to-source
fn direct_join_to_source(model: &ManifestModel, config: &Config) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::DirectJoinToSource) {
        return Ok(());
    }
    let depends_on = &model.__base_attr__.depends_on.nodes;
    if depends_on.len() < 2 {
        return Ok(());
    }
    let sources: Vec<String> = depends_on
        .iter()
        .filter(|upstream_id| upstream_id.starts_with("source."))
        .cloned()
        .collect();
    if sources.is_empty() {
        Ok(())
    } else {
        Err(ModelFailure::DirectJoinToSource(sources))
    }
}

fn missing_properties_file(node: &DbtNode, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::MissingPropertiesFile) {
        return None;
    }
    let missing_patch = match node {
        DbtNode::Model(model) => model.__common_attr__.patch_path.is_none(),
        DbtNode::Seed(seed) => seed.__common_attr__.patch_path.is_none(),
        DbtNode::Snapshot(snap) => snap.__common_attr__.patch_path.is_none(),
        _ => false,
    };
    if !missing_patch {
        return None;
    }

    // fixing this on the fly because there's will only be one IO operation anyway
    // and that will make it so the other checks can happen
    // no reason to use the change framework here
    if config.is_fixable(Selector::MissingPropertiesFile)
        && let DbtNode::Model(model) = node
    {
        if let Err(e) = write_generated_model(model, config.project_dir.as_deref()) {
            eprintln!("failed to write generated model properties: {e}");
            return Some(ModelFailure::MissingPropertiesFile);
        }
        return None;
    }

    Some(ModelFailure::MissingPropertiesFile)
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#model-fanout
/// snapshots are not counted in fanout
fn model_fanout(
    manifest: &DbtManifestV12,
    model_id: &str,
    config: &Config,
) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::ModelFanout) {
        return Ok(());
    }
    let downstream_models = manifest
        .child_map
        .get(model_id)
        .into_iter()
        .flatten()
        .filter(|id| id.starts_with("model."))
        .count();

    if downstream_models > config.model_fanout_threshold {
        Err(ModelFailure::ModelFanout(downstream_models))
    } else {
        Ok(())
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#root-models
fn root_model(model: &ManifestModel, config: &Config) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::RootModels) {
        return Ok(());
    }
    if model.__base_attr__.depends_on.nodes.is_empty() {
        Err(ModelFailure::RootModel)
    } else {
        Ok(())
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/testing/#missing-primary-key-tests
fn missing_primary_key(model: &ManifestModel, config: &Config) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::MissingPrimaryKey) {
        return Ok(());
    }
    let missing_pk = model.primary_key.as_ref().unwrap_or(&vec![]).is_empty();
    if missing_pk {
        Err(ModelFailure::MissingPrimaryKey)
    } else {
        Ok(())
    }
}

fn model_separate_from_properties_file(
    node: &DbtNode,
    config: &Config,
) -> Result<Option<ModelChange>, ModelFailure> {
    if !config.is_selected(Selector::ModelsSeparateFromPropertiesFile) {
        return Ok(None);
    }
    let (patch_path, original_path) = match node {
        DbtNode::Model(model) => (
            model.__common_attr__.patch_path.as_deref(),
            model.__common_attr__.original_file_path.as_path(),
        ),
        DbtNode::Seed(seed) => (
            seed.__common_attr__.patch_path.as_deref(),
            seed.__common_attr__.original_file_path.as_path(),
        ),
        DbtNode::Snapshot(snap) => (
            snap.__common_attr__.patch_path.as_deref(),
            snap.__common_attr__.original_file_path.as_path(),
        ),
        _ => return Ok(None),
    };

    let Some(patch_path) = patch_path.filter(|path| path.parent() != original_path.parent()) else {
        return Ok(None);
    };

    let failure = ModelFailure::ModelSeparateFromPropertiesFile {
        patch_path: patch_path.to_path_buf(),
        original_file_path: original_path.to_path_buf(),
    };

    if config.is_fixable(Selector::ModelsSeparateFromPropertiesFile)
        && let Some(original_parent) = original_path.parent()
        && let Some(file_name) = patch_path.file_name()
    {
        let new_path = original_parent.join(file_name);

        let model_id = match node {
            DbtNode::Model(model) => model.__common_attr__.unique_id.clone(),
            DbtNode::Seed(seed) => seed.__common_attr__.unique_id.clone(),
            DbtNode::Snapshot(snap) => snap.__common_attr__.unique_id.clone(),
            _ => String::new(),
        };
        let model_name = model_id.rsplit('.').next().unwrap_or(&model_id).to_string();

        Ok(Some(ModelChange::MovePropertiesFile {
            model_id,
            model_name,
            patch_path: Some(patch_path.to_path_buf()),
            new_path,
        }))
    } else {
        Err(failure)
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#rejoining-of-upstream-concepts
fn rejoining_of_upstream_concepts(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    config: &Config,
) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::RejoiningOfUpstreamConcepts) {
        return Ok(());
    }
    let base_dependencies = &model.__base_attr__.depends_on.nodes;

    let mut rejoined: BTreeSet<String> = BTreeSet::new();

    for upstream_id in base_dependencies {
        if let Some(DbtNode::Model(upstream_model)) = manifest.nodes.get(upstream_id) {
            let upstream_dependencies = &upstream_model.__base_attr__.depends_on.nodes;
            for dep in upstream_dependencies {
                if base_dependencies.contains(dep) {
                    rejoined.insert(dep.clone());
                }
            }
        }
    }

    if rejoined.is_empty() {
        Ok(())
    } else {
        Err(ModelFailure::RejoiningOfUpstreamConcepts(
            rejoined.into_iter().collect(),
        ))
    }
}

fn public_model_without_contract(
    model: &ManifestModel,
    config: &Config,
) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::PublicModelsWithoutContract) {
        return Ok(());
    }
    if is_public_model(model) && !model.__base_attr__.contract.enforced {
        Err(ModelFailure::PublicModelWithoutContract)
    } else {
        Ok(())
    }
}

fn missing_required_tests(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    config: &Config,
) -> Result<(), ModelFailure> {
    if config.required_tests.is_empty() {
        return Ok(());
    }

    let existing_tests: Vec<String> = manifest
        .child_map
        .get(&model.__common_attr__.unique_id)
        .into_iter()
        .flat_map(|children| children.iter())
        .filter(|id| id.starts_with("test."))
        .filter_map(|test_id| {
            manifest.nodes.get(test_id).and_then(|node| match node {
                DbtNode::Test(test) => Some(test.test_metadata.as_ref()?.name.clone()),
                _ => None,
            })
        })
        .collect();
    let has_required_test = existing_tests
        .iter()
        .any(|test_name| config.required_tests.contains(test_name));

    if has_required_test {
        Ok(())
    } else {
        Err(ModelFailure::MissingRequiredTests(
            config.required_tests.clone(),
        ))
    }
}

// Column checking moved into `src/check/columns.rs`.

// helper functions
fn is_public_model(model: &ManifestModel) -> bool {
    model.config.access == Some(dbt_schemas::schemas::common::Access::Public)
}

fn model_type(model: &ManifestModel) -> &str {
    // crude heuristic based on file path
    // TODO: make this configurable or at least more robust
    let ofp = &model.__common_attr__.original_file_path;
    if ofp.starts_with("models/staging/") {
        "staging"
    } else if ofp.starts_with("models/marts/") {
        "mart"
    } else if ofp.starts_with("models/intermediate/") {
        "intermediate"
    } else {
        "other"
    }
}

/// A model is considered dead if it has no downstream dependencies.
/// Tests, Unit tests, do not count as dependencies
fn dead_model(
    model: &ManifestModel,
    manifest: &DbtManifestV12,
    config: &Config,
) -> Result<(), ModelFailure> {
    if !config.is_selected(Selector::DeadModel) {
        return Ok(());
    }
    // A model is considered dead if no other models depend on it
    let model_id = &model.__common_attr__.unique_id;
    let child_map = &manifest.child_map;
    let is_dead = match child_map.get(model_id) {
        None => true,
        Some(children) => {
            let downstream_models: Vec<&String> = children
                .iter()
                .filter(|id| !id.starts_with("test.") && !id.starts_with("unit_test."))
                .collect();
            downstream_models.is_empty()
        }
    };
    if is_dead {
        Err(ModelFailure::DeadModel)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use dbt_schemas::schemas::manifest::ManifestModel;
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
    fn check_model_returns_column_changes() {
        let manifest = manifest_with_inheritable_column();
        let prior_changes = BTreeMap::<String, ModelChanges>::new();

        let config = Config {
            select: vec![
                Selector::MissingModelDescriptions,
                Selector::MissingColumnDescriptions,
            ],
            ..Default::default()
        }
        .with_fix(true);
        let model_result = check_model(&manifest, "model.test.downstream", &prior_changes, &config);

        let changes = model_result
            .changes()
            .cloned()
            .expect("expected column changes to be recorded");

        assert!(model_result.is_pass(), "model should pass after fixes");
        assert!(
            model_result
                .column_results
                .values()
                .all(ColumnResult::is_pass)
        );
        assert_eq!(changes.model_id, "model.test.downstream");
        let column_set = changes
            .column_changes
            .get("customer_id")
            .expect("customer_id column should be present");
        let change = column_set.iter().next().expect("change entry should exist");
        assert!(matches!(change, ColumnChange::ChangePropertiesFile));

        let mut saw_property_change = false;
        for change in changes.changes.iter() {
            if let ModelChange::ChangePropertiesFile { property, .. } = change {
                saw_property_change = true;
                let prop = property.as_ref().expect("property payload attached");
                assert_eq!(
                    prop.description.as_deref(),
                    Some("Auto-generated description")
                );
                let customer_column = prop
                    .columns
                    .iter()
                    .find(|col| col.name == "customer_id")
                    .expect("column payload should include customer_id");
                assert_eq!(
                    customer_column.description.as_deref(),
                    Some("Upstream description")
                );
            }
        }
        assert!(saw_property_change, "expected model-level property change");

        let column_result = model_result
            .column_results
            .get("customer_id")
            .expect("customer_id column should be present");
        assert!(column_result.is_pass(), "expected column to pass");
        let change = column_result
            .changes()
            .first()
            .expect("expected change entry for column");
        assert!(matches!(change, ColumnChange::ChangePropertiesFile));
    }

    #[test]
    fn missing_required_tests_returns_true_when_no_children_present() {
        let mut manifest = DbtManifestV12::default();
        let model_id = "model.test.without_children".to_string();
        manifest
            .nodes
            .insert(model_id.clone(), DbtNode::Model(Default::default()));

        if let Some(DbtNode::Model(model)) = manifest.nodes.get_mut(&model_id) {
            model.__common_attr__.unique_id = model_id.clone();
        } else {
            panic!("expected model to be inserted");
        }

        let model = match manifest.nodes.get(&model_id) {
            Some(DbtNode::Model(model)) => model,
            _ => panic!("expected model node"),
        };

        let config = Config {
            required_tests: vec!["unique".to_string()],
            ..Default::default()
        };

        assert!(missing_required_tests(&manifest, model, &config).is_err());
    }

    #[test]
    fn test_direct_join_to_source() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec![
            "model.test.upstream".to_string(),
            "source.test.raw_layer.orders".to_string(),
        ];
        let config = Config::default();
        assert!(direct_join_to_source(&model, &config).is_err());
    }

    #[test]
    fn test_missing_model_description_invalid_marker() {
        let mut model = ManifestModel::default();
        model.__common_attr__.description = Some("FILL ME OUT".to_string());

        let config = Config::default();
        assert!(missing_model_description(&mut model, &config).is_err());
    }

    #[test]
    fn test_direct_join_to_source_single_dependency() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec!["source.test.raw_layer.orders".to_string()];
        let config = Config::default();
        assert!(direct_join_to_source(&model, &config).is_ok());
    }

    #[test]
    fn test_direct_join_to_source_no_sources() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec![
            "model.test.upstream".to_string(),
            "model.test.another_upstream".to_string(),
        ];
        let config = Config::default();
        assert!(direct_join_to_source(&model, &config).is_ok());
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

        assert!(
            model_fanout(&manifest, "model.test.one_model", &config).is_ok(),
            "only 1 downstream"
        );
        assert!(
            model_fanout(&manifest, "model.test.lots_of_tests", &config).is_ok(),
            "lots of tests should not trigger"
        );
        assert!(
            model_fanout(&manifest, "model.test.four_models", &config).is_err(),
            "4 models exceeds threshold of 1"
        );
    }

    #[test]
    fn test_root_model() {
        let manifest = manifest_with_inheritable_column();
        let model_id = "model.test.upstream";
        let model = match manifest.nodes.get(model_id) {
            Some(DbtNode::Model(model)) => model,
            _ => panic!("expected model node"),
        };
        let config = Config::default();
        assert!(root_model(model, &config).is_err());
    }

    #[test]
    fn test_multiple_sources_joined() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec![
            "source.test.raw_layer.orders".to_string(),
            "source.test.raw_layer.customers".to_string(),
        ];
        let config = Config::default();
        assert!(
            multiple_sources_joined(&model, &config).is_err(),
            "2 sources should trigger"
        );
    }

    #[test]
    fn test_rejoining_of_upstream_concepts() {
        let mut manifest = DbtManifestV12::default();

        manifest.nodes.insert(
            "model.test.upstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.test.midstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.test.downstream".to_string(),
            DbtNode::Model(Default::default()),
        );

        if let Some(DbtNode::Model(upstream)) = manifest.nodes.get_mut("model.test.upstream") {
            upstream.__common_attr__.unique_id = "model.test.upstream".to_string();
        }

        if let Some(DbtNode::Model(midstream)) = manifest.nodes.get_mut("model.test.midstream") {
            midstream.__common_attr__.unique_id = "model.test.midstream".to_string();
            midstream.__base_attr__.depends_on.nodes = vec!["model.test.upstream".to_string()];
        }

        if let Some(DbtNode::Model(downstream)) = manifest.nodes.get_mut("model.test.downstream") {
            downstream.__common_attr__.unique_id = "model.test.downstream".to_string();
            downstream.__base_attr__.depends_on.nodes = vec![
                "model.test.upstream".to_string(),
                "model.test.midstream".to_string(),
            ];
        }

        let Some(DbtNode::Model(downstream)) = manifest.nodes.get("model.test.downstream") else {
            panic!("expected downstream model");
        };
        let config = Config::default();
        assert!(rejoining_of_upstream_concepts(&manifest, downstream, &config).is_err());
    }

    #[test]
    fn test_missing_primary_key() {
        let mut model = ManifestModel::default();
        model.__common_attr__.unique_id = "model.test".to_string();
        model.primary_key = Some(vec![]);
        let config = Config::default();
        assert!(missing_primary_key(&model, &config).is_err());
    }

    #[test]
    fn test_missing_properties_file_for_model() {
        let mut model = ManifestModel::default();
        model.__common_attr__.patch_path = None;
        let node = DbtNode::Model(model);
        assert!(missing_properties_file(&node, &Config::default()).is_some());
    }

    #[test]
    fn test_public_model_without_contract() {
        let mut model = ManifestModel::default();
        model.__common_attr__.unique_id = "model.test".to_string();
        model.config.access = Some(dbt_schemas::schemas::common::Access::Public);
        model.__base_attr__.contract.enforced = false;
        let config = Config {
            select: vec![Selector::PublicModelsWithoutContract],
            ..Default::default()
        };
        assert!(public_model_without_contract(&model, &config).is_err());
    }

    #[test]
    fn test_dead_model_detected_when_no_downstreams() {
        let mut manifest = DbtManifestV12::default();
        let model_id = "model.test.alone".to_string();
        manifest
            .nodes
            .insert(model_id.clone(), DbtNode::Model(Default::default()));

        if let Some(DbtNode::Model(model)) = manifest.nodes.get_mut(&model_id) {
            model.__common_attr__.unique_id = model_id.clone();
        } else {
            panic!("expected model to be inserted");
        }

        let model = match manifest.nodes.get(&model_id) {
            Some(DbtNode::Model(model)) => model,
            _ => panic!("expected model node"),
        };
        let config = Config {
            select: vec![Selector::DeadModel],
            ..Default::default()
        };
        assert!(dead_model(model, &manifest, &config).is_err());
    }
}
