use super::RuleOutcome;
use super::columns::{self, ColumnChange, ColumnFailure, ColumnResult};
use crate::{
    config::{Config, Selector},
    osmosis::get_upstream_col_desc,
};
use dbt_schemas::schemas::{
    dbt_column::DbtColumnRef,
    manifest::{DbtManifestV12, DbtNode, ManifestModel},
};
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

#[derive(Debug, Clone)]
pub enum ModelChange {
    MovePropertiesFile { new_path: PathBuf },
    MoveModelFile { new_path: PathBuf },
}

type ModelRuleOutcome = RuleOutcome<ModelFailure, ModelChange>;

#[derive(Default, Debug, Clone)]
pub struct ModelChanges {
    pub model_id: String,
    pub patch_path: Option<PathBuf>,
    pub changes: Vec<ModelChange>,
    pub column_changes: BTreeMap<String, BTreeSet<ColumnChange>>,
}

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
    let Some(node @ DbtNode::Model(model_meta)) = manifest.nodes.get(model_id) else {
        return ModelResult {
            model_id: model_id.to_string(),
            ..Default::default()
        };
    };

    let model_unique_id = model_meta.__common_attr__.unique_id.clone();
    let patch_path = model_meta.__common_attr__.patch_path.clone();
    let _model_type = model_type(model_meta); // currently unused

    let mut failures: Vec<ModelFailure> = Vec::new();
    let mut model_level_changes: Vec<ModelChange> = Vec::new();
    if let Some(f) = missing_model_description(model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = missing_model_tags(model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = direct_join_to_source(model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = missing_properties_file(node, config) {
        failures.push(f)
    }
    if let Some(f) = model_fanout(manifest, model_id, config) {
        failures.push(f)
    }
    if let Some(f) = missing_required_tests(manifest, model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = root_model(model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = missing_primary_key(model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = multiple_sources_joined(model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = rejoining_of_upstream_concepts(manifest, model_meta, config) {
        failures.push(f)
    }
    if let Some(f) = public_model_without_contract(model_meta, config) {
        failures.push(f)
    }
    match model_separate_from_properties_file(node, config) {
        ModelRuleOutcome::Pass => {}
        ModelRuleOutcome::Fail(failure) => failures.push(failure),
        ModelRuleOutcome::Change(change) => model_level_changes.push(change),
    }

    let column_results = check_model_columns(manifest, model_meta, prior_changes, config);

    let mut column_changes: BTreeMap<String, BTreeSet<ColumnChange>> = BTreeMap::new();
    for (column_name, column_result) in &column_results {
        for change in column_result.changes() {
            column_changes
                .entry(column_name.clone())
                .or_default()
                .insert(change.clone());
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

fn check_model_column(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    column: &DbtColumnRef,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> ColumnResult {
    let mut failures: Vec<ColumnFailure> = Vec::new();
    let mut changes: Vec<ColumnChange> = Vec::new();

    if config.is_selected(Selector::MissingColumnDescriptions)
        && let Some(failure) = columns::missing_description(column)
    {
        // Attempt to fix the missing description
        if !config.is_fixable(Selector::MissingColumnDescriptions) {
            failures.push(failure);
        } else if let Some(new_description) = get_upstream_col_desc(
            manifest,
            Some(prior_changes),
            &model.__common_attr__.unique_id,
            column.name.as_str(),
            config,
        ) {
            let old_description = column.description.clone();
            let new_description = Some(new_description);

            changes.push(ColumnChange::DescriptionChanged {
                old: old_description,
                new: new_description.clone(),
            });
        } else {
            failures.push(failure);
        }
    }

    ColumnResult {
        column_name: column.name.clone(),
        failures,
        changes,
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#multiple-sources-joined
fn missing_model_description(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::MissingModelDescriptions) {
        return None;
    }
    (model.__common_attr__.description.is_none()).then_some(ModelFailure::DescriptionMissing)
}

fn missing_model_tags(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::MissingModelTags) {
        return None;
    }
    (model.config.tags.is_none()).then_some(ModelFailure::TagsMissing(Vec::new()))
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#multiple-sources-joined
fn multiple_sources_joined(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::MultipleSourcesJoined) {
        return None;
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
        Some(ModelFailure::MultipleSourcesJoined(sources))
    } else {
        None
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#direct-join-to-source
fn direct_join_to_source(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::DirectJoinToSource) {
        return None;
    }
    let depends_on = &model.__base_attr__.depends_on.nodes;
    if depends_on.len() < 2 {
        return None;
    }
    let sources: Vec<String> = depends_on
        .iter()
        .filter(|upstream_id| upstream_id.starts_with("source."))
        .cloned()
        .collect();
    if sources.is_empty() {
        None
    } else {
        Some(ModelFailure::DirectJoinToSource(sources))
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
    missing_patch.then_some(ModelFailure::MissingPropertiesFile)
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#model-fanout
/// snapshots are not counted in fanout
fn model_fanout(
    manifest: &DbtManifestV12,
    model_id: &str,
    config: &Config,
) -> Option<ModelFailure> {
    if !config.is_selected(Selector::ModelFanout) {
        return None;
    }
    let downstream_models = manifest
        .child_map
        .get(model_id)
        .into_iter()
        .flatten()
        .filter(|id| id.starts_with("model."))
        .count();

    (downstream_models > config.model_fanout_threshold)
        .then_some(ModelFailure::ModelFanout(downstream_models))
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#root-models
fn root_model(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::RootModels) {
        return None;
    }
    (model.__base_attr__.depends_on.nodes.is_empty()).then_some(ModelFailure::RootModel)
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/testing/#missing-primary-key-tests
fn missing_primary_key(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::MissingPrimaryKey) {
        return None;
    }
    let missing_pk = model.primary_key.as_ref().unwrap_or(&vec![]).is_empty();
    missing_pk.then_some(ModelFailure::MissingPrimaryKey)
}

fn model_separate_from_properties_file(node: &DbtNode, config: &Config) -> ModelRuleOutcome {
    if !config.is_selected(Selector::ModelsSeparateFromPropertiesFile) {
        return ModelRuleOutcome::Pass;
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
        _ => return ModelRuleOutcome::Pass,
    };

    // we only care when the properties file sits in a different directory
    let Some(patch_path) = patch_path.filter(|path| path.parent() != original_path.parent()) else {
        return ModelRuleOutcome::Pass;
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
        return ModelRuleOutcome::Change(ModelChange::MovePropertiesFile { new_path });
    }

    ModelRuleOutcome::Fail(failure)
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#rejoining-of-upstream-concepts
fn rejoining_of_upstream_concepts(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    config: &Config,
) -> Option<ModelFailure> {
    if !config.is_selected(Selector::RejoiningOfUpstreamConcepts) {
        return None;
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
        None
    } else {
        Some(ModelFailure::RejoiningOfUpstreamConcepts(
            rejoined.into_iter().collect(),
        ))
    }
}

fn public_model_without_contract(model: &ManifestModel, config: &Config) -> Option<ModelFailure> {
    if !config.is_selected(Selector::PublicModelsWithoutContract) {
        return None;
    }
    if is_public_model(model) && !model.__base_attr__.contract.enforced {
        Some(ModelFailure::PublicModelWithoutContract)
    } else {
        None
    }
}

fn missing_required_tests(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    config: &Config,
) -> Option<ModelFailure> {
    if config.required_tests.is_empty() {
        return None;
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

    (!has_required_test).then_some(ModelFailure::MissingRequiredTests(
        config.required_tests.clone(),
    ))
}

fn check_model_columns(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> BTreeMap<String, ColumnResult> {
    let mut results: BTreeMap<String, ColumnResult> = BTreeMap::new();
    let columns = &model.__base_attr__.columns;

    for (col_name, column) in columns.iter() {
        let result = check_model_column(manifest, model, column, prior_changes, config);

        results.insert(col_name.clone(), result);
    }
    results
}

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
            upstream
                .__base_attr__
                .columns
                .insert("customer_id".to_string(), {
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
            downstream
                .__base_attr__
                .columns
                .insert("customer_id".to_string(), {
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

        let config = Config::default().with_fix(true);
        let model_result = check_model(&manifest, "model.test.downstream", &prior_changes, &config);

        let changes = model_result
            .changes()
            .cloned()
            .expect("expected column changes to be recorded");

        assert!(
            model_result.is_failure(),
            "model should be marked as failure"
        );
        assert!(
            model_result
                .failures()
                .contains(&ModelFailure::DescriptionMissing)
        );
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
        match change {
            ColumnChange::DescriptionChanged { new, .. } => {
                assert_eq!(new.as_deref(), Some("Upstream description"));
            }
        }

        let column_result = model_result
            .column_results
            .get("customer_id")
            .expect("customer_id column should be present");
        assert!(column_result.is_pass(), "expected column to pass");
        let change = column_result
            .changes()
            .first()
            .expect("expected change entry for column");
        match change {
            ColumnChange::DescriptionChanged { new, .. } => {
                assert_eq!(new.as_deref(), Some("Upstream description"));
            }
        }
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

        assert!(missing_required_tests(&manifest, model, &config).is_some());
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
        assert!(direct_join_to_source(&model, &config).is_some());
    }

    #[test]
    fn test_direct_join_to_source_single_dependency() {
        let mut model = ManifestModel::default();

        model.__common_attr__.unique_id = "model.test.target".to_string();
        model.__base_attr__.depends_on.nodes = vec!["source.test.raw_layer.orders".to_string()];
        let config = Config::default();
        assert!(direct_join_to_source(&model, &config).is_none());
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
        assert!(direct_join_to_source(&model, &config).is_none());
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
            model_fanout(&manifest, "model.test.one_model", &config).is_none(),
            "only 1 downstream"
        );
        assert!(
            model_fanout(&manifest, "model.test.lots_of_tests", &config).is_none(),
            "lots of tests should not trigger"
        );
        assert!(
            model_fanout(&manifest, "model.test.four_models", &config).is_some(),
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
        assert!(root_model(model, &config).is_some());
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
            multiple_sources_joined(&model, &config).is_some(),
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
        assert!(rejoining_of_upstream_concepts(&manifest, downstream, &config).is_some());
    }

    #[test]
    fn test_missing_primary_key() {
        let mut model = ManifestModel::default();
        model.__common_attr__.unique_id = "model.test".to_string();
        model.primary_key = Some(vec![]);
        let config = Config::default();
        assert!(missing_primary_key(&model, &config).is_some());
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
        assert!(public_model_without_contract(&model, &config).is_some());
    }
}
