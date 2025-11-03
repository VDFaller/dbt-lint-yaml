use crate::change_descriptors::{SourceChange, SourceChanges};
use crate::check::columns::missing_description;
use crate::config::{Config, Selector};
use crate::writeback::properties::source_property_from_manifest_differences;
use dbt_schemas::schemas::manifest::{DbtManifestV12, ManifestSource};
use std::fmt::Display;
use strum::AsRefStr;

#[derive(Debug, Clone, AsRefStr, PartialEq, Eq)]
pub enum SourceFailure {
    MissingDescription,
    DuplicateDefinition(String),
    UnusedSource,
    MissingFreshness,
    MissingSourceDescription,
    SourceTableColumnDescriptions,
    SourceFanout,
}
impl Display for SourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceFailure::DuplicateDefinition(duplicate_id) => {
                write!(f, "DuplicateDefinition:{duplicate_id}")
            }
            _ => f.write_str(self.as_ref()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SourceResult {
    pub source_id: String,
    pub failures: Vec<SourceFailure>,
    pub changes: Option<SourceChanges>,
}

impl SourceResult {
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn is_pass(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn is_failure(&self) -> bool {
        !self.is_pass()
    }

    pub fn as_failure(&self) -> Option<&Self> {
        self.is_failure().then_some(self)
    }

    pub fn failure_reasons(&self) -> Vec<String> {
        self.failures.iter().map(ToString::to_string).collect()
    }

    pub fn changes(&self) -> Option<&SourceChanges> {
        self.changes.as_ref()
    }
}

impl Display for SourceResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_pass() {
            write!(f, "SourceResult: Pass:{}", self.source_id)
        } else {
            writeln!(f, "SourceResult: Fail:{}", self.source_id)?;
            for reason in self.failure_reasons() {
                writeln!(f, "    {reason}")?;
            }
            Ok(())
        }
    }
}

pub fn check_sources(manifest: &DbtManifestV12, config: &Config) -> Vec<SourceResult> {
    manifest
        .sources
        .values()
        .map(|source| check_source(manifest, source, config))
        .collect()
}

fn check_source(
    manifest: &DbtManifestV12,
    source: &ManifestSource,
    config: &Config,
) -> SourceResult {
    let mut working_source = source.clone();
    let source_id = working_source.__common_attr__.unique_id.clone();
    let source_name = working_source.source_name.clone();
    let table_name = working_source.__common_attr__.name.clone();
    let patch_path = working_source.__common_attr__.patch_path.clone();

    let mut failures = Vec::new();
    let mut source_level_changes: Vec<SourceChange> = Vec::new();
    let mut property_change_required = false;

    match missing_source_table_description(&mut working_source, config) {
        Ok(Some(change)) => {
            property_change_required = true;
            source_level_changes.push(change);
        }
        Ok(None) => {}
        Err(failure) => failures.push(failure),
    }

    if let Err(failure) = missing_source_column_descriptions(&mut working_source, config) {
        failures.push(failure);
    }

    if let Err(failure) = duplicate_source(manifest, source, config) {
        failures.push(failure);
    }

    if let Err(failure) = unused_source(manifest, source, config) {
        failures.push(failure);
    }

    if let Err(failure) = missing_source_freshness(source, config) {
        failures.push(failure);
    }

    if let Err(failure) = missing_source_description(&working_source, config) {
        failures.push(failure);
    }

    if let Err(failure) = source_fanout(manifest, source, config) {
        failures.push(failure);
    }

    let mut changes = if source_level_changes.is_empty() {
        None
    } else {
        Some(SourceChanges {
            source_id: source_id.clone(),
            source_name: source_name.clone(),
            table_name: table_name.clone(),
            patch_path: patch_path.clone(),
            changes: source_level_changes,
        })
    };

    if config.fix
        && property_change_required
        && let Some(property) = source_property_from_manifest_differences(source, &working_source)
    {
        let mut applied = false;
        if let Some(changes_ref) = changes.as_mut() {
            for change in &mut changes_ref.changes {
                match change {
                    SourceChange::ChangePropertiesFile { property: slot, .. } => {
                        *slot = Some(property.clone());
                        applied = true;
                    }
                }
            }
        }

        if !applied {
            let change = SourceChange::ChangePropertiesFile {
                source_id: source_id.clone(),
                source_name: source_name.clone(),
                table_name: table_name.clone(),
                patch_path: patch_path.clone(),
                property: Some(property),
            };

            if let Some(changes_ref) = changes.as_mut() {
                changes_ref.changes.push(change);
            } else {
                changes = Some(SourceChanges {
                    source_id: source_id.clone(),
                    source_name: source_name.clone(),
                    table_name: table_name.clone(),
                    patch_path: patch_path.clone(),
                    changes: vec![change],
                });
            }
        }
    }

    SourceResult {
        source_id,
        failures,
        changes,
    }
}

/// Check if a source table is missing a description.
/// A description is considered missing if it is:
/// - None
/// - An empty string (after trimming)
/// - Matches any of the configured invalid descriptions (case-insensitive, after trimming)
fn missing_source_table_description(
    source: &mut ManifestSource,
    config: &Config,
) -> Result<Option<SourceChange>, SourceFailure> {
    if !config.is_selected(Selector::MissingSourceTableDescriptions) {
        return Ok(None);
    }
    let is_missing = match source.__common_attr__.description.as_ref() {
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

    if config.is_fixable(Selector::MissingSourceTableDescriptions) {
        source.__common_attr__.description = Some("Auto-generated description".to_string());
        let change = SourceChange::ChangePropertiesFile {
            source_id: source.__common_attr__.unique_id.clone(),
            source_name: source.source_name.clone(),
            table_name: source.__common_attr__.name.clone(),
            patch_path: source.__common_attr__.patch_path.clone(),
            property: None,
        };
        Ok(Some(change))
    } else {
        Err(SourceFailure::MissingDescription)
    }
}

/// Check that every column on a source table has a non-empty description.
fn missing_source_column_descriptions(
    source: &mut ManifestSource,
    config: &Config,
) -> Result<(), SourceFailure> {
    if !config.is_selected(Selector::MissingSourceColumnDescriptions) {
        return Ok(());
    }

    let has_missing = source
        .columns
        .iter()
        .any(|col| missing_description(col, config).is_err());

    if has_missing {
        Err(SourceFailure::SourceTableColumnDescriptions)
    } else {
        Ok(())
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/documentation/#undocumented-sources
/// Check if a source is missing a description.
/// A description is considered missing if it is:
/// - None
/// - An empty string (after trimming)
/// - Matches any of the configured invalid descriptions (case-insensitive, after trimming)
fn missing_source_description(
    source: &ManifestSource,
    config: &Config,
) -> Result<(), SourceFailure> {
    if !config.is_selected(Selector::MissingSourceDescriptions) {
        return Ok(());
    }
    let trimmed = source.source_description.trim();
    let is_missing = trimmed.is_empty()
        || config
            .invalid_descriptions
            .iter()
            .any(|bad| bad.eq_ignore_ascii_case(trimmed));

    if !is_missing {
        return Ok(());
    }
    Err(SourceFailure::MissingSourceDescription)
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#source-fanout
fn source_fanout(
    manifest: &DbtManifestV12,
    source: &ManifestSource,
    config: &Config,
) -> Result<(), SourceFailure> {
    if !config.is_selected(Selector::SourceFanout) {
        return Ok(());
    }

    let downstream_count = manifest
        .child_map
        .get(&source.__common_attr__.unique_id)
        .map(|children| children.len())
        .unwrap_or(0);

    if downstream_count > 1 {
        return Err(SourceFailure::SourceFanout);
    }
    Ok(())
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#duplicate-sources
fn duplicate_source(
    manifest: &DbtManifestV12,
    source: &ManifestSource,
    config: &Config,
) -> Result<(), SourceFailure> {
    if !config.is_selected(Selector::DuplicateSources)
        || source.__common_attr__.name == source.identifier
    {
        return Ok(());
    }
    // TODO: look into performance of this search in a larger project
    if let Some(duplicate) = manifest.sources.values().find(|s| {
        s.identifier == source.identifier
            && s.source_name == source.source_name
            && s.__common_attr__.unique_id != source.__common_attr__.unique_id
    }) {
        Err(SourceFailure::DuplicateDefinition(
            duplicate.__common_attr__.unique_id.clone(),
        ))
    } else {
        Ok(())
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#unused-sources
fn unused_source(
    manifest: &DbtManifestV12,
    source: &ManifestSource,
    config: &Config,
) -> Result<(), SourceFailure> {
    // A source is considered "used" if any model depends on it
    if !config.is_selected(Selector::UnusedSources) {
        return Ok(());
    }
    let has_downstream = manifest
        .child_map
        .get(&source.__common_attr__.unique_id)
        .map(|children| !children.is_empty())
        .unwrap_or(false);

    if !has_downstream {
        return Err(SourceFailure::UnusedSource);
    }
    Ok(())
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/testing/#missing-source-freshness
fn missing_source_freshness(source: &ManifestSource, config: &Config) -> Result<(), SourceFailure> {
    if !config.is_selected(Selector::MissingSourceFreshness) {
        return Ok(());
    }
    if let Some(freshness) = &source.freshness {
        if freshness.warn_after.is_none() && freshness.error_after.is_none() {
            Err(SourceFailure::MissingFreshness)
        } else {
            Ok(())
        }
    } else {
        Err(SourceFailure::MissingFreshness)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::vec;

    use super::*;
    use crate::config::Config;
    use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessPeriod, FreshnessRules};
    use dbt_schemas::schemas::dbt_column::DbtColumn;
    use dbt_schemas::schemas::manifest::ManifestSource;
    use std::sync::Arc;

    #[test]
    fn test_missing_source_description() {
        let source = ManifestSource {
            source_description: String::new(),
            ..Default::default()
        };
        let config = Config::default();
        let result = missing_source_description(&source, &config);
        assert!(matches!(
            result,
            Err(SourceFailure::MissingSourceDescription)
        ));
    }

    #[test]
    fn test_missing_source_description_invalid_marker() {
        let source = ManifestSource {
            source_description: "tbd".to_string(),
            ..Default::default()
        };

        let config = Config::default();
        let result = missing_source_description(&source, &config);
        assert!(matches!(
            result,
            Err(SourceFailure::MissingSourceDescription)
        ));
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_duplicate_source() {
        let mut manifest = DbtManifestV12::default();
        let mut source = ManifestSource::default();
        source.identifier = "orders".to_string();
        source.source_name = "raw".to_string();
        source.__common_attr__.name = "different".to_string();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        manifest
            .sources
            .insert(source.__common_attr__.unique_id.clone(), source.clone());

        let mut duplicate = ManifestSource::default();
        duplicate.identifier = "orders".to_string();
        duplicate.source_name = "raw".to_string();
        duplicate.__common_attr__.unique_id = "source.raw.orders_dupe".to_string();
        manifest.sources.insert(
            duplicate.__common_attr__.unique_id.clone(),
            duplicate.clone(),
        );

        let err = duplicate_source(&manifest, &duplicate, &Config::default())
            .expect_err("expected duplicate source failure");
        assert_eq!(
            err,
            SourceFailure::DuplicateDefinition("source.raw.orders".to_string())
        );
    }

    #[test]
    fn test_unused_source() {
        let mut manifest = DbtManifestV12::default();
        let mut source = ManifestSource::default();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        manifest
            .child_map
            .insert(source.__common_attr__.unique_id.clone(), vec![]);
        let config = Config::default();
        let result = unused_source(&manifest, &source, &config);
        assert!(matches!(result, Err(SourceFailure::UnusedSource)));
    }

    #[test]
    fn test_missing_source_freshness() {
        let mut source = ManifestSource::default();
        let mut freshness = FreshnessDefinition::default();
        source.freshness = Some(freshness.clone());
        let config = Config::default();
        assert!(matches!(
            missing_source_freshness(&source, &config),
            Err(SourceFailure::MissingFreshness)
        ));

        freshness.warn_after = Some(FreshnessRules {
            count: Some(1),
            period: Some(FreshnessPeriod::day),
        });
        source.freshness = Some(freshness.clone());
        assert!(missing_source_freshness(&source, &config).is_ok());
    }

    #[test]
    fn test_check_source_failure() {
        let mut manifest = DbtManifestV12::default();
        let mut source = ManifestSource::default();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        manifest
            .sources
            .insert(source.__common_attr__.unique_id.clone(), source.clone());
        manifest
            .child_map
            .insert(source.__common_attr__.unique_id.clone(), vec![]);

        let config = Config::default();
        let result = check_source(&manifest, &source, &config);
        assert!(result.is_failure());
    }

    #[test]
    fn test_source_fanout() {
        let mut child_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        child_map.insert(
            "source.raw.orders".to_string(),
            vec![
                "model.test.orders".to_string(),
                "model.test.orders_summary".to_string(),
            ],
        );
        let mut sources: BTreeMap<String, ManifestSource> = BTreeMap::new();
        let mut source = ManifestSource::default();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        sources.insert(source.__common_attr__.unique_id.clone(), source.clone());
        let manifest = DbtManifestV12 {
            child_map,
            sources,
            ..Default::default()
        };
        let source = manifest.sources.get("source.raw.orders").unwrap();
        let config = Config::default();
        assert!(matches!(
            source_fanout(&manifest, source, &config),
            Err(SourceFailure::SourceFanout)
        ));
    }

    #[test]
    fn test_missing_source_table_column_descriptions() {
        let mut source = ManifestSource::default();
        // create a column without a description
        let col = DbtColumn {
            name: "id".to_string(),
            description: None,
            ..Default::default()
        };
        source.columns.push(Arc::new(col));

        let config = Config {
            select: vec![Selector::MissingSourceColumnDescriptions],
            ..Default::default()
        };
        assert!(matches!(
            missing_source_column_descriptions(&mut source, &config),
            Err(SourceFailure::SourceTableColumnDescriptions)
        ));
    }

    #[test]
    fn test_missing_source_table_description_invalid_marker() {
        let mut source = ManifestSource::default();
        source.__common_attr__.description = Some("TBD".to_string());

        let config = Config::default();
        assert!(matches!(
            missing_source_table_description(&mut source, &config),
            Err(SourceFailure::MissingDescription)
        ));
    }

    #[test]
    fn test_source_table_column_descriptions_all_present() {
        let mut source = ManifestSource::default();
        let col = DbtColumn {
            name: "id".to_string(),
            description: Some("identifier".to_string()),
            ..Default::default()
        };
        source.columns.push(Arc::new(col));

        let config = Config::default();
        assert!(missing_source_column_descriptions(&mut source, &config).is_ok());
    }
}
