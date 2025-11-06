use crate::change_descriptors::{ColumnChange, ModelChanges};
use crate::config::{Config, Selector};
use crate::osmosis::get_upstream_col_desc;
use dbt_schemas::schemas::dbt_column::DbtColumnRef;
use dbt_schemas::schemas::manifest::{DbtManifestV12, ManifestModel};
use std::collections::BTreeMap;
use std::sync::Arc;
use strum::AsRefStr;

#[derive(Debug, Clone, Default)]
pub struct ColumnResult {
    pub column_name: String,
    pub failures: Vec<ColumnFailure>,
    pub changes: Vec<ColumnChange>,
}

impl ColumnResult {
    pub fn is_pass(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn is_failure(&self) -> bool {
        !self.is_pass()
    }

    pub fn changes(&self) -> &[ColumnChange] {
        &self.changes
    }

    pub fn failure_reasons(&self) -> Vec<String> {
        self.failures
            .iter()
            .map(|failure| match failure {
                ColumnFailure::DescriptionMissing => {
                    format!("Column `{}`: Missing Description", self.column_name)
                }
            })
            .collect()
    }
}

impl std::fmt::Display for ColumnResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_pass() {
            write!(f, "ColumnResult: Pass:{}", self.column_name)
        } else {
            writeln!(f, "ColumnResult: Fail:{}", self.column_name)?;
            for reason in self.failure_reasons() {
                writeln!(f, "    {reason}")?;
            }
            Ok(())
        }
    }
}

// Column behavior and writeback coordination now flow through `ModelChange` descriptors.

#[derive(Debug, Clone, Copy, AsRefStr, PartialEq, Eq)]
pub enum ColumnFailure {
    DescriptionMissing,
}

impl std::fmt::Display for ColumnFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[allow(clippy::match_single_binding)] // to allow future expansion
        let extra_info = match self {
            _ => String::new(),
        };
        write!(f, "{}{}", self.as_ref(), extra_info)
    }
}

/// Check if a column is missing a description.
/// A description is considered missing if it is:
/// - None
/// - An empty string (after trimming)
/// - Matches any of the configured invalid descriptions (case-insensitive, after trimming)
pub fn missing_description(column: &DbtColumnRef, config: &Config) -> Result<(), ColumnFailure> {
    match column.description.as_ref().map(|s| s.trim()) {
        Some(desc)
            if !desc.is_empty()
                && !config
                    .invalid_descriptions
                    .iter()
                    .any(|bad| bad.eq_ignore_ascii_case(desc)) =>
        {
            Ok(())
        }
        _ => Err(ColumnFailure::DescriptionMissing),
    }
}

/// Top-level entrypoint for checking all columns on a model.
pub fn check_model_columns(
    manifest: &DbtManifestV12,
    original_model: &ManifestModel,
    working_model: &mut ManifestModel,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> BTreeMap<String, ColumnResult> {
    let mut results: BTreeMap<String, ColumnResult> = BTreeMap::new();

    for (original_column, working_column) in original_model
        .__base_attr__
        .columns
        .iter()
        .zip(working_model.__base_attr__.columns.iter_mut())
    {
        let result = check_model_column(
            manifest,
            original_model,
            original_column,
            working_column,
            prior_changes,
            config,
        );
        results.insert(original_column.as_ref().name.clone(), result);
    }

    results
}

/// Check a single column. `original_column` is the Arc-wrapped original column and
/// `working_column` is a mutable reference into the cloned working model so we can
/// apply fixes in-place.
fn check_model_column(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    original_column: &DbtColumnRef,
    working_column: &mut DbtColumnRef,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> ColumnResult {
    let mut failures: Vec<ColumnFailure> = Vec::new();
    let mut changes: Vec<ColumnChange> = Vec::new();
    match missing_column_description(
        manifest,
        model,
        original_column,
        working_column,
        prior_changes,
        config,
    ) {
        Ok(Some(change)) => changes.push(change),
        Ok(None) => {}
        Err(failure) => failures.push(failure),
    }

    ColumnResult {
        column_name: original_column.name.clone(),
        failures,
        changes,
    }
}

/// Try to populate a missing column description from upstream if configured.
/// Returns Ok(Some(Change)) if a change was applied, Ok(None) if no-op, or
/// Err(ColumnFailure) if the column is considered failing and no fix was applied.
fn missing_column_description(
    manifest: &DbtManifestV12,
    model: &ManifestModel,
    original_column: &DbtColumnRef,
    working_column: &mut DbtColumnRef,
    prior_changes: &BTreeMap<String, ModelChanges>,
    config: &Config,
) -> Result<Option<ColumnChange>, ColumnFailure> {
    // If the selector is not enabled, or the column already has a description,
    // skip attempting to source a description from upstream.
    if !config.is_selected(Selector::MissingColumnDescriptions)
        || missing_description(original_column, config).is_ok()
    {
        return Ok(None);
    }

    if !config.is_fixable(Selector::MissingColumnDescriptions) {
        return Err(ColumnFailure::DescriptionMissing);
    }
    if let Some(new_description_text) = get_upstream_col_desc(
        manifest,
        Some(prior_changes),
        &model.__common_attr__.unique_id,
        original_column.name.as_str(),
        config,
    ) {
        let column_mut = Arc::make_mut(working_column);
        column_mut.description = Some(new_description_text);

        Ok(Some(ColumnChange::ChangePropertiesFile))
    } else {
        Err(ColumnFailure::DescriptionMissing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use dbt_schemas::schemas::dbt_column::DbtColumn;
    use std::sync::Arc;

    #[test]
    fn test_missing_description_invalid_markers() {
        let col_tbd = Arc::new(DbtColumn {
            name: "id".to_string(),
            description: Some("TBD".to_string()),
            ..Default::default()
        });

        let config = Config::default();
        assert!(missing_description(&col_tbd, &config).is_err());

        let col_fill = Arc::new(DbtColumn {
            name: "id".to_string(),
            description: Some("  fill me out  ".to_string()),
            ..Default::default()
        });
        // default invalid_descriptions contains "FILL ME OUT", trimmed and case-insensitive
        assert!(missing_description(&col_fill, &config).is_err());

        let col_ok = Arc::new(DbtColumn {
            name: "id".to_string(),
            description: Some("A proper description".to_string()),
            ..Default::default()
        });
        assert!(missing_description(&col_ok, &config).is_ok());
    }
}
