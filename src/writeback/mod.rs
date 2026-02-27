use crate::change_descriptors::SourceChange;
use crate::check::{ModelChanges, SourceChanges};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use thiserror::Error;

pub mod changes;
pub mod properties;
pub mod python;
pub mod rust;

#[derive(Debug, Error)]
pub enum WriteBackError {
    #[error("model `{model_id}` is missing a patch path in the manifest")]
    PatchPathMissing { model_id: String },
    #[error("python helper script not found at `{0}`")]
    HelperMissing(std::path::PathBuf),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize request payload: {0}")]
    SerializeFailure(#[from] serde_json::Error),
    #[error("python helper exited with status {status}: {stderr}")]
    PythonFailure { status: i32, stderr: String },
    #[error("failed to parse python helper response: {0}")]
    ResponseParseFailure(serde_json::Error),
    #[error("unsupported model change `{change}` for model `{model_id}`")]
    UnsupportedModelChange { model_id: String, change: String },
    #[error("column `{column_name}` not found for model `{model_id}`")]
    ColumnMissing {
        model_id: String,
        column_name: String,
    },
    #[error("model `{model_id}` not found in docs")]
    ModelMissing { model_id: String },
    #[error("yaml error: {0}")]
    Yaml(#[from] dbt_serde_yaml::Error),
}

/// Apply source directory moves, dispatching to the Python or Rust writeback
/// path based on the configured writeback method.
///
/// The Python path (ruamel.yaml) preserves YAML comments and formatting.
/// The Rust path (dbt_serde_yaml) is faster but does not preserve comments.
pub fn apply_source_changes(
    project_root: &Path,
    changes: &BTreeMap<String, SourceChanges>,
    config: &crate::config::Config,
) -> Result<Vec<String>, WriteBackError> {
    let by_old_path = collect_source_moves(project_root, changes);

    match config.writeback {
        crate::config::WritebackMethod::Python => {
            python::apply_source_changes_with_python(&by_old_path)
        }
        crate::config::WritebackMethod::Rust => rust::apply_source_changes_with_rust(&by_old_path),
    }
}

/// Build the deduplicated move map from raw source changes.
///
/// Groups moves by old_path, deduplicating by source_name within each file.
/// Multiple tables share the same source block, so each (old_path, source_name)
/// pair maps to a single (new_path, Vec<source_id>) entry.
///
/// Returns: old_abs_path -> source_name -> (new_abs_path, Vec<source_id>)
pub(crate) fn collect_source_moves(
    project_root: &Path,
    changes: &BTreeMap<String, SourceChanges>,
) -> BTreeMap<PathBuf, BTreeMap<String, (PathBuf, Vec<String>)>> {
    let mut by_old_path: BTreeMap<PathBuf, BTreeMap<String, (PathBuf, Vec<String>)>> =
        BTreeMap::new();

    for source_changes in changes.values() {
        for change in &source_changes.changes {
            if let SourceChange::MoveSourceToFile {
                source_id,
                source_name,
                old_patch_path,
                new_patch_path,
            } = change
            {
                let old_abs = resolve_abs(project_root, old_patch_path);
                let new_abs = resolve_abs(project_root, new_patch_path);
                let entry = by_old_path
                    .entry(old_abs)
                    .or_default()
                    .entry(source_name.clone())
                    .or_insert_with(|| (new_abs, Vec::new()));
                entry.1.push(source_id.clone());
            }
        }
    }

    by_old_path
}

pub(crate) fn resolve_abs(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

/// Dispatch based on configured writeback method.
pub fn apply_model_changes(
    project_root: &Path,
    changes: &BTreeMap<String, ModelChanges>,
    config: &crate::config::Config,
) -> Result<Vec<(String, Vec<String>)>, WriteBackError> {
    match config.writeback {
        crate::config::WritebackMethod::Python => python::apply_with_python(project_root, changes),
        crate::config::WritebackMethod::Rust => rust::apply_with_rust(project_root, changes),
    }
}
