use crate::change_descriptors::SourceChange;
use crate::check::{ModelChanges, SourceChanges};
use std::{collections::BTreeMap, path::Path};
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

/// Apply source-level file moves (e.g. relocating a source YAML to its correct directory).
///
/// Currently handles `MoveSourceToFile` by renaming the file and creating the destination
/// directory if needed. `ChangePropertiesFile` changes are not yet applied here.
pub fn apply_source_changes(
    project_root: &Path,
    changes: &BTreeMap<String, SourceChanges>,
) -> Result<Vec<String>, WriteBackError> {
    let mut applied = Vec::new();
    for source_changes in changes.values() {
        for change in &source_changes.changes {
            match change {
                SourceChange::MoveSourceToFile {
                    source_id,
                    old_patch_path,
                    new_patch_path,
                    ..
                } => {
                    let src = if old_patch_path.is_absolute() {
                        old_patch_path.clone()
                    } else {
                        project_root.join(old_patch_path)
                    };
                    let dst = if new_patch_path.is_absolute() {
                        new_patch_path.clone()
                    } else {
                        project_root.join(new_patch_path)
                    };
                    if let Some(parent) = dst.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::rename(&src, &dst)?;
                    applied.push(source_id.clone());
                }
                SourceChange::ChangePropertiesFile { .. } => {
                    // Not yet implemented in the writeback layer.
                }
            }
        }
    }
    Ok(applied)
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
