use crate::check::ModelChanges;
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;

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
    #[error("yaml error: {0}")]
    Yaml(#[from] dbt_serde_yaml::Error),
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
