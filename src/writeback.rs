use crate::check::ModelChanges;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WriteBackError {
    #[error("model `{model_id}` is missing a patch path in the manifest")]
    PatchPathMissing { model_id: String },
    #[error("python helper script not found at `{0}`")]
    HelperMissing(PathBuf),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to serialize request payload: {0}")]
    SerializeFailure(#[from] serde_json::Error),
    #[error("python helper exited with status {status}: {stderr}")]
    PythonFailure { status: i32, stderr: String },
    #[error("failed to parse python helper response: {0}")]
    ResponseParseFailure(serde_json::Error),
}

#[derive(Debug, Serialize)]
struct PythonColumnChange<'a> {
    column_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_description: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct PythonRequest<'a> {
    patch_path: &'a Path,
    model_name: &'a str,
    column_changes: Vec<PythonColumnChange<'a>>,
}

#[derive(Debug, Deserialize)]
struct PythonResponse {
    updated_columns: Vec<String>,
}

pub fn apply_model_changes_with_ruamel(
    project_root: &Path,
    changes: &BTreeMap<String, ModelChanges>,
) -> Result<Vec<(String, Vec<String>)>, WriteBackError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

    let helper_path = resolve_helper_path()?;

    let mut results = Vec::new();

    for model_changes in changes.values() {
        let patch_path =
            model_changes
                .patch_path
                .as_ref()
                .ok_or_else(|| WriteBackError::PatchPathMissing {
                    model_id: model_changes.model_id.clone(),
                })?;

        let resolved_path = if patch_path.is_absolute() {
            patch_path.clone()
        } else {
            project_root.join(patch_path)
        };

        let model_name = extract_model_name(&model_changes.model_id);

        let mut column_changes = Vec::new();
        for change_set in model_changes.column_changes.values() {
            for change in change_set {
                column_changes.push(PythonColumnChange {
                    column_name: &change.column_name,
                    new_description: change.new_description.as_deref(),
                });
            }
        }

        if column_changes.is_empty() {
            continue;
        }

        let request = PythonRequest {
            patch_path: &resolved_path,
            model_name,
            column_changes,
        };

        let response = invoke_python_helper(&helper_path, &request)?;
        results.push((model_changes.model_id.clone(), response.updated_columns));
    }

    Ok(results)
}

fn resolve_helper_path() -> Result<PathBuf, WriteBackError> {
    if let Ok(path) = env::var("DBT_LINT_YAML_HELPER") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
        return Err(WriteBackError::HelperMissing(path));
    }

    let mut candidates = Vec::new();

    if let Ok(exe_path) = env::current_exe()
        && let Some(dir) = exe_path.parent()
    {
        candidates.push(dir.join("ruamel_model_changes.py"));
        candidates.push(dir.join("scripts").join("ruamel_model_changes.py"));
    }

    let fallback =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/ruamel_model_changes.py");
    candidates.push(fallback.clone());

    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }

    Err(WriteBackError::HelperMissing(fallback))
}

fn invoke_python_helper(
    helper_path: &Path,
    request: &PythonRequest<'_>,
) -> Result<PythonResponse, WriteBackError> {
    let mut command = Command::new("python3");
    command.arg(helper_path);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let mut child = command.spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        let json = serde_json::to_vec(request)?;
        stdin.write_all(&json)?;
    }

    let output = child.wait_with_output()?;

    if !output.status.success() {
        let status = output.status.code().unwrap_or(-1);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(WriteBackError::PythonFailure { status, stderr });
    }

    let response: PythonResponse =
        serde_json::from_slice(&output.stdout).map_err(WriteBackError::ResponseParseFailure)?;

    Ok(response)
}

fn extract_model_name(unique_id: &str) -> &str {
    unique_id.rsplit('.').next().unwrap_or(unique_id)
}
