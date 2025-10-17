use super::WriteBackError;
use crate::check::{ColumnChange, ModelChange, ModelChanges};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

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

pub fn apply_with_python(
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

        let mut resolved_path = if patch_path.is_absolute() {
            patch_path.clone()
        } else {
            project_root.join(patch_path)
        };

        for change in &model_changes.changes {
            match change {
                ModelChange::MovePropertiesFile { new_path } => {
                    let new_resolved_path = if new_path.is_absolute() {
                        new_path.clone()
                    } else {
                        project_root.join(new_path)
                    };
                    if let Some(parent) = new_resolved_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::rename(&resolved_path, &new_resolved_path)?;
                    resolved_path = new_resolved_path;
                }
                other => {
                    return Err(WriteBackError::UnsupportedModelChange {
                        model_id: model_changes.model_id.clone(),
                        change: format!("{other:?}"),
                    });
                }
            }
        }

        let model_name = extract_model_name(&model_changes.model_id);

        let mut column_changes = Vec::new();
        for (column_name, change_set) in &model_changes.column_changes {
            for change in change_set {
                match change {
                    ColumnChange::DescriptionChanged { new, .. } => {
                        column_changes.push(PythonColumnChange {
                            column_name: column_name.as_str(),
                            new_description: new.as_deref(),
                        });
                    }
                }
            }
        }

        if column_changes.is_empty() {
            if !model_changes.changes.is_empty() {
                results.push((model_changes.model_id.clone(), Vec::new()));
            }
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

fn resolve_helper_path() -> Result<std::path::PathBuf, WriteBackError> {
    if let Ok(path) = std::env::var("DBT_LINT_YAML_HELPER") {
        let path = std::path::PathBuf::from(path);
        if path.exists() {
            return Ok(path);
        }
        return Err(WriteBackError::HelperMissing(path));
    }

    let mut candidates = Vec::new();

    if let Ok(exe_path) = std::env::current_exe()
        && let Some(dir) = exe_path.parent()
    {
        candidates.push(dir.join("ruamel_model_changes.py"));
        candidates.push(dir.join("scripts").join("ruamel_model_changes.py"));
    }

    let fallback = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts/ruamel_model_changes.py");
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
