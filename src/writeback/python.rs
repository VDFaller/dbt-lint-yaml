use super::WriteBackError;
use crate::change_descriptors::{ModelChange, ModelChanges};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

#[derive(Debug, Clone, Serialize)]
struct PythonColumnChange {
    column_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_description: Option<String>,
}

/// Single model update within a batch request
#[derive(Debug, Clone, Serialize)]
struct ModelUpdate {
    model_name: String,
    column_changes: Vec<PythonColumnChange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_description: Option<String>,
}

/// Batch request: single file, multiple models
#[derive(Debug, Serialize)]
struct PythonBatchRequest {
    patch_path: std::path::PathBuf,
    models: Vec<ModelUpdate>,
}

#[derive(Debug, Deserialize)]
struct PythonBatchResponse {
    results: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Serialize)]
struct LayoutRequest {
    current_patch: String,
    expected_patch: String,
    model_name: String,
}

impl LayoutRequest {
    fn new(current_patch: &Path, expected_patch: &Path, model_name: &str) -> Self {
        Self {
            current_patch: current_patch.to_string_lossy().into_owned(),
            expected_patch: expected_patch.to_string_lossy().into_owned(),
            model_name: model_name.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct LayoutResponse {
    mutated: bool,
}

pub fn apply_with_python(
    project_root: &Path,
    changes: &BTreeMap<String, ModelChanges>,
) -> Result<Vec<(String, Vec<String>)>, WriteBackError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

    use crate::writeback::changes::group_changes_by_file;

    let helper_path = resolve_helper_path()?;
    let mut layout_helper_path: Option<std::path::PathBuf> = None;

    let mut results = Vec::new();

    // Group changes by file for batching: one Python process call per file
    let grouped_changes = group_changes_by_file(changes);

    for (_patch_path, models_for_file) in grouped_changes {
        // First pass: handle file moves and layout changes, collect model updates
        let mut batch_updates: Vec<(String, ModelUpdate)> = Vec::new();
        let mut resolved_path: Option<std::path::PathBuf> = None;

        for model_changes in &models_for_file {
            let patch_path = model_changes.patch_path.as_ref().ok_or_else(|| {
                WriteBackError::PatchPathMissing {
                    model_id: model_changes.model_id.clone(),
                }
            })?;

            let mut current_path = if patch_path.is_absolute() {
                patch_path.clone()
            } else {
                project_root.join(patch_path)
            };

            // Set resolved_path on first iteration
            if resolved_path.is_none() {
                resolved_path = Some(current_path.clone());
            }

            let model_name = extract_model_name(&model_changes.model_id);
            let mut model_description_change: Option<String> = None;
            let mut property_payload: Option<&crate::writeback::properties::ModelProperty> = None;

            // Process changes for this model
            for change in &model_changes.changes {
                match change {
                    ModelChange::MovePropertiesFile {
                        patch_path,
                        new_path,
                        ..
                    } => {
                        let current_patch = patch_path.as_ref().ok_or_else(|| {
                            WriteBackError::PatchPathMissing {
                                model_id: model_changes.model_id.clone(),
                            }
                        })?;

                        let resolved_current = if current_patch.is_absolute() {
                            current_patch.clone()
                        } else {
                            project_root.join(current_patch)
                        };

                        let resolved_expected = if new_path.is_absolute() {
                            new_path.clone()
                        } else {
                            project_root.join(new_path)
                        };

                        if resolved_current != resolved_expected {
                            if layout_helper_path.is_none() {
                                layout_helper_path = Some(resolve_layout_helper_path()?);
                            }
                            let helper = layout_helper_path.as_ref().expect("layout helper set");
                            let _mutated = invoke_layout_helper(
                                helper,
                                LayoutRequest::new(
                                    &resolved_current,
                                    &resolved_expected,
                                    model_name,
                                ),
                            )?;
                        }

                        current_path = resolved_expected;
                        resolved_path = Some(current_path.clone());
                    }
                    ModelChange::ChangePropertiesFile {
                        patch_path,
                        property,
                        ..
                    } => {
                        if patch_path.is_none() {
                            eprintln!(
                                "Skipping unsupported model-level change for `{}` in python writeback",
                                model_changes.model_id
                            );
                            continue;
                        }
                        if let Some(prop) = property {
                            if let Some(desc) = prop.description.as_ref() {
                                model_description_change = Some(desc.clone());
                            }
                            property_payload = Some(prop);
                        }
                    }
                    other => {
                        return Err(WriteBackError::UnsupportedModelChange {
                            model_id: model_changes.model_id.clone(),
                            change: format!("{other:?}"),
                        });
                    }
                }
            }

            // Collect column changes for this model
            let mut column_changes: Vec<PythonColumnChange> = Vec::new();
            if let Some(prop) = property_payload {
                for column in &prop.columns {
                    column_changes.push(PythonColumnChange {
                        column_name: column.name.clone(),
                        new_description: column.description.clone(),
                    });
                }
            } else if !model_changes.column_changes.is_empty() {
                for column_name in model_changes.column_changes.keys() {
                    column_changes.push(PythonColumnChange {
                        column_name: column_name.clone(),
                        new_description: None,
                    });
                }
            }

            // Add to batch if there are changes to apply
            if !column_changes.is_empty() || model_description_change.is_some() {
                batch_updates.push((
                    model_changes.model_id.clone(),
                    ModelUpdate {
                        model_name: model_name.to_string(),
                        column_changes,
                        model_description: model_description_change,
                    },
                ));
            } else if !model_changes.changes.is_empty() {
                // Some changes were processed (e.g., moves, layout) but nothing to send to Python
                results.push((model_changes.model_id.clone(), Vec::new()));
            }
        }

        // Single batch call to Python for all models in this file
        if let Some(patch_path) = resolved_path
            && !batch_updates.is_empty()
        {
            let model_updates: Vec<ModelUpdate> = batch_updates
                .iter()
                .map(|(_, update)| update.clone())
                .collect();

            let request = PythonBatchRequest {
                patch_path,
                models: model_updates,
            };

            let response = invoke_python_batch_helper(&helper_path, &request)?;

            // Map responses back to model IDs
            for (model_id, _) in batch_updates {
                let model_name = extract_model_name(&model_id).to_string();
                let updated_cols = response
                    .results
                    .get(&model_name)
                    .cloned()
                    .unwrap_or_default();
                results.push((model_id, updated_cols));
            }
        }
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

fn resolve_layout_helper_path() -> Result<std::path::PathBuf, WriteBackError> {
    if let Ok(path) = std::env::var("DBT_LINT_YAML_LAYOUT_HELPER") {
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
        candidates.push(dir.join("ruamel_normalize_layout.py"));
        candidates.push(dir.join("scripts").join("ruamel_normalize_layout.py"));
    }

    let fallback = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts/ruamel_normalize_layout.py");
    candidates.push(fallback.clone());

    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }

    Err(WriteBackError::HelperMissing(fallback))
}

fn invoke_layout_helper(
    helper_path: &Path,
    request: LayoutRequest,
) -> Result<bool, WriteBackError> {
    let mut command = Command::new("python3");
    command.arg(helper_path);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let mut child = command.spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        let json = serde_json::to_vec(&request)?;
        stdin.write_all(&json)?;
    }

    let output = child.wait_with_output()?;

    if !output.status.success() {
        let status = output.status.code().unwrap_or(-1);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(WriteBackError::PythonFailure { status, stderr });
    }

    let response: LayoutResponse =
        serde_json::from_slice(&output.stdout).map_err(WriteBackError::ResponseParseFailure)?;

    Ok(response.mutated)
}

fn invoke_python_batch_helper(
    helper_path: &Path,
    request: &PythonBatchRequest,
) -> Result<PythonBatchResponse, WriteBackError> {
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

    let response: PythonBatchResponse =
        serde_json::from_slice(&output.stdout).map_err(WriteBackError::ResponseParseFailure)?;

    Ok(response)
}

fn extract_model_name(unique_id: &str) -> &str {
    unique_id.rsplit('.').next().unwrap_or(unique_id)
}
