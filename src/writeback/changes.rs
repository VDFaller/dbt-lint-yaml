use super::WriteBackError;
use super::doc::{ModelDoc, ModelsRoot};
use crate::change_descriptors::{ColumnChange, ModelChange};
use std::path::Path;

/// A trait implemented by changes that can be executed by the writeback layer.
///
/// This module centralizes executable change implementations. Checks still
/// produce lightweight change descriptors (in `crate::check`), and those are
/// converted into fully-owned, executable operations here at writeback time.
pub trait ExecutableChange {
    /// Apply this change. Implementations should perform any in-memory
    /// mutations against `root` and any filesystem effects using `project_root`.
    fn apply_with_fs(
        &self,
        root: &mut ModelsRoot,
        project_root: &Path,
    ) -> Result<Vec<String>, WriteBackError>;
}

// Column-level change types (centralized here so writeback ops and model-level
// changes live in the same module).
// ColumnChange and ModelChange descriptor types live in `crate::change_descriptors`.
// Implement the in-memory mutation helper here so writeback can operate on the
// shared descriptor types without moving the full implementations.
impl ColumnChange {
    /// Apply this change directly against a `ModelDoc` (in-memory).
    pub fn apply_to_model_doc(
        &self,
        model: &mut ModelDoc,
        model_id: &str,
    ) -> Result<bool, crate::writeback::WriteBackError> {
        match self {
            ColumnChange::DescriptionChanged {
                column_name, new, ..
            } => {
                if let Some(col) = model.columns.iter_mut().find(|c| c.name == *column_name) {
                    col.description = new.clone();
                    Ok(true)
                } else {
                    Err(crate::writeback::WriteBackError::ColumnMissing {
                        model_id: model_id.to_string(),
                        column_name: column_name.clone(),
                    })
                }
            }
        }
    }
}

// ColumnChange implements ExecutableChange directly.
impl ExecutableChange for ColumnChange {
    fn apply_with_fs(
        &self,
        root: &mut ModelsRoot,
        _project_root: &Path,
    ) -> Result<Vec<String>, WriteBackError> {
        match self {
            ColumnChange::DescriptionChanged {
                model_id,
                model_name,
                column_name,
                ..
            } => {
                if let Some(model) = root.find_model_mut(model_name) {
                    let mutated = self.apply_to_model_doc(model, model_id)?;
                    Ok(if mutated {
                        vec![column_name.clone()]
                    } else {
                        Vec::new()
                    })
                } else {
                    // If the model itself is not found in the docs, this is an error.
                    Err(crate::writeback::WriteBackError::ModelMissing {
                        model_id: model_id.to_string(),
                    })
                }
            }
        }
    }
}

impl ExecutableChange for ModelChange {
    fn apply_with_fs(
        &self,
        _root: &mut ModelsRoot,
        project_root: &Path,
    ) -> Result<Vec<String>, WriteBackError> {
        match self {
            ModelChange::MovePropertiesFile {
                patch_path,
                new_path,
                ..
            } => {
                let patch = patch_path
                    .clone()
                    .ok_or_else(|| WriteBackError::PatchPathMissing {
                        model_id: "".to_string(),
                    })?;
                let src = if patch.is_absolute() {
                    patch.clone()
                } else {
                    project_root.join(&patch)
                };
                let dst = if new_path.is_absolute() {
                    new_path.clone()
                } else {
                    project_root.join(new_path)
                };
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::rename(&src, &dst)?;
                Ok(Vec::new())
            }
            ModelChange::MoveModelFile {
                patch_path,
                new_path,
                ..
            } => {
                let patch = patch_path
                    .clone()
                    .ok_or_else(|| WriteBackError::PatchPathMissing {
                        model_id: "".to_string(),
                    })?;
                let src = if patch.is_absolute() {
                    patch.clone()
                } else {
                    project_root.join(&patch)
                };
                let dst = if new_path.is_absolute() {
                    new_path.clone()
                } else {
                    project_root.join(new_path)
                };
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::rename(&src, &dst)?;
                Ok(Vec::new())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_descriptors::ColumnChange;
    use crate::writeback::doc::{ColumnDoc, ModelDoc, ModelsRoot};
    use std::collections::BTreeMap;

    #[test]
    fn column_change_updates_existing_column() {
        let mut root = ModelsRoot {
            models: Vec::new(),
            extras: BTreeMap::new(),
        };
        root.models.push(ModelDoc {
            name: Some("test_model".to_string()),
            description: None,
            columns: vec![ColumnDoc {
                name: "col_a".to_string(),
                description: Some("old".to_string()),
                extras: BTreeMap::new(),
            }],
            extras: BTreeMap::new(),
        });

        let change = ColumnChange::DescriptionChanged {
            model_id: "model.test.test_model".to_string(),
            model_name: "test_model".to_string(),
            patch_path: None,
            column_name: "col_a".to_string(),
            old: Some("old".to_string()),
            new: Some("new".to_string()),
        };

        let updated = change
            .apply_with_fs(&mut root, std::path::Path::new("/"))
            .expect("apply should succeed");

        assert_eq!(updated, vec!["col_a".to_string()]);
        let m = root.find_model_mut("test_model").expect("model exists");
        assert_eq!(
            m.columns
                .iter()
                .find(|c| c.name == "col_a")
                .unwrap()
                .description
                .as_deref(),
            Some("new")
        );
    }

    #[test]
    fn column_change_appends_missing_column() {
        let mut root = ModelsRoot {
            models: Vec::new(),
            extras: BTreeMap::new(),
        };
        root.models.push(ModelDoc {
            name: Some("test_model".to_string()),
            description: None,
            columns: vec![],
            extras: BTreeMap::new(),
        });

        let change = ColumnChange::DescriptionChanged {
            model_id: "model.test.test_model".to_string(),
            model_name: "test_model".to_string(),
            patch_path: None,
            column_name: "new_col".to_string(),
            old: None,
            new: Some("added".to_string()),
        };

        let res = change.apply_with_fs(&mut root, std::path::Path::new("/"));
        assert!(res.is_err(), "expected error when column missing");
        match res.unwrap_err() {
            crate::writeback::WriteBackError::ColumnMissing {
                model_id,
                column_name,
            } => {
                // the model_id supplied in the change is the unique id
                assert_eq!(model_id, "model.test.test_model");
                assert_eq!(column_name, "new_col");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn model_change_moves_patch_file() {
        use std::fs;
        use std::path::PathBuf;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let src = dir.path().join("models.yml");
        fs::write(&src, "models: []").expect("write src");

        let change = ModelChange::MovePropertiesFile {
            model_id: "model.test.m".to_string(),
            model_name: "m".to_string(),
            patch_path: Some(PathBuf::from("models.yml")),
            new_path: PathBuf::from("moved/models.yml"),
        };

        // Apply the model change with project_root set to the temp dir
        let mut root = ModelsRoot {
            models: Vec::new(),
            extras: BTreeMap::new(),
        };
        change.apply_with_fs(&mut root, dir.path()).expect("apply");

        let dst = dir.path().join("moved/models.yml");
        assert!(dst.exists(), "destination file should exist");
        // source should no longer exist
        assert!(!src.exists(), "source file should have been moved");
    }
}
