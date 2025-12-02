//! Effect-oriented writeback planning: grouping changes by file for efficient I/O batching.
//!
//! This module provides utilities for grouping `ModelChanges` by their patch file path,
//! enabling single read/write cycles per file and laying groundwork for threaded execution.
//!
//! Previously this module contained filesystem helpers that now live in `writeback::rust`.

use crate::change_descriptors::ModelChanges;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Groups multiple model changes by their target patch file path.
///
/// This enables efficient batching: instead of reading/writing each file N times (once per model),
/// we read once, apply all changes for that file, and write once.
///
/// # Arguments
/// * `changes` - A map of model_id -> ModelChanges to be grouped
///
/// # Returns
/// A map where keys are file paths and values are vectors of ModelChanges targeting that file.
pub fn group_changes_by_file(
    changes: &BTreeMap<String, ModelChanges>,
) -> BTreeMap<PathBuf, Vec<&ModelChanges>> {
    let mut file_groups: BTreeMap<PathBuf, Vec<&ModelChanges>> = BTreeMap::new();

    for model_changes in changes.values() {
        // Use the patch_path if available; otherwise skip (patch_path is critical)
        if let Some(patch_path) = &model_changes.patch_path {
            file_groups
                .entry(patch_path.clone())
                .or_default()
                .push(model_changes);
        }
    }

    file_groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_group_changes_by_file() {
        let mut changes = BTreeMap::new();
        let path1 = PathBuf::from("models/models.yml");
        let path2 = PathBuf::from("models/staging/staging_models.yml");

        let model1 = ModelChanges {
            model_id: "model.test.model1".to_string(),
            patch_path: Some(path1.clone()),
            ..Default::default()
        };

        let model2 = ModelChanges {
            model_id: "model.test.model2".to_string(),
            patch_path: Some(path1.clone()),
            ..Default::default()
        };

        let model3 = ModelChanges {
            model_id: "model.test.model3".to_string(),
            patch_path: Some(path2.clone()),
            ..Default::default()
        };
        changes.insert("model.test.model1".to_string(), model1);
        changes.insert("model.test.model2".to_string(), model2);
        changes.insert("model.test.model3".to_string(), model3);

        let grouped = group_changes_by_file(&changes);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[&path1].len(), 2);
        assert_eq!(grouped[&path2].len(), 1);
    }
}
