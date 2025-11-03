use super::WriteBackError;
use crate::change_descriptors::ModelChange;
use crate::check::ModelChanges;
use crate::writeback::changes::ExecutableChange;
use crate::writeback::properties::PropertyFile;
use dbt_serde_yaml;
use std::collections::BTreeMap;
use std::path::Path;

pub fn apply_with_rust(
    project_root: &Path,
    changes: &BTreeMap<String, ModelChanges>,
) -> Result<Vec<(String, Vec<String>)>, WriteBackError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

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

        let yaml_str = std::fs::read_to_string(&resolved_path)?;
        let mut docs: PropertyFile = dbt_serde_yaml::from_str(&yaml_str)?;

        let mut updated_columns = Vec::new();
        let mut file_mutated = false;

        let mut reported_columns: Vec<String> = Vec::new();
        for change in &model_changes.changes {
            if let ModelChange::ChangePropertiesFile {
                property: Some(prop),
                ..
            } = change
            {
                reported_columns.extend(prop.columns.iter().map(|col| col.name.clone()));
            }
        }

        // Ask the ModelChanges to produce executable writeback ops (columns + model-level)
        let ops: Vec<Box<dyn ExecutableChange>> = model_changes.to_writeback_ops();

        // Apply ops; filesystem-affecting ops are performed via apply_with_fs
        for op in ops.iter() {
            let mutated = op.apply_with_fs(&mut docs, project_root)?;
            if !mutated.is_empty() {
                file_mutated = true;
                updated_columns.extend(mutated);
            }
        }

        // Persist YAML if we mutated in-memory docs
        if file_mutated {
            let out_str = dbt_serde_yaml::to_string(&docs)?;
            std::fs::write(&resolved_path, out_str)?;
        }

        if file_mutated {
            // Filter out synthetic markers before reporting.
            updated_columns.retain(|label| !label.starts_with("@model:"));
            if updated_columns.is_empty() {
                updated_columns = reported_columns;
            } else {
                updated_columns.extend(reported_columns);
            }
            results.push((model_changes.model_id.clone(), updated_columns));
        } else if !model_changes.changes.is_empty() {
            results.push((model_changes.model_id.clone(), Vec::new()));
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_descriptors::{ColumnChange, ModelChange};
    use std::fs;
    use tempfile::tempdir;

    fn sample_yaml() -> &'static str {
        r#"
models:
  - name: stg_order_items
    description: Individual food and drink items that make up our orders, one row per item.
    columns:
      - name: order_item_id
        description: The unique key for each order item.
      - name: order_id
        description: "{{ doc('order_id_desc') }}"
"#
    }

    #[test]
    fn rust_writeback_updates_existing_column() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("models.yml");
        fs::write(&file, sample_yaml()).unwrap();

        let mut changes = std::collections::BTreeMap::new();
        let mut mc = ModelChanges {
            model_id: "model.jaffle_shop.stg_order_items".to_string(),
            patch_path: Some(Path::new("models.yml").to_path_buf()),
            ..Default::default()
        };
        mc.column_changes.insert("order_item_id".to_string(), {
            let mut s = std::collections::BTreeSet::new();
            s.insert(ColumnChange::ChangePropertiesFile);
            s
        });
        mc.changes.push(ModelChange::ChangePropertiesFile {
            model_id: mc.model_id.clone(),
            model_name: "stg_order_items".to_string(),
            patch_path: mc.patch_path.clone(),
            property: Some(crate::writeback::properties::ModelProperty {
                name: Some("stg_order_items".to_string()),
                description: None,
                columns: vec![crate::writeback::properties::ColumnProperty {
                    name: "order_item_id".to_string(),
                    description: Some("New desc".to_string()),
                    extras: std::collections::BTreeMap::new(),
                }],
                extras: std::collections::BTreeMap::new(),
            }),
        });
        changes.insert(mc.model_id.clone(), mc);

        let res = apply_with_rust(dir.path(), &changes).unwrap();
        assert_eq!(res.len(), 1);
        let written = fs::read_to_string(dir.path().join("models.yml")).unwrap();
        assert!(written.contains("New desc"));
    }

    #[test]
    fn rust_writeback_appends_missing_column() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("models.yml");
        fs::write(&file, sample_yaml()).unwrap();

        let mut changes = std::collections::BTreeMap::new();
        let mut mc = ModelChanges {
            model_id: "model.jaffle_shop.stg_order_items".to_string(),
            patch_path: Some(Path::new("models.yml").to_path_buf()),
            ..Default::default()
        };
        mc.column_changes.insert("new_col".to_string(), {
            let mut s = std::collections::BTreeSet::new();
            s.insert(ColumnChange::ChangePropertiesFile);
            s
        });
        mc.changes.push(ModelChange::ChangePropertiesFile {
            model_id: mc.model_id.clone(),
            model_name: "stg_order_items".to_string(),
            patch_path: mc.patch_path.clone(),
            property: Some(crate::writeback::properties::ModelProperty {
                name: Some("stg_order_items".to_string()),
                description: None,
                columns: vec![crate::writeback::properties::ColumnProperty {
                    name: "new_col".to_string(),
                    description: Some("Appended".to_string()),
                    extras: std::collections::BTreeMap::new(),
                }],
                extras: std::collections::BTreeMap::new(),
            }),
        });
        changes.insert(mc.model_id.clone(), mc);

        let res = apply_with_rust(dir.path(), &changes).unwrap();
        assert_eq!(res.len(), 1);
        let written = fs::read_to_string(dir.path().join("models.yml")).unwrap();
        assert!(written.contains("Appended"));
    }
}
