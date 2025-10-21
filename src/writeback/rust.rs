use super::WriteBackError;
use crate::check::ModelChanges;
use crate::writeback::changes::ExecutableChange;
use crate::writeback::doc::ModelsRoot;
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
        let mut docs: ModelsRoot = dbt_serde_yaml::from_str(&yaml_str)?;

        let mut updated_columns = Vec::new();

        // Ask the ModelChanges to produce executable writeback ops (columns + model-level)
        let ops: Vec<Box<dyn ExecutableChange>> = model_changes.to_writeback_ops();

        // Apply ops; filesystem-affecting ops are performed via apply_with_fs
        for op in ops.iter() {
            let mutated = op.apply_with_fs(&mut docs, project_root)?;
            if !mutated.is_empty() {
                updated_columns.extend(mutated);
            }
        }

        // Persist YAML if we mutated in-memory docs
        if !updated_columns.is_empty() {
            let out_str = dbt_serde_yaml::to_string(&docs)?;
            std::fs::write(&resolved_path, out_str)?;
        }

        if !updated_columns.is_empty() {
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
    use crate::change_descriptors::ColumnChange;
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
            s.insert(ColumnChange::DescriptionChanged {
                model_id: "model.jaffle_shop.stg_order_items".to_string(),
                model_name: "stg_order_items".to_string(),
                patch_path: Some(Path::new("models.yml").to_path_buf()),
                column_name: "order_item_id".to_string(),
                old: Some("The unique key for each order item.".to_string()),
                new: Some("New desc".to_string()),
            });
            s
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
            s.insert(ColumnChange::DescriptionChanged {
                model_id: "model.jaffle_shop.stg_order_items".to_string(),
                model_name: "stg_order_items".to_string(),
                patch_path: Some(Path::new("models.yml").to_path_buf()),
                column_name: "new_col".to_string(),
                old: None,
                new: Some("Appended".to_string()),
            });
            s
        });
        changes.insert(mc.model_id.clone(), mc);

        let res = apply_with_rust(dir.path(), &changes);
        assert!(res.is_err(), "expected error when column missing");
        match res.unwrap_err() {
            WriteBackError::ColumnMissing {
                model_id,
                column_name,
            } => {
                // model_id is the model unique id from ModelChanges
                assert_eq!(model_id, "model.jaffle_shop.stg_order_items");
                assert_eq!(column_name, "new_col");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
