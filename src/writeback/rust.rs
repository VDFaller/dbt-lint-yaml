use super::WriteBackError;
use crate::check::{ColumnChange, ModelChanges};
use dbt_serde_yaml;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize)]
struct ModelDoc {
    name: Option<String>,
    description: Option<String>,
    columns: Vec<ColumnDoc>,
    #[serde(flatten)]
    extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ColumnDoc {
    name: String,
    description: Option<String>,
    #[serde(flatten)]
    extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ModelsRoot {
    models: Vec<ModelDoc>,
    #[serde(flatten)]
    extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

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

        let model_name = extract_model_name(&model_changes.model_id).to_string();

        let mut updated_columns = Vec::new();

        if let Some(model) = docs
            .models
            .iter_mut()
            .find(|m| m.name.as_deref() == Some(&model_name))
        {
            for (column_name, change_set) in &model_changes.column_changes {
                for change in change_set {
                    match change {
                        ColumnChange::DescriptionChanged { new, .. } => {
                            if let Some(col) =
                                model.columns.iter_mut().find(|c| &c.name == column_name)
                            {
                                col.description = new.clone();
                                updated_columns.push(column_name.clone());
                            } else {
                                model.columns.push(ColumnDoc {
                                    name: column_name.clone(),
                                    description: new.clone(),
                                    extras: BTreeMap::new(),
                                });
                                updated_columns.push(column_name.clone());
                            }
                        }
                    }
                }
            }

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

fn extract_model_name(unique_id: &str) -> &str {
    unique_id.rsplit('.').next().unwrap_or(unique_id)
}

#[cfg(test)]
mod tests {
    use super::*;
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
            s.insert(crate::check::ColumnChange::DescriptionChanged {
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
            s.insert(crate::check::ColumnChange::DescriptionChanged {
                old: None,
                new: Some("Appended".to_string()),
            });
            s
        });
        changes.insert(mc.model_id.clone(), mc);

        let res = apply_with_rust(dir.path(), &changes).unwrap();
        assert_eq!(res.len(), 1);
        let written = fs::read_to_string(dir.path().join("models.yml")).unwrap();
        assert!(written.contains("new_col"));
        assert!(written.contains("Appended"));
    }
}
