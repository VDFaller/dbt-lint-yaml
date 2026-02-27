use super::WriteBackError;
use crate::change_descriptors::ModelChange;
use crate::check::ModelChanges;
use crate::writeback::changes::group_changes_by_file;
use crate::writeback::properties::{ModelProperty, PropertyFile};
use dbt_serde_yaml;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub fn apply_with_rust(
    project_root: &Path,
    changes: &BTreeMap<String, ModelChanges>,
) -> Result<Vec<(String, Vec<String>)>, WriteBackError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    // Group changes by file for efficient batching: one read/write per file instead of per model
    let grouped_changes = group_changes_by_file(changes);

    for (patch_path, models_for_file) in grouped_changes {
        let mut resolved_path = if patch_path.is_absolute() {
            patch_path.clone()
        } else {
            project_root.join(&patch_path)
        };

        // Single read per file
        let mut docs = read_property_file(&resolved_path)?;
        let mut file_mutated = false;

        // Apply all changes for this file
        for model_changes in models_for_file {
            let mut updated_columns = Vec::new();
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

            // Execute each change in sequence, applying filesystem and in-memory effects.
            for change in &model_changes.changes {
                match change {
                    ModelChange::MovePropertiesFile {
                        model_id,
                        model_name,
                        patch_path,
                        new_path,
                    } => {
                        let expected_path = if new_path.is_absolute() {
                            new_path.clone()
                        } else {
                            project_root.join(new_path)
                        };

                        let mutated = move_model_property(
                            &mut docs,
                            project_root,
                            model_id,
                            model_name,
                            patch_path,
                            new_path,
                        )?;

                        resolved_path = expected_path;
                        if mutated {
                            file_mutated = true;
                            updated_columns.push(format!("@model:{}", model_name));
                        }
                    }
                    ModelChange::ChangePropertiesFile {
                        model_name,
                        property,
                        ..
                    } => {
                        let Some(prop) = property else {
                            continue;
                        };

                        if let Some(existing) = docs.find_model_mut(model_name) {
                            existing.merge(prop);
                        } else {
                            let mut new_prop = prop.clone();
                            if new_prop.name.is_none() {
                                new_prop.name = Some(model_name.clone());
                            }
                            docs.models.get_or_insert_with(Vec::new).push(new_prop);
                        }

                        file_mutated = true;
                        updated_columns.push(format!("@model:{}", model_name));
                    }
                    ModelChange::GeneratePropertiesFile { .. } => {
                        // The check phase already wrote the properties file; nothing to do.
                    }
                    ModelChange::MoveModelFile {
                        patch_path,
                        new_path,
                        ..
                    } => {
                        let patch =
                            patch_path
                                .clone()
                                .ok_or_else(|| WriteBackError::PatchPathMissing {
                                    model_id: model_changes.model_id.clone(),
                                })?;
                        let src = if patch.is_absolute() {
                            patch
                        } else {
                            project_root.join(patch)
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
                    }
                }
            }

            // Report results for this model
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

        // Single write per file after all changes for this file are applied
        if file_mutated {
            if property_file_is_empty(&docs) {
                if resolved_path.exists() {
                    std::fs::remove_file(&resolved_path)?;
                }
            } else {
                let out_str = dbt_serde_yaml::to_string(&docs)?;
                if let Some(parent) = resolved_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&resolved_path, out_str)?;
            }
        }
    }

    Ok(results)
}

fn move_model_property(
    target_root: &mut PropertyFile,
    project_root: &Path,
    model_id: &str,
    model_name: &str,
    current_patch: &Option<PathBuf>,
    expected_patch: &PathBuf,
) -> Result<bool, WriteBackError> {
    let current = current_patch
        .clone()
        .ok_or_else(|| WriteBackError::PatchPathMissing {
            model_id: model_id.to_string(),
        })?;

    if &current == expected_patch {
        return Ok(false);
    }

    let current_path = resolve_patch_path(project_root, &current);

    let mut source_doc = read_property_file(&current_path)?;
    let property = extract_model_property(model_id, model_name, &mut source_doc)?;

    upsert_model_property(target_root, property);

    write_or_remove_property_file(&current_path, &source_doc)?;

    Ok(true)
}

fn resolve_patch_path(project_root: &Path, patch_path: &Path) -> PathBuf {
    if patch_path.is_absolute() {
        patch_path.to_path_buf()
    } else {
        project_root.join(patch_path)
    }
}

fn read_property_file(path: &Path) -> Result<PropertyFile, WriteBackError> {
    if !path.exists() {
        return Ok(PropertyFile {
            models: None,
            sources: None,
            extras: Default::default(),
        });
    }

    let contents = fs::read_to_string(path)?;
    let doc = dbt_serde_yaml::from_str(&contents)?;
    Ok(doc)
}

fn write_or_remove_property_file(path: &Path, doc: &PropertyFile) -> Result<(), WriteBackError> {
    if property_file_is_empty(doc) {
        if path.exists() {
            fs::remove_file(path)?;
        }
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let yaml = dbt_serde_yaml::to_string(doc)?;
    fs::write(path, yaml)?;
    Ok(())
}

fn extract_model_property(
    model_id: &str,
    model_name: &str,
    doc: &mut PropertyFile,
) -> Result<ModelProperty, WriteBackError> {
    let Some(models) = doc.models.as_mut() else {
        return Err(WriteBackError::ModelMissing {
            model_id: model_id.to_string(),
        });
    };

    if let Some(idx) = models
        .iter()
        .position(|model| model.name.as_deref() == Some(model_name))
    {
        let mut property = models.remove(idx);
        if models.is_empty() {
            doc.models = None;
        }
        if property.name.is_none() {
            property.name = Some(model_name.to_string());
        }
        return Ok(property);
    }

    Err(WriteBackError::ModelMissing {
        model_id: model_id.to_string(),
    })
}

fn upsert_model_property(doc: &mut PropertyFile, property: ModelProperty) {
    let models = doc.models.get_or_insert_with(Vec::new);
    if let Some(existing) = models
        .iter_mut()
        .find(|model| model.name.as_deref() == property.name.as_deref())
    {
        existing.merge(&property);
    } else {
        models.push(property);
    }
}

fn property_file_is_empty(doc: &PropertyFile) -> bool {
    doc.models.as_ref().is_none_or(|models| models.is_empty())
        && doc
            .sources
            .as_ref()
            .is_none_or(|sources| sources.is_empty())
        && doc.extras.is_empty()
}

// ── Source directory writeback (Rust path) ───────────────────────────────────

use crate::writeback::properties::SourceProperty;

/// Apply source directory moves using dbt_serde_yaml.
///
/// Extracts each named source block from the old YAML file and writes it to the
/// correct destination, creating directories as needed. Deletes the old file
/// when all sources have been moved out and no other content remains.
pub fn apply_source_changes_with_rust(
    by_old_path: &BTreeMap<PathBuf, BTreeMap<String, (PathBuf, Vec<String>)>>,
) -> Result<Vec<String>, WriteBackError> {
    let mut applied = Vec::new();

    for (old_path, sources_to_move) in by_old_path {
        if !old_path.exists() {
            continue;
        }

        let contents = fs::read_to_string(old_path)?;
        let mut old_doc: PropertyFile = dbt_serde_yaml::from_str(&contents)?;

        for (source_name, (new_path, ids)) in sources_to_move {
            let Some(source_prop) = extract_source_property(source_name, &mut old_doc) else {
                continue;
            };

            let mut new_doc: PropertyFile = read_property_file(new_path)?;

            let dest_sources = new_doc.sources.get_or_insert_with(Vec::new);
            if let Some(existing) = dest_sources.iter_mut().find(|s| s.name == *source_name) {
                existing.merge(&source_prop);
            } else {
                dest_sources.push(source_prop);
            }

            if let Some(parent) = new_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(new_path, dbt_serde_yaml::to_string(&new_doc)?)?;

            applied.extend_from_slice(ids);
        }

        if source_file_is_empty(&old_doc) {
            fs::remove_file(old_path)?;
        } else {
            fs::write(old_path, dbt_serde_yaml::to_string(&old_doc)?)?;
        }
    }

    Ok(applied)
}

fn extract_source_property(source_name: &str, doc: &mut PropertyFile) -> Option<SourceProperty> {
    let sources = doc.sources.as_mut()?;
    let idx = sources.iter().position(|s| s.name == source_name)?;
    let prop = sources.remove(idx);
    if sources.is_empty() {
        doc.sources = None;
    }
    Some(prop)
}

/// A source file is empty (safe to delete) when it has no sources and no models.
/// Leftover top-level fields like `version: 2` are not meaningful on their own.
fn source_file_is_empty(doc: &PropertyFile) -> bool {
    doc.models.as_ref().is_none_or(|m| m.is_empty())
        && doc.sources.as_ref().is_none_or(|s| s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_descriptors::{ColumnChange, ModelChange};
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    // ── source writeback tests ────────────────────────────────────────────────

    const MULTI_SOURCE_YAML: &str = r#"sources:
  - name: ecom
    description: E-commerce data
    tables:
      - name: raw_orders
  - name: analytics
    description: Analytics data
    tables:
      - name: metrics
"#;

    fn make_source_move_map(
        moves: &[(&str, &str, &str)], // (source_name, old_path, new_path)
    ) -> BTreeMap<PathBuf, BTreeMap<String, (PathBuf, Vec<String>)>> {
        let mut by_old_path: BTreeMap<PathBuf, BTreeMap<String, (PathBuf, Vec<String>)>> =
            BTreeMap::new();
        for (source_name, old, new) in moves {
            let source_id = format!("source.project.{source_name}.table");
            by_old_path
                .entry(PathBuf::from(old))
                .or_default()
                .entry(source_name.to_string())
                .or_insert_with(|| (PathBuf::from(new), Vec::new()))
                .1
                .push(source_id);
        }
        by_old_path
    }

    #[test]
    fn moves_one_source_leaves_other_in_place() {
        let dir = tempdir().unwrap();
        let old_file = dir.path().join("staging/__sources.yml");
        fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        fs::write(&old_file, MULTI_SOURCE_YAML).unwrap();

        let map = make_source_move_map(&[(
            "ecom",
            old_file.to_str().unwrap(),
            dir.path()
                .join("staging/ecom/_ecom__sources.yml")
                .to_str()
                .unwrap(),
        )]);
        let applied = apply_source_changes_with_rust(&map).unwrap();
        assert_eq!(applied.len(), 1);

        let new_file = dir.path().join("staging/ecom/_ecom__sources.yml");
        assert!(new_file.exists());
        let new_contents = fs::read_to_string(&new_file).unwrap();
        assert!(new_contents.contains("ecom"));
        assert!(!new_contents.contains("analytics"));

        assert!(old_file.exists(), "old file should survive with analytics");
        let old_contents = fs::read_to_string(&old_file).unwrap();
        assert!(!old_contents.contains("ecom"));
        assert!(old_contents.contains("analytics"));
    }

    #[test]
    fn moves_all_sources_deletes_old_file() {
        let dir = tempdir().unwrap();
        let old_file = dir.path().join("staging/__sources.yml");
        fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        fs::write(&old_file, MULTI_SOURCE_YAML).unwrap();

        let old_str = old_file.to_str().unwrap();
        let map = make_source_move_map(&[
            (
                "ecom",
                old_str,
                dir.path()
                    .join("staging/ecom/_ecom__sources.yml")
                    .to_str()
                    .unwrap(),
            ),
            (
                "analytics",
                old_str,
                dir.path()
                    .join("staging/analytics/_analytics__sources.yml")
                    .to_str()
                    .unwrap(),
            ),
        ]);
        let applied = apply_source_changes_with_rust(&map).unwrap();
        assert_eq!(applied.len(), 2);

        assert!(dir.path().join("staging/ecom/_ecom__sources.yml").exists());
        assert!(
            dir.path()
                .join("staging/analytics/_analytics__sources.yml")
                .exists()
        );
        assert!(!old_file.exists(), "old file should be deleted");
    }

    #[test]
    fn deduplicates_multi_table_source_into_single_block() {
        let yaml = r#"sources:
  - name: ecom
    tables:
      - name: raw_orders
      - name: raw_customers
"#;
        let dir = tempdir().unwrap();
        let old_file = dir.path().join("staging/__sources.yml");
        fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        fs::write(&old_file, yaml).unwrap();

        let new_path = dir.path().join("staging/ecom/_ecom__sources.yml");
        // Two source_ids for the same (old_path, source_name) — both collected
        let mut by_old_path: BTreeMap<PathBuf, BTreeMap<String, (PathBuf, Vec<String>)>> =
            BTreeMap::new();
        by_old_path.entry(old_file.clone()).or_default().insert(
            "ecom".to_string(),
            (
                new_path.clone(),
                vec![
                    "source.project.ecom.raw_orders".to_string(),
                    "source.project.ecom.raw_customers".to_string(),
                ],
            ),
        );

        let applied = apply_source_changes_with_rust(&by_old_path).unwrap();
        assert_eq!(applied.len(), 2, "both source_ids should be reported");

        let new_contents = fs::read_to_string(&new_path).unwrap();
        assert_eq!(
            new_contents.matches("name: ecom").count(),
            1,
            "source block should not be duplicated"
        );
        assert!(!old_file.exists(), "old file should be deleted");
    }

    // ── model writeback tests ─────────────────────────────────────────────────

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

    #[test]
    fn rust_writeback_moves_properties_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("models.yml");
        fs::write(&file, sample_yaml()).unwrap();

        let mut changes = std::collections::BTreeMap::new();
        let mut mc = ModelChanges {
            model_id: "model.jaffle_shop.stg_order_items".to_string(),
            patch_path: Some(Path::new("models.yml").to_path_buf()),
            ..Default::default()
        };
        mc.changes.push(ModelChange::MovePropertiesFile {
            model_id: mc.model_id.clone(),
            model_name: "stg_order_items".to_string(),
            patch_path: mc.patch_path.clone(),
            new_path: Path::new("nested").join("models.yml"),
        });
        changes.insert(mc.model_id.clone(), mc);

        let res = apply_with_rust(dir.path(), &changes).unwrap();
        assert_eq!(res.len(), 1);

        assert!(!file.exists(), "original file should be moved");
        let new_file = dir.path().join("nested/models.yml");
        assert!(new_file.exists(), "moved file should exist");
    }
}
