use crate::change_descriptors::SourceChange;
use crate::check::{ModelChanges, SourceChanges};
use crate::writeback::properties::{PropertyFile, SourceProperty};
use std::{collections::BTreeMap, path::Path, path::PathBuf};
use thiserror::Error;

pub mod changes;
pub mod properties;
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
    #[error("column `{column_name}` not found for model `{model_id}`")]
    ColumnMissing {
        model_id: String,
        column_name: String,
    },
    #[error("model `{model_id}` not found in docs")]
    ModelMissing { model_id: String },
    #[error("yaml error: {0}")]
    Yaml(#[from] dbt_serde_yaml::Error),
}

/// Apply source-level file moves by extracting individual source definitions from YAML files.
///
/// For each `MoveSourceToFile` change, extracts the named source block from the old YAML file
/// and writes it to the correct destination, creating directories as needed. If the old file
/// becomes empty after all extractions, it is deleted.
///
/// Multiple sources in the same file are handled correctly: each source block is extracted
/// individually so that unrelated sources remain in the original file.
pub fn apply_source_changes(
    project_root: &Path,
    changes: &BTreeMap<String, SourceChanges>,
) -> Result<Vec<String>, WriteBackError> {
    // Deduplicate moves by (old_path, source_name) — multiple tables share the same source block.
    // Maps (old_abs_path, source_name) -> (new_abs_path, Vec<source_id>)
    let mut move_map: BTreeMap<(PathBuf, String), (PathBuf, Vec<String>)> = BTreeMap::new();

    for source_changes in changes.values() {
        for change in &source_changes.changes {
            if let SourceChange::MoveSourceToFile {
                source_id,
                source_name,
                old_patch_path,
                new_patch_path,
            } = change
            {
                let old_abs = resolve_abs(project_root, old_patch_path);
                let new_abs = resolve_abs(project_root, new_patch_path);
                let entry = move_map
                    .entry((old_abs, source_name.clone()))
                    .or_insert_with(|| (new_abs, Vec::new()));
                entry.1.push(source_id.clone());
            }
        }
    }

    // Group by old_path so we read each source file at most once.
    // Maps old_abs_path -> Vec<(source_name, new_abs_path, source_ids)>
    let mut by_old_path: BTreeMap<PathBuf, Vec<(String, PathBuf, Vec<String>)>> = BTreeMap::new();
    for ((old_path, source_name), (new_path, ids)) in move_map {
        by_old_path
            .entry(old_path)
            .or_default()
            .push((source_name, new_path, ids));
    }

    let mut applied = Vec::new();

    for (old_path, sources_to_move) in &by_old_path {
        if !old_path.exists() {
            continue;
        }

        let contents = std::fs::read_to_string(old_path)?;
        let mut old_doc: PropertyFile = dbt_serde_yaml::from_str(&contents)?;

        for (source_name, new_path, ids) in sources_to_move {
            let Some(source_prop) = extract_source_property(source_name, &mut old_doc) else {
                continue;
            };

            // Read destination file (may not exist yet).
            let mut new_doc: PropertyFile = if new_path.exists() {
                let c = std::fs::read_to_string(new_path)?;
                dbt_serde_yaml::from_str(&c)?
            } else {
                PropertyFile {
                    models: None,
                    sources: None,
                    extras: Default::default(),
                }
            };

            // Upsert the source block into the destination.
            let dest_sources = new_doc.sources.get_or_insert_with(Vec::new);
            if let Some(existing) = dest_sources.iter_mut().find(|s| s.name == *source_name) {
                existing.merge(&source_prop);
            } else {
                dest_sources.push(source_prop);
            }

            if let Some(parent) = new_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(new_path, dbt_serde_yaml::to_string(&new_doc)?)?;

            applied.extend_from_slice(ids);
        }

        // Write back old file, or delete it if all meaningful content has been moved out.
        if source_file_is_empty(&old_doc) {
            std::fs::remove_file(old_path)?;
        } else {
            std::fs::write(old_path, dbt_serde_yaml::to_string(&old_doc)?)?;
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

/// A file is considered empty (and safe to delete) when it has no sources and no models.
/// Leftover top-level fields like `version: 2` are not meaningful on their own.
fn source_file_is_empty(doc: &PropertyFile) -> bool {
    doc.models.as_ref().is_none_or(|m| m.is_empty())
        && doc.sources.as_ref().is_none_or(|s| s.is_empty())
}

fn resolve_abs(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_descriptors::{SourceChange, SourceChanges};
    use std::fs;
    use tempfile::tempdir;

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

    fn make_move(
        source_name: &str,
        table_name: &str,
        old: &str,
        new: &str,
    ) -> (String, SourceChanges) {
        let source_id = format!("source.project.{source_name}.{table_name}");
        let mut sc = SourceChanges {
            source_id: source_id.clone(),
            source_name: source_name.to_string(),
            table_name: table_name.to_string(),
            patch_path: Some(PathBuf::from(old)),
            changes: vec![],
        };
        sc.changes.push(SourceChange::MoveSourceToFile {
            source_id: source_id.clone(),
            source_name: source_name.to_string(),
            old_patch_path: PathBuf::from(old),
            new_patch_path: PathBuf::from(new),
        });
        (source_id, sc)
    }

    #[test]
    fn moves_one_source_leaves_other_in_place() {
        let dir = tempdir().unwrap();
        let old_file = dir.path().join("staging/__sources.yml");
        fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        fs::write(&old_file, MULTI_SOURCE_YAML).unwrap();

        let mut changes = BTreeMap::new();
        let (id, sc) = make_move(
            "ecom",
            "raw_orders",
            "staging/__sources.yml",
            "staging/ecom/_ecom__sources.yml",
        );
        changes.insert(id, sc);

        let applied = apply_source_changes(dir.path(), &changes).unwrap();
        assert_eq!(applied.len(), 1);

        // New file contains the ecom source.
        let new_file = dir.path().join("staging/ecom/_ecom__sources.yml");
        assert!(new_file.exists(), "new ecom file should exist");
        let new_contents = fs::read_to_string(&new_file).unwrap();
        assert!(new_contents.contains("ecom"), "ecom should be in new file");
        assert!(
            !new_contents.contains("analytics"),
            "analytics should not leak into new file"
        );

        // Old file still exists but without ecom.
        assert!(old_file.exists(), "old file should survive with analytics");
        let old_contents = fs::read_to_string(&old_file).unwrap();
        assert!(
            !old_contents.contains("ecom"),
            "ecom should be removed from old file"
        );
        assert!(
            old_contents.contains("analytics"),
            "analytics should remain in old file"
        );
    }

    #[test]
    fn moves_all_sources_deletes_old_file() {
        let dir = tempdir().unwrap();
        let old_file = dir.path().join("staging/__sources.yml");
        fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        fs::write(&old_file, MULTI_SOURCE_YAML).unwrap();

        let mut changes = BTreeMap::new();
        for (name, table, new_path) in [
            ("ecom", "raw_orders", "staging/ecom/_ecom__sources.yml"),
            (
                "analytics",
                "metrics",
                "staging/analytics/_analytics__sources.yml",
            ),
        ] {
            let (id, sc) = make_move(name, table, "staging/__sources.yml", new_path);
            changes.insert(id, sc);
        }

        let applied = apply_source_changes(dir.path(), &changes).unwrap();
        assert_eq!(applied.len(), 2);

        assert!(dir.path().join("staging/ecom/_ecom__sources.yml").exists());
        assert!(
            dir.path()
                .join("staging/analytics/_analytics__sources.yml")
                .exists()
        );
        assert!(
            !old_file.exists(),
            "old file should be deleted when all sources are moved"
        );
    }

    #[test]
    fn deduplicates_multi_table_source_into_single_block() {
        // A source with two tables in the same file: only one MoveSourceToFile block
        // should appear in the destination despite two change entries.
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

        let mut changes = BTreeMap::new();
        // Two table-level entries for the same source
        for table in ["raw_orders", "raw_customers"] {
            let (id, sc) = make_move(
                "ecom",
                table,
                "staging/__sources.yml",
                "staging/ecom/_ecom__sources.yml",
            );
            changes.insert(id, sc);
        }

        let applied = apply_source_changes(dir.path(), &changes).unwrap();
        // Both source_ids are reported
        assert_eq!(applied.len(), 2);

        let new_file = dir.path().join("staging/ecom/_ecom__sources.yml");
        let new_contents = fs::read_to_string(&new_file).unwrap();
        // Source block appears exactly once (not duplicated)
        assert_eq!(
            new_contents.matches("name: ecom").count(),
            1,
            "source block should not be duplicated"
        );
        assert!(
            !old_file.exists(),
            "old file should be deleted when the only source is moved"
        );
    }
}
