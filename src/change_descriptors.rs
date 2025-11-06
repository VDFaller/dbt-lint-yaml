use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::config::ModelPropertiesLayout;
use crate::writeback::properties::{ModelProperty, SourceProperty};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ColumnChange {
    ChangePropertiesFile,
}

#[derive(Debug, Clone)]
pub enum ModelChange {
    MovePropertiesFile {
        model_id: String,
        model_name: String,
        patch_path: Option<PathBuf>,
        new_path: PathBuf,
    },
    MoveModelFile {
        model_id: String,
        model_name: String,
        patch_path: Option<PathBuf>,
        new_path: PathBuf,
    },
    /// A descriptor indicating a properties file was generated for a model.
    /// The file is expected to already exist on disk when this change is
    /// produced (the check wrote it); the writeback layer doesn't need to
    /// perform additional filesystem work for this change.
    GeneratePropertiesFile {
        model_id: String,
        model_name: String,
        patch_path: Option<PathBuf>,
    },
    ChangePropertiesFile {
        model_id: String,
        model_name: String,
        patch_path: Option<PathBuf>,
        property: Option<ModelProperty>,
    },
    NormalizePropertiesLayout {
        model_id: String,
        model_name: String,
        current_patch: Option<PathBuf>,
        expected_patch: PathBuf,
        layout: ModelPropertiesLayout,
    },
}

#[derive(Default, Debug, Clone)]
pub struct ModelChanges {
    pub model_id: String,
    pub patch_path: Option<PathBuf>,
    pub changes: Vec<ModelChange>,
    pub column_changes: BTreeMap<String, BTreeSet<ColumnChange>>,
}

#[derive(Debug, Clone)]
pub enum SourceChange {
    ChangePropertiesFile {
        source_id: String,
        source_name: String,
        table_name: String,
        patch_path: Option<PathBuf>,
        property: Option<SourceProperty>,
    },
}

#[derive(Default, Debug, Clone)]
pub struct SourceChanges {
    pub source_id: String,
    pub source_name: String,
    pub table_name: String,
    pub patch_path: Option<PathBuf>,
    pub changes: Vec<SourceChange>,
}

impl SourceChanges {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}
