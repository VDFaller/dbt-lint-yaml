use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::writeback::changes::ExecutableChange;
use crate::writeback::properties::{ModelProperty, SourceProperty};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ColumnChange {
    DescriptionChanged {
        model_id: String,
        model_name: String,
        patch_path: Option<PathBuf>,
        column_name: String,
        old: Option<String>,
        new: Option<String>,
    },
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
    ChangePropertiesFile {
        model_id: String,
        model_name: String,
        patch_path: Option<PathBuf>,
        property: Option<ModelProperty>,
    },
}

impl ModelChange {
    /// Return a boxed executable change for existing code paths.
    pub fn new_executable(&self) -> Box<dyn ExecutableChange> {
        Box::new(self.clone())
    }
}

#[derive(Default, Debug, Clone)]
pub struct ModelChanges {
    pub model_id: String,
    pub patch_path: Option<PathBuf>,
    pub changes: Vec<ModelChange>,
    pub column_changes: BTreeMap<String, BTreeSet<ColumnChange>>,
}

impl ModelChanges {
    /// Produce a list of executable writeback ops (columns + model-level).
    pub fn to_writeback_ops(&self) -> Vec<Box<dyn ExecutableChange>> {
        let mut ops: Vec<Box<dyn ExecutableChange>> = Vec::new();

        for change_set in self.column_changes.values() {
            for change in change_set.iter() {
                ops.push(change.to_writeback_op());
            }
        }

        for change in &self.changes {
            ops.push(change.new_executable());
        }

        ops
    }
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

impl ColumnChange {
    pub fn to_writeback_op(&self) -> Box<dyn ExecutableChange> {
        Box::new(self.clone())
    }
}
