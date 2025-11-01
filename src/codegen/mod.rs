use parquet::file::reader::{FileReader, SerializedFileReader};
/// This module contains code generation utilities for the project.
/// It provides functions and structures to facilitate
/// Currently it requires dbt compile which is not SA'd yet.
/// So we're just using dbtf then assuming files are there.
use std::path::{Path, PathBuf};
// Note: we avoid relying on specific ColumnChunkMetaData accessor names here so the example
// stays compatible with the pinned parquet crate version; we print debug representations
// of column-chunk metadata and prefer file-level and schema-level metadata for descriptions.
use dbt_schemas::schemas::manifest::ManifestModel;

use crate::writeback::properties::{ColumnProperty, ModelProperty, PropertyFile};

fn generate_model(
    model: &ManifestModel,
    project_root: Option<&Path>,
) -> Result<PropertyFile, Box<dyn std::error::Error>> {
    let parquet_path = get_cached_parquet_path(model, project_root);
    let columns = get_columns_from_parquet(&parquet_path)?;

    let model_doc = ModelProperty {
        name: Some(model.__common_attr__.name.clone()),
        description: model.__common_attr__.description.clone(),
        columns,
        extras: std::collections::BTreeMap::new(),
    };

    let models_root = PropertyFile {
        models: Some(vec![model_doc]),
        sources: None,
        extras: std::collections::BTreeMap::new(),
    };

    Ok(models_root)
}

fn get_write_path(model: &ManifestModel) -> PathBuf {
    let original_file_path = &model.__common_attr__.original_file_path;
    original_file_path.with_extension("yml")
}

fn get_cached_parquet_path(model: &ManifestModel, project_root: Option<&Path>) -> PathBuf {
    if let Some(root) = project_root {
        return root.join(format!(
            "target/db/schemas/{}/{}/{}/output.parquet",
            model.__common_attr__.database,
            model.__common_attr__.schema,
            model.__common_attr__.name
        ));
    }
    PathBuf::from(format!(
        "target/db/schemas/{}/{}/{}/output.parquet",
        model.__common_attr__.database, model.__common_attr__.schema, model.__common_attr__.name
    ))
}

fn get_columns_from_parquet(
    path: &PathBuf,
) -> Result<Vec<ColumnProperty>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let schema_descr = metadata.file_metadata().schema_descr();
    let columns: Vec<ColumnProperty> = (0..schema_descr.num_columns())
        .map(|i| {
            let col = schema_descr.column(i);
            ColumnProperty {
                name: col.name().to_string(),
                description: None,
                extras: std::collections::BTreeMap::new(),
            }
        })
        .collect();
    Ok(columns)
}

pub fn write_generated_model(
    model: &ManifestModel,
    project_root: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let models_root = generate_model(model, project_root)?;
    let yaml_str = dbt_serde_yaml::to_string(&models_root)?;
    let write_path = get_write_path(model);

    let resolved = if write_path.is_absolute() {
        write_path
    } else if let Some(root) = project_root {
        root.join(write_path)
    } else {
        write_path
    };

    if let Some(parent) = resolved.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(resolved, yaml_str)?;
    Ok(())
}
