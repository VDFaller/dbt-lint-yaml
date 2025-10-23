/// This module contains code generation utilities for the project.
/// It provides functions and structures to facilitate
/// Currently it requires dbt compile which is not SA'd yet.
/// So we're just using dbtf then assuming files are there. 

use std::path::PathBuf;
use parquet::file::reader::{FileReader, SerializedFileReader};
// Note: we avoid relying on specific ColumnChunkMetaData accessor names here so the example
// stays compatible with the pinned parquet crate version; we print debug representations
// of column-chunk metadata and prefer file-level and schema-level metadata for descriptions.
use dbt_lint_yaml::writeback::doc::{ModelsRoot, ModelDoc, ColumnDoc};

fn example() {
	let fp = PathBuf::from("/home/faller/repos/dbt-lint-yaml/tests/jaffle_shop/target/db/schemas/dev_vince/notlikethis_raw/raw_customers/output.parquet");
	let table_name = fp.parent().and_then(|p| p.file_name().map(|s| s.to_os_string()));
	println!("table name: {:?}", table_name);
	let file = std::fs::File::open(fp).unwrap();
	let reader = SerializedFileReader::new(file).unwrap();
	// get table name

	// schema descriptor (column list)
	let metadata = reader.metadata();
	let schema_descr = metadata.file_metadata().schema_descr();
	println!("num columns: {}", schema_descr.num_columns());
	let columns:Vec<ColumnDoc> = (0..schema_descr.num_columns()).map(|i| {
		let col = schema_descr.column(i);
		ColumnDoc {
			name: col.name().to_string(),
			description: None,
			extras: std::collections::BTreeMap::new(),
		}
	}).collect();
	let model_doc = ModelDoc {
		name: table_name.and_then(|os| os.as_os_str().to_str().map(|s| s.to_string())),
		description: None,
		columns,
		extras: std::collections::BTreeMap::new(),
	};
	let models_root = ModelsRoot {
		models: vec![model_doc],
		extras: std::collections::BTreeMap::new(),
	};
	let yaml_str = dbt_serde_yaml::to_string(&models_root).unwrap();
	println!("YAML output:\n{}", yaml_str); 
}