//! This is my own implementation of the existing dbt Properties structs
//! In order to write this out in a semi sane way.  
//! https://github.com/dbt-labs/dbt-fusion/issues/953

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

use dbt_schemas::schemas::{dbt_column::DbtColumnRef, manifest::ManifestModel};
#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnProperty {
    pub name: String,
    pub description: Option<String>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl ColumnProperty {
    fn merge(&mut self, other: &ColumnProperty) {
        if other.description.is_some() {
            self.description = other.description.clone();
        }
        // probably won't have extras here
        for (k, v) in &other.extras {
            self.extras.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelProperty {
    pub name: Option<String>,
    pub description: Option<String>,
    pub columns: Vec<ColumnProperty>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl ModelProperty {
    pub fn merge(&mut self, other: &ModelProperty) {
        if self.description.is_none() {
            self.description = other.description.clone();
        }
        let mut other_columns_map: BTreeMap<String, &ColumnProperty> = BTreeMap::new();
        for col in &other.columns {
            other_columns_map.insert(col.name.clone(), col);
        }
        for col in &mut self.columns {
            if let Some(other_col) = other_columns_map.get(&col.name) {
                col.merge(other_col);
                // pop from other_columns_map to track which have been merged
                other_columns_map.remove(&col.name);
            }
        }
        // add any remaining columns from other that were not in self
        for (_name, col) in other_columns_map {
            self.columns.push(col.clone());
        }

        // probably won't have extras here
        for (k, v) in &other.extras {
            self.extras.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

pub fn model_property_from_manifest_differences(
    original: &ManifestModel,
    updated: &ManifestModel,
) -> Option<ModelProperty> {
    let mut model_prop = ModelProperty {
        name: Some(original.__common_attr__.name.clone()), // TODO: name shouldn't be option
        description: None,
        columns: Vec::new(),
        extras: BTreeMap::new(),
    };
    let mut has_change = false;
    if original.__common_attr__.description != updated.__common_attr__.description {
        has_change = true;
        model_prop.description = updated.__common_attr__.description.clone();
    }

    let mut original_columns_map: BTreeMap<String, &DbtColumnRef> = BTreeMap::new();
    for col in &original.__base_attr__.columns {
        original_columns_map.insert(col.name.clone(), col);
    }

    for updated_col in &updated.__base_attr__.columns {
        if let Some(orig_col) = original_columns_map.get(&updated_col.name) {
            if orig_col.description != updated_col.description {
                model_prop.columns.push(ColumnProperty {
                    name: updated_col.name.clone(),
                    description: updated_col.description.clone(),
                    extras: BTreeMap::new(),
                });
            }
        }
    }
    // I don't think this catches everything yet, but it's a start
    if !has_change {
        return None;
    }
    Some(model_prop)
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceProperty {
    pub name: String,
    pub description: Option<String>,
    pub tables: Vec<ModelProperty>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl SourceProperty {
    pub fn merge(&mut self, other: &SourceProperty) {
        if self.description.is_none() {
            self.description = other.description.clone();
        }
        let mut other_tables_map: BTreeMap<String, &ModelProperty> = BTreeMap::new();
        for table in &other.tables {
            if let Some(name) = &table.name {
                other_tables_map.insert(name.clone(), table);
            }
        }
        for table in &mut self.tables {
            if let Some(name) = &table.name
                && let Some(other_table) = other_tables_map.get(name)
            {
                table.merge(other_table);
            }
        }
        // probably won't have extras here
        for (k, v) in &other.extras {
            self.extras.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PropertyFile {
    pub models: Option<Vec<ModelProperty>>,
    pub sources: Option<Vec<SourceProperty>>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl PropertyFile {
    pub fn find_model_mut(&mut self, model_name: &str) -> Option<&mut ModelProperty> {
        self.models.as_mut().and_then(|models| {
            models
                .iter_mut()
                .find(|m| m.name.as_deref() == Some(model_name))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_column_merge_fills_description() {
        let mut a = ColumnProperty {
            name: "col_a".to_string(),
            description: None,
            extras: BTreeMap::new(),
        };
        let mut b = ColumnProperty {
            name: "col_a".to_string(),
            description: Some("desc from b".to_string()),
            extras: BTreeMap::new(),
        };

        a.merge(&b);
        assert_eq!(
            a.description.as_deref(),
            Some("desc from b"),
            "fills description"
        );

        b.description = Some("new desc from b".to_string());
        a.merge(&b);
        assert_eq!(
            a.description.as_deref(),
            Some("new desc from b"),
            "overwrites description"
        );
    }

    #[test]
    fn test_model_merge() {
        let mut self_model = ModelProperty {
            name: Some("model_1".to_string()),
            description: None,
            columns: vec![
                ColumnProperty {
                    name: "c1".to_string(),
                    description: None,
                    extras: BTreeMap::new(),
                },
                ColumnProperty {
                    name: "c3".to_string(),
                    description: Some("c3 desc".to_string()),
                    extras: BTreeMap::new(),
                },
            ],
            extras: BTreeMap::new(),
        };

        let other_model = ModelProperty {
            name: Some("model_1".to_string()),
            description: Some("model description".to_string()),
            columns: vec![
                ColumnProperty {
                    name: "c1".to_string(),
                    description: Some("c1 desc".to_string()),
                    extras: BTreeMap::new(),
                },
                ColumnProperty {
                    name: "c2".to_string(),
                    description: Some("c2 desc".to_string()),
                    extras: BTreeMap::new(),
                },
            ],
            extras: BTreeMap::new(),
        };

        self_model.merge(&other_model);
        // description should be filled
        assert_eq!(
            self_model.description.as_deref(),
            Some("model description"),
            "model description merged"
        );

        let c1 = self_model.columns.iter().find(|c| c.name == "c1").unwrap();
        assert_eq!(
            c1.description.as_deref(),
            Some("c1 desc"),
            "c1 description merged"
        );

        let c2 = self_model.columns.iter().find(|c| c.name == "c2").unwrap();
        assert_eq!(
            c2.description.as_deref(),
            Some("c2 desc"),
            "c2 description added"
        );

        let c3 = self_model.columns.iter().find(|c| c.name == "c3").unwrap();
        assert_eq!(
            c3.description.as_deref(),
            Some("c3 desc"),
            "c3 description unchanged"
        );
    }

    #[test]
    fn source_merge_merges_table_and_description() {
        let mut src_a = SourceProperty {
            name: "source_x".to_string(),
            description: None,
            tables: vec![ModelProperty {
                name: Some("t1".to_string()),
                description: None,
                columns: vec![],
                extras: BTreeMap::new(),
            }],
            extras: BTreeMap::new(),
        };

        let src_b = SourceProperty {
            name: "source_x".to_string(),
            description: Some("source desc".to_string()),
            tables: vec![ModelProperty {
                name: Some("t1".to_string()),
                description: Some("table desc".to_string()),
                columns: vec![ColumnProperty {
                    name: "col_z".to_string(),
                    description: Some("z desc".to_string()),
                    extras: BTreeMap::new(),
                }],
                extras: BTreeMap::new(),
            }],
            extras: BTreeMap::new(),
        };

        src_a.merge(&src_b);
        assert_eq!(src_a.description.as_deref(), Some("source desc"));
        let table = src_a
            .tables
            .iter()
            .find(|t| t.name.as_deref() == Some("t1"))
            .unwrap();
        assert_eq!(table.description.as_deref(), Some("table desc"));
        let col = table.columns.iter().find(|c| c.name == "col_z").unwrap();
        assert_eq!(col.description.as_deref(), Some("z desc"));
    }

    #[test]
    fn find_model_mut_returns_mutable_reference() {
        let mut root = PropertyFile {
            models: Some(vec![ModelProperty {
                name: Some("m_x".to_string()),
                description: None,
                columns: vec![],
                extras: BTreeMap::new(),
            }]),
            sources: None,
            extras: BTreeMap::new(),
        };

        let m = root.find_model_mut("m_x").expect("model found");
        m.description = Some("new desc".to_string());
        let m2 = root.find_model_mut("m_x").unwrap();
        assert_eq!(m2.description.as_deref(), Some("new desc"));
    }
}
