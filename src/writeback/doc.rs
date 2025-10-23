//! This is my own implementation of the existing dbt Properties structs
//! In order to write this out in a semi sane way.  
//! https://github.com/dbt-labs/dbt-fusion/issues/953

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::collections::BTreeMap;

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnDoc {
    pub name: String,
    pub description: Option<String>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelDoc {
    pub name: Option<String>,
    pub description: Option<String>,
    pub columns: Vec<ColumnDoc>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceDoc {
    pub name: String,
    pub description: Option<String>,
    pub tables: Vec<ModelDoc>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelsRoot {
    pub models: Option<Vec<ModelDoc>>,
    pub sources: Option<Vec<SourceDoc>>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl ModelsRoot {
    pub fn find_model_mut(&mut self, model_name: &str) -> Option<&mut ModelDoc> {
        self.models.as_mut().and_then(|models| {
            models
                .iter_mut()
                .find(|m| m.name.as_deref() == Some(model_name))
        })
    }
}
