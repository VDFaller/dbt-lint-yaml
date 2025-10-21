use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColumnDoc {
    pub name: String,
    pub description: Option<String>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelDoc {
    pub name: Option<String>,
    pub description: Option<String>,
    pub columns: Vec<ColumnDoc>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ModelsRoot {
    pub models: Vec<ModelDoc>,
    #[serde(flatten)]
    pub extras: BTreeMap<String, dbt_serde_yaml::Value>,
}

impl ModelsRoot {
    pub fn find_model_mut(&mut self, model_name: &str) -> Option<&mut ModelDoc> {
        self.models
            .iter_mut()
            .find(|m| m.name.as_deref() == Some(model_name))
    }
}
