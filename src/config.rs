use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct Config {
    #[serde(default = "default_select")]
    pub select: Vec<String>,
    #[serde(default = "default_pull_column_desc_from_upstream")]
    pub pull_column_desc_from_upstream: bool,
}

impl Config {
    pub fn from_toml(project_dir: &std::path::PathBuf) -> Self {
        let config_path = project_dir.join("dbt-lint.toml");
        if config_path.exists() {
            let config_str =
                std::fs::read_to_string(&config_path).expect("Failed to read dbt-lint.toml");
            toml::from_str(&config_str).expect("Failed to parse dbt-lint.toml")
        } else {
            Self::default()
        }
    }
}

fn default_select() -> Vec<String> {
    vec![
        "missing_column_descriptions".to_string(),
        "missing_model_descriptions".to_string(),
        "missing_model_tags".to_string(),
        "missing_source_descriptions".to_string(),
    ]
}

fn default_pull_column_desc_from_upstream() -> bool {
    true
}
