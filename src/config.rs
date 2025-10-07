use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    #[serde(default = "default_select")]
    pub select: Vec<String>,
    #[serde(default = "default_pull_column_desc_from_upstream")]
    pub pull_column_desc_from_upstream: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            select: default_select(),
            pull_column_desc_from_upstream: default_pull_column_desc_from_upstream(),
        }
    }
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

    pub fn from_str(toml_str: &str) -> Self {
        toml::from_str(toml_str).expect("Failed to parse dbt-lint.toml")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.select, default_select());
        assert_eq!(
            config.pull_column_desc_from_upstream,
            default_pull_column_desc_from_upstream()
        );
    }

    #[test]
    fn test_from_str() {
        let toml_str = r#"
            select = ["missing_column_descriptions", "missing_model_tags"]
            pull_column_desc_from_upstream = false
        "#;
        let config = Config::from_str(toml_str);
        assert_eq!(
            config.select,
            vec![
                "missing_column_descriptions".to_string(),
                "missing_model_tags".to_string()
            ]
        );
        assert_eq!(config.pull_column_desc_from_upstream, false);
    }
}
