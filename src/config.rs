use serde::{Deserialize, Serialize};
use strsim::levenshtein;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Selector {
    MissingColumnDescriptions,
    MissingModelDescriptions,
    MissingModelTags,
    MissingSourceDescriptions,
    DirectJoinToSource,
    MissingPropertiesFile,
    DuplicateSources,
    ModelFanout,
    RootModels,
    UnusedSources,
    MissingPrimaryKey,
}

impl Selector {
    pub const ALL: [Self; 11] = [
        Selector::MissingColumnDescriptions,
        Selector::MissingModelDescriptions,
        Selector::MissingModelTags,
        Selector::MissingSourceDescriptions,
        Selector::DirectJoinToSource,
        Selector::MissingPropertiesFile,
        Selector::DuplicateSources,
        Selector::ModelFanout,
        Selector::RootModels,
        Selector::UnusedSources,
        Selector::MissingPrimaryKey,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Selector::MissingColumnDescriptions => "missing_column_descriptions",
            Selector::MissingModelDescriptions => "missing_model_descriptions",
            Selector::MissingModelTags => "missing_model_tags",
            Selector::MissingSourceDescriptions => "missing_source_descriptions",
            Selector::DirectJoinToSource => "direct_join_to_source",
            Selector::MissingPropertiesFile => "missing_properties_file",
            Selector::DuplicateSources => "duplicate_sources",
            Selector::ModelFanout => "model_fanout",
            Selector::RootModels => "root_models",
            Selector::UnusedSources => "unused_sources",
            Selector::MissingPrimaryKey => "missing_primary_key",
        }
    }
}

const ALLOWED_KEYS: &[&str] = &[
    "select",
    "pull_column_desc_from_upstream",
    "model_fanout_threshold",
    "required_tests",
];

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to parse dbt-lint.toml: {0}")]
    Toml(toml::de::Error),
    #[error("Invalid config: {0}")]
    Deserialize(toml::de::Error),
    #[error("{0}")]
    UnknownKeys(String),
    #[error("Config must be a TOML table at the root")]
    InvalidRoot,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_select")]
    pub select: Vec<Selector>,
    #[serde(default = "default_pull_column_desc_from_upstream")]
    pub pull_column_desc_from_upstream: bool,
    #[serde(default = "default_model_fanout_threshold")]
    // I'm intentionally not matching dbt-project-evaluator (models_fanout_threshold)
    // because this makes more sense to me
    // even dbt isn't consistent in it, because the table is fct_model_fanout
    pub model_fanout_threshold: usize,
    #[serde(default)]
    pub required_tests: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            select: default_select(),
            pull_column_desc_from_upstream: default_pull_column_desc_from_upstream(),
            model_fanout_threshold: default_model_fanout_threshold(),
            required_tests: Vec::new(),
        }
    }
}

impl Config {
    pub fn from_toml(project_dir: &std::path::PathBuf) -> Self {
        let config_path = project_dir.join("dbt-lint.toml");
        if config_path.exists() {
            let config_str =
                std::fs::read_to_string(&config_path).expect("Failed to read dbt-lint.toml");
            Self::try_from_str(&config_str).unwrap_or_else(|err| panic!("{err}"))
        } else {
            Self::default()
        }
    }

    pub fn from_str(toml_str: &str) -> Self {
        Self::try_from_str(toml_str).unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_from_str(toml_str: &str) -> Result<Self, ConfigError> {
        let value: toml::Value = toml::from_str(toml_str).map_err(ConfigError::Toml)?;
        if let Some(table) = value.as_table() {
            validate_keys(table)?;
        } else {
            return Err(ConfigError::InvalidRoot);
        }
        value.try_into().map_err(ConfigError::Deserialize)
    }
}

fn default_select() -> Vec<Selector> {
    Selector::ALL.to_vec()
}

fn default_pull_column_desc_from_upstream() -> bool {
    true
}
fn default_model_fanout_threshold() -> usize {
    3
}

fn validate_keys(table: &toml::value::Table) -> Result<(), ConfigError> {
    let mut unknown_messages = Vec::new();

    for key in table.keys() {
        if !ALLOWED_KEYS.contains(&key.as_str()) {
            let message = match find_suggestion(key) {
                Some(suggestion) => {
                    format!("Unknown config key `{key}`. Did you mean `{suggestion}`?")
                }
                None => format!("Unknown config key `{key}`."),
            };
            unknown_messages.push(message);
        }
    }

    if unknown_messages.is_empty() {
        Ok(())
    } else {
        unknown_messages.push(format!("Supported keys: {}", ALLOWED_KEYS.join(", ")));
        Err(ConfigError::UnknownKeys(unknown_messages.join("\n")))
    }
}

fn find_suggestion(unknown: &str) -> Option<&'static str> {
    let (candidate, distance) = ALLOWED_KEYS
        .iter()
        .copied()
        .map(|candidate| (candidate, levenshtein(unknown, candidate)))
        .min_by_key(|(_, distance)| *distance)?;

    if distance <= 3 { Some(candidate) } else { None }
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
        assert_eq!(
            config.model_fanout_threshold,
            default_model_fanout_threshold()
        );
    }

    #[test]
    fn test_from_str() {
        let toml_str = r#"
            select = ["missing_column_descriptions", "missing_model_tags"]
            pull_column_desc_from_upstream = false
            model_fanout_threshold = 4
        "#;
        let config = Config::from_str(toml_str);
        assert_eq!(
            config.select,
            vec![
                Selector::MissingColumnDescriptions,
                Selector::MissingModelTags
            ]
        );
        assert_eq!(config.pull_column_desc_from_upstream, false);
        assert_eq!(config.model_fanout_threshold, 4);
    }

    #[test]
    fn test_unknown_key_suggests_alternative() {
        let toml_str = r#"
            models_fanout_threshold = 10
        "#;
        let err = Config::try_from_str(toml_str).expect_err("unknown key should error");
        let message = err.to_string();
        assert!(message.contains("models_fanout_threshold"));
        assert!(message.contains("Did you mean `model_fanout_threshold`"));
        assert!(message.contains("Supported keys"));
    }

    #[test]
    fn test_from_str_rejects_unknown_selector() {
        let toml_str = r#"
            select = ["missing_model_description"]
        "#;
        let err = toml::from_str::<Config>(toml_str).expect_err("invalid selector should error");
        let message = err.to_string();
        for variant in Selector::ALL {
            let expected = variant.as_str();
            assert!(
                message.contains(expected),
                "error should mention {expected}, got: {message}"
            );
        }
    }
}
