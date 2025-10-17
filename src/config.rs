use serde::{Deserialize, Serialize};
use strsim::levenshtein;
use struct_field_names_as_array::FieldNamesAsSlice;
use strum::{AsRefStr, EnumIter, EnumProperty, IntoEnumIterator};
use thiserror::Error;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    EnumIter,
    AsRefStr,
    EnumProperty,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Selector {
    #[strum(props(fixable = "true"))]
    MissingColumnDescriptions,
    MissingModelDescriptions,
    MissingModelTags,
    MissingSourceDescriptions,
    MissingSourceTableDescriptions,
    DirectJoinToSource,
    MissingPropertiesFile,
    DuplicateSources,
    ModelFanout,
    RootModels,
    UnusedSources,
    MissingPrimaryKey,
    MissingSourceFreshness,
    MultipleSourcesJoined,
    RejoiningOfUpstreamConcepts,
    SourceFanout,
    PublicModelsWithoutContract,
    // this is fixable, but right now it doesn't work right
    // if two models have the same patch path
    #[strum(props(fixable = "false"))]
    ModelsSeparateFromPropertiesFile,
}

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

#[derive(Debug, Deserialize, Serialize, FieldNamesAsSlice)]
pub struct Config {
    // selectors
    #[serde(default = "default_select")]
    pub select: Vec<Selector>,
    #[serde(default)]
    pub exclude: Vec<Selector>,
    #[serde(default = "default_fixable")]
    pub fixable: Vec<Selector>,
    #[serde(default)]
    pub unfixable: Vec<Selector>,
    #[serde(skip)]
    pub fix: bool,

    // args
    #[serde(default = "default_model_fanout_threshold")]
    // I'm intentionally not matching dbt-project-evaluator (models_fanout_threshold)
    // because this makes more sense to me
    // even dbt isn't consistent in it, because the table is fct_model_fanout
    pub model_fanout_threshold: usize,
    #[serde(default)]
    pub required_tests: Vec<String>,
    #[serde(default)]
    pub render_descriptions: bool,
    #[serde(default = "default_writeback")]
    pub writeback: WritebackMethod,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WritebackMethod {
    Python,
    Rust,
}

fn default_writeback() -> WritebackMethod {
    WritebackMethod::Python
}

impl Default for Config {
    fn default() -> Self {
        Self {
            select: default_select(),
            exclude: Vec::new(),
            fixable: default_fixable(),
            unfixable: Vec::new(),
            fix: false,
            model_fanout_threshold: default_model_fanout_threshold(),
            required_tests: Vec::new(),
            render_descriptions: false,
            writeback: default_writeback(),
        }
    }
}

impl Config {
    pub fn is_selected(&self, selector: Selector) -> bool {
        self.select.contains(&selector) && !self.exclude.contains(&selector)
    }

    pub fn is_fixable(&self, selector: Selector) -> bool {
        self.fix
            && self.is_selected(selector)
            && self.fixable.contains(&selector)
            && !self.unfixable.contains(&selector)
    }

    pub fn with_fix(mut self, enable: bool) -> Self {
        self.fix = enable;
        self
    }

    pub fn from_toml(project_dir: &std::path::Path) -> Self {
        let config_path = project_dir.join("dbt-lint.toml");
        if config_path.exists() {
            let config_str =
                std::fs::read_to_string(&config_path).expect("Failed to read dbt-lint.toml");
            Self::try_from_str(&config_str).unwrap_or_else(|err| panic!("{err}"))
        } else {
            Self::default()
        }
    }

    pub fn from_toml_str(toml_str: &str) -> Self {
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

    pub fn to_str(&self) -> String {
        toml::to_string_pretty(self).expect("Failed to serialize Config to TOML")
    }

    pub fn write_to_file(&self, output_path: &std::path::PathBuf) -> std::io::Result<()> {
        let config_path = output_path;
        std::fs::write(config_path, self.to_str())
    }
}

fn default_select() -> Vec<Selector> {
    Selector::iter().collect()
}

fn default_model_fanout_threshold() -> usize {
    3
}

fn default_fixable() -> Vec<Selector> {
    Selector::iter()
        .filter(|s| s.get_str("fixable") == Some("true"))
        .collect()
}

fn validate_keys(table: &toml::value::Table) -> Result<(), ConfigError> {
    let mut unknown_messages = Vec::new();

    for key in table.keys() {
        if !Config::FIELD_NAMES_AS_SLICE.contains(&key.as_str()) {
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
        unknown_messages.push(format!(
            "Supported keys: {}",
            Config::FIELD_NAMES_AS_SLICE.join(", ")
        ));
        Err(ConfigError::UnknownKeys(unknown_messages.join("\n")))
    }
}

fn find_suggestion(unknown: &str) -> Option<&'static str> {
    let (candidate, distance) = Config::FIELD_NAMES_AS_SLICE
        .iter()
        .copied()
        .map(|candidate| (candidate, levenshtein(unknown, candidate)))
        .min_by_key(|(_, distance)| *distance)?;

    if distance <= 3 { Some(candidate) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.select, default_select());
        assert_eq!(config.exclude, Vec::new());
        assert_eq!(config.fixable, default_fixable());
        assert_eq!(config.unfixable, Vec::new());
        assert_eq!(
            config.model_fanout_threshold,
            default_model_fanout_threshold()
        );
        assert!(config.is_selected(Selector::MissingColumnDescriptions));
        assert!(!config.fix, "fix should be disabled by default");
        assert!(
            !config.is_fixable(Selector::MissingColumnDescriptions),
            "fixable selectors require fix to be enabled"
        );
    }

    #[test]
    fn test_from_toml_str() {
        let toml_str = r#"
            select = ["missing_column_descriptions", "missing_model_tags"]
            exclude = ["missing_model_tags"]
            model_fanout_threshold = 4
        "#;
        let config = Config::from_toml_str(toml_str);
        assert_eq!(
            config.select,
            vec![
                Selector::MissingColumnDescriptions,
                Selector::MissingModelTags
            ],
            "Unexpected select"
        );
        assert!(
            !config.is_selected(Selector::MissingModelTags),
            "missing_model_tags not overridden by exclude"
        );
        assert_eq!(
            config.model_fanout_threshold, 4,
            "Unexpected model_fanout_threshold"
        );
    }

    #[test]
    fn test_unknown_key_suggests_alternative() {
        let toml_str = r#"
            models_fanout_threshold = 10
        "#;
        let err = Config::try_from_str(toml_str).expect_err("unknown key should error");
        let message = err.to_string();
        assert!(
            message.contains("models_fanout_threshold"),
            "Unexpected error message"
        );
        assert!(
            message.contains("Did you mean `model_fanout_threshold`"),
            "Missing suggestion"
        );
        assert!(
            message.contains("Supported keys"),
            "Supported keys not listed"
        );
    }

    #[test]
    fn test_from_str_rejects_unknown_selector() {
        let toml_str = r#"
            select = ["missing_model_description"]
        "#;
        let err = toml::from_str::<Config>(toml_str).expect_err("invalid selector should error");
        let message = err.to_string();
        for variant in Selector::iter() {
            let expected = variant.as_ref();
            assert!(
                message.contains(expected),
                "error should mention {expected}, got: {message}"
            );
        }
    }

    #[test]
    fn test_is_fixable() {
        let mut config = Config {
            fix: true,
            ..Default::default()
        };

        assert!(
            config.is_fixable(Selector::MissingColumnDescriptions),
            "MissingColumnDescriptions is fixable"
        );
        assert!(
            !config.is_fixable(Selector::DirectJoinToSource),
            "DirectJoinToSource is not fixable"
        );

        config.exclude = vec![Selector::MissingColumnDescriptions];
        assert!(
            !config.is_fixable(Selector::MissingColumnDescriptions),
            "MissingColumnDescriptions is not fixable if excluded"
        );

        config.exclude.clear();
        config.unfixable = vec![Selector::MissingColumnDescriptions];
        assert!(
            !config.is_fixable(Selector::MissingColumnDescriptions),
            "MissingColumnDescriptions is not fixable if unfixable"
        );
    }
}
