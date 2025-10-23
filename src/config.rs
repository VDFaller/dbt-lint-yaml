use serde::{Deserialize, Serialize};
use std::path::PathBuf;
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
    #[strum(props(fixable = "true"))]
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
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    UnknownKeys(String),
    #[error("Config must be a TOML table at the root")]
    InvalidRoot,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ConfigFile {
    // everything that maps into the typed runtime Config will be under base (or flattened)
    #[serde(flatten)]
    pub base: std::collections::HashMap<String, toml::Value>,
    #[serde(default)]
    pub target: std::collections::HashMap<String, toml::Value>, // target -> table or profile -> target -> table
}

// Helper: deep-merge `b` into `a` (a <- b). Tables recurse; primitives/arrays replace.
fn deep_merge(a: &mut toml::Value, b: &toml::Value) {
    if let (Some(a_tab), Some(b_tab)) = (a.as_table_mut(), b.as_table()) {
        for (k, v_b) in b_tab {
            match a_tab.get_mut(k) {
                Some(v_a) => {
                    if v_a.is_table() && v_b.is_table() {
                        deep_merge(v_a, v_b);
                    } else {
                        *v_a = v_b.clone();
                    }
                }
                None => {
                    a_tab.insert(k.clone(), v_b.clone());
                }
            }
        }
    } else {
        // Non-tables: replace
        *a = b.clone();
    }
}

impl ConfigFile {
    /// Resolve a `dbt-lint.toml` found under `iarg.project_dir` for the profile/target
    /// Returns a Result with a `ConfigError` on parse/validation/deserialization/IO errors.
    pub fn resolve(
        iarg: &dbt_jinja_utils::invocation_args::InvocationArgs,
    ) -> Result<Config, ConfigError> {
        let config_path = std::path::Path::new(&iarg.project_dir).join("dbt-lint.toml");
        if !config_path.exists() {
            return Ok(Config::default());
        }
        let config_str = std::fs::read_to_string(&config_path)?;

        // Reuse the string-based resolver to avoid duplicating parsing/merge/validation logic
        ConfigFile::resolve_from_toml_str(&config_str, iarg)
    }

    /// Test-friendly helper function so I can resolve a TOML string as if it were a file.
    pub fn resolve_from_toml_str(
        toml_str: &str,
        iarg: &dbt_jinja_utils::invocation_args::InvocationArgs,
    ) -> Result<Config, ConfigError> {
        let config_file: ConfigFile = toml::from_str(toml_str).map_err(ConfigError::Toml)?;

        // Build base toml::Value from flattened `base` HashMap
        let mut base_table: toml::value::Table = toml::value::Table::new();
        for (k, v) in config_file.base.into_iter() {
            base_table.insert(k, v);
        }
        let mut base_value = toml::Value::Table(base_table);

        // Determine profile and target from InvocationArgs
        let profile_opt = if iarg.profile.is_empty() {
            None
        } else {
            Some(iarg.profile.as_str())
        };
        let target_opt = iarg.target.as_deref();

        // Try profile-scoped override first: target.<profile>.<target>
        if let (Some(profile), Some(target)) = (profile_opt, target_opt) {
            if let Some(profile_val) = config_file.target.get(profile)
                && let Some(profile_tab) = profile_val.as_table()
                && let Some(found) = profile_tab.get(target)
            {
                deep_merge(&mut base_value, found);
            }
        } else if let Some(target) = target_opt {
            // If no profile-scoped override, try target-only: target.<target>
            if let Some(found) = config_file.target.get(target) {
                deep_merge(&mut base_value, found);
            }
        }

        // Validate merged top-level keys to avoid fat fingering
        if let Some(table) = base_value.as_table() {
            validate_keys(table)?;
        } else {
            return Err(ConfigError::InvalidRoot);
        }

        // Deserialize to typed Config
        let mut config: Config = base_value.try_into().map_err(ConfigError::Deserialize)?;
        config.project_dir = Some(std::path::Path::new(&iarg.project_dir).to_path_buf());
        Ok(config)
    }
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

    #[serde(skip)]
    pub project_dir: Option<PathBuf>,

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
            project_dir: None,
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
        // parsing helpers were removed; keep this test empty or convert to using ConfigFile.resolve
    }

    #[test]
    fn test_unknown_key_suggests_alternative() {
        let toml_str = r#"
            models_fanout_threshold = 10
        "#;

        use dbt_jinja_utils::invocation_args::InvocationArgs;
        let iarg = InvocationArgs {
            project_dir: String::new(),
            profile: String::new(),
            target: None,
            ..Default::default()
        };

        // Call resolve_from_toml_str and assert it returns an Err with a helpful message
        let result = ConfigFile::resolve_from_toml_str(toml_str, &iarg);
        assert!(
            result.is_err(),
            "resolve_from_toml_str should return Err for unknown key"
        );
        let err = result.err().unwrap();
        let message = err.to_string();

        assert!(
            message.contains("models_fanout_threshold"),
            "Unexpected error message: {message}"
        );
        assert!(
            message.contains("Did you mean `model_fanout_threshold`"),
            "Missing suggestion in: {message}"
        );
        assert!(
            message.contains("Supported keys"),
            "Supported keys not listed in: {message}"
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

    #[test]
    fn test_resolve_target_override_jaffle_shop() {
        use dbt_jinja_utils::invocation_args::InvocationArgs;

        let iarg = InvocationArgs {
            target: Some("dev".to_string()),
            profile: String::new(),
            project_dir: std::path::PathBuf::from("tests/jaffle_shop")
                .display()
                .to_string(),
            ..Default::default()
        };

        let cfg = ConfigFile::resolve(&iarg).expect("resolve failed");

        // The tests/jaffle_shop/dbt-lint.toml sets model_fanout_threshold = 3
        // at top-level and overrides it to 5 under [target.dbx]
        assert_eq!(cfg.model_fanout_threshold, 5);

        // The override also sets a specific `select` list
        assert_eq!(
            cfg.select,
            vec![
                Selector::MissingColumnDescriptions,
                Selector::MissingModelDescriptions
            ]
        );
    }
}
