use crate::config::{Config, Selector};
use dbt_schemas::schemas::manifest::{DbtManifestV12, ManifestSource};
use std::fmt::Display;

#[derive(Default, Debug)]
pub struct SourceFailure {
    pub source_id: String,
    pub description_missing: bool,
    pub duplicate_id: Option<String>,
    pub is_unused_source: bool,
    pub is_missing_source_freshness: bool,
    pub is_missing_source_description: bool,
    pub is_source_fanout: bool,
}

impl Display for SourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SourceFailure: {}", self.source_id)?;
        for reason in self.failure_reasons() {
            writeln!(f, "  - {reason}")?;
        }
        Ok(())
    }
}

impl SourceFailure {
    pub fn failure_reasons(&self) -> Vec<String> {
        let mut reasons = Vec::new();

        self.description_missing
            .then(|| reasons.push("Missing Description".to_string()));
        if let Some(duplicate_id) = self.duplicate_id.as_ref() {
            reasons.push(format!("Duplicate Source Definition: {duplicate_id}"))
        }
        self.is_unused_source
            .then(|| reasons.push("Unused Source".to_string()));
        self.is_missing_source_freshness
            .then(|| reasons.push("Missing Source Freshness".to_string()));
        self.is_missing_source_description
            .then(|| reasons.push("Missing Source Description".to_string()));
        self.is_source_fanout
            .then(|| reasons.push("Source Fanout".to_string()));
        reasons
    }
}

#[derive(Default, Debug)]
pub struct SourceSuccess {
    pub source_id: String,
}

#[derive(Debug)]
pub enum SourceResult {
    Pass(SourceSuccess),
    Fail(SourceFailure),
}

impl SourceResult {
    pub fn source_id(&self) -> &str {
        match self {
            SourceResult::Pass(success) => &success.source_id,
            SourceResult::Fail(failure) => &failure.source_id,
        }
    }

    pub fn as_failure(&self) -> Option<&SourceFailure> {
        if let SourceResult::Fail(failure) = self {
            Some(failure)
        } else {
            None
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, SourceResult::Fail(_))
    }
}

pub(crate) fn check_source(
    manifest: &DbtManifestV12,
    source: &ManifestSource,
    config: &Config,
) -> SourceResult {
    let source_id = source.__common_attr__.unique_id.clone();
    let description_missing = config
        .select
        .contains(&Selector::MissingSourceTableDescriptions)
        && source.__common_attr__.description.is_none();
    let duplicate_id = config
        .select
        .contains(&Selector::DuplicateSources)
        .then(|| duplicate_source(manifest, source))
        .flatten();
    let is_unused_source = unused_source(manifest, source, config);
    let is_missing_source_freshness = missing_source_freshness(source, config);
    let is_missing_source_description = missing_source_description(source, config);
    let is_source_fanout = source_fanout(manifest, source, config);

    if description_missing
        || duplicate_id.is_some()
        || is_unused_source
        || is_missing_source_freshness
        || is_missing_source_description
        || is_source_fanout
    {
        SourceResult::Fail(SourceFailure {
            source_id,
            description_missing,
            duplicate_id,
            is_unused_source,
            is_missing_source_freshness,
            is_missing_source_description,
            is_source_fanout,
        })
    } else {
        SourceResult::Pass(SourceSuccess { source_id })
    }
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/documentation/#undocumented-sources
fn missing_source_description(source: &ManifestSource, config: &Config) -> bool {
    if !config.select.contains(&Selector::MissingSourceDescriptions) {
        return false;
    }
    source.source_description.is_empty()
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#source-fanout
fn source_fanout(manifest: &DbtManifestV12, source: &ManifestSource, config: &Config) -> bool {
    if !config.select.contains(&Selector::SourceFanout) {
        return false;
    }

    manifest
        .child_map
        .get(&source.__common_attr__.unique_id)
        .map(|children| children.len())
        .unwrap_or(0)
        > 1
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#duplicate-sources
fn duplicate_source(manifest: &DbtManifestV12, source: &ManifestSource) -> Option<String> {
    if source.__common_attr__.name == source.identifier {
        return None;
    }
    // TODO: look into performance of this search in a larger project
    manifest
        .sources
        .values()
        .find(|s| {
            s.identifier == source.identifier
                && s.source_name == source.source_name
                && s.__common_attr__.unique_id != source.__common_attr__.unique_id
        })
        .map(|s| s.__common_attr__.unique_id.clone())
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/modeling/#unused-sources
fn unused_source(manifest: &DbtManifestV12, source: &ManifestSource, config: &Config) -> bool {
    // A source is considered "used" if any model depends on it
    if !config.select.contains(&Selector::UnusedSources) {
        return false;
    }
    manifest
        .child_map
        .get(&source.__common_attr__.unique_id)
        .unwrap_or(&vec![])
        .is_empty()
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/testing/#missing-source-freshness
fn missing_source_freshness(source: &ManifestSource, config: &Config) -> bool {
    if !config.select.contains(&Selector::MissingSourceFreshness) {
        return false;
    }
    if let Some(freshness) = &source.freshness {
        return freshness.warn_after.is_none() && freshness.error_after.is_none();
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::Config;
    use dbt_schemas::schemas::common::{FreshnessDefinition, FreshnessPeriod, FreshnessRules};
    use dbt_schemas::schemas::manifest::ManifestSource;

    #[test]
    fn test_source_failure_reasons() {
        let failure = SourceFailure {
            source_id: "source.test.raw.orders".to_string(),
            description_missing: true,
            duplicate_id: Some("source.test.raw.dupe".to_string()),
            is_unused_source: true,
            is_missing_source_freshness: true,
            is_missing_source_description: true,
            is_source_fanout: false,
        };

        let reasons = failure.failure_reasons();
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("Missing Description"))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("Duplicate Source Definition"))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("Unused Source"))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("Missing Source Freshness"))
        );
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("Missing Source Description"))
        );
    }

    #[test]
    fn test_missing_source_description() {
        let source = ManifestSource {
            source_description: String::new(),
            ..Default::default()
        };
        let config = Config::default();
        assert!(missing_source_description(&source, &config));
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_duplicate_source() {
        let mut manifest = DbtManifestV12::default();
        let mut source = ManifestSource::default();
        source.identifier = "orders".to_string();
        source.source_name = "raw".to_string();
        source.__common_attr__.name = "different".to_string();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        manifest
            .sources
            .insert(source.__common_attr__.unique_id.clone(), source.clone());

        let mut duplicate = ManifestSource::default();
        duplicate.identifier = "orders".to_string();
        duplicate.source_name = "raw".to_string();
        duplicate.__common_attr__.unique_id = "source.raw.orders_dupe".to_string();
        manifest.sources.insert(
            duplicate.__common_attr__.unique_id.clone(),
            duplicate.clone(),
        );

        assert_eq!(
            duplicate_source(&manifest, &duplicate),
            Some("source.raw.orders".to_string())
        );
    }

    #[test]
    fn test_unused_source() {
        let mut manifest = DbtManifestV12::default();
        let mut source = ManifestSource::default();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        manifest
            .child_map
            .insert(source.__common_attr__.unique_id.clone(), vec![]);
        let config = Config::default();
        assert!(unused_source(&manifest, &source, &config));
    }

    #[test]
    fn test_missing_source_freshness() {
        let mut source = ManifestSource::default();
        let mut freshness = FreshnessDefinition::default();
        source.freshness = Some(freshness.clone());
        let config = Config::default();
        assert!(missing_source_freshness(&source, &config));

        freshness.warn_after = Some(FreshnessRules {
            count: Some(1),
            period: Some(FreshnessPeriod::day),
        });
        source.freshness = Some(freshness.clone());
        assert!(!missing_source_freshness(&source, &config));
    }

    #[test]
    fn test_check_source_failure() {
        let mut manifest = DbtManifestV12::default();
        let mut source = ManifestSource::default();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        manifest
            .sources
            .insert(source.__common_attr__.unique_id.clone(), source.clone());
        manifest
            .child_map
            .insert(source.__common_attr__.unique_id.clone(), vec![]);

        let config = Config::default();
        let result = check_source(&manifest, &source, &config);
        assert!(result.is_failure());
    }

    #[test]
    fn test_source_fanout() {
        let mut child_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        child_map.insert(
            "source.raw.orders".to_string(),
            vec![
                "model.test.orders".to_string(),
                "model.test.orders_summary".to_string(),
            ],
        );
        let mut sources: BTreeMap<String, ManifestSource> = BTreeMap::new();
        let mut source = ManifestSource::default();
        source.__common_attr__.unique_id = "source.raw.orders".to_string();
        sources.insert(source.__common_attr__.unique_id.clone(), source.clone());
        let manifest = DbtManifestV12 {
            child_map,
            sources,
            ..Default::default()
        };
        let source = manifest.sources.get("source.raw.orders").unwrap();
        let config = Config::default();
        assert!(source_fanout(&manifest, source, &config));
    }
}
