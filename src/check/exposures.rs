use crate::config::{Config, Selector};
use dbt_schemas::schemas::{
    common::{Access, DbtMaterialization},
    manifest::{DbtManifestV12, DbtNode, ManifestExposure},
};
use strum::AsRefStr;

#[derive(Debug, Clone, AsRefStr, PartialEq, Eq)]
pub enum ExposureFailure {
    DependentOnPrivateModel(Vec<String>),
    DependentOnMaterializedModel(Vec<String>),
}

impl std::fmt::Display for ExposureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let extra_info = match self {
            ExposureFailure::DependentOnPrivateModel(models) => models.join(", ").to_string(),
            ExposureFailure::DependentOnMaterializedModel(models) => models.join(", ").to_string(),
        };
        write!(f, "{}({})", self.as_ref(), extra_info)
    }
}

#[derive(Debug, Clone)]
pub enum ExposureChange {
    Placeholder,
}

#[derive(Debug, Clone)]
pub struct ExposureResult {
    pub exposure_id: String,
    pub failures: Vec<ExposureFailure>,
    pub changes: Vec<ExposureChange>,
}

pub fn check_exposures(manifest: &DbtManifestV12, config: &Config) -> Vec<ExposureResult> {
    manifest
        .exposures
        .values()
        .map(|exposure| check_exposure(manifest, exposure, config))
        .collect()
}

fn check_exposure(
    manifest: &DbtManifestV12,
    exposure: &ManifestExposure,
    config: &Config,
) -> ExposureResult {
    let mut failures = vec![];
    let mut changes = vec![];

    match exposure_dependent_on_private_model(exposure, manifest, config) {
        Ok(Some(change)) => changes.push(change),
        Err(failure) => failures.push(failure),
        _ => {}
    }
    match exposure_parents_materializations(exposure, manifest, config) {
        Ok(Some(change)) => changes.push(change),
        Err(failure) => failures.push(failure),
        _ => {}
    }

    ExposureResult {
        exposure_id: exposure.__common_attr__.unique_id.clone(),
        failures,
        changes,
    }
}

// possible unsafe fix, just make the models public?
fn exposure_dependent_on_private_model(
    exposure: &ManifestExposure,
    manifest: &DbtManifestV12,
    config: &Config,
) -> Result<Option<ExposureChange>, ExposureFailure> {
    if !config.is_selected(Selector::ExposureDependentOnPrivateModel) {
        return Ok(None);
    }

    let depends_on = &exposure.__base_attr__.depends_on.nodes;
    // only models have access (to my knowledge)
    let nodes = depends_on.iter().filter(|node| node.starts_with("model"));

    let private_models: Vec<String> = nodes
        .filter_map(|node_name| {
            let node = manifest.nodes.get(node_name)?;
            match node {
                DbtNode::Model(model) => {
                    (model.access == Some(Access::Private)).then_some(node_name.clone())
                }
                _ => None,
            }
        })
        .collect();
    if !private_models.is_empty() {
        return Err(ExposureFailure::DependentOnPrivateModel(private_models));
    }
    Ok(None)
}

/// https://dbt-labs.github.io/dbt-project-evaluator/latest/rules/performance/#exposure-parents-materializations
fn exposure_parents_materializations(
    exposure: &ManifestExposure,
    manifest: &DbtManifestV12,
    config: &Config,
) -> Result<Option<ExposureChange>, ExposureFailure> {
    if !config.is_selected(Selector::ExposureParentsMaterializations) {
        return Ok(None);
    }

    let depends_on = &exposure.__base_attr__.depends_on.nodes;
    let nodes = depends_on.iter().filter(|node| node.starts_with("model"));

    let materialized_parents: Vec<String> = nodes
        .filter_map(|node_name| {
            let node = manifest.nodes.get(node_name)?;
            match node {
                DbtNode::Model(model) => {
                    // fail if materialized is not table or incremental
                    match model.config.materialized {
                        Some(DbtMaterialization::Table) | Some(DbtMaterialization::Incremental) => {
                            None
                        }
                        _ => Some(node_name.clone()),
                    }
                }
                _ => None,
            }
        })
        .collect();
    if !materialized_parents.is_empty() {
        return Err(ExposureFailure::DependentOnMaterializedModel(
            materialized_parents,
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposure_dependent_on_private_model_detects_private() {
        let mut manifest = DbtManifestV12::default();

        // insert an upstream model and mark it private
        manifest.nodes.insert(
            "model.test.upstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        if let Some(DbtNode::Model(upstream)) = manifest.nodes.get_mut("model.test.upstream") {
            upstream.__common_attr__.unique_id = "model.test.upstream".to_string();
            upstream.access = Some(Access::Private);
        }

        // build an exposure that depends on the private model
        let mut exposure = ManifestExposure {
            __common_attr__: Default::default(),
            __base_attr__: Default::default(),
            owner: Default::default(),
            label: None,
            maturity: None,
            type_: "user".to_string(),
            url: None,
            config: Default::default(),
            __other__: Default::default(),
        };
        exposure.__common_attr__.unique_id = "exposure.test.dep".to_string();
        exposure.__base_attr__.depends_on.nodes = vec!["model.test.upstream".to_string()];

        let cfg = Config {
            select: vec![Selector::ExposureDependentOnPrivateModel],
            ..Default::default()
        };

        let res = exposure_dependent_on_private_model(&exposure, &manifest, &cfg);
        assert!(res.is_err());
        if let Err(ExposureFailure::DependentOnPrivateModel(models)) = res {
            assert_eq!(models, vec!["model.test.upstream".to_string()]);
        } else {
            panic!("expected DependentOnPrivateModel failure");
        }
    }

    #[test]
    fn exposure_dependent_on_private_model_passes_when_public() {
        let mut manifest = DbtManifestV12::default();

        manifest.nodes.insert(
            "model.test.upstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        if let Some(DbtNode::Model(upstream)) = manifest.nodes.get_mut("model.test.upstream") {
            upstream.__common_attr__.unique_id = "model.test.upstream".to_string();
            upstream.access = Some(Access::Public);
        }

        let mut exposure = ManifestExposure {
            __common_attr__: Default::default(),
            __base_attr__: Default::default(),
            owner: Default::default(),
            label: None,
            maturity: None,
            type_: "user".to_string(),
            url: None,
            config: Default::default(),
            __other__: Default::default(),
        };
        exposure.__common_attr__.unique_id = "exposure.test.dep".to_string();
        exposure.__base_attr__.depends_on.nodes = vec!["model.test.upstream".to_string()];

        let cfg = Config {
            select: vec![Selector::ExposureDependentOnPrivateModel],
            ..Default::default()
        };

        let res = exposure_dependent_on_private_model(&exposure, &manifest, &cfg);
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }
}
