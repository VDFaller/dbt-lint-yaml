use dbt_schemas::schemas::{
    dbt_column::DbtColumn,
    manifest::{DbtManifestV12, DbtNode},
};
use std::sync::Arc;

pub fn inherit_column_descriptions(
    manifest: &mut DbtManifestV12,
    node_id: &str,
    col_name: &str,
) -> Result<(), String> {
    // This function will inherit column descriptions from the upstream model or source
    // todo: add sources, seeds, snapshots
    // mark unsafe if multiple upstream models have same column name
    //    or even better, know which upstream model to inherit from (SDF style)
    //    could possibly use the cached target/db/dbt/information_schema/output.parquet.  Not sure what would be faster.

    let desc = match get_upstream_col_desc(manifest, node_id, col_name) {
        Some(desc) => desc,
        None => {
            return Err(format!(
                "No upstream description found for column {} in node {}",
                col_name, node_id
            ));
        }
    };
    let model = match manifest.nodes.get_mut(node_id) {
        Some(DbtNode::Model(model)) => model,
        Some(_) => return Err(format!("Node with id {} is not a model", node_id)),
        None => return Err(format!("Node with id {} not found", node_id)),
    };

    let col_entry = match model.__base_attr__.columns.get_mut(col_name) {
        Some(col) => col,
        None => {
            return Err(format!(
                "Column {} not found in model {}",
                col_name, node_id
            ));
        }
    };

    if let Some(col) = Arc::get_mut(col_entry) {
        col.description = Some(desc.clone());
    } else {
        let col = col_entry.as_ref();
        let replacement = DbtColumn {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            description: Some(desc),
            constraints: col.constraints.clone(),
            meta: col.meta.clone(),
            tags: col.tags.clone(),
            policy_tags: col.policy_tags.clone(),
            quote: col.quote,
            deprecated_config: col.deprecated_config.clone(),
        };

        *col_entry = Arc::new(replacement);
    }
    Ok(())
}

fn get_upstream_col_desc(
    manifest: &DbtManifestV12,
    node_id: &str,
    col_name: &str,
) -> Option<String> {
    if let Some(DbtNode::Model(model)) = manifest.nodes.get(node_id) {
        let desc = model
            .__base_attr__
            .depends_on
            .nodes
            .iter()
            .filter_map(|upstream_id| {
                // the upstream id can be a node or a source
                manifest
                    .nodes
                    .get(upstream_id)
                    .map(|upstream_node| match upstream_node {
                        DbtNode::Model(upstream_model) => {
                            upstream_model.__base_attr__.columns.get(col_name)
                        }
                        DbtNode::Seed(upstream_seed) => {
                            upstream_seed.__base_attr__.columns.get(col_name)
                        }
                        DbtNode::Snapshot(upstream_snapshot) => {
                            upstream_snapshot.__base_attr__.columns.get(col_name)
                        }
                        _ => None,
                    })
                    .flatten()
                    .or_else(|| {
                        manifest
                            .sources
                            .get(upstream_id)
                            .and_then(|source| source.columns.get(col_name))
                    })
            })
            .filter_map(|dep_col| (*dep_col).description.as_ref().cloned())
            .next();
        return desc;
    } else {
        return None;
    }
}
