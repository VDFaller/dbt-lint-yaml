use crate::{change_descriptors::ColumnChange, check::ModelChanges, config::Config};
use dbt_schemas::schemas::manifest::{DbtManifestV12, DbtNode};
use std::collections::BTreeMap;

pub(crate) fn get_upstream_col_desc(
    manifest: &DbtManifestV12,
    model_changes: Option<&BTreeMap<String, ModelChanges>>,
    node_id: &str,
    col_name: &str,
    config: &Config,
) -> Option<String> {
    let upstream_ids = manifest.nodes.get(node_id).and_then(|node| match node {
        DbtNode::Model(model) => Some(model.__base_attr__.depends_on.nodes.clone()),
        _ => None,
    })?;

    // check the changes first on the assumption that manifest will be much bigger than changes
    if let Some(changes) = model_changes {
        for upstream_id in &upstream_ids {
            if let Some(desc) = lookup_model_change_description(changes, upstream_id, col_name) {
                return Some(desc);
            }
        }
    }

    let desc = upstream_ids
        .iter()
        .filter_map(|upstream_id| {
            // the upstream id can be a node or a source
            manifest
                .nodes
                .get(upstream_id)
                .and_then(|upstream_node| match upstream_node {
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
                .or_else(|| {
                    manifest
                        .sources
                        .get(upstream_id)
                        .and_then(|source| source.columns.get(col_name))
                })
        })
        .filter_map(|dep_col| {
            dep_col.as_ref().description.as_ref().and_then(|d| {
                let trimmed = d.trim();
                if trimmed.is_empty()
                    || config
                        .invalid_descriptions
                        .iter()
                        .any(|bad| bad.eq_ignore_ascii_case(trimmed))
                {
                    None
                } else {
                    Some(d.clone())
                }
            })
        })
        .next();

    if config.render_descriptions {
        return desc;
    }
    // Prefer a docs block reference when the description matches exactly.
    desc.map(|d| {
        let doc_reference = manifest.docs.values().find_map(|doc| {
            if doc.block_contents == d {
                Some(format!("{{{{doc('{name}')}}}}", name = doc.name))
            } else {
                None
            }
        });

        doc_reference.unwrap_or(d)
    })
}

fn lookup_model_change_description(
    model_changes: &BTreeMap<String, ModelChanges>,
    upstream_id: &str,
    col_name: &str,
) -> Option<String> {
    model_changes.get(upstream_id).and_then(|change| {
        change.column_changes.get(col_name).and_then(|changes| {
            changes
                .iter()
                .find_map(|column_change| match column_change {
                    ColumnChange::DescriptionChanged { new, .. } => new.clone(),
                })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{get_upstream_col_desc, lookup_model_change_description};
    use crate::{
        check::{ColumnChange, ModelChanges},
        config::Config,
    };
    use dbt_schemas::schemas::{
        dbt_column::DbtColumn,
        manifest::{DbtManifestV12, DbtNode, ManifestSeed},
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    // FIXTURES
    fn model_changes_fixture() -> BTreeMap<String, ModelChanges> {
        let mut customers_columns = BTreeSet::new();
        customers_columns.insert(ColumnChange::DescriptionChanged {
            model_id: "model.jaffle_shop.customers".to_string(),
            model_name: "customers".to_string(),
            patch_path: None,
            column_name: "customer_id".to_string(),
            old: Some("Old description".to_string()),
            new: Some("Fresh description".to_string()),
        });

        let mut orders_columns = BTreeSet::new();
        orders_columns.insert(ColumnChange::DescriptionChanged {
            model_id: "model.jaffle_shop.orders".to_string(),
            model_name: "orders".to_string(),
            patch_path: None,
            column_name: "order_id".to_string(),
            old: None,
            new: Some("New order description".to_string()),
        });

        let mut map = BTreeMap::new();
        map.insert(
            "model.jaffle_shop.customers".to_string(),
            ModelChanges {
                model_id: "model.jaffle_shop.customers".to_string(),
                patch_path: None,
                changes: Vec::new(),
                column_changes: {
                    let mut column_changes = BTreeMap::new();
                    column_changes.insert("customer_id".to_string(), customers_columns);
                    column_changes
                },
            },
        );

        map.insert(
            "model.jaffle_shop.orders".to_string(),
            ModelChanges {
                model_id: "model.jaffle_shop.orders".to_string(),
                patch_path: None,
                changes: Vec::new(),
                column_changes: {
                    let mut column_changes = BTreeMap::new();
                    column_changes.insert("order_id".to_string(), orders_columns);
                    column_changes
                },
            },
        );

        map
    }

    fn column_with_description(name: &str, description: &str) -> Arc<DbtColumn> {
        let column = DbtColumn {
            name: name.to_string(),
            description: Some(description.to_string()),
            ..Default::default()
        };
        Arc::new(column)
    }

    fn column_without_description(name: &str) -> Arc<DbtColumn> {
        let column = DbtColumn {
            name: name.to_string(),
            description: None,
            ..Default::default()
        };
        Arc::new(column)
    }

    fn manifest_fixture() -> DbtManifestV12 {
        // DAG overview for this fixture:
        //
        // source.jaffle_shop.customers ─▶ model.jaffle_shop.base_customers ─▶ model.jaffle_shop.customers ─▶ model.jaffle_shop.orders
        //                                         ▲                                      ▲
        //                                         │                                      │
        // snapshot.jaffle_shop.customers_snapshot ┘                                      │
        //                                                                                │
        // seed.jaffle_shop.payments ─▶ model.jaffle_shop.payments ───────────────────────┘
        let mut manifest = DbtManifestV12::default();

        manifest.nodes.insert(
            "model.jaffle_shop.base_customers".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.jaffle_shop.customers".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.jaffle_shop.orders".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.jaffle_shop.payments".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "seed.jaffle_shop.payments".to_string(),
            DbtNode::Seed(ManifestSeed {
                __common_attr__: Default::default(),
                __base_attr__: Default::default(),
                config: Default::default(),
                root_path: None,
                __other__: Default::default(),
            }),
        );
        manifest.nodes.insert(
            "snapshot.jaffle_shop.customers_snapshot".to_string(),
            DbtNode::Snapshot(Default::default()),
        );

        manifest.sources.insert(
            "source.jaffle_shop.customers".to_string(),
            Default::default(),
        );

        manifest
            .sources
            .get_mut("source.jaffle_shop.customers")
            .expect("source should exist")
            .columns
            .insert(
                "customer_id".to_string(),
                column_with_description("customer_id", "Customer id from source"),
            );

        match manifest
            .nodes
            .get_mut("seed.jaffle_shop.payments")
            .expect("seed should exist")
        {
            DbtNode::Seed(seed) => {
                seed.__base_attr__.columns.insert(
                    "payment_id".to_string(),
                    column_without_description("payment_id"),
                );
            }
            _ => unreachable!(),
        }

        match manifest
            .nodes
            .get_mut("snapshot.jaffle_shop.customers_snapshot")
            .expect("snapshot should exist")
        {
            DbtNode::Snapshot(snapshot) => {
                snapshot.__base_attr__.columns.insert(
                    "customer_id".to_string(),
                    column_with_description("customer_id", "Customer id from snapshot"),
                );
            }
            _ => unreachable!(),
        }

        match manifest
            .nodes
            .get_mut("model.jaffle_shop.base_customers")
            .expect("base_customers should exist")
        {
            DbtNode::Model(model) => {
                model.__base_attr__.depends_on.nodes =
                    vec!["source.jaffle_shop.customers".to_string()];
            }
            _ => unreachable!(),
        }

        match manifest
            .nodes
            .get_mut("model.jaffle_shop.customers")
            .expect("customers should exist")
        {
            DbtNode::Model(model) => {
                model.__base_attr__.depends_on.nodes = vec![
                    "model.jaffle_shop.base_customers".to_string(),
                    "seed.jaffle_shop.payments".to_string(),
                ];
                model.__base_attr__.columns.insert(
                    "customer_id".to_string(),
                    column_with_description("customer_id", "Customer id from manifest"),
                );
            }
            _ => unreachable!(),
        }

        match manifest
            .nodes
            .get_mut("model.jaffle_shop.orders")
            .expect("orders should exist")
        {
            DbtNode::Model(model) => {
                model.__base_attr__.depends_on.nodes =
                    vec!["model.jaffle_shop.customers".to_string()];
            }
            _ => unreachable!(),
        }

        match manifest
            .nodes
            .get_mut("model.jaffle_shop.payments")
            .expect("payments model should exist")
        {
            DbtNode::Model(model) => {
                model.__base_attr__.depends_on.nodes =
                    vec!["seed.jaffle_shop.payments".to_string()];
            }
            _ => unreachable!(),
        }

        manifest
    }

    // get_upstream_col_desc tests
    #[test]
    fn prefers_model_changes_over_manifest_columns() {
        let manifest = manifest_fixture();
        let model_changes_map = model_changes_fixture();

        let result = get_upstream_col_desc(
            &manifest,
            Some(&model_changes_map),
            "model.jaffle_shop.orders",
            "customer_id",
            &Config::default(),
        );

        assert_eq!(result.as_deref(), Some("Fresh description"));
    }

    #[test]
    fn returns_description_from_upstream_model_column() {
        let manifest = manifest_fixture();

        let result = get_upstream_col_desc(
            &manifest,
            None,
            "model.jaffle_shop.orders",
            "customer_id",
            &Config::default(),
        );

        assert_eq!(result.as_deref(), Some("Customer id from manifest"));
    }

    #[test]
    fn returns_description_from_upstream_source_column() {
        let manifest = manifest_fixture();

        let result = get_upstream_col_desc(
            &manifest,
            None,
            "model.jaffle_shop.base_customers",
            "customer_id",
            &Config::default(),
        );

        assert_eq!(result.as_deref(), Some("Customer id from source"));
    }

    #[test]
    fn returns_none_when_no_upstream_description_found() {
        let manifest = manifest_fixture();
        let model_changes_map = model_changes_fixture();

        let result = get_upstream_col_desc(
            &manifest,
            Some(&model_changes_map),
            "model.jaffle_shop.payments",
            "payment_id",
            &Config::default(),
        );

        assert!(result.is_none());
    }

    #[test]
    fn does_not_propagate_invalid_upstream_model_description() {
        let mut manifest = DbtManifestV12::default();

        manifest.nodes.insert(
            "model.upstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        manifest.nodes.insert(
            "model.downstream".to_string(),
            DbtNode::Model(Default::default()),
        );

        // upstream column has a placeholder description
        match manifest.nodes.get_mut("model.upstream").unwrap() {
            DbtNode::Model(model) => {
                model
                    .__base_attr__
                    .columns
                    .insert("col".to_string(), column_with_description("col", "TBD"));
            }
            _ => unreachable!(),
        }

        // downstream depends on upstream
        match manifest.nodes.get_mut("model.downstream").unwrap() {
            DbtNode::Model(model) => {
                model.__base_attr__.depends_on.nodes = vec!["model.upstream".to_string()];
            }
            _ => unreachable!(),
        }

        let result = get_upstream_col_desc(
            &manifest,
            None,
            "model.downstream",
            "col",
            &Config::default(),
        );
        assert!(result.is_none());
    }

    #[test]
    fn does_not_propagate_invalid_upstream_source_description() {
        let mut manifest = DbtManifestV12::default();

        // create a source with a placeholder column description
        manifest
            .sources
            .insert("source.test.src".to_string(), Default::default());
        manifest
            .sources
            .get_mut("source.test.src")
            .unwrap()
            .columns
            .insert(
                "col".to_string(),
                column_with_description("col", "FILL ME OUT"),
            );

        // downstream model depends on the source
        manifest.nodes.insert(
            "model.downstream".to_string(),
            DbtNode::Model(Default::default()),
        );
        match manifest.nodes.get_mut("model.downstream").unwrap() {
            DbtNode::Model(model) => {
                model.__base_attr__.depends_on.nodes = vec!["source.test.src".to_string()];
            }
            _ => unreachable!(),
        }

        let result = get_upstream_col_desc(
            &manifest,
            None,
            "model.downstream",
            "col",
            &Config::default(),
        );
        assert!(result.is_none());
    }

    // lookup_model_change_description tests
    #[test]
    fn returns_new_description_when_present() {
        let model_changes_map = model_changes_fixture();

        let result = lookup_model_change_description(
            &model_changes_map,
            "model.jaffle_shop.customers",
            "customer_id",
        );

        assert_eq!(result.as_deref(), Some("Fresh description"));
    }

    #[test]
    fn returns_none_when_model_or_column_missing() {
        let model_changes_map = model_changes_fixture();

        let missing_column = lookup_model_change_description(
            &model_changes_map,
            "model.jaffle_shop.orders",
            "customer_id",
        );
        assert!(missing_column.is_none());

        let missing_model = lookup_model_change_description(
            &model_changes_map,
            "model.jaffle_shop.customers",
            "order_id",
        );
        assert!(missing_model.is_none());
    }
}
