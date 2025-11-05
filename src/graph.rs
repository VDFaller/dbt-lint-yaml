use dbt_schemas::schemas::manifest::DbtManifestV12;
use petgraph::Direction;
use petgraph::graph::{Graph, NodeIndex};
use std::collections::HashMap;

pub struct DbtGraph {
    pub graph: Graph<String, ()>,
    pub index: HashMap<String, NodeIndex>,
}

impl DbtGraph {
    pub fn children(&self, uid: &str) -> impl Iterator<Item = String> {
        self.index
            .get(uid)
            .map(|&node_idx| self.graph.neighbors_directed(node_idx, Direction::Outgoing))
            .into_iter()
            .flatten()
            .filter_map(|n| self.graph.node_weight(n).cloned())
    }

    pub fn parents(&self, uid: &str) -> impl Iterator<Item = String> {
        self.index
            .get(uid)
            .map(|&node_idx| self.graph.neighbors_directed(node_idx, Direction::Incoming))
            .into_iter()
            .flatten()
            .filter_map(|n| self.graph.node_weight(n).cloned())
    }
}

impl From<&DbtManifestV12> for DbtGraph {
    fn from(manifest: &DbtManifestV12) -> Self {
        use std::collections::BTreeSet;

        let mut graph = Graph::<String, ()>::new();
        let mut index: HashMap<String, NodeIndex> = HashMap::new();

        // Collect all UIDs appearing as keys or values in child_map
        let mut all_uids: BTreeSet<String> = BTreeSet::new();
        for (parent, children) in manifest.child_map.iter() {
            all_uids.insert(parent.clone());
            for child in children.iter() {
                all_uids.insert(child.clone());
            }
        }

        // create nodes for every uid found in the child_map
        for uid in all_uids {
            let ni = graph.add_node(uid.clone());
            index.insert(uid, ni);
        }

        // add edges parent -> child
        for (parent, children) in manifest.child_map.iter() {
            if let Some(&p_idx) = index.get(parent) {
                for child in children.iter() {
                    if let Some(&c_idx) = index.get(child) {
                        graph.add_edge(p_idx, c_idx, ());
                    }
                }
            }
        }

        DbtGraph { graph, index }
    }
}