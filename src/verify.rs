//! Independent invariant checkers used by the correctness gate.
//!
//! These functions re read the final cluster state and confirm the properties
//! Shepherd promises, without trusting the code paths that produced that state.
//! Checking constraints from scratch is what makes the fuzz tests meaningful:
//! the scheduler could be wrong and these checkers would still catch it.

use crate::cluster::Cluster;
use crate::object::{selector_matches, AffinityTerm, PodPhase};

/// Confirm every running pod honours the constraints under which it was placed.
/// Returns `Err` with a description of the first violation found.
pub fn verify_constraints(cluster: &Cluster) -> Result<(), String> {
    for pod in cluster.pods.values() {
        if pod.phase != PodPhase::Running {
            continue;
        }
        let node_id = match &pod.node {
            Some(n) => n,
            None => return Err(format!("running pod {} has no node", pod.id)),
        };
        let node = match cluster.nodes.get(node_id) {
            Some(n) => n,
            None => return Err(format!("pod {} bound to missing node {}", pod.id, node_id)),
        };

        if !node.healthy {
            return Err(format!("pod {} runs on unhealthy node {}", pod.id, node_id));
        }

        if !selector_matches(&pod.spec.node_selector, &node.labels) {
            return Err(format!(
                "pod {} node selector not satisfied by node {}",
                pod.id, node_id
            ));
        }

        for taint in &node.taints {
            if !pod.spec.tolerations.iter().any(|t| t.tolerates(taint)) {
                return Err(format!(
                    "pod {} does not tolerate taint {} on node {}",
                    pod.id, taint.key, node_id
                ));
            }
        }

        for term in &pod.spec.affinity {
            let mut others = cluster
                .running_pods_on(node_id)
                .filter(|other| other.id != pod.id);
            match term {
                AffinityTerm::Affinity(sel) => {
                    if !others.any(|o| selector_matches(sel, &o.spec.labels)) {
                        return Err(format!(
                            "pod {} affinity unsatisfied on node {}",
                            pod.id, node_id
                        ));
                    }
                }
                AffinityTerm::AntiAffinity(sel) => {
                    if others.any(|o| selector_matches(sel, &o.spec.labels)) {
                        return Err(format!(
                            "pod {} anti-affinity violated on node {}",
                            pod.id, node_id
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Node, PodTemplate, Taint};
    use crate::scheduler::{schedule_pod, ScorePolicy};

    #[test]
    fn clean_placement_passes() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000), 0);
        let id = c.create_pod("web", PodTemplate::new(500, 500));
        assert!(schedule_pod(&mut c, &id, ScorePolicy::BinPack));
        assert!(verify_constraints(&c).is_ok());
    }

    #[test]
    fn detects_taint_violation() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000), 0);
        let id = c.create_pod("web", PodTemplate::new(500, 500));
        assert!(schedule_pod(&mut c, &id, ScorePolicy::BinPack));
        // Taint the node after the fact so the running pod now violates it.
        c.nodes
            .get_mut("a")
            .unwrap()
            .taints
            .push(Taint::no_schedule("gpu", "true"));
        assert!(verify_constraints(&c).is_err());
    }
}
