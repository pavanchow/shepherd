//! Voluntary eviction and rolling update support.
//!
//! A voluntary eviction removes a running pod on purpose (maintenance, a
//! rollout rotating a pod onto a new template). The [`evict_pod`] gate refuses
//! any eviction that would push a matching [`PodDisruptionBudget`] below its
//! floor, so disruption stays inside the declared budget. Rolling updates
//! themselves run through the reconciler: replacing a controller's template
//! starts a roll that rotates old pods onto the new template while honouring
//! the controller's [`RolloutSettings`] and every matching budget.

use crate::cluster::Cluster;
use crate::object::{PodPhase, RolloutSettings};

/// Why a voluntary eviction was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvictionRefused {
    /// No pod with this id exists.
    UnknownPod,
    /// The pod exists but is not running, so there is nothing to disrupt.
    NotRunning,
    /// A disruption budget refuses the eviction: the matched running count
    /// would fall below `floor`.
    BudgetExhausted {
        /// The budget that refused.
        budget: String,
        /// The lowest running count the budget allows.
        floor: u32,
        /// The matched running count at the time of the request.
        running: u32,
    },
}

/// Evict a running pod voluntarily, honouring every disruption budget whose
/// selector matches the pod's labels. On success the pod is removed from the
/// cluster and the owning controller's reconciler replaces it.
///
/// # Errors
/// Returns [`EvictionRefused`] when the pod is unknown, not running, or a
/// matching budget refuses the disruption.
pub fn evict_pod(cluster: &mut Cluster, pod_id: &str) -> Result<(), EvictionRefused> {
    let labels = {
        let pod = cluster.pods.get(pod_id).ok_or(EvictionRefused::UnknownPod)?;
        if pod.phase != PodPhase::Running {
            return Err(EvictionRefused::NotRunning);
        }
        pod.spec.labels.clone()
    };

    for pdb in cluster.pdbs.values() {
        if !crate::object::selector_matches(&pdb.selector, &labels) {
            continue;
        }
        let running = cluster.running_matching(&pdb.selector);
        let floor = cluster.pdb_floor(&labels, &cluster.pods[pod_id].owner);
        if running.saturating_sub(1) < floor {
            return Err(EvictionRefused::BudgetExhausted {
                budget: pdb.name.clone(),
                floor,
                running,
            });
        }
    }

    cluster.pods.remove(pod_id);
    Ok(())
}

/// True when a controller has started but not finished a roll: some of its
/// live pods were stamped from a template other than the current one.
#[must_use]
pub fn rollout_in_progress(cluster: &Cluster, controller: &str) -> bool {
    match cluster.controllers.get(controller) {
        Some(c) => cluster
            .live_pods_of(controller)
            .any(|p| p.spec != c.template),
        None => false,
    }
}

/// Number of live pods of a controller still stamped from the previous
/// template.
#[must_use]
pub fn old_revision_count(cluster: &Cluster, controller: &str) -> usize {
    match cluster.controllers.get(controller) {
        Some(c) => cluster
            .live_pods_of(controller)
            .filter(|p| p.spec != c.template)
            .count(),
        None => 0,
    }
}

/// The running count floor a roll must respect: the desired replica count
/// minus the allowed unavailability, never below zero.
#[must_use]
pub fn availability_floor(desired: u32, settings: RolloutSettings) -> u32 {
    desired.saturating_sub(settings.max_unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Controller, Node, PodDisruptionBudget, PodTemplate};
    use crate::reconciler::reconcile_to_fixed_point;
    use crate::scheduler::ScorePolicy;

    #[test]
    fn eviction_respects_budget() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4096), 0);
        c.add_controller(Controller::deployment(
            "web",
            3,
            PodTemplate::new(100, 100).with_label("app", "web"),
        ));
        reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        c.add_pdb(PodDisruptionBudget::new("web-pdb", 1).with_selector("app", "web"));

        // First eviction is inside the budget (3 running, floor 2).
        let pods: Vec<String> = c
            .pods
            .keys()
            .filter(|k| c.pods[*k].owner == "web")
            .cloned()
            .collect();
        assert!(evict_pod(&mut c, &pods[0]).is_ok());
        assert_eq!(c.running_count("web"), 2);

        // The second would dip below the floor and is refused.
        let pods: Vec<String> = c
            .pods
            .keys()
            .filter(|k| c.pods[*k].owner == "web")
            .cloned()
            .collect();
        let refused = evict_pod(&mut c, &pods[0]);
        assert_eq!(
            refused,
            Err(EvictionRefused::BudgetExhausted {
                budget: "web-pdb".to_string(),
                floor: 2,
                running: 2
            })
        );
        assert_eq!(c.running_count("web"), 2);
    }
}
