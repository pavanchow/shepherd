//! The reconciliation control loop.
//!
//! Reconciliation is the beating heart of Shepherd. Each pass compares desired
//! state (controller replica counts) with observed state (live pods) and takes
//! the smallest step toward agreement: create missing pods, delete surplus
//! pods, garbage collect lost pods, then schedule everything pending. A pass
//! reports whether it changed anything. Driving the loop until a pass reports
//! no change reaches a fixed point, which is exactly convergence.

use crate::cluster::Cluster;
use crate::object::PodPhase;
use crate::scheduler::{schedule_pod, ScorePolicy};

/// What a single reconciliation pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    pub collected: usize,
    pub created: usize,
    pub deleted: usize,
    pub scheduled: usize,
    /// Pod ids that remain pending because no node is feasible.
    pub pending: Vec<String>,
}

impl PassReport {
    /// True when the pass mutated the cluster in any way. When a pass reports
    /// no change the loop has reached a fixed point.
    pub fn changed(&self) -> bool {
        self.collected > 0 || self.created > 0 || self.deleted > 0 || self.scheduled > 0
    }
}

/// Run exactly one reconciliation pass.
pub fn reconcile_once(cluster: &mut Cluster, policy: ScorePolicy) -> PassReport {
    // 1. Garbage collect pods whose node has failed.
    let collected = cluster.gc_lost();
    let mut created = 0usize;
    let mut deleted = 0usize;

    // 2. Drive each controller toward its desired replica count. Controllers
    //    are visited in sorted order for determinism.
    let names: Vec<String> = cluster.controllers.keys().cloned().collect();
    for name in names {
        let (desired, template) = {
            let c = &cluster.controllers[&name];
            (c.replicas, c.template.clone())
        };
        let observed = cluster.live_pods_of(&name).count() as u32;

        if observed < desired {
            for _ in 0..(desired - observed) {
                cluster.create_pod(&name, template.clone());
                created += 1;
            }
        } else if observed > desired {
            // Delete surplus, preferring pending pods first, then the highest
            // ids, so deletion is deterministic and cheap.
            let mut victims: Vec<(bool, String)> = cluster
                .live_pods_of(&name)
                .map(|p| (p.phase == PodPhase::Running, p.id.clone()))
                .collect();
            // Pending (false) sort before running (true); within a group the
            // higher id is removed first.
            victims.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            for (_, id) in victims.into_iter().take((observed - desired) as usize) {
                cluster.pods.remove(&id);
                deleted += 1;
            }
        }
    }

    // 3. Schedule every pending pod. Sorted order keeps placement stable.
    let queue: Vec<String> = cluster
        .pods
        .values()
        .filter(|p| p.phase == PodPhase::Pending)
        .map(|p| p.id.clone())
        .collect();
    let mut scheduled = 0usize;
    let mut pending = Vec::new();
    for id in queue {
        if schedule_pod(cluster, &id, policy) {
            scheduled += 1;
        } else {
            pending.push(id);
        }
    }

    PassReport {
        collected,
        created,
        deleted,
        scheduled,
        pending,
    }
}

/// Outcome of driving the loop to a fixed point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Convergence {
    /// True when a pass reported no change within the iteration budget.
    pub converged: bool,
    /// Number of passes executed.
    pub passes: usize,
    /// Pod ids still pending at the fixed point (genuinely unschedulable).
    pub pending: Vec<String>,
}

/// Reconcile repeatedly until a pass makes no change or the budget is spent.
pub fn reconcile_to_fixed_point(
    cluster: &mut Cluster,
    policy: ScorePolicy,
    max_passes: usize,
) -> Convergence {
    let mut last_pending = Vec::new();
    for pass in 1..=max_passes {
        let report = reconcile_once(cluster, policy);
        last_pending = report.pending.clone();
        if !report.changed() {
            return Convergence {
                converged: true,
                passes: pass,
                pending: last_pending,
            };
        }
    }
    Convergence {
        converged: false,
        passes: max_passes,
        pending: last_pending,
    }
}

/// True when every controller has exactly its desired number of running pods.
pub fn fully_satisfied(cluster: &Cluster) -> bool {
    cluster
        .controllers
        .values()
        .all(|c| cluster.running_count(&c.name) == c.replicas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Controller, Node, PodTemplate};

    #[test]
    fn converges_and_is_idempotent() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000), 0);
        c.add_node(Node::new("b", 4000, 4000), 0);
        c.add_controller(Controller::deployment(
            "web",
            5,
            PodTemplate::new(500, 500),
        ));

        let result = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert!(result.converged);
        assert!(fully_satisfied(&c));
        assert!(c.verify_capacity().is_ok());

        // A second run at the fixed point must do nothing.
        let again = reconcile_once(&mut c, ScorePolicy::BinPack);
        assert!(!again.changed());
    }

    #[test]
    fn scale_up_then_down_converges() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 8000, 8000), 0);
        c.add_controller(Controller::deployment(
            "web",
            2,
            PodTemplate::new(500, 500),
        ));
        reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert_eq!(c.running_count("web"), 2);

        c.controllers.get_mut("web").unwrap().replicas = 6;
        reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert_eq!(c.running_count("web"), 6);

        c.controllers.get_mut("web").unwrap().replicas = 1;
        reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert_eq!(c.running_count("web"), 1);
        assert!(c.verify_capacity().is_ok());
    }

    #[test]
    fn reports_insufficient_capacity() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("tiny", 1000, 1000), 0);
        c.add_controller(Controller::deployment(
            "web",
            5,
            PodTemplate::new(600, 600),
        ));
        let result = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        // The loop still reaches a stable fixed point.
        assert!(result.converged);
        // Only one replica fits; the rest are correctly reported pending.
        assert_eq!(c.running_count("web"), 1);
        assert_eq!(result.pending.len(), 4);
        assert!(!fully_satisfied(&c));
    }
}
