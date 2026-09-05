//! The reconciliation control loop.
//!
//! Reconciliation is the beating heart of Shepherd. Each pass compares desired
//! state (controller replica counts) with observed state (live pods) and takes
//! the smallest step toward agreement: create missing pods, delete surplus
//! pods, garbage collect lost pods, then schedule everything pending. When a
//! controller's template changes while its pods exist, the pass instead drives
//! a rolling update: it rotates pods stamped from the previous template onto
//! the new one inside the disruption bounds of the controller's
//! [`RolloutSettings`] and every matching [`PodDisruptionBudget`]. A pass
//! reports whether it changed anything. Driving the loop until a pass reports
//! no change reaches a fixed point, which is exactly convergence.

use crate::cluster::Cluster;
use crate::object::{PodPhase, RolloutSettings};
use crate::rollout::{availability_floor, evict_pod, EvictionRefused};
use crate::scheduler::{schedule_pod, ScorePolicy};

/// What a single reconciliation pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    pub collected: usize,
    pub created: usize,
    pub deleted: usize,
    pub scheduled: usize,
    /// Old revision running pods evicted by a rolling update through the
    /// budget honouring eviction path.
    pub rotated: usize,
    /// Pod ids that remain pending because no node is feasible.
    pub pending: Vec<String>,
}

impl PassReport {
    /// True when the pass mutated the cluster in any way. When a pass reports
    /// no change the loop has reached a fixed point.
    #[must_use]
    pub fn changed(&self) -> bool {
        self.collected > 0
            || self.created > 0
            || self.deleted > 0
            || self.scheduled > 0
            || self.rotated > 0
    }
}

/// Run exactly one reconciliation pass.
pub fn reconcile_once(cluster: &mut Cluster, policy: ScorePolicy) -> PassReport {
    // 1. Garbage collect pods whose node has failed.
    let collected = cluster.gc_lost();
    let mut created = 0usize;
    let mut deleted = 0usize;
    let mut rotated = 0usize;

    // 2. Drive each controller toward its desired replica count. Controllers
    //    are visited in sorted order for determinism.
    let names: Vec<String> = cluster.controllers.keys().cloned().collect();
    for name in names {
        let (desired, template, rollout) = {
            let c = &cluster.controllers[&name];
            (c.replicas, c.template.clone(), c.rollout)
        };

        // Partition live pods by revision against the current template.
        let mut old_pending: Vec<String> = Vec::new();
        let mut old_running: Vec<String> = Vec::new();
        for p in cluster.live_pods_of(&name) {
            if p.spec != template {
                if p.phase == PodPhase::Pending {
                    old_pending.push(p.id.clone());
                } else {
                    old_running.push(p.id.clone());
                }
            }
        }

        if old_pending.is_empty() && old_running.is_empty() {
            // Steady state: plain replica count reconciliation.
            let observed = cluster.live_pods_of(&name).count();
            let observed = u32::try_from(observed).unwrap_or(u32::MAX);

            if observed < desired {
                for _ in 0..(desired - observed) {
                    cluster.create_pod(&name, template.clone());
                    created += 1;
                }
            } else if observed > desired {
                // Delete surplus, preferring pending pods first, then the
                // highest ids, so deletion is deterministic and cheap.
                let mut victims: Vec<(bool, String)> = cluster
                    .live_pods_of(&name)
                    .map(|p| (p.phase == PodPhase::Running, p.id.clone()))
                    .collect();
                // Pending (false) sort before running (true); within a group
                // the higher id is removed first.
                victims.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
                for (_, id) in victims.into_iter().take((observed - desired) as usize) {
                    cluster.pods.remove(&id);
                    deleted += 1;
                }
            }
        } else {
            // Rolling update: rotate old revision pods onto the new template
            // without dipping below the availability floor.
            let (c, d, r) = roll_forward(
                cluster,
                &name,
                &template,
                desired,
                rollout,
                &old_pending,
                old_running,
            );
            created += c;
            deleted += d;
            rotated += r;
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
        rotated,
        pending,
    }
}

/// One rollout step for a controller whose live pods include old revisions.
/// Returns the number of pods created, deleted and rotated.
///
/// The step (1) removes old revision pending pods, which never counted as
/// available, (2) drives the live total toward the surge target with new
/// revision pods, preferring old revision running pods when a mid roll scale
/// down leaves surplus, and (3) evicts old revision running pods through
/// [`evict_pod`] while the running count has headroom above the availability
/// floor, so a budget refusal pauses the rotation instead of breaching it.
fn roll_forward(
    cluster: &mut Cluster,
    name: &str,
    template: &crate::object::PodTemplate,
    desired: u32,
    rollout: RolloutSettings,
    old_pending: &[String],
    mut old_running: Vec<String>,
) -> (usize, usize, usize) {
    let mut created = 0usize;
    let mut deleted = 0usize;
    let mut rotated = 0usize;

    // 1. Old revision pending pods are dead weight, they hold no availability.
    for id in old_pending {
        cluster.pods.remove(id);
        deleted += 1;
    }

    // 2. Drive the live total toward the surge target with new revision pods.
    let surge_target = desired.saturating_add(rollout.max_surge);
    let observed = cluster.live_pods_of(name).count();
    let observed = u32::try_from(observed).unwrap_or(u32::MAX);
    if observed < surge_target {
        for _ in 0..(surge_target - observed) {
            cluster.create_pod(name, template.clone());
            created += 1;
        }
    } else if observed > surge_target {
        // Mid roll scale down: surplus is removed preferring old revision
        // pods first, then pending, then the highest ids.
        let mut victims: Vec<(bool, bool, String)> = cluster
            .live_pods_of(name)
            .map(|p| (p.spec == *template, p.phase == PodPhase::Running, p.id.clone()))
            .collect();
        // Old revision (false) sorts first, pending (false) before running.
        victims.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(b.2.cmp(&a.2))
        });
        for (_, _, id) in victims.into_iter().take((observed - surge_target) as usize) {
            cluster.pods.remove(&id);
            deleted += 1;
        }
    }

    // 3. Rotate: evict old revision running pods while the running count has
    // headroom above the availability floor. `evict_pod` enforces every
    // matching disruption budget, so a refusal pauses the rotation.
    let floor = availability_floor(desired, rollout);
    old_running.sort_by(|a, b| b.cmp(a)); // highest id first, deterministic
    for id in &old_running {
        let running = cluster.running_count(name);
        if running <= floor {
            break;
        }
        match evict_pod(cluster, id) {
            Ok(()) => rotated += 1,
            // Budget refusal: state did not change, retrying would refuse
            // again, so pause the rotation for this pass.
            Err(EvictionRefused::BudgetExhausted { .. }) => break,
            // Stale id (already removed by the surplus step above): skip it.
            Err(_) => continue,
        }
    }

    (created, deleted, rotated)
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
        let unchanged = !report.changed();
        last_pending = report.pending;
        if unchanged {
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
#[must_use]
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
