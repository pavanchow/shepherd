//! The scheduler: filter feasible nodes, score them, and bind.
//!
//! Scheduling one pod is a two stage pipeline. First [`feasible`] rejects any
//! node that would violate a hard constraint (health, resource fit, node
//! selector, taints, affinity). Then [`score`] ranks the survivors under a
//! placement policy and the highest score wins, with a deterministic tie break
//! on node id. Because the filter proves resource fit before binding, a bind
//! can never overcommit a node.

use crate::cluster::Cluster;
use crate::object::{selector_matches, AffinityTerm, Node, Pod, PodPhase};

/// How to rank feasible nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScorePolicy {
    /// Prefer the fullest node that still fits, packing pods tightly so whole
    /// nodes can later be drained.
    BinPack,
    /// Prefer the emptiest node, spreading load evenly.
    LeastLoaded,
}

/// Why a node was rejected for a pod. Used for human readable reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfeasibleReason {
    NodeUnhealthy,
    InsufficientResources,
    NodeSelectorMismatch,
    UntoleratedTaint,
    AffinityUnsatisfied,
    AntiAffinityConflict,
}

/// Evaluate every hard constraint for placing `pod` on `node`.
///
/// # Errors
/// Returns the first reason the node is not feasible for the pod.
pub fn feasibility(cluster: &Cluster, node: &Node, pod: &Pod) -> Result<(), InfeasibleReason> {
    if !node.healthy {
        return Err(InfeasibleReason::NodeUnhealthy);
    }

    if !pod.spec.requests.fits_within(cluster.free(&node.id)) {
        return Err(InfeasibleReason::InsufficientResources);
    }

    if !selector_matches(&pod.spec.node_selector, &node.labels) {
        return Err(InfeasibleReason::NodeSelectorMismatch);
    }

    for taint in &node.taints {
        let tolerated = pod.spec.tolerations.iter().any(|t| t.tolerates(taint));
        if !tolerated {
            return Err(InfeasibleReason::UntoleratedTaint);
        }
    }

    for term in &pod.spec.affinity {
        match term {
            AffinityTerm::Affinity(sel) => {
                if !cluster.node_hosts_match(&node.id, sel) {
                    return Err(InfeasibleReason::AffinityUnsatisfied);
                }
            }
            AffinityTerm::AntiAffinity(sel) => {
                if cluster.node_hosts_match(&node.id, sel) {
                    return Err(InfeasibleReason::AntiAffinityConflict);
                }
            }
        }
    }

    Ok(())
}

/// True when the node is a feasible target for the pod.
#[must_use]
pub fn feasible(cluster: &Cluster, node: &Node, pod: &Pod) -> bool {
    feasibility(cluster, node, pod).is_ok()
}

/// Score a feasible node under a policy. Higher is better.
fn score(policy: ScorePolicy, cluster: &Cluster, node: &Node, pod: &Pod) -> i64 {
    let free = cluster.free(&node.id);
    let remaining = free.saturating_sub(pod.spec.requests);
    // Saturating conversion: capacities beyond the i64 range must clamp rather
    // than wrap, or scores become incomparable and tie breaks break down.
    let cpu = i64::try_from(remaining.cpu).unwrap_or(i64::MAX);
    let mem = i64::try_from(remaining.mem).unwrap_or(i64::MAX);
    let slack = cpu.saturating_add(mem);
    match policy {
        // Least leftover wins, so negate.
        ScorePolicy::BinPack => -slack,
        // Most leftover wins.
        ScorePolicy::LeastLoaded => slack,
    }
}

/// Pick the best feasible node for a pod under a policy, or `None` if the pod
/// is unschedulable right now. Ties are broken by lowest node id because nodes
/// are visited in sorted order and a strictly greater score is required to
/// replace the incumbent.
#[must_use]
pub fn choose_node(cluster: &Cluster, pod: &Pod, policy: ScorePolicy) -> Option<String> {
    let mut best: Option<(i64, String)> = None;
    for node in cluster.nodes.values() {
        if !feasible(cluster, node, pod) {
            continue;
        }
        let s = score(policy, cluster, node, pod);
        match &best {
            Some((best_score, _)) if *best_score >= s => {}
            _ => best = Some((s, node.id.clone())),
        }
    }
    best.map(|(_, id)| id)
}

/// Attempt to bind a single pending pod. On success the pod becomes `Running`
/// on the chosen node and `true` is returned. If no node is feasible the pod
/// stays `Pending` and `false` is returned.
///
/// # Panics
/// Panics if the pod disappeared from the cluster between the lookup and the
/// bind, which cannot happen through public APIs.
pub fn schedule_pod(cluster: &mut Cluster, pod_id: &str, policy: ScorePolicy) -> bool {
    let pod = match cluster.pods.get(pod_id) {
        Some(p) if p.phase == PodPhase::Pending => p.clone(),
        _ => return false,
    };
    match choose_node(cluster, &pod, policy) {
        Some(node_id) => {
            let pod = cluster.pods.get_mut(pod_id).expect("pod exists");
            pod.node = Some(node_id);
            pod.phase = PodPhase::Running;
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Node, PodTemplate, Taint, Toleration};

    #[test]
    fn resource_fit_filters() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("small", 500, 512), 0);
        c.add_node(Node::new("big", 4000, 8192), 0);
        let id = c.create_pod("web", PodTemplate::new(1000, 2048));
        let node = choose_node(&c, c.pods.get(&id).unwrap(), ScorePolicy::BinPack);
        assert_eq!(node, Some("big".to_string()));
    }

    #[test]
    fn bin_pack_prefers_fuller_node() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000), 0);
        c.add_node(Node::new("b", 4000, 4000), 0);
        // Pre load node a so it is fuller but still fits.
        let seed = c.create_pod("bg", PodTemplate::new(3000, 3000));
        assert!(schedule_pod(&mut c, &seed, ScorePolicy::BinPack));
        assert_eq!(c.pods.get(&seed).unwrap().node.as_deref(), Some("a"));

        let id = c.create_pod("web", PodTemplate::new(500, 500));
        assert!(schedule_pod(&mut c, &id, ScorePolicy::BinPack));
        // Bin pack tops off node a rather than spreading to b.
        assert_eq!(c.pods.get(&id).unwrap().node.as_deref(), Some("a"));
        assert!(c.verify_capacity().is_ok());
    }

    #[test]
    fn least_loaded_spreads() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000), 0);
        c.add_node(Node::new("b", 4000, 4000), 0);
        let seed = c.create_pod("bg", PodTemplate::new(3000, 3000));
        assert!(schedule_pod(&mut c, &seed, ScorePolicy::LeastLoaded));
        assert_eq!(c.pods.get(&seed).unwrap().node.as_deref(), Some("a"));

        let id = c.create_pod("web", PodTemplate::new(500, 500));
        assert!(schedule_pod(&mut c, &id, ScorePolicy::LeastLoaded));
        // Least loaded avoids the busy node.
        assert_eq!(c.pods.get(&id).unwrap().node.as_deref(), Some("b"));
    }

    #[test]
    fn taints_require_toleration() {
        let mut c = Cluster::new(3);
        c.add_node(
            Node::new("gpu", 4000, 4000).with_taint(Taint::no_schedule("gpu", "true")),
            0,
        );
        let plain = c.create_pod("web", PodTemplate::new(100, 100));
        assert!(!schedule_pod(&mut c, &plain, ScorePolicy::BinPack));

        let tolerant = c.create_pod(
            "ml",
            PodTemplate::new(100, 100).with_toleration(Toleration::exact("gpu", "true")),
        );
        assert!(schedule_pod(&mut c, &tolerant, ScorePolicy::BinPack));
    }

    #[test]
    fn huge_capacities_do_not_break_scoring() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("huge", u64::MAX / 2, u64::MAX / 2), 0);
        c.add_node(Node::new("big", 4000, 4000), 0);
        let id = c.create_pod("web", PodTemplate::new(1, 1));
        // Scores clamp instead of overflowing on absurd capacities.
        assert!(schedule_pod(&mut c, &id, ScorePolicy::BinPack));
        assert!(c.verify_capacity().is_ok());
    }

    #[test]
    fn anti_affinity_spreads_one_per_node() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000), 0);
        c.add_node(Node::new("b", 4000, 4000), 0);
        let tmpl = PodTemplate::new(100, 100)
            .with_label("app", "web")
            .with_affinity(AffinityTerm::AntiAffinity({
                let mut s = crate::object::Selector::new();
                s.insert("app".to_string(), "web".to_string());
                s
            }));
        let p1 = c.create_pod("web", tmpl.clone());
        let p2 = c.create_pod("web", tmpl.clone());
        let p3 = c.create_pod("web", tmpl);
        assert!(schedule_pod(&mut c, &p1, ScorePolicy::BinPack));
        assert!(schedule_pod(&mut c, &p2, ScorePolicy::BinPack));
        // Only two nodes, so the third cannot be placed without co-locating.
        assert!(!schedule_pod(&mut c, &p3, ScorePolicy::BinPack));
        let n1 = c.pods.get(&p1).unwrap().node.clone();
        let n2 = c.pods.get(&p2).unwrap().node.clone();
        assert_ne!(n1, n2);
    }
}
