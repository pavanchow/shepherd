//! The observed cluster state and the operations that mutate it.
//!
//! A [`Cluster`] holds every node, pod and controller. All collections are
//! `BTreeMap`s so iteration order is deterministic, which the scheduler and
//! reconciler rely on. Pod identifiers are handed out from per controller
//! counters rather than the PRNG so that identity is stable and easy to read.

use std::collections::BTreeMap;

use crate::object::{
    selector_matches, Controller, Labels, Node, Pod, PodDisruptionBudget, PodPhase, PodTemplate,
    Resources, Selector,
};

/// The full state of the world: what nodes exist, what pods run where, and what
/// the controllers want.
#[derive(Clone, Debug)]
pub struct Cluster {
    pub nodes: BTreeMap<String, Node>,
    pub pods: BTreeMap<String, Pod>,
    pub controllers: BTreeMap<String, Controller>,
    /// Disruption budgets limiting voluntary evictions.
    pub pdbs: BTreeMap<String, PodDisruptionBudget>,
    /// A node is declared failed once this many ticks pass with no heartbeat.
    pub heartbeat_timeout: u64,
    /// Per controller monotonic counter used to mint stable pod identifiers.
    seq: BTreeMap<String, u64>,
}

impl Cluster {
    /// Create an empty cluster with the given heartbeat timeout in ticks.
    #[must_use]
    pub fn new(heartbeat_timeout: u64) -> Self {
        Cluster {
            nodes: BTreeMap::new(),
            pods: BTreeMap::new(),
            controllers: BTreeMap::new(),
            pdbs: BTreeMap::new(),
            heartbeat_timeout,
            seq: BTreeMap::new(),
        }
    }

    /// Register a node. Its heartbeat is initialised to `now`.
    ///
    /// Re-registering an id that already exists replaces the node object.
    /// Every running pod bound to that id is released back to `Pending` so the
    /// reconciler revalidates it against the new capacity, labels and taints.
    /// Without this a re-registration could silently leave a node
    /// overcommitted or host pods that violate their constraints.
    pub fn add_node(&mut self, mut node: Node, now: u64) {
        node.last_heartbeat = now;
        node.healthy = true;
        node.beating = true;
        if self.nodes.contains_key(&node.id) {
            for pod in self.pods.values_mut() {
                if pod.node.as_deref() == Some(node.id.as_str()) && pod.phase == PodPhase::Running {
                    pod.node = None;
                    pod.phase = PodPhase::Pending;
                }
            }
        }
        self.nodes.insert(node.id.clone(), node);
    }

    /// Register or replace a controller (declaring desired state).
    pub fn add_controller(&mut self, controller: Controller) {
        self.seq.entry(controller.name.clone()).or_insert(0);
        self.controllers.insert(controller.name.clone(), controller);
    }

    /// Remove a controller and all of its pods.
    pub fn remove_controller(&mut self, name: &str) {
        self.controllers.remove(name);
        self.pods.retain(|_, p| p.owner != name);
    }

    /// Register or replace a disruption budget.
    pub fn add_pdb(&mut self, pdb: PodDisruptionBudget) {
        self.pdbs.insert(pdb.name.clone(), pdb);
    }

    /// Mint the next stable pod id for a controller.
    pub fn next_pod_id(&mut self, owner: &str) -> String {
        let counter = self.seq.entry(owner.to_string()).or_insert(0);
        let id = format!("{owner}-{counter}");
        *counter += 1;
        id
    }

    /// Create and register a fresh pending pod for a controller.
    pub fn create_pod(&mut self, owner: &str, template: PodTemplate) -> String {
        let id = self.next_pod_id(owner);
        let pod = Pod::pending(id.clone(), owner.to_string(), template);
        self.pods.insert(id.clone(), pod);
        id
    }

    /// Resources currently committed on a node by its running pods.
    #[must_use]
    pub fn allocated(&self, node_id: &str) -> Resources {
        self.pods
            .values()
            .filter(|p| p.phase == PodPhase::Running && p.node.as_deref() == Some(node_id))
            .fold(Resources::default(), |acc, p| acc + p.spec.requests)
    }

    /// Resources still free on a node (capacity minus allocated).
    #[must_use]
    pub fn free(&self, node_id: &str) -> Resources {
        match self.nodes.get(node_id) {
            Some(node) => node.capacity.saturating_sub(self.allocated(node_id)),
            None => Resources::default(),
        }
    }

    /// Running pods bound to a node.
    pub fn running_pods_on<'a>(&'a self, node_id: &'a str) -> impl Iterator<Item = &'a Pod> + 'a {
        self.pods
            .values()
            .filter(move |p| p.phase == PodPhase::Running && p.node.as_deref() == Some(node_id))
    }

    /// True when a node hosts at least one running pod matching the selector.
    #[must_use]
    pub fn node_hosts_match(&self, node_id: &str, selector: &Selector) -> bool {
        self.running_pods_on(node_id)
            .any(|p| selector_matches(selector, &p.spec.labels))
    }

    /// Record a heartbeat from a node, restoring health if it had been lost.
    /// Heartbeats from a node that has stopped beating are ignored: only the
    /// node itself can prove liveness, so an out of band heartbeat cannot
    /// revive a corpse and hand the scheduler a dead target.
    pub fn heartbeat(&mut self, node_id: &str, now: u64) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            if node.beating {
                node.last_heartbeat = now;
                node.healthy = true;
            }
        }
    }

    /// Stop a node from beating (simulate a crash). Health is downgraded once
    /// the timeout elapses in [`Cluster::detect_failures`].
    pub fn fail_node(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.beating = false;
        }
    }

    /// Let a previously failed node beat again. It regains health on its next
    /// heartbeat.
    pub fn recover_node(&mut self, node_id: &str) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.beating = true;
        }
    }

    /// Downgrade nodes whose heartbeat is stale and mark their pods `Lost`.
    /// Returns the ids of nodes newly declared failed.
    pub fn detect_failures(&mut self, now: u64) -> Vec<String> {
        let mut failed = Vec::new();
        for node in self.nodes.values_mut() {
            if node.healthy && now.saturating_sub(node.last_heartbeat) > self.heartbeat_timeout {
                node.healthy = false;
                failed.push(node.id.clone());
            }
        }
        for id in &failed {
            for pod in self.pods.values_mut() {
                if pod.node.as_deref() == Some(id.as_str()) && pod.phase == PodPhase::Running {
                    pod.phase = PodPhase::Lost;
                }
            }
        }
        failed
    }

    /// Remove every pod in the `Lost` phase. Returns how many were collected.
    pub fn gc_lost(&mut self) -> usize {
        let before = self.pods.len();
        self.pods.retain(|_, p| p.phase != PodPhase::Lost);
        before - self.pods.len()
    }

    /// Pods owned by a controller that still count toward desired state
    /// (anything not `Lost`).
    pub fn live_pods_of<'a>(&'a self, owner: &'a str) -> impl Iterator<Item = &'a Pod> + 'a {
        self.pods
            .values()
            .filter(move |p| p.owner == owner && p.phase != PodPhase::Lost)
    }

    /// Count of running pods whose labels match the selector.
    #[must_use]
    pub fn running_matching(&self, selector: &Selector) -> u32 {
        let count = self
            .pods
            .values()
            .filter(|p| p.phase == PodPhase::Running && selector_matches(selector, &p.spec.labels))
            .count();
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    /// The lowest running count every disruption budget matching `labels`
    /// allows. The baseline for each budget is the owning controller's
    /// replica count when the owner exists, otherwise the current matched
    /// running count. A voluntary eviction that would dip the matched running
    /// count below this floor is refused.
    #[must_use]
    pub fn pdb_floor(&self, labels: &Labels, owner: &str) -> u32 {
        let mut floor = 0u32;
        for pdb in self.pdbs.values() {
            if !selector_matches(&pdb.selector, labels) {
                continue;
            }
            let baseline = self
                .controllers
                .get(owner)
                .map_or_else(|| self.running_matching(&pdb.selector), |c| c.replicas);
            floor = floor.max(baseline.saturating_sub(pdb.max_unavailable));
        }
        floor
    }

    /// Count of running pods owned by a controller.
    #[must_use]
    pub fn running_count(&self, owner: &str) -> u32 {
        let count = self
            .pods
            .values()
            .filter(|p| p.owner == owner && p.phase == PodPhase::Running)
            .count();
        // A pod count overflowing u32 would need four billion pods in one
        // map, but clamp rather than truncate if it ever happens.
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    /// Invariant check: no node has more committed than its capacity.
    ///
    /// # Errors
    /// Returns `Err` describing the first overcommitted node found.
    pub fn verify_capacity(&self) -> Result<(), String> {
        for node in self.nodes.values() {
            let used = self.allocated(&node.id);
            if !used.fits_within(node.capacity) {
                return Err(format!(
                    "node {} overcommitted: used cpu={} mem={} capacity cpu={} mem={}",
                    node.id, used.cpu, used.mem, node.capacity.cpu, node.capacity.mem
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::PodPhase;

    #[test]
    fn allocation_tracks_running_pods() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("n1", 4000, 8192), 0);
        let id = c.create_pod("web", PodTemplate::new(1000, 2048));
        // Pending pods do not consume capacity.
        assert_eq!(c.allocated("n1"), Resources::new(0, 0));

        let pod = c.pods.get_mut(&id).unwrap();
        pod.node = Some("n1".to_string());
        pod.phase = PodPhase::Running;
        assert_eq!(c.allocated("n1"), Resources::new(1000, 2048));
        assert_eq!(c.free("n1"), Resources::new(3000, 6144));
    }

    #[test]
    fn failure_marks_pods_lost() {
        let mut c = Cluster::new(2);
        c.add_node(Node::new("n1", 4000, 8192), 0);
        let id = c.create_pod("web", PodTemplate::new(1000, 2048));
        {
            let pod = c.pods.get_mut(&id).unwrap();
            pod.node = Some("n1".to_string());
            pod.phase = PodPhase::Running;
        }
        c.fail_node("n1");
        // Not yet past the timeout.
        assert!(c.detect_failures(2).is_empty());
        // Past the timeout: node fails, pod becomes Lost.
        let failed = c.detect_failures(5);
        assert_eq!(failed, vec!["n1".to_string()]);
        assert_eq!(c.pods.get(&id).unwrap().phase, PodPhase::Lost);
        assert_eq!(c.gc_lost(), 1);
        assert!(c.pods.is_empty());
    }

    #[test]
    fn stable_pod_ids() {
        let mut c = Cluster::new(3);
        c.add_controller(Controller::deployment("web", 0, PodTemplate::new(100, 100)));
        assert_eq!(c.next_pod_id("web"), "web-0");
        assert_eq!(c.next_pod_id("web"), "web-1");
        assert_eq!(c.next_pod_id("web"), "web-2");
    }

    #[test]
    fn re_registration_releases_stale_bindings() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("n1", 4000, 8192), 0);
        c.add_node(Node::new("n2", 4000, 8192), 0);
        let id = c.create_pod("web", PodTemplate::new(1000, 1024));
        {
            let pod = c.pods.get_mut(&id).unwrap();
            pod.node = Some("n1".to_string());
            pod.phase = PodPhase::Running;
        }
        // Shrink the re-registered node: the bound pod must be released rather
        // than left overcommitted against the new capacity.
        c.add_node(Node::new("n1", 500, 512), 1);
        assert!(c.verify_capacity().is_ok());
        assert_eq!(c.pods.get(&id).unwrap().phase, PodPhase::Pending);
        // The reconciler relocates it onto the healthy node.
        assert!(crate::scheduler::schedule_pod(&mut c, &id, crate::scheduler::ScorePolicy::BinPack));
        assert_eq!(c.pods.get(&id).unwrap().node.as_deref(), Some("n2"));
        assert!(c.verify_capacity().is_ok());
    }

    #[test]
    fn heartbeat_ignores_non_beating_node() {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("n1", 4000, 8192), 0);
        c.fail_node("n1");
        assert_eq!(c.detect_failures(10), vec!["n1".to_string()]);
        // An out of band heartbeat cannot revive a node that stopped beating.
        c.heartbeat("n1", 10);
        assert!(!c.nodes["n1"].healthy);
        // Recovery restarts the heartbeat, which restores health.
        c.recover_node("n1");
        c.heartbeat("n1", 11);
        assert!(c.nodes["n1"].healthy);
    }
}
