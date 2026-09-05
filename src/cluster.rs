//! The observed cluster state and the operations that mutate it.
//!
//! A [`Cluster`] holds every node, pod and controller. All collections are
//! `BTreeMap`s so iteration order is deterministic, which the scheduler and
//! reconciler rely on. Pod identifiers are handed out from per controller
//! counters rather than the PRNG so that identity is stable and easy to read.

use std::collections::BTreeMap;

use crate::object::{
    selector_matches, Controller, Node, Pod, PodPhase, PodTemplate, Resources, Selector,
};

/// The full state of the world: what nodes exist, what pods run where, and what
/// the controllers want.
#[derive(Clone, Debug)]
pub struct Cluster {
    pub nodes: BTreeMap<String, Node>,
    pub pods: BTreeMap<String, Pod>,
    pub controllers: BTreeMap<String, Controller>,
    /// A node is declared failed once this many ticks pass with no heartbeat.
    pub heartbeat_timeout: u64,
    /// Per controller monotonic counter used to mint stable pod identifiers.
    seq: BTreeMap<String, u64>,
}

impl Cluster {
    /// Create an empty cluster with the given heartbeat timeout in ticks.
    pub fn new(heartbeat_timeout: u64) -> Self {
        Cluster {
            nodes: BTreeMap::new(),
            pods: BTreeMap::new(),
            controllers: BTreeMap::new(),
            heartbeat_timeout,
            seq: BTreeMap::new(),
        }
    }

    /// Register a node. Its heartbeat is initialised to `now`.
    pub fn add_node(&mut self, mut node: Node, now: u64) {
        node.last_heartbeat = now;
        node.healthy = true;
        node.beating = true;
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
    pub fn allocated(&self, node_id: &str) -> Resources {
        self.pods
            .values()
            .filter(|p| p.phase == PodPhase::Running && p.node.as_deref() == Some(node_id))
            .fold(Resources::default(), |acc, p| acc + p.spec.requests)
    }

    /// Resources still free on a node (capacity minus allocated).
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
    pub fn node_hosts_match(&self, node_id: &str, selector: &Selector) -> bool {
        self.running_pods_on(node_id)
            .any(|p| selector_matches(selector, &p.spec.labels))
    }

    /// Record a heartbeat from a node, restoring health if it had been lost.
    pub fn heartbeat(&mut self, node_id: &str, now: u64) {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.last_heartbeat = now;
            node.healthy = true;
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

    /// Count of running pods owned by a controller.
    pub fn running_count(&self, owner: &str) -> u32 {
        self.pods
            .values()
            .filter(|p| p.owner == owner && p.phase == PodPhase::Running)
            .count() as u32
    }

    /// Invariant check: no node has more committed than its capacity.
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
}
