//! A deterministic cluster simulator.
//!
//! The simulator wires together the injected clock, the seeded PRNG, the
//! cluster state and a script of timed events. Each [`Simulator::step`] advances
//! the clock by one tick, delivers heartbeats from healthy nodes, applies any
//! events scheduled for that tick, detects newly failed nodes and then drives
//! the reconciler to a fixed point. Same seed plus same script always produces
//! the same history.

use crate::clock::Clock;
use crate::cluster::Cluster;
use crate::object::{Controller, Node, PodDisruptionBudget, PodTemplate};
use crate::reconciler::{reconcile_to_fixed_point, Convergence};
use crate::rng::Rng;
use crate::scheduler::ScorePolicy;

/// A mutation applied to the cluster at a scheduled tick.
#[derive(Clone, Debug)]
pub enum Event {
    AddNode(Node),
    AddController(Controller),
    ScaleController { name: String, replicas: u32 },
    RemoveController(String),
    FailNode(String),
    RecoverNode(String),
    AddPdb(PodDisruptionBudget),
    /// Replace a controller's template, which starts a rolling update that
    /// rotates existing pods onto the new template.
    UpdateTemplate { name: String, template: PodTemplate },
}

/// An event bound to the tick it should fire on.
#[derive(Clone, Debug)]
pub struct ScheduledEvent {
    pub at: u64,
    pub event: Event,
}

/// A deterministic simulation of a cluster over logical time.
pub struct Simulator {
    pub cluster: Cluster,
    pub clock: Clock,
    pub rng: Rng,
    pub policy: ScorePolicy,
    script: Vec<ScheduledEvent>,
    max_passes: usize,
}

impl Simulator {
    /// Create a simulator with a seed, a placement policy and a heartbeat
    /// timeout expressed in ticks.
    #[must_use]
    pub fn new(seed: u64, policy: ScorePolicy, heartbeat_timeout: u64) -> Self {
        Simulator {
            cluster: Cluster::new(heartbeat_timeout),
            clock: Clock::new(0),
            rng: Rng::new(seed),
            policy,
            script: Vec::new(),
            max_passes: 1000,
        }
    }

    /// Queue an event to fire at a given tick.
    pub fn schedule(&mut self, at: u64, event: Event) {
        self.script.push(ScheduledEvent { at, event });
    }

    /// Apply an event immediately against the current clock.
    fn apply(&mut self, event: Event) {
        let now = self.clock.now();
        match event {
            Event::AddNode(node) => self.cluster.add_node(node, now),
            Event::AddController(controller) => self.cluster.add_controller(controller),
            Event::ScaleController { name, replicas } => {
                if let Some(c) = self.cluster.controllers.get_mut(&name) {
                    c.replicas = replicas;
                }
            }
            Event::RemoveController(name) => self.cluster.remove_controller(&name),
            Event::FailNode(id) => self.cluster.fail_node(&id),
            Event::RecoverNode(id) => self.cluster.recover_node(&id),
            Event::AddPdb(pdb) => self.cluster.add_pdb(pdb),
            Event::UpdateTemplate { name, template } => {
                if let Some(c) = self.cluster.controllers.get_mut(&name) {
                    c.template = template;
                }
            }
        }
    }

    /// Advance one tick: deliver heartbeats, fire due events, detect failures,
    /// then reconcile to a fixed point. Returns the convergence outcome and the
    /// ids of any nodes that failed on this tick.
    pub fn step(&mut self) -> (Convergence, Vec<String>) {
        let now = self.clock.advance(1);

        // Fire events due at or before now (a fresh sim starts at tick 1, so
        // events queued for tick 0 still fire on the first step).
        let mut due: Vec<Event> = Vec::new();
        let mut remaining: Vec<ScheduledEvent> = Vec::new();
        for scheduled in self.script.drain(..) {
            if scheduled.at <= now {
                due.push(scheduled.event);
            } else {
                remaining.push(scheduled);
            }
        }
        self.script = remaining;
        for event in due {
            self.apply(event);
        }

        // Healthy, beating nodes report in.
        let beating: Vec<String> = self
            .cluster
            .nodes
            .values()
            .filter(|n| n.beating)
            .map(|n| n.id.clone())
            .collect();
        for id in beating {
            self.cluster.heartbeat(&id, now);
        }

        let failed = self.cluster.detect_failures(now);
        let convergence = reconcile_to_fixed_point(&mut self.cluster, self.policy, self.max_passes);
        (convergence, failed)
    }

    /// Run for a number of ticks, returning the final convergence outcome.
    pub fn run(&mut self, ticks: u64) -> Convergence {
        let mut last = Convergence {
            converged: true,
            passes: 0,
            pending: Vec::new(),
        };
        for _ in 0..ticks {
            let (c, _) = self.step();
            last = c;
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::PodTemplate;
    use crate::reconciler::fully_satisfied;

    fn scenario(seed: u64) -> Simulator {
        let mut sim = Simulator::new(seed, ScorePolicy::BinPack, 3);
        sim.schedule(0, Event::AddNode(Node::new("a", 4000, 4000)));
        sim.schedule(0, Event::AddNode(Node::new("b", 4000, 4000)));
        sim.schedule(0, Event::AddNode(Node::new("c", 4000, 4000)));
        sim.schedule(
            0,
            Event::AddController(Controller::deployment(
                "web",
                6,
                PodTemplate::new(500, 500),
            )),
        );
        sim
    }

    #[test]
    fn recovers_desired_count_after_node_failure() {
        let mut sim = scenario(1);
        sim.run(3);
        assert!(fully_satisfied(&sim.cluster));

        // Kill a node and let heartbeats lapse.
        sim.schedule(4, Event::FailNode("a".to_string()));
        sim.run(10);

        // Desired count restored on the survivors.
        assert!(fully_satisfied(&sim.cluster));
        assert_eq!(sim.cluster.running_count("web"), 6);
        let on_a = sim
            .cluster
            .pods
            .values()
            .filter(|p| p.node.as_deref() == Some("a"))
            .count();
        assert_eq!(on_a, 0);
    }

    #[test]
    fn same_seed_same_placement() {
        let mut a = scenario(99);
        let mut b = scenario(99);
        a.schedule(4, Event::FailNode("b".to_string()));
        b.schedule(4, Event::FailNode("b".to_string()));
        a.run(12);
        b.run(12);

        let placement = |sim: &Simulator| -> Vec<(String, Option<String>)> {
            sim.cluster
                .pods
                .values()
                .map(|p| (p.id.clone(), p.node.clone()))
                .collect()
        };
        assert_eq!(placement(&a), placement(&b));
    }
}
