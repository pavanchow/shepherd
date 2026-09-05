//! The correctness gate.
//!
//! Four randomized, bounded property tests plus determinism checks. The bound
//! is controlled by the `SHEPHERD_FUZZ_OPS` environment variable so CI runs a
//! quick sweep while a developer can crank it up locally.
//!
//! Each gate re checks its property from scratch with the independent verifiers
//! in `shepherd::verify` and `Cluster::verify_capacity`, so a bug in the
//! scheduler cannot hide behind the code that produced the state.

use shepherd::cluster::Cluster;
use shepherd::object::{AffinityTerm, Controller, Node, PodTemplate, Selector, Taint, Toleration};
use shepherd::reconciler::{fully_satisfied, reconcile_once, reconcile_to_fixed_point};
use shepherd::rng::Rng;
use shepherd::scheduler::{choose_node, schedule_pod, ScorePolicy};
use shepherd::simulator::{Event, Simulator};
use shepherd::verify::verify_constraints;

fn ops() -> u64 {
    std::env::var("SHEPHERD_FUZZ_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

/// Build a random cluster with plain (constraint free) apps.
fn random_plain_cluster(seed: u64) -> Cluster {
    let mut rng = Rng::new(seed);
    let mut cluster = Cluster::new(3);
    let nodes = rng.range(2, 6);
    for i in 0..nodes {
        let cpu = rng.range(2, 8) * 1000;
        let mem = rng.range(2, 8) * 1024;
        cluster.add_node(Node::new(&format!("n{i}"), cpu, mem), 0);
    }
    let apps = rng.range(1, 5);
    for i in 0..apps {
        let replicas = rng.range(0, 8) as u32;
        let cpu = rng.range(1, 4) * 500;
        let mem = rng.range(1, 4) * 512;
        cluster.add_controller(Controller::deployment(
            &format!("app{i}"),
            replicas,
            PodTemplate::new(cpu, mem),
        ));
    }
    cluster
}

/// Build a random cluster exercising every constraint kind.
fn random_constrained_cluster(seed: u64) -> Cluster {
    let mut rng = Rng::new(seed);
    let mut cluster = Cluster::new(3);
    let nodes = rng.range(3, 7);
    for i in 0..nodes {
        let cpu = rng.range(4, 10) * 1000;
        let mem = rng.range(4, 10) * 1024;
        let zone = if rng.chance(50) { "east" } else { "west" };
        let mut node = Node::new(&format!("n{i}"), cpu, mem).with_label("zone", zone);
        if rng.chance(30) {
            node = node.with_taint(Taint::no_schedule("dedicated", "gpu"));
        }
        cluster.add_node(node, 0);
    }
    let apps = rng.range(2, 5);
    for i in 0..apps {
        let replicas = rng.range(1, 5) as u32;
        let cpu = rng.range(1, 3) * 500;
        let mem = rng.range(1, 3) * 512;
        let mut tmpl = PodTemplate::new(cpu, mem).with_label("app", &format!("app{i}"));
        if rng.chance(45) {
            let zone = if rng.chance(50) { "east" } else { "west" };
            tmpl = tmpl.with_node_selector("zone", zone);
        }
        if rng.chance(30) {
            tmpl = tmpl.with_toleration(Toleration::exact("dedicated", "gpu"));
        }
        if rng.chance(40) {
            let mut sel = Selector::new();
            sel.insert("app".to_string(), format!("app{i}"));
            tmpl = tmpl.with_affinity(AffinityTerm::AntiAffinity(sel));
        }
        cluster.add_controller(Controller::deployment(&format!("app{i}"), replicas, tmpl));
    }
    cluster
}

/// Gate 1: the reconciler always reaches a stable fixed point. At that point
/// either every app is satisfied, or every still pending pod is genuinely
/// unschedulable (no feasible node exists for it right now). No oscillation.
#[test]
fn gate_convergence() {
    for seed in 0..ops() {
        let mut cluster = random_plain_cluster(seed);
        let result = reconcile_to_fixed_point(&mut cluster, ScorePolicy::BinPack, 10_000);
        assert!(result.converged, "seed {seed}: did not converge to a fixed point");

        // No oscillation: one more pass at the fixed point changes nothing.
        let again = reconcile_once(&mut cluster, ScorePolicy::BinPack);
        assert!(!again.changed(), "seed {seed}: oscillated after fixed point");

        if !fully_satisfied(&cluster) {
            // Every pending pod must be genuinely unschedulable.
            let pending: Vec<_> = cluster
                .pods
                .values()
                .filter(|p| p.phase == shepherd::object::PodPhase::Pending)
                .cloned()
                .collect();
            assert!(!pending.is_empty(), "seed {seed}: unsatisfied yet nothing pending");
            for pod in pending {
                assert!(
                    choose_node(&cluster, &pod, ScorePolicy::BinPack).is_none(),
                    "seed {seed}: pod {} left pending but a node was feasible",
                    pod.id
                );
            }
        }
    }
}

/// Gate 2: no scheduling decision ever overcommits a node, checked after every
/// single bind in a randomized workload.
#[test]
fn gate_capacity_invariant() {
    for seed in 0..ops() {
        let mut cluster = random_plain_cluster(seed);
        // Materialize desired pods without scheduling.
        let specs: Vec<(String, PodTemplate, u32)> = cluster
            .controllers
            .values()
            .map(|c| (c.name.clone(), c.template.clone(), c.replicas))
            .collect();
        let mut queue = Vec::new();
        for (name, tmpl, replicas) in specs {
            for _ in 0..replicas {
                queue.push(cluster.create_pod(&name, tmpl.clone()));
            }
        }
        // Schedule one pod at a time, checking the invariant after each bind.
        for id in queue {
            schedule_pod(&mut cluster, &id, ScorePolicy::BinPack);
            assert!(
                cluster.verify_capacity().is_ok(),
                "seed {seed}: capacity overcommitted after binding {id}"
            );
        }
        // And once more after a full reconcile.
        reconcile_to_fixed_point(&mut cluster, ScorePolicy::BinPack, 10_000);
        assert!(cluster.verify_capacity().is_ok(), "seed {seed}: overcommit after reconcile");
    }
}

/// Gate 3: killing a node reschedules its pods to restore the desired count
/// when the survivors can fit them, and the same seed produces identical
/// placement.
#[test]
fn gate_failure_recovery_and_determinism() {
    let count = ops().min(120);
    for seed in 0..count {
        let placement = |sim: &Simulator| -> Vec<(String, Option<String>)> {
            let mut v: Vec<_> = sim
                .cluster
                .pods
                .values()
                .map(|p| (p.id.clone(), p.node.clone()))
                .collect();
            v.sort();
            v
        };

        // Generously sized nodes so recovery is always feasible.
        let build = |seed: u64| -> Simulator {
            let mut rng = Rng::new(seed);
            let mut sim = Simulator::new(seed, ScorePolicy::BinPack, 3);
            let nodes = 4 + (seed % 3);
            for i in 0..nodes {
                sim.schedule(0, Event::AddNode(Node::new(&format!("n{i}"), 16000, 16384)));
            }
            let replicas = rng.range(2, 6) as u32;
            sim.schedule(
                0,
                Event::AddController(Controller::deployment(
                    "web",
                    replicas,
                    PodTemplate::new(1000, 1024).with_label("app", "web"),
                )),
            );
            sim
        };

        let mut a = build(seed);
        a.run(3);
        assert!(fully_satisfied(&a.cluster), "seed {seed}: not satisfied before failure");
        let desired = a.cluster.controllers["web"].replicas;

        a.schedule(4, Event::FailNode("n0".to_string()));
        a.run(10);
        assert!(
            fully_satisfied(&a.cluster),
            "seed {seed}: desired count not restored after failure"
        );
        assert_eq!(a.cluster.running_count("web"), desired);
        let on_dead = a
            .cluster
            .pods
            .values()
            .filter(|p| p.node.as_deref() == Some("n0"))
            .count();
        assert_eq!(on_dead, 0, "seed {seed}: pods still bound to the failed node");

        // Determinism: an identical run reproduces the placement exactly.
        let mut b = build(seed);
        b.run(3);
        b.schedule(4, Event::FailNode("n0".to_string()));
        b.run(10);
        assert_eq!(placement(&a), placement(&b), "seed {seed}: non-deterministic placement");
    }
}

/// Gate 4: no binding ever violates a taint, node selector or affinity rule.
#[test]
fn gate_constraints_respected() {
    for seed in 0..ops() {
        let mut cluster = random_constrained_cluster(seed);
        reconcile_to_fixed_point(&mut cluster, ScorePolicy::BinPack, 10_000);
        assert!(
            verify_constraints(&cluster).is_ok(),
            "seed {seed}: constraint violated: {:?}",
            verify_constraints(&cluster)
        );
        assert!(cluster.verify_capacity().is_ok(), "seed {seed}: capacity violated");
    }
}
