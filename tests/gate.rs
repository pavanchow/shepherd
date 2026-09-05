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
use shepherd::object::{AffinityTerm, Controller, Node, PodPhase, PodTemplate, Selector, Taint, Toleration};
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
        // Replica counts are tiny by construction, the cast cannot truncate.
        #[allow(clippy::cast_possible_truncation)]
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
        // Replica counts are tiny by construction, the cast cannot truncate.
        #[allow(clippy::cast_possible_truncation)]
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
            // Replica counts are tiny by construction, the cast cannot truncate.
            #[allow(clippy::cast_possible_truncation)]
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

/// Gate 5: adversarial edges. Zero-replica apps, pods that fit no node, and
/// selectors or affinity that reference things that do not exist must all
/// settle as clean fixed points. Unschedulable pods stay `Pending` forever
/// without blocking their schedulable neighbours, and no pass ever mutates a
/// settled cluster.
#[test]
fn gate_edge_cases_settle_cleanly() {
    // Zero replicas: nothing is created, and scale 3 -> 0 -> 3 round trips
    // cleanly through deletion and recreation.
    let mut c = Cluster::new(3);
    c.add_node(Node::new("a", 4000, 4000), 0);
    c.add_controller(Controller::deployment("empty", 0, PodTemplate::new(100, 100)));
    let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
    assert!(r.converged);
    assert!(c.pods.is_empty());
    assert!(fully_satisfied(&c));
    c.controllers.get_mut("empty").unwrap().replicas = 3;
    reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
    assert_eq!(c.running_count("empty"), 3);
    c.controllers.get_mut("empty").unwrap().replicas = 0;
    reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
    assert!(c.pods.is_empty());
    assert!(c.verify_capacity().is_ok());

    for seed in 0..ops() {
        // Pods too big for every node must pend forever without blocking the
        // schedulable app that shares the cluster.
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 1000, 1000), 0);
        c.add_node(Node::new("b", 1000, 1000), 0);
        c.add_controller(Controller::deployment("huge", 3, PodTemplate::new(1001, 1)));
        c.add_controller(Controller::deployment("tiny", 6, PodTemplate::new(1, 1)));
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 10_000);
        assert!(r.converged, "seed {seed}: no fixed point with unschedulable pods");
        assert_eq!(c.running_count("huge"), 0, "seed {seed}: oversize pod placed");
        assert_eq!(c.running_count("tiny"), 6, "seed {seed}: tiny app blocked by huge app");
        assert_eq!(r.pending.len(), 3, "seed {seed}: unschedulable pods not reported");
        let again = reconcile_once(&mut c, ScorePolicy::BinPack);
        assert!(!again.changed(), "seed {seed}: churn with unschedulable pods");
        assert!(c.verify_capacity().is_ok());

        // A node selector naming a zone no node carries, and pod affinity
        // naming a label nothing matches, both leave pods permanently pending
        // while the healthy app still converges.
        let mut c = Cluster::new(3);
        c.add_node(Node::new("a", 4000, 4000).with_label("zone", "east"), 0);
        c.add_controller(Controller::deployment(
            "ghost",
            2,
            PodTemplate::new(100, 100).with_node_selector("zone", "atlantis"),
        ));
        c.add_controller(Controller::deployment(
            "lonely",
            2,
            PodTemplate::new(100, 100).with_affinity(AffinityTerm::Affinity({
                let mut s = Selector::new();
                s.insert("app".to_string(), "nonexistent".to_string());
                s
            })),
        ));
        c.add_controller(Controller::deployment("normal", 2, PodTemplate::new(100, 100)));
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 10_000);
        assert!(r.converged, "seed {seed}: no fixed point with ghost selectors");
        assert_eq!(c.running_count("ghost"), 0, "seed {seed}: ghost zone placed");
        assert_eq!(c.running_count("lonely"), 0, "seed {seed}: dead affinity placed");
        assert_eq!(c.running_count("normal"), 2, "seed {seed}: normal app blocked");
        assert!(verify_constraints(&c).is_ok(), "seed {seed}: constraint violated");
    }
}

/// Gate 6: duplicate node registration and node rejoin with stale pods keep
/// every invariant intact. Re-registering an id with a smaller, tainted,
/// relabelled body releases its running pods instead of leaving them
/// overcommitted or in violation, and a failed node that rejoins never leaves
/// ghost bindings behind.
#[test]
fn gate_duplicate_node_and_rejoin() {
    for seed in 0..ops().min(50) {
        let mut c = Cluster::new(3);
        c.add_node(Node::new("n0", 4000, 4096).with_label("zone", "east"), 0);
        c.add_node(Node::new("n1", 4000, 4096).with_label("zone", "east"), 0);
        c.add_controller(Controller::deployment("web", 5, PodTemplate::new(500, 512)));
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert!(r.converged, "seed {seed}: baseline did not converge");
        assert_eq!(c.running_count("web"), 5);

        // Duplicate registration with a shrunken, tainted, relabelled body.
        c.add_node(
            Node::new("n0", 1000, 1024)
                .with_label("zone", "west")
                .with_taint(Taint::no_schedule("ded", "gpu")),
            1,
        );
        assert!(
            c.verify_capacity().is_ok(),
            "seed {seed}: overcommit after re-registration"
        );
        assert!(
            verify_constraints(&c).is_ok(),
            "seed {seed}: constraint violation after re-registration"
        );
        // The released pods reconverge onto n1 (n0 now rejects everything via
        // its taint and its shrunken capacity).
        let r2 = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert!(r2.converged, "seed {seed}: did not reconverge after re-registration");
        assert_eq!(c.running_count("web"), 5, "seed {seed}: replicas lost to re-registration");
        assert_eq!(c.running_pods_on("n0").count(), 0, "seed {seed}: pod on hostile node");
        assert!(
            verify_constraints(&c).is_ok(),
            "seed {seed}: constraint violation after reconvergence"
        );
        let again = reconcile_once(&mut c, ScorePolicy::BinPack);
        assert!(!again.changed(), "seed {seed}: churn after re-registration");

        // Node rejoin with stale pods: kill n1, let the failure trip and the
        // replacements land, then let the node rejoin. No pod may end up
        // bound to a missing, unhealthy or non-beating node.
        let mut sim = Simulator::new(seed, ScorePolicy::BinPack, 2);
        sim.schedule(0, Event::AddNode(Node::new("a", 4000, 4096)));
        sim.schedule(0, Event::AddNode(Node::new("b", 4000, 4096)));
        sim.schedule(
            0,
            Event::AddController(Controller::deployment(
                "web",
                6,
                PodTemplate::new(500, 512).with_label("app", "web"),
            )),
        );
        sim.run(3);
        assert_eq!(sim.cluster.running_count("web"), 6, "seed {seed}: sim baseline");
        sim.schedule(4, Event::FailNode("a".to_string()));
        sim.run(4);
        sim.schedule(9, Event::RecoverNode("a".to_string()));
        sim.run(6);
        assert!(fully_satisfied(&sim.cluster), "seed {seed}: rejoin did not recover");
        assert!(sim.cluster.verify_capacity().is_ok(), "seed {seed}: rejoin overcommit");
        assert!(verify_constraints(&sim.cluster).is_ok(), "seed {seed}: rejoin violation");
        for pod in sim.cluster.pods.values() {
            if pod.phase == PodPhase::Running {
                let node_id = pod.node.as_deref().unwrap_or_else(|| {
                    panic!("seed {seed}: running pod {} has no node", pod.id)
                });
                let node = &sim.cluster.nodes[node_id];
                assert!(
                    node.healthy && node.beating,
                    "seed {seed}: pod {} ghost-bound to {node_id}",
                    pod.id
                );
            }
        }
    }
}

/// Gate 7: the capacity boundary. A pod whose request exactly equals the node
/// capacity fits on every dimension, and one unit more on any dimension does
/// not. Checked across random boundary values on both resource axes.
#[test]
fn gate_boundary_capacity_exact_fit() {
    // Fixed boundary: exact fits, and a one millicore overshoot does not.
    let mut c = Cluster::new(3);
    c.add_node(Node::new("exact", 2000, 2048), 0);
    c.add_controller(Controller::deployment("eq", 1, PodTemplate::new(2000, 2048)));
    let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
    assert!(r.converged);
    assert!(fully_satisfied(&c));
    assert!(c.verify_capacity().is_ok());

    let mut c = Cluster::new(3);
    c.add_node(Node::new("exact", 2000, 2048), 0);
    c.add_controller(Controller::deployment("over", 1, PodTemplate::new(2001, 2048)));
    let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
    assert!(r.converged);
    assert_eq!(c.running_count("over"), 0);
    assert_eq!(r.pending.len(), 1);

    // Random boundary sweep on both axes, plus the memory dimension alone.
    for seed in 0..ops().min(50) {
        let mut rng = Rng::new(seed);
        let cpu = rng.range(1, 64) * 100;
        let mem = rng.range(1, 64) * 128;
        let mut c = Cluster::new(3);
        c.add_node(Node::new("n", cpu, mem), 0);
        c.add_controller(Controller::deployment("eq", 1, PodTemplate::new(cpu, mem)));
        reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert!(fully_satisfied(&c), "seed {seed}: exact fit cpu={cpu} mem={mem}");
        assert!(c.verify_capacity().is_ok(), "seed {seed}: boundary overcommit");

        let mut c = Cluster::new(3);
        c.add_node(Node::new("n", cpu, mem), 0);
        c.add_controller(Controller::deployment("over", 1, PodTemplate::new(cpu, mem + 1)));
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 100);
        assert_eq!(c.running_count("over"), 0, "seed {seed}: mem overshoot placed");
        assert_eq!(r.pending.len(), 1, "seed {seed}: mem overshoot not pending");
    }
}

/// Gate 8: reconcile idempotence on fully constrained clusters. Reconciling a
/// converged cluster again is a placement no-op under both policies: no
/// creates, no deletes, no re-bindings, and an identical pod-to-node map.
#[test]
fn gate_idempotent_on_constrained_clusters() {
    let snapshot = |c: &Cluster| -> Vec<(String, Option<String>, PodPhase)> {
        c.pods
            .values()
            .map(|p| (p.id.clone(), p.node.clone(), p.phase))
            .collect()
    };
    for seed in 0..ops() {
        for policy in [ScorePolicy::BinPack, ScorePolicy::LeastLoaded] {
            let mut c = random_constrained_cluster(seed);
            let r = reconcile_to_fixed_point(&mut c, policy, 10_000);
            assert!(r.converged, "seed {seed}: {policy:?} did not converge");
            let before = snapshot(&c);
            let report = reconcile_once(&mut c, policy);
            assert!(
                !report.changed(),
                "seed {seed}: {policy:?} second pass mutated the cluster"
            );
            assert_eq!(
                before,
                snapshot(&c),
                "seed {seed}: {policy:?} placement drifted on idempotent pass"
            );
            assert!(
                verify_constraints(&c).is_ok(),
                "seed {seed}: {policy:?} constraint violation at rest"
            );
        }
    }
}
