//! The rollout gate.
//!
//! Property tests for rolling updates and disruption budgets. Each test drives
//! template changes through the reconciler and checks the rollout invariants
//! after every single pass, using the independent verifiers in
//! `shepherd::verify` and `Cluster::verify_capacity` so a bug in the rotation
//! logic cannot hide behind the code that produced the state.

use shepherd::cluster::Cluster;
use shepherd::object::{Controller, Node, PodDisruptionBudget, PodPhase, PodTemplate};
use shepherd::reconciler::{reconcile_once, reconcile_to_fixed_point};
use shepherd::rollout::{evict_pod, old_revision_count, rollout_in_progress, EvictionRefused};
use shepherd::scheduler::ScorePolicy;
use shepherd::simulator::{Event, Simulator};
use shepherd::verify::verify_constraints;

fn ops() -> u64 {
    std::env::var("SHEPHERD_FUZZ_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

/// Build a cluster with generous capacity for `replicas` web pods and drive it
/// to a converged baseline running the old template.
fn baseline_cluster(seed: u64, replicas: u32, cpu: u64, mem: u64) -> Cluster {
    let mut c = Cluster::new(3);
    let nodes = 3 + (seed % 3);
    for i in 0..nodes {
        c.add_node(Node::new(&format!("n{i}"), 8000, 8192), 0);
    }
    c.add_controller(Controller::deployment(
        "web",
        replicas,
        PodTemplate::new(cpu, mem).with_label("app", "web"),
    ));
    let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 1_000);
    assert!(r.converged, "seed {seed}: baseline did not converge");
    assert_eq!(c.running_count("web"), replicas);
    c
}

/// Gate R1: a template roll never dips the running count below
/// `replicas - max_unavailable`, never exceeds `replicas + max_surge` live
/// pods, and completes with every pod on the new template. The independent
/// verifiers hold after every single pass, and the roll is deterministic.
#[test]
fn rollout_gate_floor_and_surge() {
    for seed in 0..ops().min(100) {
        let (replicas, unavail, surge) = match seed % 4 {
            0 => (8, 2, 1),
            1 => (6, 1, 2),
            2 => (9, 3, 3),
            _ => (5, 1, 1),
        };
        let mut c = baseline_cluster(seed, replicas, 400, 512);
        c.controllers.get_mut("web").unwrap().rollout =
            shepherd::object::RolloutSettings::new(unavail, surge);
        c.controllers.get_mut("web").unwrap().template =
            PodTemplate::new(600, 512).with_label("app", "web");

        let mut passes = 0;
        loop {
            let report = reconcile_once(&mut c, ScorePolicy::BinPack);
            passes += 1;
            assert!(passes < 500, "seed {seed}: roll did not settle");
            let running = c.running_count("web");
            let live = c.live_pods_of("web").count();
            assert!(
                running >= replicas - unavail,
                "seed {seed} pass {passes}: running {running} dipped below floor {}",
                replicas - unavail
            );
            assert!(
                live <= (replicas + surge) as usize,
                "seed {seed} pass {passes}: live {live} exceeded surge cap {}",
                replicas + surge
            );
            assert!(
                c.verify_capacity().is_ok(),
                "seed {seed} pass {passes}: capacity violated mid roll"
            );
            assert!(
                verify_constraints(&c).is_ok(),
                "seed {seed} pass {passes}: constraints violated mid roll"
            );
            if !report.changed() {
                break;
            }
        }
        // The roll completed: no old revision pods remain and every replica
        // runs the new template.
        assert_eq!(old_revision_count(&c, "web"), 0, "seed {seed}: roll incomplete");
        assert_eq!(c.running_count("web"), replicas, "seed {seed}: wrong final count");
        assert!(!rollout_in_progress(&c, "web"), "seed {seed}: still rolling");
        for pod in c.pods.values() {
            assert_eq!(pod.spec.requests.cpu, 600, "seed {seed}: old pod survived");
        }

        // Determinism: the identical roll reproduces the placement exactly.
        let placement = |cl: &Cluster| -> Vec<(String, Option<String>)> {
            let mut v: Vec<_> = cl
                .pods
                .values()
                .map(|p| (p.id.clone(), p.node.clone()))
                .collect();
            v.sort();
            v
        };
        let mut d = baseline_cluster(seed, replicas, 400, 512);
        d.controllers.get_mut("web").unwrap().rollout =
            shepherd::object::RolloutSettings::new(unavail, surge);
        d.controllers.get_mut("web").unwrap().template =
            PodTemplate::new(600, 512).with_label("app", "web");
        reconcile_to_fixed_point(&mut d, ScorePolicy::BinPack, 1_000);
        assert_eq!(placement(&c), placement(&d), "seed {seed}: non-deterministic roll");
    }
}

/// Gate R2: a disruption budget refuses evictions that would breach it, both
/// through the direct eviction API and through the rotation of a rolling
/// update. The rotation pauses at the budget floor even when the controller's
/// own settings would allow dipping deeper.
#[test]
fn rollout_gate_pdb_blocks_eviction() {
    for seed in 0..ops().min(50) {
        // Direct API: 3 running, floor 2. First eviction succeeds, second is
        // refused and leaves the pod in place.
        let mut c = baseline_cluster(seed, 3, 500, 512);
        c.add_pdb(PodDisruptionBudget::new("web-pdb", 1).with_selector("app", "web"));
        let ids: Vec<String> = c
            .pods
            .values()
            .filter(|p| p.owner == "web")
            .map(|p| p.id.clone())
            .collect();
        assert!(evict_pod(&mut c, &ids[0]).is_ok(), "seed {seed}: first evict refused");
        assert_eq!(c.running_count("web"), 2, "seed {seed}: first evict lost");
        let refused = evict_pod(&mut c, &ids[1]);
        assert_eq!(
            refused,
            Err(EvictionRefused::BudgetExhausted {
                budget: "web-pdb".to_string(),
                floor: 2,
                running: 2
            }),
            "seed {seed}: budget did not refuse the breach"
        );
        assert!(
            c.pods.contains_key(&ids[1]),
            "seed {seed}: refused eviction removed the pod"
        );
        assert_eq!(c.running_count("web"), 2, "seed {seed}: refused evict changed state");

        // Rotation path: the controller allows 2 unavailable but the budget
        // allows only 1, so the roll must pause at the budget floor with old
        // pods still running, and every invariant intact.
        let mut c = baseline_cluster(seed, 6, 400, 512);
        c.add_pdb(PodDisruptionBudget::new("web-pdb", 1).with_selector("app", "web"));
        c.controllers.get_mut("web").unwrap().rollout =
            shepherd::object::RolloutSettings::new(2, 1);
        c.controllers.get_mut("web").unwrap().template =
            PodTemplate::new(600, 512).with_label("app", "web");
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 1_000);
        assert!(r.converged, "seed {seed}: rotation did not settle");
        assert!(
            c.running_count("web") >= 5,
            "seed {seed}: rotation dipped below the budget floor"
        );
        assert_eq!(
            old_revision_count(&c, "web"),
            0,
            "seed {seed}: rotation did not drain old pods despite surge room"
        );
        assert!(c.verify_capacity().is_ok(), "seed {seed}: capacity violated");
        assert!(verify_constraints(&c).is_ok(), "seed {seed}: constraints violated");
    }
}

/// Gate R3: a rollout whose new template cannot be scheduled stalls honestly
/// (it never breaches its floor), and can be rolled back to the previous
/// template, after which the cluster converges fully onto the old template.
#[test]
fn rollout_gate_failed_roll_rolls_back() {
    for seed in 0..ops().min(50) {
        let mut c = baseline_cluster(seed, 4, 500, 512);
        c.controllers.get_mut("web").unwrap().rollout =
            shepherd::object::RolloutSettings::new(1, 1);
        // New requests do not fit any node, so the roll must stall.
        c.controllers.get_mut("web").unwrap().template =
            PodTemplate::new(9000, 512).with_label("app", "web");
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 1_000);
        assert!(r.converged, "seed {seed}: stalled roll did not reach a fixed point");
        let running = c.running_count("web");
        assert!(
            running >= 3,
            "seed {seed}: stalled roll dipped below replicas - maxUnavailable"
        );
        assert!(
            rollout_in_progress(&c, "web"),
            "seed {seed}: stalled roll should still count as in progress"
        );
        assert!(c.verify_capacity().is_ok(), "seed {seed}: capacity violated mid roll");
        assert!(verify_constraints(&c).is_ok(), "seed {seed}: constraints violated mid roll");

        // Roll back to the previous template: the roll completes onto it.
        c.controllers.get_mut("web").unwrap().template =
            PodTemplate::new(500, 512).with_label("app", "web");
        let r = reconcile_to_fixed_point(&mut c, ScorePolicy::BinPack, 1_000);
        assert!(r.converged, "seed {seed}: rollback did not converge");
        assert_eq!(old_revision_count(&c, "web"), 0, "seed {seed}: rollback left old pods");
        assert_eq!(c.running_count("web"), 4, "seed {seed}: rollback lost replicas");
        assert!(fully_satisfied_after_rollback(&c), "seed {seed}: not satisfied after rollback");
        for pod in c.pods.values() {
            assert_eq!(pod.spec.requests.cpu, 500, "seed {seed}: oversized pod survived rollback");
            assert_eq!(pod.phase, PodPhase::Running, "seed {seed}: pod left pending after rollback");
        }
        assert!(c.verify_capacity().is_ok(), "seed {seed}: capacity violated after rollback");
        assert!(verify_constraints(&c).is_ok(), "seed {seed}: constraints violated after rollback");
        let again = reconcile_once(&mut c, ScorePolicy::BinPack);
        assert!(!again.changed(), "seed {seed}: churn after rollback");
    }
}

/// Gate R4: a roll driven through the simulator survives a node failure in the
/// middle of it. The verifier holds after every tick and the roll completes
/// once the cluster absorbs the failure.
#[test]
fn rollout_gate_survives_node_failure_mid_roll() {
    for seed in 0..ops().min(40) {
        let build = |s: u64| -> Simulator {
            let mut sim = Simulator::new(s, ScorePolicy::BinPack, 3);
            for i in 0..5u64 {
                sim.schedule(0, Event::AddNode(Node::new(&format!("n{i}"), 8000, 8192)));
            }
            sim.schedule(
                0,
                Event::AddController(Controller::deployment(
                    "web",
                    10,
                    PodTemplate::new(400, 512).with_label("app", "web"),
                )),
            );
            sim.schedule(
                2,
                Event::UpdateTemplate {
                    name: "web".to_string(),
                    template: PodTemplate::new(600, 512).with_label("app", "web"),
                },
            );
            sim.schedule(3, Event::AddPdb(PodDisruptionBudget::new("web-pdb", 2).with_selector("app", "web")));
            sim.schedule(4, Event::FailNode("n0".to_string()));
            sim
        };
        let mut sim = build(seed);
        let mut ticks = 0;
        while ticks < 60 {
            let (conv, _) = sim.step();
            ticks += 1;
            assert!(conv.converged, "seed {seed} tick {ticks}: no fixed point mid roll");
            assert!(
                sim.cluster.verify_capacity().is_ok(),
                "seed {seed} tick {ticks}: capacity violated"
            );
            assert!(
                verify_constraints(&sim.cluster).is_ok(),
                "seed {seed} tick {ticks}: constraints violated"
            );
            // The budget floor: at most 2 of the 10 replicas disrupted at once.
            assert!(
                sim.cluster.running_count("web") >= 8,
                "seed {seed} tick {ticks}: running dipped below the budget floor"
            );
            if !rollout_in_progress(&sim.cluster, "web") && ticks > 6 {
                break;
            }
        }
        assert_eq!(
            old_revision_count(&sim.cluster, "web"),
            0,
            "seed {seed}: roll never completed despite spare capacity"
        );
        assert_eq!(sim.cluster.running_count("web"), 10, "seed {seed}: desired count not restored");
    }
}

fn fully_satisfied_after_rollback(c: &Cluster) -> bool {
    c.controllers
        .values()
        .all(|ctl| c.running_count(&ctl.name) == ctl.replicas)
}
