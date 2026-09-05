//! The Shepherd command line: apply specs, step the reconciler and watch the
//! cluster converge.
//!
//! Subcommands:
//!   demo                 a scripted tour: bin pack, scale, kill a node, recover
//!   converge [--seed N] [--nodes N] [--apps N]
//!                        generate a random workload and reconcile to a fixed point
//!   help                 print usage

use std::env;

use shepherd::cluster::Cluster;
use shepherd::object::{AffinityTerm, Controller, Node, PodTemplate, Selector};
use shepherd::reconciler::{fully_satisfied, reconcile_to_fixed_point, Convergence};
use shepherd::rng::Rng;
use shepherd::scheduler::ScorePolicy;
use shepherd::simulator::{Event, Simulator};

fn main() {
    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(String::as_str).unwrap_or("help");
    match command {
        "demo" => demo(),
        "converge" => converge(&args[2..]),
        "help" | "--help" | "-h" => usage(),
        other => {
            eprintln!("unknown command: {other}\n");
            usage();
            std::process::exit(2);
        }
    }
}

fn usage() {
    println!(
        "shepherd - a dependency-free reconciliation control loop\n\n\
         USAGE:\n\
         \x20 shepherd demo\n\
         \x20 shepherd converge [--seed N] [--nodes N] [--apps N]\n\
         \x20 shepherd help\n"
    );
}

fn bar(used: u64, capacity: u64, width: usize) -> String {
    let filled = if capacity == 0 {
        0
    } else {
        ((used as u128 * width as u128) / capacity as u128) as usize
    };
    let filled = filled.min(width);
    let pct = (used * 100).checked_div(capacity).unwrap_or(0);
    format!(
        "[{}{}] {:>3}%",
        "#".repeat(filled),
        ".".repeat(width - filled),
        pct
    )
}

fn print_cluster(cluster: &Cluster) {
    println!("  nodes:");
    for node in cluster.nodes.values() {
        let used = cluster.allocated(&node.id);
        let health = if node.healthy { "healthy" } else { "FAILED " };
        let pods: Vec<&str> = cluster
            .running_pods_on(&node.id)
            .map(|p| p.id.as_str())
            .collect();
        println!(
            "    {:<6} {}  cpu {}  mem {}  pods: {}",
            node.id,
            health,
            bar(used.cpu, node.capacity.cpu, 12),
            bar(used.mem, node.capacity.mem, 12),
            if pods.is_empty() {
                "-".to_string()
            } else {
                pods.join(", ")
            }
        );
    }
    println!("  apps (desired vs observed):");
    for c in cluster.controllers.values() {
        let running = cluster.running_count(&c.name);
        let state = if running == c.replicas {
            "converged"
        } else {
            "reconciling"
        };
        println!(
            "    {:<6} desired={} observed={} {}",
            c.name, c.replicas, running, state
        );
    }
    let status = if fully_satisfied(cluster) {
        "CONVERGED"
    } else {
        "NOT CONVERGED (insufficient capacity)"
    };
    println!("  status: {status}\n");
}

fn demo() {
    println!("=== Shepherd demo ===\n");
    let mut sim = Simulator::new(7, ScorePolicy::BinPack, 3);

    println!("Step 1: three nodes join and a 5-replica 'web' app is declared.");
    sim.schedule(0, Event::AddNode(Node::new("node-a", 4000, 4096)));
    sim.schedule(0, Event::AddNode(Node::new("node-b", 4000, 4096)));
    sim.schedule(0, Event::AddNode(Node::new("node-c", 4000, 4096)));
    sim.schedule(
        0,
        Event::AddController(Controller::deployment(
            "web",
            5,
            PodTemplate::new(1000, 1024).with_label("app", "web"),
        )),
    );
    sim.step();
    print_cluster(&sim.cluster);

    println!("Step 2: scale 'web' up to 8 replicas and watch it bin-pack.");
    sim.schedule(
        1,
        Event::ScaleController {
            name: "web".to_string(),
            replicas: 8,
        },
    );
    sim.step();
    print_cluster(&sim.cluster);

    println!("Step 3: kill node-a. Its pods are lost and rescheduled to converge.");
    sim.schedule(2, Event::FailNode("node-a".to_string()));
    // Advance enough ticks for the heartbeat timeout to trip and reconciliation
    // to restore the desired count.
    for _ in 0..6 {
        sim.step();
    }
    print_cluster(&sim.cluster);

    println!("Step 4: node-a recovers and rejoins the pool.");
    sim.schedule(sim.clock.now() + 1, Event::RecoverNode("node-a".to_string()));
    for _ in 0..3 {
        sim.step();
    }
    print_cluster(&sim.cluster);

    println!(
        "Final: {}",
        if fully_satisfied(&sim.cluster) {
            "observed == desired for every app. Converged."
        } else {
            "some replicas remain pending (insufficient capacity)."
        }
    );
}

fn parse_flag(args: &[String], name: &str, default: u64) -> u64 {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == name {
            if let Some(v) = it.next() {
                if let Ok(n) = v.parse::<u64>() {
                    return n;
                }
            }
        }
    }
    default
}

fn converge(args: &[String]) {
    let seed = parse_flag(args, "--seed", 1);
    let node_count = parse_flag(args, "--nodes", 5).max(1);
    let app_count = parse_flag(args, "--apps", 4).max(1);

    println!("=== Shepherd converge (seed={seed}, nodes={node_count}, apps={app_count}) ===\n");

    let mut rng = Rng::new(seed);
    let mut cluster = Cluster::new(3);

    for i in 0..node_count {
        let cpu = rng.range(2, 8) * 1000;
        let mem = rng.range(2, 8) * 1024;
        let zone = if rng.chance(50) { "east" } else { "west" };
        cluster.add_node(
            Node::new(&format!("node-{i}"), cpu, mem).with_label("zone", zone),
            0,
        );
    }

    for i in 0..app_count {
        let replicas = rng.range(1, 6) as u32;
        let cpu = rng.range(1, 3) * 500;
        let mem = rng.range(1, 3) * 512;
        let mut tmpl = PodTemplate::new(cpu, mem).with_label("app", &format!("app-{i}"));
        if rng.chance(40) {
            let mut sel = Selector::new();
            sel.insert("app".to_string(), format!("app-{i}"));
            tmpl = tmpl.with_affinity(AffinityTerm::AntiAffinity(sel));
        }
        cluster.add_controller(Controller::deployment(&format!("app-{i}"), replicas, tmpl));
    }

    let result: Convergence = reconcile_to_fixed_point(&mut cluster, ScorePolicy::BinPack, 1000);
    print_cluster(&cluster);
    println!(
        "converged={} passes={} pending={}",
        result.converged,
        result.passes,
        result.pending.len()
    );
    match cluster.verify_capacity() {
        Ok(()) => println!("capacity invariant: OK (no node overcommitted)"),
        Err(e) => println!("capacity invariant: VIOLATED: {e}"),
    }
}
