# Shepherd

A dependency free, Kubernetes style reconciliation control loop you can simulate.

Shepherd continuously drives the observed state of a cluster toward a declared
desired state. You declare nodes and controllers (a replica count plus a pod
template), and Shepherd schedules pods onto nodes by resource fit and
constraints, keeps replica counts satisfied, reschedules pods when a node fails,
and bin packs to keep the cluster tight. It runs over a deterministic node and
pod simulator with an injected clock and a seeded event stream, so every run is
reproducible.

Zero external dependencies. Pure Rust standard library, edition 2021.

Live playground: https://pavanchow.github.io/shepherd/

## The gap it fills

Real orchestrators (Kubernetes and friends) are enormous, need a live cluster,
and hide the control loop behind network calls and etcd. That makes the core
idea, a reconciler that converges observed state to desired state without ever
overcommitting a node, hard to see, hard to test, and impossible to fuzz.

Shepherd is the opposite. The reconciler is a few hundred lines of ordinary Rust
with no I/O, no threads, and no randomness beyond one seeded PRNG. Time is a tick
counter you inject. Node failures, recoveries, scale changes and spec edits are
scripted events. Because the whole system is a deterministic function of its
seed and script, the correctness properties are checked by randomized property
tests that run in a fraction of a second.

Why a person or an AI agent would reach for it:

- Learn or teach scheduling and reconciliation without standing up a cluster.
- Prototype scheduling policies (bin pack versus least loaded) and see the
  placement change deterministically.
- Get a small, readable, fuzzable reference for the reconcile to fixed point
  pattern that you can embed, fork, or reason about.
- An agent can drive it as a pure library, script a scenario, and assert
  convergence without any environment setup.

## Quickstart

```
cargo run --release -- demo
cargo run --release -- converge --seed 7 --nodes 6 --apps 4
cargo test
```

`demo` runs a scripted tour: three nodes join, an app is declared and bin
packed, the app scales up, a node is killed and its pods reschedule to restore
the desired count, then the node recovers. `converge` generates a random
workload from a seed and reconciles it to a fixed point, printing per node
utilization and per app desired versus observed.

## API

```rust
use shepherd::cluster::Cluster;
use shepherd::object::{Controller, Node, PodTemplate};
use shepherd::reconciler::{fully_satisfied, reconcile_to_fixed_point};
use shepherd::scheduler::ScorePolicy;

let mut cluster = Cluster::new(3); // heartbeat timeout in ticks
cluster.add_node(Node::new("a", 4000, 4000), 0);
cluster.add_node(Node::new("b", 4000, 4000), 0);
cluster.add_controller(Controller::deployment("web", 5, PodTemplate::new(500, 500)));

let result = reconcile_to_fixed_point(&mut cluster, ScorePolicy::BinPack, 100);
assert!(result.converged);
assert!(fully_satisfied(&cluster));
assert!(cluster.verify_capacity().is_ok());
```

For time driven scenarios with node failures use `shepherd::simulator::Simulator`,
which owns the injected clock, the seeded PRNG and a script of `Event`s. Each
`step` advances one tick, delivers heartbeats, fires due events, detects failed
nodes and reconciles to a fixed point.

Module map:

- `object` the declarative nouns: `Node`, `Pod`, `Controller`, `Resources`,
  taints, tolerations, selectors and affinity terms.
- `cluster` the observed state and the mutations that change it.
- `scheduler` filter feasible nodes, score them, bind (`ScorePolicy::BinPack`
  or `LeastLoaded`).
- `reconciler` `reconcile_once`, `reconcile_to_fixed_point`, `fully_satisfied`.
- `verify` independent invariant checkers used by the gate.
- `simulator` deterministic time, PRNG and scriptable events.
- `clock`, `rng` the injected sources of time and randomness.

## The correctness gate

The provable core is that the reconciler converges and never overcommits a node.
Four randomized property tests in `tests/gate.rs` enforce it, each re checking
its property from scratch with independent verifiers:

1. **Convergence.** From random nodes and controller specs, reconciling to a
   fixed point yields observed equals desired, or a correct report that some
   pods are genuinely unschedulable. No oscillation: one more pass at the fixed
   point changes nothing.
2. **Capacity invariant.** In a randomized workload, the sum of pod requests
   bound to any node never exceeds that node's capacity, checked after every
   single scheduling decision.
3. **Failure recovery and determinism.** Killing a node reschedules its pods to
   restore the desired count on the remaining feasible nodes, and the same seed
   produces identical placement.
4. **Constraints respected.** No binding ever violates a taint, node selector,
   affinity or anti affinity rule.

The sweep size is bounded by `SHEPHERD_FUZZ_OPS` (default 200) so CI stays fast
and a developer can crank it up:

```
SHEPHERD_FUZZ_OPS=5000 cargo test --release --test gate
```

See `DESIGN.md` for the architecture and the convergence argument.

## License

MIT.
