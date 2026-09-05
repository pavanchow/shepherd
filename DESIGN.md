# Shepherd design

Shepherd is a Kubernetes style reconciliation control loop over a deterministic
simulator. This document explains the architecture, the object model, the
scheduler, the reconciliation loop and its convergence argument, failure
handling, and why each correctness gate proves what it claims.

The whole system is a pure function of a seed and a script of events. There is
no I/O, no wall clock, no threads, and no randomness outside a single seeded
PRNG. That is the design constraint everything else follows from, because it is
what makes the behaviour reproducible and cheaply fuzzable.

## Architecture

The crate is a thin stack of modules, each with one job.

- `clock` an injected logical clock measured in ticks. Failure detection is
  expressed in ticks, never in real time.
- `rng` a SplitMix64 PRNG. Same seed, same stream. Used only to generate
  workloads, never to make scheduling decisions.
- `object` the declarative data model. Plain structs with a few pure predicates.
- `cluster` the observed state (nodes, pods, controllers) and the operations
  that mutate it. All collections are ordered maps so iteration is
  deterministic.
- `scheduler` the placement pipeline: filter, score, bind.
- `reconciler` the control loop that drives observed state to desired state.
- `verify` independent invariant checkers that re read final state from scratch.
- `simulator` the deterministic driver that ties clock, PRNG, cluster and a
  script of events together.

Determinism is enforced structurally. Nodes, pods and controllers live in
`BTreeMap`s so every traversal is in sorted key order. Pod identifiers come from
per controller counters (`web-0`, `web-1`, ...) rather than the PRNG, so
identity is stable and readable. Scheduler tie breaks resolve to the lowest node
id. Given the same inputs, the same placement comes out every time.

## Object model

Desired state is declared with two kinds of object.

A **Node** has a fixed resource capacity (CPU in millicores, memory in MiB), a
set of labels, a list of taints, a health flag, a beating flag (whether it still
emits heartbeats), and the tick of its last heartbeat.

A **Controller** (a deployment or a replica set, both reconcile identically)
declares a desired replica count and a **PodTemplate**. The template carries the
resource requests, labels, a node selector, a list of tolerations, and a list of
affinity terms.

The reconciler materializes templates into **Pod** objects. A pod has an id, its
owning controller, a copy of the template it was stamped from, an optional bound
node, and a phase. The phase is `Pending` (created but not placed), `Running`
(bound to a healthy node and counting toward desired), or `Lost` (its node
failed, so it no longer counts and will be collected and replaced).

Constraints are modelled as pure predicates. A selector matches a label set when
every one of its key value pairs is present and equal. A toleration covers a
taint when the keys match and either the values match or the toleration accepts
any value. An affinity term is either `Affinity(selector)` (require a co located
running pod matching the selector on the same node) or `AntiAffinity(selector)`
(forbid one).

## Scheduler: filter then score

Scheduling one pending pod is a two stage pipeline.

**Filter.** A node is feasible for a pod only if all of the following hold. The
node is healthy. The pod requests fit within the node's currently free resources
(capacity minus what running pods already consume). The pod's node selector is
satisfied by the node's labels. Every `NoSchedule` taint on the node is tolerated
by the pod. Every affinity term is satisfied, and no anti affinity term is
violated, against the pods currently running on that node. If any check fails the
node is rejected, and the first failing reason is available for reporting.

**Score.** Feasible nodes are ranked under a policy. `BinPack`, the default,
prefers the node with the least resource left over after placement, which packs
pods tightly so whole nodes stay empty and drainable. `LeastLoaded` prefers the
node with the most left over, spreading load. Ties break to the lowest node id
because nodes are visited in sorted order and a strictly greater score is
required to displace the incumbent.

**Bind.** The winning node is recorded on the pod and the pod becomes `Running`.
Because the filter proves resource fit before a bind ever happens, and pods are
scheduled one at a time so each bind sees the effect of the previous one, a bind
can never overcommit a node. If no node is feasible the pod stays `Pending`,
which is the correct representation of insufficient capacity or an infeasible
constraint set.

## Reconciliation loop and convergence

A single pass (`reconcile_once`) does four things in order.

1. Garbage collect every `Lost` pod.
2. For each controller in sorted order, compare desired replicas with the count
   of its live (non `Lost`) pods. If short, create the difference as new
   `Pending` pods from the template. If over, delete the surplus, preferring
   `Pending` pods and then the highest ids so deletion is deterministic.
3. Schedule every `Pending` pod through the scheduler.
4. Return a report of what changed (collected, created, deleted, scheduled) plus
   the ids that remain pending.

`reconcile_to_fixed_point` runs passes until one reports no change or a pass
budget is hit. A pass that changes nothing is a fixed point, and a fixed point is
exactly convergence.

The loop terminates. Consider what a pass can do once the previous pass has run.
Lost pods are collected at most once, after which there are none. For each
controller, once the live count equals desired, no pod is created or deleted.
Pending pods that could be placed were placed in step three, so on the next pass
the only pods still pending are ones for which no node is feasible, and trying
them again places none of them. So the second pass after state settles creates
nothing, deletes nothing, collects nothing and schedules nothing, which reports
no change. In practice convergence is reached in a small constant number of
passes.

The loop is idempotent at the fixed point. Running another pass on a converged
cluster produces an empty report and no mutation, which the gate checks
directly.

At the fixed point one of two things is true. Either every controller has exactly
its desired number of running pods (`fully_satisfied` is true), or some pods are
pending and every one of them is genuinely unschedulable, meaning no feasible
node exists for it in the current state. The second case is the correct
insufficient capacity or infeasible report, not a failure to converge.

## Failure handling

Node failure is modelled through heartbeats on the injected clock. Each tick,
every beating node records a heartbeat at the current tick. Failing a node stops
it beating. On each tick the cluster declares a node failed once the gap between
now and its last heartbeat exceeds the heartbeat timeout. When a node is declared
failed, its health flag drops and every running pod bound to it moves to `Lost`,
which immediately frees that node's accounted capacity.

The next reconciliation collects the `Lost` pods, sees the affected controllers
now short of desired, creates replacements, and schedules them onto the remaining
feasible nodes. If the survivors have room the desired count is fully restored.
If they do not, the shortfall shows up as pending pods, which is the correct
report. A recovered node starts beating again and regains health on its next
heartbeat, rejoining the pool for future placements.

## Why each gate proves its claim

The gate lives in `tests/gate.rs`. Every gate re checks its property from
scratch with the checkers in `verify` and `Cluster::verify_capacity`, so a bug in
the scheduler cannot be masked by the code that produced the state. The sweep
size is set by `SHEPHERD_FUZZ_OPS`.

**Convergence.** For many seeds it builds a random cluster, reconciles to a fixed
point, and asserts the loop reported convergence. It then runs one more pass and
asserts nothing changed, which rules out oscillation. When the cluster is not
fully satisfied it asserts that some pods are pending and that the scheduler
finds no feasible node for any of them, which proves the reconciler did not leave
a placeable pod unplaced. This is the exact statement of the convergence claim.

**Capacity invariant.** It creates all desired pods, then schedules them one at a
time, calling `verify_capacity` after every single bind. `verify_capacity`
independently sums the requests of running pods on each node and compares against
capacity. Checking after each decision, rather than only at the end, proves the
invariant holds at every intermediate step, not just at rest.

**Failure recovery and determinism.** It builds a generously sized scenario in
the simulator, confirms it converges, then kills a node and advances time past
the heartbeat timeout. It asserts the desired count is restored, and that zero
pods remain bound to the dead node. It then builds the identical scenario from
the same seed, runs the same script, and asserts the full pod to node placement
vector is identical, which proves determinism end to end including through a
failure.

**Constraints respected.** It builds a random cluster that exercises taints,
node selectors and anti affinity, reconciles to a fixed point, and runs
`verify_constraints`, which re checks every running pod against its node for
selector match, taint toleration, and affinity and anti affinity, independently
of the scheduler that made the decisions.
