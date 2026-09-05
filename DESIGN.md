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
- `reconciler` the control loop that drives observed state to desired state,
  including rolling updates.
- `rollout` voluntary eviction under disruption budgets and rollout inspection
  helpers.
- `verify` independent invariant checkers that reread final state from scratch.
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
affinity terms. A controller also carries **RolloutSettings**
(`max_unavailable`, `max_surge`) that bound how disruptively the workload rolls
onto a new template.

A **PodDisruptionBudget** names a selector and a `max_unavailable` count. It
caps how many pods matching the selector may be voluntarily disrupted at once.
The baseline for the budget is the owning controller's replica count, or the
current matched running count when the pods have no controller. An eviction
that would dip the matched running count below `baseline - max_unavailable`
is refused.

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

Node registration is total. Re-registering an id that already exists replaces
the node object and releases every running pod bound to it back to `Pending`,
so the reconciler revalidates the pods against the new capacity, labels and
taints. Without that rule a duplicate registration with a smaller body could
silently leave a node overcommitted or hosting pods that violate their
constraints. Heartbeats from a node that has stopped beating are ignored, so an
out of band heartbeat cannot revive a dead node and hand the scheduler a corpse
to place on.

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

## Rolling updates and disruption budgets

Replacing a controller's template while its pods exist starts a rolling update.
A pod is a member of the *new revision* when its stamped spec equals the
controller's current template, and of the *old revision* otherwise. A pass
whose controller has old revision pods runs `roll_forward` instead of the plain
count reconciliation. The step does three things in order.

1. Remove old revision **pending** pods. They hold no availability, so
   removing them is free and unblocks the accounting.
2. Drive the live total toward the **surge target** (`replicas + max_surge`)
   by creating new revision pods. When a mid roll scale down leaves surplus,
   the surplus is removed preferring old revision pods first, then pending
   pods, then the highest ids.
3. **Rotate.** Evict old revision running pods, highest id first, while the
   running count has headroom above the **availability floor**
   (`replicas - max_unavailable`). Each eviction goes through `evict_pod`,
   which refuses the removal when any matching `PodDisruptionBudget` would be
   breached, so a budget pauses the rotation at the budget floor even when the
   controller's own settings would allow dipping deeper. The floor used for
   the pause is the stricter of the two.

The roll terminates. Every pass either removes an old revision pod, creates
toward the surge target, or schedules a pending pod. Old revision pods are
finite, creations stop at the surge target, and scheduling is monotone, so a
pass eventually reports no change. When the new template is unschedulable the
roll stalls at an honest fixed point: old pods stay running, up to
`max_unavailable` of them evicted, new pods pending, and nothing oscillates.
Setting the template back to the previous version rolls back, because the
pending new revision pods become old revision pods that are removed for free
and replacements are recreated on the old template. Settings where
`max_unavailable` and `max_surge` are both zero cannot rotate any pod and
stall by construction, which is why the defaults are one and one.

`evict_pod` is also a public API for maintenance evictions outside a roll. It
refuses when the pod is unknown, not running, or a matching budget refuses.
Node failure evictions are involuntary and deliberately bypass budgets, as in
real orchestrators.

## Why each gate proves its claim

The gate lives in `tests/gate.rs`, with the rollout properties in
`tests/rollout.rs`. Every gate rechecks its property from scratch with the
checkers in `verify` and `Cluster::verify_capacity`, so a bug in the scheduler
cannot be masked by the code that produced the state. The sweep size is set by
`SHEPHERD_FUZZ_OPS`.

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
`verify_constraints`, which rechecks every running pod against its node for
selector match, taint toleration, and affinity and anti affinity, independently
of the scheduler that made the decisions.

**Adversarial edges.** It reconciles clusters containing zero replica apps,
oversized pods, ghost zone selectors and affinity against labels nothing
carries. It asserts each settles at a fixed point, that unschedulable pods
remain pending while their schedulable neighbours converge, that a further pass
changes nothing, and that capacity and constraint verifiers stay clean.

**Duplicate nodes and rejoins.** It re-registers a live node id with a smaller,
tainted, relabelled body and asserts the capacity and constraint invariants
survive, the released pods reconverge onto the survivors, and the cluster is
idempotent afterwards. It then kills a node in the simulator, lets the failure
trip and replacements land, rejoins the node, and asserts every running pod is
bound to an existing, healthy, beating node, which rules out ghost bindings.

**Capacity boundary.** For random boundary values on both resource axes it
places a pod whose request exactly equals node capacity and asserts full
satisfaction, then a pod one unit larger on either axis and asserts it stays
pending with capacity clean.

**Idempotence.** It snapshots the pod-to-node map of a converged constrained
cluster, runs one more pass under the same and under the other policy, and
asserts the report is empty and the snapshot identical.

**Rollout floor and surge.** After every single pass of a roll it asserts the
running count never dipped below `replicas - maxUnavailable`, the live count
never exceeded `replicas + maxSurge`, and both verifiers hold. At the end it
asserts no old revision pod remains and an identical rerun reproduces the
placement exactly.

**Disruption budgets.** It evicts directly until a budget refuses and asserts
the refusal names the budget and leaves the pod in place. It then rolls a
controller whose own settings allow two unavailable pods under a budget that
allows one and asserts the rotation completes without ever dipping below the
budget floor.

**Failed rollbacks.** It rolls onto a template no node can fit, asserts the
stall keeps the floor and the verifiers, sets the template back, and asserts
the cluster converges onto the old template with an idempotent second pass.

**Rolls through failure.** It drives a roll through the simulator with a node
failure scheduled mid roll, asserts convergence, capacity, constraints and the
budget floor after every tick, and asserts the roll still completes with the
desired count restored.
