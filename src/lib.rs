//! # Shepherd
//!
//! Shepherd is a dependency free, Kubernetes style reconciliation control loop.
//! It continuously drives observed cluster state toward declared desired state:
//! it schedules pods onto nodes by resource fit and constraints, maintains
//! replica counts, reschedules on node failure and bin packs, all over a
//! deterministic node and pod simulator with an injected clock and a seeded
//! event stream.
//!
//! The provable core is small: the reconciler converges (observed equals
//! desired under churn, or reports genuine infeasibility) and never
//! overcommits a node.
//!
//! ## Modules
//!
//! - [`object`] declarative nouns: nodes, pods, controllers, constraints,
//!   disruption budgets.
//! - [`cluster`] observed state plus the mutations that change it.
//! - [`scheduler`] filter, score and bind a pending pod.
//! - [`reconciler`] the control loop that reaches a fixed point, including
//!   rolling updates.
//! - [`rollout`] voluntary evictions under disruption budgets.
//! - [`verify`] independent invariant checkers used by the gate.
//! - [`simulator`] deterministic time, PRNG and scriptable events.
//! - [`clock`], [`rng`] the injected sources of time and randomness.
//!
//! ## Example
//!
//! ```
//! use shepherd::cluster::Cluster;
//! use shepherd::object::{Controller, Node, PodTemplate};
//! use shepherd::reconciler::{fully_satisfied, reconcile_to_fixed_point};
//! use shepherd::scheduler::ScorePolicy;
//!
//! let mut cluster = Cluster::new(3);
//! cluster.add_node(Node::new("a", 4000, 4000), 0);
//! cluster.add_node(Node::new("b", 4000, 4000), 0);
//! cluster.add_controller(Controller::deployment("web", 5, PodTemplate::new(500, 500)));
//!
//! let result = reconcile_to_fixed_point(&mut cluster, ScorePolicy::BinPack, 100);
//! assert!(result.converged);
//! assert!(fully_satisfied(&cluster));
//! assert!(cluster.verify_capacity().is_ok());
//! ```

pub mod clock;
pub mod cluster;
pub mod object;
pub mod reconciler;
pub mod rng;
pub mod rollout;
pub mod scheduler;
pub mod simulator;
pub mod verify;
