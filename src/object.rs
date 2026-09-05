//! The declarative object model.
//!
//! These are the nouns of the system. Users declare desired state with
//! [`Node`] and [`Controller`] objects, and the reconciler materializes that
//! desire into [`Pod`] objects bound to nodes. Everything here is plain data
//! with no behaviour beyond a few pure predicates used by the scheduler.

use std::collections::BTreeMap;

/// A set of key value labels. `BTreeMap` keeps iteration order stable, which
/// matters because the scheduler must be deterministic.
pub type Labels = BTreeMap<String, String>;

/// A label selector. A selector matches a label set when every one of its
/// key/value pairs is present and equal in the label set.
pub type Selector = BTreeMap<String, String>;

/// Return true when `labels` satisfies every entry of `selector`.
#[must_use]
pub fn selector_matches(selector: &Selector, labels: &Labels) -> bool {
    selector
        .iter()
        .all(|(k, v)| labels.get(k).is_some_and(|found| found == v))
}

/// A compute resource pair. CPU is measured in millicores, memory in MiB.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Resources {
    pub cpu: u64,
    pub mem: u64,
}

impl std::ops::Add for Resources {
    type Output = Resources;

    /// Component wise saturating addition.
    fn add(self, other: Resources) -> Resources {
        Resources::new(
            self.cpu.saturating_add(other.cpu),
            self.mem.saturating_add(other.mem),
        )
    }
}

impl Resources {
    /// Build a resource pair.
    #[must_use]
    pub fn new(cpu: u64, mem: u64) -> Self {
        Resources { cpu, mem }
    }

    /// True when `self` fits within `capacity` on every dimension.
    #[must_use]
    pub fn fits_within(self, capacity: Resources) -> bool {
        self.cpu <= capacity.cpu && self.mem <= capacity.mem
    }

    /// Component wise saturating subtraction (never underflows below zero).
    #[must_use]
    pub fn saturating_sub(self, other: Resources) -> Resources {
        Resources::new(
            self.cpu.saturating_sub(other.cpu),
            self.mem.saturating_sub(other.mem),
        )
    }
}

/// The effect of a taint. Only the scheduling gate is modelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaintEffect {
    /// A pod that does not tolerate the taint cannot be scheduled here.
    NoSchedule,
}

/// A taint repels pods from a node unless the pod carries a matching
/// toleration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Taint {
    pub key: String,
    pub value: String,
    pub effect: TaintEffect,
}

impl Taint {
    /// Build a `NoSchedule` taint.
    #[must_use]
    pub fn no_schedule(key: &str, value: &str) -> Self {
        Taint {
            key: key.to_string(),
            value: value.to_string(),
            effect: TaintEffect::NoSchedule,
        }
    }
}

/// A toleration lets a pod be scheduled onto a node carrying a matching taint.
/// A `None` value tolerates any value for the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Toleration {
    pub key: String,
    pub value: Option<String>,
}

impl Toleration {
    /// Tolerate a specific key/value pair.
    #[must_use]
    pub fn exact(key: &str, value: &str) -> Self {
        Toleration {
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    /// Tolerate any taint carrying this key regardless of value.
    #[must_use]
    pub fn any_value(key: &str) -> Self {
        Toleration {
            key: key.to_string(),
            value: None,
        }
    }

    /// True when this toleration covers the given taint.
    #[must_use]
    pub fn tolerates(&self, taint: &Taint) -> bool {
        if self.key != taint.key {
            return false;
        }
        match &self.value {
            None => true,
            Some(v) => v == &taint.value,
        }
    }
}

/// An affinity rule expressed against other pods on the same node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AffinityTerm {
    /// The pod must land on a node that already hosts a running pod whose
    /// labels match the selector.
    Affinity(Selector),
    /// The pod must not land on a node that hosts a running pod whose labels
    /// match the selector.
    AntiAffinity(Selector),
}

/// The blueprint a controller stamps out for each replica.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PodTemplate {
    pub requests: Resources,
    pub labels: Labels,
    pub node_selector: Selector,
    pub tolerations: Vec<Toleration>,
    pub affinity: Vec<AffinityTerm>,
}

impl PodTemplate {
    /// A minimal template requesting the given resources.
    #[must_use]
    pub fn new(cpu: u64, mem: u64) -> Self {
        PodTemplate {
            requests: Resources::new(cpu, mem),
            ..Default::default()
        }
    }

    /// Attach a label (builder style).
    #[must_use]
    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    /// Constrain to nodes carrying this label (builder style).
    #[must_use]
    pub fn with_node_selector(mut self, key: &str, value: &str) -> Self {
        self.node_selector
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Add a toleration (builder style).
    #[must_use]
    pub fn with_toleration(mut self, toleration: Toleration) -> Self {
        self.tolerations.push(toleration);
        self
    }

    /// Add an affinity term (builder style).
    #[must_use]
    pub fn with_affinity(mut self, term: AffinityTerm) -> Self {
        self.affinity.push(term);
        self
    }
}

/// The lifecycle phase of a pod.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodPhase {
    /// Created but not yet bound to a node.
    Pending,
    /// Bound to a healthy node and counted toward the desired replica total.
    Running,
    /// The node it ran on failed. The pod no longer counts and will be garbage
    /// collected and replaced.
    Lost,
}

/// A running unit of work owned by a controller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pod {
    pub id: String,
    pub owner: String,
    pub spec: PodTemplate,
    pub node: Option<String>,
    pub phase: PodPhase,
}

impl Pod {
    /// Create a fresh pending pod from a template.
    #[must_use]
    pub fn pending(id: String, owner: String, spec: PodTemplate) -> Self {
        Pod {
            id,
            owner,
            spec,
            node: None,
            phase: PodPhase::Pending,
        }
    }
}

/// A worker node with a fixed resource capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub id: String,
    pub capacity: Resources,
    pub labels: Labels,
    pub taints: Vec<Taint>,
    /// Whether the control plane currently considers the node healthy.
    pub healthy: bool,
    /// Whether the node is still emitting heartbeats. A failed node stops.
    pub beating: bool,
    /// The tick of the most recent observed heartbeat.
    pub last_heartbeat: u64,
}

impl Node {
    /// Build a healthy node with the given capacity.
    #[must_use]
    pub fn new(id: &str, cpu: u64, mem: u64) -> Self {
        Node {
            id: id.to_string(),
            capacity: Resources::new(cpu, mem),
            labels: Labels::new(),
            taints: Vec::new(),
            healthy: true,
            beating: true,
            last_heartbeat: 0,
        }
    }

    /// Attach a label (builder style).
    #[must_use]
    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    /// Attach a taint (builder style).
    #[must_use]
    pub fn with_taint(mut self, taint: Taint) -> Self {
        self.taints.push(taint);
        self
    }
}

/// Whether a controller is a plain replica set or a deployment. Both drive the
/// same reconciliation, the kind is carried for reporting fidelity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControllerKind {
    ReplicaSet,
    Deployment,
}

/// A controller declares a desired number of identical replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Controller {
    pub name: String,
    pub kind: ControllerKind,
    pub replicas: u32,
    pub template: PodTemplate,
    /// Rolling update settings applied when the template changes after pods
    /// already exist. Changing `template` starts a roll that rotates pods
    /// stamped from the previous template onto the new one.
    pub rollout: RolloutSettings,
}

impl Controller {
    /// Build a deployment controller.
    #[must_use]
    pub fn deployment(name: &str, replicas: u32, template: PodTemplate) -> Self {
        Controller {
            name: name.to_string(),
            kind: ControllerKind::Deployment,
            replicas,
            template,
            rollout: RolloutSettings::default(),
        }
    }

    /// Build a replica set controller.
    #[must_use]
    pub fn replica_set(name: &str, replicas: u32, template: PodTemplate) -> Self {
        Controller {
            name: name.to_string(),
            kind: ControllerKind::ReplicaSet,
            replicas,
            template,
            rollout: RolloutSettings::default(),
        }
    }
}

/// How aggressively a rolling update may disrupt the workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RolloutSettings {
    /// How many replicas below `replicas` the running count may dip while the
    /// roll evicts old pods. The running count never goes under
    /// `replicas - max_unavailable` because of the roll.
    pub max_unavailable: u32,
    /// How many pods above `replicas` may exist temporarily while the roll
    /// creates new pods ahead of evicting old ones.
    pub max_surge: u32,
}

impl RolloutSettings {
    /// Explicit settings.
    #[must_use]
    pub fn new(max_unavailable: u32, max_surge: u32) -> Self {
        RolloutSettings {
            max_unavailable,
            max_surge,
        }
    }
}

impl Default for RolloutSettings {
    /// One unavailable, one surging: the smallest settings that let a roll
    /// always make progress. Settings where both are zero cannot rotate any
    /// pod and stall the roll at its fixed point.
    fn default() -> Self {
        RolloutSettings {
            max_unavailable: 1,
            max_surge: 1,
        }
    }
}

/// A budget that limits how many pods matching a selector may be disrupted
/// (voluntarily evicted) at the same time. While the matched running count is
/// at `baseline - max_unavailable` or below, evictions that would dip further
/// are refused. The baseline is the owning controller's replica count, or the
/// current matched running count when the pods have no controller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodDisruptionBudget {
    pub name: String,
    pub selector: Selector,
    pub max_unavailable: u32,
}

impl PodDisruptionBudget {
    /// A budget named `name` allowing `max_unavailable` simultaneous
    /// disruptions of pods matching its selector.
    #[must_use]
    pub fn new(name: &str, max_unavailable: u32) -> Self {
        PodDisruptionBudget {
            name: name.to_string(),
            selector: Selector::new(),
            max_unavailable,
        }
    }

    /// Match pods carrying this label (builder style).
    #[must_use]
    pub fn with_selector(mut self, key: &str, value: &str) -> Self {
        self.selector.insert(key.to_string(), value.to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_matching() {
        let mut labels = Labels::new();
        labels.insert("zone".to_string(), "a".to_string());
        labels.insert("disk".to_string(), "ssd".to_string());

        let mut sel = Selector::new();
        sel.insert("zone".to_string(), "a".to_string());
        assert!(selector_matches(&sel, &labels));

        sel.insert("disk".to_string(), "hdd".to_string());
        assert!(!selector_matches(&sel, &labels));
    }

    #[test]
    fn resource_arithmetic() {
        let a = Resources::new(1000, 2048);
        let b = Resources::new(400, 512);
        assert_eq!(a + b, Resources::new(1400, 2560));
        assert_eq!(a.saturating_sub(b), Resources::new(600, 1536));
        assert_eq!(b.saturating_sub(a), Resources::new(0, 0));
        assert!(b.fits_within(a));
        assert!(!a.fits_within(b));
    }

    #[test]
    fn toleration_semantics() {
        let taint = Taint::no_schedule("gpu", "true");
        assert!(Toleration::exact("gpu", "true").tolerates(&taint));
        assert!(!Toleration::exact("gpu", "false").tolerates(&taint));
        assert!(Toleration::any_value("gpu").tolerates(&taint));
        assert!(!Toleration::any_value("net").tolerates(&taint));
    }
}
