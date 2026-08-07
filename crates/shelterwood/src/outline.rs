//! Serializable, resolved tree declarations.
//!
//! An outline is a policy/topology fingerprint, not a tree constructor or a
//! distribution format. It contains no implementation, arguments, closures,
//! or runtime state. Restartable subtree factories remain deliberately opaque
//! so producing one never invokes user code.

use std::num::NonZeroUsize;

use crate::{
    ChildId, Intensity, MailboxShutdown, Readiness, ReadinessDeadline, RestartPolicy, Retention,
    ScopeKind, Shutdown, Strategy,
};

/// Serializable projection of one fully resolved tree declaration.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Outline {
    /// Resolved root scope and its declaration-ordered children.
    pub root: ScopeOutline,
}

/// Resolved scope policy and topology.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeOutline {
    /// Ordered or dynamic scope flavor.
    pub kind: ScopeKind,
    /// Fate-sharing strategy for ordered scopes; `None` for dynamic scopes.
    pub strategy: Option<Strategy>,
    /// Scope-wide restart intensity.
    pub intensity: Intensity,
    /// Every effective default inherited by child declarations.
    pub defaults: ResolvedScopeDefaults,
    /// Children in declaration order.
    pub children: Vec<ChildOutline>,
}

/// Fully resolved scope defaults carried by an outline.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedScopeDefaults {
    /// Default child restart policy.
    pub child_restart: RestartPolicy,
    /// Default child shutdown policy.
    pub child_shutdown: Shutdown,
    /// Default actor mailbox kind and capacity.
    pub mailbox: ResolvedMailbox,
    /// Default frozen-prefix mailbox behavior.
    pub mailbox_shutdown: MailboxShutdown,
    /// Default gated-readiness deadline.
    pub readiness_deadline: ReadinessDeadline,
}

/// Actor mailbox policy with every inherited capacity resolved.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub enum ResolvedMailbox {
    /// Bounded FIFO mailbox.
    Queue {
        /// Resolved non-zero message capacity.
        capacity: NonZeroUsize,
    },
    /// Single latest-value slot.
    Latest,
    /// Bounded latest-value-by-key mailbox.
    LatestByKey {
        /// Resolved non-zero distinct-key capacity.
        capacity: NonZeroUsize,
    },
}

/// Typed child flavor retained in a resolved outline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub enum OutlineChildKind {
    /// Callback-oriented actor.
    Actor,
    /// Loop-owning raw actor.
    RawActor,
    /// Supervised async task.
    Task,
    /// Nested ordered scope.
    OrderedScope,
    /// Nested dynamic scope.
    DynamicScope,
}

/// One child row with every inherited policy field resolved.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildOutline {
    /// Child id within this scope.
    pub id: ChildId,
    /// Typed declaration flavor.
    pub kind: OutlineChildKind,
    /// Effective restart policy.
    pub restart: RestartPolicy,
    /// Effective shutdown policy.
    pub shutdown: Shutdown,
    /// Effective readiness mode.
    pub readiness: Readiness,
    /// Effective readiness deadline.
    pub readiness_deadline: ReadinessDeadline,
    /// Effective terminal-membership retention.
    pub retention: Retention,
    /// Effective mailbox policy for actor children only.
    pub mailbox: Option<ResolvedMailbox>,
    /// Effective mailbox shutdown behavior for actor children only.
    pub mailbox_shutdown: Option<MailboxShutdown>,
    /// Nested topology for scope children only.
    pub interior: Option<OutlineInterior>,
}

/// Visibility of a nested scope declaration.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub enum OutlineInterior {
    /// Recursively carried one-shot subtree declaration.
    Recursive(ScopeOutline),
    /// Restartable subtree factory whose code is deliberately never invoked.
    Opaque,
}

/// Failure to outline an incomplete declaration.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum OutlineError {
    /// Every unresolved reservation, expressed as a root-relative child path.
    #[error("tree contains unfilled reservations")]
    UnfilledReservations {
        /// Deterministic declaration-order paths to unresolved slots.
        paths: Vec<Vec<ChildId>>,
    },
}
