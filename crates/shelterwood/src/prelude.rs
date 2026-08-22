//! The names a program writes down, importable as one glob.
//!
//! The crate root stays flat and canonical: every item lives there, and
//! [`crate`] is where each one is documented. This module re-exports a
//! subset of those root paths so a file that defines actors and tasks,
//! declares a tree, sends and calls, and shuts down can open with a single
//! `use`. Nothing is defined here, nothing is renamed, and nothing foreign
//! is re-exported — the prelude is an index, not a second API.
//!
//! # Admission
//!
//! An item enters this module iff a program that defines actors and tasks,
//! declares a tree, sends and calls, and shuts down *must write its name*
//! — in an impl header, a struct field, or a function signature. Items that
//! appear only in return position you match occasionally, or only when
//! tuning behavior, live in a bundle instead. The criterion governs future
//! additions as well as the current contents.
//!
//! # Three rings
//!
//! - **Ring 0** — `use shelterwood::prelude::*;`, the names above. This is
//!   the ordinary opening line of a file that uses the framework.
//! - **Ring 1** — `use shelterwood::prelude::policy::*;` and its siblings
//!   ([`observe`], [`errors`], [`raw`], [`wiring`]): one area at a time,
//!   for a file that tunes supervision, reads observation, matches the
//!   error taxonomy, writes a raw receive loop, or wires slots.
//! - **Ring 2** — `use shelterwood::{Admission, ReserveError};`, the flat
//!   crate root, which is canonical and complete. Everything reachable
//!   through ring 0 or ring 1 is reachable there under the same name, and
//!   the root additionally carries what no ring exports.
//!
//! Runtime admission outcomes — [`crate::Admission`], [`crate::Removal`],
//! [`crate::RemoveOutcome`], [`crate::ReserveError`], and
//! [`crate::NotAdmittingCause`] — are deliberately ring 2 only. They are
//! matched at a handful of call sites in programs that admit children at
//! runtime, which is not the shape this module is sized for.
//!
//! # The `Context` collision
//!
//! [`Context`] is the actor callback context, and the name is contested.
//! [`std::task::Context`] appears in any manual [`Future`]
//! implementation, and `anyhow::Context` is an extension trait many
//! programs import. Rust's glob rules make this quiet rather than loud: an
//! explicit `use` shadows a glob-imported name without an error, so
//! `use anyhow::Context;` beneath `use shelterwood::prelude::*;` silently
//! retargets every bare `Context` in the file to the trait, and the actor
//! callbacks then fail to compile against a signature that no longer names
//! what it did.
//!
//! Import the trait for its methods only, or path-qualify the other side:
//!
//! ```ignore
//! use shelterwood::prelude::*;
//! use anyhow::Context as _; // methods available, name not bound
//!
//! fn poll_manually(context: &mut std::task::Context<'_>) { /* ... */ }
//! ```
//!
//! There is no `ActorContext` alias. One name for one type is worth more
//! than an escape hatch from a collision an explicit `use` already fixes.
//!
//! # The glob caveat
//!
//! Every glob prelude trades control for brevity: a name added here in a
//! later release can collide with a name the importing crate defines
//! itself. That is the standing cost of rings 0 and 1, accepted rather
//! than engineered around. A crate that wants immunity imports from the
//! root.
//!
//! # Composing upward
//!
//! Utility-tier crates (`specs/non-core.md` §27) depend only on the
//! supported façade and re-export their items through preludes of their
//! own. Those preludes are expected to glob `shelterwood::prelude::*` into
//! themselves, so this module is the base of that stack and its admission
//! rule is what keeps the stack's ground floor small.

#[doc(no_inline)]
pub use crate::{
    Actor, ActorDef, ActorOnceDef, ActorRef, Context, DynamicScopeRef, DynamicTree, ExitError,
    ExitResult, Replied, Reply, ScopeRef, StopContext, SubtreeDef, System, TaskContext, TaskDef,
    TaskOnceDef, TaskRef, Tree,
};

/// Supervision policy: restart, backoff, readiness, shutdown, mailboxes.
///
/// Policy is plain validated data, built once when a tree is declared and
/// not named again at runtime. Import this bundle in the file that
/// declares the tree; a file that only implements [`Actor`] rarely needs
/// it.
pub mod policy {
    #[doc(no_inline)]
    pub use crate::{
        Backoff, BackoffFactor, DefaultsInheritance, ExponentialBackoff, FixedBackoff, Intensity,
        Jitter, JitterSample, Mailbox, MailboxShutdown, NonZeroDuration, PolicyError, Readiness,
        ReadinessDeadline, RestartAttempt, RestartCondition, RestartCount, RestartPolicy,
        Retention, ScopeDefaults, ScopeFlavor, Shutdown, Strategy, TotalRestarts,
    };
}

/// Observation: current-state snapshots, lifecycle histories, identity.
///
/// The two observation surfaces are the authoritative recursive snapshot
/// ([`crate::ScopeSnapshot`]) and the lifecycle history ([`crate::LifecycleEvents`]);
/// the identities they report ([`crate::Membership`], [`crate::Incarnation`],
/// [`crate::ChildId`]) come with them because a reader almost always keys on one.
pub mod observe {
    #[doc(no_inline)]
    pub use crate::{
        ChildId, ChildSnapshot, ChildState, Incarnation, LifecycleEvent, LifecycleEventKind,
        LifecycleEvents, LifecycleItem, LifecycleSeq, LifecycleTryRecvError, Membership,
        MembershipStatus, ScopeSnapshot, ScopeState, SnapshotClosed, SnapshotReceiver, WaitError,
    };
}

/// The exit and error taxonomy, as a single import.
///
/// This bundle is congruent with the catalog in [`crate::guides::errors`]:
/// the structured result of an incarnation, the supervisor-side terminal
/// reasons, the startup and shutdown reports, and the failures returned by
/// sending, calling, replying, spawning, declaring, and validating policy.
/// Import it in the file that decides what a failure means.
pub mod errors {
    #[doc(no_inline)]
    pub use crate::{
        BuildError, CallError, CallErrorKind, Cancellation, Exit, ExitError, ExitKind, ExitResult,
        GracePhase, IntensityTrip, PolicyError, ReplyError, SendError, SendErrorKind,
        ShutdownStraggler, ShutdownTimeout, StartOrShutdownError, StartupError, StartupFailure,
        StartupFailureCause, StopReason,
    };
}

/// Raw actors, offloads, and blocking work.
///
/// A [`crate::RawActor`] owns its receive loop instead of returning to a callback
/// dispatcher. The offload machinery in this bundle is shared with
/// callback actors, so a file that leases work out without writing a raw
/// loop still wants it.
pub mod raw {
    #[doc(no_inline)]
    pub use crate::{
        Blocking, DeadlineBudget, DeadlineElapsed, Guard, Handler, RawActor, RawContext, RawDef,
        RawOnceDef, Rejected,
    };
}

/// Slots, subtrees, and the errors declaration raises.
///
/// Slots split reservation from definition, which is what lets cyclically
/// wired children hold each other's handles before either is defined.
/// Subtree composition rides in the same file, and [`crate::BuildError`] is what
/// both report.
pub mod wiring {
    #[doc(no_inline)]
    pub use crate::{
        ActorSlot, BuildError, DynamicActorSlot, DynamicSubtreeSlot, DynamicTaskSlot, Subtree,
        SubtreeOnceDef, SubtreeSlot, TaskSlot,
    };
}
