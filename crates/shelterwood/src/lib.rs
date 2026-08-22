#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.
//!
//! A system is declared as a tree of actors, plain tasks, and nested scopes;
//! the tree owns startup order, readiness, restart policy, bounded mailboxes,
//! shutdown, and observation. Stable membership and incarnation identities
//! make failure recovery explicit without a global registry.
//!
//! # Vocabulary
//!
//! - A **scope** is a supervising node. An ordered scope ([`Tree`],
//!   [`Subtree`] declarations) has fixed membership: declaration order is
//!   startup order and reverse shutdown order, and each child's readiness
//!   gates the next. A **dynamic scope** ([`DynamicTree`],
//!   [`DynamicScopeRef`]) starts members concurrently and admits and removes
//!   them at runtime.
//! - A **child** is one supervised member of a scope — a callback [`Actor`],
//!   a [`RawActor`] owning its own receive loop, a plain task
//!   ([`TaskDef`]/[`TaskOnceDef`]), or a nested subtree. Actors and tasks are
//!   peers; nothing needs a mailbox merely to obtain supervision.
//! - A **membership** ([`Membership`]) is a child's identity within its
//!   scope: minted when the child is declared or admitted, stable across
//!   restarts, and never reused by a same-id replacement. An [`Incarnation`]
//!   identifies one run of a membership; restarts produce superseding
//!   incarnations. Handles ([`ActorRef`], [`TaskRef`], [`ScopeRef`]) address
//!   a membership and follow its restarts.
//! - The [`System`] is the sole owning handle for a spawned root. It waits
//!   for startup, drives bounded shutdown, and joins the root on return.
//!
//! # Quickstart
//!
//! A counter actor with a request/reply protocol, spawned under an ordered
//! tree inside an ambient Tokio runtime. The opening glob is
//! [`prelude`]; every name it brings in also lives at the crate root,
//! which is where each one is documented.
//!
//! ```rust
//! use std::time::Duration;
//!
//! use shelterwood::prelude::*;
//!
//! struct Counter {
//!     count: u64,
//! }
//!
//! enum Msg {
//!     Add(u64),
//!     Total(Reply<u64>),
//! }
//!
//! impl Actor for Counter {
//!     type Msg = Msg;
//!     type Args = ();
//!
//!     async fn init(_args: (), _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
//!         Ok(Self { count: 0 })
//!     }
//!
//!     async fn handle(&mut self, message: Msg, _context: &mut Context<'_, Self>) -> ExitResult {
//!         match message {
//!             Msg::Add(n) => self.count += n,
//!             Msg::Total(reply) => reply.send(self.count),
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut tree = Tree::new();
//!     let counter = tree.add_actor("counter", ActorDef::<Counter>::cloned(()))?;
//!
//!     let system = tree.spawn()?;
//!     system.wait_started().await?;
//!
//!     counter.send(Msg::Add(2)).await?;
//!     let replied = counter.call(Msg::Total, Duration::from_secs(1)).await?;
//!     assert_eq!(replied.value, 2);
//!
//!     system.shutdown(Duration::from_secs(5)).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Prelude
//!
//! [`prelude`] re-exports the names a program has to write down —
//! [`Actor`], [`Context`], the handles, the tree types — so a file can
//! open with one glob. Its submodules ([`prelude::policy`],
//! [`prelude::observe`], [`prelude::errors`], [`prelude::raw`],
//! [`prelude::wiring`]) bundle one area each. Neither ring adds surface:
//! the crate root below is canonical and complete, and it is where every
//! item is documented. See [`prelude`] for the admission rule, the
//! [`Context`] name collision, and the standing glob caveat.
//!
//! # The API by area
//!
//! The exports are deliberately flat; this index is the grouping.
//!
//! ## Actors
//!
//! [`Actor`] is the callback contract (`init`/`handle`/`on_stop`), declared
//! through [`ActorDef`] (restartable) or [`ActorOnceDef`] (one-shot).
//! Callbacks receive a [`Context`]; teardown receives the narrowed
//! [`StopContext`]. [`Handler`] is the raw-actor wrapper the callback loop
//! rides on, public for decorator composition.
//!
//! ## Mailboxes and messaging
//!
//! [`ActorRef`] is the membership-addressed send handle: [`ActorRef::send`]
//! (waits for acceptance), [`ActorRef::try_send`] (fail-fast),
//! [`ActorRef::send_timeout`] (bounded wait, message recovered on expiry),
//! and [`ActorRef::call`] (request/reply under one deadline, resolving to
//! [`Replied`]). Reply capabilities are [`Reply`], [`ReplyReceiver`], and
//! its [`ReplyReceive`] future; send/call futures are [`SendFuture`],
//! [`SendTimeout`], and [`CallFuture`]. Failures are [`SendError`] /
//! [`SendErrorKind`], [`CallError`] / [`CallErrorKind`], and [`ReplyError`]
//! — the decision table lives in [`guides::errors`]. Mailbox declarations
//! are [`Mailbox`] (bounded queue or latest-value) and [`MailboxShutdown`]
//! (drain or discard the frozen prefix).
//!
//! ## Trees and slots
//!
//! [`Tree`] and [`DynamicTree`] declare a root; [`System`] owns it once
//! spawned ([`StartOrShutdownError`] reports a rolled-back startup).
//! Subtrees compose through [`SubtreeDef`], [`SubtreeOnceDef`], and the
//! sealed [`Subtree`] dispatch trait. Slots ([`ActorSlot`], [`TaskSlot`],
//! [`SubtreeSlot`], [`DynamicActorSlot`], [`DynamicTaskSlot`],
//! [`DynamicSubtreeSlot`]) split reservation from definition so cyclically
//! wired children can hold each other's handles before either is defined.
//! Declaration failures surface as [`BuildError`].
//!
//! ## Scopes and admission
//!
//! [`ScopeRef`] addresses an ordered scope; [`DynamicScopeRef`] adds
//! runtime admission and removal, whose outcomes are [`Admission`],
//! [`Removal`], [`RemoveOutcome`], [`ReserveError`], and
//! [`NotAdmittingCause`]. Pre-spawn declarations return
//! [`StaticReserveError`]. Scope-level state is [`ScopeState`] and
//! [`MembershipStatus`].
//!
//! ## Tasks
//!
//! [`TaskDef`] declares a restartable supervised task and [`TaskOnceDef`] a
//! one-shot with a typed completion claimed through [`OneShotTaskRef`].
//! Tasks run with a [`TaskContext`] and are addressed by [`TaskRef`].
//!
//! ## Raw actors, offloads, and blocking work
//!
//! [`RawActor`] owns its receive loop directly, declared through [`RawDef`]
//! or [`RawOnceDef`] and driven with a [`RawContext`]. Offload machinery is
//! shared with callback actors: [`Guard`] (cancel-on-drop lease),
//! [`Blocking`] (cooperative blocking work), [`DeadlineElapsed`], and
//! [`Rejected`] (operation refused by a stopping incarnation).
//!
//! ## Policy
//!
//! Supervision policy is plain validated data: [`RestartPolicy`],
//! [`RestartCondition`], [`Backoff`] ([`FixedBackoff`],
//! [`ExponentialBackoff`], [`BackoffFactor`], [`Jitter`], [`JitterSample`]),
//! [`Intensity`], [`Strategy`], [`Readiness`], [`ReadinessDeadline`],
//! [`Shutdown`], [`Retention`], [`ScopeDefaults`], [`DefaultsInheritance`],
//! [`ScopeFlavor`], and [`NonZeroDuration`], with counters
//! [`RestartAttempt`], [`RestartCount`], and [`TotalRestarts`]. Invalid
//! configuration is unrepresentable; construction fails with
//! [`PolicyError`].
//!
//! ## Exits and errors
//!
//! [`Exit`] is the structured result of one incarnation (or a never-started
//! membership): classification [`ExitKind`], application error
//! [`ExitError`], and the [`ExitResult`] alias child code returns.
//! Supervisor-side terminal reasons are [`StopReason`], [`Cancellation`],
//! and [`GracePhase`]; startup failures are [`StartupError`],
//! [`StartupFailure`], and [`StartupFailureCause`]; restart-budget trips
//! are [`IntensityTrip`]; shutdown deadline reports are [`ShutdownTimeout`]
//! and [`ShutdownStraggler`].
//!
//! ## Observation
//!
//! [`ScopeSnapshot`] and [`ChildSnapshot`] are authoritative recursive
//! current state ([`ChildState`]), watched through [`SnapshotReceiver`]
//! (closed: [`SnapshotClosed`], waits: [`WaitError`]). Lifecycle histories
//! are [`LifecycleEvents`] streams of [`LifecycleItem`] /
//! [`LifecycleEvent`] / [`LifecycleEventKind`] ordered by [`LifecycleSeq`]
//! (non-blocking reads: [`LifecycleTryRecvError`]).
//!
//! ## Identity, timing, and cancellation
//!
//! [`ChildId`] names a child within one scope; [`Membership`] is the
//! process-wide unique key; [`Incarnation`] orders restarts via
//! `supersedes`. Deadline-bearing operations take a [`DeadlineBudget`];
//! cooperative shutdown and abort surface as [`CancellationToken`]s.
//!
//! # Guides
//!
//! - [`guides::retry_and_ordering`] — calls, retries, and message ordering.
//! - [`guides::shutdown_and_resources`] — shutdown and resource ownership.
//! - [`guides::errors`] — the error catalog: which type means what and what
//!   to match on.
//!
//! # Operational preconditions
//!
//! Supervision classifies panics only under `panic = "unwind"`; with
//! `panic = "abort"` the process ends before a supervisor can observe the
//! panic, and Rust's ordinary double-panic rule still aborts when a
//! destructor panics during an unwind. Spawn systems inside a Tokio runtime
//! with time enabled, and resolve [`System::shutdown`] (or let the dropped
//! owner finish teardown) before destroying that runtime; destroying the
//! runtime around a live system is outside the contract.

#[macro_use]
mod definition;
mod actor;
mod cells;
mod deadline;
mod driver;
mod engine;
mod exit;
mod identity;
mod mailbox;
mod plan;
mod policy;
mod raw;
mod runtime;
mod scope;
mod task;
mod tree;

pub use actor::{Actor, ActorDef, ActorOnceDef, Context, Handler, StopContext};
pub use cells::{
    CancellationToken, ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind,
    LifecycleEvents, LifecycleItem, LifecycleSeq, LifecycleTryRecvError, NotAdmittingCause,
    RemoveOutcome, ReserveError, ScopeSnapshot, SnapshotClosed, SnapshotReceiver,
    StaticReserveError, WaitError,
};
pub use deadline::DeadlineBudget;
pub use engine::{MembershipStatus, ScopeState};
pub use exit::{
    Cancellation, Exit, ExitError, ExitKind, ExitResult, GracePhase, IntensityTrip,
    ShutdownStraggler, ShutdownTimeout, StartupError, StartupFailure, StartupFailureCause,
    StopReason,
};
pub use identity::{ChildId, Incarnation, Membership};
pub use mailbox::{
    ActorRef, CallError, CallErrorKind, CallFuture, Replied, Reply, ReplyError, ReplyReceive,
    ReplyReceiver, SendError, SendErrorKind, SendFuture, SendTimeout,
};
pub use policy::{
    Backoff, BackoffFactor, DefaultsInheritance, ExponentialBackoff, FixedBackoff, Intensity,
    Jitter, JitterSample, Mailbox, MailboxShutdown, NonZeroDuration, PolicyError, Readiness,
    ReadinessDeadline, RestartAttempt, RestartCondition, RestartCount, RestartPolicy, Retention,
    ScopeDefaults, ScopeFlavor, Shutdown, Strategy, TotalRestarts,
};
pub use raw::{
    Blocking, DeadlineElapsed, Guard, RawActor, RawContext, RawDef, RawOnceDef, Rejected,
};
pub use scope::{DynamicScopeRef, ScopeRef};
pub use task::{OneShotTaskRef, TaskContext, TaskDef, TaskOnceDef, TaskRef};
pub use tree::{
    ActorSlot, Admission, BuildError, DynamicActorSlot, DynamicSubtreeSlot, DynamicTaskSlot,
    DynamicTree, Removal, StartOrShutdownError, Subtree, SubtreeDef, SubtreeOnceDef, SubtreeSlot,
    System, TaskSlot, Tree,
};

pub mod prelude;

/// Long-form operational guides, rendered with the API reference.
///
/// Each page documents a contract that spans many items — the material that
/// does not fit a single type's documentation. The modules under this one
/// exist only to carry the pages; they export nothing.
pub mod guides {
    #[doc = include_str!("../docs/retry-and-ordering.md")]
    pub mod retry_and_ordering {}

    #[doc = include_str!("../docs/shutdown-and-resources.md")]
    pub mod shutdown_and_resources {}

    #[doc = include_str!("../docs/errors.md")]
    pub mod errors {}
}
