//! Dynamic membership admission errors and removal outcomes.

use crate::policy::{ChildId, InvalidPolicy};

/// A child reservation or dynamic admission error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ReserveError {
    /// No ambient supported async runtime exists.
    #[error("no ambient Tokio runtime is available")]
    NoRuntime,
    /// The child id was empty.
    #[error("child id must not be empty")]
    EmptyId,
    /// A resident membership already occupies the id.
    #[error("child id `{0}` is already resident")]
    DuplicateId(ChildId),
    /// A same-id membership is currently being removed.
    #[error("child id `{0}` is being removed")]
    RemovalInProgress(ChildId),
    /// The target dynamic scope is not admitting.
    #[error("scope is not admitting: {0:?}")]
    NotAdmitting(NotAdmittingCause),
    /// The scope can mint no further membership identities.
    #[error("membership identity space is exhausted")]
    IdentityExhausted,
    /// A public policy representation contained an invalid literal value.
    #[error(transparent)]
    InvalidPolicy(InvalidPolicy),
}

/// Exact reason an admission operation could not proceed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NotAdmittingCause {
    /// The scope membership is terminal.
    Terminal,
    /// The live scope incarnation is draining.
    Draining,
    /// The dynamic root is parked after startup failure.
    StartupFailed,
    /// No scope incarnation is currently live.
    NoLiveIncarnation,
    /// This operation's reservation ended before admission.
    ReservationEnded,
}

/// Outcome of an idempotent dynamic removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    /// A reservation or resident membership was removed.
    Removed,
    /// No matching membership remained to remove.
    AlreadyAbsent,
}
