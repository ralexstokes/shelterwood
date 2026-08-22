//! Dynamic membership admission errors and removal outcomes.

use std::fmt;

use shelterwood_core::ChildId;

/// A child reservation or dynamic admission error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
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
    #[error("scope is not admitting: {0}")]
    NotAdmitting(NotAdmittingCause),
    /// The scope can mint no further membership identities.
    #[error("membership identity space is exhausted")]
    IdentityExhausted,
}

/// Exact reason an admission operation could not proceed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

impl fmt::Display for NotAdmittingCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Terminal => "terminal",
            Self::Draining => "draining",
            Self::StartupFailed => "startup failed",
            Self::NoLiveIncarnation => "no live incarnation",
            Self::ReservationEnded => "reservation ended",
        })
    }
}

/// Outcome of an idempotent dynamic removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    /// A reservation or resident membership was removed.
    Removed,
    /// No matching membership remained to remove.
    AlreadyAbsent,
}

// The admission and removal response receivers drop these inline when a
// caller abandons them (`DisposingReceiver::new_framework`); the marker is
// the claim that no variant can ever own a user destructor. A variant that
// grows one must lose the marker and move its receivers back to the
// disposal-lane constructor.
// SAFETY: every variant owns only framework enums and `ChildId`; none can
// invoke user code or block on drop. A user-owned field must remove this impl.
unsafe impl crate::runtime::FrameworkPlain for ReserveError {}

// SAFETY: this fieldless framework enum has no drop glue.
unsafe impl crate::runtime::FrameworkPlain for RemoveOutcome {}

#[cfg(test)]
mod tests {
    use super::{NotAdmittingCause, ReserveError};

    #[test]
    fn not_admitting_display_is_stable_and_does_not_delegate_to_debug() {
        let cases = [
            (NotAdmittingCause::Terminal, "terminal"),
            (NotAdmittingCause::Draining, "draining"),
            (NotAdmittingCause::StartupFailed, "startup failed"),
            (NotAdmittingCause::NoLiveIncarnation, "no live incarnation"),
            (NotAdmittingCause::ReservationEnded, "reservation ended"),
        ];

        for (cause, expected) in cases {
            assert_eq!(cause.to_string(), expected);
            assert_eq!(
                ReserveError::NotAdmitting(cause).to_string(),
                format!("scope is not admitting: {expected}")
            );
        }
    }
}
