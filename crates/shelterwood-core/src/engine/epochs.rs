use crate::identity::MonotonicCounter;

/// One scope incarnation's ownership token: minted for the driver that runs
/// the incarnation, and also addressed forward by a shutdown request that
/// targets the next incarnation before any driver has begun it.
///
/// Epochs are minted per scope in strictly increasing order starting at
/// `Epoch::FIRST`, so plain ordering is total over every minted epoch and
/// `Epoch` derives `Ord`. Unlike an incarnation identity, an epoch carries no scope tag, so ordering
/// is meaningful only between epochs of one scope; every comparison site
/// draws both operands from that scope's own control plane.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Epoch(u64);

impl Epoch {
    /// The first epoch a scope can mint.
    const FIRST: Self = Self(1);

    /// The epoch minted after `previous`, or the first with no predecessor.
    fn after(previous: Option<Self>) -> Self {
        previous.map_or(Self::FIRST, Self::successor)
    }

    fn successor(self) -> Self {
        Self(MonotonicCounter::successor(self.0))
    }
}

/// The epoch a scope shutdown request lands on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestTarget {
    pub epoch: Epoch,
    /// The target incarnation has not begun: the request addresses the next
    /// epoch a future driver will mint.
    pub pending_incarnation: bool,
}

/// Cross-incarnation liveness encoded once as an epoch state, rather than an
/// epoch pair plus an independently mutable `live` bit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeEpochs {
    Idle {
        last_stopped: Option<Epoch>,
    },
    Live {
        current: Epoch,
        last_stopped: Option<Epoch>,
    },
}

impl Default for ScopeEpochs {
    fn default() -> Self {
        Self::Idle { last_stopped: None }
    }
}

impl ScopeEpochs {
    pub fn begin(&mut self) -> Option<Epoch> {
        let last_stopped = match *self {
            Self::Idle { last_stopped } => last_stopped,
            // One scope cell cannot own two simultaneous drivers. Rejecting
            // a second begin also prevents it from invalidating the live
            // driver's epoch while trying to advance the counter.
            Self::Live { .. } => return None,
        };
        let current = Epoch::after(last_stopped);
        *self = Self::Live {
            current,
            last_stopped,
        };
        // A shutdown wait settles on `finished(target)`, so a freshly minted
        // epoch must not already read as finished — that would settle a wait
        // against the incarnation it just started.
        // Debug-only: callers hold scope control (see the lock rule in AGENTS.md).
        debug_assert!(
            !self.finished(current),
            "a freshly minted epoch is not already finished"
        );
        Some(current)
    }

    pub fn live_epoch(self) -> Option<Epoch> {
        match self {
            Self::Idle { .. } => None,
            Self::Live { current, .. } => Some(current),
        }
    }

    pub fn request_target(self) -> RequestTarget {
        match self {
            Self::Live { current, .. } => RequestTarget {
                epoch: current,
                pending_incarnation: false,
            },
            Self::Idle { last_stopped } => RequestTarget {
                epoch: Epoch::after(last_stopped),
                pending_incarnation: true,
            },
        }
    }

    pub fn finish(&mut self, epoch: Epoch) -> bool {
        match *self {
            Self::Live { current, .. } if current == epoch => {
                *self = Self::Idle {
                    last_stopped: Some(epoch),
                };
                // Settlement is monotone: once an owner finishes its epoch,
                // every later `finished(epoch)` — including one asked across a
                // subsequent incarnation — keeps reporting it. A waiter that
                // missed the pulse can therefore never park forever.
                // Debug-only: callers hold scope control (see the lock rule in AGENTS.md).
                debug_assert!(
                    self.finished(epoch),
                    "a finished epoch stays observably finished"
                );
                true
            }
            Self::Idle { .. } | Self::Live { .. } => false,
        }
    }

    /// Marks the next unminted epoch finished without it ever running.
    ///
    /// A stop request accepted with no live incarnation targets exactly
    /// that epoch (`request_target`). When the parent resolves such a
    /// request without constructing an incarnation, vacating the target
    /// settles every wait on it and makes the next `begin` mint the epoch
    /// after it, whose request latch is therefore clear. Only the idle
    /// plane's next epoch can be vacated; any other epoch is refused.
    pub fn vacate(&mut self, epoch: Epoch) -> bool {
        match *self {
            Self::Idle { last_stopped } if Epoch::after(last_stopped) == epoch => {
                *self = Self::Idle {
                    last_stopped: Some(epoch),
                };
                true
            }
            Self::Idle { .. } | Self::Live { .. } => false,
        }
    }

    pub fn is_current(self, epoch: Epoch) -> bool {
        self.live_epoch() == Some(epoch)
    }

    pub fn request_is_pending(self, epoch: Epoch) -> bool {
        matches!(self, Self::Idle { last_stopped } if Some(epoch) > last_stopped)
    }

    pub fn finished(self, epoch: Epoch) -> bool {
        match self {
            Self::Idle { last_stopped } | Self::Live { last_stopped, .. } => {
                last_stopped.is_some_and(|last_stopped| last_stopped >= epoch)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Epoch, RequestTarget, ScopeEpochs};

    #[test]
    fn only_the_live_epoch_is_current() {
        let mut epochs = ScopeEpochs::default();
        assert!(
            !epochs.is_current(Epoch::FIRST),
            "an idle scope has no current epoch"
        );
        let first = epochs.begin().expect("first epoch is available");
        let unminted = first.successor();
        assert!(epochs.is_current(first));
        assert!(
            !epochs.is_current(unminted),
            "a future epoch is not current"
        );
        assert!(epochs.finish(first));
        assert!(
            !epochs.is_current(first),
            "a finished epoch is no longer current"
        );

        let second = epochs.begin().expect("second epoch is available");
        assert_eq!(second, unminted);
        assert!(epochs.is_current(second));
        assert!(
            !epochs.is_current(first),
            "a stale epoch is not current under a later live incarnation"
        );
    }

    #[test]
    fn scope_epochs_mint_in_order_and_settle_monotonically() {
        let mut epochs = ScopeEpochs::default();
        assert_eq!(
            epochs.request_target(),
            RequestTarget {
                epoch: Epoch(1),
                pending_incarnation: true,
            }
        );
        let first = epochs.begin().expect("first epoch is available");
        assert_eq!(epochs.live_epoch(), Some(first));
        assert_eq!(
            epochs.request_target(),
            RequestTarget {
                epoch: first,
                pending_incarnation: false,
            }
        );
        assert_eq!(epochs.begin(), None, "a live epoch cannot be replaced");
        let unminted = first.successor();
        assert!(!epochs.request_is_pending(first));
        assert!(!epochs.request_is_pending(unminted));
        assert!(!epochs.finished(first));
        assert!(!epochs.finished(unminted));
        assert!(!epochs.finish(unminted));
        assert_eq!(epochs.live_epoch(), Some(first));
        assert!(epochs.finish(first));
        assert!(!epochs.finish(first), "a stopped epoch cannot finish twice");
        assert_eq!(epochs.live_epoch(), None);
        assert!(epochs.finished(first));
        assert!(epochs.request_is_pending(unminted));
    }

    #[test]
    fn vacating_the_pending_target_settles_it_and_advances_the_next_mint() {
        let mut epochs = ScopeEpochs::default();
        let first = epochs.request_target().epoch;
        let second = first.successor();
        assert!(!epochs.vacate(second), "only the next epoch can be vacated");
        assert!(epochs.vacate(first));
        assert!(epochs.finished(first), "a vacated target reads as settled");
        assert!(!epochs.request_is_pending(first));
        assert_eq!(
            epochs.request_target(),
            RequestTarget {
                epoch: second,
                pending_incarnation: true,
            }
        );
        assert!(!epochs.vacate(first), "a vacated epoch cannot vacate twice");

        let live = epochs.begin().expect("the plane stays idle after a vacate");
        assert_eq!(live, second, "the next mint skips the vacated epoch");
        assert!(!epochs.finished(live));
        assert!(
            !epochs.vacate(live.successor()),
            "a live plane has no pending target to vacate"
        );
        assert_eq!(epochs.live_epoch(), Some(live));
    }
}
