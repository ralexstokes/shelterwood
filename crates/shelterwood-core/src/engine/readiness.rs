use std::time::Instant;

use crate::policy::Readiness;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadinessState {
    Waiting { deadline: Option<Instant> },
    Ready,
    Disarmed,
}

/// The authoritative per-incarnation readiness state machine.
///
/// The shell only applies returned effects (publish ready, arm/cancel a
/// deadline, or begin timeout shutdown); it never assigns readiness state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessGate {
    state: ReadinessState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessEvent {
    Signal,
    Deadline { now: Instant, signal_seen: bool },
    Shutdown,
    Exit { signal_seen: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessEffect {
    BecameReady,
    ArmDeadline { deadline: Instant },
    TimedOut { deadline: Instant },
    Disarmed,
}

impl ReadinessGate {
    /// Configures one incarnation's gate from its definition-level readiness
    /// and returns the configuration's own effect: `Immediate` is ready at
    /// once, and a gated mode arms its deadline, if it has one.
    pub fn configure(
        readiness: Readiness,
        deadline: Option<Instant>,
    ) -> (Self, Option<ReadinessEffect>) {
        match readiness {
            Readiness::Immediate => (
                Self {
                    state: ReadinessState::Ready,
                },
                Some(ReadinessEffect::BecameReady),
            ),
            Readiness::Manual | Readiness::AfterInit => (
                Self {
                    state: ReadinessState::Waiting { deadline },
                },
                deadline.map(|deadline| ReadinessEffect::ArmDeadline { deadline }),
            ),
        }
    }

    /// Whether a retained signal watcher is needed for this incarnation.
    pub fn needs_signal_watch(self) -> bool {
        matches!(self.state, ReadinessState::Waiting { .. })
    }

    pub fn step(&mut self, event: ReadinessEvent) -> Option<ReadinessEffect> {
        match (self.state, event) {
            (ReadinessState::Waiting { .. }, ReadinessEvent::Signal)
            | (
                ReadinessState::Waiting { .. },
                ReadinessEvent::Deadline {
                    signal_seen: true, ..
                },
            )
            | (ReadinessState::Waiting { .. }, ReadinessEvent::Exit { signal_seen: true }) => {
                self.state = ReadinessState::Ready;
                Some(ReadinessEffect::BecameReady)
            }
            (
                ReadinessState::Waiting {
                    deadline: Some(deadline),
                },
                ReadinessEvent::Deadline {
                    now,
                    signal_seen: false,
                },
            ) if now >= deadline => {
                self.state = ReadinessState::Disarmed;
                Some(ReadinessEffect::TimedOut { deadline })
            }
            (
                ReadinessState::Waiting { .. },
                ReadinessEvent::Shutdown | ReadinessEvent::Exit { signal_seen: false },
            ) => {
                self.state = ReadinessState::Disarmed;
                Some(ReadinessEffect::Disarmed)
            }
            (
                ReadinessState::Waiting { .. },
                ReadinessEvent::Deadline {
                    signal_seen: false, ..
                },
            )
            | (ReadinessState::Ready | ReadinessState::Disarmed, _) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{ReadinessEffect, ReadinessEvent, ReadinessGate};
    use crate::policy::Readiness;

    #[test]
    fn readiness_configuration_and_signal_deadline_race_are_engine_owned() {
        let deadline = Instant::now();
        let (mut ready, configured) = ReadinessGate::configure(Readiness::Manual, Some(deadline));
        assert_eq!(configured, Some(ReadinessEffect::ArmDeadline { deadline }));
        assert!(ready.needs_signal_watch());
        assert_eq!(
            ready.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: true,
            }),
            Some(ReadinessEffect::BecameReady)
        );
        assert!(!ready.needs_signal_watch());
        assert_eq!(
            ready.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: false,
            }),
            None
        );

        let (mut exited, configured) = ReadinessGate::configure(Readiness::Manual, None);
        assert_eq!(configured, None);
        assert_eq!(
            exited.step(ReadinessEvent::Exit { signal_seen: true }),
            Some(ReadinessEffect::BecameReady)
        );

        let (mut unsignaled_exit, _) = ReadinessGate::configure(Readiness::Manual, None);
        assert_eq!(
            unsignaled_exit.step(ReadinessEvent::Exit { signal_seen: false }),
            Some(ReadinessEffect::Disarmed)
        );
        assert!(!unsignaled_exit.needs_signal_watch());

        let (mut unbounded, _) = ReadinessGate::configure(Readiness::Manual, None);
        assert_eq!(
            unbounded.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: false,
            }),
            None,
            "a spurious deadline cannot resolve an unbounded readiness wait"
        );
        assert!(unbounded.needs_signal_watch());

        let (mut timed_out, configured) =
            ReadinessGate::configure(Readiness::AfterInit, Some(deadline));
        assert_eq!(configured, Some(ReadinessEffect::ArmDeadline { deadline }));
        assert_eq!(
            timed_out.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: false,
            }),
            Some(ReadinessEffect::TimedOut { deadline })
        );
        assert!(!timed_out.needs_signal_watch());

        let configured_deadline = deadline + Duration::from_secs(2);
        let (mut premature, configured) =
            ReadinessGate::configure(Readiness::Manual, Some(configured_deadline));
        assert_eq!(
            configured,
            Some(ReadinessEffect::ArmDeadline {
                deadline: configured_deadline
            })
        );
        assert_eq!(
            premature.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: false,
            }),
            None,
            "a deadline event before the configured instant cannot time readiness out"
        );
        assert!(premature.needs_signal_watch());
        assert_eq!(
            premature.step(ReadinessEvent::Deadline {
                now: configured_deadline,
                signal_seen: false,
            }),
            Some(ReadinessEffect::TimedOut {
                deadline: configured_deadline
            }),
            "the original deadline remains armed after a premature event"
        );

        let (immediate, configured) = ReadinessGate::configure(Readiness::Immediate, None);
        assert_eq!(configured, Some(ReadinessEffect::BecameReady));
        assert!(!immediate.needs_signal_watch());

        let (mut shutdown, configured) = ReadinessGate::configure(Readiness::Manual, None);
        assert_eq!(configured, None);
        assert_eq!(
            shutdown.step(ReadinessEvent::Shutdown),
            Some(ReadinessEffect::Disarmed)
        );
        assert!(!shutdown.needs_signal_watch());
    }
}
