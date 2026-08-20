//! Shared private machinery for declaration definitions.

use crate::policy::ChildMode;

/// Repeatable or consuming definition payload state.
///
/// The explicit `Spent` state lets consumers move a one-shot payload without
/// wrapping that payload in a second `Option`. Repeatable payloads remain
/// borrowed in place and therefore survive every construction attempt.
pub(crate) enum DefinitionSource<R, O> {
    Restartable(R),
    OneShot(O),
    Spent,
}

impl<R, O> DefinitionSource<R, O> {
    pub(crate) fn is_one_shot(&self) -> bool {
        matches!(self, Self::OneShot(_) | Self::Spent)
    }

    pub(crate) fn mode(&self) -> ChildMode {
        if self.is_one_shot() {
            ChildMode::OneShot
        } else {
            ChildMode::Restartable
        }
    }

    pub(crate) fn restartable(&self) -> Option<&R> {
        match self {
            Self::Restartable(source) => Some(source),
            Self::OneShot(_) | Self::Spent => None,
        }
    }

    pub(crate) fn take_one_shot(&mut self) -> Option<O> {
        match std::mem::replace(self, Self::Spent) {
            Self::OneShot(source) => Some(source),
            Self::Restartable(source) => {
                *self = Self::Restartable(source);
                None
            }
            Self::Spent => None,
        }
    }
}

/// Generates the option setters shared by the public definition families.
macro_rules! common_options_setters {
    ($($setter:ident),* $(,)?) => {
        $(common_options_setters!(@emit $setter);)*
    };

    (@emit restart) => {
        /// Overrides the restart policy.
        #[must_use]
        pub fn restart(mut self, restart: RestartPolicy) -> Self {
            self.options.restart = Some(restart);
            self
        }
    };

    (@emit shutdown) => {
        /// Overrides the shutdown policy.
        #[must_use]
        pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
            self.options.shutdown = Some(shutdown);
            self
        }
    };

    (@emit mailbox) => {
        /// Overrides the actor mailbox kind and capacity.
        #[must_use]
        pub fn mailbox(mut self, mailbox: Mailbox) -> Self {
            self.options.mailbox = Some(mailbox);
            self
        }
    };

    (@emit mailbox_shutdown) => {
        /// Overrides frozen-prefix drain versus discard behavior.
        #[must_use]
        pub fn mailbox_shutdown(mut self, shutdown: MailboxShutdown) -> Self {
            self.options.mailbox_shutdown = Some(shutdown);
            self
        }
    };

    (@emit actor_readiness) => {
        /// Overrides the actor's declared readiness mode.
        #[must_use]
        pub fn readiness(mut self, readiness: Readiness) -> Self {
            self.options.readiness = Some(readiness);
            self
        }
    };

    (@emit raw_readiness) => {
        common_options_setters!(@readiness "Overrides the actor's declared readiness mode.");
    };

    (@emit task_readiness) => {
        common_options_setters!(@readiness "Overrides task readiness (`Immediate` or `Manual`).");
    };

    (@readiness $doc:literal) => {
        #[doc = $doc]
        pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
            if readiness == Readiness::AfterInit {
                return Err(PolicyError::UnsupportedReadiness);
            }
            self.options.readiness = Some(readiness);
            Ok(self)
        }
    };

    (@emit structural_readiness_deadline) => {
        common_options_setters!(@readiness_deadline "Overrides the structural readiness deadline.");
    };

    (@emit task_readiness_deadline) => {
        common_options_setters!(@readiness_deadline "Overrides the readiness deadline.");
    };

    (@readiness_deadline $doc:literal) => {
        #[doc = $doc]
        #[must_use]
        pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
            self.options.readiness_deadline = deadline;
            self
        }
    };

    (@emit retention) => {
        /// Overrides terminal-membership retention.
        #[must_use]
        pub fn retention(mut self, retention: Retention) -> Self {
            self.options.retention = Some(retention);
            self
        }
    };
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::policy::ChildMode;

    use super::DefinitionSource;

    #[test]
    fn restartable_source_remains_available() {
        let mut source = DefinitionSource::<_, ()>::Restartable(String::from("factory"));

        assert!(!source.is_one_shot());
        assert_eq!(source.mode(), ChildMode::Restartable);
        assert_eq!(source.restartable().map(String::as_str), Some("factory"));
        assert_eq!(source.take_one_shot(), None);
        assert_eq!(source.restartable().map(String::as_str), Some("factory"));
    }

    #[test]
    fn one_shot_source_transitions_directly_to_spent() {
        let mut source = DefinitionSource::<(), _>::OneShot(String::from("body"));

        assert!(source.is_one_shot());
        assert_eq!(source.mode(), ChildMode::OneShot);
        assert_eq!(source.take_one_shot().as_deref(), Some("body"));
        assert!(source.is_one_shot());
        assert!(source.take_one_shot().is_none());
        assert!(matches!(source, DefinitionSource::Spent));
        assert_eq!(source.mode(), ChildMode::OneShot);
    }

    #[test]
    fn source_keeps_exactly_one_owner_across_consumption() {
        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let mut source = DefinitionSource::<(), _>::OneShot(DropProbe(Arc::clone(&drops)));
        let payload = source
            .take_one_shot()
            .expect("payload is initially available");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(payload);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
