//! Read-only cancellation capabilities shared by supervised children and work.

use crate::runtime::{self, Latch};

/// A library-owned cancellation token.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    primary: Latch,
    secondary: Option<Latch>,
}

impl CancellationToken {
    pub(crate) fn from_latch(latch: Latch) -> Self {
        Self {
            primary: latch,
            secondary: None,
        }
    }

    /// Reports whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.primary.is_fired() || self.secondary.as_ref().is_some_and(Latch::is_fired)
    }

    /// Waits until cancellation is requested.
    pub async fn cancelled(&self) {
        if let Some(secondary) = &self.secondary {
            let _ = runtime::select_two(self.primary.fired(), secondary.fired()).await;
        } else {
            self.primary.fired().await;
        }
    }
}

/// Internal capability that alone may derive a locally cancellable token.
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct ParentCancellationToken {
    primary: Latch,
}

impl ParentCancellationToken {
    pub fn from_latch(primary: Latch) -> Self {
        Self { primary }
    }

    pub fn token(&self) -> CancellationToken {
        CancellationToken::from_latch(self.primary.clone())
    }

    pub fn child(&self, cancellation: Latch) -> CancellationToken {
        CancellationToken {
            primary: self.primary.clone(),
            secondary: Some(cancellation),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.primary.is_fired()
    }

    pub async fn cancelled(&self) {
        self.primary.fired().await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::runtime::{self, JoinOutcome, Latch, Timeout};

    use super::ParentCancellationToken;

    #[crate::runtime::test]
    async fn local_cancellation_cancels_only_the_derived_token() {
        let primary = Latch::default();
        let local = Latch::default();
        let supervisor = ParentCancellationToken::from_latch(primary.clone());
        let operation = supervisor.child(local.clone());

        assert!(local.fire());
        assert!(matches!(
            runtime::timeout(Duration::from_secs(1), operation.cancelled()).await,
            Timeout::Completed(())
        ));
        assert!(operation.is_cancelled());
        assert!(!supervisor.is_cancelled());
        assert!(!primary.is_fired());
    }

    #[crate::runtime::test]
    async fn supervisor_cancellation_cancels_the_derived_token() {
        let primary = Latch::default();
        let local = Latch::default();
        let supervisor = ParentCancellationToken::from_latch(primary.clone());
        let operation = supervisor.child(local.clone());

        assert!(primary.fire());
        assert!(matches!(
            runtime::timeout(Duration::from_secs(1), operation.cancelled()).await,
            Timeout::Completed(())
        ));
        assert!(supervisor.is_cancelled());
        assert!(operation.is_cancelled());
        assert!(!local.is_fired());
    }

    #[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
    async fn simultaneous_supervisor_and_local_cancellation_wake_the_operation() {
        for _ in 0..128 {
            let primary = Latch::default();
            let local = Latch::default();
            let operation =
                ParentCancellationToken::from_latch(primary.clone()).child(local.clone());
            let waiter = runtime::spawn(async move {
                operation.cancelled().await;
            });

            let primary_firer = runtime::spawn(async move {
                runtime::yield_now().await;
                primary.fire()
            });
            let local_firer = runtime::spawn(async move {
                runtime::yield_now().await;
                local.fire()
            });

            assert!(matches!(
                runtime::join(primary_firer).await,
                JoinOutcome::Ok { value: true }
            ));
            assert!(matches!(
                runtime::join(local_firer).await,
                JoinOutcome::Ok { value: true }
            ));
            assert!(matches!(
                runtime::timeout(Duration::from_secs(1), runtime::join(waiter)).await,
                Timeout::Completed(JoinOutcome::Ok { value: () })
            ));
        }
    }
}
