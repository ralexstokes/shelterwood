use std::{
    fmt,
    sync::{Mutex, atomic::Ordering},
    task::{Context, Poll, Waker},
};

use shelterwood_core::identity::AtomicMonotonicCounter;

use super::PanicAccumulator;

/// A caller-waker registry whose lock protects only inert storage changes.
///
/// Registration clones happen before entering the registry. Removal and
/// draining only move wakers out; their vtables run after unlock, one behind
/// each accumulator boundary. A waiter's identity is a plain id minted by
/// its registry.
///
/// Cancellation destroys a removed caller-waker clone inline on the thread
/// dropping `LatchWait` or `WatchWait`: unlike the external-primitive proxy
/// family, the registry owns the waker directly and has no foreign drop seam
/// that requires detached retirement. A slow caller-waker destructor stalls
/// the abandoning waiter alone, and a hostile one is contained by the
/// accumulator above.
#[derive(Default)]
pub(crate) struct WaiterRegistry {
    waiters: Mutex<Vec<RegisteredWaker>>,
    identities: AtomicMonotonicCounter,
}

pub(crate) struct RegisteredWaker {
    identity: u64,
    waker: Waker,
}

impl WaiterRegistry {
    pub(crate) fn mint_identity(&self) -> u64 {
        self.identities.mint(Ordering::Relaxed, Ordering::Relaxed)
    }

    fn register(&self, identity: u64, waker: Waker) -> Option<RegisteredWaker> {
        let mut waiters = self
            .waiters
            .lock()
            .expect("waiter registry lock is never held across caller code");
        if let Some(index) = waiters
            .iter()
            .position(|waiter| waiter.identity == identity)
        {
            Some(std::mem::replace(
                &mut waiters[index],
                RegisteredWaker { identity, waker },
            ))
        } else {
            waiters.push(RegisteredWaker { identity, waker });
            None
        }
    }

    pub(crate) fn remove(&self, identity: u64) -> Option<RegisteredWaker> {
        let mut waiters = self
            .waiters
            .lock()
            .expect("waiter registry lock is never held across caller code");
        waiters
            .iter()
            .position(|waiter| waiter.identity == identity)
            .map(|index| waiters.swap_remove(index))
    }

    pub(crate) fn wake_all(&self) {
        let waiters = {
            let mut waiters = self
                .waiters
                .lock()
                .expect("waiter registry lock is never held across caller code");
            std::mem::take(&mut *waiters)
        };
        let mut panics = PanicAccumulator::default();
        for RegisteredWaker { waker, .. } in waiters {
            panics.run(|| waker.wake());
        }
    }

    pub(crate) fn drop_registered(waiters: impl IntoIterator<Item = Option<RegisteredWaker>>) {
        let mut panics = PanicAccumulator::default();
        for waiter in waiters.into_iter().flatten() {
            panics.run(|| drop(waiter));
        }
    }

    /// Polls one register/recheck waiter protocol around `ready`.
    ///
    /// Caller waker cloning happens before the registry lock is acquired. A
    /// displaced registration and the registration removed after a successful
    /// recheck are both destroyed only after that recheck completes, so a
    /// hostile waker destructor cannot strand an already-published outcome.
    pub(crate) fn poll_registered<T>(
        &self,
        identity: u64,
        context: &Context<'_>,
        mut ready: impl FnMut() -> Option<T>,
    ) -> Poll<T> {
        if let Some(output) = ready() {
            let registered = self.remove(identity);
            Self::drop_registered([registered]);
            return Poll::Ready(output);
        }

        // Caller code runs before the registry lock is acquired.
        let waker = context.waker().clone();
        let displaced = self.register(identity, waker);
        let output = ready();
        let registered = output.is_some().then(|| self.remove(identity)).flatten();
        // Complete the publication recheck before a hostile displaced waker
        // destructor is allowed to resume its panic.
        Self::drop_registered([displaced, registered]);
        output.map_or(Poll::Pending, Poll::Ready)
    }

    pub(crate) fn len(&self) -> usize {
        self.waiters
            .lock()
            .expect("waiter registry lock is never held across caller code")
            .len()
    }
}

impl fmt::Debug for WaiterRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WaiterRegistry")
            .field("waiters", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::{RegisteredWaker, WaiterRegistry};
    use crate::test_wakers::{assert_panic_message, last_drop_panics_waker};

    #[test]
    fn registered_waker_cleanup_keeps_the_first_panic_and_attempts_every_drop() {
        const FIRST: &str = "first registered waker drop panic";
        const SECOND: &str = "second registered waker drop panic";

        let first_drops = Arc::new(AtomicUsize::new(0));
        let second_drops = Arc::new(AtomicUsize::new(0));
        let first = RegisteredWaker {
            identity: 1,
            waker: last_drop_panics_waker(FIRST, Arc::clone(&first_drops)),
        };
        let second = RegisteredWaker {
            identity: 2,
            waker: last_drop_panics_waker(SECOND, Arc::clone(&second_drops)),
        };

        let payload = catch_unwind(AssertUnwindSafe(|| {
            WaiterRegistry::drop_registered([Some(first), Some(second)]);
        }))
        .expect_err("registered-waker cleanup resumes its first panic");

        assert_panic_message(&*payload, FIRST);
        assert_eq!(first_drops.load(Ordering::SeqCst), 1);
        assert_eq!(second_drops.load(Ordering::SeqCst), 1);
    }
}
