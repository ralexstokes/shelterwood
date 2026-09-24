//! The only boundary between the library and its async runtime.
//!
//! This module is the build's runtime selection. Every façade layer — driver,
//! mailbox, reply channels, deadline futures, raw actors — reaches the adapter
//! through it by static dispatch; nothing installs a runtime per object.
//! Replacing the executor means supplying an adapter crate with the same
//! module surface and selecting it here.

pub(crate) use shelterwood_runtime::*;

// Test builds shadow three adapter items with thread-local test hooks; an
// explicit import takes precedence over the glob above. With no hook
// installed each wrapper is a plain delegation.
#[cfg(test)]
pub(crate) use hooks::{Signal, dispose_detached, now};

/// Thread-local clock, disposal, and pulse hooks for this crate's own tests.
///
/// A hook sees only calls made on the thread that installed it, so a test
/// using one must drive the hooked effects synchronously on its own thread.
/// Each installer returns a guard that removes the hook when dropped.
#[cfg(test)]
pub(crate) mod hooks {
    use std::{cell::RefCell, time::Instant};

    type NowHook = Box<dyn Fn() -> Instant>;
    type RecordHook = Box<dyn Fn()>;

    thread_local! {
        static NOW: RefCell<Option<NowHook>> = const { RefCell::new(None) };
        static DISPOSAL: RefCell<Option<RecordHook>> = const { RefCell::new(None) };
        static PULSE: RefCell<Option<RecordHook>> = const { RefCell::new(None) };
    }

    fn record(hook: &'static std::thread::LocalKey<RefCell<Option<RecordHook>>>) {
        hook.with_borrow(|hook| {
            if let Some(record) = hook {
                record();
            }
        });
    }

    /// The adapter clock, unless this thread installed an override.
    pub(crate) fn now() -> Instant {
        NOW.with_borrow(|hook| hook.as_ref().map(|now| now()))
            .unwrap_or_else(shelterwood_runtime::now)
    }

    /// The adapter's detached disposal, reported first to this thread's
    /// recorder when one is installed.
    pub(crate) fn dispose_detached<T: Send + 'static>(value: T) {
        record(&DISPOSAL);
        shelterwood_runtime::dispose_detached(value);
    }

    /// The adapter's change signal, whose pulses are reported first to this
    /// thread's recorder when one is installed.
    #[derive(Clone, Debug, Default)]
    pub(crate) struct Signal(shelterwood_runtime::Signal);

    impl Signal {
        pub(crate) fn pulse(&self) {
            record(&PULSE);
            self.0.pulse();
        }

        pub(crate) fn watcher(&self) -> shelterwood_runtime::SignalWatcher {
            self.0.watcher()
        }
    }

    /// Removes its hook when dropped.
    #[must_use = "the hook is removed when the guard drops"]
    pub(crate) struct HookGuard(fn());

    impl Drop for HookGuard {
        fn drop(&mut self) {
            (self.0)();
        }
    }

    /// Overrides [`now`] on this thread until the guard drops.
    pub(crate) fn override_now(now: impl Fn() -> Instant + 'static) -> HookGuard {
        NOW.set(Some(Box::new(now)));
        HookGuard(|| NOW.set(None))
    }

    /// Reports every [`dispose_detached`] submission on this thread until the
    /// guard drops.
    pub(crate) fn record_disposals(record: impl Fn() + 'static) -> HookGuard {
        DISPOSAL.set(Some(Box::new(record)));
        HookGuard(|| DISPOSAL.set(None))
    }

    /// Reports every [`Signal::pulse`] on this thread until the guard drops.
    pub(crate) fn record_pulses(record: impl Fn() + 'static) -> HookGuard {
        PULSE.set(Some(Box::new(record)));
        HookGuard(|| PULSE.set(None))
    }
}
