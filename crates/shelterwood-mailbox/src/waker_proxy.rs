use std::{
    sync::{Arc, Mutex},
    task::{Wake, Waker},
};

use crate::cell::waker_slot::{WakerAction, WakerEffects, WakerSlot};

/// Stable framework-owned waker registered with an external primitive.
///
/// The external primitive sees only `proxy`, whose clone and drop vtable is
/// `Arc` bookkeeping over framework-owned state. The caller's real waker stays
/// in the private slot and every path that removes it queues the resulting
/// user-code effect before releasing the slot mutex.
pub(crate) struct WakerProxy {
    proxy: Waker,
    state: Arc<WakerProxyState>,
}

#[derive(Default)]
struct WakerProxyState {
    caller: Mutex<WakerSlot>,
}

impl WakerProxy {
    pub(crate) fn new() -> Self {
        let state = Arc::new(WakerProxyState::default());
        let proxy = Waker::from(Arc::clone(&state));
        Self { proxy, state }
    }

    /// Installs the current caller without cloning or dropping its waker
    /// while the proxy mutex is held.
    pub(crate) fn register(&self, current: &Waker) {
        let mut replacement = None;
        loop {
            // Effects precede the guard, so a bookkeeping unwind still
            // releases the mutex before a displaced RawWaker vtable runs.
            let mut effects = WakerEffects::default();
            let needs_clone = {
                let mut caller = self
                    .state
                    .caller
                    .lock()
                    .expect("waker proxy mutex poisoned");
                if caller.will_wake(current) {
                    false
                } else if let Some(replacement) = replacement.take() {
                    caller.replace(replacement, &mut effects);
                    false
                } else {
                    true
                }
            };
            drop(effects);

            if !needs_clone {
                return;
            }
            // `Waker::clone` dispatches through a caller-owned RawWaker
            // vtable, so it belongs outside the proxy mutex. Re-check after
            // cloning because a concurrent wake can empty the slot between
            // the two critical sections.
            replacement = Some(current.clone());
        }
    }

    pub(crate) fn waker(&self) -> &Waker {
        &self.proxy
    }

    /// Moves the caller waker into an explicitly chosen post-unlock effect.
    pub(crate) fn retire(&self, action: WakerAction, effects: &mut WakerEffects) {
        let mut caller = self
            .state
            .caller
            .lock()
            .expect("waker proxy mutex poisoned");
        caller.take(action, effects);
    }
}

impl Drop for WakerProxy {
    fn drop(&mut self) {
        let mut effects = WakerEffects::default();
        self.retire(WakerAction::DropInline, &mut effects);
    }
}

impl Wake for WakerProxyState {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut effects = WakerEffects::default();
        {
            let mut caller = self.caller.lock().expect("waker proxy mutex poisoned");
            caller.take(WakerAction::Wake, &mut effects);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Weak,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
    };

    use super::{WakerProxy, WakerProxyState};

    #[derive(Default)]
    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReentrantWake {
        proxy: Weak<WakerProxyState>,
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            let state = self.proxy.upgrade().expect("the proxy remains live");
            let _guard = state
                .caller
                .try_lock()
                .expect("a forwarded wake runs after the proxy mutex is released");
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReentrantDrop(Weak<WakerProxyState>);

    impl Wake for ReentrantDrop {
        fn wake(self: Arc<Self>) {
            panic!("the replacement test never wakes its caller")
        }
    }

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            let state = self.0.upgrade().expect("the proxy remains live");
            let _guard = state
                .caller
                .try_lock()
                .expect("a displaced waker drops after the proxy mutex is released");
        }
    }

    #[test]
    fn registration_preserves_the_proxy_identity_and_short_circuits_matching_callers() {
        let proxy = WakerProxy::new();
        let target = Arc::new(CountWake::default());
        let caller = Waker::from(Arc::clone(&target));

        proxy.register(&caller);
        let registered_count = Arc::strong_count(&target);
        proxy.register(&caller);

        assert_eq!(Arc::strong_count(&target), registered_count);
        assert!(proxy.waker().will_wake(proxy.waker()));
    }

    #[test]
    fn forwarding_takes_the_caller_before_invoking_its_wake_vtable() {
        let proxy = WakerProxy::new();
        let wakes = Arc::new(AtomicUsize::new(0));
        let caller = Waker::from(Arc::new(ReentrantWake {
            proxy: Arc::downgrade(&proxy.state),
            wakes: Arc::clone(&wakes),
        }));
        proxy.register(&caller);

        proxy.waker().wake_by_ref();
        proxy.waker().wake_by_ref();

        assert_eq!(wakes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn replacement_drops_the_displaced_caller_after_unlock() {
        let proxy = WakerProxy::new();
        let caller = Waker::from(Arc::new(ReentrantDrop(Arc::downgrade(&proxy.state))));
        proxy.register(&caller);
        drop(caller);

        proxy.register(Waker::noop());
    }
}
