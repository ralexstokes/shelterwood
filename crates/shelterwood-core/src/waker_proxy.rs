use std::{
    mem,
    sync::{Arc, Mutex},
    task::{Context, Wake, Waker},
};

use crate::{
    panic::PanicAccumulator,
    waker::{WakerAction, WakerEffects, WakerSlot},
};

/// Reusable probe, registration, and proxy-poll state machine.
///
/// The first poll uses a framework no-op waker, preserving the already-ready
/// fast path without cloning a caller waker. A pending result installs the
/// stable proxy, registers the real caller behind it, and immediately polls
/// again so the external primitive never retains a raw caller waker.
///
/// Ready retirement is uniform and part of every poll: a ready result may own
/// a user value (or a panic payload that owns one), so the stored caller
/// clone is destroyed synchronously with its panic contained and discarded,
/// subordinate to returning that result intact (#398 ruling 3; the mailbox
/// `DisposingReceiver::retire_reply_waker` documents the two costs riding on
/// the discard). Only *pending* retirement — cancellation and drop glue —
/// remains venue-specific: mailbox users call [`Self::retire`] with an
/// effects sink; `shelterwood-runtime` calls [`Self::retire_with`] through
/// its wrapper after selecting inline or detached destruction.
#[doc(hidden)]
pub struct ProxiedPoll {
    proxy: Option<WakerProxy>,
}

impl ProxiedPoll {
    #[doc(hidden)]
    pub fn new() -> Self {
        Self { proxy: None }
    }

    /// Polls `target` without ever parking the caller's raw waker in it, and
    /// retires the caller registration before returning a ready result.
    #[doc(hidden)]
    pub fn poll<T, R>(
        &mut self,
        target: &mut T,
        context: &mut Context<'_>,
        mut poll: impl FnMut(&mut T, &mut Context<'_>) -> R,
        is_pending: impl Fn(&R) -> bool,
    ) -> R {
        let result = self.poll_proxied(target, context, &mut poll, &is_pending);
        if !is_pending(&result) {
            self.retire_ready();
        }
        result
    }

    /// The probe / install / register / re-poll core, with no retirement.
    fn poll_proxied<T, R>(
        &mut self,
        target: &mut T,
        context: &mut Context<'_>,
        poll: &mut impl FnMut(&mut T, &mut Context<'_>) -> R,
        is_pending: &impl Fn(&R) -> bool,
    ) -> R {
        if self.proxy.is_none() {
            let mut probe = Context::from_waker(Waker::noop());
            let result = poll(target, &mut probe);
            if !is_pending(&result) {
                return result;
            }
            self.proxy = Some(WakerProxy::new());
        }

        let proxy = self
            .proxy
            .as_ref()
            .expect("a pending proxied poll retains its waker proxy");
        proxy.register(context.waker());
        let mut proxy_context = Context::from_waker(proxy.waker());
        poll(target, &mut proxy_context)
    }

    /// Synchronous contained retirement for the ready edge: the stored caller
    /// clone drops through the effects path with no proxy mutex held, and a
    /// hostile destructor panic is discarded rather than raised over the
    /// result the caller is owed.
    fn retire_ready(&mut self) {
        let mut effects = WakerEffects::default();
        self.retire(WakerAction::DropInline, &mut effects);
        let mut panics = PanicAccumulator::default();
        effects.flush(&mut panics);
        crate::panic::discard_panic(panics.take());
    }

    /// Test-only visibility: whether a pending poll has a proxy installed.
    #[cfg(test)]
    pub(crate) fn is_parked(&self) -> bool {
        self.proxy.is_some()
    }

    /// Retires the current caller registration into a mailbox effects sink.
    #[doc(hidden)]
    pub fn retire(&mut self, action: WakerAction, effects: &mut WakerEffects) {
        let proxy = self.proxy.take();
        if let Some(proxy) = &proxy {
            proxy.retire(action, effects);
        }
        drop(proxy);
    }

    /// Retires the current caller registration through a cross-crate effect.
    #[doc(hidden)]
    pub fn retire_with(&mut self, effect: fn(Waker), panics: &mut PanicAccumulator) {
        let proxy = self.proxy.take();
        if let Some(proxy) = &proxy {
            proxy.retire_with(effect, panics);
        }
        panics.run(|| drop(proxy));
    }
}

/// Stable framework-owned waker registered with an external primitive.
///
/// The external primitive sees only `proxy`, whose clone and drop vtable is
/// `Arc` bookkeeping over framework-owned state. The caller's real waker stays
/// in the private slot and every path that removes it queues the resulting
/// user-code effect before releasing the slot mutex.
///
/// The proxy mutex is a **leaf**: [`Wake::wake_by_ref`] takes it from whatever
/// thread drives the external primitive, so nothing this type does under it may
/// take another framework lock.
#[doc(hidden)]
pub struct WakerProxy {
    proxy: Waker,
    state: Arc<WakerProxyState>,
}

/// The proxy's mutable half: the caller's waker beside the record of a wake
/// that has not yet reached the caller who is polling now.
#[derive(Default)]
struct Registration {
    caller: WakerSlot,
    woken: bool,
}

#[derive(Default)]
struct WakerProxyState {
    caller: Mutex<Registration>,
}

impl WakerProxy {
    #[doc(hidden)]
    pub fn new() -> Self {
        let state = Arc::new(WakerProxyState::default());
        let proxy = Waker::from(Arc::clone(&state));
        Self { proxy, state }
    }

    /// Installs the current caller without cloning or dropping its waker
    /// while the proxy mutex is held.
    ///
    /// # The lost-wake handshake
    ///
    /// Cloning `current` dispatches a caller-owned vtable, so it has to happen
    /// between two critical sections. A wake landing in that window finds
    /// either an empty slot or the *previous* poll's waker, and once a future
    /// has migrated between tasks that previous waker no longer wakes the task
    /// polling now. Installing the fresh waker on top of that would leave no
    /// record the wake ever happened, and the future would never be polled
    /// again.
    ///
    /// So [`Wake::wake_by_ref`] records `woken` in the same critical section in
    /// which it takes the slot, and every registration that installs a waker
    /// reads-and-clears that flag: a set flag takes the just-installed waker
    /// straight back out and hands it to the effects sink, which wakes it after
    /// unlock. No waker vtable is invoked or cloned under the mutex, and no
    /// wake is lost. The cost is a spurious re-poll, which the `Future`
    /// contract permits — a lost wake is not.
    ///
    /// The `will_wake` short-circuit rides the same read-and-clear rather than
    /// returning early. It cannot in fact observe a set flag: `wake_by_ref`
    /// empties the slot in the critical section that sets the flag, so a slot
    /// still holding a matching waker proves nothing has woken since that waker
    /// was installed. Handling it anyway costs one branch and keeps the "no
    /// wake is lost" claim from resting on that invariant.
    #[doc(hidden)]
    pub fn register(&self, current: &Waker) {
        let mut replacement = None;
        loop {
            // Effects precede the guard, so a bookkeeping unwind still
            // releases the mutex before a displaced RawWaker vtable runs.
            let mut effects = WakerEffects::default();
            let needs_clone = {
                let mut registration = self
                    .state
                    .caller
                    .lock()
                    .expect("waker proxy mutex poisoned");
                let installed = if registration.caller.will_wake(current) {
                    true
                } else if let Some(replacement) = replacement.take() {
                    registration.caller.replace(replacement, &mut effects);
                    true
                } else {
                    // Nothing was installed, so the flag stays set for the
                    // registration that eventually succeeds.
                    false
                };
                if installed && mem::take(&mut registration.woken) {
                    registration.caller.take(WakerAction::Wake, &mut effects);
                }
                !installed
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

    #[doc(hidden)]
    pub fn waker(&self) -> &Waker {
        &self.proxy
    }

    /// Retires the caller registration through a framework-selected effect.
    ///
    /// The function pointer is queued while the proxy mutex is held and is
    /// invoked only after unlock. This is the cross-crate adapter seam used by
    /// `shelterwood-runtime`: the runtime owns the detached-disposal venue,
    /// while this runtime-neutral crate owns the slot that makes it impossible
    /// to remove a caller waker without first choosing that post-unlock venue.
    #[doc(hidden)]
    pub fn retire_with(&self, effect: fn(Waker), panics: &mut PanicAccumulator) {
        let mut effects = WakerEffects::default();
        self.retire(WakerAction::Run(effect), &mut effects);
        effects.flush(panics);
    }

    /// Moves the caller waker into an explicitly chosen post-unlock effect,
    /// discarding any unconsumed wake record with it.
    ///
    /// `retire` tears the registration down rather than replacing it: its
    /// callers have either observed the readiness the wake would have announced
    /// or are abandoning the registration outright, so there is no later poll
    /// for a retained flag to reach. Keeping the flag would only mean the next
    /// caller to register on a reused proxy is woken once for an event that
    /// predates it.
    pub(crate) fn retire(&self, action: WakerAction, effects: &mut WakerEffects) {
        let mut registration = self
            .state
            .caller
            .lock()
            .expect("waker proxy mutex poisoned");
        registration.woken = false;
        registration.caller.take(action, effects);
    }
}

impl Drop for WakerProxy {
    /// Retires the stored caller waker **inline, on the dropping thread**: drop
    /// glue has no effects sink to hand the disposition to, so the waker's drop
    /// vtable runs here (contained, but synchronously).
    ///
    /// A caller whose retirement must reach the disposal lane — because the
    /// waker's destructor may block, or because the dropping thread holds a
    /// lock the destructor could re-enter — must call [`WakerProxy::retire`]
    /// with the chosen [`WakerAction`] before the proxy is dropped. Reaching
    /// this `Drop` with a waker still installed is the fallback, not the
    /// contract.
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
            let mut registration = self.caller.lock().expect("waker proxy mutex poisoned");
            // Recorded whether or not a caller waker is installed: an empty
            // slot means the wake landed in `register`'s clone window, and the
            // flag is the only thing that will carry it to the caller polling
            // now. See `WakerProxy::register`.
            registration.woken = true;
            registration.caller.take(WakerAction::Wake, &mut effects);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        mem::ManuallyDrop,
        sync::{
            Arc, Weak,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{RawWaker, RawWakerVTable, Wake, Waker},
    };

    use super::{WakerProxy, WakerProxyState};
    use crate::waker::{WakerAction, WakerEffects};

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

    struct ReentrantClone {
        proxy: Weak<WakerProxyState>,
        clones: Arc<AtomicUsize>,
    }

    unsafe fn clone_reentrant(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<ReentrantClone>::from_raw(data.cast()) });
        let state = probe.proxy.upgrade().expect("the proxy remains live");
        let _guard = state
            .caller
            .try_lock()
            .expect("a caller waker clones after the proxy mutex is released");
        probe.clones.fetch_add(1, Ordering::SeqCst);
        RawWaker::new(
            Arc::into_raw(Arc::clone(&probe)).cast(),
            &REENTRANT_CLONE_VTABLE,
        )
    }

    unsafe fn wake_reentrant(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<ReentrantClone>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_reentrant(_data: *const ()) {}

    unsafe fn drop_reentrant(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<ReentrantClone>::from_raw(data.cast()) });
    }

    static REENTRANT_CLONE_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_reentrant,
        wake_reentrant,
        wake_by_ref_reentrant,
        drop_reentrant,
    );

    fn reentrant_clone_waker(proxy: Weak<WakerProxyState>, clones: Arc<AtomicUsize>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(ReentrantClone { proxy, clones })).cast(),
            &REENTRANT_CLONE_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    /// A caller waker that wakes the proxy from inside its own `clone` vtable.
    ///
    /// `register` clones outside the proxy mutex, so this is a single-threaded
    /// replica of a driver thread waking between `register`'s two critical
    /// sections — the window the `woken` handshake exists to close.
    struct WindowWake {
        proxy: Weak<WakerProxyState>,
        armed: AtomicBool,
        wakes: Arc<AtomicUsize>,
    }

    unsafe fn clone_window(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<WindowWake>::from_raw(data.cast()) });
        if probe.armed.swap(false, Ordering::SeqCst) {
            let state = probe.proxy.upgrade().expect("the proxy remains live");
            state.wake_by_ref();
        }
        RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &WINDOW_VTABLE)
    }

    unsafe fn wake_window(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        let waker = unsafe { Arc::<WindowWake>::from_raw(data.cast()) };
        waker.wakes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn wake_by_ref_window(data: *const ()) {
        // SAFETY: wake_by_ref borrows the Arc reference represented by this
        // waker, which ManuallyDrop preserves.
        let probe = ManuallyDrop::new(unsafe { Arc::<WindowWake>::from_raw(data.cast()) });
        probe.wakes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn drop_window(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<WindowWake>::from_raw(data.cast()) });
    }

    static WINDOW_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_window, wake_window, wake_by_ref_window, drop_window);

    fn window_wake_waker(proxy: Weak<WakerProxyState>, wakes: Arc<AtomicUsize>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(WindowWake {
                proxy,
                armed: AtomicBool::new(true),
                wakes,
            }))
            .cast(),
            &WINDOW_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    #[test]
    fn registration_keeps_a_stable_proxy_identity_across_replacements() {
        let proxy = WakerProxy::new();
        // Minted before any registration: the identity an external primitive's
        // own `will_wake` short-circuit keys on has to survive every later
        // caller swap, which is the whole point of a stable proxy.
        let minted = proxy.waker().clone();

        let first = Arc::new(CountWake::default());
        let second = Arc::new(CountWake::default());
        proxy.register(&Waker::from(first));
        proxy.register(&Waker::from(second));

        assert!(minted.will_wake(proxy.waker()));
    }

    #[test]
    fn re_registering_the_same_caller_never_reaches_its_clone_vtable() {
        let proxy = WakerProxy::new();
        let clones = Arc::new(AtomicUsize::new(0));
        let caller = reentrant_clone_waker(Arc::downgrade(&proxy.state), Arc::clone(&clones));

        proxy.register(&caller);
        proxy.register(&caller);

        // One clone for the initial install and none for the repeat: the
        // `will_wake` short-circuit is what keeps the caller's vtable out of
        // the second registration.
        assert_eq!(clones.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_wake_in_the_registration_window_reaches_the_caller_registering_now() {
        let proxy = WakerProxy::new();
        let wakes = Arc::new(AtomicUsize::new(0));
        let caller = window_wake_waker(Arc::downgrade(&proxy.state), Arc::clone(&wakes));

        // The wake fires while the slot is still empty, so the `woken` record
        // is the only thing that can carry it forward.
        proxy.register(&caller);

        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "a wake landing in the clone window must reach the waker that same registration installs"
        );
    }

    #[test]
    fn a_window_wake_consuming_the_previous_caller_still_reaches_the_new_one() {
        let proxy = WakerProxy::new();
        let previous = Arc::new(CountWake::default());
        proxy.register(&Waker::from(Arc::clone(&previous)));

        let wakes = Arc::new(AtomicUsize::new(0));
        let caller = window_wake_waker(Arc::downgrade(&proxy.state), Arc::clone(&wakes));
        proxy.register(&caller);

        // The window wake took the previous poll's waker, which after a future
        // migrates between tasks belongs to a task that no longer holds it...
        assert_eq!(previous.0.load(Ordering::SeqCst), 1);
        // ...so the caller polling now has to be woken as well.
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "waking the previous caller does not discharge the wake for the current one"
        );
    }

    #[test]
    fn retirement_discards_an_unconsumed_wake_record() {
        let proxy = WakerProxy::new();
        proxy.waker().wake_by_ref();

        let mut effects = WakerEffects::default();
        proxy.retire(WakerAction::DropInline, &mut effects);
        drop(effects);

        let target = Arc::new(CountWake::default());
        proxy.register(&Waker::from(Arc::clone(&target)));

        assert_eq!(
            target.0.load(Ordering::SeqCst),
            0,
            "a retired registration leaves no wake for the next caller to inherit"
        );
    }

    #[test]
    fn registration_clones_the_caller_after_unlock() {
        let proxy = WakerProxy::new();
        let clones = Arc::new(AtomicUsize::new(0));
        let caller = reentrant_clone_waker(Arc::downgrade(&proxy.state), Arc::clone(&clones));

        proxy.register(&caller);
        proxy.register(&caller);

        assert_eq!(clones.load(Ordering::SeqCst), 1);
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

    #[test]
    fn proxy_drop_retires_the_caller_after_unlock() {
        let proxy = WakerProxy::new();
        let caller = Waker::from(Arc::new(ReentrantDrop(Arc::downgrade(&proxy.state))));
        proxy.register(&caller);
        drop(caller);

        drop(proxy);
    }
}
