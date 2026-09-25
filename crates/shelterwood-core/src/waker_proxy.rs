use std::{
    sync::{Arc, Mutex, MutexGuard, PoisonError},
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
/// again so the external primitive never retains a raw caller waker. The
/// target must expose readiness level-wise across that probe and re-poll: a
/// wake delivered to the no-op probe is recovered only when the immediate
/// re-poll observes the state change that prompted it.
///
/// Ready retirement is uniform and part of every poll: a ready result may own
/// a user value (or a panic payload that owns one), so the stored caller
/// clone is destroyed synchronously with its panic contained and discarded,
/// subordinate to returning that result intact (#398 ruling 3; the mailbox
/// `DisposingReceiver::retire_reply_waker` documents the two costs riding on
/// the discard). Only *pending* retirement — cancellation and drop glue —
/// remains venue-specific, so [`Self::retire`] takes the disposition: the
/// mailbox reply receiver passes [`WakerAction::DropInline`], and
/// `shelterwood-runtime`'s wrapper passes [`WakerAction::Run`] with its
/// detached disposal lane.
///
/// This is the only waker-proxy item outside this module. The proxy itself
/// is module-private, so no other code can register with, wake, or retire it
/// except through this state machine.
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
        let mut panics = PanicAccumulator::default();
        self.retire(WakerAction::DropInline, &mut panics);
        crate::panic::discard_panic(panics.take());
    }

    /// Test-only visibility: whether a pending poll has a proxy installed.
    #[cfg(test)]
    pub(crate) fn is_parked(&self) -> bool {
        self.proxy.is_some()
    }

    /// Retires the current caller registration through `action`.
    ///
    /// The caller waker is moved into an effects sink under the proxy's leaf
    /// mutex and the chosen effect runs only after unlock; any panic it
    /// raises lands in `panics`. The proxy is then dropped with its slot
    /// already empty.
    #[doc(hidden)]
    pub fn retire(&mut self, action: WakerAction, panics: &mut PanicAccumulator) {
        if let Some(proxy) = self.proxy.take() {
            let mut effects = WakerEffects::default();
            proxy.retire(action, &mut effects);
            effects.flush(panics);
        }
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
/// take another framework lock. Its guard acquisition deliberately recovers
/// poison: nothing the leaf guards carries an invariant a panic could tear
/// (see [`WakerProxyState::registration`]), and a poisoned leaf must not
/// introduce a panic into unwind-reachable retirement.
///
/// Private to this module: [`ProxiedPoll`] is its only owner, so every
/// registration goes through the probe/re-poll protocol and every retirement
/// through an effects flush.
struct WakerProxy {
    proxy: Waker,
    state: Arc<WakerProxyState>,
}

/// The proxy's mutable half: the caller's waker beside a count of every wake
/// the proxy has received, which lets [`WakerProxy::register`] detect a wake
/// landing while it clones outside the mutex.
#[derive(Default)]
struct Registration {
    caller: WakerSlot,
    wakes: u64,
}

#[derive(Default)]
struct WakerProxyState {
    caller: Mutex<Registration>,
}

impl WakerProxyState {
    /// Acquires the leaf registration without turning an earlier bookkeeping
    /// panic into a second panic at a later proxy operation.
    ///
    /// No user code runs under this mutex: every critical section here only
    /// compares waker pointers, bumps a counter, or moves a `Waker` between
    /// the slot and an effects sink that was built before the guard, so a
    /// recovered guard never exposes a half-applied update.
    ///
    /// Every acquisition goes through this helper so the non-panicking policy
    /// is structural, including drop-glue retirement, where an `.expect` on a
    /// poisoned leaf during an unwind aborts rather than fails.
    fn registration(&self) -> MutexGuard<'_, Registration> {
        self.caller.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl WakerProxy {
    fn new() -> Self {
        let state = Arc::new(WakerProxyState::default());
        let proxy = Waker::from(Arc::clone(&state));
        Self { proxy, state }
    }

    /// Installs the current caller without cloning or dropping its waker
    /// while the proxy mutex is held.
    ///
    /// Every wake after this returns reaches `current`. A wake that lands
    /// while `current` is cloned outside the mutex — when the slot is empty
    /// or still holds a previous poll's waker — is detected by the moved
    /// wake count and delivered to `current` after unlock. Wakes before this
    /// call are the caller's to observe: it must poll its level-readiness
    /// target after registering, as [`ProxiedPoll`]'s re-poll does.
    fn register(&self, current: &Waker) {
        let observed = {
            let registration = self.state.registration();
            if registration.caller.will_wake(current) {
                return;
            }
            registration.wakes
        };
        // `Waker::clone` dispatches through a caller-owned vtable, so it runs
        // between the two critical sections.
        let replacement = current.clone();
        // Effects precede the guard, so every exit — an unwind included —
        // releases the mutex before a displaced or woken vtable runs.
        let mut effects = WakerEffects::default();
        let mut registration = self.state.registration();
        registration.caller.replace(replacement, &mut effects);
        if registration.wakes != observed {
            registration.caller.take(WakerAction::Wake, &mut effects);
        }
    }

    fn waker(&self) -> &Waker {
        &self.proxy
    }

    /// Moves the caller waker into an explicitly chosen post-unlock effect.
    ///
    /// A [`WakerAction::Run`] disposer — the runtime adapter's detached
    /// disposal lane — is queued here under the leaf mutex and invoked only
    /// by the sink's flush, after unlock.
    fn retire(&self, action: WakerAction, effects: &mut WakerEffects) {
        self.state.registration().caller.take(action, effects);
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
            let mut registration = self.registration();
            // Counted whether or not a caller is installed: an in-flight
            // `register` compares the count across its clone window.
            registration.wakes = registration.wakes.wrapping_add(1);
            registration.caller.take(WakerAction::Wake, &mut effects);
        }
    }
}

#[cfg(test)]
#[allow(unsafe_code)] // raw-waker test doubles
mod tests {
    use std::{
        mem::ManuallyDrop,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, TryLockError, Weak,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
    };

    use super::{ProxiedPoll, WakerProxy, WakerProxyState};
    use crate::{
        panic::PanicAccumulator,
        waker::{WakerAction, WakerEffects},
    };

    #[derive(Default)]
    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Fails the calling test when the proxy mutex is still held.
    ///
    /// Only `WouldBlock` means user code is running under the guard:
    /// `try_lock` also reports a *free* mutex as an error once it is poisoned,
    /// which the poison-recovery test injects deliberately. Treating that as a
    /// held lock would turn every reentrancy probe into a false failure.
    fn assert_proxy_released(state: &Weak<WakerProxyState>, held: &'static str) {
        let state = state.upgrade().expect("the proxy remains live");
        if let Err(TryLockError::WouldBlock) = state.caller.try_lock() {
            panic!("{held}");
        }
    }

    struct ReentrantWake {
        proxy: Weak<WakerProxyState>,
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            assert_proxy_released(
                &self.proxy,
                "a forwarded wake runs after the proxy mutex is released",
            );
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
            assert_proxy_released(
                &self.0,
                "a displaced waker drops after the proxy mutex is released",
            );
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
        assert_proxy_released(
            &probe.proxy,
            "a caller waker clones after the proxy mutex is released",
        );
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
    /// sections — the window the wake count exists to close.
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

    #[derive(Default)]
    struct PanickingDropWaker {
        drops: AtomicUsize,
    }

    unsafe fn clone_panicking_drop(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let state = ManuallyDrop::new(unsafe { Arc::<PanickingDropWaker>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&state)).cast(),
            &PANICKING_DROP_VTABLE,
        )
    }

    unsafe fn wake_panicking_drop(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<PanickingDropWaker>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_panicking_drop(_data: *const ()) {}

    unsafe fn drop_panicking_drop(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<PanickingDropWaker>::from_raw(data.cast()) };
        let first_drop = state.drops.fetch_add(1, Ordering::SeqCst) == 0;
        drop(state);
        if first_drop {
            panic!("hostile caller-waker destructor");
        }
    }

    static PANICKING_DROP_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_panicking_drop,
        wake_panicking_drop,
        wake_by_ref_panicking_drop,
        drop_panicking_drop,
    );

    fn panicking_drop_waker(state: &Arc<PanickingDropWaker>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::clone(state)).cast(),
            &PANICKING_DROP_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    struct ReadyPayload(Arc<AtomicUsize>);

    impl Drop for ReadyPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PendingThenPayload {
        polls: usize,
        payload_drops: Arc<AtomicUsize>,
    }

    impl PendingThenPayload {
        fn poll(&mut self, _context: &mut Context<'_>) -> Poll<ReadyPayload> {
            self.polls += 1;
            if self.polls < 3 {
                Poll::Pending
            } else {
                Poll::Ready(ReadyPayload(Arc::clone(&self.payload_drops)))
            }
        }
    }

    #[derive(Default)]
    struct AlwaysPending {
        polls: usize,
        registered: Option<Waker>,
    }

    impl AlwaysPending {
        fn poll(&mut self, context: &mut Context<'_>) -> Poll<()> {
            self.polls += 1;
            self.registered = Some(context.waker().clone());
            Poll::Pending
        }
    }

    fn forward_wake(waker: Waker) {
        waker.wake();
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

        // The wake fires while the slot is still empty, so the moved wake
        // count is the only thing that can carry it forward.
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
    fn a_delivered_wake_does_not_wake_the_next_registration() {
        let proxy = WakerProxy::new();
        let delivered = Arc::new(CountWake::default());
        proxy.register(&Waker::from(Arc::clone(&delivered)));

        proxy.waker().wake_by_ref();
        assert_eq!(delivered.0.load(Ordering::SeqCst), 1);

        let next = Arc::new(CountWake::default());
        proxy.register(&Waker::from(Arc::clone(&next)));

        // The wake completed before this registration began, so the caller's
        // own poll after registering observes its cause.
        assert_eq!(
            next.0.load(Ordering::SeqCst),
            0,
            "a wake delivered before registration is not replayed to the next caller"
        );
    }

    #[test]
    fn a_wake_into_an_empty_slot_is_not_replayed_to_a_later_caller() {
        let proxy = WakerProxy::new();
        proxy.waker().wake_by_ref();

        let target = Arc::new(CountWake::default());
        proxy.register(&Waker::from(Arc::clone(&target)));

        assert_eq!(target.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn an_ordinary_delivery_wakes_the_proxied_caller_exactly_once() {
        let wakes = Arc::new(CountWake::default());
        let caller = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&caller);
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let mut target = PendingThenPayload {
            polls: 0,
            payload_drops: Arc::clone(&payload_drops),
        };
        let mut proxied = ProxiedPoll::new();
        let registered = std::cell::RefCell::new(None);
        let mut poll = |target: &mut PendingThenPayload, context: &mut Context<'_>| {
            *registered.borrow_mut() = Some(context.waker().clone());
            target.poll(context)
        };

        // Probe and proxied re-poll are both pending.
        assert!(
            proxied
                .poll(&mut target, &mut context, &mut poll, Poll::is_pending)
                .is_pending()
        );
        registered
            .take()
            .expect("the pending target registered the proxy")
            .wake();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);

        // The woken task polls once and finds the result ready.
        assert!(
            proxied
                .poll(&mut target, &mut context, &mut poll, Poll::is_pending)
                .is_ready()
        );
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "delivering a result must not schedule a further, empty poll of the caller"
        );
    }

    #[test]
    fn poisoned_proxy_leaf_remains_usable_across_every_transition() {
        let proxy = WakerProxy::new();
        let injected = catch_unwind(AssertUnwindSafe(|| {
            let _registration = proxy
                .state
                .caller
                .lock()
                .expect("the fresh proxy mutex is not poisoned");
            panic!("poison the framework-only proxy leaf");
        }));
        assert!(injected.is_err());
        // Every step below would also pass on an unpoisoned proxy, so the
        // premise is asserted rather than assumed: a panic escaping before the
        // guard was acquired would leave nothing to recover from.
        assert!(
            proxy.state.caller.is_poisoned(),
            "the injected panic has to leave the leaf poisoned for recovery to be under test"
        );

        let first = Arc::new(CountWake::default());
        proxy.register(&Waker::from(Arc::clone(&first)));
        proxy.waker().wake_by_ref();
        assert_eq!(
            first.0.load(Ordering::SeqCst),
            1,
            "registration and wake delivery both recover the poisoned leaf"
        );

        // Then each retirement seam in turn, because every one of them is
        // reachable from drop glue, where a panicking acquisition during an
        // unwind is an abort rather than a failure. First the mailbox seam:
        // `retire` into an effects sink (`DisposingReceiver::drop`). The
        // adapter's `Run` disposition takes this same acquisition; its
        // after-unlock forwarding is `run_retirement_invokes_its_disposer_after_unlock`.
        let retired = Waker::from(Arc::new(ReentrantDrop(Arc::downgrade(&proxy.state))));
        proxy.register(&retired);
        drop(retired);
        let mut effects = WakerEffects::default();
        proxy.retire(WakerAction::DropInline, &mut effects);
        drop(effects);

        // Finally the fallback: leave a caller installed so drop glue both
        // acquires the poisoned leaf and drains a real user waker. Its
        // reentrant destructor proves the recovered guard was released before
        // the effect ran.
        let dropped = Waker::from(Arc::new(ReentrantDrop(Arc::downgrade(&proxy.state))));
        proxy.register(&dropped);
        drop(dropped);
        drop(proxy);
    }

    #[test]
    fn run_retirement_invokes_its_disposer_after_unlock() {
        // Driven through `ProxiedPoll::retire`, the seam the runtime adapter
        // and the mailbox reply receiver actually cross: a pending poll parks
        // the proxy, then a reentrant caller is registered behind it.
        let mut proxied = ProxiedPoll::new();
        let mut target = AlwaysPending::default();
        let mut probe = Context::from_waker(Waker::noop());
        assert!(
            proxied
                .poll(
                    &mut target,
                    &mut probe,
                    AlwaysPending::poll,
                    Poll::is_pending
                )
                .is_pending()
        );
        let state = Arc::downgrade(
            &proxied
                .proxy
                .as_ref()
                .expect("a pending poll parks the proxy")
                .state,
        );
        let wakes = Arc::new(AtomicUsize::new(0));
        let caller = Waker::from(Arc::new(ReentrantWake {
            proxy: Weak::clone(&state),
            wakes: Arc::clone(&wakes),
        }));
        let mut context = Context::from_waker(&caller);
        assert!(
            proxied
                .poll(
                    &mut target,
                    &mut context,
                    AlwaysPending::poll,
                    Poll::is_pending
                )
                .is_pending()
        );

        let mut panics = PanicAccumulator::default();
        proxied.retire(WakerAction::Run(forward_wake), &mut panics);

        assert!(panics.take().is_none());
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(!proxied.is_parked());
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

    #[test]
    fn ready_retirement_contains_a_hostile_waker_drop_and_returns_the_payload() {
        let waker_state = Arc::new(PanickingDropWaker::default());
        let mut caller = ManuallyDrop::new(panicking_drop_waker(&waker_state));
        let mut context = Context::from_waker(&caller);
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let mut target = PendingThenPayload {
            polls: 0,
            payload_drops: Arc::clone(&payload_drops),
        };
        let mut proxied = ProxiedPoll::new();

        assert!(
            proxied
                .poll(
                    &mut target,
                    &mut context,
                    PendingThenPayload::poll,
                    Poll::is_pending
                )
                .is_pending()
        );
        let Poll::Ready(payload) = proxied.poll(
            &mut target,
            &mut context,
            PendingThenPayload::poll,
            Poll::is_pending,
        ) else {
            panic!("the third target poll is ready");
        };

        assert_eq!(
            waker_state.drops.load(Ordering::SeqCst),
            1,
            "ready retirement ran and contained the hostile caller clone's destructor"
        );
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        assert!(!proxied.is_parked());
        drop(payload);
        assert_eq!(
            payload_drops.load(Ordering::SeqCst),
            1,
            "the ready payload survived retirement and reached the caller"
        );

        // The first (proxied) clone was the hostile drop. The original caller
        // can now be reclaimed without raising a second panic.
        drop(unsafe { ManuallyDrop::take(&mut caller) });
    }

    /// A per-poll caller waker for the stress test: it records its own wake
    /// and unparks the polling thread. Its `clone` yields first, widening the
    /// window `register` leaves between its two critical sections.
    struct ParkWake {
        woken: AtomicBool,
        poller: std::thread::Thread,
    }

    unsafe fn clone_park(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let state = ManuallyDrop::new(unsafe { Arc::<ParkWake>::from_raw(data.cast()) });
        std::thread::yield_now();
        RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &PARK_VTABLE)
    }

    unsafe fn wake_park(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<ParkWake>::from_raw(data.cast()) };
        state.woken.store(true, Ordering::SeqCst);
        state.poller.unpark();
    }

    unsafe fn wake_by_ref_park(data: *const ()) {
        // SAFETY: wake_by_ref borrows the Arc reference represented by this
        // waker, which ManuallyDrop preserves.
        let state = ManuallyDrop::new(unsafe { Arc::<ParkWake>::from_raw(data.cast()) });
        state.woken.store(true, Ordering::SeqCst);
        state.poller.unpark();
    }

    unsafe fn drop_park(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<ParkWake>::from_raw(data.cast()) });
    }

    static PARK_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_park, wake_park, wake_by_ref_park, drop_park);

    fn park_waker(state: &Arc<ParkWake>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(Arc::clone(state)).cast(), &PARK_VTABLE);
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    /// A level-readiness primitive: a monotonic event count plus the one
    /// waker its latest pending poll registered, which it keeps (rather than
    /// takes) so every later wake re-enters the same proxy.
    #[derive(Default)]
    struct LevelCounter {
        produced: AtomicUsize,
        registered: std::sync::Mutex<Option<Waker>>,
    }

    impl LevelCounter {
        fn poll(&self, seen: usize, context: &mut Context<'_>) -> Poll<usize> {
            // Register before reading, as a real primitive does, so a
            // producer that publishes after the read finds this waker.
            *self.registered.lock().expect("unpoisoned") = Some(context.waker().clone());
            let produced = self.produced.load(Ordering::SeqCst);
            if produced > seen {
                Poll::Ready(produced)
            } else {
                Poll::Pending
            }
        }

        fn wake(&self) {
            let waker = self.registered.lock().expect("unpoisoned").clone();
            if let Some(waker) = waker {
                waker.wake_by_ref();
            }
        }
    }

    #[test]
    fn concurrent_wakes_across_re_registration_are_never_lost() {
        const EVENTS: usize = 10_000;
        const STALL: std::time::Duration = std::time::Duration::from_secs(10);

        let target = Arc::new(LevelCounter::default());
        let consumed = Arc::new(AtomicUsize::new(0));
        let producer = {
            let target = Arc::clone(&target);
            let consumed = Arc::clone(&consumed);
            std::thread::spawn(move || {
                for event in 1..=EVENTS {
                    // One state change and exactly one wake per event, issued
                    // only once the previous event is consumed: no later wake
                    // can rescue one that goes astray, so a lost wake stalls
                    // the poller below.
                    let started = std::time::Instant::now();
                    while consumed.load(Ordering::SeqCst) < event - 1 {
                        assert!(started.elapsed() < STALL, "the poller stalled");
                        std::thread::yield_now();
                    }
                    for _ in 0..event % 5 {
                        std::thread::yield_now();
                    }
                    target.produced.store(event, Ordering::SeqCst);
                    target.wake();
                }
            })
        };

        let mut proxied = ProxiedPoll::new();
        let mut seen = 0;
        let mut polls = 0usize;
        while seen < EVENTS {
            // A few pending re-polls per round, each under a fresh caller
            // identity, as when a future migrates between tasks: the proxy
            // persists, so every one re-registers and clones outside the
            // lock, and a wake landing on an earlier identity reaches a
            // "task" that is no longer waiting. Only the last identity is
            // waited on.
            let mut caller = None;
            for _ in 0..=polls % 4 {
                let fresh = Arc::new(ParkWake {
                    woken: AtomicBool::new(false),
                    poller: std::thread::current(),
                });
                let waker = park_waker(&fresh);
                let mut context = Context::from_waker(&waker);
                polls += 1;
                let result = proxied.poll(
                    &mut &*target,
                    &mut context,
                    |target, context| target.poll(seen, context),
                    Poll::is_pending,
                );
                if let Poll::Ready(produced) = result {
                    seen = produced;
                    consumed.store(seen, Ordering::SeqCst);
                    caller = None;
                    break;
                }
                caller = Some(fresh);
            }
            let Some(caller) = caller else { continue };
            let started = std::time::Instant::now();
            while !caller.woken.load(Ordering::SeqCst) {
                let elapsed = started.elapsed();
                assert!(
                    elapsed < STALL,
                    "lost wakeup: pending at {seen}/{EVENTS} with {} produced after {polls} polls",
                    target.produced.load(Ordering::SeqCst),
                );
                std::thread::park_timeout(STALL - elapsed);
            }
        }
        producer.join().expect("the producer thread completes");
    }

    #[test]
    fn polling_after_retirement_rearms_with_a_fresh_proxy() {
        let wakes = Arc::new(CountWake::default());
        let caller = Waker::from(Arc::clone(&wakes));
        let mut context = Context::from_waker(&caller);
        let mut target = AlwaysPending::default();
        let mut proxied = ProxiedPoll::new();

        assert!(
            proxied
                .poll(
                    &mut target,
                    &mut context,
                    AlwaysPending::poll,
                    Poll::is_pending
                )
                .is_pending()
        );
        let stale = target
            .registered
            .as_ref()
            .expect("the pending target retains the first proxy")
            .clone();

        let mut panics = PanicAccumulator::default();
        proxied.retire(WakerAction::DropInline, &mut panics);
        assert!(panics.take().is_none());
        assert!(!proxied.is_parked());

        assert!(
            proxied
                .poll(
                    &mut target,
                    &mut context,
                    AlwaysPending::poll,
                    Poll::is_pending
                )
                .is_pending()
        );
        let rearmed = target
            .registered
            .as_ref()
            .expect("the pending target retains the replacement proxy");
        assert!(!stale.will_wake(rearmed));
        assert_eq!(target.polls, 4, "re-arming probes and then proxy-polls");

        stale.wake_by_ref();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
        rearmed.wake_by_ref();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    }
}
