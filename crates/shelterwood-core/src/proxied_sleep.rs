use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    BoxedSleep, MailboxRuntime,
    panic::PanicAccumulator,
    waker::{WakerAction, WakerEffects},
    waker_proxy::ProxiedPoll,
};

/// Timer future that keeps a caller-owned waker out of the runtime's wheel.
///
/// Runtime adapters supply the raw timer, while this wrapper owns the common
/// framework boundary: the external primitive registers only a stable proxy,
/// and the caller's waker stays in the proxy's effects-mediated private slot.
/// A timer that is ready on the first no-op probe allocates no proxy.
///
/// Poll-path retirement is synchronous and contained. Drop-glue retirement
/// first hands the caller waker to the runtime's disposal lane, then cancels
/// the framework-only wheel entry synchronously. This is the venue split
/// established for public mailbox deadlines by #398.
#[doc(hidden)]
pub struct ProxiedSleep {
    runtime: Arc<dyn MailboxRuntime>,
    timer: Option<BoxedSleep>,
    timer_poll: ProxiedPoll,
    completed: bool,
}

impl ProxiedSleep {
    /// Wraps one raw runtime timer.
    ///
    /// The timer must be framework-owned: its poll and drop implementations
    /// are invoked inside framework containment boundaries, and the supported
    /// façade exposes no way for an application to construct this type.
    #[doc(hidden)]
    pub fn new(timer: BoxedSleep, runtime: Arc<dyn MailboxRuntime>) -> Self {
        Self {
            runtime,
            timer: Some(timer),
            timer_poll: ProxiedPoll::new(),
            completed: false,
        }
    }

    /// Retires an armed timer on the polling thread.
    ///
    /// Used when a containing future completes by a non-timer branch. The
    /// caller-waker destructor is fully drained before the containing future
    /// hands its result back, and the wheel entry is synchronously removed.
    #[doc(hidden)]
    pub fn retire_inline(&mut self, panics: &mut PanicAccumulator) {
        self.retire(WakerAction::DropInline, panics);
    }

    /// Cancels this timer on a containing future's successful poll path.
    ///
    /// Public only as a sibling-crate implementation seam. Runtime selection
    /// helpers call it when a non-timer branch wins, so that semantic poll
    /// path retains the synchronous-contained half of the venue split even
    /// though the timer itself is the losing future. Retirement fuses the
    /// sleep: a later poll reports ready rather than reaching for the
    /// emptied timer slot.
    #[doc(hidden)]
    pub fn cancel_inline(&mut self) {
        let mut panics = PanicAccumulator::default();
        self.retire_inline(&mut panics);
        crate::panic::discard_panic(panics.take());
    }

    fn retire_disposing(&mut self, panics: &mut PanicAccumulator) {
        self.retire(WakerAction::Dispose(Arc::clone(&self.runtime)), panics);
    }

    fn retire(&mut self, action: WakerAction, panics: &mut PanicAccumulator) {
        let mut effects = WakerEffects::default();
        // Proxy retirement only moves framework-owned bookkeeping into the
        // effects sink, and its leaf-lock acquisition recovers poison. The
        // proxy's own drop then finds the slot already emptied, so no caller
        // destructor runs here either; the caller-owned waker stays inside
        // the accumulator-backed flush below.
        self.timer_poll.retire(action, &mut effects);
        effects.flush(panics);

        // Slot first, timer second: once the caller waker is gone, cancelling
        // the wheel entry can deliver no stale wake to a caller that already
        // has its answer. The timer still drops synchronously so the entry is
        // gone when retirement returns.
        let timer = self.timer.take();
        panics.run(|| drop(timer));
        // Retirement is the single point that empties both slots, so it also
        // fuses the future: without this, a poll after `cancel_inline` would
        // panic on the emptied timer slot instead of reporting ready.
        self.completed = true;
    }
}

impl Future for ProxiedSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.as_mut().get_mut();
        if this.completed {
            return Poll::Ready(());
        }
        // `ProxiedPoll::poll` retires the caller slot inline on the ready
        // edge; `retire_inline` below then finds it empty and only cancels
        // the wheel entry, preserving the slot-first, timer-second order.
        let result = this.timer_poll.poll(
            this.timer
                .as_mut()
                .expect("an incomplete proxied sleep retains its timer"),
            context,
            |timer, context| timer.as_mut().poll(context),
            Poll::is_pending,
        );
        if result.is_ready() {
            let mut panics = PanicAccumulator::default();
            this.retire_inline(&mut panics);
            crate::panic::discard_panic(panics.take());
        }
        result
    }
}

impl Drop for ProxiedSleep {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        self.retire_disposing(&mut panics);
    }
}

/// White-box pins beside the type: these tests reach the private `timer` and
/// `timer_poll` slots to hold the retirement contract — the wheel entry is
/// gone and the proxy uninstalled when retirement returns — which black-box
/// waker counting alone cannot distinguish from deferred cleanup. The test
/// that needs a real disposal lane lives in the façade's mailbox timer
/// module, so this crate keeps no dev-dependencies.
#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
        time::Instant,
    };

    use super::ProxiedSleep;
    use crate::{
        BoxedSleep, ErasedOneShotReceiver, ErasedOneShotSender, MailboxRuntime, MailboxSignal,
    };

    /// Stub capability object: `ProxiedSleep` itself touches the runtime only
    /// through `dispose`, and every path these tests drive retires the caller
    /// slot inline, so every capability is unreachable by construction.
    struct InertRuntime;

    impl MailboxRuntime for InertRuntime {
        fn oneshot(
            &self,
        ) -> (
            Box<dyn ErasedOneShotSender>,
            Pin<Box<dyn ErasedOneShotReceiver>>,
        ) {
            unreachable!("proxied-sleep tests never open a one-shot");
        }

        fn signal(&self) -> Arc<dyn MailboxSignal> {
            unreachable!("proxied-sleep tests never mint a signal");
        }

        fn dispose(&self, _value: Box<dyn Send + 'static>) {
            unreachable!("inline retirement leaves drop glue nothing to dispose");
        }

        fn now(&self) -> Instant {
            unreachable!("proxied-sleep tests never read the clock");
        }

        fn sleep_until(&self, _deadline: Option<Instant>) -> BoxedSleep {
            unreachable!("proxied-sleep tests supply their raw timer directly");
        }
    }

    fn runtime() -> Arc<dyn MailboxRuntime> {
        Arc::new(InertRuntime)
    }

    #[derive(Default)]
    struct WakerCounts {
        clones: AtomicUsize,
        drops: AtomicUsize,
        wakes: AtomicUsize,
    }

    unsafe fn clone_counting(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let state = ManuallyDrop::new(unsafe { Arc::<WakerCounts>::from_raw(data.cast()) });
        state.clones.fetch_add(1, Ordering::SeqCst);
        RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &COUNTING_VTABLE)
    }

    unsafe fn wake_counting(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<WakerCounts>::from_raw(data.cast()) };
        state.wakes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn wake_by_ref_counting(data: *const ()) {
        // SAFETY: wake_by_ref borrows the Arc reference represented by this
        // waker, which ManuallyDrop preserves.
        let state = ManuallyDrop::new(unsafe { Arc::<WakerCounts>::from_raw(data.cast()) });
        state.wakes.fetch_add(1, Ordering::SeqCst);
    }

    unsafe fn drop_counting(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<WakerCounts>::from_raw(data.cast()) };
        state.drops.fetch_add(1, Ordering::SeqCst);
    }

    static COUNTING_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_counting,
        wake_counting,
        wake_by_ref_counting,
        drop_counting,
    );

    fn counting_waker(state: &Arc<WakerCounts>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(Arc::clone(state)).cast(), &COUNTING_VTABLE);
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    struct ParkThenReady {
        polls: usize,
        registered: Option<Waker>,
    }

    impl Future for ParkThenReady {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            self.polls += 1;
            self.registered = Some(context.waker().clone());
            if self.polls < 3 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    #[derive(Default)]
    struct WakeOnDropTimer {
        registered: Option<Waker>,
    }

    impl Future for WakeOnDropTimer {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            self.registered = Some(context.waker().clone());
            Poll::Pending
        }
    }

    impl Drop for WakeOnDropTimer {
        fn drop(&mut self) {
            if let Some(registered) = &self.registered {
                registered.wake_by_ref();
            }
        }
    }

    #[test]
    fn immediately_ready_timer_never_clones_the_caller_waker() {
        let state = Arc::new(WakerCounts::default());
        let waker = ManuallyDrop::new(counting_waker(&state));
        let mut context = Context::from_waker(&waker);
        let raw: BoxedSleep = Box::pin(std::future::ready(()));
        let mut timer = Box::pin(ProxiedSleep::new(raw, runtime()));

        assert!(timer.as_mut().poll(&mut context).is_ready());
        assert_eq!(state.clones.load(Ordering::SeqCst), 0);
        assert_eq!(state.drops.load(Ordering::SeqCst), 0);
        assert!(
            timer.timer.is_none(),
            "the already-ready raw timer leaves no wheel entry to cancel"
        );
        assert!(
            !timer.timer_poll.is_parked(),
            "the no-op probe allocates no proxy on the ready path"
        );

        assert!(
            timer.as_mut().poll(&mut context).is_ready(),
            "a completed proxied timer is fused"
        );
        assert_eq!(
            state.clones.load(Ordering::SeqCst),
            0,
            "repolling a completed timer never reaches the caller vtable"
        );
    }

    #[test]
    fn ready_poll_retires_the_caller_waker_inline_before_returning() {
        let state = Arc::new(WakerCounts::default());
        let waker = ManuallyDrop::new(counting_waker(&state));
        let mut context = Context::from_waker(&waker);
        let raw: BoxedSleep = Box::pin(ParkThenReady {
            polls: 0,
            registered: None,
        });
        let mut timer = Box::pin(ProxiedSleep::new(raw, runtime()));

        assert!(timer.as_mut().poll(&mut context).is_pending());
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.drops.load(Ordering::SeqCst), 0);
        assert!(
            timer.timer_poll.is_parked(),
            "a pending poll parks the caller behind the installed proxy"
        );

        assert!(timer.as_mut().poll(&mut context).is_ready());
        assert_eq!(
            state.drops.load(Ordering::SeqCst),
            1,
            "the caller clone is gone synchronously when the ready poll returns"
        );
        assert!(
            timer.timer.is_none(),
            "the wheel entry is gone when ready retirement returns"
        );
        assert!(!timer.timer_poll.is_parked());
    }

    #[test]
    fn containing_ready_path_cancels_the_losing_timer_inline() {
        let state = Arc::new(WakerCounts::default());
        let waker = ManuallyDrop::new(counting_waker(&state));
        let mut context = Context::from_waker(&waker);
        let raw: BoxedSleep = Box::pin(std::future::pending());
        let mut timer = ProxiedSleep::new(raw, runtime());

        assert!(Pin::new(&mut timer).poll(&mut context).is_pending());
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);

        timer.cancel_inline();

        assert_eq!(
            state.drops.load(Ordering::SeqCst),
            1,
            "a sibling branch returning Ready retires the timer caller before handing its value back"
        );
        assert!(timer.timer.is_none());
        assert!(!timer.timer_poll.is_parked());

        assert!(
            Pin::new(&mut timer).poll(&mut context).is_ready(),
            "a cancelled proxied timer is fused rather than reaching for its emptied slots"
        );
        assert_eq!(
            state.clones.load(Ordering::SeqCst),
            1,
            "repolling a cancelled timer never reaches the caller vtable"
        );
    }

    #[test]
    fn retirement_empties_the_caller_slot_before_the_timer_is_cancelled() {
        let state = Arc::new(WakerCounts::default());
        let waker = ManuallyDrop::new(counting_waker(&state));
        let mut context = Context::from_waker(&waker);
        let raw: BoxedSleep = Box::pin(WakeOnDropTimer::default());
        let mut timer = ProxiedSleep::new(raw, runtime());

        assert!(Pin::new(&mut timer).poll(&mut context).is_pending());
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);

        timer.cancel_inline();

        assert_eq!(
            state.wakes.load(Ordering::SeqCst),
            0,
            "the timer's drop-time proxy wake cannot reach a retired caller"
        );
        assert_eq!(state.drops.load(Ordering::SeqCst), 1);
        assert!(timer.timer.is_none());
        assert!(!timer.timer_poll.is_parked());
    }
}
