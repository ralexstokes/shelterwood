use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use crate::{
    BoxedSleep, MailboxRuntime,
    cell::waker_slot::{WakerAction, WakerEffects},
    panic::PanicAccumulator,
    waker_proxy::WakerProxy,
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
    timer_waker: Option<WakerProxy>,
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
            timer_waker: None,
            completed: false,
        }
    }

    /// Retires an armed timer on the polling thread.
    ///
    /// Used when a containing future completes by a non-timer branch. The
    /// caller-waker destructor is fully drained before the containing future
    /// hands its result back, and the wheel entry is synchronously removed.
    pub(crate) fn retire_inline(&mut self, panics: &mut PanicAccumulator) {
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
        let timer_waker = self.timer_waker.as_ref();
        // The proxy mutex guards framework-owned data and is documented
        // unpoisonable, but this path also runs during unwind. Keep every
        // cleanup step inside the accumulator so a bookkeeping defect cannot
        // turn a caller panic into an abort.
        panics.run(|| {
            if let Some(timer_waker) = timer_waker {
                timer_waker.retire(action, &mut effects);
            }
        });
        effects.flush(panics);

        // Slot first, timer second: once the caller waker is gone, cancelling
        // the wheel entry can deliver no stale wake to a caller that already
        // has its answer. The timer still drops synchronously so the entry is
        // gone when retirement returns.
        let timer = self.timer.take();
        panics.run(|| drop(timer));
        let timer_waker = self.timer_waker.take();
        panics.run(|| drop(timer_waker));
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
        if this.timer_waker.is_none() {
            let mut probe = Context::from_waker(Waker::noop());
            if this
                .timer
                .as_mut()
                .expect("an incomplete proxied sleep retains its timer")
                .as_mut()
                .poll(&mut probe)
                .is_ready()
            {
                this.completed = true;
                let mut panics = PanicAccumulator::default();
                this.retire_inline(&mut panics);
                crate::panic::discard_panic(panics.take());
                return Poll::Ready(());
            }
            this.timer_waker = Some(WakerProxy::new());
        }

        let timer_waker = this
            .timer_waker
            .as_ref()
            .expect("a parked proxied sleep retains its waker proxy");
        timer_waker.register(context.waker());
        let mut proxy_context = Context::from_waker(timer_waker.waker());
        if this
            .timer
            .as_mut()
            .expect("an incomplete proxied sleep retains its timer")
            .as_mut()
            .poll(&mut proxy_context)
            .is_pending()
        {
            return Poll::Pending;
        }

        this.completed = true;
        let mut panics = PanicAccumulator::default();
        this.retire_inline(&mut panics);
        crate::panic::discard_panic(panics.take());
        Poll::Ready(())
    }
}

impl Drop for ProxiedSleep {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        self.retire_disposing(&mut panics);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
        thread::{self, ThreadId},
        time::Duration,
    };

    use super::ProxiedSleep;

    #[derive(Default)]
    struct WakerCounts {
        clones: AtomicUsize,
        drops: AtomicUsize,
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
        drop(unsafe { Arc::<WakerCounts>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_counting(_data: *const ()) {}

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

    #[test]
    fn immediately_ready_timer_never_clones_the_caller_waker() {
        let state = Arc::new(WakerCounts::default());
        let waker = ManuallyDrop::new(counting_waker(&state));
        let mut context = Context::from_waker(&waker);
        let raw: crate::BoxedSleep = Box::pin(std::future::ready(()));
        let mut timer = Box::pin(ProxiedSleep::new(raw, crate::capability::tests::runtime()));

        assert!(timer.as_mut().poll(&mut context).is_ready());
        assert_eq!(state.clones.load(Ordering::SeqCst), 0);
        assert_eq!(state.drops.load(Ordering::SeqCst), 0);
        assert!(
            timer.timer.is_none(),
            "the already-ready raw timer leaves no wheel entry to cancel"
        );
        assert!(
            timer.timer_waker.is_none(),
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
        let raw: crate::BoxedSleep = Box::pin(ParkThenReady {
            polls: 0,
            registered: None,
        });
        let mut timer = Box::pin(ProxiedSleep::new(raw, crate::capability::tests::runtime()));

        assert!(timer.as_mut().poll(&mut context).is_pending());
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);
        assert_eq!(state.drops.load(Ordering::SeqCst), 0);

        assert!(timer.as_mut().poll(&mut context).is_ready());
        assert_eq!(
            state.drops.load(Ordering::SeqCst),
            1,
            "the caller clone is gone synchronously when the ready poll returns"
        );
    }

    #[test]
    fn containing_ready_path_cancels_the_losing_timer_inline() {
        let state = Arc::new(WakerCounts::default());
        let waker = ManuallyDrop::new(counting_waker(&state));
        let mut context = Context::from_waker(&waker);
        let raw: crate::BoxedSleep = Box::pin(std::future::pending());
        let mut timer = ProxiedSleep::new(raw, crate::capability::tests::runtime());

        assert!(Pin::new(&mut timer).poll(&mut context).is_pending());
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);

        timer.cancel_inline();

        assert_eq!(
            state.drops.load(Ordering::SeqCst),
            1,
            "a sibling branch returning Ready retires the timer caller before handing its value back"
        );
        assert!(timer.timer.is_none());
        assert!(timer.timer_waker.is_none());

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

    struct ThreadDrop(mpsc::Sender<ThreadId>);

    unsafe fn clone_thread_drop(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let state = ManuallyDrop::new(unsafe { Arc::<ThreadDrop>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&state)).cast(),
            &THREAD_DROP_VTABLE,
        )
    }

    unsafe fn wake_thread_drop(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<ThreadDrop>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_thread_drop(_data: *const ()) {}

    unsafe fn drop_thread_drop(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<ThreadDrop>::from_raw(data.cast()) };
        let _ = state.0.send(thread::current().id());
    }

    static THREAD_DROP_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_thread_drop,
        wake_thread_drop,
        wake_by_ref_thread_drop,
        drop_thread_drop,
    );

    fn thread_drop_waker(sender: mpsc::Sender<ThreadId>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(ThreadDrop(sender))).cast(),
            &THREAD_DROP_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    #[crate::runtime::test]
    async fn drop_glue_retires_the_caller_waker_on_the_disposal_lane() {
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let caller_thread = thread::current().id();
        let waker = ManuallyDrop::new(thread_drop_waker(dropped_tx));
        let mut context = Context::from_waker(&waker);
        let raw: crate::BoxedSleep = Box::pin(std::future::pending());
        let mut timer = Box::pin(ProxiedSleep::new(raw, crate::capability::tests::runtime()));

        assert!(timer.as_mut().poll(&mut context).is_pending());
        drop(timer);

        let destructor_thread = dropped_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the disposal lane retires the stored caller clone");
        assert_ne!(
            destructor_thread, caller_thread,
            "drop glue must not run the caller-waker destructor on its own thread"
        );
    }
}
