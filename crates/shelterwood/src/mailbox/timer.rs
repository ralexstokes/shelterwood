pub(super) use shelterwood_core::ProxiedSleep;

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
        let raw: crate::mailbox::BoxedSleep = Box::pin(std::future::ready(()));
        let mut timer = Box::pin(ProxiedSleep::new(
            raw,
            crate::mailbox::capability::tests::runtime(),
        ));

        assert!(timer.as_mut().poll(&mut context).is_ready());
        assert_eq!(state.clones.load(Ordering::SeqCst), 0);
        assert_eq!(state.drops.load(Ordering::SeqCst), 0);
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
        let raw: crate::mailbox::BoxedSleep = Box::pin(ParkThenReady {
            polls: 0,
            registered: None,
        });
        let mut timer = Box::pin(ProxiedSleep::new(
            raw,
            crate::mailbox::capability::tests::runtime(),
        ));

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
        let raw: crate::mailbox::BoxedSleep = Box::pin(std::future::pending());
        let mut timer = ProxiedSleep::new(raw, crate::mailbox::capability::tests::runtime());

        assert!(Pin::new(&mut timer).poll(&mut context).is_pending());
        assert_eq!(state.clones.load(Ordering::SeqCst), 1);

        timer.cancel_inline();

        assert_eq!(
            state.drops.load(Ordering::SeqCst),
            1,
            "a sibling branch returning Ready retires the timer caller before handing its value back"
        );
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
        let raw: crate::mailbox::BoxedSleep = Box::pin(std::future::pending());
        let mut timer = Box::pin(ProxiedSleep::new(
            raw,
            crate::mailbox::capability::tests::runtime(),
        ));

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
