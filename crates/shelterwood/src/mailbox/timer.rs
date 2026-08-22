//! Adapter-integration coverage for `shelterwood_core::ProxiedSleep`.
//!
//! The timer's white-box pins live beside the type in
//! `shelterwood-core/src/proxied_sleep.rs`, where its private slots are
//! reachable. This module keeps only the test that needs the real runtime's
//! disposal lane, so core itself retains no dev-dependencies.

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        sync::{Arc, mpsc},
        task::{Context, RawWaker, RawWakerVTable, Waker},
        thread::{self, ThreadId},
        time::Duration,
    };

    use shelterwood_core::{BoxedSleep, ProxiedSleep};

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
        let raw: BoxedSleep = Box::pin(std::future::pending());
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
