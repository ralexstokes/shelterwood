//! Adapter-integration coverage for `shelterwood_core::ProxiedSleep`.
//!
//! The timer's white-box pins live beside the type in
//! `shelterwood-core/src/proxied_sleep.rs`, where its private slots are
//! reachable. This module keeps only the test that needs the real runtime's
//! disposal lane, so core itself retains no dev-dependencies.

mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        sync::mpsc,
        task::{Context, Waker},
        thread::{self, ThreadId},
        time::Duration,
    };

    use shelterwood_core::{BoxedSleep, ProxiedSleep};

    use crate::mailbox::test_support::probe_waker;

    fn thread_drop_waker(sender: mpsc::Sender<ThreadId>) -> Waker {
        probe_waker(
            || {},
            move || {
                let _ = sender.send(thread::current().id());
            },
        )
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
