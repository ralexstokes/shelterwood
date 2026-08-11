use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use shelterwood::{TaskDef, Tree};

pub(crate) fn task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
}

pub(crate) fn signalled_waiting_task(
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
) -> TaskDef {
    TaskDef::new(move |context| {
        let started = Arc::clone(&started);
        let cancelled = Arc::clone(&cancelled);
        async move {
            started.store(true, Ordering::SeqCst);
            context.shutdown_token().cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
}

pub(crate) fn tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("worker", task()).expect("valid task");
    tree
}
