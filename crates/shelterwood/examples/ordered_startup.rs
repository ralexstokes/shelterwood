//! Ordered startup: in an ordered tree each child's readiness gates the next
//! declared child's start, and shutdown stops one fully joined child at a
//! time in reverse declaration order.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use shelterwood::{PolicyError, Readiness, TaskDef, Tree};

type Log = Arc<Mutex<Vec<String>>>;

// ANCHOR: ordered_startup
// ANCHOR: worker
fn worker(name: &'static str, log: Log) -> Result<TaskDef, PolicyError> {
    TaskDef::new(move |context| {
        let log = Arc::clone(&log);
        async move {
            log.lock()
                .expect("the log mutex is never poisoned")
                .push(format!("start:{name}"));
            // Manual readiness: the next declared child starts only after
            // this signal.
            context.mark_ready();
            context.shutdown_token().cancelled().await;
            log.lock()
                .expect("the log mutex is never poisoned")
                .push(format!("stop:{name}"));
            Ok(())
        }
    })
    .readiness(Readiness::Manual)
}
// ANCHOR_END: worker

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log = Log::default();

    let mut tree = Tree::new();
    for name in ["first", "second", "third"] {
        tree.add_task(name, worker(name, Arc::clone(&log))?)?;
    }

    let system = tree.spawn()?;
    system.wait_started().await?;
    assert_eq!(
        *log.lock().expect("the log mutex is never poisoned"),
        ["start:first", "start:second", "start:third"],
    );

    system.shutdown(Duration::from_secs(5)).await?;
    assert_eq!(
        *log.lock().expect("the log mutex is never poisoned"),
        [
            "start:first",
            "start:second",
            "start:third",
            "stop:third",
            "stop:second",
            "stop:first",
        ],
    );
    println!("startup followed declaration order; shutdown reversed it");
    Ok(())
}
// ANCHOR_END: ordered_startup
