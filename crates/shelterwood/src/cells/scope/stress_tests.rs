//! Multithreaded observation-gate handoff stress (review gap T-1c).
//!
//! Adoption re-homes a whole subtree onto its new parent's gate while other
//! threads admit into that subtree, write member records through the gate
//! they currently hold, and walk residency. The lock rule's one gate-to-gate
//! exemption (`MemberCell::with_handoff_gate`, parent to child only, with a
//! re-read of the installed pointer under the acquired guard) is what keeps
//! this deadlock-free and consistent. The assertions are schedule-independent:
//! every thread finishes within a stall budget, and any thread holding a gate
//! sees every resident reachable from it on that same gate.

use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use shelterwood_core::policy::ScopeFlavor;

use super::{ObservationGate, ResidentProjection, ScopeCell};
use crate::cells::{
    MemberStage,
    test_support::{child_member, child_scope, isolated_scope},
};

const MIDS: usize = 4;
const LEAVES_BEFORE: usize = 3;
const LEAVES_DURING: usize = 24;
const ROUNDS: usize = 40;
/// Far beyond a round's real runtime (milliseconds); only a deadlock or a
/// livelocked retry loop reaches it.
const STALL: Duration = Duration::from_secs(20);

fn projection(scope: &Arc<ScopeCell>) -> ResidentProjection {
    ResidentProjection::new(Arc::clone(&scope.member), Some(Arc::clone(scope)))
}

/// Walks every announced resident reachable from `scope`, requiring each to
/// sit on `held` — the gate the caller is inside — and to read `Admitted`.
/// Returns how many residents it visited.
fn assert_residents_on(scope: &ScopeCell, held: &ObservationGate) -> usize {
    let residents: Vec<_> = scope
        .current_children()
        .iter()
        .map(|resident| {
            assert!(
                resident.announced,
                "a gate holder never observes a half-wired admission"
            );
            resident.projection().clone()
        })
        .collect();
    let mut visited = residents.len();
    for resident in residents {
        assert!(
            resident.member.current_observation_gate().shares_gate(held),
            "a resident of the held tree sits on a foreign gate"
        );
        assert!(
            matches!(resident.member.record().stage, MemberStage::Admitted),
            "an announced resident reads Admitted"
        );
        if let Some(scope) = &resident.scope {
            visited += assert_residents_on(scope, held);
        }
    }
    visited
}

type Report = std::thread::Result<()>;

/// Runs `work` on a named thread and reports its outcome, panic included, on
/// `done`, so a failed assertion surfaces at once rather than as a stall.
fn spawn_reporting(
    name: String,
    done: &mpsc::Sender<Report>,
    work: impl FnOnce() + Send + 'static,
) -> thread::JoinHandle<()> {
    let done = done.clone();
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)));
        })
        .expect("stress thread spawns")
}

/// A completed first pass is required before the coordinator can stop any
/// open-ended worker. The coordinator itself never acquires a monitored gate.
fn observe_until_stopped(stop: &AtomicBool, first_passes: &AtomicUsize, mut work: impl FnMut()) {
    work();
    first_passes.fetch_add(1, Ordering::Release);
    while !stop.load(Ordering::Acquire) {
        work();
        thread::yield_now();
    }
}

fn round() -> usize {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mids: Vec<_> = (0..MIDS)
        .map(|index| child_scope(&root, &format!("mid-{index}"), ScopeFlavor::Ordered))
        .collect();
    // Each mid already owns a small residency on its own gate, so the root
    // adoption must carry those descendants across in the same cut.
    let deep: Vec<_> = mids
        .iter()
        .map(|mid| {
            for leaf in 0..LEAVES_BEFORE {
                assert!(mid.admit_child(ResidentProjection::new(
                    child_member(mid, &format!("before-{leaf}")),
                    None,
                )));
            }
            let deep = child_scope(mid, "deep", ScopeFlavor::Ordered);
            assert!(deep.admit_child(ResidentProjection::new(
                child_member(&deep, "deep-leaf"),
                None
            )));
            deep
        })
        .collect();

    let (done, finished) = mpsc::channel();
    let start = Arc::new(Barrier::new(MIDS * 4 + 2));
    let first_passes = Arc::new(AtomicUsize::new(0));
    let root_walks = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    let mut workers = 0;

    for (index, (mid, deep)) in mids.iter().zip(&deep).enumerate() {
        // Adopter: root admits the mid, re-homing its subtree onto the root
        // gate while holding both gates.
        let (root_a, mid_a, start_a) = (Arc::clone(&root), Arc::clone(mid), Arc::clone(&start));
        threads.push(spawn_reporting(
            format!("adopt-{index}"),
            &done,
            move || {
                start_a.wait();
                assert!(root_a.admit_child(projection(&mid_a)));
            },
        ));
        // Sub-admitter: admits fresh leaves and then the deep scope into the
        // mid while its gate is being replaced. Each admission must land on
        // whichever gate the mid holds when it gets in.
        let (mid_s, deep_s, start_s) = (Arc::clone(mid), Arc::clone(deep), Arc::clone(&start));
        threads.push(spawn_reporting(
            format!("admit-{index}"),
            &done,
            move || {
                start_s.wait();
                for leaf in 0..LEAVES_DURING {
                    assert!(mid_s.admit_child(ResidentProjection::new(
                        child_member(&mid_s, &format!("during-{leaf}")),
                        None,
                    )));
                    if leaf == LEAVES_DURING / 2 {
                        assert!(mid_s.admit_child(projection(&deep_s)));
                    }
                }
            },
        ));
        // Writer: record writes through the deep scope's and a leaf's own
        // current gate, which moves twice (deep → mid → root) underneath.
        let (deep_w, stop_w, start_w) = (Arc::clone(deep), Arc::clone(&stop), Arc::clone(&start));
        let passes_w = Arc::clone(&first_passes);
        threads.push(spawn_reporting(
            format!("write-{index}"),
            &done,
            move || {
                start_w.wait();
                let leaf = deep_w.resident_projections()[0].member.clone();
                observe_until_stopped(&stop_w, &passes_w, || {
                    // `update` debug-asserts that its transaction holds the
                    // member's current gate.
                    deep_w.member.update(|_| {});
                    leaf.update(|_| {});
                });
            },
        ));
        // Observer: walks the mid's residency from inside the mid's gate.
        let (mid_o, stop_o, start_o) = (Arc::clone(mid), Arc::clone(&stop), Arc::clone(&start));
        let passes_o = Arc::clone(&first_passes);
        threads.push(spawn_reporting(
            format!("observe-{index}"),
            &done,
            move || {
                start_o.wait();
                observe_until_stopped(&stop_o, &passes_o, || {
                    mid_o.with_observation_gate(|_| {
                        let held = mid_o.current_observation_gate();
                        assert_residents_on(&mid_o, &held);
                    });
                });
            },
        ));
        workers += 4;
    }

    // All gate acquisitions, including the final count, run on reporting
    // workers so a deadlocked root gate cannot block deadline enforcement.
    let (root_o, stop_o, start_o, passes_o, walks_o) = (
        Arc::clone(&root),
        Arc::clone(&stop),
        Arc::clone(&start),
        Arc::clone(&first_passes),
        Arc::clone(&root_walks),
    );
    threads.push(spawn_reporting(
        "observe-root".to_owned(),
        &done,
        move || {
            start_o.wait();
            observe_until_stopped(&stop_o, &passes_o, || {
                root_o.with_observation_gate(|_| {
                    let held = root_o.current_observation_gate();
                    assert_residents_on(&root_o, &held);
                });
                walks_o.fetch_add(1, Ordering::Relaxed);
            });
            let expected_per_mid = LEAVES_BEFORE + LEAVES_DURING + 2; // + deep + deep-leaf
            let visited = root_o.with_observation_gate(|_| {
                let held = root_o.current_observation_gate();
                assert_residents_on(&root_o, &held)
            });
            assert_eq!(visited, MIDS * (1 + expected_per_mid));
        },
    ));
    workers += 1;

    start.wait();
    let deadline = Instant::now() + STALL;
    let mut finished_count = 0;
    let mut stopping = false;
    while finished_count < workers {
        // Every bounded worker and every observer's first pass must finish
        // before the open-ended loops are allowed to stop.
        if !stopping
            && finished_count >= MIDS * 2
            && first_passes.load(Ordering::Acquire) == MIDS * 2 + 1
        {
            stop.store(true, Ordering::Release);
            stopping = true;
        }
        match finished.recv_timeout(Duration::from_micros(200)) {
            Ok(Ok(())) => finished_count += 1,
            Ok(Err(panic)) => {
                stop.store(true, Ordering::Release);
                std::panic::resume_unwind(panic);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                unreachable!("the test thread keeps a reporting sender")
            }
        }
        if Instant::now() >= deadline {
            let stuck: Vec<_> = threads
                .iter()
                .filter(|thread| !thread.is_finished())
                .filter_map(|thread| thread.thread().name().map(str::to_owned))
                .collect();
            // The stuck threads are leaked; the process ends with the test.
            panic!("gate handoff stalled (deadlock or livelock): {stuck:?} never finished");
        }
    }
    for thread in threads {
        thread
            .join()
            .expect("a reporting worker contains its own panic");
    }

    root_walks.load(Ordering::Relaxed)
}

#[test]
fn concurrent_adoption_admission_and_observation_share_one_gate() {
    let started = Instant::now();
    let mut root_walks = 0;
    for _ in 0..ROUNDS {
        root_walks += round();
    }
    eprintln!(
        "{ROUNDS} gate-handoff rounds in {:?} ({root_walks} root walks)",
        started.elapsed()
    );
}
