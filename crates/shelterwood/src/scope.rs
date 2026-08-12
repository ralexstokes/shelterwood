//! Ordered and dynamic scope handles.

use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::{
    DeadlineBudget,
    cells::ScopeCell,
    exit::StopReason,
    identity::{ChildId, Membership},
    observe::{ChildSnapshot, LifecycleEvents, ScopeSnapshot, SnapshotReceiver, WaitError},
    policy::ScopeFlavor,
};

/// A cheap, membership-addressed ordered scope handle.
#[derive(Clone)]
pub struct ScopeRef {
    pub(crate) cell: Arc<ScopeCell>,
}

impl ScopeRef {
    /// Returns this scope's child id within its parent.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.cell.member.id()
    }

    /// Returns the scope membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.cell.member.membership()
    }

    /// Computes an authoritative recursive snapshot on demand.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.cell.snapshot()
    }

    /// Subscribes to conflated recursive snapshots.
    #[must_use]
    pub fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.cell.subscribe_snapshots()
    }

    /// Subscribes to this scope's lifecycle and all forwarded descendants.
    #[must_use]
    pub fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.cell.subscribe_lifecycle()
    }

    /// Looks up a direct child in an authoritative current snapshot.
    #[must_use]
    pub fn child(&self, id: impl AsRef<str>) -> Option<ChildSnapshot> {
        self.snapshot().child(id).cloned()
    }

    /// Traverses a child-id path in an authoritative current snapshot.
    #[must_use]
    pub fn descendant<I, S>(&self, path: I) -> Option<ChildSnapshot>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.snapshot().descendant(path).cloned()
    }

    /// Requests shutdown without waiting.
    pub fn request_shutdown(&self) {
        let _ = self.cell.request_shutdown();
    }
}

impl ScopeRef {
    /// Waits for a named child snapshot satisfying an at-or-past predicate.
    ///
    /// Snapshot watches conflate intermediate states, so `pred` should accept
    /// every state at or beyond the desired edge and must remain cheap and
    /// non-blocking.
    /// A zero budget evaluates the current snapshot exactly once, with
    /// predicate match taking precedence over terminal scope and timeout.
    ///
    /// `id` is converted through `Into<ChildId>` eagerly, on the calling
    /// thread, before the future exists — so the conversion runs even when the
    /// returned future is never polled. That is what keeps the future `Send`
    /// for a non-`Send` `id`; the wait itself still begins at first poll.
    pub fn wait_for_child<P>(
        &self,
        id: impl Into<ChildId>,
        pred: P,
        timeout: DeadlineBudget,
    ) -> impl std::future::Future<Output = Result<ChildSnapshot, WaitError>> + Send
    where
        P: FnMut(&ChildSnapshot) -> bool + Send,
    {
        let id = id.into();
        self.wait_for_child_inner(id, pred, timeout)
    }

    async fn wait_for_child_inner<P>(
        &self,
        id: ChildId,
        mut pred: P,
        timeout: DeadlineBudget,
    ) -> Result<ChildSnapshot, WaitError>
    where
        P: FnMut(&ChildSnapshot) -> bool + Send,
    {
        let poll_once = crate::deadline::select_zero_budget_behavior(
            timeout,
            crate::deadline::ZeroBudgetBehavior::PollOnce,
        )
        .is_some();
        let expires = crate::deadline::Deadline::after_budget(crate::runtime::now(), timeout);
        let mut snapshots = self.subscribe_snapshots();

        loop {
            let (snapshot, closed) = snapshots.borrow_latest_and_closed();
            if let Some(child) = snapshot.child(id.as_str())
                && pred(child)
            {
                return Ok(child.clone());
            }
            // The published stream is the waiter's sole authority. Closure
            // and its final snapshot commit together under ObservationTxn, so
            // the terminal payload cannot come from an earlier cut.
            if closed {
                return Err(WaitError::ScopeTerminated {
                    state: snapshot.state.clone(),
                });
            }
            // Precedence is predicate, final termination, then deadline on
            // both the fast and awaited paths.
            if poll_once || expires.is_due(crate::runtime::now()) {
                return Err(WaitError::TimedOut);
            }
            match crate::runtime::select_two(snapshots.changed(), async {
                match expires.instant() {
                    Some(expires) => crate::runtime::sleep_until_std(expires).await,
                    None => std::future::pending().await,
                }
            })
            .await
            {
                crate::runtime::Either::Left(Ok(_) | Err(_))
                | crate::runtime::Either::Right(()) => continue,
            }
        }
    }

    /// Waits for terminal membership state.
    pub async fn wait_stopped(&self) -> StopReason {
        self.cell.wait_stopped().await
    }
}

impl ScopeRef {
    /// Dynamically queries whether this scope has dynamic capabilities.
    #[must_use]
    pub fn dynamic(&self) -> Option<DynamicScopeRef> {
        (self.cell.flavor == ScopeFlavor::Dynamic).then(|| DynamicScopeRef(self.clone()))
    }
}

impl fmt::Debug for ScopeRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeRef")
            .field("membership", &self.membership())
            .finish()
    }
}

// Handle identity is the slot cell, not the membership token: declaration
// lowering can rebase a provisional token behind live pre-spawn handles, and
// a token-value hash would strand entries keyed before that rebase.
impl PartialEq for ScopeRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cell, &other.cell)
    }
}

impl Eq for ScopeRef {}

impl Hash for ScopeRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.cell).hash(state);
    }
}

/// A cheap scope handle carrying dynamic-membership capability.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DynamicScopeRef(pub(crate) ScopeRef);

impl DynamicScopeRef {
    /// Returns the underlying observation/control scope handle.
    #[must_use]
    pub fn as_scope(&self) -> &ScopeRef {
        &self.0
    }
}
