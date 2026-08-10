//! Ordered and dynamic scope handles.

use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use crate::{
    cells::{MemberStage, ScopeCell},
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

/// Declares the shared synchronous observation/control surface once for both
/// handle types: `ScopeRef` gets the inherent method bodies and
/// `DynamicScopeRef` gets an identical inherent forward, so a method added
/// here can never silently miss the dynamic handle (UFCS and rustdoc
/// included). The `Deref` impl below additionally backstops methods added
/// outside this macro.
macro_rules! impl_scope_ref_sync_surface {
    (
        $(
            $(#[$attribute:meta])*
            fn $method:ident $(<$($generic:ident),+>)? (
                &$receiver:ident $(, $argument:ident: $argument_type:ty)* $(,)?
            ) $(-> $output:ty)?
            $(where [$($constraint:tt)*])?
            $body:block
        )*
    ) => {
        impl ScopeRef {
            $(
                $(#[$attribute])*
                pub fn $method $(<$($generic),+>)? (
                    &$receiver,
                    $($argument: $argument_type),*
                ) $(-> $output)?
                $(where $($constraint)*)?
                $body
            )*
        }

        impl DynamicScopeRef {
            $(
                $(#[$attribute])*
                pub fn $method $(<$($generic),+>)? (
                    &$receiver,
                    $($argument: $argument_type),*
                ) $(-> $output)?
                $(where $($constraint)*)?
                {
                    $receiver.0.$method($($argument),*)
                }
            )*
        }
    };
}

/// The asynchronous half of the shared surface; see
/// [`impl_scope_ref_sync_surface`]. `shutdown_and_wait` is deliberately not
/// declared here: it calls into the driver, which the scope layer must not
/// reference, so both handles hand-forward it from `tree.rs` instead.
macro_rules! impl_scope_ref_async_surface {
    (
        $(
            $(#[$attribute:meta])*
            fn $method:ident $(<$($generic:ident),+>)? (
                &$receiver:ident $(, $argument:ident: $argument_type:ty)* $(,)?
            ) $(-> $output:ty)?
            $(where [$($constraint:tt)*])?
            $body:block
        )*
    ) => {
        impl ScopeRef {
            $(
                $(#[$attribute])*
                pub async fn $method $(<$($generic),+>)? (
                    &$receiver,
                    $($argument: $argument_type),*
                ) $(-> $output)?
                $(where $($constraint)*)?
                $body
            )*
        }

        impl DynamicScopeRef {
            $(
                $(#[$attribute])*
                pub async fn $method $(<$($generic),+>)? (
                    &$receiver,
                    $($argument: $argument_type),*
                ) $(-> $output)?
                $(where $($constraint)*)?
                {
                    $receiver.0.$method($($argument),*).await
                }
            )*
        }
    };
}

impl_scope_ref_sync_surface! {
    /// Returns this scope's child id within its parent.
    #[must_use]
    fn id(&self) -> &ChildId {
        self.cell.member.id()
    }

    /// Returns the scope membership identity.
    #[must_use]
    fn membership(&self) -> Membership {
        self.cell.member.membership()
    }

    /// Computes an authoritative recursive snapshot on demand.
    #[must_use]
    fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.cell.snapshot()
    }

    /// Subscribes to conflated recursive snapshots.
    #[must_use]
    fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.cell.subscribe_snapshots()
    }

    /// Subscribes to this scope's lifecycle and all forwarded descendants.
    #[must_use]
    fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.cell.subscribe_lifecycle()
    }

    /// Looks up a direct child in an authoritative current snapshot.
    #[must_use]
    fn child(&self, id: impl AsRef<str>) -> Option<ChildSnapshot> {
        self.snapshot().child(id).cloned()
    }

    /// Traverses a child-id path in an authoritative current snapshot.
    #[must_use]
    fn descendant<I, S>(&self, path: I) -> Option<ChildSnapshot>
    where [
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    ] {
        self.snapshot().descendant(path).cloned()
    }

    /// Requests shutdown without waiting.
    fn request_shutdown(&self) {
        let _ = self.cell.request_shutdown();
    }
}

impl_scope_ref_async_surface! {
    /// Waits for a named child snapshot satisfying an at-or-past predicate.
    ///
    /// Snapshot watches conflate intermediate states, so `pred` should accept
    /// every state at or beyond the desired edge and must remain cheap and
    /// non-blocking.
    fn wait_for_child<P>(
        &self,
        id: impl Into<ChildId>,
        pred: P,
        timeout: Duration,
    ) -> Result<ChildSnapshot, WaitError>
    where [P: FnMut(&ChildSnapshot) -> bool + Send]
    {
        let id = id.into();
        let mut pred = pred;
        let expires = crate::deadline::Deadline::after(crate::runtime::now(), timeout);
        let mut snapshots = self.subscribe_snapshots();

        loop {
            let snapshot = snapshots.borrow_latest();
            if let Some(child) = snapshot.child(id.as_str())
                && pred(child)
            {
                return Ok(child.clone());
            }
            // Inspect the current snapshot before rejecting even an already
            // elapsed (including zero-duration) budget.
            if expires.is_due(crate::runtime::now()) {
                return Err(WaitError::TimedOut);
            }
            if matches!(self.cell.member.record().stage, MemberStage::Terminal(_)) {
                return Err(WaitError::ScopeTerminated {
                    state: snapshot.state.clone(),
                });
            }
            match crate::runtime::select_two(snapshots.changed(), async {
                match expires.instant() {
                    Some(expires) => crate::runtime::sleep_until_std(expires).await,
                    None => std::future::pending().await,
                }
            })
            .await
            {
                crate::runtime::Either::Left(Ok(_)) => {}
                crate::runtime::Either::Left(Err(_)) => {
                    let snapshot = snapshots.borrow_latest();
                    if let Some(child) = snapshot.child(id.as_str())
                        && pred(child)
                    {
                        return Ok(child.clone());
                    }
                    return Err(WaitError::ScopeTerminated {
                        state: snapshot.state.clone(),
                    });
                }
                crate::runtime::Either::Right(()) => {
                    let snapshot = snapshots.borrow_latest();
                    if let Some(child) = snapshot.child(id.as_str())
                        && pred(child)
                    {
                        return Ok(child.clone());
                    }
                    return Err(WaitError::TimedOut);
                }
            }
        }
    }

    /// Waits for terminal membership state.
    fn wait_stopped(&self) -> StopReason {
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

// Handle identity is the slot cell, not the membership token: lowering a
// rebuilt nested declaration rebases the token behind live pre-spawn handles,
// and a token-value hash would strand entries keyed before the rebase.
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

/// Backstop for methods declared outside the shared-surface macros: any
/// future `ScopeRef` method remains reachable on `DynamicScopeRef` through
/// deref, even before the shared declaration is updated.
impl Deref for DynamicScopeRef {
    type Target = ScopeRef;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
