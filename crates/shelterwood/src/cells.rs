//! Restart-stable state, cancellation, and observation projections.
//!
//! The module is structurally below Shelterwood's mutable driver: it owns the
//! neutral synchronization and observation projection layer shared by public
//! handles, mailboxes, actor/task contexts, and the driver itself.

mod admission;
mod cancellation;
mod gate;
mod member;
mod observe;
mod retained;
mod scope;

pub use admission::*;
pub use cancellation::*;
pub(crate) use gate::*;
pub(crate) use member::*;
pub use observe::*;
pub(crate) use retained::*;
pub(crate) use scope::*;

/// Shared test constructors for the restart-stable cell graph.
///
/// `pub(crate)` because the driver tests build the same isolated fixtures:
/// keeping one construction path means a change to construction invariants
/// cannot silently leave one suite exercising a path production never takes.
#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        fmt,
        sync::{Arc, mpsc},
        time::Duration,
    };

    use shelterwood_core::{
        ChildId,
        identity::ScopeIdentity,
        policy::{
            ChildMode, CommonOptions, Readiness, ResolvedDefaults, ScopeFlavor, resolve_common,
        },
    };

    use super::{MemberCell, ScopeCell};

    pub(super) const TEST_WAIT: Duration = Duration::from_secs(10);

    pub(super) fn resolve_options(member: &MemberCell) {
        member.set_options(resolve_common(
            &CommonOptions::default(),
            &ResolvedDefaults::default(),
            ChildMode::Restartable,
            Readiness::Immediate,
        ));
    }

    /// The one isolated-scope construction path. `configure` runs on the root
    /// member before the scope cell wraps it; the cells tests resolve options
    /// there, while the driver fixtures pass a no-op.
    pub(crate) fn isolated_scope_with(
        id: &str,
        flavor: ScopeFlavor,
        configure: impl FnOnce(&MemberCell),
    ) -> Arc<ScopeCell> {
        let id = ChildId::from(id);
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            identity
                .mint_membership(&id)
                .expect("scope membership is available"),
        );
        configure(&member);
        ScopeCell::new(member, flavor, ScopeIdentity::new())
    }

    pub(super) fn isolated_scope(id: &str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
        isolated_scope_with(id, flavor, resolve_options)
    }

    pub(super) fn child_member(parent: &ScopeCell, id: &str) -> Arc<MemberCell> {
        let id = ChildId::from(id);
        let member = MemberCell::new(
            parent
                .mint_membership(&id)
                .expect("child membership is available"),
        );
        resolve_options(&member);
        member
    }

    pub(super) fn child_scope(parent: &ScopeCell, id: &str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
        ScopeCell::new(child_member(parent, id), flavor, ScopeIdentity::new())
    }

    /// A user error payload that reports the thread its destructor ran on.
    ///
    /// Shared with the driver tests as their venue probe for a *losing*
    /// application error: the framework selects a different verdict, so the
    /// loser is never published and its destruction thread is the only
    /// observable it leaves behind.
    pub(crate) struct ThreadProbe(pub(crate) mpsc::SyncSender<std::thread::ThreadId>);

    impl fmt::Debug for ThreadProbe {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ThreadProbe")
        }
    }

    impl fmt::Display for ThreadProbe {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("thread probe")
        }
    }

    impl std::error::Error for ThreadProbe {}

    impl Drop for ThreadProbe {
        fn drop(&mut self) {
            let _ = self.0.send(std::thread::current().id());
        }
    }
}
