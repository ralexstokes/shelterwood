//! Restart-stable shared member and scope state.
//!
//! These cells are the neutral synchronization and observation projection
//! layer shared by public handles, mailboxes, actor/task contexts, and the
//! mutable supervision driver. In particular, this module does not depend on
//! mutable driver state. Its dynamic-route interface retains only the one
//! close-admission hook needed by restart-stable scope transitions.

mod gate;
mod member;
mod retained;
mod scope;

pub use gate::*;
pub use member::*;
pub use retained::*;
pub use scope::*;

#[cfg(test)]
mod test_support {
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

    pub(super) fn isolated_scope(id: &str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
        let id = ChildId::from(id);
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("scope membership is available"),
        );
        resolve_options(&member);
        ScopeCell::new(member, flavor, ScopeIdentity::new())
    }

    pub(super) fn child_member(parent: &ScopeCell, id: &str) -> Arc<MemberCell> {
        let id = ChildId::from(id);
        let member = MemberCell::new(
            id.clone(),
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

    pub(super) struct ThreadProbe(pub(super) mpsc::SyncSender<std::thread::ThreadId>);

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
