//! Membership-owned actor mailboxes and request/reply capabilities.
//!
//! The split below is structural enforcement, not tidiness. Supported façade
//! types are re-exported `pub`; every cross-crate implementation seam is
//! re-exported `pub(crate)` or not at all, so a `pub use mailbox::…` added to
//! the crate root fails to compile — E0365 for a seam listed here, E0432 for
//! one that is not — instead of relying on a maintained deny list. The sibling
//! `cells` shim gets the same protection from its `pub(crate) use … ::*`.

pub use shelterwood_mailbox::{
    ActorRef, CallError, CallErrorKind, CallFuture, Replied, Reply, ReplyError, ReplyReceive,
    ReplyReceiver, SendError, SendErrorKind, SendFuture, SendTimeout,
};

pub(crate) use shelterwood_mailbox::{
    AcceptedSequence, MailboxCell, MailboxControl, MailboxReceiver, actor_ref_from_parts,
};
