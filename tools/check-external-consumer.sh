#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/tools/external-consumer/Cargo.toml"
diagnostics="$(mktemp)"
trap 'rm -f "$diagnostics"' EXIT

cargo check --locked --manifest-path "$manifest"
cargo test --locked --manifest-path "$manifest"

if cargo check --locked --manifest-path "$manifest" --features from-latch >"$diagnostics" 2>&1; then
    echo "external consumers can call CancellationToken::from_latch" >&2
    exit 1
fi
if ! grep -Fq 'associated function `from_latch` is private' "$diagnostics"; then
    cat "$diagnostics" >&2
    echo "from_latch probe failed for an unexpected reason" >&2
    exit 1
fi

if cargo check --locked --manifest-path "$manifest" --features installable-seams >"$diagnostics" 2>&1; then
    echo "the supported façade exports lower-crate installation seams" >&2
    exit 1
fi
for seam in \
    ActorIdentity \
    DynamicRoute \
    MailboxCell \
    MailboxControl \
    MailboxRuntime \
    MailboxTermination \
    MemberCell \
    ParentCancellationToken \
    ScopeCell \
    actor_ref_from_parts
do
    if ! grep -Fq "no \`$seam\` in the root" "$diagnostics"; then
        cat "$diagnostics" >&2
        echo "installable-seam probe failed for an unexpected reason" >&2
        exit 1
    fi
done

if cargo check --locked --manifest-path "$manifest" --features sealed-mailbox-seams >"$diagnostics" 2>&1; then
    echo "external consumers can implement sealed mailbox seams" >&2
    exit 1
fi
# The unsatisfied-bound error names the sealed supertrait whether or not `mod
# private` is reachable, so a bare name grep would still pass against a
# `pub mod private`. Only rustc's "which is not accessible" note distinguishes
# a genuine seal from a consumer that merely forgot a supertrait impl.
for seam in SealedMailboxControl SealedMailboxTermination; do
    if ! grep -Fq "\`shelterwood_mailbox::private::$seam\`, which is not accessible" "$diagnostics"; then
        cat "$diagnostics" >&2
        echo "sealed-mailbox-seam probe failed for an unexpected reason" >&2
        exit 1
    fi
done

# Belt and braces for the same property, stated directly: the seal module
# itself must be unnameable from outside its defining crate.
if cargo check --locked --manifest-path "$manifest" --features private-seal-module >"$diagnostics" 2>&1; then
    echo "external consumers can name the mailbox seal module" >&2
    exit 1
fi
if ! grep -Fq 'module `private` is private' "$diagnostics"; then
    cat "$diagnostics" >&2
    echo "private-seal-module probe failed for an unexpected reason" >&2
    exit 1
fi
