#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/tools/external-consumer/Cargo.toml"
diagnostics="$(mktemp)"
trap 'rm -f "$diagnostics"' EXIT

cargo check --locked --manifest-path "$manifest"
cargo test --locked --manifest-path "$manifest"

if cargo check --locked --manifest-path "$manifest" --features subtree-ref-conversion >"$diagnostics" 2>&1; then
    echo "external consumers can retype a ScopeRef through Subtree's supertrait" >&2
    exit 1
fi
if ! grep -Fq 'argument #2 of type' "$diagnostics" || ! grep -Fq 'RefToken' "$diagnostics"; then
    cat "$diagnostics" >&2
    echo "subtree-ref-conversion probe failed for an unexpected reason" >&2
    exit 1
fi

if cargo check --locked --manifest-path "$manifest" --features exit-new >"$diagnostics" 2>&1; then
    echo "external consumers can construct an Exit from an arbitrary ExitKind" >&2
    exit 1
fi
if ! grep -Fq 'no associated function or constant named `new` found for struct `Exit`' "$diagnostics"; then
    cat "$diagnostics" >&2
    echo "Exit::new probe failed for an unexpected reason" >&2
    exit 1
fi

if cargo check --locked --manifest-path "$manifest" --features from-latch >"$diagnostics" 2>&1; then
    echo "external consumers can call CancellationToken::from_latch" >&2
    exit 1
fi
if ! grep -Fq 'associated function `from_latch` is private' "$diagnostics"; then
    cat "$diagnostics" >&2
    echo "from_latch probe failed for an unexpected reason" >&2
    exit 1
fi

if cargo check --locked --manifest-path "$manifest" --features lifecycle-capacity >"$diagnostics" 2>&1; then
    echo "the supported façade exports its lifecycle buffer capacity" >&2
    exit 1
fi
if ! grep -Fq 'no `LIFECYCLE_EVENT_CAPACITY` in the root' "$diagnostics"; then
    cat "$diagnostics" >&2
    echo "lifecycle-capacity probe failed for an unexpected reason" >&2
    exit 1
fi

if cargo check --locked --manifest-path "$manifest" --features installable-seams >"$diagnostics" 2>&1; then
    echo "the supported façade exports private installation seams" >&2
    exit 1
fi
# Match each seam in the E0432 header (`unresolved imports ...`), not the
# per-span "no `X` in the root" labels: rustc caps rendered span labels, so
# a list this long leaves later names labelless while the header stays
# complete. The trailing backtick keeps a seam from matching a longer name
# that shares its prefix.
for seam in \
    BoxedSleep \
    DynamicRoute \
    MailboxCell \
    MailboxControl \
    MailboxEffectQueue \
    MailboxEffectSink \
    MailboxTermination \
    MemberCell \
    ParentCancellationToken \
    ProxiedPoll \
    ProxiedSleep \
    ScopeCell \
    WakerAction \
    WakerEffects \
    WakerSlot \
    actor_ref_from_parts
do
    if ! grep -Fq "\`shelterwood::$seam\`" "$diagnostics"; then
        cat "$diagnostics" >&2
        echo "installable-seam probe failed for an unexpected reason" >&2
        exit 1
    fi
done
