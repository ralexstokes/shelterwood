#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/tools/external-consumer/Cargo.toml"
diagnostics="$(mktemp)"
trap 'rm -f "$diagnostics"' EXIT

cargo check --locked --manifest-path "$manifest"
cargo test --locked --manifest-path "$manifest"

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
