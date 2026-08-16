#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/tools/external-consumer/Cargo.toml"
diagnostics="$(mktemp)"
trap 'rm -f "$diagnostics"' EXIT

cargo check --locked --manifest-path "$manifest"

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
for seam in ActorIdentity DynamicRoute MailboxControl MailboxRuntime MailboxTermination; do
    if ! grep -Fq "no \`$seam\` in the root" "$diagnostics"; then
        cat "$diagnostics" >&2
        echo "installable-seam probe failed for an unexpected reason" >&2
        exit 1
    fi
done
