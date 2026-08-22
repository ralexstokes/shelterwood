#!/usr/bin/env bash
set -euo pipefail

# SPEC §16.13 lets cross-crate core re-exports stay opaque to the façade's
# rustdoc-JSON reachability walk because `shelterwood-core`'s manifest is
# runtime-free: a doc-hidden core signature cannot name an adapter type its
# crate cannot reach. That argument is only structural while CI asserts it,
# so this check pins core's direct normal dependencies to the allowlist
# below. Growing the list is a deliberate boundary decision, not a
# convenience: every crate added here becomes nameable by doc-hidden
# signatures no reachability gate ever sees.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

expected="thiserror"
actual="$(
    cargo tree --locked --manifest-path "$repo_root/Cargo.toml" \
        -p shelterwood-core -e normal --depth 1 --prefix none |
        tail -n +2 | awk '{print $1}' | sort -u
)"

if [ "$actual" != "$expected" ]; then
    echo "shelterwood-core's direct normal dependencies changed:" >&2
    diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
    echo "update tools/check-core-manifest.sh only as a deliberate SPEC §16.13 boundary decision" >&2
    exit 1
fi
