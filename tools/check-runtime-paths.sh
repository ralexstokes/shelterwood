#!/usr/bin/env bash
set -euo pipefail

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly source_root="crates/shelterwood/src"
readonly forbidden='\b(tokio|tokio_util|fastrand)::'

set +e
matches="$({
  # The exemption globs are anchored to the exact module path: rg matches
  # globs against the path as searched, so the bare forms `runtime.rs` and
  # `runtime/**` would respectively exempt any same-named file anywhere and
  # exempt nothing at all.
  rg --line-number \
    --glob '*.rs' \
    --glob "!$source_root/runtime.rs" \
    --glob "!$source_root/runtime/**" \
    "$forbidden" \
    "$source_root"
} 2>&1)"
status=$?
set -e

case "$status" in
  0)
    echo "runtime or randomness paths found outside the runtime module:" >&2
    echo "$matches" >&2
    exit 1
    ;;
  1)
    echo "runtime module path restriction: clean"
    ;;
  *)
    echo "$matches" >&2
    exit "$status"
    ;;
esac
