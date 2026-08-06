#!/usr/bin/env bash
set -euo pipefail

readonly source_root="crates/shelterwood/src"
readonly forbidden='\b(tokio|tokio_util|fastrand)::'

set +e
matches="$({
  rg --line-number \
    --glob '*.rs' \
    --glob '!runtime.rs' \
    --glob '!runtime/**' \
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
