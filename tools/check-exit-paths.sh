#!/usr/bin/env bash
set -euo pipefail

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly forbidden='[.]downcast(_ref|_mut)?([[:space:]]*)?(::)?[<(]'
readonly exit_path=(
  crates/shelterwood/src/driver.rs
  crates/shelterwood/src/engine.rs
  crates/shelterwood/src/exit.rs
)

set +e
matches="$({ rg --line-number "$forbidden" "${exit_path[@]}"; } 2>&1)"
status=$?
set -e

case "$status" in
  0)
    echo "runtime type recovery found on the exit path:" >&2
    echo "$matches" >&2
    exit 1
    ;;
  1)
    echo "exit-path downcast restriction: clean"
    ;;
  *)
    echo "$matches" >&2
    exit "$status"
    ;;
esac
