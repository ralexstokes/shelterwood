#!/usr/bin/env bash
set -euo pipefail

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
