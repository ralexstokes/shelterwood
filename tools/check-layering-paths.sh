#!/usr/bin/env bash
set -euo pipefail

readonly lower_layers=(
  crates/shelterwood/src/mailbox.rs
  crates/shelterwood/src/raw.rs
  crates/shelterwood/src/task.rs
)
readonly cells_path="crates/shelterwood/src/cells.rs"

check_forbidden() {
  local message="$1"
  local forbidden="$2"
  shift 2

  local matches
  local status
  set +e
  matches="$({ rg --line-number "$forbidden" "$@"; } 2>&1)"
  status=$?
  set -e

  case "$status" in
    0)
      echo "$message" >&2
      echo "$matches" >&2
      exit 1
      ;;
    1) ;;
    *)
      echo "$matches" >&2
      exit "$status"
      ;;
  esac
}

check_forbidden \
  "upward driver references found below the driver layer:" \
  '\bdriver::' \
  "${lower_layers[@]}"

check_forbidden \
  "upward driver or tree references found in the shared cells layer:" \
  '\b(driver|tree)::' \
  "$cells_path"

echo "shared-cell layering restrictions: clean"
