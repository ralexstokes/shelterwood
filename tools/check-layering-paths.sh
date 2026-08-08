#!/usr/bin/env bash
set -euo pipefail

readonly below_driver_layers=(
  crates/shelterwood/src/actor.rs
  crates/shelterwood/src/cells.rs
  crates/shelterwood/src/deadline.rs
  crates/shelterwood/src/definition.rs
  crates/shelterwood/src/engine.rs
  crates/shelterwood/src/exit.rs
  crates/shelterwood/src/identity.rs
  crates/shelterwood/src/mailbox.rs
  crates/shelterwood/src/observe.rs
  crates/shelterwood/src/plan.rs
  crates/shelterwood/src/policy.rs
  crates/shelterwood/src/raw.rs
  crates/shelterwood/src/runtime.rs
  crates/shelterwood/src/task.rs
)
readonly driver_path="crates/shelterwood/src/driver.rs"

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
  "upward driver or tree references found below the driver layer:" \
  '\b(driver|tree)::' \
  "${below_driver_layers[@]}"

check_forbidden \
  "upward tree references found in the driver layer:" \
  '\btree::' \
  "$driver_path"

echo "supervision layering restrictions: clean"
