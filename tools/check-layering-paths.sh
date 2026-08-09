#!/usr/bin/env bash
set -euo pipefail

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly driver_path="crates/shelterwood/src/driver.rs"
readonly tree_path="crates/shelterwood/src/tree.rs"

# Every top-level Rust module except the two orchestration layers belongs below
# the driver. Derive the set so adding a module cannot silently bypass this
# check; lib.rs is the crate root and is allowed to wire every layer together.
below_driver_layers=()
for path in crates/shelterwood/src/*.rs; do
  case "$path" in
    "$driver_path"|"$tree_path"|crates/shelterwood/src/lib.rs) ;;
    *) below_driver_layers+=("$path") ;;
  esac
done
readonly -a below_driver_layers

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

check_forbidden \
  "child option resolution escaped the shared plan funnel:" \
  '\bresolve_common\b' \
  "$driver_path"

check_forbidden \
  "dynamic child-id validation escaped the reservation boundary:" \
  '\bchecked_id\b' \
  "$tree_path"

echo "supervision layering restrictions: clean"
