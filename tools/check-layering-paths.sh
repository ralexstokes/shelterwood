#!/usr/bin/env bash
set -euo pipefail

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly source_root="crates/shelterwood/src"
readonly driver_path="$source_root/driver.rs"
readonly tree_path="$source_root/tree.rs"
readonly cells_path="$source_root/cells.rs"

# Every Rust source except the two orchestration modules and their submodules
# belongs below the driver. Derive all four sets recursively so either a flat
# module or the conventional `module/mod.rs` layout cannot silently bypass the
# check; lib.rs is the crate root and may wire every layer together.
below_driver_layers=()
driver_layers=()
tree_layers=()
cells_layers=()
while IFS= read -r -d '' path; do
  case "$path" in
    "$source_root/lib.rs") ;;
    "$driver_path"|"$source_root/driver/"*) driver_layers+=("$path") ;;
    "$tree_path"|"$source_root/tree/"*) tree_layers+=("$path") ;;
    "$cells_path"|"$source_root/cells/"*)
      cells_layers+=("$path")
      below_driver_layers+=("$path")
      ;;
    *) below_driver_layers+=("$path") ;;
  esac
done < <(find "$source_root" -type f -name '*.rs' -print0)
readonly -a below_driver_layers
readonly -a driver_layers
readonly -a tree_layers
readonly -a cells_layers

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
  "${driver_layers[@]}"

check_forbidden \
  "plan references found in the restart-stable cell layer:" \
  '\bplan::' \
  "${cells_layers[@]}"

check_forbidden \
  "child option resolution escaped the shared plan funnel:" \
  '\bresolve_common\b' \
  "${driver_layers[@]}"

check_forbidden \
  "dynamic child-id validation escaped the reservation boundary:" \
  '\bchecked_id\b' \
  "${tree_layers[@]}"

echo "supervision layering restrictions: clean"
