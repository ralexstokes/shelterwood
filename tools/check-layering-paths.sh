#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly source_root="crates/shelterwood/src"
readonly lib_path="$source_root/lib.rs"
readonly driver_path="$source_root/driver.rs"
readonly tree_path="$source_root/tree.rs"
readonly cells_path="$source_root/cells.rs"
readonly use_parser_path="$script_dir/check-layering-uses.awk"

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

# Direct `tree::`/`driver::` references are independent of how a source file is
# nested. Crate-root paths and use trees need module-depth-aware parsing and
# are checked below by `check-layering-uses.awk`.
readonly upward_module_pattern='\b(driver|tree)::'

# Derive the tree-layer names visible at the crate root from the source of
# truth. The token parser handles visibility modifiers, alternate/nested use
# trees, aliases, and comments. It fails closed on tree globs or unsupported
# syntax instead of silently returning an incomplete pattern.
set +e
tree_root_exports="$(awk -v mode=lib -v base_module_depth=0 -f "$use_parser_path" "$lib_path")"
tree_root_export_status=$?
set -e
if [[ "$tree_root_export_status" -ne 0 ]]; then
  exit "$tree_root_export_status"
fi
readonly tree_root_exports

tree_root_export_pattern=""
while IFS= read -r tree_root_export; do
  [[ -z "$tree_root_export" ]] && continue
  tree_root_export_pattern+="${tree_root_export_pattern:+|}${tree_root_export}"
done <<< "$tree_root_exports"
if [[ -z "$tree_root_export_pattern" ]]; then
  echo "no pub use tree re-exports derived from $lib_path" >&2
  exit 1
fi
readonly tree_root_export_pattern

check_forbidden() {
  local message="$1"
  local forbidden="$2"
  shift 2

  # With no paths `rg` falls back to scanning the working directory
  # recursively, so an empty derived set would silently widen the check
  # instead of failing it.
  if (($# == 0)); then
    echo "no files derived for check: $message" >&2
    exit 1
  fi

  local matches
  local status
  set +e
  matches="$({ rg --multiline --line-number "$forbidden" "$@"; } 2>&1)"
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

module_depth_for_file() {
  local layer_file="$1"
  local relative="${layer_file#"$source_root/"}"
  local -a components
  IFS=/ read -r -a components <<< "$relative"

  local depth="${#components[@]}"
  if [[ "${components[-1]}" == "mod.rs" ]]; then
    ((depth--))
  fi
  printf '%s\n' "$depth"
}

collect_layer_findings() {
  local layer_file
  for layer_file in "${below_driver_layers[@]}"; do
    local module_depth
    module_depth="$(module_depth_for_file "$layer_file")"

    local analysis
    local status
    set +e
    analysis="$(
      awk \
        -v mode=lower \
        -v base_module_depth="$module_depth" \
        -v forbidden_exports="$tree_root_export_pattern" \
        -f "$use_parser_path" \
        "$layer_file" 2>&1
    )"
    status=$?
    set -e
    if [[ "$status" -ne 0 ]]; then
      printf '%s\n' "$analysis"
      return "$status"
    fi

    local kind
    local line
    local detail
    while IFS=$'\t' read -r kind line detail; do
      [[ -z "$kind" ]] && continue
      printf '%s\t%s:%s\t%s\n' "$kind" "$layer_file" "$line" "$detail"
    done <<< "$analysis"
  done
}

check_layer_findings() {
  local message="$1"
  local expected_kind="$2"
  local findings="$3"
  local matches=""

  local kind
  local location
  local detail
  while IFS=$'\t' read -r kind location detail; do
    if [[ "$kind" == "$expected_kind" ]]; then
      matches+="${matches:+$'\n'}$location: $detail"
    fi
  done <<< "$findings"

  if [[ -n "$matches" ]]; then
    echo "$message" >&2
    echo "$matches" >&2
    exit 1
  fi
}

check_forbidden \
  "upward driver or tree references found below the driver layer:" \
  "$upward_module_pattern" \
  "${below_driver_layers[@]}"

set +e
layer_findings="$(collect_layer_findings)"
layer_findings_status=$?
set -e
if [[ "$layer_findings_status" -ne 0 ]]; then
  echo "$layer_findings" >&2
  exit "$layer_findings_status"
fi
readonly layer_findings

check_layer_findings \
  "upward driver or tree references found below the driver layer:" \
  module \
  "$layer_findings"
check_layer_findings \
  "upward tree root re-exports found below the driver layer:" \
  export \
  "$layer_findings"
check_layer_findings \
  "glob imports of the crate root found below the driver layer:" \
  glob \
  "$layer_findings"

# A crate-root alias can hide a forbidden name across files and nested modules.
# There is no useful lower-layer spelling that requires one, so reject the
# alias at its declaration instead of trying to reconstruct Rust name lookup.
check_layer_findings \
  "crate-root aliases found below the driver layer:" \
  alias \
  "$layer_findings"

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
