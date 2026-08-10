#!/usr/bin/env bash
set -euo pipefail

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly source_root="crates/shelterwood/src"
readonly driver_path="$source_root/driver.rs"
readonly tree_path="$source_root/tree.rs"
readonly cells_path="$source_root/cells.rs"
readonly scope_path="$source_root/scope.rs"

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

# A direct `tree::`/`driver::` reference is the usual spelling, while the
# longer alternatives cover aliases imported from either the crate root or a
# grouped root import. `--multiline` below lets the grouped form span the
# rustfmt-normalized lines of a `use crate::{ ... };` statement.
readonly upward_module_pattern='\b(driver|tree)::|\b(crate|super)::(driver|tree)\b|\b(crate|super)::\{[^;]*\b(driver|tree)\b[[:space:]]*(,|}|as\b)'

# `lib.rs` re-exports these tree-layer types at the crate root. Naming one via
# `crate::System`, for example, is still an upward dependency even though the
# source contains no `tree::` token.
readonly tree_root_export_pattern='ActorSlot|Admission|BuildError|DynamicActorSlot|DynamicSubtreeSlot|DynamicTaskSlot|DynamicTree|Removal|StartOrShutdownError|Subtree|SubtreeDef|SubtreeOnceDef|SubtreeSlot|System|TaskSlot|Tree'

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

collect_scope_root_aliases() {
  local pattern="$1"
  local aliases
  local status
  set +e
  aliases="$({ rg --multiline --no-filename --only-matching --replace '$2' "$pattern" "$scope_path"; } 2>&1)"
  status=$?
  set -e

  case "$status" in
    0) printf '%s\n' "$aliases" ;;
    1) ;;
    *)
      echo "$aliases" >&2
      exit "$status"
      ;;
  esac
}

check_forbidden \
  "upward driver or tree references found below the driver layer:" \
  "$upward_module_pattern" \
  "${below_driver_layers[@]}"

check_forbidden \
  "upward tree root re-exports found in the scope layer:" \
  "\\b(crate|super)::($tree_root_export_pattern)\\b|\\buse[[:space:]]+(crate|super)::\\{[^;]*\\b($tree_root_export_pattern)\\b" \
  "$scope_path"

# A glob import of the crate root pulls in every tree-layer re-export without
# naming any of them, so neither pattern above can see it. The grouped
# alternative covers a `*` anywhere inside a `use (crate|super)::{ ... };`
# tree, which `--multiline` lets span rustfmt-normalized lines.
check_forbidden \
  "glob imports of the crate root found in the scope layer:" \
  '\buse[[:space:]]+(crate|super)::(\*|\{[^;]*\*)' \
  "$scope_path"

# A crate-root alias can hide both direct root exports and the module names
# checked above. Extract direct and grouped `self` aliases, then scan uses of
# each exact identifier so unrelated aliases remain permitted.
scope_root_aliases="$(
  collect_scope_root_aliases '\buse[[:space:]]+(crate|super)[[:space:]]+as[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;'
  collect_scope_root_aliases '\buse[[:space:]]+(crate|super)::\{[^;]*\bself[[:space:]]+as[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*(,|})'
)"
while IFS= read -r scope_root_alias; do
  [[ -z "$scope_root_alias" ]] && continue
  check_forbidden \
    "upward tree root re-exports found in the scope layer:" \
    "\\b${scope_root_alias}::($tree_root_export_pattern|driver|tree)\\b" \
    "$scope_path"
done <<< "$scope_root_aliases"

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
