#!/usr/bin/env bash
set -euo pipefail

# The fixture harness re-points this scan at a mirrored source tree; the
# default scans the repository from the invoking directory as before.
cd "${SHELTERWOOD_ENFORCEMENT_ROOT:-.}"

readonly source_root="crates/shelterwood/src"
readonly lib_path="$source_root/lib.rs"
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

# A direct `tree::`/`driver::` reference is the usual spelling, while the
# longer alternatives cover aliases imported from either the crate root or a
# grouped root import. `--multiline` below lets the grouped form span the
# rustfmt-normalized lines of a `use crate::{ ... };` statement.
readonly upward_module_pattern='\b(driver|tree)::|\b(crate|super)::(driver|tree)\b|\b(crate|super)::\{[^;]*\b(driver|tree)\b[[:space:]]*(,|}|as\b)'

# Derive the tree-layer names visible at the crate root from the source of
# truth. The parser accepts rustfmt's grouped form plus a single-item re-export
# and honors `as` aliases, which are the identifiers lower layers can name.
set +e
tree_root_exports="$(
  awk '
    function trim(value) {
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+$/, "", value)
      return value
    }

    function emit(item, parts, count) {
      sub(/\/\/.*/, "", item)
      item = trim(item)
      if (item == "") {
        return
      }
      count = split(item, parts, /[[:space:]]+as[[:space:]]+/)
      item = trim(parts[count])
      if (item !~ /^[A-Za-z_][A-Za-z0-9_]*$/) {
        print "unsupported tree re-export item in lib.rs: " item > "/dev/stderr"
        invalid = 1
        return
      }
      print item
    }

    BEGIN {
      prefix = "^[[:space:]]*pub[[:space:]]+use[[:space:]]+((crate|self)::)?tree::"
      group_prefix = prefix "[{]"
    }

    {
      line = $0
      if (!in_group) {
        if (line ~ group_prefix) {
          sub(group_prefix, "", line)
          in_group = 1
        } else if (line ~ prefix) {
          sub(prefix, "", line)
          sub(/[[:space:]]*;.*/, "", line)
          emit(line)
          next
        } else {
          next
        }
      }

      if (line ~ /[}][[:space:]]*;/) {
        sub(/[}][[:space:]]*;.*/, "", line)
        in_group = 0
      }
      item_count = split(line, items, ",")
      for (item_index = 1; item_index <= item_count; item_index++) {
        emit(items[item_index])
      }
    }

    END {
      if (in_group) {
        print "unterminated pub use tree group in lib.rs" > "/dev/stderr"
        invalid = 1
      }
      if (invalid) {
        exit 2
      }
    }
  ' "$lib_path"
)"
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

collect_root_aliases() {
  local pattern="$1"
  local layer_file="$2"
  local aliases
  local status
  set +e
  aliases="$({ rg --multiline --no-filename --only-matching --replace '$2' "$pattern" "$layer_file"; } 2>&1)"
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
  "upward tree root re-exports found below the driver layer:" \
  "\\b(crate|super)::($tree_root_export_pattern)\\b|\\buse[[:space:]]+(crate|super)::\\{[^;]*\\b($tree_root_export_pattern)\\b" \
  "${below_driver_layers[@]}"

# A glob import of the crate root pulls in every tree-layer re-export without
# naming any of them, so neither pattern above can see it. The grouped
# alternative covers a `*` anywhere inside a `use (crate|super)::{ ... };`
# tree, which `--multiline` lets span rustfmt-normalized lines.
check_forbidden \
  "glob imports of the crate root found below the driver layer:" \
  '\buse[[:space:]]+(crate|super)::(\*|\{[^;]*\*)' \
  "${below_driver_layers[@]}"

# A crate-root alias can hide both direct root exports and the module names
# checked above. Extract direct and grouped `self` aliases, then scan uses of
# each exact identifier in the file that declared it so unrelated aliases in
# another module remain permitted.
for layer_file in "${below_driver_layers[@]}"; do
  root_aliases="$(
    collect_root_aliases '\buse[[:space:]]+(crate|super)[[:space:]]+as[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;' "$layer_file"
    collect_root_aliases '\buse[[:space:]]+(crate|super)::\{[^;]*\bself[[:space:]]+as[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*(,|})' "$layer_file"
  )"
  while IFS= read -r root_alias; do
    [[ -z "$root_alias" ]] && continue
    check_forbidden \
      "upward tree root re-exports found below the driver layer:" \
      "\\b${root_alias}::($tree_root_export_pattern|driver|tree)\\b" \
      "$layer_file"
  done <<< "$root_aliases"
done

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
