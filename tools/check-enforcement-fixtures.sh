#!/usr/bin/env bash
set -euo pipefail

# Exercises the regex enforcement scripts against known-good and known-bad
# fixture trees so a pattern regression can neither stop flagging a forbidden
# construct nor start flagging a near-miss identifier.
#
# Each case stages the skeleton tree (whose files are the known-good
# near-misses) into a scratch directory, overlays at most one seeded
# violation, and runs every enforcement script against the staged tree via
# SHELTERWOOD_ENFORCEMENT_ROOT. The script named by the case must fail with
# its own diagnostic; every other script must stay clean.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly repo_root
readonly fixtures_root="$repo_root/tools/enforcement-fixtures"
readonly scripts=(
  check-runtime-paths.sh
  check-exit-paths.sh
  check-layering-paths.sh
)

workspace="$(mktemp -d)"
readonly workspace
trap 'rm -rf "$workspace"' EXIT

failures=0

run_check() {
  local script="$1"
  local root="$2"
  set +e
  check_output="$(SHELTERWOOD_ENFORCEMENT_ROOT="$root" bash "$repo_root/tools/$script" 2>&1)"
  check_status=$?
  set -e
}

expect_case() {
  local case_name="$1"
  local flagged_script="${2:-}"
  local diagnostic="${3:-}"

  local root="$workspace/$case_name"
  cp -R "$fixtures_root/skeleton" "$root"
  if [[ -n "$flagged_script" ]]; then
    cp -R "$fixtures_root/overlays/$case_name/." "$root/"
  fi

  local script
  for script in "${scripts[@]}"; do
    run_check "$script" "$root"
    if [[ "$script" == "$flagged_script" ]]; then
      if [[ "$check_status" -eq 0 ]]; then
        echo "fixture case '$case_name': $script must flag the seeded violation" >&2
        failures=1
      elif [[ "$check_output" != *"$diagnostic"* ]]; then
        echo "fixture case '$case_name': $script reported an unexpected diagnostic:" >&2
        echo "$check_output" >&2
        failures=1
      fi
    elif [[ "$check_status" -ne 0 ]]; then
      echo "fixture case '$case_name': $script must pass but reported:" >&2
      echo "$check_output" >&2
      failures=1
    fi
  done
}

expect_case clean
expect_case runtime-tokio check-runtime-paths.sh \
  "runtime or randomness paths found outside the runtime module:"
expect_case runtime-tokio-util check-runtime-paths.sh \
  "runtime or randomness paths found outside the runtime module:"
expect_case runtime-fastrand check-runtime-paths.sh \
  "runtime or randomness paths found outside the runtime module:"
expect_case exit-downcast check-exit-paths.sh \
  "runtime type recovery found on the exit path:"
expect_case exit-downcast-ref check-exit-paths.sh \
  "runtime type recovery found on the exit path:"
expect_case exit-downcast-mut check-exit-paths.sh \
  "runtime type recovery found on the exit path:"
expect_case exit-downcast-in-driver-module check-exit-paths.sh \
  "runtime type recovery found on the exit path:"
expect_case layering-driver-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-new-module-driver-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-directory-module-driver-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-cancellation-driver-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-scope-driver-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-scope-tree-alias-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-scope-crate-root-alias-below check-layering-paths.sh \
  "crate-root aliases found below the driver layer:"
expect_case layering-scope-crate-root-glob-below check-layering-paths.sh \
  "glob imports of the crate root found below the driver layer:"
expect_case layering-scope-tree-grouped-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-scope-tree-root-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-policy-tree-root-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-observe-crate-root-alias-below check-layering-paths.sh \
  "crate-root aliases found below the driver layer:"
expect_case layering-mailbox-crate-root-glob-below check-layering-paths.sh \
  "glob imports of the crate root found below the driver layer:"
expect_case layering-nested-tree-root-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-nested-crate-root-glob-below check-layering-paths.sh \
  "glob imports of the crate root found below the driver layer:"
expect_case layering-nested-crate-root-alias-below check-layering-paths.sh \
  "crate-root aliases found below the driver layer:"
expect_case layering-crate-root-grouped-alias-below check-layering-paths.sh \
  "crate-root aliases found below the driver layer:"
expect_case layering-crate-root-glob-alias-below check-layering-paths.sh \
  "crate-root aliases found below the driver layer:"
expect_case layering-lib-alternate-tree-group-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-lib-restricted-tree-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-lib-commented-tree-group-reexport-below check-layering-paths.sh \
  "upward tree root re-exports found below the driver layer:"
expect_case layering-lib-tree-glob-fails-closed check-layering-paths.sh \
  "tree glob imports cannot be derived safely"
expect_case layering-tree-below check-layering-paths.sh \
  "upward driver or tree references found below the driver layer:"
expect_case layering-tree-in-driver check-layering-paths.sh \
  "upward tree references found in the driver layer:"
expect_case layering-plan-in-cells check-layering-paths.sh \
  "plan references found in the restart-stable cell layer:"
expect_case layering-directory-module-plan-in-cells check-layering-paths.sh \
  "plan references found in the restart-stable cell layer:"
expect_case layering-tree-in-driver-module check-layering-paths.sh \
  "upward tree references found in the driver layer:"
expect_case layering-resolve-common check-layering-paths.sh \
  "child option resolution escaped the shared plan funnel:"
expect_case layering-checked-id check-layering-paths.sh \
  "dynamic child-id validation escaped the reservation boundary:"

if [[ "$failures" -ne 0 ]]; then
  echo "enforcement fixture check failed" >&2
  exit 1
fi

echo "enforcement fixture coverage: clean"
