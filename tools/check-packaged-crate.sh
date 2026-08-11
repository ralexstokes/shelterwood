#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stage_root="$(mktemp -d "${TMPDIR:-/tmp}/shelterwood-package-check.XXXXXX")"

cleanup() {
  rm -rf -- "$stage_root"
}
trap cleanup EXIT

cd "$repo_root"

# `cargo package` cannot verify an unpublished path dependency by resolving it
# from crates.io. Ask Cargo for each package's authoritative inclusion list,
# stage exactly those real source files as a fresh workspace, and run the
# façade doctests there. This preserves the archive-content check without
# pretending the sibling implementation crates have already been published.
copy_package() {
  local manifest="$1"
  local package_root="${manifest%/Cargo.toml}"
  local destination="$stage_root/$package_root"
  local entry source target

  mkdir -p "$destination"
  while IFS= read -r entry; do
    case "$entry" in
      .cargo_vcs_info.json|Cargo.lock|Cargo.toml.orig)
        continue
        ;;
    esac
    source="$repo_root/$package_root/$entry"
    if [[ ! -f "$source" ]]; then
      echo "package list contains no real source file: $package_root/$entry" >&2
      exit 1
    fi
    target="$destination/$entry"
    mkdir -p "$(dirname "$target")"
    cp "$source" "$target"
  done < <(
    cargo package \
      --manifest-path "$manifest" \
      --locked \
      --allow-dirty \
      --list
  )
}

copy_package crates/shelterwood-core/Cargo.toml
copy_package crates/shelterwood-runtime/Cargo.toml
copy_package crates/shelterwood-mailbox/Cargo.toml
copy_package crates/shelterwood/Cargo.toml
copy_package tools/api-reachability/Cargo.toml
cp Cargo.toml Cargo.lock "$stage_root/"

(
  cd "$stage_root"
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/target}" \
    cargo test --locked --offline --doc --all-features -p shelterwood
)
