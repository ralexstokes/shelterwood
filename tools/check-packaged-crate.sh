#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="$repo_root/crates/shelterwood/Cargo.toml"

cd "$repo_root"
package_id="$(cargo pkgid --manifest-path "$manifest")"
version="${package_id##*#}"
target_dir="${CARGO_TARGET_DIR:-target}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$repo_root/$target_dir"
fi
archive="$target_dir/package/shelterwood-$version.crate"

cargo package \
  --manifest-path "$manifest" \
  --locked \
  --allow-dirty \
  --offline

if [[ ! -f "$archive" ]]; then
  echo "cargo package did not produce $archive" >&2
  exit 1
fi

unpack_root="$(mktemp -d "${TMPDIR:-/tmp}/shelterwood-package-check.XXXXXX")"
cleanup() {
  rm -rf -- "$unpack_root"
}
trap cleanup EXIT

tar -xzf "$archive" -C "$unpack_root"
unpacked="$unpack_root/shelterwood-$version"
if [[ ! -d "$unpacked" ]]; then
  echo "crate archive did not contain shelterwood-$version" >&2
  exit 1
fi

(
  cd "$unpacked"
  cargo test --locked --offline --doc --all-features
)
