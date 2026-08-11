#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stage_root="$(mktemp -d "${TMPDIR:-/tmp}/shelterwood-package-check.XXXXXX")"
package_home="$stage_root/package-home"
package_target="$stage_root/package-target"
doctest_target="$stage_root/doctest-target"
vendor_root="$stage_root/vendor"
cleanup() {
  rm -rf -- "$stage_root"
}
trap cleanup EXIT

cd "$repo_root"

# The publishable crates depend on sibling versions that are not on crates.io
# yet. Build an isolated directory source from the locked third-party
# dependencies, then add each exact archive to it in dependency order. This
# keeps package normalization honest even in Nix, where crates.io is already
# replaced by a read-only vendored source.
cargo vendor --locked --offline --respect-source-config "$vendor_root" >/dev/null
mkdir -p "$package_home"
cat > "$package_home/config.toml" <<EOF
[source.crates-io]
replace-with = "package-check-vendor"

[source.package-check-vendor]
directory = "$vendor_root"
EOF

add_archive_to_vendor() {
  local archive="$1"
  local member="$2"
  local destination="$vendor_root/$member"
  local package_checksum relative file_checksum separator

  tar -xzf "$archive" -C "$vendor_root"
  package_checksum="$(sha256sum "$archive")"
  package_checksum="${package_checksum%% *}"
  separator=""
  {
    printf '{"files":{'
    while IFS= read -r -d '' relative; do
      file_checksum="$(sha256sum "$destination/$relative")"
      file_checksum="${file_checksum%% *}"
      printf '%s"%s":"%s"' "$separator" "$relative" "$file_checksum"
      separator=","
    done < <(
      find "$destination" -type f ! -name .cargo-checksum.json -printf '%P\0' \
        | sort -z
    )
    printf '},"package":"%s"}\n' "$package_checksum"
  } > "$destination/.cargo-checksum.json"
}

packages=(
  shelterwood-core
  shelterwood-runtime
  shelterwood-mailbox
  shelterwood
)
members=()
for package in "${packages[@]}"; do
  package_id="$(cargo pkgid -p "$package")"
  version="${package_id##*#}"
  member="$package-$version"
  CARGO_HOME="$package_home" CARGO_TARGET_DIR="$package_target" \
    cargo package \
      -p "$package" \
      --registry crates-io \
      --no-verify \
      --locked \
      --allow-dirty \
      --offline
  archive="$package_target/package/$package-$version.crate"
  if [[ ! -f "$archive" ]]; then
    echo "cargo package did not produce $archive" >&2
    exit 1
  fi
  add_archive_to_vendor "$archive" "$member"
  tar -xzf "$archive" -C "$stage_root"
  members+=("$member")
done

# Compile every library from the normalized archive manifests and then run the
# facade's packaged rustdoc tests from those same extracted archives.
cat > "$stage_root/Cargo.toml" <<EOF
[workspace]
members = [
  "${members[0]}",
  "${members[1]}",
  "${members[2]}",
  "${members[3]}",
]
resolver = "3"

[patch.crates-io]
shelterwood-core = { path = "${members[0]}" }
shelterwood-runtime = { path = "${members[1]}" }
shelterwood-mailbox = { path = "${members[2]}" }
EOF

(
  cd "$stage_root"
  CARGO_HOME="$package_home" \
    cargo generate-lockfile --offline
  CARGO_HOME="$package_home" CARGO_TARGET_DIR="$doctest_target" \
    cargo check --locked --offline --workspace --all-features
  CARGO_HOME="$package_home" CARGO_TARGET_DIR="$doctest_target" \
    cargo test --locked --offline --doc --all-features -p shelterwood
)
