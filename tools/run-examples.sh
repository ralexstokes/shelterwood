#!/usr/bin/env sh
# Run every example of the façade crate, in the same (glob-sorted) order in
# local and clean Nix checks. The list is discovered rather than hard-coded so
# a new example cannot silently skip this gate.
# Invoke from the repository root inside its devshell (./tools/dev just examples).
set -eu

found=0
for path in crates/shelterwood/examples/*.rs crates/shelterwood/examples/*/main.rs; do
    [ -f "$path" ] || continue
    found=1
    case "$path" in
        crates/shelterwood/examples/*/main.rs) example="$(basename "$(dirname "$path")")" ;;
        *) example="$(basename "$path" .rs)" ;;
    esac
    cargo run --locked -p shelterwood --example "$example"
done

if [ "$found" -eq 0 ]; then
    echo "run-examples: no examples found under crates/shelterwood/examples" >&2
    exit 1
fi
