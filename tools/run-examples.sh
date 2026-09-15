#!/usr/bin/env sh
# Run the asserting examples in the same order in local and clean Nix checks.
# Invoke from the repository root inside its devshell (./tools/dev just examples).
set -eu

for example in quickstart request_reply supervision_restart \
    ordered_startup dynamic_scope graceful_shutdown observation \
    cyclic_wiring embedding; do
    cargo run --locked -p shelterwood --example "$example"
done
