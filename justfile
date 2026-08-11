# The recipes used by `ci` mirror the checks built by the `rust-env` flake
# input (the authoritative CI definitions, run via `just ci-nix`) — keep them
# in sync when overriding anything in flake.nix.

default:
    @just --list

fmt:
    cargo +nightly fmt --all --check

lint:
    cargo +nightly clippy --locked --workspace --all-targets --all-features -- -D warnings

check:
    cargo check --locked --workspace --all-targets --all-features

build:
    cargo build --locked --workspace --all-targets --all-features

test:
    cargo nextest run --locked --workspace --all-features --lib --bins --tests --examples
    cargo test --locked --workspace --doc --all-features

doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features

# Rustdoc JSON does not inline cross-crate re-exports, so the façade document
# describes only the items the façade itself defines. Every crate that
# contributes items to the public façade is therefore walked on its own
# document; `shelterwood-core` needs no walk because it has no adapter, tokio,
# or fastrand dependency for an item to name.
runtime-api-check:
    RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo +nightly rustdoc --locked -p shelterwood --all-features --lib
    cargo run --locked -p shelterwood-api-reachability -- target/doc/shelterwood.json
    RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo +nightly rustdoc --locked -p shelterwood-cells --all-features --lib
    cargo run --locked -p shelterwood-api-reachability -- target/doc/shelterwood_cells.json
    RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo +nightly rustdoc --locked -p shelterwood-mailbox --all-features --lib
    cargo run --locked -p shelterwood-api-reachability -- target/doc/shelterwood_mailbox.json

packaged-docs-sync:
    ./tools/sync-packaged-docs.sh --write

packaged-docs-check:
    ./tools/sync-packaged-docs.sh --check

nixfmt-check:
    git ls-files -z '*.nix' | xargs -0 nixfmt --check

# Fast local CI mirror — reuses the local cargo cache and incremental builds.
# The clean Nix lane retains the explicit all-target build for non-test codegen coverage.
ci: fmt lint test doc-check runtime-api-check packaged-docs-check nixfmt-check

# Full clean Nix CI lane; use before pushing or when touching Nix files.
ci-nix:
    nix flake check --no-update-lock-file

doc:
    cargo doc --workspace --no-deps --open
