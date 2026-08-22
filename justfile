# The recipes used by `ci` mirror the checks built by the `rust-env` flake
# input (the authoritative CI definitions, run via `just ci-nix`) — keep them
# in sync when overriding anything in flake.nix.

default:
    @just --list

fmt:
    cargo +nightly fmt --all --check
    cargo +nightly fmt --manifest-path tools/external-consumer/Cargo.toml -- --check

lint:
    cargo +nightly clippy --locked --workspace --all-targets --all-features -- -D warnings

lint-default:
    cargo +nightly clippy --locked --workspace --lib -- -D warnings

check:
    cargo check --locked --workspace --all-targets --all-features

build:
    cargo build --locked --workspace --all-targets --all-features

test:
    cargo nextest run --locked --workspace --all-features --lib --bins --tests --examples
    cargo test --locked --workspace --doc --all-features

doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features

# Mailbox and cell items are defined inside the façade, so one rustdoc document
# now covers their full public signatures. `shelterwood-core` needs no separate
# walk because it has no adapter, tokio, or fastrand dependency for an item to
# name — an assumption check-core-manifest.sh asserts rather than assumes.
runtime-api-check:
    RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo +nightly rustdoc --locked -p shelterwood --all-features --lib
    cargo run --locked -p shelterwood-api-reachability -- target/doc/shelterwood.json
    ./tools/check-core-manifest.sh

external-consumer-check:
    ./tools/check-external-consumer.sh

nixfmt-check:
    git ls-files -z '*.nix' | xargs -0 nixfmt --check

# Fast local CI mirror — reuses the local cargo cache and incremental builds.
# The clean Nix lane retains the explicit all-target build for non-test codegen coverage.
ci: fmt lint lint-default test doc-check runtime-api-check external-consumer-check nixfmt-check

# Full clean Nix CI lane; use before pushing or when touching Nix files.
ci-nix:
    nix flake check --no-update-lock-file

doc:
    cargo doc --workspace --no-deps --open
