# The recipes used by `ci` mirror the checks built by the `rust-env` flake
# input (the authoritative CI definitions, run via `just ci-nix`) — keep them
# in sync when overriding anything in flake.nix.

default:
    @just --list

fmt:
    cargo +nightly fmt --all --check
    cargo +nightly fmt --manifest-path tools/external-consumer/Cargo.toml -- --check
    cargo +nightly fmt --manifest-path tools/benchmarks/Cargo.toml -- --check

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

# A successful build proves the chapter include paths and example anchors
# resolve; the included code compiles and runs in the `examples` recipe, so
# the book needs no test lane of its own.
book:
    mdbook build book

# Examples are smoke tests: each ends in assertions and a nonzero exit fails
# the recipe. Keep the list in sync with crates/shelterwood/examples/ and the
# flake's examples-run check.
examples:
    cargo run --locked -p shelterwood --example quickstart
    cargo run --locked -p shelterwood --example request_reply
    cargo run --locked -p shelterwood --example supervision_restart
    cargo run --locked -p shelterwood --example ordered_startup
    cargo run --locked -p shelterwood --example dynamic_scope
    cargo run --locked -p shelterwood --example graceful_shutdown
    cargo run --locked -p shelterwood --example observation
    cargo run --locked -p shelterwood --example cyclic_wiring
    cargo run --locked -p shelterwood --example embedding

external-consumer-check:
    ./tools/check-external-consumer.sh

# The benchmark package is intentionally outside the production workspace.
# Compile and lint it explicitly so the harness cannot rot between data runs.
# The toolchain split is deliberate and mirrored by the flake's
# `benchmark-check` (nightly) and `benchmark-build` (stable) derivations: lints
# run on nightly like every other lint lane, and the compile runs on the pinned
# stable toolchain that `bench` below measures with.
bench-check:
    cargo +nightly clippy --locked --manifest-path tools/benchmarks/Cargo.toml --all-targets -- -D warnings
    cargo bench --locked --manifest-path tools/benchmarks/Cargo.toml --no-run

bench:
    cargo bench --locked --manifest-path tools/benchmarks/Cargo.toml

nixfmt-check:
    git ls-files -z '*.nix' | xargs -0 nixfmt --check

# Fast local CI mirror — reuses the local cargo cache and incremental builds.
# The clean Nix lane retains the explicit all-target build for non-test codegen coverage.
ci: fmt lint lint-default test examples doc-check book runtime-api-check external-consumer-check bench-check nixfmt-check

# Full clean Nix CI lane; use before pushing or when touching Nix files.
ci-nix:
    nix flake check --no-update-lock-file

doc:
    cargo doc --workspace --no-deps --open
