# The recipes used by `ci` mirror the checks built by the `rust-env` flake
# input (the authoritative CI definitions, run via `just ci-nix`) — keep them
# in sync when overriding anything in flake.nix.

default:
    @just --list

fmt:
    cargo +nightly fmt --all --check

lint:
    cargo +nightly clippy --locked --workspace --all-targets --all-features -- -D warnings -W unreachable-pub

check:
    cargo check --locked --workspace --all-targets --all-features

build:
    cargo build --locked --workspace --all-targets --all-features

test:
    cargo nextest run --locked --workspace --all-features --lib --bins --tests --examples
    cargo test --locked --workspace --doc --all-features

doc-check:
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --no-deps --all-features

runtime-api-check:
    RUSTDOCFLAGS="-Z unstable-options --output-format json" cargo +nightly rustdoc --locked -p shelterwood --all-features --lib
    cargo run --locked -p shelterwood-api-reachability -- target/doc/shelterwood.json

runtime-path-check:
    ./tools/check-runtime-paths.sh

exit-path-check:
    ./tools/check-exit-paths.sh

layering-path-check:
    ./tools/check-layering-paths.sh

enforcement-fixture-check:
    ./tools/check-enforcement-fixtures.sh

packaged-docs-sync:
    ./tools/sync-packaged-docs.sh --write

packaged-docs-check:
    ./tools/sync-packaged-docs.sh --check

package-check:
    ./tools/check-packaged-crate.sh

nixfmt-check:
    git ls-files -z '*.nix' | xargs -0 nixfmt --check

# Fast local CI mirror — reuses the local cargo cache and incremental builds.
# The clean Nix lane retains the explicit all-target build for non-test codegen coverage.
ci: fmt lint test doc-check runtime-api-check runtime-path-check exit-path-check layering-path-check enforcement-fixture-check packaged-docs-check package-check nixfmt-check

# Full clean Nix CI lane; use before pushing or when touching Nix files.
ci-nix:
    nix flake check --no-update-lock-file

doc:
    cargo doc --workspace --no-deps --open
