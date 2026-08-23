{
  description = "Shelterwood Rust development environment";

  inputs = {
    rust-env.url = "github:ralexstokes/rust-nix-template";
    nixpkgs.follows = "rust-env/nixpkgs";
    rust-overlay.follows = "rust-env/rust-overlay";
  };

  outputs =
    { rust-env, ... }:
    rust-env.lib.mkRustProject {
      src = ./.;
      # Markdown rides along for `include_str!` doc pages and the book;
      # scripts under tools/ run in the clean build sandbox, nextest reads
      # its timeout config, and book.toml configures the mdbook check.
      extraSourceFilter =
        path: type:
        type == "regular"
        && (
          builtins.match ".*\\.md" (toString path) != null
          || builtins.match ".*\\.sh" (toString path) != null
          || builtins.match ".*/\\.config/nextest\\.toml" (toString path) != null
          || builtins.match ".*/book\\.toml" (toString path) != null
        );
      extraShellPackages = pkgs: [ pkgs.mdbook ];
      extraChecks =
        {
          pkgs,
          craneLibNightly,
          commonArgs,
          cargoArtifactsNightly,
          ...
        }:
        let
          # Vendor evaluation happens during `nix flake check --no-build`,
          # before the filtered cargo source has necessarily been realised.
          # Point at the checked-in lockfile directly so evaluation remains
          # independent of that build ordering.
          benchmarkLock = ./tools/benchmarks/Cargo.lock;
          benchmarkArgs = commonArgs // {
            cargoLock = benchmarkLock;
            cargoVendorDir = craneLibNightly.vendorCargoDeps { cargoLock = benchmarkLock; };
            pname = "shelterwood-benchmark-check";
          };
        in
        {
          cargo-clippy-default = craneLibNightly.cargoClippy (
            commonArgs
            // {
              cargoArtifacts = cargoArtifactsNightly;
              cargoExtraArgs = "--locked";
              cargoClippyExtraArgs = "--workspace --lib -- -D warnings";
              doInstallCargoArtifacts = false;
            }
          );

          # The book's code blocks are includes of anchored regions from
          # examples/, so a successful build proves the include paths and
          # anchors resolve; the compile/run half lives in examples-run.
          book-build = craneLibNightly.mkCargoDerivation (
            commonArgs
            // {
              cargoArtifacts = cargoArtifactsNightly;
              nativeBuildInputs = [ pkgs.mdbook ];
              buildPhaseCargoCommand = ''
                mdbook build book
              '';
              doInstallCargoArtifacts = false;
            }
          );

          # Examples are asserting smoke tests; running them is the check.
          # Keep the list in sync with the justfile's `examples` recipe.
          examples-run = craneLibNightly.mkCargoDerivation (
            commonArgs
            // {
              cargoArtifacts = cargoArtifactsNightly;
              buildPhaseCargoCommand = ''
                for example in quickstart request_reply supervision_restart \
                  ordered_startup dynamic_scope graceful_shutdown observation \
                  cyclic_wiring embedding; do
                  cargo run --locked -p shelterwood --example "$example"
                done
              '';
              doInstallCargoArtifacts = false;
            }
          );

          api-enforcement = craneLibNightly.mkCargoDerivation (
            commonArgs
            // {
              cargoArtifacts = cargoArtifactsNightly;
              nativeBuildInputs = [ pkgs.ripgrep ];
              buildPhaseCargoCommand = ''
                cargo fmt --manifest-path tools/external-consumer/Cargo.toml -- --check
                RUSTDOCFLAGS="-Z unstable-options --output-format json" \
                  cargo rustdoc --locked -p shelterwood --all-features --lib
                cargo run --locked -p shelterwood-api-reachability -- \
                  target/doc/shelterwood.json
                ${pkgs.bash}/bin/bash ./tools/check-core-manifest.sh
                ${pkgs.bash}/bin/bash ./tools/check-external-consumer.sh
              '';
              doInstallCargoArtifacts = false;
            }
          );

          # The Criterion harness has its own workspace and lockfile so its
          # dependencies stay out of ordinary production builds. Compile it
          # explicitly in the authoritative clean lane to prevent drift.
          benchmark-check = craneLibNightly.mkCargoDerivation (
            benchmarkArgs
            // {
              cargoArtifacts = null;
              buildPhaseCargoCommand = ''
                cargo clippy --locked --manifest-path tools/benchmarks/Cargo.toml \
                  --all-targets -- -D warnings
                cargo bench --locked --manifest-path tools/benchmarks/Cargo.toml --no-run
              '';
              doInstallCargoArtifacts = false;
            }
          );
        };
    };
}
