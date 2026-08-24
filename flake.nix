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
          craneLibStable,
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
            # `--manifest-path` would otherwise place the build directory at
            # tools/benchmarks/target, where crane's install/inherit hooks do
            # not look, silently reducing the prebuilt dependency artifacts
            # below to a no-op.
            CARGO_TARGET_DIR = "target";
            cargoLock = benchmarkLock;
            # Vendoring only reads the lockfile, so one vendor directory serves
            # both toolchains.
            cargoVendorDir = craneLibNightly.vendorCargoDeps { cargoLock = benchmarkLock; };
            pname = "shelterwood-benchmark";
          };
          # Prebuild the benchmark package's dependencies against a dummy
          # source keyed on the manifests and the benchmark lockfile, exactly
          # as `cargoArtifactsNightly` does for the production workspace.
          # Without this the Criterion dependency tree recompiles on every
          # tracked source edit anywhere in the repository.
          benchmarkDeps =
            craneLib: suffix: buildCommand:
            craneLib.buildDepsOnly (
              benchmarkArgs
              // {
                pname = "${benchmarkArgs.pname}-${suffix}";
                # `mkDummySrc` carries only the workspace-root lockfile across.
                # The benchmark package has its own, and `--locked` needs it.
                extraDummyScript = ''
                  cp ${benchmarkLock} $out/tools/benchmarks/Cargo.lock
                '';
                doCheck = false;
                buildPhaseCargoCommand = buildCommand;
              }
            );
          benchmarkClippyDeps = benchmarkDeps craneLibNightly "clippy-deps" ''
            cargo check --locked --manifest-path tools/benchmarks/Cargo.toml --all-targets
          '';
          benchmarkBuildDeps = benchmarkDeps craneLibStable "build-deps" ''
            cargo bench --locked --manifest-path tools/benchmarks/Cargo.toml --no-run
          '';
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
                cargo fmt --manifest-path tools/benchmarks/Cargo.toml -- --check
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
          # dependencies stay out of ordinary production builds. Compile and
          # lint it explicitly in the authoritative clean lane to prevent
          # drift. The two halves are separate derivations because they run on
          # separate toolchains, mirroring the justfile's `bench-check` recipe:
          # lints on nightly like every other lint lane, and compiles on the
          # pinned stable toolchain that `just bench` actually measures with,
          # so a nightly-only construct cannot pass here and then fail there.
          benchmark-check = craneLibNightly.mkCargoDerivation (
            benchmarkArgs
            // {
              cargoArtifacts = benchmarkClippyDeps;
              buildPhaseCargoCommand = ''
                cargo clippy --locked --manifest-path tools/benchmarks/Cargo.toml \
                  --all-targets -- -D warnings
              '';
              doInstallCargoArtifacts = false;
            }
          );

          benchmark-build = craneLibStable.mkCargoDerivation (
            benchmarkArgs
            // {
              cargoArtifacts = benchmarkBuildDeps;
              buildPhaseCargoCommand = ''
                cargo bench --locked --manifest-path tools/benchmarks/Cargo.toml --no-run
              '';
              doInstallCargoArtifacts = false;
            }
          );
        };
    };
}
