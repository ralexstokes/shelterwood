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
      # Repository docs and their packaged doctest copies are compared in the
      # clean build sandbox, so keep Markdown alongside Cargo sources; the
      # the documentation script under tools/ runs there too, and nextest
      # reads its timeout config.
      extraSourceFilter =
        path: type:
        type == "regular"
        && (
          builtins.match ".*\\.md" (toString path) != null
          || builtins.match ".*\\.sh" (toString path) != null
          || builtins.match ".*/\\.config/nextest\\.toml" (toString path) != null
        );
      extraChecks =
        {
          pkgs,
          craneLibNightly,
          commonArgs,
          cargoArtifactsNightly,
          ...
        }:
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
                ${pkgs.bash}/bin/bash ./tools/sync-packaged-docs.sh --check
              '';
              doInstallCargoArtifacts = false;
            }
          );
        };
    };
}
