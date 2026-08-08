{
  description = "Shelterwood Rust development environment";

  inputs = {
    rust-env.url = "github:ralexstokes/rust-nix-template";
    crane.follows = "rust-env/crane";
    nixpkgs.follows = "rust-env/nixpkgs";
    rust-overlay.follows = "rust-env/rust-overlay";
  };

  outputs =
    {
      crane,
      rust-env,
      ...
    }:
    rust-env.lib.mkRustProject {
      src = ./.;
      # Repository docs and their packaged doctest copies are compared in the
      # clean build sandbox, so keep Markdown alongside Cargo sources.
      extraSourceFilter =
        path: type: type == "regular" && builtins.match ".*\\.md" (toString path) != null;
      extraChecks =
        pkgs:
        let
          nightlyToolchain = pkgs.rust-bin.selectLatestNightlyWith (
            toolchain:
            toolchain.default.override {
              extensions = [ "rustfmt" ];
            }
          );
          craneLib = crane.mkLib pkgs;
          craneLibNightly = craneLib.overrideToolchain nightlyToolchain;
          source = pkgs.lib.cleanSourceWith {
            src = pkgs.lib.cleanSource ./.;
            filter =
              path: type:
              type == "directory"
              || craneLib.filterCargoSources path type
              || pkgs.lib.hasSuffix ".sh" (toString path)
              || pkgs.lib.hasSuffix ".md" (toString path);
          };
          commonArgs = {
            pname = "shelterwood-part0-enforcement";
            version = "0.1.0";
            src = source;
            strictDeps = true;
            cargoExtraArgs = "--locked --workspace --all-features";
          };
          cargoArtifacts = craneLibNightly.buildDepsOnly commonArgs;
        in
        {
          part0-enforcement = craneLibNightly.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              nativeBuildInputs = [ pkgs.ripgrep ];
              buildPhaseCargoCommand = ''
                RUSTDOCFLAGS="-Z unstable-options --output-format json" \
                  cargo rustdoc --locked -p shelterwood --all-features --lib
                cargo run --locked -p shelterwood-api-reachability -- \
                  target/doc/shelterwood.json
                ${pkgs.bash}/bin/bash ./tools/check-runtime-paths.sh
                ${pkgs.bash}/bin/bash ./tools/check-exit-paths.sh
                ${pkgs.bash}/bin/bash ./tools/sync-packaged-docs.sh --check
                ${pkgs.bash}/bin/bash ./tools/check-packaged-crate.sh
              '';
              doInstallCargoArtifacts = false;
            }
          );
        };
    };
}
