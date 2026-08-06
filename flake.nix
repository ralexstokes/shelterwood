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
      self,
      crane,
      nixpkgs,
      rust-env,
      rust-overlay,
    }:
    let
      base = rust-env.lib.mkRustProject {
        src = ./.;
      };
    in
    base
    // {
      checks = builtins.mapAttrs (
        system: baseChecks:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ (import rust-overlay) ];
          };
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
              || pkgs.lib.hasSuffix ".sh" (toString path);
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
        baseChecks
        // {
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
              '';
              doInstallCargoArtifacts = false;
            }
          );
        }
      ) base.checks;
    };
}
