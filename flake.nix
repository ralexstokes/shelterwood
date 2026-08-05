{
  description = "Shelterwood Rust development environment";

  inputs = {
    rust-env.url = "github:ralexstokes/rust-nix-template";
  };

  outputs =
    { self, rust-env }:
    rust-env.lib.mkRustProject {
      src = ./.;
      # Escape hatches (see the template repo's README):
      #   projectName = "my-project";
      #   extraShellPackages = pkgs: [ pkgs.mdbook ];
      #   extraChecks = pkgs: { };
      #   extraCiCommands = "cargo run --locked -p hello --example smoke";
      #   extraSourceFilter = path: type: false;
    };
}
