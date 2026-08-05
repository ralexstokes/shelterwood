# shelterwood

Bootstrapped from [rust-nix-template](https://github.com/ralexstokes/rust-nix-template).

## Getting started

All tooling comes from the Nix development shell. See `AGENTS.md` for the
development contract.

```sh
direnv allow                  # interactive shells; agents use ./scripts/dev
./scripts/dev just ci         # fast local CI mirror
./scripts/dev just ci-nix     # authoritative clean lane (nix flake check)
```

The starter workspace contains the `crates/shelterwood` library. Add new
workspace crates under `crates/` and list them in `Cargo.toml`.

CI uses the `ralexstokes` Cachix cache. Publishing the development shell on
pushes to `main` requires a `CACHIX_AUTH_TOKEN` repository secret.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you shall be dual licensed as above, without
any additional terms or conditions.
