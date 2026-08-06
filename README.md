# shelterwood

## Getting started

All tooling comes from the Nix development shell. 

```sh
nix develop
just test  # to run tests
```

### Note

The runtime-path and public-API-reachability checks are temporary guardrails
for the initial implementation. Once the runtime boundary is clear from the
surrounding source code, these checks can be removed rather than maintained as
permanent project infrastructure.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you shall be dual licensed as above, without
any additional terms or conditions.
