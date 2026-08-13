//! Application-scale acceptance scenarios from `SPEC.md` Appendix C.

register_test_modules!(
    "tests/acceptance";
    mod assistant;
    mod shard_store;
    mod sidecar;
);
