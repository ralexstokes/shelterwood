//! Integration tests for Shelterwood's public behavior.

use std::{collections::BTreeSet, fs, path::Path};

macro_rules! register_test_modules {
    ($directory:literal; $($visibility:vis mod $module:ident;)+) => {
        $($visibility mod $module;)+

        #[test]
        fn every_test_module_is_registered() {
            crate::assert_registered_test_modules(
                $directory,
                &[$(stringify!($module)),+],
            );
        }
    };
}

fn assert_registered_test_modules(directory: &str, registered: &[&str]) {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join(directory);
    let registered = registered
        .iter()
        .map(|module| (*module).to_owned())
        .collect::<BTreeSet<_>>();
    let discovered = fs::read_dir(&directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
        .filter_map(|entry| {
            let path = entry
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to read an entry in {}: {error}",
                        directory.display()
                    )
                })
                .path();

            if path.is_file()
                && path.extension().is_some_and(|extension| extension == "rs")
                && path.file_name().is_some_and(|name| name != "mod.rs")
            {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(str::to_owned)
            } else if path.is_dir() && path.join("mod.rs").is_file() {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
            } else {
                None
            }
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(
        discovered,
        registered,
        "test module registration does not match {}",
        directory.display()
    );
}

register_test_modules!(
    "tests";
    mod acceptance;
    mod actor;
    mod api_trait_conformance;
    mod common;
    mod deadlines;
    mod defaults;
    mod delivery;
    mod disposal;
    mod drain;
    mod dynamic;
    mod events;
    mod lifecycle;
    mod mailbox;
    mod observation;
    mod offloads;
    mod one_shot_resource_ownership;
    mod policy_validation;
    mod raw;
    mod readiness;
    mod rebind;
    mod subtrees;
    mod tasks;
);
