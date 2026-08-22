use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    env,
    error::Error,
    fs,
    path::PathBuf,
};

use serde_json::Value;

const FORBIDDEN_ROOTS: &[&str] = &["fastrand", "shelterwood_runtime", "tokio", "tokio_util"];
const SUPPORTED_FORMAT_VERSION: u64 = 61;

// This walk is scoped to the items one document actually contains. Mailbox and
// cell items now live in the façade, so its document covers those signatures
// directly. Rustdoc JSON still does not inline the remaining cross-crate core
// re-exports: they appear only as `use` items whose target signatures are
// invisible here. Core's runtime-free dependency graph is the structural proof
// for that opaque residue, and `tools/check-core-manifest.sh` asserts it in
// the same `runtime-api-check` lane; see that recipe and SPEC §16.13.

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: shelterwood-api-reachability <rustdoc-json>")?;
    let document: Value = serde_json::from_slice(&fs::read(&path)?)?;
    let leaks = find_leaks(&document)?;

    if !leaks.is_empty() {
        eprintln!("runtime types are reachable from public Shelterwood items:");
        for leak in leaks {
            eprintln!("  {leak}");
        }
        return Err("public API runtime reachability check failed".into());
    }
    println!("public API runtime reachability: clean");

    let escapes = find_prelude_escapes(&document)?;
    if !escapes.is_empty() {
        eprintln!("prelude re-exports items the crate root does not export:");
        for escape in escapes {
            eprintln!("  {escape}");
        }
        return Err("prelude containment check failed".into());
    }
    println!("prelude containment: clean");

    Ok(())
}

fn find_leaks(document: &Value) -> Result<BTreeSet<String>, Box<dyn Error>> {
    validate_format_version(document)?;
    let index = object_field(document, "index")?;
    let paths = object_field(document, "paths")?;

    let local_paths: HashMap<String, String> = paths
        .iter()
        .filter(|(_, summary)| summary.get("crate_id").and_then(Value::as_u64) == Some(0))
        .map(|(id, summary)| (id.clone(), display_path(summary)))
        .collect();
    let known_ids: HashSet<String> = index.keys().chain(paths.keys()).cloned().collect();

    let mut leaks = BTreeSet::new();
    for (root_id, root_path) in &local_paths {
        let mut queue = VecDeque::from([root_id.clone()]);
        let mut visited = HashSet::new();

        while let Some(id) = queue.pop_front() {
            if !visited.insert(id.clone()) {
                continue;
            }
            let Some(item) = index.get(&id) else {
                continue;
            };
            if is_blanket_impl(item) {
                continue;
            }
            let mut references = Vec::new();
            collect_references(item, &known_ids, &mut references);
            for reference in references {
                let Some(summary) = paths.get(&reference) else {
                    if index.contains_key(&reference) {
                        queue.push_back(reference);
                    }
                    continue;
                };
                if summary.get("crate_id").and_then(Value::as_u64) == Some(0) {
                    queue.push_back(reference);
                    continue;
                }
                let path = path_segments(summary);
                if path
                    .first()
                    .is_some_and(|root| FORBIDDEN_ROOTS.contains(&root.as_str()))
                {
                    leaks.insert(format!("{root_path} -> {}", path.join("::")));
                }
            }
        }
    }

    Ok(leaks)
}

// The prelude adds no surface: it re-exports crate-root paths and nothing
// else. That claim is checked structurally rather than by convention. Every
// `use` under `shelterwood::prelude` must name an item the crate root already
// re-exports, under the same name — so "prelude ⊆ public root" holds by
// construction, and with it "the prelude exposes nothing doc(hidden)", which
// the root's own boundary (tools/check-external-consumer.sh) already pins.
// Globs, locally defined items, and a missing `prelude` module are all
// failures: the last one keeps the check from passing vacuously.
fn find_prelude_escapes(document: &Value) -> Result<BTreeSet<String>, Box<dyn Error>> {
    let index = object_field(document, "index")?;
    let root_id = document
        .get("root")
        .map(id_key)
        .ok_or("rustdoc JSON has no field `root`")?;
    let root_items =
        module_items(index, &root_id).ok_or("rustdoc JSON field `root` does not name a module")?;

    let mut root_exports: HashSet<(String, String)> = HashSet::new();
    let mut prelude_id = None;
    for item_id in root_items {
        let Some(item) = index.get(&item_id) else {
            continue;
        };
        if let Some(reexport) = item.pointer("/inner/use") {
            if let (Some(name), Some(target)) = (use_name(reexport), use_target(reexport)) {
                root_exports.insert((name, target));
            }
            continue;
        }
        if item.pointer("/inner/module").is_some()
            && item.get("name").and_then(Value::as_str) == Some("prelude")
        {
            prelude_id = Some(item_id);
        }
    }

    let prelude_id = prelude_id.ok_or("the façade document has no public `prelude` module")?;
    if root_exports.is_empty() {
        return Err("the façade document's crate root re-exports nothing".into());
    }

    let mut escapes = BTreeSet::new();
    let mut queue = VecDeque::from([("prelude".to_owned(), prelude_id)]);
    while let Some((module_path, module_id)) = queue.pop_front() {
        let Some(items) = module_items(index, &module_id) else {
            escapes.insert(format!("{module_path} is not a module in this document"));
            continue;
        };
        for item_id in items {
            let Some(item) = index.get(&item_id) else {
                escapes.insert(format!("{module_path} holds an item the document omits"));
                continue;
            };
            if let Some(reexport) = item.pointer("/inner/use") {
                let name = use_name(reexport).unwrap_or_else(|| "<unnamed>".to_owned());
                if reexport.get("is_glob").and_then(Value::as_bool) == Some(true) {
                    escapes.insert(format!("{module_path} re-exports the glob `{name}::*`"));
                    continue;
                }
                let Some(target) = use_target(reexport) else {
                    escapes.insert(format!("{module_path}::{name} names no resolvable item"));
                    continue;
                };
                if !root_exports.contains(&(name.clone(), target)) {
                    escapes.insert(format!(
                        "{module_path}::{name} is not `shelterwood::{name}`"
                    ));
                }
                continue;
            }
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>")
                .to_owned();
            if item.pointer("/inner/module").is_some() {
                queue.push_back((format!("{module_path}::{name}"), item_id));
                continue;
            }
            escapes.insert(format!(
                "{module_path} defines `{name}` instead of re-exporting"
            ));
        }
    }

    Ok(escapes)
}

fn module_items(index: &serde_json::Map<String, Value>, id: &str) -> Option<Vec<String>> {
    let items = index.get(id)?.pointer("/inner/module/items")?.as_array()?;
    Some(items.iter().map(id_key).collect())
}

fn use_name(reexport: &Value) -> Option<String> {
    reexport
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn use_target(reexport: &Value) -> Option<String> {
    reexport.get("id").filter(|id| !id.is_null()).map(id_key)
}

fn id_key(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn validate_format_version(document: &Value) -> Result<(), Box<dyn Error>> {
    let version = document
        .get("format_version")
        .and_then(Value::as_u64)
        .ok_or("rustdoc JSON has no integer field `format_version`")?;

    if version == SUPPORTED_FORMAT_VERSION {
        return Ok(());
    }

    Err(format!(
        "unsupported rustdoc JSON format version {version}; expected {SUPPORTED_FORMAT_VERSION}"
    )
    .into())
}

fn is_blanket_impl(item: &Value) -> bool {
    item.pointer("/inner/impl/blanket_impl")
        .is_some_and(|blanket| !blanket.is_null())
}

fn object_field<'a>(
    document: &'a Value,
    name: &str,
) -> Result<&'a serde_json::Map<String, Value>, Box<dyn Error>> {
    document
        .get(name)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("rustdoc JSON has no object field `{name}`").into())
}

fn display_path(summary: &Value) -> String {
    let path = path_segments(summary);
    if path.is_empty() {
        "<unnamed-local-item>".to_owned()
    } else {
        path.join("::")
    }
}

fn path_segments(summary: &Value) -> Vec<String> {
    summary
        .get("path")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn collect_references(value: &Value, known_ids: &HashSet<String>, output: &mut Vec<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_references(value, known_ids, output);
            }
        }
        Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(
                    name.as_str(),
                    "id" | "items" | "impls" | "fields" | "variants" | "implementations"
                ) {
                    collect_ids(value, known_ids, output);
                } else {
                    collect_references(value, known_ids, output);
                }
            }
        }
        _ => {}
    }
}

fn collect_ids(value: &Value, known_ids: &HashSet<String>, output: &mut Vec<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_ids(value, known_ids, output);
            }
        }
        Value::Object(fields) => {
            for value in fields.values() {
                collect_ids(value, known_ids, output);
            }
        }
        Value::Number(value) => push_known(value.to_string(), known_ids, output),
        Value::String(value) => push_known(value.clone(), known_ids, output),
        _ => {}
    }
}

fn push_known(value: String, known_ids: &HashSet<String>, output: &mut Vec<String>) {
    if known_ids.contains(&value) {
        output.push(value);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::{Value, json};

    use super::{find_leaks, find_prelude_escapes};

    /// A crate root re-exporting `Actor` (id 10) and `Hidden` (id 11), plus a
    /// `prelude` module (id 3) whose items the caller supplies.
    fn document_with_prelude(prelude_items: Value, extra_index: Value) -> Value {
        let mut document = json!({
            "format_version": 61,
            "root": 0,
            "index": {
                "0": {
                    "id": 0,
                    "name": "shelterwood",
                    "inner": { "module": { "items": [1, 2, 3] } }
                },
                "1": {
                    "id": 1,
                    "name": null,
                    "inner": { "use": { "source": "actor::Actor", "name": "Actor", "id": 10, "is_glob": false } }
                },
                "2": {
                    "id": 2,
                    "name": null,
                    "inner": { "use": { "source": "cells::Hidden", "name": "Hidden", "id": 11, "is_glob": false } }
                },
                "3": {
                    "id": 3,
                    "name": "prelude",
                    "inner": { "module": { "items": [] } }
                }
            },
            "paths": {}
        });
        document["index"]["3"]["inner"]["module"]["items"] = prelude_items;
        let index = document["index"]
            .as_object_mut()
            .expect("index is an object");
        for (id, item) in extra_index.as_object().expect("extra index is an object") {
            index.insert(id.clone(), item.clone());
        }
        document
    }

    #[test]
    fn accepts_a_prelude_that_only_re_exports_crate_root_names() {
        let document = document_with_prelude(
            json!([20, 21]),
            json!({
                "20": {
                    "id": 20,
                    "name": null,
                    "inner": { "use": { "source": "crate::Actor", "name": "Actor", "id": 10, "is_glob": false } }
                },
                "21": {
                    "id": 21,
                    "name": "errors",
                    "inner": { "module": { "items": [22] } }
                },
                "22": {
                    "id": 22,
                    "name": null,
                    "inner": { "use": { "source": "crate::Hidden", "name": "Hidden", "id": 11, "is_glob": false } }
                }
            }),
        );

        assert_eq!(
            find_prelude_escapes(&document).expect("the fixture is a complete document"),
            BTreeSet::new()
        );
    }

    #[test]
    fn rejects_a_prelude_re_export_the_crate_root_does_not_carry() {
        let document = document_with_prelude(
            json!([20]),
            json!({
                "20": {
                    "id": 20,
                    "name": null,
                    "inner": { "use": { "source": "crate::cells::MemberCell", "name": "MemberCell", "id": 12, "is_glob": false } }
                }
            }),
        );

        assert_eq!(
            find_prelude_escapes(&document).expect("the fixture is a complete document"),
            BTreeSet::from(["prelude::MemberCell is not `shelterwood::MemberCell`".to_owned()])
        );
    }

    /// A name the root also exports is still an escape when the prelude's
    /// `use` resolves to a different item — the check pairs name with target.
    #[test]
    fn rejects_a_shadowing_re_export_that_reuses_a_root_name() {
        let document = document_with_prelude(
            json!([20]),
            json!({
                "20": {
                    "id": 20,
                    "name": null,
                    "inner": { "use": { "source": "crate::raw::Actor", "name": "Actor", "id": 12, "is_glob": false } }
                }
            }),
        );

        assert_eq!(
            find_prelude_escapes(&document).expect("the fixture is a complete document"),
            BTreeSet::from(["prelude::Actor is not `shelterwood::Actor`".to_owned()])
        );
    }

    #[test]
    fn rejects_a_prelude_that_globs_an_internal_module() {
        let document = document_with_prelude(
            json!([20]),
            json!({
                "20": {
                    "id": 20,
                    "name": null,
                    "inner": { "use": { "source": "crate::cells", "name": "cells", "id": 12, "is_glob": true } }
                }
            }),
        );

        assert_eq!(
            find_prelude_escapes(&document).expect("the fixture is a complete document"),
            BTreeSet::from(["prelude re-exports the glob `cells::*`".to_owned()])
        );
    }

    #[test]
    fn rejects_a_prelude_that_defines_its_own_item() {
        let document = document_with_prelude(
            json!([20]),
            json!({
                "20": {
                    "id": 20,
                    "name": "ActorContext",
                    "inner": { "type_alias": {} }
                }
            }),
        );

        assert_eq!(
            find_prelude_escapes(&document).expect("the fixture is a complete document"),
            BTreeSet::from(["prelude defines `ActorContext` instead of re-exporting".to_owned()])
        );
    }

    /// Fail closed: a document without the module means the walk found
    /// nothing to check, which must not read as a pass.
    #[test]
    fn rejects_a_document_with_no_prelude_module() {
        let document = json!({
            "format_version": 61,
            "root": 0,
            "index": {
                "0": { "id": 0, "name": "shelterwood", "inner": { "module": { "items": [] } } }
            },
            "paths": {}
        });

        let error = find_prelude_escapes(&document).expect_err("a missing prelude is an error");
        assert_eq!(
            error.to_string(),
            "the façade document has no public `prelude` module"
        );
    }

    #[test]
    fn follows_public_type_ids_without_treating_every_number_as_an_id() {
        let document = json!({
            "format_version": 61,
            "index": {
                "0": {
                    "id": 0,
                    "span": { "begin": [3, 1] },
                    "inner": { "module": { "items": [1] } }
                },
                "1": {
                    "id": 1,
                    "span": { "begin": [3, 2] },
                    "inner": {
                        "function": {
                            "sig": {
                                "output": {
                                    "resolved_path": { "id": 2 }
                                }
                            }
                        }
                    }
                }
            },
            "paths": {
                "0": { "crate_id": 0, "path": ["shelterwood"] },
                "1": { "crate_id": 0, "path": ["shelterwood", "leak"] },
                "2": { "crate_id": 7, "path": ["tokio", "time", "Instant"] },
                "3": { "crate_id": 7, "path": ["tokio", "sync", "watch"] }
            }
        });

        let leaks = find_leaks(&document).expect("fixture must be valid rustdoc-shaped JSON");
        assert_eq!(
            leaks,
            BTreeSet::from([
                "shelterwood -> tokio::time::Instant".to_owned(),
                "shelterwood::leak -> tokio::time::Instant".to_owned(),
            ])
        );
    }

    #[test]
    fn rejects_runtime_adapter_types_that_hide_tokio() {
        let document = json!({
            "format_version": 61,
            "index": {
                "0": {
                    "id": 0,
                    "inner": { "module": { "items": [1] } }
                },
                "1": {
                    "id": 1,
                    "inner": {
                        "function": {
                            "sig": {
                                "output": {
                                    "resolved_path": { "id": 2 }
                                }
                            }
                        }
                    }
                }
            },
            "paths": {
                "0": { "crate_id": 0, "path": ["shelterwood"] },
                "1": { "crate_id": 0, "path": ["shelterwood", "leaked_join"] },
                "2": {
                    "crate_id": 8,
                    "path": ["shelterwood_runtime", "spawn", "JoinHandle"]
                }
            }
        });

        let leaks = find_leaks(&document).expect("fixture must be valid rustdoc-shaped JSON");
        assert_eq!(
            leaks,
            BTreeSet::from([
                "shelterwood -> shelterwood_runtime::spawn::JoinHandle".to_owned(),
                "shelterwood::leaked_join -> shelterwood_runtime::spawn::JoinHandle".to_owned(),
            ])
        );
    }

    /// Pins the limitation that keeps cross-crate core signatures opaque.
    ///
    /// A façade item re-exported from a sibling crate reaches this document
    /// only as a `use` pointing at an id the document does not describe, so
    /// whatever that item's signature names is unreachable from here even
    /// though rustdoc renders it as public façade API. Core has no forbidden
    /// runtime dependency for such a hidden signature to name. If rustdoc ever
    /// begins inlining cross-crate re-exports this test fails, which is the
    /// signal to revisit that assumption.
    #[test]
    fn cannot_see_through_a_cross_crate_re_export() {
        let document = json!({
            "format_version": 61,
            "index": {
                "0": {
                    "id": 0,
                    "inner": { "module": { "items": [1] } }
                },
                "1": {
                    "id": 1,
                    "inner": { "use": { "id": 2, "name": "DeadlineBudget" } }
                }
            },
            "paths": {
                "0": { "crate_id": 0, "path": ["shelterwood"] },
                // The re-exported type itself is not forbidden; the leak would
                // be inside one of its methods, which this document omits
                // entirely because the item is defined in another crate.
                "2": { "crate_id": 5, "path": ["shelterwood_core", "deadline", "DeadlineBudget"] }
            }
        });

        let leaks = find_leaks(&document).expect("fixture must be valid rustdoc-shaped JSON");
        assert!(leaks.is_empty());
    }

    #[test]
    fn ignores_materialized_blanket_impls_but_follows_explicit_impls() {
        let document = json!({
            "format_version": 61,
            "index": {
                "0": {
                    "id": 0,
                    "inner": { "module": { "items": [1, 4] } }
                },
                "1": {
                    "id": 1,
                    "inner": { "struct": { "impls": [2] } }
                },
                "2": {
                    "id": 2,
                    "inner": {
                        "impl": {
                            "trait": { "id": 3 },
                            "blanket_impl": { "generic": "T" }
                        }
                    }
                },
                "4": {
                    "id": 4,
                    "inner": { "struct": { "impls": [5] } }
                },
                "5": {
                    "id": 5,
                    "inner": {
                        "impl": {
                            "trait": { "id": 3 },
                            "blanket_impl": null
                        }
                    }
                }
            },
            "paths": {
                "0": { "crate_id": 0, "path": ["shelterwood"] },
                "1": { "crate_id": 0, "path": ["shelterwood", "Blanketed"] },
                "2": { "crate_id": 0, "path": ["shelterwood", "impl-blanket"] },
                "3": { "crate_id": 7, "path": ["tokio_util", "future", "FutureExt"] },
                "4": { "crate_id": 0, "path": ["shelterwood", "Explicit"] },
                "5": { "crate_id": 0, "path": ["shelterwood", "impl-explicit"] }
            }
        });

        let leaks = find_leaks(&document).expect("fixture must be valid rustdoc-shaped JSON");
        assert_eq!(
            leaks,
            BTreeSet::from([
                "shelterwood -> tokio_util::future::FutureExt".to_owned(),
                "shelterwood::Explicit -> tokio_util::future::FutureExt".to_owned(),
                "shelterwood::impl-explicit -> tokio_util::future::FutureExt".to_owned(),
            ])
        );
    }

    #[test]
    fn accepts_supported_format_version() {
        let document = json!({
            "format_version": 61,
            "index": {},
            "paths": {}
        });

        assert_eq!(
            find_leaks(&document).expect("format version 61 must remain supported"),
            BTreeSet::new()
        );
    }

    #[test]
    fn rejects_unsupported_format_version() {
        let document = json!({
            "format_version": 60,
            "index": {},
            "paths": {}
        });

        let error = find_leaks(&document).expect_err("older schemas must be rejected");
        assert_eq!(
            error.to_string(),
            "unsupported rustdoc JSON format version 60; expected 61"
        );
    }
}
