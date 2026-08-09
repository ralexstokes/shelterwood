use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    env,
    error::Error,
    fs,
    path::PathBuf,
};

use serde_json::Value;

const FORBIDDEN_ROOTS: &[&str] = &["tokio", "tokio_util"];
const SUPPORTED_FORMAT_VERSION: u64 = 61;

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: shelterwood-api-reachability <rustdoc-json>")?;
    let document: Value = serde_json::from_slice(&fs::read(&path)?)?;
    let leaks = find_leaks(&document)?;

    if leaks.is_empty() {
        println!("public API runtime reachability: clean");
        return Ok(());
    }

    eprintln!("runtime types are reachable from public Shelterwood items:");
    for leak in leaks {
        eprintln!("  {leak}");
    }
    Err("public API runtime reachability check failed".into())
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

    use serde_json::json;

    use super::find_leaks;

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
