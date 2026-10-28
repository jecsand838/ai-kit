use std::{fs, path::Path};

const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

fn production_sources() -> Vec<(String, String)> {
    let source_root = Path::new(MANIFEST_DIR).join("src");
    let mut pending = vec![source_root];
    let mut sources = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("read production source directory") {
            let entry = entry.expect("read source entry");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                let relative = path
                    .strip_prefix(MANIFEST_DIR)
                    .expect("source path under crate")
                    .display()
                    .to_string();
                let contents = fs::read_to_string(&path).expect("read source");
                sources.push((relative, contents));
            }
        }
    }
    sources
}

#[test]
fn deleted_product_surfaces_do_not_return() {
    let forbidden = [
        "Conformance",
        "FactoryRequest",
        "DatabaseFactory",
        "StateStoreFactory",
        "Realtime",
        "NativeOperation",
        "SparseVector",
        "MultiVector",
        "VectorValue",
        "StateCommitOutcome",
        "QdrantConfig",
        "QdrantModeConfig",
        "LookupRequest",
        "MergeRequest",
        "MergeActions",
        "SearchByIdRequest",
        "RandomSampleRequest",
        "pub fast_search:",
        "DistanceMetric::Dot",
        "macro_rules!",
        "#[cfg(test)]",
    ];

    for (path, source) in production_sources() {
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{path} unexpectedly contains deleted or forbidden `{token}`"
            );
        }
    }
}

#[test]
fn default_dispatchers_are_closed_and_erasure_free() {
    let sources = production_sources();
    let combined = sources
        .iter()
        .map(|(_, source)| source.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    assert_eq!(
        combined.matches("pub fn from_backend").count(),
        0,
        "storage handles must not normalize caller implementations"
    );
    for forbidden in [
        "DatabaseImplementationId",
        "ErasedDatabaseBackend",
        "ErasedStateStoreBackend",
        "ErasedStateTransactionBackend",
        "\n    Extension(",
    ] {
        assert!(
            !combined.contains(forbidden),
            "storage production unexpectedly contains `{forbidden}`"
        );
    }
}

#[test]
fn production_sources_keep_the_workspace_safety_and_structure_rules() {
    for (path, source) in production_sources() {
        for forbidden in [
            "unsafe ",
            "unsafe{",
            "async_trait",
            "include!(",
            "#[path",
            "cfg(not(",
            "use super::*",
            "#[allow(",
            "#[expect(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{path} unexpectedly contains forbidden `{forbidden}`"
            );
        }
        for line in source.lines() {
            assert!(
                !(line.starts_with("type ")
                    || line.starts_with("pub type ")
                    || line.starts_with("pub(crate) type ")),
                "{path} contains a forbidden Rust item type alias: {line}"
            );
        }
    }
}

#[test]
fn public_handle_behavior_has_only_the_trait_contract() {
    let owners = [
        (
            "Database",
            &["capabilities", "catalog", "data", "query", "index"][..],
        ),
        ("StateStore", &["load", "list", "begin_transaction"][..]),
        (
            "StateTransaction",
            &["load", "put", "delete", "commit", "rollback"][..],
        ),
    ];
    for (path, source) in production_sources() {
        for (owner, methods) in owners {
            for tail in source.split("impl ").skip(1) {
                let Some((header, body)) = tail.split_once('{') else {
                    continue;
                };
                if header.trim() != owner {
                    continue;
                }
                let mut depth = 1_i32;
                let end = body
                    .char_indices()
                    .find_map(|(index, character)| {
                        match character {
                            '{' => depth += 1,
                            '}' => depth -= 1,
                            _ => {}
                        }
                        (depth == 0).then_some(index)
                    })
                    .expect("balanced inherent implementation");
                let body = &body[..end];
                for method in methods {
                    assert!(
                        !body.contains(&format!("fn {method}(")),
                        "{path}: {owner} must expose `{method}` only through its public backend trait"
                    );
                }
            }
        }
    }
}
