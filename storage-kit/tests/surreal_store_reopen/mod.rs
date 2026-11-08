use std::{collections::BTreeMap, fs, path::PathBuf, process::Command};

use storage_kit::{
    Database, DatabaseBackend,
    database::{
        StorageLocator,
        catalog::{
            CatalogRequest, CatalogResponse, CollectionSchema, CreateCollectionMode,
            CreateCollectionRequest, FieldSchema, FieldType,
        },
        config::{CommonConfig, DatabaseConfig, SurrealConfig, SurrealModeConfig},
        data::{DataRecord, DataRequest, DataResponse, ScanRequest},
    },
};

#[test]
fn surrealdb_fresh_surrealkv_cold_reopen() {
    if let Some(path) = std::env::var_os("STORAGE_FRESH_STORE") {
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let snapshot = runtime.block_on(async {
                    let database = Database::connect(
                        DatabaseConfig::surreal(SurrealConfig {
                            mode: SurrealModeConfig::SurrealKv {
                                path: PathBuf::from(path),
                            },
                            ..SurrealConfig::default()
                        })
                        .with_common(CommonConfig {
                            connect_timeout_ms: 10_000,
                            request_timeout_ms: 10_000,
                            ..CommonConfig::default()
                        }),
                    )
                    .await
                    .expect("fresh public connection");
                    let locator = StorageLocator::new("cold_records");
                    if std::env::var("STORAGE_FRESH_PHASE").unwrap() == "seed" {
                        let schema = CollectionSchema {
                            fields: vec![
                                FieldSchema {
                                    name: "label".into(),
                                    field_type: FieldType::String,
                                    nullable: false,
                                    metadata: BTreeMap::new(),
                                },
                                FieldSchema {
                                    name: "sequence".into(),
                                    field_type: FieldType::I64,
                                    nullable: false,
                                    metadata: BTreeMap::new(),
                                },
                            ],
                            metadata: BTreeMap::new(),
                        };
                        database
                            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                                locator: locator.clone(),
                                schema: Some(schema),
                                mode: CreateCollectionMode::Create,
                                records: vec![
                                    DataRecord::new("a")
                                        .json_field("label", serde_json::json!("alpha"))
                                        .json_field("sequence", serde_json::json!(1)),
                                    DataRecord::new("b")
                                        .json_field("label", serde_json::json!("beta"))
                                        .json_field("sequence", serde_json::json!(2)),
                                ],
                            }))
                            .await
                            .expect("seed fresh native store");
                    }
                    let CatalogResponse::Collection(info) = database
                        .catalog(CatalogRequest::OpenCollection {
                            locator: locator.clone(),
                        })
                        .await
                        .expect("open exact catalog")
                    else {
                        panic!("catalog response")
                    };
                    assert_eq!(info.locator, locator);
                    let DataResponse::Records(mut rows) = database
                        .data(DataRequest::Scan(ScanRequest::new(locator)))
                        .await
                        .expect("read exact records")
                    else {
                        panic!("records response")
                    };
                    rows.records
                        .sort_by_key(|record| record.fields["sequence"].as_i64().unwrap());
                    assert_eq!(rows.records.len(), 2);
                    for (record, (label, sequence)) in
                        rows.records.iter().zip([("alpha", 1), ("beta", 2)])
                    {
                        assert!(record.id.is_some());
                        assert!(record.vectors.is_empty());
                        assert_eq!(
                            record.fields,
                            serde_json::Map::from_iter([
                                ("label".into(), serde_json::json!(label)),
                                ("sequence".into(), serde_json::json!(sequence)),
                            ])
                        );
                    }
                    let snapshot = serde_json::json!({"catalog": info, "records": rows.records});
                    drop(database);
                    snapshot
                });
                drop(runtime);
                fs::write(
                    std::env::var_os("STORAGE_FRESH_OUTCOME").unwrap(),
                    serde_json::to_vec(&snapshot).unwrap(),
                )
                .unwrap();
            })
            .expect("owned native thread")
            .join()
            .expect("settled native thread");
        return;
    }
    let output = Command::new("python3")
        .arg(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/surreal_legacy_store/fixture.py"),
        )
        .arg("fresh")
        .arg("--executable")
        .arg(std::env::current_exe().unwrap())
        .output()
        .expect("owned cold-reopen verifier");
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
