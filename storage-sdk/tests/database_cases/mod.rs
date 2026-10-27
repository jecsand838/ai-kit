use std::{collections::BTreeSet, panic::AssertUnwindSafe, time::Duration};

use futures::FutureExt;
use serde_json::json;
use storage_sdk::{
    Database, DatabaseBackend, StorageError,
    database::{
        StorageLocator,
        catalog::{
            AddColumnsRequest, AlterColumnsRequest, CatalogRequest, CatalogResponse,
            CollectionSchema, ColumnAlterationSpec, ComputedColumn, CreateCollectionMode,
            CreateCollectionRequest, DropColumnsRequest, FieldSchema, FieldType,
            ListCollectionsRequest, SchemaEvolutionRequest, VectorElementType,
        },
        config::{CommonConfig, DatabaseConfig, LanceConfig},
        data::{
            ComparisonOp, CountRequest, DataRecord, DataRequest, DataResponse, DeleteRequest,
            DenseVector, FilterExpr, InsertRequest, LookupRequest, MergeActions, MergeRequest,
            RecordId, ReplacePartitionRequest, ScanRequest, UpdateAssignment, UpdateRequest,
            WriteMode,
        },
        index::{CreateIndexRequest, IndexKind, IndexRequest, IndexResponse, WaitForIndexRequest},
        query::{
            DistanceMetric, NearestRequest, Projection, QueryRequest, RandomSampleRequest,
            ScopedQuery, ScoreKind, ScrollRequest, SearchByIdRequest, TextSearchRequest,
            VectorQueryOptions, VectorSearchParams,
        },
    },
};

#[cfg(feature = "surrealdb")]
use storage_sdk::database::config::{SurrealConfig, SurrealModeConfig};

#[cfg(feature = "qdrant")]
use storage_sdk::database::config::QdrantConfig;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Lance,
    LanceMemory,
    #[cfg(feature = "surrealdb")]
    SurrealMemory,
    #[cfg(feature = "surrealdb")]
    SurrealRocks,
    #[cfg(feature = "surrealdb")]
    SurrealKv,
    #[cfg(feature = "surrealdb")]
    SurrealHttp,
    #[cfg(feature = "surrealdb")]
    SurrealWs,
    #[cfg(feature = "qdrant")]
    QdrantEdge,
    #[cfg(feature = "qdrant")]
    QdrantRest,
}

impl Mode {
    #[cfg(feature = "qdrant")]
    async fn qdrant_cases(cases: &[Case]) {
        let mut failures = Vec::new();
        for mode in [Self::QdrantEdge, Self::QdrantRest] {
            if let Err(error) = AssertUnwindSafe(mode.cases(cases)).catch_unwind().await {
                let message = error
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| error.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic");
                failures.push(format!("{mode:?}: {message}"));
            }
        }
        assert!(
            failures.is_empty(),
            "Qdrant mode failures:\n{}",
            failures.join("\n")
        );
    }

    #[cfg(feature = "qdrant")]
    fn select_qdrant_default(configuration: &mut DatabaseConfig) {
        let DatabaseConfig::Qdrant { config, .. } = configuration else {
            panic!("Qdrant fixture configuration")
        };
        config.default_vector_name = Some("embedding".to_owned());
    }

    #[cfg(feature = "surrealdb")]
    const SURREAL: [Self; 5] = [
        Self::SurrealMemory,
        Self::SurrealRocks,
        Self::SurrealKv,
        Self::SurrealHttp,
        Self::SurrealWs,
    ];

    #[cfg(feature = "surrealdb")]
    fn surreal_cases(modes: &[Self], cases: &[Case]) {
        let modes = modes.to_vec();
        let cases = cases.to_vec();
        // SurrealDB 3.3.0's .cargo/config.toml documents the 8 MiB stack
        // needed by deep native query futures in unoptimised builds. Keep
        // that test-runtime requirement local to these native regressions.
        let worker = std::thread::Builder::new()
            .name("surreal-native-regressions".to_owned())
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("native regression runtime")
                    .block_on(async {
                        let mut failures = Vec::new();
                        for mode in modes {
                            if let Err(error) =
                                AssertUnwindSafe(mode.cases(&cases)).catch_unwind().await
                            {
                                let message = error
                                    .downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| error.downcast_ref::<&str>().copied())
                                    .unwrap_or("non-string panic");
                                failures.push(format!("{mode:?}: {message}"));
                            }
                        }
                        assert!(
                            failures.is_empty(),
                            "Surreal mode failures:\n{}",
                            failures.join("\n")
                        );
                    });
            })
            .expect("start owned native regression thread");
        if let Err(error) = worker.join() {
            std::panic::resume_unwind(error);
        }
    }

    fn is_lance(self) -> bool {
        matches!(self, Self::Lance | Self::LanceMemory)
    }

    fn is_surreal(self) -> bool {
        match self {
            #[cfg(feature = "surrealdb")]
            Self::SurrealMemory
            | Self::SurrealRocks
            | Self::SurrealKv
            | Self::SurrealHttp
            | Self::SurrealWs => true,
            Self::Lance | Self::LanceMemory => false,
            #[cfg(feature = "qdrant")]
            Self::QdrantEdge | Self::QdrantRest => false,
        }
    }

    fn is_qdrant(self) -> bool {
        !self.is_lance() && !self.is_surreal()
    }

    fn is_edge(self) -> bool {
        match self {
            #[cfg(feature = "qdrant")]
            Self::QdrantEdge => true,
            Self::Lance | Self::LanceMemory => false,
            #[cfg(feature = "surrealdb")]
            Self::SurrealMemory
            | Self::SurrealRocks
            | Self::SurrealKv
            | Self::SurrealHttp
            | Self::SurrealWs => false,
            #[cfg(feature = "qdrant")]
            Self::QdrantRest => false,
        }
    }

    fn configuration(self, directory: &std::path::Path) -> DatabaseConfig {
        match self {
            Self::Lance => DatabaseConfig::lance(LanceConfig::embedded_path(directory)),
            Self::LanceMemory => DatabaseConfig::lance(LanceConfig::embedded_path("memory://")),
            #[cfg(feature = "surrealdb")]
            Self::SurrealMemory => DatabaseConfig::surreal(SurrealConfig::default()),
            #[cfg(feature = "surrealdb")]
            Self::SurrealRocks => DatabaseConfig::surreal(SurrealConfig {
                mode: SurrealModeConfig::RocksDb {
                    path: directory.join("rocks"),
                },
                ..SurrealConfig::default()
            }),
            #[cfg(feature = "surrealdb")]
            Self::SurrealKv => DatabaseConfig::surreal(SurrealConfig {
                mode: SurrealModeConfig::SurrealKv {
                    path: directory.join("kv"),
                },
                ..SurrealConfig::default()
            }),
            #[cfg(feature = "surrealdb")]
            Self::SurrealHttp => DatabaseConfig::surreal(super::authenticated_surreal(
                SurrealModeConfig::RemoteHttp {
                    endpoint: std::env::var("TEST_SURREALDB_REMOTE_HTTP")
                        .expect("TEST_SURREALDB_REMOTE_HTTP"),
                },
            )),
            #[cfg(feature = "surrealdb")]
            Self::SurrealWs => {
                DatabaseConfig::surreal(super::authenticated_surreal(SurrealModeConfig::RemoteWs {
                    endpoint: std::env::var("TEST_SURREALDB_REMOTE_WS")
                        .expect("TEST_SURREALDB_REMOTE_WS"),
                }))
            }
            #[cfg(feature = "qdrant")]
            Self::QdrantEdge => DatabaseConfig::qdrant(QdrantConfig::edge(directory)),
            #[cfg(feature = "qdrant")]
            Self::QdrantRest => DatabaseConfig::qdrant(QdrantConfig::remote_rest(
                std::env::var("TEST_QDRANT_REMOTE_REST").expect("TEST_QDRANT_REMOTE_REST"),
            )),
        }
    }

    async fn cases(self, cases: &[Case]) {
        let mut failures = Vec::new();
        for case in cases {
            // Each case owns a new connection, collection and embedded directory.
            // A red case is cleaned up and recorded before the next case starts.
            let directory = tempfile::tempdir().expect("isolated real database directory");
            let configuration = self.configuration(directory.path());
            #[cfg(feature = "qdrant")]
            let configuration = {
                let mut configuration = configuration;
                if matches!(
                    case,
                    Case::QdrantLookup
                        | Case::QdrantNamedVectors
                        | Case::QdrantPayloadIndex
                        | Case::QdrantUuidPages
                        | Case::QdrantVectorRepresentations
                ) {
                    Self::select_qdrant_default(&mut configuration);
                }
                configuration
            };
            let database = tokio::time::timeout(
                Duration::from_secs(30),
                Database::connect(configuration.clone()),
            )
            .await
            .expect("real database connection deadline")
            .expect("real database connection");
            let fixture = DatabaseCase {
                database,
                configuration,
                mode: self,
                locator: StorageLocator::new(format!(
                    "{}proof_{}",
                    match case {
                        #[cfg(feature = "surrealdb")]
                        Case::SqCollection => "sq_",
                        _ => "",
                    },
                    uuid::Uuid::new_v4().simple()
                )),
            };
            // Heap-pin the concrete case future to reduce the outer runner's
            // debug-build stack pressure. Native polling and parser calls still
            // use the same runtime thread and remain inside this timeout.
            let observed = AssertUnwindSafe(tokio::time::timeout(
                Duration::from_secs(120),
                Box::pin(fixture.exercise(*case)),
            ))
            .catch_unwind()
            .await;
            let cleanup_locators = match case {
                #[cfg(feature = "surrealdb")]
                Case::ConcurrentScopes => {
                    let mut locators = vec![fixture.locator.clone()];
                    locators.extend(fixture.scope_locators());
                    locators
                }
                _ => vec![fixture.locator.clone()],
            };
            for locator in cleanup_locators {
                let cleanup = tokio::time::timeout(
                    Duration::from_secs(20),
                    fixture.database.catalog(CatalogRequest::DropCollection {
                        locator: locator.clone(),
                        ignore_missing: true,
                    }),
                )
                .await;
                if !matches!(cleanup, Ok(Ok(_))) {
                    failures.push(format!(
                        "{self:?} {case:?} {locator:?} collection cleanup: {cleanup:?}"
                    ));
                }
            }
            drop(fixture);
            let directory_cleanup = directory.close();
            match observed {
                Ok(Ok(())) => eprintln!("{self:?} {case:?}: passed"),
                Ok(Err(error)) => failures.push(format!("{self:?} {case:?}: {error}")),
                Err(error) => {
                    let message = error
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| error.downcast_ref::<&str>().copied())
                        .unwrap_or("non-string assertion panic");
                    failures.push(format!("{self:?} {case:?}: {message}"));
                }
            }
            if let Err(error) = directory_cleanup {
                failures.push(format!("{self:?} {case:?} directory cleanup: {error}"));
            }
        }
        assert!(
            failures.is_empty(),
            "real database cases failed:\n{}",
            failures.join("\n")
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum ScalarStringPredicate {
    Greater,
    Between,
    Prefix,
}

impl ScalarStringPredicate {
    fn filter(self) -> FilterExpr {
        match self {
            Self::Greater => FilterExpr::Compare {
                field: "mixed".to_owned(),
                op: ComparisonOp::Gt,
                value: json!("b"),
            },
            Self::Between => FilterExpr::Between {
                field: "mixed".to_owned(),
                lower: json!("b"),
                upper: json!("zz"),
            },
            Self::Prefix => FilterExpr::Like {
                field: "mixed".to_owned(),
                pattern: "a%".to_owned(),
                case_insensitive: false,
                negated: false,
            },
        }
    }

    fn expected(self) -> usize {
        match self {
            Self::Greater | Self::Between => 1,
            Self::Prefix => 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    #[cfg(feature = "qdrant")]
    QdrantLookup,
    #[cfg(feature = "qdrant")]
    QdrantNamedVectors,
    #[cfg(feature = "qdrant")]
    QdrantPayloadIndex,
    #[cfg(feature = "qdrant")]
    QdrantUuidPages,
    #[cfg(feature = "qdrant")]
    QdrantVectorRepresentations,
    #[cfg(feature = "surrealdb")]
    SqCollection,
    #[cfg(feature = "surrealdb")]
    FilteredIndexedVectors,
    #[cfg(feature = "surrealdb")]
    FullTextUpdate,
    #[cfg(feature = "surrealdb")]
    FullTextPartition,
    #[cfg(feature = "surrealdb")]
    ConcurrentScopes,
    Catalog,
    Data,
    UnfilteredUpdate,
    Lookup,
    Merge,
    Partition,
    PartitionCollision,
    PartitionNullCollision,
    PartitionNativeFailure,
    Schema,
    SqlReadColumnsRejection,
    SqlBackfill,
    ScanDeletedRows,
    CastAlreadyRequired,
    QuotedFilters {
        field: &'static str,
    },
    PartitionExactCounts,
    Nearest,
    #[cfg(feature = "surrealdb")]
    NearestValidationOrder,
    NearestOffset,
    SearchById,
    Scroll,
    Sample,
    ScalarIndex,
    FullTextIndex,
    #[cfg(feature = "surrealdb")]
    FullTextCollectionDrop,
    VectorIndex,
    InvalidIndex,
    Overwrite,
    EmptyOverwrite,
    #[cfg(feature = "surrealdb")]
    ScoreCollision {
        by_id: bool,
        projected: bool,
        numeric_payload: bool,
    },
    #[cfg(feature = "surrealdb")]
    TextScoreCollision {
        projected: bool,
        numeric_payload: bool,
    },
    #[cfg(feature = "surrealdb")]
    PlainScoreCollision {
        sample: bool,
    },
    RejectedOverwrite {
        unsupported_id: bool,
    },
    #[cfg(feature = "qdrant")]
    RejectedDimensionsOverwrite,
    #[cfg(feature = "qdrant")]
    EmptyWriteIdentity,
    #[cfg(feature = "qdrant")]
    RestVectorLimit {
        exceeds: bool,
    },
    JsonRoundTrip,
    JsonExplicitRoundTrip,
    ScalarStringEquality {
        json_column: bool,
    },
    ScalarStringAppend {
        json_column: bool,
    },
    ScalarStringUpdate {
        json_column: bool,
    },
    ScalarStringPredicate {
        json_column: bool,
        predicate: ScalarStringPredicate,
    },
    #[cfg(feature = "surrealdb")]
    RejectedSchemaOverwrite {
        invalid_name: bool,
    },
}

impl Case {
    const CATALOG_DATA: [Self; 9] = [
        Self::Catalog,
        Self::Data,
        Self::UnfilteredUpdate,
        Self::Lookup,
        Self::Merge,
        Self::Partition,
        Self::PartitionCollision,
        Self::PartitionNullCollision,
        Self::Schema,
    ];
    const QUERY_INDEX: [Self; 11] = [
        Self::Nearest,
        Self::NearestOffset,
        Self::SearchById,
        Self::Scroll,
        Self::Sample,
        Self::ScalarIndex,
        Self::FullTextIndex,
        Self::VectorIndex,
        Self::InvalidIndex,
        Self::Overwrite,
        Self::EmptyOverwrite,
    ];
}

struct DatabaseCase {
    database: Database,
    configuration: DatabaseConfig,
    mode: Mode,
    locator: StorageLocator,
}

impl DatabaseCase {
    fn record(&self, number: u64) -> DataRecord {
        let record = if self.mode.is_lance() {
            DataRecord::without_id()
        } else {
            DataRecord::new(number + 1)
        };
        record
            .vector("embedding", Self::vector(number))
            .json_field("marker", json!(format!("row-{number:04}")))
            .json_field("partition", json!(if number < 2 { "a" } else { "b" }))
            .json_field(
                "body",
                json!(if number == 0 {
                    "quartz unique"
                } else {
                    "ordinary text"
                }),
            )
            .json_field("number", json!(number))
            .json_field("status", json!("original"))
    }

    fn vector(number: u64) -> DenseVector {
        DenseVector::F32(vec![1.0, number as f32 + 1.0, 0.5, 0.25])
    }

    async fn seed(&self, count: u64) {
        self.create(
            (0..count).map(|number| self.record(number)).collect(),
            CreateCollectionMode::Create,
        )
        .await;
    }

    async fn create(&self, records: Vec<DataRecord>, mode: CreateCollectionMode) {
        let result = self
            .database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: None,
                records,
                mode,
            }))
            .await
            .expect("portable collection creation");
        assert!(matches!(result, CatalogResponse::Collection(_)));
    }

    async fn rows(&self) -> Vec<DataRecord> {
        let mut rows = Vec::new();
        let mut token = None;
        for _ in 0..128 {
            let mut request = ScanRequest::new(self.locator.clone()).limit(100);
            request.page_token = token;
            let DataResponse::Records(page) = self
                .database
                .data(DataRequest::Scan(request))
                .await
                .expect("public scan snapshot")
            else {
                panic!("scan returns records")
            };
            rows.extend(page.records);
            token = page.next_page;
            if token.is_none() {
                rows.sort_by_key(|row| row.fields["marker"].as_str().unwrap().to_owned());
                return rows;
            }
        }
        panic!("bounded public snapshot traversal did not terminate");
    }

    async fn snapshot(&self) -> (Vec<DataRecord>, CatalogResponse, Option<IndexResponse>) {
        let rows = self.rows().await;
        let catalog = self
            .database
            .catalog(CatalogRequest::OpenCollection {
                locator: self.locator.clone(),
            })
            .await
            .expect("public catalog snapshot");
        if let CatalogResponse::Collection(info) = &catalog
            && let Some(count) = info.record_count
        {
            assert_eq!(
                count,
                rows.len() as u64,
                "reported catalog count must be exact"
            );
        }
        let indexes = self
            .database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await;
        let indexes = if self.mode.is_edge() {
            // The supplied Edge adapter does not expose inventory. Record that
            // absence after its explicit Unsupported result, never an empty list.
            Self::unsupported(indexes);
            None
        } else {
            Some(indexes.expect("public index snapshot"))
        };
        (rows, catalog, indexes)
    }

    fn unsupported<T: std::fmt::Debug>(result: Result<T, StorageError>) {
        assert!(
            matches!(result, Err(StorageError::UnsupportedCapability { .. })),
            "operation must explicitly reject unsupported semantics: {result:?}"
        );
    }

    fn markers(rows: &[DataRecord]) -> BTreeSet<String> {
        rows.iter()
            .map(|row| row.fields["marker"].as_str().unwrap().to_owned())
            .collect()
    }

    fn assert_projected(row: &DataRecord) {
        assert_eq!(
            row.fields.len(),
            1,
            "projection returns only the requested payload field"
        );
        assert!(row.fields.contains_key("marker"));
        assert!(
            row.vectors.is_empty(),
            "unrequested vectors are not projected"
        );
    }

    async fn exercise(&self, case: Case) {
        match case {
            #[cfg(feature = "qdrant")]
            Case::QdrantLookup => self.qdrant_lookup().await,
            #[cfg(feature = "qdrant")]
            Case::QdrantNamedVectors => self.qdrant_named_vectors().await,
            #[cfg(feature = "qdrant")]
            Case::QdrantPayloadIndex => self.qdrant_payload_index().await,
            #[cfg(feature = "qdrant")]
            Case::QdrantUuidPages => self.qdrant_uuid_pages().await,
            #[cfg(feature = "qdrant")]
            Case::QdrantVectorRepresentations => self.qdrant_vector_representations().await,
            #[cfg(feature = "surrealdb")]
            Case::SqCollection => self.sq_collection().await,
            #[cfg(feature = "surrealdb")]
            Case::FilteredIndexedVectors => self.filtered_indexed_vectors().await,
            #[cfg(feature = "surrealdb")]
            Case::FullTextUpdate => self.fulltext_update().await,
            #[cfg(feature = "surrealdb")]
            Case::FullTextPartition => self.fulltext_partition().await,
            #[cfg(feature = "surrealdb")]
            Case::ConcurrentScopes => self.concurrent_scopes().await,
            Case::Catalog => self.catalog().await,
            Case::Data => self.data().await,
            Case::UnfilteredUpdate => self.unfiltered_update().await,
            Case::Lookup => self.lookup().await,
            Case::Merge => self.merge().await,
            Case::Partition => self.partition().await,
            Case::PartitionCollision => self.partition_collision(false).await,
            Case::PartitionNullCollision => self.partition_collision(true).await,
            Case::PartitionNativeFailure => self.partition_native_failure().await,
            Case::Schema => self.schema().await,
            Case::SqlReadColumnsRejection => self.sql_read_columns_rejection().await,
            Case::SqlBackfill => self.sql_backfill().await,
            Case::ScanDeletedRows => {
                self.seed_scan_deleted_rows().await;
                self.assert_scan_deleted_rows().await;
            }
            Case::CastAlreadyRequired => self.cast_already_required().await,
            Case::QuotedFilters { field } => self.quoted_filters(field).await,
            Case::PartitionExactCounts => self.partition_exact_counts().await,
            Case::Nearest => self.nearest().await,
            #[cfg(feature = "surrealdb")]
            Case::NearestValidationOrder => self.nearest_validation_order().await,
            Case::NearestOffset => self.nearest_offset().await,
            Case::SearchById => self.search_by_id().await,
            Case::Scroll => self.scroll().await,
            Case::Sample => self.sample().await,
            Case::ScalarIndex => self.index(IndexKind::Scalar, "marker").await,
            Case::FullTextIndex => self.index(IndexKind::FullText, "body").await,
            #[cfg(feature = "surrealdb")]
            Case::FullTextCollectionDrop => self.fulltext_collection_drop().await,
            Case::VectorIndex => {
                self.index(
                    IndexKind::Vector {
                        dimension: 4,
                        distance: Some(DistanceMetric::L2),
                    },
                    "embedding",
                )
                .await
            }
            Case::InvalidIndex => self.invalid_index().await,
            Case::Overwrite => self.overwrite().await,
            Case::EmptyOverwrite => self.empty_overwrite().await,
            #[cfg(feature = "surrealdb")]
            Case::ScoreCollision {
                by_id,
                projected,
                numeric_payload,
            } => {
                self.score_collision(by_id, projected, numeric_payload)
                    .await
            }
            #[cfg(feature = "surrealdb")]
            Case::TextScoreCollision {
                projected,
                numeric_payload,
            } => self.text_score_collision(projected, numeric_payload).await,
            #[cfg(feature = "surrealdb")]
            Case::PlainScoreCollision { sample } => self.plain_score_collision(sample).await,
            Case::RejectedOverwrite { unsupported_id } => {
                self.rejected_overwrite(unsupported_id).await
            }
            #[cfg(feature = "qdrant")]
            Case::RejectedDimensionsOverwrite => self.rejected_dimensions_overwrite().await,
            #[cfg(feature = "qdrant")]
            Case::EmptyWriteIdentity => self.empty_write_identity().await,
            #[cfg(feature = "qdrant")]
            Case::RestVectorLimit { exceeds } => self.rest_vector_limit(exceeds).await,
            Case::JsonRoundTrip => self.json_round_trip(false).await,
            Case::JsonExplicitRoundTrip => self.json_round_trip(true).await,
            Case::ScalarStringEquality { json_column } => {
                self.scalar_string_equality(json_column).await
            }
            Case::ScalarStringAppend { json_column } => {
                self.scalar_string_append(json_column).await
            }
            Case::ScalarStringUpdate { json_column } => {
                self.scalar_string_update(json_column).await
            }
            Case::ScalarStringPredicate {
                json_column,
                predicate,
            } => self.scalar_string_predicate(json_column, predicate).await,
            #[cfg(feature = "surrealdb")]
            Case::RejectedSchemaOverwrite { invalid_name } => {
                self.rejected_schema_overwrite(invalid_name).await
            }
        }
    }

    #[cfg(feature = "surrealdb")]
    fn surreal_record(&self, number: u64) -> DataRecord {
        DataRecord::new(number + 1)
            .json_field("marker", json!(format!("row-{number:04}")))
            .json_field("number", json!(number))
            .json_field("partition", json!(if number < 2 { "a" } else { "b" }))
            .json_field(
                "body",
                json!(if number == 0 {
                    "quartz unique"
                } else {
                    "ordinary text"
                }),
            )
    }

    #[cfg(feature = "surrealdb")]
    fn surreal_row(&self, number: u64) -> DataRecord {
        // The supplied Surreal codec preserves the native table-qualified ID.
        // Construct expected data; never rewrite or normalize returned records.
        DataRecord {
            id: Some(RecordId::String(format!(
                "{}:{}",
                self.locator.collection,
                number + 1
            ))),
            ..self.surreal_record(number)
        }
    }

    #[cfg(feature = "surrealdb")]
    async fn sq_collection(&self) {
        assert!(self.locator.collection.starts_with("sq_"));
        let DatabaseConfig::Surreal { config, .. } = &self.configuration else {
            panic!("Surreal fixture configuration")
        };
        let expected_locator = self
            .locator
            .clone()
            .with_namespace([config.namespace.clone().expect("configured namespace")])
            .with_database(config.database.clone().expect("configured database"));
        let expected = (0..3)
            .map(|number| self.surreal_record(number))
            .collect::<Vec<_>>();
        self.create(expected, CreateCollectionMode::Create).await;
        let CatalogResponse::Collection(opened) = self
            .database
            .catalog(CatalogRequest::OpenCollection {
                locator: self.locator.clone(),
            })
            .await
            .expect("open populated sq-prefixed collection")
        else {
            panic!("collection info")
        };
        for populated in [true, false] {
            let CatalogResponse::Collections(collections) = self
                .database
                .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
                    namespace: Vec::new(),
                    limit: None,
                    start_after: None,
                }))
                .await
                .expect("list with sq-prefixed table keys")
            else {
                panic!("collection inventory")
            };
            let matching = collections
                .iter()
                .filter(|info| info.locator.collection == self.locator.collection)
                .collect::<Vec<_>>();
            if populated {
                assert_eq!(matching.len(), 1);
                assert_eq!(matching[0].locator, expected_locator);
                assert_eq!(opened.locator, self.locator);
                assert_eq!(opened.locator.collection, self.locator.collection);
                assert_eq!(
                    self.rows().await,
                    (0..3)
                        .map(|number| self.surreal_row(number))
                        .collect::<Vec<_>>()
                );
                self.database
                    .catalog(CatalogRequest::DropCollection {
                        locator: self.locator.clone(),
                        ignore_missing: false,
                    })
                    .await
                    .expect("drop populated sq-prefixed collection");
            } else {
                assert!(matching.is_empty());
                assert!(
                    self.database
                        .catalog(CatalogRequest::OpenCollection {
                            locator: self.locator.clone(),
                        })
                        .await
                        .is_err(),
                    "dropped sq-prefixed collection cannot reopen"
                );
            }
        }
    }

    #[cfg(feature = "surrealdb")]
    async fn create_ready_index(&self, kind: IndexKind, column: &str) -> IndexResponse {
        let IndexResponse::Indexes(before) = self
            .database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await
            .expect("inventory before create")
        else {
            panic!("index inventory")
        };
        assert_eq!(
            self.database
                .index(IndexRequest::Create(CreateIndexRequest {
                    locator: self.locator.clone(),
                    columns: vec![column.to_owned()],
                    kind,
                    wait_timeout_ms: Some(30_000),
                }))
                .await
                .expect("create real index"),
            IndexResponse::Completed
        );
        let inventory = self
            .database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await
            .expect("index inventory");
        let IndexResponse::Indexes(indexes) = &inventory else {
            panic!("index inventory")
        };
        let matching = indexes
            .iter()
            .filter(|index| !before.iter().any(|old| old.name == index.name))
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert_eq!(indexes.len(), before.len() + 1);
        if !matching[0].columns.is_empty() {
            assert_eq!(matching[0].columns, [column]);
        }
        let name = matching[0].name.clone().expect("index name");
        self.database
            .index(IndexRequest::WaitFor(WaitForIndexRequest {
                locator: self.locator.clone(),
                index_names: vec![name],
                timeout_ms: 30_000,
            }))
            .await
            .expect("wait for real index");
        self.database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await
            .expect("ready inventory")
    }

    #[cfg(feature = "surrealdb")]
    async fn filtered_indexed_vectors(&self) {
        // Physical identity order differs from distance order.
        let ids = [71, 22, 63, 14, 55, 36, 47];
        let records = (1..=7)
            .map(|number| {
                DataRecord {
                    id: Some(RecordId::U64(ids[(number - 1) as usize])),
                    ..self.surreal_record(number)
                }
                .vector(
                    "embedding",
                    DenseVector::F32(vec![number as f32 * 10.0, 0.0, 0.0, 0.0]),
                )
                .json_field(
                    "category",
                    json!(if [1, 2, 4, 5, 7].contains(&number) {
                        "a"
                    } else {
                        "b"
                    }),
                )
                .json_field("flag", json!(![2, 4].contains(&number)))
            })
            .collect::<Vec<_>>();
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(CollectionSchema {
                    fields: vec![FieldSchema {
                        name: "category".to_owned(),
                        field_type: FieldType::String,
                        nullable: false,
                        metadata: Default::default(),
                    }],
                    metadata: Default::default(),
                }),
                records,
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("seed scalar-prefilter fixture");
        self.create_ready_index(IndexKind::Scalar, "category").await;
        self.create_ready_index(
            IndexKind::Vector {
                dimension: 4,
                distance: Some(DistanceMetric::L2),
            },
            "embedding",
        )
        .await;
        let before = self.snapshot().await;
        let query = ScopedQuery::vector(self.locator.clone())
            .vector_field("embedding")
            .filter(FilterExpr::eq("category", "a").and(FilterExpr::eq("flag", true)))
            .projection(Projection::Columns(vec![
                "id".to_owned(),
                "marker".to_owned(),
                "category".to_owned(),
                "flag".to_owned(),
            ]))
            .params(VectorSearchParams {
                limit: 2,
                distance: None,
                ef: Some(64),
                ..VectorSearchParams::default()
            });
        for by_id in [false, true] {
            let request = if by_id {
                QueryRequest::SearchById(SearchByIdRequest {
                    query: query.clone(),
                    id: RecordId::U64(ids[0]),
                })
            } else {
                QueryRequest::Nearest(NearestRequest {
                    query: query.clone(),
                    vector: DenseVector::F32(vec![0.0; 4]),
                })
            };
            let response = self
                .database
                .query(request)
                .await
                .expect("filtered indexed top-k");
            let expected = if by_id { [5, 7] } else { [1, 5] };
            assert_eq!(
                response.hits.len(),
                2,
                "excluded nearer rows cannot consume top-k slots"
            );
            for (hit, number) in response.hits.iter().zip(expected) {
                assert_eq!(
                    hit.record.id,
                    Some(RecordId::String(format!(
                        "{}:{}",
                        self.locator.collection,
                        ids[(number - 1) as usize]
                    )))
                );
                assert_eq!(hit.record.fields["marker"], format!("row-{number:04}"));
                assert_eq!(hit.record.fields["category"], "a");
                assert_eq!(hit.record.fields["flag"], true);
                assert_eq!(hit.record.fields.len(), 3);
                assert!(hit.record.vectors.is_empty());
            }
            eprintln!(
                "{:?} indexed {} ranked identities and projection: passed",
                self.mode,
                if by_id { "search-by-ID" } else { "nearest" }
            );
            let empty_query = query
                .clone()
                .filter(FilterExpr::eq("category", "absent").and(FilterExpr::eq("flag", true)));
            let empty = if by_id {
                QueryRequest::SearchById(SearchByIdRequest {
                    query: empty_query,
                    id: RecordId::U64(ids[0]),
                })
            } else {
                QueryRequest::Nearest(NearestRequest {
                    query: empty_query,
                    vector: DenseVector::F32(vec![0.0; 4]),
                })
            };
            assert!(
                self.database
                    .query(empty)
                    .await
                    .expect("empty indexed predicate")
                    .hits
                    .is_empty()
            );
        }
        assert_eq!(self.snapshot().await, before);
    }

    #[cfg(feature = "surrealdb")]
    async fn assert_text_records(&self, text: &str, expected: &[DataRecord]) {
        let response = self
            .database
            .query(QueryRequest::Text(TextSearchRequest {
                query: ScopedQuery::text(self.locator.clone())
                    .text_field("body")
                    .limit(10),
                text: text.to_owned(),
            }))
            .await
            .expect("native full-text results after mutation");
        let mut actual = response
            .hits
            .into_iter()
            .map(|hit| hit.record)
            .collect::<Vec<_>>();
        actual.sort_by_key(|row| row.fields["marker"].as_str().unwrap().to_owned());
        assert_eq!(actual, expected, "exact full-text records for {text}");
    }

    #[cfg(feature = "surrealdb")]
    async fn fulltext_update(&self) {
        self.create(
            vec![self.surreal_record(0), self.surreal_record(1)],
            CreateCollectionMode::Create,
        )
        .await;
        let inventory = self.create_ready_index(IndexKind::FullText, "body").await;
        self.assert_text_records("quartz", &[self.surreal_row(0)])
            .await;
        self.database
            .data(DataRequest::Update(UpdateRequest {
                locator: self.locator.clone(),
                filter: Some(FilterExpr::eq("marker", "row-0000")),
                assignments: vec![UpdateAssignment {
                    column: "body".to_owned(),
                    value: json!("amber unique"),
                }],
            }))
            .await
            .expect("update indexed text without rebuilding");
        let changed = self
            .surreal_row(0)
            .json_field("body", json!("amber unique"));
        self.assert_text_records("quartz", &[]).await;
        self.assert_text_records("amber", std::slice::from_ref(&changed))
            .await;
        self.assert_text_records("ordinary", &[self.surreal_row(1)])
            .await;
        assert_eq!(self.rows().await, [changed, self.surreal_row(1)]);
        assert_eq!(
            self.database
                .index(IndexRequest::List {
                    locator: self.locator.clone()
                })
                .await
                .unwrap(),
            inventory
        );
    }

    #[cfg(feature = "surrealdb")]
    async fn fulltext_partition(&self) {
        let original = (0..4)
            .map(|number| {
                self.surreal_record(number)
                    .json_field("body", json!("quartz shared"))
            })
            .collect::<Vec<_>>();
        self.create(original.clone(), CreateCollectionMode::Create)
            .await;
        let inventory = self.create_ready_index(IndexKind::FullText, "body").await;
        let original = (0..4)
            .map(|number| {
                self.surreal_row(number)
                    .json_field("body", json!("quartz shared"))
            })
            .collect::<Vec<_>>();
        self.assert_text_records("quartz", &original).await;
        let replacement = (4..6)
            .map(|number| DataRecord {
                id: None,
                ..self
                    .surreal_record(number)
                    .json_field("partition", json!("a"))
                    .json_field("body", json!("amber shared"))
            })
            .collect::<Vec<_>>();
        for records in [replacement, Vec::new()] {
            self.database
                .data(DataRequest::ReplacePartition(
                    ReplacePartitionRequest::new(
                        self.locator.clone(),
                        FilterExpr::eq("partition", "a"),
                        "marker",
                        records.clone(),
                    )
                    .expect("valid replacement"),
                ))
                .await
                .expect("atomic indexed partition replacement");
            let stored = self.rows().await;
            assert_eq!(stored.len(), 2 + records.len());
            assert_eq!(
                &stored[..2],
                &original[2..],
                "unaffected records keep their native identities and complete payloads"
            );
            let mut identities = BTreeSet::new();
            for (actual, input) in stored[2..].iter().zip(&records) {
                assert_eq!(actual.fields, input.fields);
                assert_eq!(actual.vectors, input.vectors);
                let id = actual
                    .id
                    .as_ref()
                    .expect("replacement receives a native identity");
                assert!(
                    identities.insert(serde_json::to_string(id).unwrap()),
                    "replacement identities are distinct"
                );
                assert!(
                    !original.iter().any(|old| old.id.as_ref() == Some(id)),
                    "replacement uses new identities"
                );
            }
            // The public replacement request forbids caller IDs. Compare text
            // hits with the fully checked stored rows, preserving native IDs.
            self.assert_text_records("quartz", &original[2..]).await;
            self.assert_text_records("amber", &stored[2..]).await;
            assert_eq!(
                self.database
                    .index(IndexRequest::List {
                        locator: self.locator.clone()
                    })
                    .await
                    .unwrap(),
                inventory
            );
        }
    }

    #[cfg(feature = "surrealdb")]
    fn scope_locators(&self) -> Vec<StorageLocator> {
        let ns_a = format!("ns_a_{}", self.locator.collection);
        let ns_b = format!("ns_b_{}", self.locator.collection);
        let db_a = format!("db_a_{}", self.locator.collection);
        let db_b = format!("db_b_{}", self.locator.collection);
        [(ns_a.clone(), db_a.clone()), (ns_a, db_b), (ns_b, db_a)]
            .into_iter()
            .map(|(namespace, database)| {
                self.locator
                    .clone()
                    .with_namespace([namespace])
                    .with_database(database)
            })
            .collect()
    }

    #[cfg(feature = "surrealdb")]
    async fn assert_missing_surreal_table(&self) {
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            self.database.catalog(CatalogRequest::OpenCollection {
                locator: self.locator.clone(),
            }),
        )
        .await
        .expect("missing scoped collection observation deadline");
        let Err(StorageError::BackendOperation { operation, message }) = result else {
            panic!("expected native missing-table failure for owned collection, got {result:?}");
        };
        assert_eq!(operation, "query");
        // Exact target receipt captured through OpenCollection in all five
        // real modes. An empty collection, transport error or other native
        // failure does not prove that the failed create left no table.
        assert_eq!(
            message,
            format!(
                r#"{{0: Error {{ code: -32000, message: "The table '{name}' does not exist", details: NotFound(Some(Table {{ name: "{name}" }})), cause: None }}}}"#,
                name = self.locator.collection,
            )
        );
    }

    #[cfg(feature = "surrealdb")]
    async fn concurrent_scopes(&self) {
        self.create(vec![self.surreal_record(0)], CreateCollectionMode::Create)
            .await; // Sentinel in the configured default context only.
        let scopes = self
            .scope_locators()
            .into_iter()
            .map(|locator| Self {
                database: self.database.clone(),
                configuration: self.configuration.clone(),
                mode: self.mode,
                locator,
            })
            .collect::<Vec<_>>();
        let expected = (0..3)
            .map(|number| self.surreal_record(0).json_field("scope", json!(number)))
            .collect::<Vec<_>>();
        let barrier = tokio::sync::Barrier::new(scopes.len());
        let results =
            futures::future::join_all(scopes.iter().zip(&expected).map(|(scope, record)| {
                let barrier = &barrier;
                async move {
                    barrier.wait().await;
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        scope.database.catalog(CatalogRequest::CreateCollection(
                            CreateCollectionRequest {
                                locator: scope.locator.clone(),
                                schema: None,
                                records: vec![record.clone()],
                                mode: CreateCollectionMode::Create,
                            },
                        )),
                    )
                    .await
                }
            }))
            .await;
        // Every cold first-use future settles before assertion or recovery.
        // Native metadata creation can conflict. This caller retries once,
        // serially, only after proving that the exact target conflict left no
        // collection; the SDK does not replay the write.
        for ((scope, record), result) in scopes.iter().zip(&expected).zip(results) {
            let response = match result.expect("concurrent create deadline") {
                Ok(response) => response,
                Err(error) => {
                    let StorageError::BackendOperation { operation, message } = &error else {
                        panic!("unexpected cold scoped create failure: {error:?}");
                    };
                    assert_eq!(
                        operation, "query",
                        "unexpected cold scoped create failure: {error:?}"
                    );
                    assert_eq!(
                        message,
                        r#"{0: Error { code: -32000, message: "There was a problem with the key-value store: Transaction conflict: Resource busy. This transaction can be retried", details: Query(Some(TransactionConflict)), cause: None }}"#,
                        "only the single observed native transaction conflict permits absence inspection"
                    );
                    scope.assert_missing_surreal_table().await;
                    eprintln!(
                        "{:?} scoped create conflict: exact missing table proven; one caller retry",
                        self.mode
                    );
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        scope.database.catalog(CatalogRequest::CreateCollection(
                            CreateCollectionRequest {
                                locator: scope.locator.clone(),
                                schema: None,
                                records: vec![record.clone()],
                                mode: CreateCollectionMode::Create,
                            },
                        )),
                    )
                    .await
                    .expect("caller scoped create retry deadline")
                    .expect("single serial caller retry after proven absence")
                }
            };
            let CatalogResponse::Collection(info) = response else {
                panic!("expected created collection info, got {response:?}");
            };
            assert_eq!(info.locator, scope.locator);
            assert_eq!(info.record_count, Some(1));
        }
        let expected = (0..3)
            .map(|number| self.surreal_row(0).json_field("scope", json!(number)))
            .collect::<Vec<_>>();
        for _ in 0..3 {
            for (scope, record) in scopes.iter().zip(&expected) {
                assert_eq!(scope.rows().await, std::slice::from_ref(record));
            }
            assert_eq!(self.rows().await, [self.surreal_row(0)]);
        }
        scopes[0]
            .database
            .data(DataRequest::Update(UpdateRequest {
                locator: scopes[0].locator.clone(),
                filter: None,
                assignments: vec![UpdateAssignment {
                    column: "scope".to_owned(),
                    value: json!("changed"),
                }],
            }))
            .await
            .expect("mutate one scoped clone");
        assert_eq!(
            scopes[0].rows().await,
            [expected[0].clone().json_field("scope", json!("changed"))]
        );
        for (scope, record) in scopes[1..].iter().zip(&expected[1..]) {
            assert_eq!(scope.rows().await, std::slice::from_ref(record));
        }
        assert_eq!(self.rows().await, [self.surreal_row(0)]);
        scopes[0]
            .database
            .catalog(CatalogRequest::DropCollection {
                locator: scopes[0].locator.clone(),
                ignore_missing: false,
            })
            .await
            .expect("drop one scope");
        scopes[0].assert_missing_surreal_table().await;
        for (scope, record) in scopes[1..].iter().zip(&expected[1..]) {
            assert_eq!(scope.rows().await, std::slice::from_ref(record));
        }
        assert_eq!(self.rows().await, [self.surreal_row(0)]);
    }

    async fn catalog(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        let duplicate = self
            .database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: None,
                records: vec![self.record(9)],
                mode: CreateCollectionMode::Create,
            }))
            .await;
        assert!(
            duplicate.is_err(),
            "Create must reject an existing collection"
        );
        assert_eq!(self.snapshot().await, before);
        self.create(vec![self.record(9)], CreateCollectionMode::CreateIfMissing)
            .await;
        assert_eq!(
            self.snapshot().await,
            before,
            "reuse ignores replacement seed"
        );
        let CatalogResponse::Collections(collections) = self
            .database
            .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
                namespace: Vec::new(),
                limit: None,
                start_after: None,
            }))
            .await
            .expect("list real collections")
        else {
            panic!("catalog listing")
        };
        assert!(
            collections
                .iter()
                .any(|info| info.locator.collection == self.locator.collection)
        );
        let CatalogResponse::Collections(page) = self
            .database
            .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
                namespace: Vec::new(),
                limit: Some(1),
                start_after: Some(self.locator.collection.clone()),
            }))
            .await
            .expect("exclusive catalog page")
        else {
            panic!("catalog page")
        };
        assert!(page.len() <= 1);
        assert!(
            page.iter()
                .all(|info| info.locator.collection > self.locator.collection)
        );
        self.create(vec![self.record(9)], CreateCollectionMode::Overwrite)
            .await;
        assert_eq!(
            Self::markers(&self.rows().await),
            BTreeSet::from(["row-0009".to_owned()])
        );
        self.database
            .catalog(CatalogRequest::DropCollection {
                locator: self.locator.clone(),
                ignore_missing: false,
            })
            .await
            .expect("drop existing collection");
        assert!(
            self.database
                .catalog(CatalogRequest::OpenCollection {
                    locator: self.locator.clone(),
                })
                .await
                .is_err(),
            "dropped collection must be absent"
        );
        assert!(
            self.database
                .catalog(CatalogRequest::DropCollection {
                    locator: self.locator.clone(),
                    ignore_missing: false,
                })
                .await
                .is_err(),
            "strict drop reports missing target"
        );
        self.database
            .catalog(CatalogRequest::DropCollection {
                locator: self.locator.clone(),
                ignore_missing: true,
            })
            .await
            .expect("explicitly tolerate missing target");
    }

    async fn data(&self) {
        self.seed(3).await;
        self.database
            .data(DataRequest::Insert(InsertRequest::append(
                self.locator.clone(),
                vec![self.record(3)],
            )))
            .await
            .expect("append a new record");
        let appended = self.rows().await;
        assert_eq!(appended.len(), 4);
        assert_eq!(
            Self::markers(&appended),
            BTreeSet::from([
                "row-0000".to_owned(),
                "row-0001".to_owned(),
                "row-0002".to_owned(),
                "row-0003".to_owned(),
            ])
        );
        for (filter, count) in [(None, 4), (Some(FilterExpr::eq("partition", "a")), 2)] {
            assert_eq!(
                self.database
                    .data(DataRequest::Count(CountRequest {
                        locator: self.locator.clone(),
                        filter,
                    }))
                    .await
                    .expect("exact public count"),
                DataResponse::Count(count)
            );
        }
        let DataResponse::Records(page) = self
            .database
            .data(DataRequest::Scan(
                ScanRequest::new(self.locator.clone())
                    .filter(FilterExpr::eq("partition", "a"))
                    .projection(Projection::Columns(vec!["marker".to_owned()]))
                    .limit(1),
            ))
            .await
            .expect("filtered projected scan")
        else {
            panic!("scan records")
        };
        assert_eq!(page.records.len(), 1);
        Self::assert_projected(&page.records[0]);
        assert!(
            ["row-0000", "row-0001"].contains(&page.records[0].fields["marker"].as_str().unwrap())
        );
        let mut seen = BTreeSet::new();
        let mut token = None;
        let mut ended = false;
        for _ in 0..5 {
            let mut request = ScanRequest::new(self.locator.clone()).limit(2);
            request.page_token = token;
            let DataResponse::Records(page) = self
                .database
                .data(DataRequest::Scan(request))
                .await
                .expect("data scan continuation")
            else {
                panic!("scan records")
            };
            assert!(page.records.len() <= 2);
            for record in page.records {
                assert!(
                    seen.insert(record.fields["marker"].as_str().unwrap().to_owned()),
                    "data scan continuation cannot repeat a record"
                );
            }
            token = page.next_page;
            if token.is_none() {
                ended = true;
                break;
            }
        }
        assert!(ended, "data scan pagination must terminate");
        assert_eq!(seen, Self::markers(&self.rows().await));
        let before = self.rows().await;
        self.database
            .data(DataRequest::Update(UpdateRequest {
                locator: self.locator.clone(),
                filter: Some(FilterExpr::eq("partition", "a")),
                assignments: vec![UpdateAssignment {
                    column: "status".to_owned(),
                    value: json!("changed"),
                }],
            }))
            .await
            .expect("filtered update");
        let after = self.rows().await;
        assert_eq!(after.len(), before.len(), "update preserves every record");
        for (old, new) in before.iter().zip(&after) {
            let mut expected = old.clone();
            expected.fields.insert(
                "status".to_owned(),
                json!(if old.fields["partition"] == "a" {
                    "changed"
                } else {
                    "original"
                }),
            );
            assert_eq!(
                new, &expected,
                "filtered update changes only its assignment"
            );
        }
        self.database
            .data(DataRequest::Delete(DeleteRequest {
                locator: self.locator.clone(),
                filter: FilterExpr::eq("partition", "a"),
            }))
            .await
            .expect("filtered deletion");
        assert_eq!(
            self.rows().await,
            after
                .into_iter()
                .filter(|row| row.fields["partition"] == "b")
                .collect::<Vec<_>>(),
            "deletion preserves every nonmatching record unchanged"
        );
    }

    async fn unfiltered_update(&self) {
        self.seed(3).await;
        let before = self.rows().await;
        self.database
            .data(DataRequest::Update(UpdateRequest {
                locator: self.locator.clone(),
                filter: None,
                assignments: vec![UpdateAssignment {
                    column: "status".to_owned(),
                    value: json!("all"),
                }],
            }))
            .await
            .expect("unfiltered update");
        let expected = before
            .into_iter()
            .map(|mut row| {
                row.fields.insert("status".to_owned(), json!("all"));
                row
            })
            .collect::<Vec<_>>();
        assert_eq!(
            self.rows().await,
            expected,
            "unfiltered update preserves all other data"
        );
    }

    async fn lookup(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        let result = self
            .database
            .data(DataRequest::Lookup(
                LookupRequest::new(self.locator.clone(), [1_u64, 999_u64])
                    .projection(Projection::Columns(vec!["marker".to_owned()])),
            ))
            .await;
        if self.mode.is_lance() {
            Self::unsupported(result);
            let mut record = self.record(9);
            record.id = Some(RecordId::U64(10));
            Self::unsupported(
                self.database
                    .data(DataRequest::Insert(InsertRequest::append(
                        self.locator.clone(),
                        vec![record],
                    )))
                    .await,
            );
        } else {
            let DataResponse::Records(page) = result.expect("lookup existing and missing IDs")
            else {
                panic!("lookup records")
            };
            assert_eq!(page.records.len(), 1);
            Self::assert_projected(&page.records[0]);
            assert_eq!(page.records[0].fields["marker"], "row-0000");
            let DataResponse::Records(all) = self
                .database
                .data(DataRequest::Lookup(LookupRequest::new(
                    self.locator.clone(),
                    [1_u64, 2_u64, 999_u64],
                )))
                .await
                .expect("multiple point targets with all fields")
            else {
                panic!("lookup records")
            };
            assert_eq!(all.records.len(), 2);
            assert_eq!(
                all.records
                    .iter()
                    .map(|record| record.fields["marker"].as_str().unwrap())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from(["row-0000", "row-0001"])
            );
            let DataResponse::Records(empty) = self
                .database
                .data(DataRequest::Lookup(
                    LookupRequest::new(self.locator.clone(), Vec::<u64>::new())
                        .projection(Projection::Columns(vec!["marker".to_owned()])),
                ))
                .await
                .expect("empty logical-id lookup")
            else {
                panic!("lookup records")
            };
            assert!(empty.records.is_empty());
        }
        assert_eq!(self.snapshot().await, before);
    }

    async fn merge(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        let request = MergeRequest {
            locator: self.locator.clone(),
            on: vec!["id".to_owned()],
            records: vec![
                self.record(0).json_field("status", json!("merged")),
                self.record(9),
            ],
            actions: MergeActions::default(),
        };
        if self.mode.is_lance() {
            Self::unsupported(self.database.data(DataRequest::Merge(request)).await);
            assert_eq!(self.snapshot().await, before);
            return;
        }
        for invalid in [
            MergeRequest {
                on: vec!["marker".to_owned()],
                ..request.clone()
            },
            MergeRequest {
                actions: MergeActions {
                    delete_not_matched_by_source: true,
                    ..MergeActions::default()
                },
                ..request.clone()
            },
        ] {
            Self::unsupported(self.database.data(DataRequest::Merge(invalid)).await);
            assert_eq!(
                self.snapshot().await,
                before,
                "unsupported merge cannot mutate"
            );
        }
        for on in [vec!["id".to_owned()], Vec::new()] {
            self.database
                .data(DataRequest::Merge(MergeRequest {
                    on,
                    ..request.clone()
                }))
                .await
                .expect("default ID upsert merge");
            let rows = self.rows().await;
            assert_eq!(
                rows.len(),
                4,
                "merge preserves unrelated rows without duplicates"
            );
            assert_eq!(
                Self::markers(&rows),
                BTreeSet::from([
                    "row-0000".to_owned(),
                    "row-0001".to_owned(),
                    "row-0002".to_owned(),
                    "row-0009".to_owned(),
                ])
            );
            assert_eq!(rows[0].fields["status"], "merged");
            assert_eq!(rows[1].fields["status"], "original");
        }
    }

    async fn partition(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        let mut replacement = self.record(9).json_field("partition", json!("a"));
        replacement.id = None;
        let request = ReplacePartitionRequest::new(
            self.locator.clone(),
            FilterExpr::eq("partition", "a"),
            "marker",
            vec![replacement.clone()],
        )
        .expect("valid bounded replacement");
        if self.mode.is_qdrant() {
            Self::unsupported(
                self.database
                    .data(DataRequest::ReplacePartition(request))
                    .await,
            );
            assert_eq!(self.snapshot().await, before);
            return;
        }
        let invalid = ReplacePartitionRequest {
            records: vec![replacement.clone(), replacement],
            ..request.clone()
        };
        assert!(matches!(
            self.database
                .data(DataRequest::ReplacePartition(invalid))
                .await,
            Err(StorageError::InvalidRequest(_))
        ));
        assert_eq!(self.snapshot().await, before);
        self.database
            .data(DataRequest::ReplacePartition(request.clone()))
            .await
            .expect("replace one bounded partition");
        assert_eq!(
            Self::markers(&self.rows().await),
            BTreeSet::from(["row-0002".to_owned(), "row-0009".to_owned()])
        );
        self.database
            .data(DataRequest::ReplacePartition(ReplacePartitionRequest {
                records: Vec::new(),
                ..request
            }))
            .await
            .expect("empty replacement clears only its partition");
        assert_eq!(
            Self::markers(&self.rows().await),
            BTreeSet::from(["row-0002".to_owned()])
        );
    }

    async fn schema(&self) {
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(CollectionSchema {
                    fields: [
                        ("marker", FieldType::String),
                        ("partition", FieldType::String),
                        ("body", FieldType::String),
                        ("number", FieldType::I64),
                        ("status", FieldType::String),
                        (
                            "embedding",
                            FieldType::DenseVector {
                                dim: 4,
                                element: VectorElementType::F32,
                            },
                        ),
                    ]
                    .into_iter()
                    .map(|(name, field_type)| FieldSchema {
                        name: name.to_owned(),
                        field_type,
                        nullable: false,
                        metadata: Default::default(),
                    })
                    .collect(),
                    metadata: Default::default(),
                }),
                records: (0..3).map(|number| self.record(number)).collect(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("create explicit portable schema for alteration cases");
        let before = self.snapshot().await;
        let add = AddColumnsRequest {
            locator: self.locator.clone(),
            expressions: vec![ComputedColumn {
                name: "extra".to_owned(),
                expression_sql: "1".to_owned(),
            }],
            read_columns: None,
        };
        let rename = ColumnAlterationSpec {
            path: "status".to_owned(),
            rename: Some("state".to_owned()),
            nullable: None,
            cast_to: None,
        };
        let drop = DropColumnsRequest {
            locator: self.locator.clone(),
            columns: vec!["status".to_owned()],
        };
        if self.mode.is_qdrant() {
            for request in [
                SchemaEvolutionRequest::AddColumns(add),
                SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                    locator: self.locator.clone(),
                    alterations: vec![rename],
                }),
                SchemaEvolutionRequest::DropColumns(drop),
            ] {
                Self::unsupported(
                    self.database
                        .catalog(CatalogRequest::AlterCollection(request))
                        .await,
                );
                assert_eq!(self.snapshot().await, before);
            }
            return;
        }
        for read_columns in [vec!["number".to_owned()], Vec::new()] {
            Self::unsupported(
                self.database
                    .catalog(CatalogRequest::AlterCollection(
                        SchemaEvolutionRequest::AddColumns(AddColumnsRequest {
                            read_columns: Some(read_columns),
                            ..add.clone()
                        }),
                    ))
                    .await,
            );
            assert_eq!(self.snapshot().await, before);
        }
        if self.mode.is_surreal() {
            Self::unsupported(
                self.database
                    .catalog(CatalogRequest::AlterCollection(
                        SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                            locator: self.locator.clone(),
                            alterations: vec![
                                rename.clone(),
                                ColumnAlterationSpec {
                                    path: "body".to_owned(),
                                    rename: None,
                                    nullable: Some(true),
                                    cast_to: None,
                                },
                            ],
                        }),
                    ))
                    .await,
            );
            assert_eq!(
                self.snapshot().await,
                before,
                "later unsupported alteration cannot apply earlier rename"
            );
        }
        self.database
            .catalog(CatalogRequest::AlterCollection(
                SchemaEvolutionRequest::AddColumns(add),
            ))
            .await
            .expect("add computed column");
        for row in self.rows().await {
            assert_eq!(row.fields["extra"].as_u64(), Some(1));
        }
        let mut alterations = vec![
            rename,
            ColumnAlterationSpec {
                path: "number".to_owned(),
                rename: Some("numeric".to_owned()),
                nullable: Some(false),
                // Native Lance permits integer-to-integer casts,
                // not integer-to-float schema evolution.
                cast_to: Some(FieldType::I64),
            },
        ];
        if self.mode.is_lance() {
            let before = self.snapshot().await;
            Self::unsupported(
                self.database
                    .catalog(CatalogRequest::AlterCollection(
                        SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                            locator: self.locator.clone(),
                            alterations: alterations.clone(),
                        }),
                    ))
                    .await,
            );
            assert_eq!(
                self.snapshot().await,
                before,
                "rejected combined cast cannot apply the earlier rename"
            );
            alterations[1].nullable = None;
        }
        self.database
            .catalog(CatalogRequest::AlterCollection(
                SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                    locator: self.locator.clone(),
                    alterations,
                }),
            ))
            .await
            .expect("rename and cast columns");
        if self.mode.is_lance() {
            self.database
                .catalog(CatalogRequest::AlterCollection(
                    SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                        locator: self.locator.clone(),
                        alterations: vec![ColumnAlterationSpec {
                            path: "numeric".to_owned(),
                            rename: None,
                            nullable: Some(false),
                            cast_to: None,
                        }],
                    }),
                ))
                .await
                .expect("tighten nullability after the completed cast");
            let CatalogResponse::Collection(info) = self.snapshot().await.1 else {
                panic!("open returns the altered collection schema");
            };
            let schema = info.schema.expect("native schema is available");
            let field = schema
                .fields
                .iter()
                .find(|field| field.name == "numeric")
                .unwrap();
            assert_eq!(field.field_type, FieldType::I64);
            assert!(!field.nullable, "standalone tightening is persisted");
        }
        for row in self.rows().await {
            assert!(!row.fields.contains_key("status"));
            assert!(!row.fields.contains_key("number"));
            assert_eq!(row.fields["state"], "original");
            assert!(row.fields["numeric"].as_i64().is_some());
        }
        self.database
            .catalog(CatalogRequest::AlterCollection(
                SchemaEvolutionRequest::DropColumns(DropColumnsRequest {
                    locator: self.locator.clone(),
                    columns: vec!["extra".to_owned()],
                }),
            ))
            .await
            .expect("drop computed column");
        assert!(
            self.rows()
                .await
                .iter()
                .all(|row| !row.fields.contains_key("extra")),
            "dropping a computed column removes its stored values as well as its definition"
        );
    }

    async fn cast_already_required(&self) {
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(CollectionSchema {
                    fields: [("marker", FieldType::String), ("number", FieldType::U64)]
                        .into_iter()
                        .map(|(name, field_type)| FieldSchema {
                            name: name.to_owned(),
                            field_type,
                            nullable: false,
                            metadata: Default::default(),
                        })
                        .collect(),
                    metadata: Default::default(),
                }),
                records: Vec::new(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("declare required unsigned cast inputs");
        self.database
            .data(DataRequest::Insert(InsertRequest::append(
                self.locator.clone(),
                [0_u64, 2, 9]
                    .into_iter()
                    .enumerate()
                    .map(|(index, number)| {
                        DataRecord::without_id()
                            .json_field("marker", json!(format!("row-{index:04}")))
                            .json_field("number", json!(number))
                    })
                    .collect(),
            )))
            .await
            .expect("seed unsigned inputs");
        self.scalar_marker_index().await;
        let before = self.snapshot().await;
        let CatalogResponse::Collection(info) = &before.1 else {
            panic!("open returns the collection schema");
        };
        let field = info
            .schema
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .find(|field| field.name == "number")
            .unwrap();
        assert!(!field.nullable);
        assert_eq!(field.field_type, FieldType::U64);
        self.database
            .catalog(CatalogRequest::AlterCollection(
                SchemaEvolutionRequest::AlterColumns(AlterColumnsRequest {
                    locator: self.locator.clone(),
                    alterations: vec![ColumnAlterationSpec {
                        path: "number".to_owned(),
                        rename: Some("numeric".to_owned()),
                        nullable: Some(false),
                        cast_to: Some(FieldType::I64),
                    }],
                }),
            ))
            .await
            .expect("cast and keep an already-required field required");
        let after = self.snapshot().await;
        let mut expected = before.0;
        for record in &mut expected {
            let value = record.fields.remove("number").unwrap();
            record.fields.insert("numeric".to_owned(), value);
        }
        assert_eq!(after.0, expected);
        let mut catalog = before.1;
        let CatalogResponse::Collection(info) = &mut catalog else {
            panic!("open returns the collection schema");
        };
        let field = info
            .schema
            .as_mut()
            .unwrap()
            .fields
            .iter_mut()
            .find(|field| field.name == "number")
            .unwrap();
        field.name = "numeric".to_owned();
        field.field_type = FieldType::I64;
        assert_eq!(after.1, catalog);
        assert_eq!(after.2, before.2, "unrelated index inventory is preserved");
    }

    async fn scalar_marker_index(&self) {
        self.database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["marker".to_owned()],
                kind: IndexKind::Scalar,
                wait_timeout_ms: Some(5_000),
            }))
            .await
            .expect("create real scalar index for mutation preservation proof");
    }

    async fn assert_count(&self, expected: u64) {
        assert_eq!(
            self.database
                .data(DataRequest::Count(CountRequest {
                    locator: self.locator.clone(),
                    filter: None,
                }))
                .await
                .expect("exact public count"),
            DataResponse::Count(expected)
        );
    }

    async fn seed_scan_deleted_rows(&self) {
        self.seed(8).await;
        for start in [8, 16, 24] {
            let DataResponse::Mutation(counts) = self
                .database
                .data(DataRequest::Insert(InsertRequest::append(
                    self.locator.clone(),
                    (start..start + 8)
                        .map(|number| self.record(number))
                        .collect(),
                )))
                .await
                .expect("append a separate scan fixture group")
            else {
                panic!("append returns mutation counts")
            };
            for (actual, expected) in [
                (counts.inserted, 8),
                (counts.updated, 0),
                (counts.deleted, 0),
                (counts.upserted, 0),
                (counts.matched, 0),
            ] {
                if let Some(actual) = actual {
                    assert_eq!(actual, expected, "reported append count is exact");
                    assert!(counts.counts_exact);
                }
            }
        }
        self.assert_count(32).await;
        let DataResponse::Mutation(counts) = self
            .database
            .data(DataRequest::Delete(DeleteRequest {
                locator: self.locator.clone(),
                filter: FilterExpr::In {
                    field: "number".to_owned(),
                    values: [
                        0, 3, 7, 8, 10, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 28, 31,
                    ]
                    .into_iter()
                    .map(|number| json!(number))
                    .collect(),
                },
            }))
            .await
            .expect("delete holes and one complete append group")
        else {
            panic!("delete returns mutation counts")
        };
        assert_eq!(counts.deleted, Some(17));
        assert!(counts.counts_exact);
        for (actual, expected) in [
            (counts.inserted, 0),
            (counts.updated, 0),
            (counts.upserted, 0),
            (counts.matched, 17),
        ] {
            if let Some(actual) = actual {
                assert_eq!(actual, expected, "reported delete count is exact");
            }
        }
    }

    async fn scan_reference(
        &self,
        request: ScanRequest,
        expected: &[DataRecord],
    ) -> Vec<DataRecord> {
        let DataResponse::Records(page) = self
            .database
            .data(DataRequest::Scan(request))
            .await
            .expect("same-state scan reference")
        else {
            panic!("scan returns records")
        };
        assert_eq!(page.records.len(), expected.len());
        assert_eq!(Self::markers(&page.records).len(), expected.len());
        let mut sorted = page.records.clone();
        sorted.sort_by_key(|row| row.fields["marker"].as_str().unwrap().to_owned());
        assert_eq!(sorted, expected, "independent complete survivor oracle");
        // Only this unchanged database state's observed sequence is an ordering
        // oracle. Neither insertion order nor order across reopen is required.
        page.records
    }

    async fn assert_scan_pages(&self, mut request: ScanRequest, reference: &[DataRecord]) {
        let limit = request.limit.expect("bounded scan page size");
        let initial_offset = request.offset.unwrap_or(0);
        let expected = &reference[initial_offset.min(reference.len())..];
        let mut observed = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..16 {
            let DataResponse::Records(page) = self
                .database
                .data(DataRequest::Scan(request.clone()))
                .await
                .expect("deleted-row scan continuation")
            else {
                panic!("scan returns records")
            };
            let end = (observed.len() + limit).min(expected.len());
            assert_eq!(page.records, expected[observed.len()..end]);
            if page.records.is_empty() {
                assert!(
                    page.next_page.is_none(),
                    "empty final page terminates traversal"
                );
            }
            for row in &page.records {
                assert!(
                    seen.insert(row.fields["marker"].as_str().unwrap().to_owned()),
                    "scan pagination cannot duplicate a surviving marker"
                );
            }
            observed.extend(page.records);
            request.page_token = page.next_page;
            // Keep the initial numeric offset: a returned token takes precedence.
            if request.page_token.is_none() {
                assert_eq!(observed, expected, "complete exact surviving suffix");
                return;
            }
            // A full final page may yield one empty terminal page.
        }
        panic!("deleted-row scan traversal exceeded 16 requests");
    }

    async fn assert_scan_deleted_rows(&self) {
        let expected = [1, 2, 4, 5, 6, 9, 11, 12, 13, 14, 25, 26, 27, 29, 30]
            .into_iter()
            .map(|number| self.record(number))
            .collect::<Vec<_>>();
        self.assert_count(15).await;
        let reference = self
            .scan_reference(ScanRequest::new(self.locator.clone()), &expected)
            .await;
        for offset in [0, 4, 5, 9, 10, 14, 15, 40] {
            let DataResponse::Records(page) = self
                .database
                .data(DataRequest::Scan(ScanRequest {
                    offset: Some(offset),
                    ..ScanRequest::new(self.locator.clone()).limit(4)
                }))
                .await
                .expect("bounded scan across deleted rows")
            else {
                panic!("scan returns records")
            };
            let start = offset.min(reference.len());
            let end = (start + 4).min(reference.len());
            assert_eq!(page.records.len(), end - start, "offset {offset}");
            assert_eq!(page.records, reference[start..end], "offset {offset}");
        }
        for limit in [4, 5] {
            for offset in [0, 2] {
                self.assert_scan_pages(
                    ScanRequest {
                        offset: Some(offset),
                        ..ScanRequest::new(self.locator.clone()).limit(limit)
                    },
                    &reference,
                )
                .await;
            }
        }
        let projected = reference
            .iter()
            .map(|row| {
                DataRecord::without_id()
                    .json_field("marker", row.fields["marker"].clone())
                    .json_field("number", row.fields["number"].clone())
            })
            .collect::<Vec<_>>();
        self.assert_scan_pages(
            ScanRequest::new(self.locator.clone())
                .projection(Projection::Columns(vec![
                    "marker".to_owned(),
                    "number".to_owned(),
                ]))
                .limit(4),
            &projected,
        )
        .await;
        let filtered_expected = expected
            .into_iter()
            .filter(|row| row.fields["number"].as_u64().unwrap() >= 10)
            .collect::<Vec<_>>();
        let filtered = ScanRequest::new(self.locator.clone()).filter(FilterExpr::Compare {
            field: "number".to_owned(),
            op: ComparisonOp::Gte,
            value: json!(10),
        });
        let filtered_reference = self
            .scan_reference(filtered.clone(), &filtered_expected)
            .await;
        self.assert_scan_pages(filtered.limit(4), &filtered_reference)
            .await;
    }

    fn sql_backfill_request(&self, read_columns: Option<Vec<String>>) -> CatalogRequest {
        CatalogRequest::AlterCollection(SchemaEvolutionRequest::AddColumns(AddColumnsRequest {
            locator: self.locator.clone(),
            expressions: vec![ComputedColumn {
                name: "derived".to_owned(),
                expression_sql: "number * 2 + 1".to_owned(),
            }],
            read_columns,
        }))
    }

    async fn seed_sql_backfill(&self) {
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(CollectionSchema {
                    fields: [("marker", FieldType::String), ("number", FieldType::I64)]
                        .into_iter()
                        .map(|(name, field_type)| FieldSchema {
                            name: name.to_owned(),
                            field_type,
                            nullable: false,
                            metadata: Default::default(),
                        })
                        .collect(),
                    metadata: Default::default(),
                }),
                records: Vec::new(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("declare signed inputs for SQL backfill");
        self.database
            .data(DataRequest::Insert(InsertRequest::append(
                self.locator.clone(),
                [-3_i64, 0, 2, 9]
                    .into_iter()
                    .enumerate()
                    .map(|(index, number)| {
                        DataRecord::without_id()
                            .json_field("marker", json!(format!("row-{index:04}")))
                            .json_field("number", json!(number))
                    })
                    .collect(),
            )))
            .await
            .expect("seed distinct signed SQL inputs");
    }

    async fn reject_sql_read_columns(&self) {
        let before = self.snapshot().await;
        self.assert_count(before.0.len() as u64).await;
        for read_columns in [vec!["number".to_owned()], Vec::new()] {
            let result = self
                .database
                .catalog(self.sql_backfill_request(Some(read_columns)))
                .await;
            assert_eq!(
                self.snapshot().await,
                before,
                "rejection preserves rows, schema and indexes"
            );
            self.assert_count(before.0.len() as u64).await;
            assert!(
                matches!(&result, Err(StorageError::UnsupportedCapability { capability, .. })
                    if capability == "SCHEMA_EVOLUTION_READ_COLUMNS"),
                "SQL source-column options must be rejected before mutation: {result:?}"
            );
        }
    }

    async fn assert_sql_backfill(&self) {
        let before = self.snapshot().await;
        assert_eq!(
            self.database
                .catalog(self.sql_backfill_request(None))
                .await
                .expect("eager SQL backfill completes"),
            CatalogResponse::Completed
        );
        // The very first read after completion must contain the stored values.
        let after = self.snapshot().await;
        let expected = before
            .0
            .iter()
            .map(|row| {
                let number = row.fields["number"].as_i64().expect("seeded integer");
                row.clone().json_field("derived", json!(number * 2 + 1))
            })
            .collect::<Vec<_>>();
        assert_eq!(after.0, expected, "backfill changes only the derived field");
        self.assert_count(before.0.len() as u64).await;
        let (CatalogResponse::Collection(old), CatalogResponse::Collection(new)) =
            (&before.1, &after.1)
        else {
            panic!("catalog returns collection schema")
        };
        let mut expected_schema = old.schema.clone().expect("original schema");
        let schema = new.schema.as_ref().expect("backfilled schema");
        let derived = schema
            .fields
            .iter()
            .find(|field| field.name == "derived")
            .expect("committed derived field");
        assert_eq!(derived.field_type, FieldType::I64);
        expected_schema.fields.push(derived.clone());
        assert_eq!(
            schema, &expected_schema,
            "all original schema fields remain unchanged"
        );
        assert_eq!(after.2, before.2, "backfill preserves indexes");
    }

    async fn sql_read_columns_rejection(&self) {
        self.seed_sql_backfill().await;
        self.scalar_marker_index().await;
        self.reject_sql_read_columns().await;
        self.assert_sql_backfill().await;
    }

    async fn sql_backfill(&self) {
        self.seed_sql_backfill().await;
        self.assert_sql_backfill().await;
    }

    async fn quoted_filters(&self, field: &str) {
        let fields = [field];
        let literals = [
            "O'Brien",
            "a \"quoted\" value",
            "`tick`",
            r"back\slash",
            "x' OR TRUE --",
        ];
        let mut schema = CollectionSchema {
            fields: ["marker", "status"]
                .into_iter()
                .chain(fields)
                .map(|name| FieldSchema {
                    name: name.to_owned(),
                    field_type: FieldType::String,
                    nullable: true,
                    metadata: Default::default(),
                })
                .collect(),
            metadata: Default::default(),
        };
        schema
            .fields
            .sort_by(|left, right| left.name.cmp(&right.name));
        let mut expected = Vec::new();
        for (field_number, field) in fields.iter().enumerate() {
            for (value_number, value) in literals.iter().enumerate() {
                let mut row = DataRecord::without_id()
                    .json_field(
                        "marker",
                        json!(format!("row-{field_number:02}-{value_number:02}")),
                    )
                    .json_field("status", json!("original"));
                for name in fields {
                    row = row
                        .json_field(name, json!(if name == *field { *value } else { "control" }));
                }
                expected.push(row);
            }
        }
        let mut control = DataRecord::without_id()
            .json_field("marker", json!("row-control"))
            .json_field("status", json!("original"));
        for field in fields {
            control = control.json_field(field, json!("control"));
        }
        expected.push(control);
        // Use the public populated-table path and verify its complete schema.
        let CatalogResponse::Collection(created) = self
            .database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(schema.clone()),
                records: expected.clone(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("create exact String literals")
        else {
            panic!("created collection")
        };
        assert_eq!(
            created.schema,
            Some(schema),
            "quoted fields retain declared String types"
        );
        assert_eq!(self.rows().await, expected);
        let before = self.snapshot().await;
        for field in fields {
            for value in literals {
                let filter = FilterExpr::eq(field, value);
                let selected = expected
                    .iter()
                    .position(|row| row.fields[field] == value)
                    .expect("one matching seed row");
                let DataResponse::Records(page) = self
                    .database
                    .data(DataRequest::Scan(
                        ScanRequest::new(self.locator.clone()).filter(filter.clone()),
                    ))
                    .await
                    .expect("quoted identifier and literal scan")
                else {
                    panic!("scan records")
                };
                assert_eq!(page.records, vec![expected[selected].clone()]);
                assert!(page.next_page.is_none());
                assert_eq!(
                    self.database
                        .data(DataRequest::Count(CountRequest {
                            locator: self.locator.clone(),
                            filter: Some(filter.clone()),
                        }))
                        .await
                        .expect("quoted filter count"),
                    DataResponse::Count(1)
                );
                let DataResponse::Mutation(updated) = self
                    .database
                    .data(DataRequest::Update(UpdateRequest {
                        locator: self.locator.clone(),
                        filter: Some(filter.clone()),
                        assignments: vec![UpdateAssignment {
                            column: "status".to_owned(),
                            value: json!("selected"),
                        }],
                    }))
                    .await
                    .expect("quoted filter update")
                else {
                    panic!("update counts")
                };
                assert_eq!(updated.updated, Some(1));
                assert!(updated.counts_exact);
                expected[selected]
                    .fields
                    .insert("status".to_owned(), json!("selected"));
                assert_eq!(
                    self.rows().await,
                    expected,
                    "update preserves every nonmatching field and row"
                );
                let DataResponse::Mutation(deleted) = self
                    .database
                    .data(DataRequest::Delete(DeleteRequest {
                        locator: self.locator.clone(),
                        filter,
                    }))
                    .await
                    .expect("quoted filter delete")
                else {
                    panic!("delete counts")
                };
                assert_eq!(deleted.deleted, Some(1));
                assert!(deleted.counts_exact);
                expected.remove(selected);
                assert_eq!(
                    self.rows().await,
                    expected,
                    "delete preserves every control row"
                );
                let after = self.snapshot().await;
                let (CatalogResponse::Collection(old), CatalogResponse::Collection(new)) =
                    (&before.1, &after.1)
                else {
                    panic!("collection schema")
                };
                assert_eq!(new.schema, old.schema);
                self.assert_count(expected.len() as u64).await;
            }
        }
    }

    async fn partition_exact_counts(&self) {
        let initial = (0..3)
            .map(|number| {
                self.record(number).json_field(
                    "partition",
                    json!(if number < 2 { "O'Brien" } else { "outside" }),
                )
            })
            .collect::<Vec<_>>();
        self.create(initial.clone(), CreateCollectionMode::Create)
            .await;
        self.scalar_marker_index().await;
        let before = self.snapshot().await;
        let updated = initial[0].clone().json_field("status", json!("replaced"));
        let inserted = self.record(9).json_field("partition", json!("O'Brien"));
        let request = ReplacePartitionRequest::new(
            self.locator.clone(),
            FilterExpr::eq("partition", "O'Brien"),
            "marker",
            vec![updated.clone(), inserted.clone()],
        )
        .expect("bounded replacement with matched, missing and new identities");
        let DataResponse::Mutation(counts) = self
            .database
            .data(DataRequest::ReplacePartition(request.clone()))
            .await
            .expect("atomic native merge replacement")
        else {
            panic!("merge counts")
        };
        assert_eq!(counts.inserted, Some(1));
        assert_eq!(counts.updated, Some(1));
        assert_eq!(counts.deleted, Some(1));
        assert_eq!(counts.upserted, Some(2));
        assert_eq!(counts.matched, Some(1));
        assert!(counts.counts_exact);
        let after = self.snapshot().await;
        assert_eq!(after.0, vec![updated, initial[2].clone(), inserted]);
        assert_eq!(after.1, before.1, "replacement preserves schema and count");
        assert_eq!(after.2, before.2, "replacement preserves index inventory");
        let DataResponse::Mutation(cleared) = self
            .database
            .data(DataRequest::ReplacePartition(ReplacePartitionRequest {
                records: Vec::new(),
                ..request
            }))
            .await
            .expect("empty replacement clears only selected partition")
        else {
            panic!("delete counts")
        };
        assert_eq!(cleared.inserted, Some(0));
        assert_eq!(cleared.updated, Some(0));
        assert_eq!(cleared.deleted, Some(2));
        assert!(cleared.counts_exact);
        let cleared = self.snapshot().await;
        assert_eq!(cleared.0, vec![initial[2].clone()]);
        let (CatalogResponse::Collection(old), CatalogResponse::Collection(new)) =
            (&before.1, &cleared.1)
        else {
            panic!("collection schema")
        };
        assert_eq!(new.schema, old.schema);
        assert_eq!(cleared.2, before.2);
        self.assert_count(1).await;
    }

    async fn partition_native_failure(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        let rejected = self
            .record(9)
            .json_field("partition", json!("a"))
            .json_field("number", json!("incompatible with existing integer column"));
        let result = self
            .database
            .data(DataRequest::ReplacePartition(
                ReplacePartitionRequest::new(
                    self.locator.clone(),
                    FilterExpr::eq("partition", "a"),
                    "marker",
                    vec![rejected],
                )
                .unwrap(),
            ))
            .await;
        assert!(result.is_err(), "native Arrow schema mismatch is an error");
        assert_eq!(
            self.snapshot().await,
            before,
            "native failed replacement leaves prior partition intact"
        );
        self.database
            .data(DataRequest::ReplacePartition(
                ReplacePartitionRequest::new(
                    self.locator.clone(),
                    FilterExpr::eq("partition", "a"),
                    "marker",
                    vec![self.record(0).json_field("status", json!("after-error"))],
                )
                .unwrap(),
            ))
            .await
            .expect("the owned mutation worker remains usable after a native error");
        let rows = self.rows().await;
        assert_eq!(
            Self::markers(&rows),
            BTreeSet::from(["row-0000".to_owned(), "row-0002".to_owned()])
        );
        assert_eq!(rows[0].fields["status"], "after-error");
    }

    fn scalar_string_schema(json_column: bool) -> CollectionSchema {
        CollectionSchema {
            fields: [
                ("marker", FieldType::String),
                (
                    "mixed",
                    if json_column {
                        FieldType::Json
                    } else {
                        FieldType::String
                    },
                ),
            ]
            .into_iter()
            .map(|(name, field_type)| FieldSchema {
                name: name.to_owned(),
                field_type,
                nullable: true,
                metadata: Default::default(),
            })
            .collect(),
            metadata: Default::default(),
        }
    }

    async fn scalar_string_predicate(&self, json_column: bool, predicate: ScalarStringPredicate) {
        let schema = Self::scalar_string_schema(json_column);
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(schema),
                records: Vec::new(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("create declared scalar column for predicate proof");
        let records = ["alpha", "zeta"]
            .into_iter()
            .enumerate()
            .map(|(number, value)| {
                DataRecord::without_id()
                    .json_field("marker", json!(format!("row-{number:04}")))
                    .json_field("mixed", json!(value))
            })
            .collect::<Vec<_>>();
        self.database
            .data(DataRequest::Insert(InsertRequest::append(
                self.locator.clone(),
                records.clone(),
            )))
            .await
            .expect("seed native scalar predicate values");
        assert_eq!(
            self.rows().await,
            records,
            "predicate setup round-trips exact strings"
        );
        self.database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["marker".to_owned()],
                kind: IndexKind::Scalar,
                wait_timeout_ms: Some(5_000),
            }))
            .await
            .expect("create scalar index before predicate rejection");
        let before = self.snapshot().await;
        let result = self
            .database
            .data(DataRequest::Scan(ScanRequest {
                filter: Some(predicate.filter()),
                ..ScanRequest::new(self.locator.clone())
            }))
            .await;
        assert_eq!(
            self.snapshot().await,
            before,
            "query preserves declared schema and stored values"
        );
        if json_column {
            // The optional filter contract does not promise these operations on
            // Json fields. Exact Unsupported is required; a successful query
            // over encoded UTF8 bytes is not a portable logical comparison.
            let mut failures = Vec::new();
            if !matches!(result, Err(StorageError::UnsupportedCapability { .. })) {
                failures.push(format!("Json scan must reject this predicate: {result:?}"));
            }
            for request in [
                DataRequest::Update(UpdateRequest {
                    locator: self.locator.clone(),
                    filter: Some(predicate.filter()),
                    assignments: vec![UpdateAssignment {
                        column: "marker".to_owned(),
                        value: json!("unexpected mutation"),
                    }],
                }),
                DataRequest::Delete(DeleteRequest {
                    locator: self.locator.clone(),
                    filter: predicate.filter(),
                }),
            ] {
                let result = self.database.data(request).await;
                if !matches!(result, Err(StorageError::UnsupportedCapability { .. })) {
                    failures.push(format!(
                        "Json mutation must reject this predicate: {result:?}"
                    ));
                }
                if self.snapshot().await != before {
                    failures.push("rejected predicate changed rows, schema, or indexes".to_owned());
                }
            }
            assert!(failures.is_empty(), "{}", failures.join("\n"));
        } else {
            let DataResponse::Records(page) = result.expect("ordinary String predicate") else {
                panic!("scan records")
            };
            assert_eq!(
                page.records,
                vec![records[predicate.expected()].clone()],
                "ordinary String predicates retain exact native semantics"
            );
        }
    }

    async fn scalar_string_update(&self, json_column: bool) {
        let schema = Self::scalar_string_schema(json_column);
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(schema.clone()),
                records: Vec::new(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("create declared scalar column before updates");
        let mut record = DataRecord::without_id()
            .json_field("marker", json!("row-0000"))
            .json_field("mixed", json!("seed"));
        self.database
            .data(DataRequest::Insert(InsertRequest::append(
                self.locator.clone(),
                vec![record.clone()],
            )))
            .await
            .expect("seed declared scalar column through public append");
        assert_eq!(
            self.rows().await,
            vec![record.clone()],
            "update setup round-trips"
        );
        for value in ["not JSON syntax", "123"] {
            self.database
                .data(DataRequest::Update(UpdateRequest {
                    locator: self.locator.clone(),
                    filter: Some(FilterExpr::eq("marker", "row-0000")),
                    assignments: vec![UpdateAssignment {
                        column: "mixed".to_owned(),
                        value: json!(value),
                    }],
                }))
                .await
                .expect("public scalar assignment completes");
            let CatalogResponse::Collection(info) = self
                .database
                .catalog(CatalogRequest::OpenCollection {
                    locator: self.locator.clone(),
                })
                .await
                .expect("read declared schema after update")
            else {
                panic!("collection response")
            };
            assert_eq!(
                info.schema,
                Some(schema.clone()),
                "update preserves declared type"
            );
            record.fields.insert("mixed".to_owned(), json!(value));
            assert_eq!(
                self.rows().await,
                vec![record.clone()],
                "assignment preserves portable scalar string"
            );
        }
    }

    async fn scalar_string_append(&self, json_column: bool) {
        let schema = Self::scalar_string_schema(json_column);
        self.database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: Some(schema.clone()),
                records: Vec::new(),
                mode: CreateCollectionMode::Create,
            }))
            .await
            .expect("create declared scalar column before independent writes");
        let mut expected = Vec::new();
        for (number, value) in ["not JSON syntax", "123"].into_iter().enumerate() {
            let record = DataRecord::without_id()
                .json_field("marker", json!(format!("row-{number:04}")))
                .json_field("mixed", json!(value));
            self.database
                .data(DataRequest::Insert(InsertRequest::append(
                    self.locator.clone(),
                    vec![record.clone()],
                )))
                .await
                .expect("append a scalar-only batch using the declared column semantics");
            let CatalogResponse::Collection(info) = self
                .database
                .catalog(CatalogRequest::OpenCollection {
                    locator: self.locator.clone(),
                })
                .await
                .expect("read declared schema after append")
            else {
                panic!("collection response")
            };
            assert_eq!(
                info.schema,
                Some(schema.clone()),
                "append preserves declared types"
            );
            expected.push(record);
            assert_eq!(
                self.rows().await,
                expected,
                "each independent append preserves portable strings"
            );
        }
    }

    async fn scalar_string_equality(&self, json_column: bool) {
        let other = if json_column {
            json!({"nested": true})
        } else {
            json!("other")
        };
        self.create(
            vec![
                self.record(0).json_field("mixed", json!("123")),
                self.record(1).json_field("mixed", other),
            ],
            CreateCollectionMode::Create,
        )
        .await;
        assert_eq!(
            self.rows().await[0].fields["mixed"],
            json!("123"),
            "scalar string setup round-trips"
        );
        let DataResponse::Records(page) = self
            .database
            .data(DataRequest::Scan(ScanRequest {
                filter: Some(FilterExpr::eq("mixed", "123")),
                ..ScanRequest::new(self.locator.clone())
            }))
            .await
            .expect("portable scalar string equality scan")
        else {
            panic!("scan records")
        };
        assert_eq!(
            page.records.len(),
            1,
            "equality compares the portable scalar string, regardless of physical JSON encoding"
        );
        assert_eq!(page.records[0].fields["marker"], "row-0000");
        assert_eq!(page.records[0].fields["mixed"], "123");
    }

    async fn json_round_trip(&self, explicit_schema: bool) {
        let values = [
            json!("not JSON syntax"),
            serde_json::Value::Null,
            json!({"nested": true}),
            json!("123"),
            json!("null"),
            json!(["value", 7]),
        ];
        let mut records = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let number = u64::try_from(index).expect("small fixture index");
                let record = if explicit_schema {
                    DataRecord::without_id().json_field("marker", json!(format!("row-{number:04}")))
                } else {
                    self.record(number)
                };
                record.json_field("mixed", value.clone())
            })
            .collect::<Vec<_>>();
        if explicit_schema {
            self.database
                .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                    locator: self.locator.clone(),
                    schema: Some(CollectionSchema {
                        fields: [("marker", FieldType::String), ("mixed", FieldType::Json)]
                            .into_iter()
                            .map(|(name, field_type)| FieldSchema {
                                name: name.to_owned(),
                                field_type,
                                nullable: true,
                                metadata: Default::default(),
                            })
                            .collect(),
                        metadata: Default::default(),
                    }),
                    records: Vec::new(),
                    mode: CreateCollectionMode::Create,
                }))
                .await
                .expect("create explicitly declared JSON column");
            records.push(DataRecord::without_id().json_field("marker", json!("row-0006")));
            self.database
                .data(DataRequest::Insert(InsertRequest::append(
                    self.locator.clone(),
                    records,
                )))
                .await
                .expect("append explicit JSON values and an absent field");
        } else {
            self.create(records, CreateCollectionMode::Create).await;
        }
        let rows = self.rows().await;
        assert_eq!(rows.len(), values.len() + usize::from(explicit_schema));
        if explicit_schema {
            assert!(
                !rows.last().unwrap().fields.contains_key("mixed"),
                "absent Arrow value remains absent, distinct from explicit JSON null"
            );
        }
        for (row, expected) in rows.iter().zip(values) {
            assert_eq!(
                row.fields.get("mixed"),
                Some(&expected),
                "a JSON column preserves scalar strings, nulls, arrays and objects"
            );
        }
    }

    async fn partition_collision(&self, outside_is_null: bool) {
        self.seed(3).await;
        if outside_is_null {
            self.database
                .data(DataRequest::Update(UpdateRequest {
                    locator: self.locator.clone(),
                    filter: Some(FilterExpr::eq("marker", "row-0002")),
                    assignments: vec![UpdateAssignment {
                        column: "partition".to_owned(),
                        value: serde_json::Value::Null,
                    }],
                }))
                .await
                .expect("set the outside row's existing nullable column to null");
            let rows = self.rows().await;
            let outside = rows
                .iter()
                .find(|row| row.fields["marker"] == "row-0002")
                .expect("outside row remains present");
            assert!(
                outside
                    .fields
                    .get("partition")
                    .is_none_or(serde_json::Value::is_null),
                "native nullable scalar must no longer retain its old string value"
            );
        }
        let before = self.snapshot().await;
        // ReplacePartition's identity_field is a globally unique ordinary
        // payload field, not DataRecord.id (which this request forbids).
        let mut collision = self.record(2).json_field("partition", json!("a"));
        collision.id = None;
        let request = ReplacePartitionRequest::new(
            self.locator.clone(),
            FilterExpr::eq("partition", "a"),
            "marker",
            vec![collision],
        )
        .expect("structurally valid bounded partition request");
        let result = self
            .database
            .data(DataRequest::ReplacePartition(request))
            .await;
        if self.mode.is_qdrant() {
            Self::unsupported(result);
        } else {
            assert!(
                result.is_err(),
                "bounded replacement cannot move or duplicate an outside partition's global identity"
            );
        }
        assert_eq!(
            self.snapshot().await,
            before,
            "rejected cross-partition identity collision must preserve both partitions"
        );
    }

    fn vector_query(&self, limit: usize) -> ScopedQuery<VectorQueryOptions> {
        ScopedQuery::vector(self.locator.clone())
            .vector_field("embedding")
            .params(VectorSearchParams {
                limit,
                distance: if self.mode.is_qdrant() {
                    None
                } else {
                    Some(DistanceMetric::L2)
                },
                ..VectorSearchParams::default()
            })
    }

    async fn nearest(&self) {
        self.seed(4).await;
        let before = self.snapshot().await;
        let response = self
            .database
            .query(QueryRequest::Nearest(NearestRequest {
                query: self.vector_query(2),
                vector: Self::vector(0),
            }))
            .await
            .expect("real nearest query");
        assert_eq!(response.hits.len(), 2);
        assert_eq!(response.hits[0].record.fields["marker"], "row-0000");
        assert_eq!(response.hits[1].record.fields["marker"], "row-0001");
        if let Some(score) = response.hits[0].raw_score {
            let expected = if self.mode.is_qdrant() {
                assert_eq!(response.hits[0].score_kind, ScoreKind::Similarity);
                1.0
            } else {
                assert_eq!(response.hits[0].score_kind, ScoreKind::Distance);
                0.0
            };
            assert!(
                (score - expected).abs() < 0.0001,
                "exact-vector score is truthful"
            );
        }
        let response = self
            .database
            .query(QueryRequest::Nearest(NearestRequest {
                query: self
                    .vector_query(4)
                    .filter(FilterExpr::eq("partition", "b"))
                    .projection(Projection::Columns(vec!["marker".to_owned()])),
                vector: Self::vector(2),
            }))
            .await
            .expect("filtered projected nearest");
        assert_eq!(response.hits.len(), 2);
        assert_eq!(
            Self::markers(
                &response
                    .hits
                    .iter()
                    .map(|hit| hit.record.clone())
                    .collect::<Vec<_>>()
            ),
            BTreeSet::from(["row-0002".to_owned(), "row-0003".to_owned()]),
            "filtered nearest returns both distinct matching records"
        );
        for hit in response.hits {
            Self::assert_projected(&hit.record);
            assert!(
                ["row-0002", "row-0003"].contains(&hit.record.fields["marker"].as_str().unwrap())
            );
        }
        let mut unsupported = self.vector_query(2);
        if self.mode.is_surreal() {
            unsupported.options.params.fast_search = true;
            Self::unsupported(
                self.database
                    .query(QueryRequest::Nearest(NearestRequest {
                        query: unsupported,
                        vector: Self::vector(0),
                    }))
                    .await,
            );
        } else if self.mode.is_qdrant() {
            unsupported.options.params.distance = Some(DistanceMetric::L2);
            Self::unsupported(
                self.database
                    .query(QueryRequest::Nearest(NearestRequest {
                        query: unsupported,
                        vector: Self::vector(0),
                    }))
                    .await,
            );
        }
        if !self.mode.is_surreal() {
            assert!(
                self.database
                    .query(QueryRequest::Nearest(NearestRequest {
                        query: self.vector_query(2),
                        vector: DenseVector::F32(vec![1.0, 2.0]),
                    }))
                    .await
                    .is_err(),
                "fixed-schema vector dimensionality must be enforced"
            );
        }
        assert_eq!(self.snapshot().await, before);
    }

    #[cfg(feature = "surrealdb")]
    async fn nearest_validation_order(&self) {
        self.seed(1).await;
        let before = self.snapshot().await;
        let mut failures = Vec::new();
        for (label, params, expected_capability) in [
            (
                "empty projection with missing indexed ef",
                VectorSearchParams {
                    limit: 2,
                    ..VectorSearchParams::default()
                },
                "EMPTY_PROJECTION",
            ),
            (
                "empty projection with incompatible distance and ef",
                VectorSearchParams {
                    limit: 2,
                    distance: Some(DistanceMetric::L2),
                    ef: Some(32),
                    ..VectorSearchParams::default()
                },
                "EMPTY_PROJECTION",
            ),
            (
                "unsupported fast search with empty projection",
                VectorSearchParams {
                    limit: 2,
                    fast_search: true,
                    ..VectorSearchParams::default()
                },
                "VECTOR_SEARCH_PARAMS",
            ),
        ] {
            let result = self
                .database
                .query(QueryRequest::Nearest(NearestRequest {
                    query: self
                        .vector_query(2)
                        .params(params)
                        .projection(Projection::Columns(Vec::new())),
                    vector: Self::vector(0),
                }))
                .await;
            if !matches!(
                &result,
                Err(StorageError::UnsupportedCapability { capability, .. })
                    if capability == expected_capability
            ) {
                failures.push(format!(
                    "{label}: expected {expected_capability}, got {result:?}"
                ));
            }
        }
        assert_eq!(
            self.snapshot().await,
            before,
            "rejected nearest queries preserve data"
        );
        assert!(
            failures.is_empty(),
            "nearest validation changed the first public error:\n{}",
            failures.join("\n")
        );
    }

    async fn nearest_offset(&self) {
        #[cfg(feature = "qdrant")]
        if self.mode.is_qdrant() {
            self.qdrant_exact_offsets().await;
            return;
        }
        self.seed(4).await;
        let before = self.snapshot().await;
        let response = self
            .database
            .query(QueryRequest::Nearest(NearestRequest {
                query: self
                    .vector_query(3)
                    .map_options(|options| options.params.offset = Some(1)),
                vector: Self::vector(0),
            }))
            .await
            .expect("nearest offset");
        assert!(!response.hits.is_empty());
        assert!(response.hits.len() <= 3, "result honors the hit limit");
        assert_eq!(
            response.hits[0].record.fields["marker"], "row-0001",
            "offset skips the exact nearest record"
        );
        assert!(
            response
                .hits
                .iter()
                .all(|hit| hit.record.fields["marker"] != "row-0000")
        );
        assert_eq!(self.snapshot().await, before);
    }

    async fn search_by_id(&self) {
        self.seed(4).await;
        let before = self.snapshot().await;
        let result = self
            .database
            .query(QueryRequest::SearchById(SearchByIdRequest {
                query: self
                    .vector_query(4)
                    .filter(FilterExpr::Compare {
                        field: "marker".to_owned(),
                        op: ComparisonOp::Ne,
                        value: json!("row-0000"),
                    })
                    .projection(Projection::Columns(vec!["marker".to_owned()])),
                id: RecordId::U64(1),
            }))
            .await;
        if self.mode.is_lance() {
            Self::unsupported(result);
        } else {
            let response = result.expect("search from stored vector");
            assert_eq!(response.hits.len(), 3);
            assert_eq!(response.hits[0].record.fields["marker"], "row-0001");
            assert_eq!(
                Self::markers(
                    &response
                        .hits
                        .iter()
                        .map(|hit| hit.record.clone())
                        .collect::<Vec<_>>()
                ),
                BTreeSet::from([
                    "row-0001".to_owned(),
                    "row-0002".to_owned(),
                    "row-0003".to_owned()
                ]),
                "search-by-id honors the caller's filter with distinct ranked records"
            );
            for hit in response.hits {
                Self::assert_projected(&hit.record);
                assert_ne!(
                    hit.record.fields["marker"], "row-0000",
                    "explicit filter is preserved"
                );
            }
        }
        assert_eq!(self.snapshot().await, before);
    }

    async fn scroll(&self) {
        self.seed(5).await;
        let mut token = None;
        let mut observed = BTreeSet::new();
        let mut ended = false;
        for _ in 0..8 {
            let response = self
                .database
                .query(QueryRequest::Scroll(ScrollRequest {
                    locator: self.locator.clone(),
                    filter: Some(FilterExpr::eq("partition", "b")),
                    projection: Projection::Columns(vec!["marker".to_owned()]),
                    limit: Some(2),
                    offset: None,
                    page_token: token,
                }))
                .await
                .expect("real paginated scroll");
            assert!(response.hits.len() <= 2);
            for hit in response.hits {
                Self::assert_projected(&hit.record);
                assert!(
                    observed.insert(hit.record.fields["marker"].as_str().unwrap().to_owned()),
                    "scroll must not repeat records across pages"
                );
            }
            token = response.next_page;
            if token.is_none() {
                ended = true;
                break;
            }
        }
        assert!(
            ended,
            "scroll must terminate within fixture-sized page bound"
        );
        assert_eq!(
            observed,
            BTreeSet::from([
                "row-0002".to_owned(),
                "row-0003".to_owned(),
                "row-0004".to_owned(),
            ])
        );
    }

    async fn sample(&self) {
        self.seed(5).await;
        let before = self.snapshot().await;
        let result = self
            .database
            .query(QueryRequest::RandomSample(RandomSampleRequest {
                locator: self.locator.clone(),
                filter: Some(FilterExpr::eq("partition", "b")),
                projection: Projection::Columns(vec!["marker".to_owned()]),
                limit: 2,
            }))
            .await;
        if self.mode.is_lance() || self.mode.is_edge() {
            Self::unsupported(result);
        } else {
            let response = result.expect("real random sample");
            assert_eq!(response.hits.len(), 2);
            let mut markers = BTreeSet::new();
            for hit in response.hits {
                Self::assert_projected(&hit.record);
                let marker = hit.record.fields["marker"].as_str().unwrap().to_owned();
                assert!(["row-0002", "row-0003", "row-0004"].contains(&marker.as_str()));
                assert!(markers.insert(marker), "sample cannot repeat a record");
            }
        }
        assert_eq!(self.snapshot().await, before);
    }

    async fn index(&self, kind: IndexKind, column: &str) {
        self.seed(
            if self.mode.is_lance() && matches!(kind, IndexKind::Vector { .. }) {
                1024
            } else {
                4
            },
        )
        .await;
        let before = self.snapshot().await;
        let unsupported = self.mode.is_qdrant()
            && matches!(kind, IndexKind::Vector { .. } | IndexKind::Scalar)
            || self.mode.is_edge() && matches!(kind, IndexKind::FullText);
        let create = self
            .database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec![column.to_owned()],
                kind: kind.clone(),
                wait_timeout_ms: Some(30_000),
            }))
            .await;
        if unsupported {
            Self::unsupported(create);
            if self.mode.is_edge() && matches!(kind, IndexKind::Scalar) {
                Self::unsupported(
                    self.database
                        .index(IndexRequest::Drop {
                            locator: self.locator.clone(),
                            name: column.to_owned(),
                        })
                        .await,
                );
                assert!(
                    self.database
                        .index(IndexRequest::WaitFor(WaitForIndexRequest {
                            locator: self.locator.clone(),
                            index_names: vec![column.to_owned()],
                            timeout_ms: 100,
                        }))
                        .await
                        .is_err(),
                    "rejected index creation cannot leave a ready index"
                );
            }
            if matches!(kind, IndexKind::FullText) {
                Self::unsupported(
                    self.database
                        .query(QueryRequest::Text(TextSearchRequest {
                            query: ScopedQuery::text(self.locator.clone()).text_field("body"),
                            text: "quartz".to_owned(),
                        }))
                        .await,
                );
            }
            assert_eq!(self.snapshot().await, before);
            return;
        }
        assert_eq!(
            create.expect("create portable index"),
            IndexResponse::Completed
        );
        #[cfg(feature = "surrealdb")]
        if self.mode.is_surreal() && matches!(kind, IndexKind::FullText) {
            // Reusing one analyzer must not require a fresh database or a
            // caller-provided native prerequisite.
            self.database
                .index(IndexRequest::Create(CreateIndexRequest {
                    locator: self.locator.clone(),
                    columns: vec![column.to_owned()],
                    kind: IndexKind::FullText,
                    wait_timeout_ms: Some(30_000),
                }))
                .await
                .expect("repeat full-text creation preserves owned analyzer");
            if matches!(self.mode, Mode::SurrealHttp | Mode::SurrealWs) {
                let reopened = Database::connect(self.configuration.clone())
                    .await
                    .expect("fresh connection reuses persistent analyzer");
                reopened
                    .index(IndexRequest::Create(CreateIndexRequest {
                        locator: self.locator.clone(),
                        columns: vec![column.to_owned()],
                        kind: IndexKind::FullText,
                        wait_timeout_ms: Some(30_000),
                    }))
                    .await
                    .expect("reopened full-text creation");
            }
        }
        let IndexResponse::Indexes(indexes) = self
            .database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await
            .expect("list created index")
        else {
            panic!("index inventory")
        };
        let Some(IndexResponse::Indexes(original_indexes)) = &before.2 else {
            panic!("initial index inventory")
        };
        let created = indexes
            .iter()
            .filter(|index| {
                !original_indexes
                    .iter()
                    .any(|original| original.name == index.name)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            created.len(),
            1,
            "one created index appears in the public inventory"
        );
        let index = created[0];
        if !index.columns.is_empty() {
            assert!(
                index.columns.iter().any(|field| field == column),
                "reported index columns identify the requested field"
            );
        }
        let name = index
            .name
            .clone()
            .expect("created index has a portable name for wait/drop");
        self.database
            .index(IndexRequest::WaitFor(WaitForIndexRequest {
                locator: self.locator.clone(),
                index_names: vec![name.clone()],
                timeout_ms: 30_000,
            }))
            .await
            .expect("wait for actual created index");
        let IndexResponse::Indexes(ready) = self
            .database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await
            .expect("index inventory after readiness")
        else {
            panic!("index inventory")
        };
        let ready = ready
            .iter()
            .find(|index| index.name.as_ref() == Some(&name))
            .expect("waited index remains present");
        assert_ne!(
            ready.ready,
            Some(false),
            "successful wait cannot report a non-ready index"
        );
        assert!(
            self.database
                .index(IndexRequest::WaitFor(WaitForIndexRequest {
                    locator: self.locator.clone(),
                    index_names: vec!["absent_proof_index".to_owned()],
                    timeout_ms: 100,
                }))
                .await
                .is_err(),
            "missing index cannot be reported ready"
        );
        match kind {
            IndexKind::Scalar => {
                let DataResponse::Records(page) = self
                    .database
                    .data(DataRequest::Scan(
                        ScanRequest::new(self.locator.clone())
                            .filter(FilterExpr::eq("marker", "row-0000")),
                    ))
                    .await
                    .expect("query indexed scalar")
                else {
                    panic!("scalar records")
                };
                assert_eq!(page.records.len(), 1);
                assert_eq!(page.records[0].fields["marker"], "row-0000");
            }
            IndexKind::FullText => {
                let result = self
                    .database
                    .query(QueryRequest::Text(TextSearchRequest {
                        query: ScopedQuery::text(self.locator.clone())
                            .text_field("body")
                            .limit(2)
                            .projection(Projection::Columns(vec!["marker".to_owned()])),
                        text: "quartz".to_owned(),
                    }))
                    .await;
                if self.mode.is_qdrant() {
                    Self::unsupported(result);
                } else {
                    let response = result.expect("full-text match through portable query");
                    assert_eq!(response.hits.len(), 1);
                    assert_eq!(response.hits[0].record.fields["marker"], "row-0000");
                    Self::assert_projected(&response.hits[0].record);
                    if self.mode.is_surreal() {
                        self.database
                            .index(IndexRequest::Create(CreateIndexRequest {
                                locator: self.locator.clone(),
                                columns: vec!["status".to_owned()],
                                kind: IndexKind::FullText,
                                wait_timeout_ms: Some(30_000),
                            }))
                            .await
                            .expect("a second index shares the same owned analyzer");
                        let second = self
                            .database
                            .query(QueryRequest::Text(TextSearchRequest {
                                query: ScopedQuery::text(self.locator.clone())
                                    .text_field("status")
                                    .limit(4),
                                text: "original".to_owned(),
                            }))
                            .await
                            .expect("query second full-text index");
                        assert_eq!(second.hits.len(), 4);
                        assert_eq!(
                            second
                                .hits
                                .iter()
                                .map(|hit| hit.record.fields["marker"].as_str().unwrap())
                                .collect::<BTreeSet<_>>(),
                            BTreeSet::from(["row-0000", "row-0001", "row-0002", "row-0003"])
                        );
                    }
                }
            }
            IndexKind::Vector { .. } => {
                let mut query = self.vector_query(1);
                if self.mode.is_surreal() {
                    query.options.params.distance = None;
                    query.options.params.ef = Some(64);
                }
                let response = self
                    .database
                    .query(QueryRequest::Nearest(NearestRequest {
                        query,
                        vector: Self::vector(0),
                    }))
                    .await
                    .expect("query portable vector index");
                assert_eq!(response.hits.len(), 1);
                assert_eq!(response.hits[0].record.fields["marker"], "row-0000");
            }
        }
        let dropped = self
            .database
            .index(IndexRequest::Drop {
                locator: self.locator.clone(),
                name: name.clone(),
            })
            .await;
        assert_eq!(
            dropped.expect("drop created index"),
            IndexResponse::Completed
        );
        let IndexResponse::Indexes(indexes) = self
            .database
            .index(IndexRequest::List {
                locator: self.locator.clone(),
            })
            .await
            .expect("list after drop")
        else {
            panic!("index inventory")
        };
        assert!(
            !indexes
                .iter()
                .any(|index| index.name.as_ref() == Some(&name))
        );
        assert_eq!(
            self.rows().await,
            before.0,
            "index lifecycle preserves source records"
        );
    }

    async fn overwrite(&self) {
        self.seed(3).await;
        self.database
            .data(DataRequest::Insert(InsertRequest {
                locator: self.locator.clone(),
                records: vec![self.record(0).json_field("status", json!("new"))],
                mode: WriteMode::Overwrite,
            }))
            .await
            .expect("portable collection-content overwrite");
        let rows = self.rows().await;
        assert_eq!(
            rows.len(),
            1,
            "Overwrite replaces collection contents, including unmatched old rows"
        );
        assert_eq!(rows[0].fields["status"], "new");
        assert_eq!(rows[0].fields["marker"], "row-0000");
    }

    async fn invalid_index(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        for request in [
            IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["embedding".to_owned()],
                kind: IndexKind::Vector {
                    dimension: 0,
                    distance: None,
                },
                wait_timeout_ms: None,
            }),
            IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: Vec::new(),
                kind: IndexKind::Scalar,
                wait_timeout_ms: None,
            }),
            IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["marker".to_owned()],
                kind: IndexKind::Scalar,
                wait_timeout_ms: Some(0),
            }),
            IndexRequest::Drop {
                locator: self.locator.clone(),
                name: " ".to_owned(),
            },
            IndexRequest::WaitFor(WaitForIndexRequest {
                locator: self.locator.clone(),
                index_names: vec!["missing".to_owned()],
                timeout_ms: 0,
            }),
        ] {
            assert!(
                matches!(
                    self.database.index(request).await,
                    Err(StorageError::InvalidRequest(_))
                ),
                "invalid index input is rejected"
            );
            assert_eq!(
                self.snapshot().await,
                before,
                "invalid request cannot mutate"
            );
        }
        if self.mode.is_surreal() {
            for distance in [DistanceMetric::Dot, DistanceMetric::Hamming] {
                Self::unsupported(
                    self.database
                        .index(IndexRequest::Create(CreateIndexRequest {
                            locator: self.locator.clone(),
                            columns: vec!["embedding".to_owned()],
                            kind: IndexKind::Vector {
                                dimension: 4,
                                distance: Some(distance),
                            },
                            wait_timeout_ms: None,
                        }))
                        .await,
                );
                assert_eq!(
                    self.snapshot().await,
                    before,
                    "unsupported metric cannot mutate"
                );
            }
        }
    }

    #[cfg(feature = "surrealdb")]
    async fn score_collision(&self, by_id: bool, projected: bool, numeric_payload: bool) {
        let payload = if numeric_payload {
            json!(987654321)
        } else {
            json!("caller-payload")
        };
        self.create(
            (0..3)
                .map(|number| {
                    self.record(number)
                        .json_field("__distance", payload.clone())
                })
                .collect(),
            CreateCollectionMode::Create,
        )
        .await;
        let before = self.rows().await;
        let projection = if projected {
            Projection::Columns(vec!["marker".to_owned(), "__distance".to_owned()])
        } else {
            Projection::All
        };
        let query = self.vector_query(3).projection(projection);
        let request = if by_id {
            QueryRequest::SearchById(SearchByIdRequest {
                query,
                id: RecordId::U64(1),
            })
        } else {
            QueryRequest::Nearest(NearestRequest {
                query,
                vector: Self::vector(0),
            })
        };
        let response = self
            .database
            .query(request)
            .await
            .expect("real Surreal vector query");
        assert!(
            !response.hits.is_empty(),
            "collision proof requires actual vector hits"
        );
        // Self-exclusion/cardinality belongs to the independent SearchById
        // case; its failure must not hide this payload preservation regression.
        for hit in response.hits {
            let original = before
                .iter()
                .find(|row| row.fields["marker"] == hit.record.fields["marker"])
                .expect("hit belongs to seeded records");
            assert_eq!(
                hit.record.fields.get("__distance"),
                original.fields.get("__distance"),
                "caller payload must not be consumed as SDK scoring metadata"
            );
            if let Some(score) = hit.raw_score {
                assert_eq!(hit.score_kind, ScoreKind::Distance);
                let number = original.fields["number"].as_f64().unwrap();
                assert!(
                    (score - number).abs() < 0.0001,
                    "returned L2 score must be computed from vectors, not payload: {score}"
                );
            }
        }
    }

    #[cfg(feature = "surrealdb")]
    async fn plain_score_collision(&self, sample: bool) {
        self.create(
            vec![self.record(0).json_field("__score", json!(12345))],
            CreateCollectionMode::Create,
        )
        .await;
        let request = if sample {
            QueryRequest::RandomSample(RandomSampleRequest {
                locator: self.locator.clone(),
                filter: None,
                projection: Projection::All,
                limit: 1,
            })
        } else {
            QueryRequest::Scroll(ScrollRequest {
                locator: self.locator.clone(),
                filter: None,
                projection: Projection::All,
                limit: Some(1),
                offset: None,
                page_token: None,
            })
        };
        let response = self
            .database
            .query(request)
            .await
            .expect("unscored real query");
        assert_eq!(response.hits.len(), 1);
        let hit = &response.hits[0];
        assert_eq!(
            hit.record.fields.get("__score"),
            Some(&json!(12345)),
            "unscored queries preserve caller score-shaped fields"
        );
        assert_eq!(
            hit.raw_score, None,
            "unscored queries must not invent a raw score"
        );
    }

    #[cfg(feature = "surrealdb")]
    async fn fulltext_collection_drop(&self) {
        // A successful full-text query proves the index is usable, but does
        // not stop SurrealDB's independent native index-compaction commits.
        // Exercise immediate collection removal repeatedly without a sleep
        // or a test-side retry that could hide a catalog failure.
        for cycle in 0..16 {
            self.create(
                (0..64).map(|number| self.surreal_record(number)).collect(),
                CreateCollectionMode::Create,
            )
            .await;
            assert_eq!(
                self.database
                    .index(IndexRequest::Create(CreateIndexRequest {
                        locator: self.locator.clone(),
                        columns: vec!["body".to_owned()],
                        kind: IndexKind::FullText,
                        wait_timeout_ms: Some(30_000),
                    }))
                    .await
                    .expect("full-text index before immediate collection drop"),
                IndexResponse::Completed
            );
            let response = self
                .database
                .query(QueryRequest::Text(TextSearchRequest {
                    query: ScopedQuery::text(self.locator.clone()).text_field("body"),
                    text: "quartz".to_owned(),
                }))
                .await
                .expect("native full-text query before collection drop");
            assert_eq!(response.hits.len(), 1);
            assert_eq!(response.hits[0].record, self.surreal_row(0));
            assert_eq!(response.hits[0].score_kind, ScoreKind::Relevance);
            assert_eq!(
                self.database
                    .catalog(CatalogRequest::DropCollection {
                        locator: self.locator.clone(),
                        ignore_missing: cycle % 2 == 0,
                    })
                    .await
                    .expect("immediate full-text collection drop must commit"),
                CatalogResponse::Completed
            );
            self.assert_missing_surreal_table().await;
            let missing = self
                .database
                .catalog(CatalogRequest::DropCollection {
                    locator: self.locator.clone(),
                    ignore_missing: false,
                })
                .await;
            assert!(
                matches!(missing, Err(StorageError::BackendOperation { message, .. })
                    if message.contains("does not exist") && message.contains(&self.locator.collection)),
                "ignore_missing=false must retain the native missing-table error"
            );
            assert_eq!(
                self.database
                    .catalog(CatalogRequest::DropCollection {
                        locator: self.locator.clone(),
                        ignore_missing: true,
                    })
                    .await
                    .expect("ignore_missing=true commits for an absent collection"),
                CatalogResponse::Completed
            );
            self.assert_missing_surreal_table().await;
        }
    }

    #[cfg(feature = "surrealdb")]
    async fn text_score_collision(&self, projected: bool, numeric_payload: bool) {
        let payload = if numeric_payload {
            json!(987654321)
        } else {
            json!("caller-payload")
        };
        self.create(
            (0..3)
                .map(|number| {
                    self.record(number)
                        .json_field("__score", payload.clone())
                        .json_field("record", json!("caller-record"))
                        .json_field("score", json!("caller-score"))
                })
                .collect(),
            CreateCollectionMode::Create,
        )
        .await;
        self.database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["body".to_owned()],
                kind: IndexKind::FullText,
                wait_timeout_ms: Some(30_000),
            }))
            .await
            .expect("fresh full-text setup through the public index API");
        let projection = if projected {
            Projection::Columns(vec!["marker".to_owned(), "__score".to_owned()])
        } else {
            Projection::All
        };
        let response = self
            .database
            .query(QueryRequest::Text(TextSearchRequest {
                query: ScopedQuery::text(self.locator.clone())
                    .text_field("body")
                    .limit(2)
                    .projection(projection),
                text: "quartz".to_owned(),
            }))
            .await
            .expect("real full-text query with colliding caller field");
        assert_eq!(response.hits.len(), 1);
        let hit = &response.hits[0];
        assert_eq!(hit.record.fields["marker"], "row-0000");
        assert_eq!(
            hit.record.fields.get("__score"),
            Some(&payload),
            "full-text relevance metadata cannot replace or remove caller payload"
        );
        assert_eq!(hit.score_kind, ScoreKind::Relevance);
        if let Some(score) = hit.raw_score {
            assert!(score.is_finite());
            assert_ne!(
                score, 987654321.0,
                "relevance cannot come from caller payload"
            );
        }
        if projected {
            assert_eq!(hit.record.fields.len(), 2);
        } else {
            assert_eq!(
                hit.record.fields.get("record"),
                Some(&json!("caller-record"))
            );
            assert_eq!(hit.record.fields.get("score"), Some(&json!("caller-score")));
        }
    }

    #[cfg(feature = "qdrant")]
    async fn rest_vector_limit(&self, exceeds: bool) {
        self.seed(3).await;
        self.database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["body".to_owned()],
                kind: IndexKind::FullText,
                wait_timeout_ms: Some(30_000),
            }))
            .await
            .expect("index before native vector boundary replacement");
        let before = self.snapshot().await;
        let result = self
            .database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                records: Vec::new(),
                mode: CreateCollectionMode::Overwrite,
                schema: Some(CollectionSchema {
                    fields: vec![FieldSchema {
                        name: "embedding".to_owned(),
                        field_type: FieldType::DenseVector {
                            dim: if exceeds { 65_537 } else { 65_536 },
                            element: VectorElementType::F32,
                        },
                        nullable: false,
                        metadata: Default::default(),
                    }],
                    metadata: Default::default(),
                }),
            }))
            .await;
        if exceeds {
            assert!(
                result.is_err(),
                "pinned REST rejects dimensions above 65536"
            );
            assert_eq!(
                self.snapshot().await,
                before,
                "native-invalid replacement must preserve prior collection and index"
            );
        } else {
            assert!(
                matches!(result, Ok(CatalogResponse::Collection(_))),
                "pinned REST permits dimension 65536: {result:?}"
            );
            assert_eq!(
                self.database
                    .data(DataRequest::Count(CountRequest {
                        locator: self.locator.clone(),
                        filter: None
                    }))
                    .await
                    .expect("count valid empty replacement"),
                DataResponse::Count(0)
            );
        }
    }

    #[cfg(feature = "qdrant")]
    async fn empty_write_identity(&self) {
        self.seed(3).await;
        if !self.mode.is_edge() {
            self.database
                .index(IndexRequest::Create(CreateIndexRequest {
                    locator: self.locator.clone(),
                    columns: vec!["body".to_owned()],
                    kind: IndexKind::FullText,
                    wait_timeout_ms: Some(30_000),
                }))
                .await
                .expect("index before empty identity operations");
        }
        let before = self.snapshot().await;
        for merge in [false, true] {
            for missing in [false, true] {
                let mut locator = self.locator.clone();
                if missing {
                    locator.collection.push_str("_missing");
                }
                let cleanup_locator = missing.then(|| locator.clone());
                let request = if merge {
                    DataRequest::Merge(MergeRequest {
                        locator,
                        records: Vec::new(),
                        on: vec!["id".to_owned()],
                        actions: MergeActions::default(),
                    })
                } else {
                    DataRequest::Insert(InsertRequest::append(locator, Vec::new()))
                };
                let result = self.database.data(request).await;
                if let Some(locator) = cleanup_locator {
                    let cleanup = self
                        .database
                        .catalog(CatalogRequest::DropCollection {
                            locator,
                            ignore_missing: true,
                        })
                        .await;
                    assert!(
                        cleanup.is_ok(),
                        "missing-target cleanup failed: {cleanup:?}; operation: {result:?}"
                    );
                    assert!(
                        result.is_err(),
                        "an identity write still requires an existing collection"
                    );
                } else {
                    assert!(
                        matches!(result, Ok(DataResponse::Mutation(_))),
                        "zero-row identity operation succeeds on existing collection: {result:?}"
                    );
                }
                assert_eq!(
                    self.snapshot().await,
                    before,
                    "identity preserves rows/schema/indexes"
                );
            }
        }
    }

    async fn empty_overwrite(&self) {
        self.seed(3).await;
        if !self.mode.is_edge() {
            self.database
                .index(IndexRequest::Create(CreateIndexRequest {
                    locator: self.locator.clone(),
                    columns: vec![
                        if self.mode.is_qdrant() {
                            "body"
                        } else {
                            "marker"
                        }
                        .to_owned(),
                    ],
                    kind: if self.mode.is_qdrant() {
                        IndexKind::FullText
                    } else {
                        IndexKind::Scalar
                    },
                    wait_timeout_ms: Some(30_000),
                }))
                .await
                .expect("index before collection-content clear");
        }
        let before = self.snapshot().await;
        self.database
            .data(DataRequest::Insert(InsertRequest {
                locator: self.locator.clone(),
                records: Vec::new(),
                mode: WriteMode::Overwrite,
            }))
            .await
            .expect("empty overwrite clears the collection contents");
        let after = self.snapshot().await;
        assert!(after.0.is_empty());
        assert_eq!(
            self.database
                .data(DataRequest::Count(CountRequest {
                    locator: self.locator.clone(),
                    filter: None,
                }))
                .await
                .expect("count after empty replacement"),
            DataResponse::Count(0)
        );
        let CatalogResponse::Collection(mut expected) = before.1 else {
            panic!("collection info")
        };
        if expected.record_count.is_some() {
            expected.record_count = Some(0);
        }
        assert_eq!(
            after.1,
            CatalogResponse::Collection(expected),
            "clear preserves collection identity and schema"
        );
        assert_eq!(
            after.2, before.2,
            "clear preserves every available index inventory field"
        );
    }

    #[cfg(feature = "surrealdb")]
    async fn rejected_schema_overwrite(&self, invalid_name: bool) {
        self.seed(3).await;
        self.database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["marker".to_owned()],
                kind: IndexKind::Scalar,
                wait_timeout_ms: Some(30_000),
            }))
            .await
            .expect("original indexed collection");
        let before = self.snapshot().await;
        let request = CreateCollectionRequest {
            locator: self.locator.clone(),
            schema: Some(CollectionSchema {
                fields: vec![FieldSchema {
                    name: if invalid_name { " " } else { "body" }.to_owned(),
                    field_type: FieldType::I64,
                    nullable: false,
                    metadata: Default::default(),
                }],
                metadata: Default::default(),
            }),
            records: vec![self.record(9)],
            mode: CreateCollectionMode::Overwrite,
        };
        assert!(
            self.database
                .catalog(CatalogRequest::CreateCollection(request))
                .await
                .is_err(),
            "invalid identifier or schema-incompatible replacement is rejected"
        );
        assert_eq!(
            self.snapshot().await,
            before,
            "rejected catalog replacement preserves rows, catalog and indexes"
        );
    }

    #[cfg(feature = "qdrant")]
    async fn rejected_dimensions_overwrite(&self) {
        self.seed(3).await;
        let before = self.snapshot().await;
        let result = self
            .database
            .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                locator: self.locator.clone(),
                schema: None,
                records: vec![
                    self.record(9),
                    self.record(10)
                        .vector("embedding", DenseVector::F32(vec![1.0, 2.0, 3.0])),
                ],
                mode: CreateCollectionMode::Overwrite,
            }))
            .await;
        assert!(
            result.is_err(),
            "inconsistent seed vector dimensions are rejected"
        );
        assert_eq!(
            self.snapshot().await,
            before,
            "all replacement seed records are validated before removing original collection"
        );
    }

    async fn rejected_overwrite(&self, unsupported_id: bool) {
        self.seed(3).await;
        if !self.mode.is_edge() {
            self.database
                .index(IndexRequest::Create(CreateIndexRequest {
                    locator: self.locator.clone(),
                    columns: vec![if self.mode.is_qdrant() {
                        "body".to_owned()
                    } else {
                        "marker".to_owned()
                    }],
                    kind: if self.mode.is_qdrant() {
                        IndexKind::FullText
                    } else {
                        IndexKind::Scalar
                    },
                    wait_timeout_ms: Some(30_000),
                }))
                .await
                .expect("original collection has an index to preserve");
        }
        let before = self.snapshot().await;
        let mut record = self.record(9);
        if unsupported_id {
            record.id = Some(RecordId::String("caller-string-id".to_owned()));
        } else if self.mode.is_qdrant() {
            let vector = record.vectors.remove("embedding").unwrap();
            record.vectors.insert(String::new(), vector);
        } else if self.mode.is_surreal() {
            record
                .fields
                .insert("id".to_owned(), json!("caller-payload"));
        } else {
            record.id = Some(RecordId::U64(10));
        }
        Self::unsupported(
            self.database
                .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                    locator: self.locator.clone(),
                    schema: None,
                    records: vec![record],
                    mode: CreateCollectionMode::Overwrite,
                }))
                .await,
        );
        assert_eq!(
            self.snapshot().await,
            before,
            "unsupported overwrite preserves the collection and every available snapshot field"
        );
    }
}

#[tokio::test]
async fn lancedb_cast_preserves_already_required_nullability() {
    for mode in [Mode::Lance, Mode::LanceMemory] {
        mode.cases(&[Case::CastAlreadyRequired]).await;
    }
}

#[cfg(feature = "qdrant")]
impl DatabaseCase {
    fn qdrant_record(&self, number: u64) -> DataRecord {
        let rank = match number {
            1 => 3,
            3 => 1,
            other => other,
        };
        self.record(number).vector("alternate", Self::vector(rank))
    }

    async fn qdrant_seed(&self, count: u64) {
        self.create(
            (0..count).map(|n| self.qdrant_record(n)).collect(),
            CreateCollectionMode::Create,
        )
        .await;
    }

    async fn qdrant_lookup_rows(
        &self,
        ids: Vec<RecordId>,
        projection: Projection,
    ) -> Vec<DataRecord> {
        let DataResponse::Records(page) = self
            .database
            .data(DataRequest::Lookup(
                LookupRequest::new(self.locator.clone(), ids).projection(projection),
            ))
            .await
            .expect("Qdrant lookup")
        else {
            panic!("lookup records")
        };
        assert!(
            page.next_page.is_none(),
            "lookup is complete, including missing IDs"
        );
        let mut rows = page.records;
        rows.sort_by_key(|row| row.id.as_ref().unwrap().as_string_lossy());
        rows
    }

    async fn qdrant_lookup(&self) {
        self.qdrant_seed(4).await;
        let before = self.rows().await;
        // First prove full payloads and both vector names; native cosine storage
        // returns normalized f32 vectors, not the original numeric representation.
        for (n, row) in before.iter().enumerate() {
            let input = self.qdrant_record(n as u64);
            assert_eq!(row.id, input.id);
            assert_eq!(row.fields, input.fields);
            assert_eq!(row.vectors.len(), 2);
            for (name, vector) in input.vectors {
                let DenseVector::F32(input) = vector else {
                    panic!("f32 fixture")
                };
                let DenseVector::F32(actual) = &row.vectors[&name] else {
                    panic!("native f32")
                };
                let norm = input.iter().map(|x| x * x).sum::<f32>().sqrt();
                assert_eq!(actual.len(), input.len());
                for (actual, input) in actual.iter().zip(input) {
                    assert!((actual - input / norm).abs() < 0.00001);
                }
            }
        }
        for projection in [
            Projection::All,
            Projection::Columns(vec!["marker".into(), "status".into()]),
            Projection::Columns(vec!["alternate".into(), "marker".into()]),
        ] {
            let mut expected = vec![before[0].clone(), before[2].clone()];
            if let Projection::Columns(columns) = &projection {
                for row in &mut expected {
                    row.fields.retain(|name, _| columns.contains(name));
                    row.vectors.retain(|name, _| columns.contains(name));
                }
            }
            let rows = self
                .qdrant_lookup_rows(
                    vec![999_u64.into(), 3_u64.into(), 998_u64.into(), 1_u64.into()],
                    projection.clone(),
                )
                .await;
            assert_eq!(rows, expected);
            assert!(
                self.qdrant_lookup_rows(vec![], projection.clone())
                    .await
                    .is_empty()
            );
            assert!(
                self.qdrant_lookup_rows(vec![998_u64.into(), 999_u64.into()], projection)
                    .await
                    .is_empty()
            );
        }
        assert_eq!(self.rows().await, before);
    }

    async fn qdrant_assert_named_queries(
        &self,
        expected_default: &[u64],
        expected_alternate: &[u64],
    ) {
        let mut projected_rows = self.rows().await;
        for row in &mut projected_rows {
            row.fields.retain(|name, _| name == "marker");
            row.vectors.retain(|name, _| name == "alternate");
        }
        for (field, expected) in [
            (None, expected_default),
            (Some("embedding"), expected_default),
            (Some("alternate"), expected_alternate),
        ] {
            for by_id in [false, true] {
                let mut query = ScopedQuery::vector(self.locator.clone())
                    .params(VectorSearchParams {
                        limit: expected.len(),
                        ..Default::default()
                    })
                    .projection(Projection::Columns(vec![
                        "marker".into(),
                        "alternate".into(),
                    ]));
                if let Some(field) = field {
                    query = query.vector_field(field);
                }
                let request = if by_id {
                    QueryRequest::SearchById(SearchByIdRequest {
                        query,
                        id: 1_u64.into(),
                    })
                } else {
                    QueryRequest::Nearest(NearestRequest {
                        query,
                        vector: Self::vector(0),
                    })
                };
                let response = self
                    .database
                    .query(request)
                    .await
                    .expect("named-vector query");
                let expected = if by_id { &expected[1..] } else { expected };
                assert_eq!(
                    response
                        .hits
                        .iter()
                        .map(|hit| hit.record.id.clone().unwrap())
                        .collect::<Vec<_>>(),
                    expected
                        .iter()
                        .copied()
                        .map(RecordId::U64)
                        .collect::<Vec<_>>(),
                    "field={field:?}, by_id={by_id}"
                );
                assert!(response.next_page.is_none());
                for hit in response.hits {
                    let expected = projected_rows
                        .iter()
                        .find(|row| row.id == hit.record.id)
                        .expect("ranked hit has a stored projected record");
                    assert_eq!(
                        &hit.record, expected,
                        "projected values for field={field:?}, by_id={by_id}"
                    );
                }
            }
        }
    }

    async fn qdrant_named_vectors(&self) {
        self.qdrant_seed(4).await;
        self.qdrant_assert_named_queries(&[1, 2, 3, 4], &[1, 4, 3, 2])
            .await;
        let before = self.rows().await;
        // ID 2 has different vectors in each named field. Its alternate vector
        // ranks IDs 3, 4, 1; accidentally fetching embedding ranks 4, 3, 1.
        let response = self
            .database
            .query(QueryRequest::SearchById(SearchByIdRequest {
                query: self
                    .vector_query(3)
                    .vector_field("alternate")
                    .filter(FilterExpr::eq("status", "original"))
                    .projection(Projection::Columns(vec![
                        "marker".into(),
                        "alternate".into(),
                    ])),
                id: 2_u64.into(),
            }))
            .await
            .expect("source lookup uses the selected named vector");
        assert!(response.next_page.is_none());
        let mut expected = vec![before[2].clone(), before[3].clone(), before[0].clone()];
        for row in &mut expected {
            row.fields.retain(|name, _| name == "marker");
            row.vectors.retain(|name, _| name == "alternate");
        }
        assert_eq!(
            response
                .hits
                .into_iter()
                .map(|hit| hit.record)
                .collect::<Vec<_>>(),
            expected,
            "selected seed vector, source exclusion, rank, and projected values"
        );
        for filter in [
            None,
            Some(FilterExpr::eq("status", "original")),
            Some(FilterExpr::Compare {
                field: "marker".into(),
                op: ComparisonOp::Ne,
                value: json!("row-0000"),
            }),
            Some(FilterExpr::eq("status", "absent")),
        ] {
            let empty = filter == Some(FilterExpr::eq("status", "absent"));
            let mut query = self.vector_query(3);
            query.scope.filter = filter;
            let response = self
                .database
                .query(QueryRequest::SearchById(SearchByIdRequest {
                    query,
                    id: 1_u64.into(),
                }))
                .await
                .expect("source exclusion with caller filter");
            let expected = if empty {
                vec![]
            } else {
                vec![before[1].clone(), before[2].clone(), before[3].clone()]
            };
            assert_eq!(
                response
                    .hits
                    .into_iter()
                    .map(|hit| hit.record)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(self.rows().await, before);
    }

    async fn qdrant_exact_offsets(&self) {
        self.seed(4).await;
        let before = self.rows().await;
        for (filter, offset, limit, expected) in [
            (None, 1, 3, vec![2, 3, 4]),
            (None, 2, 1, vec![3]),
            (None, 2, 2, vec![3, 4]),
            (None, 0, 3, vec![1, 2, 3]),
            (None, 5, 3, vec![]),
            (
                Some(FilterExpr::Compare {
                    field: "number".into(),
                    op: ComparisonOp::Gte,
                    value: json!(1),
                }),
                1,
                3,
                vec![3, 4],
            ),
        ] {
            let mut query = self
                .vector_query(limit)
                .projection(Projection::Columns(vec!["marker".into(), "number".into()]))
                .map_options(|options| options.params.offset = Some(offset));
            query.scope.filter = filter;
            let response = self
                .database
                .query(QueryRequest::Nearest(NearestRequest {
                    query,
                    vector: Self::vector(0),
                }))
                .await
                .expect("exact offset query");
            assert_eq!(response.hits.len(), expected.len());
            assert_eq!(
                response
                    .hits
                    .into_iter()
                    .map(|hit| {
                        assert_eq!(hit.record.fields.len(), 2);
                        assert!(hit.record.vectors.is_empty());
                        (
                            hit.record.id.unwrap(),
                            hit.record.fields["marker"].clone(),
                            hit.record.fields["number"].clone(),
                        )
                    })
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|id| {
                        (
                            RecordId::U64(id),
                            json!(format!("row-{:04}", id - 1)),
                            json!(id - 1),
                        )
                    })
                    .collect::<Vec<_>>()
            );
        }
        // A selective filter still has enough candidates to fill offset + limit.
        self.database
            .data(DataRequest::Insert(InsertRequest::append(
                self.locator.clone(),
                vec![self.record(4)],
            )))
            .await
            .expect("additional matching record");
        let response = self
            .database
            .query(QueryRequest::Nearest(NearestRequest {
                query: self
                    .vector_query(3)
                    .filter(FilterExpr::Compare {
                        field: "number".into(),
                        op: ComparisonOp::Gte,
                        value: json!(1),
                    })
                    .map_options(|options| options.params.offset = Some(1)),
                vector: Self::vector(0),
            }))
            .await
            .expect("selective offset fills requested cardinality");
        assert_eq!(
            response
                .hits
                .into_iter()
                .map(|hit| hit.record.id.unwrap())
                .collect::<Vec<_>>(),
            vec![RecordId::U64(3), RecordId::U64(4), RecordId::U64(5)]
        );
        assert_eq!(&self.rows().await[..4], &before);
    }

    async fn qdrant_uuid_pages(&self) {
        let records = [6, 2, 4, 0, 5, 1, 3]
            .into_iter()
            .map(|n| {
                let mut row = self.qdrant_record(n);
                row.id = Some(uuid::Uuid::from_u128(n as u128 + 1).into());
                row.json_field("selected", json!(n != 0 && n != 6))
            })
            .collect();
        self.create(records, CreateCollectionMode::Create).await;
        let before = self.rows().await;
        for scroll in [false, true] {
            for limit in [2, 3] {
                let mut token = None;
                let mut collected = Vec::new();
                let expected = &before[2..6]; // selected rows 1..5, initial offset skips row 1
                for (page_number, chunk) in expected.chunks(limit).enumerate() {
                    let (records, next) = if scroll {
                        let response = self
                            .database
                            .query(QueryRequest::Scroll(ScrollRequest {
                                locator: self.locator.clone(),
                                filter: Some(FilterExpr::eq("selected", true)),
                                projection: Projection::All,
                                limit: Some(limit),
                                offset: Some(1),
                                page_token: token,
                            }))
                            .await
                            .expect("UUID scroll");
                        (
                            response
                                .hits
                                .into_iter()
                                .map(|hit| hit.record)
                                .collect::<Vec<_>>(),
                            response.next_page,
                        )
                    } else {
                        let DataResponse::Records(page) = self
                            .database
                            .data(DataRequest::Scan(ScanRequest {
                                locator: self.locator.clone(),
                                filter: Some(FilterExpr::eq("selected", true)),
                                projection: Projection::All,
                                limit: Some(limit),
                                offset: Some(1),
                                page_token: token,
                            }))
                            .await
                            .expect("UUID scan")
                        else {
                            panic!("scan records")
                        };
                        (page.records, page.next_page)
                    };
                    assert_eq!(
                        records, chunk,
                        "scroll={scroll}, limit={limit}, page={page_number}"
                    );
                    collected.extend(records);
                    assert_eq!(
                        next.is_none(),
                        collected.len() == expected.len(),
                        "short and exactly full final pages terminate"
                    );
                    token = next; // Opaque native cursor is returned unchanged; offset stays set.
                }
                assert_eq!(collected, expected);
            }
        }
        assert_eq!(self.rows().await, before);
    }

    async fn qdrant_assert_filter(&self, filter: FilterExpr, expected: &[DataRecord]) {
        let DataResponse::Count(count) = self
            .database
            .data(DataRequest::Count(CountRequest {
                locator: self.locator.clone(),
                filter: Some(filter.clone()),
            }))
            .await
            .expect("filtered count")
        else {
            panic!("count response")
        };
        assert_eq!(count, expected.len() as u64);
        let DataResponse::Records(page) = self
            .database
            .data(DataRequest::Scan(
                ScanRequest::new(self.locator.clone())
                    .filter(filter.clone())
                    .limit(20),
            ))
            .await
            .expect("filtered scan")
        else {
            panic!("scan records")
        };
        assert_eq!(page.records, expected);
        assert!(page.next_page.is_none());
        let response = self
            .database
            .query(QueryRequest::Nearest(NearestRequest {
                query: self.vector_query(20).filter(filter),
                vector: Self::vector(0),
            }))
            .await
            .expect("filtered nearest");
        assert_eq!(
            response
                .hits
                .into_iter()
                .map(|hit| hit.record)
                .collect::<Vec<_>>(),
            expected
        );
    }

    async fn qdrant_payload_index(&self) {
        self.qdrant_seed(6).await;
        let before = self.rows().await;
        let create = self
            .database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: self.locator.clone(),
                columns: vec!["body".into()],
                kind: IndexKind::FullText,
                wait_timeout_ms: Some(30_000),
            }))
            .await;
        let inventory = if self.mode.is_edge() {
            Self::unsupported(create);
            Self::unsupported(
                self.database
                    .index(IndexRequest::List {
                        locator: self.locator.clone(),
                    })
                    .await,
            );
            Self::unsupported(
                self.database
                    .index(IndexRequest::Drop {
                        locator: self.locator.clone(),
                        name: "body".into(),
                    })
                    .await,
            );
            assert!(
                self.database
                    .index(IndexRequest::WaitFor(WaitForIndexRequest {
                        locator: self.locator.clone(),
                        index_names: vec!["body".into()],
                        timeout_ms: 100,
                    }))
                    .await
                    .is_err()
            );
            None
        } else {
            assert_eq!(
                create.expect("create payload index"),
                IndexResponse::Completed
            );
            self.database
                .index(IndexRequest::WaitFor(WaitForIndexRequest {
                    locator: self.locator.clone(),
                    index_names: vec!["body".into()],
                    timeout_ms: 30_000,
                }))
                .await
                .expect("ready payload index");
            let inventory = self
                .database
                .index(IndexRequest::List {
                    locator: self.locator.clone(),
                })
                .await
                .expect("index inventory");
            let IndexResponse::Indexes(indexes) = &inventory else {
                panic!("indexes")
            };
            assert_eq!(indexes.len(), 1);
            assert_eq!(indexes[0].name.as_deref(), Some("body"));
            Some(inventory)
        };
        assert_eq!(
            self.rows().await,
            before,
            "index operations preserve full records"
        );
        let range = FilterExpr::Between {
            field: "number".into(),
            lower: json!(1),
            upper: json!(4),
        };
        let composed = FilterExpr::eq("partition", "b").and(range.clone());
        for (filter, expected) in [
            (FilterExpr::eq("partition", "a"), &before[..2]),
            (range, &before[1..5]),
            (composed.clone(), &before[2..5]),
        ] {
            self.qdrant_assert_filter(filter, expected).await;
        }
        self.database
            .data(DataRequest::Update(UpdateRequest {
                locator: self.locator.clone(),
                filter: Some(composed),
                assignments: vec![
                    UpdateAssignment {
                        column: "body".into(),
                        value: json!("updated searchable payload"),
                    },
                    UpdateAssignment {
                        column: "status".into(),
                        value: json!("updated"),
                    },
                ],
            }))
            .await
            .expect("completed filtered payload update");
        let mut expected = before;
        for row in &mut expected[2..5] {
            row.fields
                .insert("body".into(), json!("updated searchable payload"));
            row.fields.insert("status".into(), json!("updated"));
        }
        assert_eq!(
            self.rows().await,
            expected,
            "all unassigned fields and vectors preserved"
        );
        self.qdrant_assert_filter(FilterExpr::eq("status", "updated"), &expected[2..5])
            .await;
        self.database
            .data(DataRequest::Delete(DeleteRequest {
                locator: self.locator.clone(),
                filter: FilterExpr::eq("status", "updated").and(FilterExpr::Compare {
                    field: "number".into(),
                    op: ComparisonOp::Gte,
                    value: json!(3),
                }),
            }))
            .await
            .expect("completed filtered delete");
        expected.retain(|row| row.fields["number"] != 3 && row.fields["number"] != 4);
        assert_eq!(self.rows().await, expected);
        self.assert_count(4).await;
        self.qdrant_assert_filter(FilterExpr::eq("status", "updated"), &expected[2..3])
            .await;
        if let Some(inventory) = inventory {
            assert_eq!(
                self.database
                    .index(IndexRequest::List {
                        locator: self.locator.clone()
                    })
                    .await
                    .expect("retained index"),
                inventory
            );
            self.database
                .index(IndexRequest::Drop {
                    locator: self.locator.clone(),
                    name: "body".into(),
                })
                .await
                .expect("drop payload index");
            assert_eq!(
                self.database
                    .index(IndexRequest::List {
                        locator: self.locator.clone()
                    })
                    .await
                    .expect("empty index inventory"),
                IndexResponse::Indexes(vec![])
            );
            assert!(
                self.database
                    .index(IndexRequest::WaitFor(WaitForIndexRequest {
                        locator: self.locator.clone(),
                        index_names: vec!["body".into()],
                        timeout_ms: 100,
                    }))
                    .await
                    .is_err(),
                "dropped index cannot be ready"
            );
        }
        assert_eq!(self.rows().await, expected);
    }

    async fn qdrant_vector_representations(&self) {
        let vectors = [
            DenseVector::F32(vec![3., 4., 0., 0.]),
            DenseVector::F64(vec![3., 4., 0., 0.]),
            DenseVector::I16(vec![3, 4, 0, 0]),
            DenseVector::I32(vec![3, 4, 0, 0]),
            DenseVector::I64(vec![3, 4, 0, 0]),
        ];
        self.create(
            vectors
                .into_iter()
                .enumerate()
                .map(|(n, vector)| self.record(n as u64).vector("embedding", vector))
                .collect(),
            CreateCollectionMode::Create,
        )
        .await;
        let before = self.rows().await;
        assert_eq!(before.len(), 5);
        for row in &before {
            let DenseVector::F32(values) = &row.vectors["embedding"] else {
                panic!("native f32 readback")
            };
            assert_eq!(values.len(), 4);
            for (actual, expected) in values.iter().zip([0.6, 0.8, 0., 0.]) {
                assert!((actual - expected).abs() < 0.00001);
            }
        }
        for vector in [
            DenseVector::F64(vec![0.1, 4., 0., 0.]),
            DenseVector::I32(vec![16_777_217, 4, 0, 0]),
            DenseVector::I64(vec![16_777_217, 4, 0, 0]),
        ] {
            assert!(
                self.database
                    .data(DataRequest::Insert(InsertRequest::append(
                        self.locator.clone(),
                        vec![self.record(9).vector("embedding", vector.clone())]
                    )))
                    .await
                    .is_err(),
                "lossy write rejected"
            );
            assert!(
                self.database
                    .query(QueryRequest::Nearest(NearestRequest {
                        query: self.vector_query(1),
                        vector
                    }))
                    .await
                    .is_err(),
                "lossy query rejected"
            );
            assert_eq!(self.rows().await, before);
        }
    }
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_lookup_missing_ids_and_projections() {
    Mode::qdrant_cases(&[Case::QdrantLookup]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_named_vectors_and_search_by_id_source_exclusion() {
    Mode::qdrant_cases(&[Case::QdrantNamedVectors]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_nearest_offset_exact_cardinality() {
    Mode::qdrant_cases(&[Case::NearestOffset]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_uuid_pagination_with_initial_offset() {
    Mode::qdrant_cases(&[Case::QdrantUuidPages]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_filtered_payload_and_index_lifecycle() {
    Mode::qdrant_cases(&[Case::QdrantPayloadIndex]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_retained_vector_representations() {
    Mode::qdrant_cases(&[Case::QdrantVectorRepresentations]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_edge_mutations_persist_after_reopen() {
    let directory = tempfile::tempdir().expect("isolated Edge filesystem");
    let mut configuration = Mode::QdrantEdge.configuration(directory.path());
    Mode::select_qdrant_default(&mut configuration);
    let locator = StorageLocator::new(format!("proof_{}", uuid::Uuid::new_v4().simple()));
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
        let mut expected = {
            let fixture = DatabaseCase {
                database: Database::connect(configuration.clone())
                    .await
                    .expect("initial Edge connection"),
                configuration: configuration.clone(),
                mode: Mode::QdrantEdge,
                locator: locator.clone(),
            };
            fixture.qdrant_seed(4).await;
            fixture
                .database
                .data(DataRequest::Insert(InsertRequest::append(
                    locator.clone(),
                    vec![fixture.qdrant_record(4)],
                )))
                .await
                .expect("insert before close");
            let mut expected = fixture.rows().await;
            fixture
                .database
                .data(DataRequest::Update(UpdateRequest {
                    locator: locator.clone(),
                    filter: Some(FilterExpr::eq("partition", "b")),
                    assignments: vec![UpdateAssignment {
                        column: "status".into(),
                        value: json!("persisted"),
                    }],
                }))
                .await
                .expect("update before close");
            for row in &mut expected[2..] {
                row.fields.insert("status".into(), json!("persisted"));
            }
            fixture
                .database
                .data(DataRequest::Delete(DeleteRequest {
                    locator: locator.clone(),
                    filter: FilterExpr::eq("number", 1),
                }))
                .await
                .expect("delete before close");
            expected.remove(1);
            assert_eq!(fixture.rows().await, expected);
            fixture
                .qdrant_assert_named_queries(&[1, 3, 4, 5], &[1, 4, 3, 5])
                .await;
            drop(fixture); // No Database clone or accepted operation survives this scope.
            expected
        };
        for second_reopen in [false, true] {
            let fixture = DatabaseCase {
                database: Database::connect(configuration.clone())
                    .await
                    .expect("reopen after all owners drop"),
                configuration: configuration.clone(),
                mode: Mode::QdrantEdge,
                locator: locator.clone(),
            };
            let CatalogResponse::Collection(info) = fixture
                .database
                .catalog(CatalogRequest::OpenCollection {
                    locator: locator.clone(),
                })
                .await
                .expect("open existing collection")
            else {
                panic!("collection information")
            };
            assert_eq!(info.record_count, Some(expected.len() as u64));
            let CatalogResponse::Collections(collections) = fixture
                .database
                .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
                    namespace: vec![],
                    limit: None,
                    start_after: None,
                }))
                .await
                .expect("list persisted collection")
            else {
                panic!("collection listing")
            };
            assert!(collections.iter().any(|item| item.locator == locator));
            fixture.assert_count(expected.len() as u64).await;
            assert_eq!(fixture.rows().await, expected);
            assert_eq!(
                fixture
                    .qdrant_lookup_rows((1..=6).map(RecordId::U64).collect(), Projection::All)
                    .await,
                expected
            );
            let mut projected = expected.clone();
            for row in &mut projected {
                row.fields.retain(|name, _| name == "marker");
                row.vectors.retain(|name, _| name == "alternate");
            }
            assert_eq!(
                fixture
                    .qdrant_lookup_rows(
                        (1..=6).map(RecordId::U64).collect(),
                        Projection::Columns(vec!["marker".into(), "alternate".into()])
                    )
                    .await,
                projected
            );
            let (default, alternate) = if second_reopen {
                (vec![1, 3, 4], vec![1, 4, 3])
            } else {
                (vec![1, 3, 4, 5], vec![1, 4, 3, 5])
            };
            fixture
                .qdrant_assert_named_queries(&default, &alternate)
                .await;
            for field in ["embedding", "alternate"] {
                assert!(
                    matches!(
                        fixture
                            .database
                            .data(DataRequest::Insert(InsertRequest::append(
                                locator.clone(),
                                vec![
                                    fixture
                                        .qdrant_record(9)
                                        .vector(field, DenseVector::F32(vec![1., 2.]))
                                ]
                            )))
                            .await,
                        Err(StorageError::InvalidRequest(_))
                    ),
                    "reopened schema rejects wrong dimensions for {field} before mutation"
                );
                assert_eq!(fixture.rows().await, expected);
            }
            assert_eq!(fixture.rows().await, expected);
            if !second_reopen {
                fixture
                    .database
                    .data(DataRequest::Delete(DeleteRequest {
                        locator: locator.clone(),
                        filter: FilterExpr::eq("number", 4),
                    }))
                    .await
                    .expect("mutation through reopened owner");
                expected.pop();
            }
            drop(fixture);
        }
    }))
    .catch_unwind()
    .await;
    let cleanup = tokio::time::timeout(Duration::from_secs(30), async {
        let database = Database::connect(configuration).await?;
        database
            .catalog(CatalogRequest::DropCollection {
                locator,
                ignore_missing: true,
            })
            .await
    })
    .await;
    let directory_cleanup = directory.close();
    assert!(
        matches!(observed, Ok(Ok(()))),
        "normal reopen proof: {observed:?}; cleanup: {cleanup:?}; directory cleanup: {directory_cleanup:?}"
    );
    assert!(
        matches!(cleanup, Ok(Ok(_))),
        "collection cleanup: {cleanup:?}"
    );
    directory_cleanup.expect("directory cleanup");
}

#[tokio::test]
async fn lancedb_relative_db_prefix_stays_local_and_reopens_absolute() {
    let directory = tempfile::Builder::new()
        .prefix("db-storage-proof-")
        .tempdir_in(".")
        .expect("owned relative database fixture");
    let relative = std::path::PathBuf::from(directory.path().file_name().expect("relative root"))
        .join("not-yet-created");
    assert!(!relative.is_absolute());
    assert!(relative.to_string_lossy().starts_with("db"));
    assert!(
        !relative.exists(),
        "the connection must create its local directory"
    );
    let absolute = std::path::absolute(&relative).expect("absolute fixture identity");
    let locator = StorageLocator::new("relative_path_proof");
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
        let configuration = DatabaseConfig::lance(LanceConfig::embedded_path(&relative));
        let fixture = DatabaseCase {
            database: Database::connect(configuration.clone())
                .await
                .expect("relative db-prefix filesystem connection"),
            configuration,
            mode: Mode::Lance,
            locator: locator.clone(),
        };
        let DatabaseConfig::Lance { config, .. } = &fixture.configuration else {
            panic!("filesystem fixture retains its selected configuration");
        };
        assert_eq!(config.path, relative, "caller configuration stays relative");
        fixture.seed(3).await;
        let expected = fixture.snapshot().await;
        drop(fixture);
        assert!(
            absolute.is_dir(),
            "the supplied filesystem path was created"
        );
        let configuration = DatabaseConfig::lance(LanceConfig::embedded_path(&absolute));
        let reopened = DatabaseCase {
            database: Database::connect(configuration.clone())
                .await
                .expect("fresh absolute-path connection"),
            configuration,
            mode: Mode::Lance,
            locator,
        };
        assert_eq!(reopened.snapshot().await, expected);
        drop(reopened);
    }))
    .catch_unwind()
    .await;
    let cleanup = directory.close();
    assert!(
        matches!(observed, Ok(Ok(()))),
        "relative local path and absolute reopen: {observed:?}; cleanup: {cleanup:?}"
    );
    cleanup.expect("settled relative database directory");
}

#[tokio::test]
async fn lancedb_scan_deleted_rows_memory() {
    Mode::LanceMemory.cases(&[Case::ScanDeletedRows]).await;
}

#[tokio::test]
async fn lancedb_scan_deleted_rows_filesystem_reopen() {
    let directory = tempfile::tempdir().expect("isolated filesystem database");
    let configuration = Mode::Lance.configuration(directory.path());
    let locator = StorageLocator::new(format!("proof_{}", uuid::Uuid::new_v4().simple()));
    let observed = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(120),
        Box::pin(async {
            {
                let fixture = DatabaseCase {
                    database: tokio::time::timeout(
                        Duration::from_secs(30),
                        Database::connect(configuration.clone()),
                    )
                    .await
                    .expect("initial connection deadline")
                    .expect("initial connection"),
                    configuration: configuration.clone(),
                    mode: Mode::Lance,
                    locator: locator.clone(),
                };
                fixture.seed_scan_deleted_rows().await;
                fixture.assert_scan_deleted_rows().await;
                drop(fixture);
            }
            // No previous DatabaseCase or Database owner survives this boundary.
            let fixture = DatabaseCase {
                database: tokio::time::timeout(
                    Duration::from_secs(30),
                    Database::connect(configuration.clone()),
                )
                .await
                .expect("reopen connection deadline")
                .expect("reopen completed deletes"),
                configuration: configuration.clone(),
                mode: Mode::Lance,
                locator: locator.clone(),
            };
            fixture.assert_scan_deleted_rows().await;
        }),
    ))
    .catch_unwind()
    .await;
    let cleanup = tokio::time::timeout(Duration::from_secs(30), async {
        let database = Database::connect(configuration).await?;
        database
            .catalog(CatalogRequest::DropCollection {
                locator,
                ignore_missing: true,
            })
            .await
    })
    .await;
    let directory_cleanup = directory.close();
    assert!(
        matches!(observed, Ok(Ok(()))),
        "deleted-row reopen proof: {observed:?}; cleanup: {cleanup:?}; directory cleanup: {directory_cleanup:?}"
    );
    assert!(
        matches!(cleanup, Ok(Ok(_))),
        "collection cleanup: {cleanup:?}"
    );
    directory_cleanup.expect("directory cleanup");
}

#[tokio::test]
async fn lancedb_sql_read_columns_rejection_preserves_collection() {
    for mode in [Mode::Lance, Mode::LanceMemory] {
        mode.cases(&[Case::SqlReadColumnsRejection]).await;
    }
}

#[tokio::test]
async fn lancedb_sql_backfill_is_eager() {
    for mode in [Mode::Lance, Mode::LanceMemory] {
        mode.cases(&[Case::SqlBackfill]).await;
    }
}

#[tokio::test]
async fn lancedb_sql_backfill_persists_after_reopen() {
    let directory = tempfile::tempdir().expect("isolated filesystem database");
    let configuration = Mode::Lance.configuration(directory.path());
    let locator = StorageLocator::new(format!("proof_{}", uuid::Uuid::new_v4().simple()));
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(120), async {
        let before_reopen = {
            let fixture = DatabaseCase {
                database: Database::connect(configuration.clone())
                    .await
                    .expect("initial connection"),
                configuration: configuration.clone(),
                mode: Mode::Lance,
                locator: locator.clone(),
            };
            fixture.seed_sql_backfill().await;
            fixture.scalar_marker_index().await;
            fixture.reject_sql_read_columns().await;
            let snapshot = fixture.snapshot().await;
            drop(fixture);
            snapshot
        };
        let backfilled = {
            let fixture = DatabaseCase {
                database: Database::connect(configuration.clone())
                    .await
                    .expect("reopen rejected request"),
                configuration: configuration.clone(),
                mode: Mode::Lance,
                locator: locator.clone(),
            };
            assert_eq!(
                fixture.snapshot().await,
                before_reopen,
                "rejected option preserves durable state"
            );
            fixture.assert_sql_backfill().await;
            let snapshot = fixture.snapshot().await;
            drop(fixture);
            snapshot
        };
        // Every previous Database owner has been dropped before this connection.
        let fixture = DatabaseCase {
            database: Database::connect(configuration.clone())
                .await
                .expect("reopen completed backfill"),
            configuration: configuration.clone(),
            mode: Mode::Lance,
            locator: locator.clone(),
        };
        assert_eq!(
            fixture.snapshot().await,
            backfilled,
            "derived values and complete schema persist"
        );
        fixture.assert_count(4).await;
    }))
    .catch_unwind()
    .await;
    let cleanup = tokio::time::timeout(Duration::from_secs(30), async {
        let database = Database::connect(configuration).await?;
        database
            .catalog(CatalogRequest::DropCollection {
                locator,
                ignore_missing: true,
            })
            .await
    })
    .await;
    let directory_cleanup = directory.close();
    assert!(
        matches!(observed, Ok(Ok(()))),
        "reopen proof failed: {observed:?}; cleanup: {cleanup:?}; directory cleanup: {directory_cleanup:?}"
    );
    assert!(
        matches!(cleanup, Ok(Ok(_))),
        "collection cleanup: {cleanup:?}"
    );
    directory_cleanup.expect("directory cleanup");
}

#[tokio::test]
async fn lancedb_quoted_filters_preserve_identifiers_and_literals() {
    let mut failures = Vec::new();
    for mode in [Mode::Lance, Mode::LanceMemory] {
        if AssertUnwindSafe(mode.cases(&[
            Case::QuotedFilters {
                field: "space field",
            },
            Case::QuotedFilters {
                field: "double\"quote",
            },
        ]))
        .catch_unwind()
        .await
        .is_err()
        {
            failures.push(format!("{mode:?}"));
        }
    }
    assert!(failures.is_empty(), "quoted filters failed in {failures:?}");
}

#[tokio::test]
async fn lancedb_partition_replacement_reports_exact_counts() {
    for mode in [Mode::Lance, Mode::LanceMemory] {
        mode.cases(&[Case::PartitionExactCounts]).await;
    }
}

#[tokio::test]
async fn lancedb_json_values_round_trip() {
    Mode::Lance.cases(&[Case::JsonRoundTrip]).await;
}

#[tokio::test]
async fn lancedb_memory_mutation_owner_controls() {
    Mode::LanceMemory
        .cases(&[
            Case::Data,
            Case::Partition,
            Case::PartitionNativeFailure,
            Case::PartitionCollision,
            Case::RejectedOverwrite {
                unsupported_id: false,
            },
        ])
        .await;
}

#[tokio::test]
async fn lancedb_independent_database_mutation_controls() {
    tokio::join!(
        Mode::Lance.cases(&[Case::Partition, Case::PartitionNativeFailure]),
        Mode::Lance.cases(&[Case::Partition, Case::PartitionNativeFailure]),
    );
}

#[tokio::test]
async fn lancedb_catalog_data_schema() {
    Mode::Lance.cases(&Case::CATALOG_DATA).await;
}
#[tokio::test]
async fn lancedb_query_index() {
    Mode::Lance.cases(&Case::QUERY_INDEX).await;
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_memory_catalog_data_schema() {
    Mode::surreal_cases(&[Mode::SurrealMemory], &Case::CATALOG_DATA);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_memory_query_index() {
    Mode::surreal_cases(&[Mode::SurrealMemory], &Case::QUERY_INDEX);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_rocksdb_catalog_data_schema() {
    Mode::surreal_cases(&[Mode::SurrealRocks], &Case::CATALOG_DATA);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_rocksdb_query_index() {
    Mode::surreal_cases(&[Mode::SurrealRocks], &Case::QUERY_INDEX);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_surrealkv_catalog_data_schema() {
    Mode::surreal_cases(&[Mode::SurrealKv], &Case::CATALOG_DATA);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_surrealkv_query_index() {
    Mode::surreal_cases(&[Mode::SurrealKv], &Case::QUERY_INDEX);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_remote_http_catalog_data_schema() {
    Mode::surreal_cases(&[Mode::SurrealHttp], &Case::CATALOG_DATA);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_remote_http_query_index() {
    Mode::surreal_cases(&[Mode::SurrealHttp], &Case::QUERY_INDEX);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_remote_websocket_catalog_data_schema() {
    Mode::surreal_cases(&[Mode::SurrealWs], &Case::CATALOG_DATA);
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_remote_websocket_query_index() {
    Mode::surreal_cases(&[Mode::SurrealWs], &Case::QUERY_INDEX);
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_edge_catalog_data_schema() {
    Mode::QdrantEdge.cases(&Case::CATALOG_DATA).await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_edge_query_index() {
    Mode::QdrantEdge.cases(&Case::QUERY_INDEX).await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_remote_rest_catalog_data_schema() {
    Mode::QdrantRest.cases(&Case::CATALOG_DATA).await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_remote_rest_query_index() {
    Mode::QdrantRest.cases(&Case::QUERY_INDEX).await;
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_vector_payload_score_collision() {
    Mode::surreal_cases(
        &Mode::SURREAL,
        &[
            Case::ScoreCollision {
                by_id: false,
                projected: false,
                numeric_payload: true,
            },
            Case::ScoreCollision {
                by_id: false,
                projected: true,
                numeric_payload: true,
            },
            Case::ScoreCollision {
                by_id: true,
                projected: false,
                numeric_payload: true,
            },
            Case::ScoreCollision {
                by_id: true,
                projected: true,
                numeric_payload: true,
            },
            Case::ScoreCollision {
                by_id: false,
                projected: false,
                numeric_payload: false,
            },
            Case::ScoreCollision {
                by_id: false,
                projected: true,
                numeric_payload: false,
            },
            Case::ScoreCollision {
                by_id: true,
                projected: false,
                numeric_payload: false,
            },
            Case::ScoreCollision {
                by_id: true,
                projected: true,
                numeric_payload: false,
            },
        ],
    );
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_overwrite_replaces_collection_contents() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::Overwrite]);
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_overwrite_replaces_collection_contents() {
    Mode::QdrantEdge.cases(&[Case::Overwrite]).await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_edge_unsupported_overwrite_preserves_collection() {
    Mode::QdrantEdge
        .cases(&[
            Case::RejectedOverwrite {
                unsupported_id: false,
            },
            Case::RejectedOverwrite {
                unsupported_id: true,
            },
            Case::RejectedDimensionsOverwrite,
        ])
        .await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_remote_rest_unsupported_overwrite_preserves_collection() {
    Mode::QdrantRest
        .cases(&[
            Case::RejectedOverwrite {
                unsupported_id: false,
            },
            Case::RejectedOverwrite {
                unsupported_id: true,
            },
            Case::RejectedDimensionsOverwrite,
        ])
        .await;
}
#[tokio::test]
async fn lancedb_unsupported_overwrite_preserves_collection() {
    Mode::Lance
        .cases(&[Case::RejectedOverwrite {
            unsupported_id: false,
        }])
        .await;
}
#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_fresh_fulltext_index_and_query() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::FullTextIndex]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_rejected_schema_overwrite_preserves_collection() {
    Mode::surreal_cases(
        &Mode::SURREAL,
        &[
            Case::RejectedSchemaOverwrite { invalid_name: true },
            Case::RejectedSchemaOverwrite {
                invalid_name: false,
            },
        ],
    );
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_unsupported_overwrite_preserves_collection() {
    Mode::surreal_cases(
        &Mode::SURREAL,
        &[Case::RejectedOverwrite {
            unsupported_id: false,
        }],
    );
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_repeated_fulltext_collection_drop() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::FullTextCollectionDrop]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_fulltext_payload_score_collision() {
    Mode::surreal_cases(
        &Mode::SURREAL,
        &[
            Case::TextScoreCollision {
                projected: false,
                numeric_payload: true,
            },
            Case::TextScoreCollision {
                projected: true,
                numeric_payload: true,
            },
            Case::TextScoreCollision {
                projected: false,
                numeric_payload: false,
            },
            Case::TextScoreCollision {
                projected: true,
                numeric_payload: false,
            },
        ],
    );
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_unscored_queries_preserve_score_payload() {
    Mode::surreal_cases(
        &Mode::SURREAL,
        &[
            Case::PlainScoreCollision { sample: false },
            Case::PlainScoreCollision { sample: true },
        ],
    );
}

struct LanceObserverLoss {
    child: std::process::Child,
}

impl LanceObserverLoss {
    fn supervise(test: &str) {
        let directory = tempfile::tempdir().expect("observer-loss directory");
        let output_path = directory.path().join("child.log");
        let output = std::fs::File::create(&output_path).expect("child log");
        let child =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .args([test, "--exact", "--nocapture"])
                .env(
                    "STORAGE_SDK_LANCE_OBSERVER_CHILD",
                    directory.path().join("database"),
                )
                .stdin(std::process::Stdio::null())
                .stdout(output.try_clone().expect("child stdout"))
                .stderr(output)
                .spawn()
                .expect("owned observer-loss subprocess");
        let mut owner = Self { child };
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = owner.child.try_wait().expect("observe child") {
                break Some(status);
            }
            if std::time::Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        drop(owner); // Kill and join on timeout before reporting the assertion.
        let log = std::fs::read_to_string(output_path).expect("child output");
        assert!(
            status.is_some_and(|status| status.success()),
            "owned replacement did not settle: {status:?}\n{log}"
        );
        directory.close().expect("observer-loss directory cleanup");
    }

    fn exercise(path: std::path::PathBuf) {
        use std::{future::Future, task::Poll};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("original runtime");
        let config = DatabaseConfig::lance(LanceConfig::embedded_path(path));
        let fixture = runtime.block_on(async {
            let fixture = DatabaseCase {
                database: Database::connect(config.clone())
                    .await
                    .expect("original database"),
                configuration: config.clone(),
                mode: Mode::Lance,
                locator: StorageLocator::new("observer_loss"),
            };
            fixture.seed(3).await;
            fixture
        });
        let request = ReplacePartitionRequest::new(
            fixture.locator.clone(),
            FilterExpr::eq("partition", "a"),
            "marker",
            vec![fixture.record(0).json_field("status", json!("settled"))],
        )
        .expect("replacement request");
        let mut observation = Box::pin(
            fixture
                .database
                .data(DataRequest::ReplacePartition(request)),
        );
        let first = runtime.block_on(std::future::poll_fn(|context| {
            Poll::Ready(observation.as_mut().poll(context))
        }));
        drop(observation);
        if let Poll::Ready(result) = first {
            result.expect("already completed replacement");
        }
        let locator = fixture.locator.clone();
        drop(fixture);
        // The original runtime is deliberately alive but no longer progressing.
        // Last-owner cleanup must not depend on polling that caller's runtime.
        let verifier = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("independent verifier runtime");
        let rows = verifier.block_on(async {
            let database = Database::connect(config.clone())
                .await
                .expect("reopen after final owner drop");
            let DataResponse::Records(rows) = database
                .data(DataRequest::Scan(ScanRequest::new(locator.clone())))
                .await
                .expect("read settled contents")
            else {
                panic!("records")
            };
            rows.records
        });
        drop(verifier);
        let after = runtime.block_on(async {
            let database = Database::connect(config)
                .await
                .expect("public mutation barrier database");
            let unaffected = rows
                .iter()
                .find(|row| row.fields["marker"] == "row-0002")
                .expect("unrelated row")
                .clone()
                .json_field("status", json!("barrier"));
            database
                .data(DataRequest::ReplacePartition(
                    ReplacePartitionRequest::new(
                        locator.clone(),
                        FilterExpr::eq("partition", "b"),
                        "marker",
                        vec![unaffected],
                    )
                    .expect("barrier request"),
                ))
                .await
                .expect("later unrelated public mutation");
            let DataResponse::Records(after) = database
                .data(DataRequest::Scan(ScanRequest::new(locator)))
                .await
                .expect("read after original runtime resumes")
            else {
                panic!("records")
            };
            after.records
        });
        drop(runtime);
        // Pending does not establish admission: either the complete old or new
        // partition may remain. Neither may change later when its old caller
        // runtime is resumed. The unrelated mutation creates observable forward
        // progress without asserting a private task count or lock filename.
        assert!(matches!(rows.len(), 2 | 3), "replacement remains atomic");
        let mut before_partition = rows
            .into_iter()
            .filter(|row| row.fields["partition"] == "a")
            .collect::<Vec<_>>();
        let mut after_partition = after
            .into_iter()
            .filter(|row| row.fields["partition"] == "a")
            .collect::<Vec<_>>();
        before_partition.sort_by_key(|row| row.fields["marker"].to_string());
        after_partition.sort_by_key(|row| row.fields["marker"].to_string());
        assert_eq!(
            after_partition, before_partition,
            "resuming a dropped caller runtime cannot publish late abandoned work after the last database owner disappeared"
        );
    }

    fn lock_deadline(path: std::path::PathBuf) {
        use std::{future::Future, task::Poll};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("paused caller runtime");
        let config = DatabaseConfig::lance(LanceConfig::embedded_path(path));
        let fixture = runtime.block_on(async {
            let fixture = DatabaseCase {
                database: Database::connect(config.clone())
                    .await
                    .expect("first database"),
                configuration: config.clone(),
                mode: Mode::Lance,
                locator: StorageLocator::new("admission"),
            };
            fixture.seed(3).await;
            fixture
        });
        let request = ReplacePartitionRequest::new(
            fixture.locator.clone(),
            FilterExpr::eq("partition", "a"),
            "marker",
            vec![fixture.record(0)],
        )
        .expect("first replacement");
        let mut first = Box::pin(
            fixture
                .database
                .data(DataRequest::ReplacePartition(request.clone())),
        );
        let initial = runtime.block_on(std::future::poll_fn(|context| {
            Poll::Ready(first.as_mut().poll(context))
        }));
        if let Poll::Ready(result) = initial {
            result.expect("first replacement already completed");
        }
        let independent = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("independent waiter runtime");
        let second_request = ReplacePartitionRequest::new(
            fixture.locator.clone(),
            FilterExpr::eq("partition", "b"),
            "marker",
            vec![fixture.record(2)],
        )
        .expect("second replacement");
        let result = independent.block_on(async {
            let database = Database::connect(config.with_common(CommonConfig {
                request_timeout_ms: 100,
                ..CommonConfig::default()
            }))
            .await
            .expect("deadline-bound second database");
            tokio::time::timeout(
                Duration::from_secs(2),
                database.data(DataRequest::ReplacePartition(second_request)),
            )
            .await
        });
        drop(independent);
        drop(first);
        // Settle the original native work before diagnosing the deadline. The
        // watchdog process owns cleanup if this public barrier itself hangs.
        runtime
            .block_on(
                fixture
                    .database
                    .data(DataRequest::ReplacePartition(request)),
            )
            .expect("settle original writer through public mutation");
        drop(fixture);
        drop(runtime);
        match result.expect("configured admission/lock deadline must beat the outer watchdog even when another caller runtime stops progressing") {
            Ok(DataResponse::Mutation(_)) => {},
            Err(StorageError::BackendOperation { message, .. }) if message.contains("timed out") => {},
            other => panic!("expected actual completion or SDK admission deadline: {other:?}"),
        }
    }
}

impl Drop for LanceObserverLoss {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn lancedb_observer_loss_settles_before_last_owner_drop() {
    match std::env::var_os("STORAGE_SDK_LANCE_OBSERVER_CHILD") {
        Some(path) => LanceObserverLoss::exercise(path.into()),
        None => LanceObserverLoss::supervise(
            "database_cases::lancedb_observer_loss_settles_before_last_owner_drop",
        ),
    }
}

#[test]
fn lancedb_waiting_mutation_respects_configured_deadline() {
    match std::env::var_os("STORAGE_SDK_LANCE_OBSERVER_CHILD") {
        Some(path) => LanceObserverLoss::lock_deadline(path.into()),
        None => LanceObserverLoss::supervise(
            "database_cases::lancedb_waiting_mutation_respects_configured_deadline",
        ),
    }
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_edge_empty_writes_preserve_existing_collection() {
    Mode::QdrantEdge.cases(&[Case::EmptyWriteIdentity]).await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_remote_rest_empty_writes_preserve_existing_collection() {
    Mode::QdrantRest.cases(&[Case::EmptyWriteIdentity]).await;
}

#[tokio::test]
async fn lancedb_explicit_json_column_preserves_null_and_absence() {
    Mode::Lance.cases(&[Case::JsonExplicitRoundTrip]).await;
}

#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_remote_rest_vector_dimension_limit_preserves_rejected_overwrite() {
    Mode::QdrantRest
        .cases(&[Case::RestVectorLimit { exceeds: true }])
        .await;
}
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_remote_rest_maximum_vector_dimension_is_supported() {
    Mode::QdrantRest
        .cases(&[Case::RestVectorLimit { exceeds: false }])
        .await;
}

#[tokio::test]
async fn lancedb_json_scalar_string_equality_matches_portable_value() {
    Mode::Lance
        .cases(&[Case::ScalarStringEquality { json_column: true }])
        .await;
}
#[tokio::test]
async fn lancedb_string_equality_matches_portable_value() {
    Mode::Lance
        .cases(&[Case::ScalarStringEquality { json_column: false }])
        .await;
}

#[tokio::test]
async fn lancedb_json_column_scalar_only_append_preserves_declared_type() {
    Mode::Lance
        .cases(&[Case::ScalarStringAppend { json_column: true }])
        .await;
}
#[tokio::test]
async fn lancedb_string_column_scalar_only_append_preserves_declared_type() {
    Mode::Lance
        .cases(&[Case::ScalarStringAppend { json_column: false }])
        .await;
}

#[tokio::test]
async fn lancedb_json_column_scalar_update_preserves_declared_type() {
    Mode::Lance
        .cases(&[Case::ScalarStringUpdate { json_column: true }])
        .await;
}
#[tokio::test]
async fn lancedb_string_column_scalar_update_preserves_declared_type() {
    Mode::Lance
        .cases(&[Case::ScalarStringUpdate { json_column: false }])
        .await;
}

#[tokio::test]
async fn lancedb_json_greater_predicate_contract() {
    Mode::Lance
        .cases(&[Case::ScalarStringPredicate {
            json_column: true,
            predicate: ScalarStringPredicate::Greater,
        }])
        .await;
}

#[tokio::test]
async fn lancedb_string_greater_predicate_contract() {
    Mode::Lance
        .cases(&[Case::ScalarStringPredicate {
            json_column: false,
            predicate: ScalarStringPredicate::Greater,
        }])
        .await;
}

#[tokio::test]
async fn lancedb_json_between_predicate_contract() {
    Mode::Lance
        .cases(&[Case::ScalarStringPredicate {
            json_column: true,
            predicate: ScalarStringPredicate::Between,
        }])
        .await;
}

#[tokio::test]
async fn lancedb_string_between_predicate_contract() {
    Mode::Lance
        .cases(&[Case::ScalarStringPredicate {
            json_column: false,
            predicate: ScalarStringPredicate::Between,
        }])
        .await;
}

#[tokio::test]
async fn lancedb_json_like_predicate_contract() {
    Mode::Lance
        .cases(&[Case::ScalarStringPredicate {
            json_column: true,
            predicate: ScalarStringPredicate::Prefix,
        }])
        .await;
}

#[tokio::test]
async fn lancedb_string_like_predicate_contract() {
    Mode::Lance
        .cases(&[Case::ScalarStringPredicate {
            json_column: false,
            predicate: ScalarStringPredicate::Prefix,
        }])
        .await;
}

#[tokio::test]
async fn lancedb_memory_catalog_data_schema() {
    Mode::LanceMemory.cases(&Case::CATALOG_DATA).await;
}

#[tokio::test]
async fn lancedb_memory_query_index() {
    Mode::LanceMemory.cases(&Case::QUERY_INDEX).await;
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_sq_prefixed_collection_lifecycle() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::SqCollection]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_filtered_indexed_vectors_are_exact() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::FilteredIndexedVectors]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_fulltext_removal_after_update() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::FullTextUpdate]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_fulltext_removal_after_partition_replacement() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::FullTextPartition]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_concurrent_first_use_scopes_are_isolated() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::ConcurrentScopes]);
}

#[cfg(feature = "surrealdb")]
#[test]
fn surrealdb_nearest_preserves_validation_order() {
    Mode::surreal_cases(&Mode::SURREAL, &[Case::NearestValidationOrder]);
}
