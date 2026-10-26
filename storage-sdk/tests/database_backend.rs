use std::sync::Mutex;

use storage_sdk::{
    Database, DatabaseBackend, StorageError,
    database::{
        CapabilitySet, StorageLocator,
        catalog::{
            CatalogRequest, CatalogResponse, CollectionInfo, CollectionSchema,
            CreateCollectionMode, CreateCollectionRequest, FieldSchema, FieldType,
            ListCollectionsRequest,
        },
        config::{DatabaseConfig, LanceConfig},
        data::{CountRequest, DataRecord, DataRequest, DataResponse, FilterExpr, PageToken},
        index::{CreateIndexRequest, IndexKind, IndexRequest, IndexResponse},
        query::{Projection, QueryRequest, QueryResponse, ScoreKind, ScoredRecord, ScrollRequest},
    },
};

struct RecordingDatabase {
    calls: Mutex<Vec<&'static str>>,
}

#[tokio::test]
async fn lance_memory_catalog_supports_namespaces_and_isolates_connections() {
    let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        "memory://",
    )))
    .await
    .expect("connect memory database");
    let locators = [
        (StorageLocator::new("managed_knowledge"), 1),
        (
            StorageLocator::new("managed_knowledge")
                .with_namespace(["agent_sdk"])
                .with_database("fixture-agent"),
            2,
        ),
    ];
    for (locator, count) in &locators {
        let request = CreateCollectionRequest {
            locator: locator.clone(),
            schema: Some(CollectionSchema {
                fields: vec![FieldSchema {
                    name: "text".to_owned(),
                    field_type: FieldType::String,
                    nullable: false,
                    metadata: Default::default(),
                }],
                metadata: Default::default(),
            }),
            records: (0..*count)
                .map(|row| {
                    DataRecord::without_id()
                        .json_field("text", serde_json::json!(format!("retained-{row}")))
                })
                .collect(),
            mode: CreateCollectionMode::CreateIfMissing,
        };
        database
            .catalog(CatalogRequest::CreateCollection(request))
            .await
            .expect("root and namespaced memory collections must be created");
        database
            .index(IndexRequest::Create(CreateIndexRequest {
                locator: locator.clone(),
                columns: vec!["text".to_owned()],
                kind: IndexKind::Scalar,
                wait_timeout_ms: None,
            }))
            .await
            .expect("memory index must retain its table bytes");
    }
    let cloned = database.clone();
    drop(database);
    for (locator, count) in locators {
        assert_eq!(
            cloned
                .data(DataRequest::Count(CountRequest {
                    locator: locator.clone(),
                    filter: None,
                }))
                .await
                .expect("cloned owner retains each namespace and indexed table"),
            DataResponse::Count(count)
        );
        assert_eq!(
            cloned
                .data(DataRequest::Count(CountRequest {
                    locator,
                    filter: Some(FilterExpr::eq("text", "retained-0")),
                }))
                .await
                .expect("filter indexed memory collection"),
            DataResponse::Count(1),
        );
    }
    let independent = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        "memory://",
    )))
    .await
    .expect("connect independent memory database");
    let isolated = independent
        .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
            namespace: vec!["agent_sdk".to_owned(), "fixture-agent".to_owned()],
            limit: None,
            start_after: None,
        }))
        .await
        .expect("independent memory catalog remains readable");
    assert_eq!(isolated, CatalogResponse::Collections(Vec::new()));
}

#[tokio::test]
async fn built_in_database_validates_before_dispatch() {
    let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        "memory://",
    )))
    .await
    .expect("in-memory Lance connection");
    let error = database
        .index(IndexRequest::Create(CreateIndexRequest {
            locator: StorageLocator::new("documents"),
            columns: Vec::new(),
            kind: IndexKind::Scalar,
            wait_timeout_ms: None,
        }))
        .await
        .expect_err("empty index columns must be rejected");
    assert!(matches!(error, StorageError::InvalidRequest(_)));
}

impl DatabaseBackend for RecordingDatabase {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::DENSE_VECTOR | CapabilitySet::FILTERING
    }

    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        self.calls.lock().expect("call log").push("catalog");
        let CatalogRequest::ListCollections(request) = request else {
            panic!("list request")
        };
        assert_eq!(request.namespace, ["tenant"]);
        Ok(CatalogResponse::Collections(vec![CollectionInfo {
            locator: StorageLocator::new("documents")
                .with_namespace(["tenant"])
                .with_database("app"),
            schema: None,
            record_count: Some(7),
        }]))
    }

    async fn data(&self, _request: DataRequest) -> Result<DataResponse, StorageError> {
        self.calls.lock().expect("call log").push("data");
        Ok(DataResponse::Count(7))
    }

    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
        self.calls.lock().expect("call log").push("query");
        let QueryRequest::Scroll(request) = request else {
            panic!("scroll request")
        };
        assert_eq!(request.locator.namespace, ["tenant"]);
        assert_eq!(request.locator.database.as_deref(), Some("app"));
        assert_eq!(request.locator.collection, "documents");
        let first = match request.page_token {
            None => true,
            Some(token) => {
                assert_eq!(token.as_str(), "caller-page-2");
                false
            }
        };
        Ok(QueryResponse {
            hits: vec![ScoredRecord {
                record: DataRecord::new(if first { "doc-1" } else { "doc-2" })
                    .json_field("text", serde_json::json!("caller knowledge")),
                raw_score: Some(0.875),
                score_kind: ScoreKind::Similarity,
            }],
            next_page: first.then(|| PageToken::new("caller-page-2")),
        })
    }

    async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        self.calls.lock().expect("call log").push("index");
        match request {
            IndexRequest::List { .. } => Ok(IndexResponse::Indexes(Vec::new())),
            IndexRequest::Create(_) => Err(StorageError::unsupported_capability(
                "SCALAR_INDEX",
                "caller rejects creation",
            )),
            IndexRequest::Drop { .. } | IndexRequest::WaitFor(_) => {
                panic!("unexpected index operation")
            }
        }
    }
}

#[tokio::test]
async fn downstream_database_implements_every_retained_operation() {
    let database = RecordingDatabase {
        calls: Mutex::new(Vec::new()),
    };
    let locator = StorageLocator::new("documents")
        .with_namespace(["tenant"])
        .with_database("app");

    assert_eq!(
        database.capabilities(),
        CapabilitySet::DENSE_VECTOR | CapabilitySet::FILTERING
    );

    let catalog = database
        .catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
            namespace: vec!["tenant".to_owned()],
            limit: None,
            start_after: None,
        }))
        .await
        .expect("catalog response");
    let CatalogResponse::Collections(collections) = catalog else {
        panic!("collection response")
    };
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].locator, locator);
    assert_eq!(collections[0].record_count, Some(7));

    let data = database
        .data(DataRequest::Count(CountRequest {
            locator: locator.clone(),
            filter: None,
        }))
        .await
        .expect("data response");
    assert_eq!(data, DataResponse::Count(7));

    let query = database
        .query(QueryRequest::Scroll(ScrollRequest {
            locator: locator.clone(),
            filter: None,
            projection: Projection::All,
            limit: Some(1),
            offset: None,
            page_token: None,
        }))
        .await
        .expect("query response");
    assert_eq!(query.hits.len(), 1);
    assert_eq!(
        query.hits[0]
            .record
            .id
            .as_ref()
            .expect("id")
            .as_string_lossy(),
        "doc-1"
    );
    assert_eq!(
        query.hits[0].record.fields["text"],
        serde_json::json!("caller knowledge")
    );
    assert_eq!(query.hits[0].raw_score, Some(0.875));
    assert_eq!(query.hits[0].score_kind, ScoreKind::Similarity);
    let continuation = query.next_page.expect("continuation");
    assert_eq!(continuation.as_str(), "caller-page-2");
    let final_page = database
        .query(QueryRequest::Scroll(ScrollRequest {
            locator: locator.clone(),
            filter: None,
            projection: Projection::All,
            limit: Some(1),
            offset: None,
            page_token: Some(continuation),
        }))
        .await
        .expect("second page");
    assert_eq!(
        final_page.hits[0]
            .record
            .id
            .as_ref()
            .expect("id")
            .as_string_lossy(),
        "doc-2"
    );
    assert!(final_page.next_page.is_none());

    let index = database
        .index(IndexRequest::List {
            locator: locator.clone(),
        })
        .await
        .expect("index response");
    assert_eq!(index, IndexResponse::Indexes(Vec::new()));
    let unsupported = database
        .index(IndexRequest::Create(CreateIndexRequest {
            locator,
            columns: vec!["title".to_owned()],
            kind: IndexKind::Scalar,
            wait_timeout_ms: None,
        }))
        .await
        .expect_err("caller rejects unsupported creation");
    assert!(
        matches!(unsupported, StorageError::UnsupportedCapability { capability, .. } if capability == "SCALAR_INDEX")
    );

    assert_eq!(
        database.calls.into_inner().expect("call log"),
        vec!["catalog", "data", "query", "query", "index", "index"]
    );
}
