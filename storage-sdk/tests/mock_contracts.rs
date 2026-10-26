//! Direct caller implementations; no supplied database or storage is constructed.

use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use storage_sdk::{
    DatabaseBackend, StateStoreBackend, StateStoreError, StateTransactionBackend, StorageError,
    database::{
        CapabilitySet, StorageLocator,
        catalog::{
            AddColumnsRequest, AlterColumnsRequest, CatalogRequest, CatalogResponse,
            CollectionInfo, ColumnAlterationSpec, ComputedColumn, CreateCollectionMode,
            CreateCollectionRequest, DropColumnsRequest, ListCollectionsRequest,
            SchemaEvolutionRequest,
        },
        data::{
            CountRequest, DataRecord, DataRequest, DataResponse, DeleteRequest, DenseVector,
            FilterExpr, InsertRequest, LookupRequest, MergeActions, MergeRequest, MutationResult,
            PageToken, RecordsPage, ReplacePartitionRequest, ScanRequest, UpdateAssignment,
            UpdateRequest,
        },
        index::{
            CreateIndexRequest, IndexInfo, IndexKind, IndexRequest, IndexResponse,
            WaitForIndexRequest,
        },
        query::{
            NearestRequest, Projection, QueryRequest, QueryResponse, RandomSampleRequest,
            ScopedQuery, ScoreKind, ScoredRecord, ScrollRequest, SearchByIdRequest,
            TextSearchRequest,
        },
    },
    state::record::{
        StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord,
        StateRecordSnapshot, StateRevision,
    },
};
use tokio::sync::Notify;

struct Script<Q, R> {
    required: VecDeque<(Q, Result<R, StorageError>)>,
    observed: Vec<Q>,
}

impl<Q: Clone + PartialEq, R> Script<Q, R> {
    fn new(required: Vec<(Q, Result<R, StorageError>)>) -> Self {
        Self {
            required: required.into(),
            observed: Vec::new(),
        }
    }

    fn take(&mut self, request: Q) -> Result<R, StorageError> {
        let Some((expected, _)) = self.required.front() else {
            return Err(StorageError::InvalidRequest(
                "unexpected call: script exhausted".into(),
            ));
        };
        if expected != &request {
            return Err(StorageError::InvalidRequest(
                "wrong arguments: expectation retained".into(),
            ));
        }
        self.observed.push(request);
        self.required.pop_front().expect("checked expectation").1
    }

    fn finish(&self) -> Result<(), StorageError> {
        if self.required.is_empty() {
            Ok(())
        } else {
            Err(StorageError::InvalidRequest(
                "unconsumed required expectation".into(),
            ))
        }
    }
}

struct ScriptDatabase {
    catalog: Mutex<Script<CatalogRequest, CatalogResponse>>,
    data: Mutex<Script<DataRequest, DataResponse>>,
    query: Mutex<Script<QueryRequest, QueryResponse>>,
    index: Mutex<Script<IndexRequest, IndexResponse>>,
}

impl ScriptDatabase {
    fn full() -> Self {
        Self {
            catalog: Mutex::new(Script::new(DatabaseCases::catalog())),
            data: Mutex::new(Script::new(DatabaseCases::data())),
            query: Mutex::new(Script::new(DatabaseCases::query())),
            index: Mutex::new(Script::new(DatabaseCases::index())),
        }
    }

    fn finish(&self) {
        self.catalog.lock().unwrap().finish().unwrap();
        self.data.lock().unwrap().finish().unwrap();
        self.query.lock().unwrap().finish().unwrap();
        self.index.lock().unwrap().finish().unwrap();
    }
}

impl DatabaseBackend for ScriptDatabase {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::DENSE_VECTOR | CapabilitySet::FILTERING
    }
    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        self.catalog.lock().unwrap().take(request)
    }
    async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        self.data.lock().unwrap().take(request)
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
        self.query.lock().unwrap().take(request)
    }
    async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        self.index.lock().unwrap().take(request)
    }
}

enum CallerDatabase {
    First(ScriptDatabase),
    Second(ScriptDatabase),
}

impl DatabaseBackend for CallerDatabase {
    fn capabilities(&self) -> CapabilitySet {
        match self {
            Self::First(db) => db.capabilities(),
            Self::Second(db) => db.capabilities(),
        }
    }
    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        match self {
            Self::First(db) => db.catalog(request).await,
            Self::Second(db) => db.catalog(request).await,
        }
    }
    async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        match self {
            Self::First(db) => db.data(request).await,
            Self::Second(db) => db.data(request).await,
        }
    }
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
        match self {
            Self::First(db) => db.query(request).await,
            Self::Second(db) => db.query(request).await,
        }
    }
    async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        match self {
            Self::First(db) => db.index(request).await,
            Self::Second(db) => db.index(request).await,
        }
    }
}

struct DatabaseCases;

impl DatabaseCases {
    fn locator() -> StorageLocator {
        StorageLocator::new("records")
            .with_namespace(["tenant"])
            .with_database("application")
    }
    fn row() -> DataRecord {
        DataRecord::new("doc-1").json_field("text", serde_json::json!("retained"))
    }
    fn unsupported() -> StorageError {
        StorageError::unsupported_capability(
            "SCHEMA_EVOLUTION",
            "scripted request-specific rejection",
        )
    }
    fn catalog() -> Vec<(CatalogRequest, Result<CatalogResponse, StorageError>)> {
        let locator = Self::locator();
        let info = CollectionInfo {
            locator: locator.clone(),
            schema: None,
            record_count: Some(1),
        };
        vec![
            (
                CatalogRequest::CreateCollection(CreateCollectionRequest {
                    locator: locator.clone(),
                    schema: None,
                    records: vec![Self::row()],
                    mode: CreateCollectionMode::CreateIfMissing,
                }),
                Ok(CatalogResponse::Completed),
            ),
            (
                CatalogRequest::OpenCollection {
                    locator: locator.clone(),
                },
                Ok(CatalogResponse::Collection(info.clone())),
            ),
            (
                CatalogRequest::ListCollections(ListCollectionsRequest {
                    namespace: vec!["tenant".into()],
                    limit: Some(2),
                    start_after: Some("previous".into()),
                }),
                Ok(CatalogResponse::Collections(vec![info])),
            ),
            (
                CatalogRequest::AlterCollection(SchemaEvolutionRequest::DropColumns(
                    DropColumnsRequest {
                        locator: locator.clone(),
                        columns: vec!["obsolete".into()],
                    },
                )),
                Err(Self::unsupported()),
            ),
            (
                CatalogRequest::DropCollection {
                    locator,
                    ignore_missing: false,
                },
                Ok(CatalogResponse::Completed),
            ),
            (
                CatalogRequest::AlterCollection(SchemaEvolutionRequest::AddColumns(
                    AddColumnsRequest {
                        locator: Self::locator(),
                        expressions: vec![ComputedColumn {
                            name: "length".into(),
                            expression_sql: "length(text)".into(),
                        }],
                        read_columns: None,
                    },
                )),
                Ok(CatalogResponse::Completed),
            ),
            (
                CatalogRequest::AlterCollection(SchemaEvolutionRequest::AlterColumns(
                    AlterColumnsRequest {
                        locator: Self::locator(),
                        alterations: vec![ColumnAlterationSpec {
                            path: "text".into(),
                            rename: Some("body".into()),
                            nullable: None,
                            cast_to: None,
                        }],
                    },
                )),
                Ok(CatalogResponse::Completed),
            ),
        ]
    }
    fn data() -> Vec<(DataRequest, Result<DataResponse, StorageError>)> {
        let locator = Self::locator();
        let mutation = DataResponse::Mutation(MutationResult {
            inserted: Some(1),
            counts_exact: true,
            ..Default::default()
        });
        let records = DataResponse::Records(RecordsPage {
            records: vec![Self::row()],
            total: Some(3),
            next_page: Some(PageToken::new("opaque-next")),
        });
        vec![
            (
                DataRequest::Insert(InsertRequest::append(locator.clone(), vec![Self::row()])),
                Ok(mutation.clone()),
            ),
            (
                DataRequest::Insert(InsertRequest::overwrite(locator.clone(), vec![])),
                Ok(DataResponse::Mutation(MutationResult {
                    deleted: Some(1),
                    counts_exact: true,
                    ..Default::default()
                })),
            ),
            (
                DataRequest::Lookup(LookupRequest {
                    locator: locator.clone(),
                    ids: vec!["doc-1".into()],
                    projection: Projection::All,
                }),
                Ok(records.clone()),
            ),
            (
                DataRequest::Scan(ScanRequest {
                    locator: locator.clone(),
                    filter: Some(FilterExpr::eq("text", "retained")),
                    projection: Projection::All,
                    limit: Some(1),
                    offset: None,
                    page_token: Some(PageToken::new("opaque-previous")),
                }),
                Ok(records),
            ),
            (
                DataRequest::Update(UpdateRequest {
                    locator: locator.clone(),
                    filter: Some(FilterExpr::eq("id", "doc-1")),
                    assignments: vec![UpdateAssignment {
                        column: "text".into(),
                        value: serde_json::json!("new"),
                    }],
                }),
                Ok(DataResponse::Mutation(MutationResult {
                    updated: Some(1),
                    counts_exact: true,
                    ..Default::default()
                })),
            ),
            (
                DataRequest::Delete(DeleteRequest {
                    locator: locator.clone(),
                    filter: FilterExpr::eq("id", "doc-1"),
                }),
                Ok(DataResponse::Mutation(MutationResult {
                    deleted: Some(1),
                    counts_exact: true,
                    ..Default::default()
                })),
            ),
            (
                DataRequest::Merge(MergeRequest {
                    locator: locator.clone(),
                    on: vec!["id".into()],
                    records: vec![Self::row()],
                    actions: MergeActions {
                        update_matched: true,
                        insert_unmatched: true,
                        delete_not_matched_by_source: false,
                    },
                }),
                Ok(mutation.clone()),
            ),
            (
                DataRequest::ReplacePartition(ReplacePartitionRequest {
                    locator: locator.clone(),
                    partition: FilterExpr::eq("text", "retained"),
                    identity_field: "id".into(),
                    records: vec![Self::row()],
                }),
                Ok(mutation),
            ),
            (
                DataRequest::Count(CountRequest {
                    locator: locator.clone(),
                    filter: Some(FilterExpr::eq("text", "retained")),
                }),
                Ok(DataResponse::Count(7)),
            ),
            (
                DataRequest::Count(CountRequest {
                    locator,
                    filter: None,
                }),
                Err(StorageError::BackendOperation {
                    operation: "count".into(),
                    message: "preserved diagnostic".into(),
                }),
            ),
        ]
    }
    fn query() -> Vec<(QueryRequest, Result<QueryResponse, StorageError>)> {
        let locator = Self::locator();
        let scored = QueryResponse {
            hits: vec![ScoredRecord {
                record: Self::row(),
                raw_score: Some(0.75),
                score_kind: ScoreKind::Similarity,
            }],
            next_page: Some(PageToken::new("query-next")),
        };
        vec![
            (
                QueryRequest::Nearest(NearestRequest::new(
                    locator.clone(),
                    DenseVector::F32(vec![0.1, 0.2]),
                )),
                Ok(scored.clone()),
            ),
            (
                QueryRequest::SearchById(SearchByIdRequest {
                    query: ScopedQuery::vector(locator.clone()),
                    id: "doc-1".into(),
                }),
                Ok(scored.clone()),
            ),
            (
                QueryRequest::Text(TextSearchRequest {
                    query: ScopedQuery::text(locator.clone()).text_field("text"),
                    text: "retained".into(),
                }),
                Ok(scored.clone()),
            ),
            (
                QueryRequest::Scroll(ScrollRequest {
                    locator: locator.clone(),
                    filter: None,
                    projection: Projection::All,
                    limit: Some(1),
                    offset: None,
                    page_token: Some(PageToken::new("query-before")),
                }),
                Ok(QueryResponse {
                    hits: vec![ScoredRecord {
                        record: Self::row(),
                        raw_score: None,
                        score_kind: ScoreKind::Unknown,
                    }],
                    next_page: None,
                }),
            ),
            (
                QueryRequest::RandomSample(RandomSampleRequest {
                    locator,
                    filter: None,
                    projection: Projection::All,
                    limit: 1,
                }),
                Err(StorageError::unsupported_capability(
                    "RANDOM_SAMPLE",
                    "caller fixture",
                )),
            ),
        ]
    }
    fn index() -> Vec<(IndexRequest, Result<IndexResponse, StorageError>)> {
        let locator = Self::locator();
        vec![
            (
                IndexRequest::Create(CreateIndexRequest {
                    locator: locator.clone(),
                    columns: vec!["text".into()],
                    kind: IndexKind::Scalar,
                    wait_timeout_ms: None,
                }),
                Ok(IndexResponse::Completed),
            ),
            (
                IndexRequest::List {
                    locator: locator.clone(),
                },
                Ok(IndexResponse::Indexes(vec![IndexInfo {
                    name: Some("text_index".into()),
                    columns: vec!["text".into()],
                    ready: Some(false),
                }])),
            ),
            (
                IndexRequest::WaitFor(WaitForIndexRequest {
                    locator: locator.clone(),
                    index_names: vec!["text_index".into()],
                    timeout_ms: 5,
                }),
                Err(StorageError::BackendOperation {
                    operation: "wait_for_index".into(),
                    message: "not ready".into(),
                }),
            ),
            (
                IndexRequest::Drop {
                    locator,
                    name: "text_index".into(),
                },
                Ok(IndexResponse::Completed),
            ),
        ]
    }
    fn response<R: PartialEq + std::fmt::Debug>(
        actual: Result<R, StorageError>,
        expected: Result<R, StorageError>,
    ) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
            (Err(actual), Err(expected)) => {
                assert_eq!(
                    std::mem::discriminant(&actual),
                    std::mem::discriminant(&expected)
                );
                assert_eq!(actual.to_string(), expected.to_string());
            }
            (actual, expected) => panic!("result mismatch: {actual:?} versus {expected:?}"),
        }
    }
    async fn exercise<D: DatabaseBackend>(database: &D) {
        assert_eq!(
            database.capabilities(),
            CapabilitySet::DENSE_VECTOR | CapabilitySet::FILTERING
        );
        for (request, expected) in Self::catalog() {
            Self::response(database.catalog(request).await, expected);
        }
        for (request, expected) in Self::data() {
            Self::response(database.data(request).await, expected);
        }
        for (request, expected) in Self::query() {
            Self::response(database.query(request).await, expected);
        }
        for (request, expected) in Self::index() {
            Self::response(database.index(request).await, expected);
        }
    }
}

#[tokio::test]
async fn database_struct_and_every_enum_arm_forward_retained_typed_operations() {
    let concrete = ScriptDatabase::full();
    DatabaseCases::exercise(&concrete).await;
    concrete.finish();
    for database in [
        CallerDatabase::First(ScriptDatabase::full()),
        CallerDatabase::Second(ScriptDatabase::full()),
    ] {
        DatabaseCases::exercise(&database).await;
        match database {
            CallerDatabase::First(db) => db.finish(),
            CallerDatabase::Second(db) => db.finish(),
        }
    }
}

#[tokio::test]
async fn database_script_rejects_wrong_unexpected_exhausted_and_unconsumed_calls() {
    let database = ScriptDatabase::full();
    assert!(database.data.lock().unwrap().finish().is_err());
    assert!(matches!(
        database
            .data(DataRequest::Count(CountRequest {
                locator: DatabaseCases::locator(),
                filter: None
            }))
            .await,
        Err(StorageError::InvalidRequest(_))
    ));
    DatabaseCases::exercise(&database).await;
    database.finish();
    let observed = database.data.lock().unwrap().observed.clone();
    assert!(matches!(
        database.data(DatabaseCases::data().remove(0).0).await,
        Err(StorageError::InvalidRequest(_))
    ));
    assert_eq!(observed, database.data.lock().unwrap().observed);
    let independent = ScriptDatabase::full();
    assert!(independent.data.lock().unwrap().observed.is_empty());
    assert!(independent.data.lock().unwrap().finish().is_err());
}

#[tokio::test]
async fn database_cursor_round_trip_keeps_opaque_tokens_and_score_semantics() {
    let first = ScrollRequest {
        locator: DatabaseCases::locator(),
        filter: Some(FilterExpr::eq("scope", "one")),
        projection: Projection::All,
        limit: Some(2),
        offset: None,
        page_token: None,
    };
    let token = PageToken::new("opaque cursor: ä/next?x=1");
    let mut second = first.clone();
    second.page_token = Some(token.clone());
    let rows = vec![
        ScoredRecord {
            record: DatabaseCases::row(),
            raw_score: Some(0.5),
            score_kind: ScoreKind::Distance,
        },
        ScoredRecord {
            record: DatabaseCases::row(),
            raw_score: Some(12.0),
            score_kind: ScoreKind::Relevance,
        },
    ];
    let database = ScriptDatabase {
        catalog: Mutex::new(Script::new(vec![])),
        data: Mutex::new(Script::new(vec![])),
        index: Mutex::new(Script::new(vec![])),
        query: Mutex::new(Script::new(vec![
            (
                QueryRequest::Scroll(first.clone()),
                Ok(QueryResponse {
                    hits: rows.clone(),
                    next_page: Some(token),
                }),
            ),
            (
                QueryRequest::Scroll(second),
                Ok(QueryResponse {
                    hits: vec![],
                    next_page: None,
                }),
            ),
        ])),
    };
    let page = database
        .query(QueryRequest::Scroll(first.clone()))
        .await
        .unwrap();
    assert_eq!(page.hits, rows);
    let next = database
        .query(QueryRequest::Scroll(ScrollRequest {
            page_token: page.next_page,
            ..first
        }))
        .await
        .unwrap();
    assert!(next.hits.is_empty());
    assert!(next.next_page.is_none());
    database.finish();
}

#[tokio::test]
async fn ordinary_write_failure_and_atomic_partition_rejection_keep_distinct_requests() {
    let ordinary = DataRequest::Insert(InsertRequest::overwrite(
        DatabaseCases::locator(),
        vec![DatabaseCases::row()],
    ));
    let atomic = DataRequest::ReplacePartition(ReplacePartitionRequest {
        locator: DatabaseCases::locator(),
        partition: FilterExpr::eq("text", "retained"),
        identity_field: "id".into(),
        records: vec![],
    });
    let database = ScriptDatabase {
        catalog: Mutex::new(Script::new(vec![])),
        query: Mutex::new(Script::new(vec![])),
        index: Mutex::new(Script::new(vec![])),
        data: Mutex::new(Script::new(vec![
            (
                ordinary.clone(),
                Err(StorageError::BackendOperation {
                    operation: "insert".into(),
                    message: "outcome uncertain".into(),
                }),
            ),
            (
                atomic.clone(),
                Err(StorageError::unsupported_capability(
                    "ATOMIC_PARTITION_REPLACE",
                    "rejected before mutation",
                )),
            ),
        ])),
    };
    assert!(matches!(
        database.data(ordinary).await,
        Err(StorageError::BackendOperation { .. })
    ));
    assert!(matches!(
        database.data(atomic).await,
        Err(StorageError::UnsupportedCapability { .. })
    ));
    database.finish();
    assert_eq!(
        database.data.lock().unwrap().observed.len(),
        2,
        "no implicit retry or rollback-capable multi-request transaction"
    );
}

#[derive(Clone)]
struct Entry {
    revision: StateRevision,
    value: Option<StateRecord>,
}

impl Entry {
    fn address(key: &StateKey) -> (String, String) {
        (key.namespace().as_str().to_owned(), key.key().to_owned())
    }

    fn snapshot(&self) -> Option<StateRecordSnapshot> {
        self.value.clone().map(|record| StateRecordSnapshot {
            record,
            revision: self.revision,
        })
    }
}

#[derive(Default)]
struct Ledger {
    records: BTreeMap<(String, String), Entry>,
    pending: Vec<Publication>,
    store_fault: Option<StateStoreError>,
}

struct Publication {
    writes: WriteSet,
    receipt: Arc<Receipt>,
}

#[derive(Default)]
struct Receipt {
    result: Mutex<Option<Result<(), StateStoreError>>>,
    ready: Notify,
}

impl Receipt {
    async fn wait(&self) -> Result<(), StateStoreError> {
        loop {
            let ready = self.ready.notified();
            if let Some(result) = self.result.lock().expect("receipt").clone() {
                return result;
            }
            ready.await;
        }
    }

    fn finish(&self, result: Result<(), StateStoreError>) {
        *self.result.lock().expect("receipt") = Some(result);
        self.ready.notify_one();
    }
}

impl Ledger {
    fn publish(&mut self, writes: WriteSet) -> Result<(), StateStoreError> {
        for address in writes.reads {
            if self.records.get(&address).map(|entry| entry.revision)
                != writes.observed.get(&address).map(|entry| entry.revision)
            {
                return Err(StateStoreError::transaction_conflict(
                    "commit",
                    "observed revision changed",
                ));
            }
        }
        self.records.extend(writes.staged);
        Ok(())
    }

    fn settle(&mut self) {
        for publication in std::mem::take(&mut self.pending) {
            let result = self.publish(publication.writes);
            publication.receipt.finish(result);
        }
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        // Admitted finalization is owned by the ledger, not by a reply future.
        // This fixture has no native resources or asynchronous cleanup.
        self.settle();
    }
}

struct MemoryStore {
    ledger: Arc<Mutex<Ledger>>,
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            ledger: Arc::new(Mutex::new(Ledger::default())),
        }
    }

    fn settle(&self) {
        self.ledger.lock().expect("ledger").settle();
    }

    fn pending(&self) -> usize {
        self.ledger.lock().expect("ledger").pending.len()
    }

    fn fail_next_store_operation(&self, error: StateStoreError) {
        self.ledger.lock().expect("ledger").store_fault = Some(error);
    }

    fn seed_boundary(&self, key: StateKey) {
        self.ledger.lock().expect("ledger").records.insert(
            Entry::address(&key),
            Entry {
                revision: StateRevision::new(u64::MAX).expect("positive revision"),
                value: Some(StateRecord::new(key, serde_json::json!("boundary"))),
            },
        );
    }
}

struct WriteSet {
    observed: BTreeMap<(String, String), Entry>,
    staged: BTreeMap<(String, String), Entry>,
    reads: BTreeSet<(String, String)>,
}

enum TransactionState {
    Active(WriteSet),
    Invalid,
}

enum OperationFault {
    None,
    Fail(StateStoreError),
    Wait(Arc<Notify>),
}

enum CommitMode {
    Immediate,
    Before(StateStoreError),
    After(StateStoreError),
    Gated,
}

struct MemoryTransaction {
    ledger: Arc<Mutex<Ledger>>,
    state: TransactionState,
    fault: OperationFault,
    commit_mode: CommitMode,
    rollback_error: Option<StateStoreError>,
    // The public transaction contract requires Send, but not Sync.
    operations: Cell<usize>,
}

impl MemoryTransaction {
    fn fail_next(&mut self, error: StateStoreError) {
        self.fault = OperationFault::Fail(error);
    }

    fn wait_next(&mut self, gate: Arc<Notify>) {
        self.fault = OperationFault::Wait(gate);
    }

    fn finalize_as(&mut self, mode: CommitMode) {
        self.commit_mode = mode;
    }

    async fn admission(&mut self) -> Result<WriteSet, StateStoreError> {
        let TransactionState::Active(writes) =
            std::mem::replace(&mut self.state, TransactionState::Invalid)
        else {
            return Err(StateStoreError::invalid_request("transaction is invalid"));
        };
        self.operations.set(self.operations.get() + 1);
        match std::mem::replace(&mut self.fault, OperationFault::None) {
            OperationFault::None => {}
            OperationFault::Fail(error) => return Err(error),
            OperationFault::Wait(gate) => gate.notified().await,
        }
        // Cancellation before this return leaves the transaction Invalid.
        Ok(writes)
    }

    async fn mutate(
        &mut self,
        key: StateKey,
        value: Option<StateRecord>,
    ) -> Result<(), StateStoreError> {
        let mut writes = self.admission().await?;
        let address = Entry::address(&key);
        writes.reads.insert(address.clone());
        let previous = writes
            .staged
            .get(&address)
            .or_else(|| writes.observed.get(&address));
        let revision = previous
            .map_or(Some(1), |entry| entry.revision.get().checked_add(1))
            .ok_or_else(|| StateStoreError::invalid_request("revision overflow"))?;
        writes.staged.insert(
            address,
            Entry {
                revision: StateRevision::new(revision)?,
                value,
            },
        );
        self.state = TransactionState::Active(writes);
        Ok(())
    }
}

impl StateStoreBackend for MemoryStore {
    type Transaction = MemoryTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let mut ledger = self.ledger.lock().expect("ledger");
        if let Some(error) = ledger.store_fault.take() {
            return Err(error);
        }
        Ok(ledger
            .records
            .get(&Entry::address(&key))
            .and_then(Entry::snapshot))
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let mut ledger = self.ledger.lock().expect("ledger");
        if let Some(error) = ledger.store_fault.take() {
            return Err(error);
        }
        let mut matching = ledger
            .records
            .iter()
            .filter(|((namespace, key), _)| {
                namespace == request.namespace().as_str()
                    && request
                        .prefix()
                        .is_none_or(|prefix| key.starts_with(prefix))
                    && request
                        .start_after_key()
                        .is_none_or(|cursor| key.as_str() > cursor)
            })
            .filter_map(|(_, entry)| entry.snapshot());
        let records = matching.by_ref().take(request.limit()).collect::<Vec<_>>();
        let next_start_after = matching.next().and_then(|_| {
            records
                .last()
                .map(|entry| entry.record.key.key().to_owned())
        });
        Ok(StateListResponse {
            records,
            next_start_after,
        })
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let mut ledger = self.ledger.lock().expect("ledger");
        if let Some(error) = ledger.store_fault.take() {
            return Err(error);
        }
        Ok(MemoryTransaction {
            ledger: Arc::clone(&self.ledger),
            state: TransactionState::Active(WriteSet {
                observed: ledger.records.clone(),
                staged: BTreeMap::new(),
                reads: BTreeSet::new(),
            }),
            fault: OperationFault::None,
            commit_mode: CommitMode::Immediate,
            rollback_error: None,
            operations: Cell::new(0),
        })
    }
}

impl StateTransactionBackend for MemoryTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let mut writes = self.admission().await?;
        let address = Entry::address(&key);
        writes.reads.insert(address.clone());
        let result = writes
            .staged
            .get(&address)
            .or_else(|| writes.observed.get(&address))
            .and_then(Entry::snapshot);
        self.state = TransactionState::Active(writes);
        Ok(result)
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.mutate(record.key.clone(), Some(record)).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.mutate(key, None).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let TransactionState::Active(writes) = self.state else {
            return Err(StateStoreError::invalid_request(
                "invalid transaction cannot commit",
            ));
        };
        match self.commit_mode {
            CommitMode::Immediate => self.ledger.lock().expect("ledger").publish(writes),
            CommitMode::Before(error) => Err(error),
            CommitMode::After(error) => {
                self.ledger.lock().expect("ledger").publish(writes)?;
                Err(error)
            }
            CommitMode::Gated => {
                let receipt = Arc::new(Receipt::default());
                self.ledger
                    .lock()
                    .expect("ledger")
                    .pending
                    .push(Publication {
                        writes,
                        receipt: Arc::clone(&receipt),
                    });
                receipt.wait().await
            }
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        match self.rollback_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

enum CallerStore {
    First(MemoryStore),
    Second(MemoryStore),
}

enum CallerTransaction {
    First(MemoryTransaction),
    Second(MemoryTransaction),
}

impl StateStoreBackend for CallerStore {
    type Transaction = CallerTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::First(store) => store.load(key).await,
            Self::Second(store) => store.load(key).await,
        }
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        match self {
            Self::First(store) => store.list(request).await,
            Self::Second(store) => store.list(request).await,
        }
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        match self {
            Self::First(store) => store
                .begin_transaction()
                .await
                .map(CallerTransaction::First),
            Self::Second(store) => store
                .begin_transaction()
                .await
                .map(CallerTransaction::Second),
        }
    }
}

impl StateTransactionBackend for CallerTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::First(transaction) => transaction.load(key).await,
            Self::Second(transaction) => transaction.load(key).await,
        }
    }
    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        match self {
            Self::First(transaction) => transaction.put(record).await,
            Self::Second(transaction) => transaction.put(record).await,
        }
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        match self {
            Self::First(transaction) => transaction.delete(key).await,
            Self::Second(transaction) => transaction.delete(key).await,
        }
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        match self {
            Self::First(transaction) => transaction.commit().await,
            Self::Second(transaction) => transaction.commit().await,
        }
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        match self {
            Self::First(transaction) => transaction.rollback().await,
            Self::Second(transaction) => transaction.rollback().await,
        }
    }
}

struct StateCases;

impl StateCases {
    fn key(namespace: &str, key: &str) -> StateKey {
        StateKey::new(namespace, key).expect("valid fixture key")
    }

    async fn put<S: StateStoreBackend>(store: &S, key: StateKey, value: serde_json::Value) {
        let mut transaction = store.begin_transaction().await.expect("begin");
        transaction
            .put(StateRecord::new(key, value))
            .await
            .expect("put");
        transaction.commit().await.expect("commit");
    }

    async fn hierarchy<S: StateStoreBackend>(store: S) {
        let key = Self::key("one", "key");
        let mut transaction = store.begin_transaction().await.expect("begin");
        transaction
            .delete(key.clone())
            .await
            .expect("first tombstone");
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!(1)))
            .await
            .expect("put");
        assert_eq!(
            transaction
                .load(key.clone())
                .await
                .unwrap()
                .unwrap()
                .revision
                .get(),
            2
        );
        assert!(store.load(key.clone()).await.unwrap().is_none());
        transaction.commit().await.unwrap();
        let mut other = store.begin_transaction().await.unwrap();
        other.delete(key.clone()).await.unwrap();
        other.rollback().await.unwrap();
        assert_eq!(
            store
                .load(key.clone())
                .await
                .unwrap()
                .unwrap()
                .revision
                .get(),
            2
        );
        assert!(store.load(Self::key("two", "key")).await.unwrap().is_none());
        assert_eq!(
            store
                .list(StateListRequest::new("one").unwrap())
                .await
                .unwrap()
                .records
                .len(),
            1
        );
    }

    async fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
        let mut future = future;
        let poll = std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
        assert!(poll.is_pending(), "fixture gate must suspend the operation");
    }
}

#[tokio::test]
async fn store_failures_keep_exact_categories_and_do_not_mutate_retained_state() {
    let store = MemoryStore::new();
    let key = StateCases::key("faults", "key");
    StateCases::put(&store, key.clone(), serde_json::json!("retained")).await;
    store.fail_next_store_operation(StateStoreError::unavailable("load", "transport"));
    assert!(
        matches!(store.load(key.clone()).await, Err(StateStoreError::Unavailable { operation, message }) if operation == "load" && message == "transport")
    );
    store.fail_next_store_operation(StateStoreError::timed_out("list", 7));
    assert!(
        matches!(store.list(StateListRequest::new("faults").unwrap()).await, Err(StateStoreError::TimedOut { operation, timeout_ms: 7 }) if operation == "list")
    );
    store.fail_next_store_operation(StateStoreError::backend_operation("begin", "admission"));
    assert!(
        matches!(store.begin_transaction().await, Err(StateStoreError::BackendOperation { operation, message }) if operation == "begin" && message == "admission")
    );
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction.delete(key.clone()).await.unwrap();
    transaction.rollback_error = Some(StateStoreError::unavailable("rollback", "cleanup"));
    assert!(
        matches!(transaction.rollback().await, Err(StateStoreError::Unavailable { operation, message }) if operation == "rollback" && message == "cleanup")
    );
    assert_eq!(store.load(key).await.unwrap().unwrap().revision.get(), 1);
}

#[tokio::test]
async fn state_enum_arms_preserve_store_and_transaction_error_fidelity() {
    for store in [
        CallerStore::First(MemoryStore::new()),
        CallerStore::Second(MemoryStore::new()),
    ] {
        match &store {
            CallerStore::First(inner) => {
                inner.fail_next_store_operation(StateStoreError::serialization("decode"))
            }
            CallerStore::Second(inner) => {
                inner.fail_next_store_operation(StateStoreError::serialization("decode"))
            }
        }
        assert!(
            matches!(store.load(StateCases::key("enum", "key")).await, Err(StateStoreError::Serialization(message)) if message == "decode")
        );
        let mut transaction = store.begin_transaction().await.unwrap();
        match &mut transaction {
            CallerTransaction::First(inner) => {
                inner.fail_next(StateStoreError::invalid_request("rejected"))
            }
            CallerTransaction::Second(inner) => {
                inner.fail_next(StateStoreError::invalid_request("rejected"))
            }
        }
        assert!(
            matches!(transaction.delete(StateCases::key("enum", "key")).await, Err(StateStoreError::InvalidRequest(message)) if message == "rejected")
        );
        assert!(matches!(
            transaction.commit().await,
            Err(StateStoreError::InvalidRequest(_))
        ));
        assert!(
            store
                .load(StateCases::key("enum", "key"))
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn structs_and_every_enum_arm_preserve_associated_non_clone_children() {
    StateCases::hierarchy(MemoryStore::new()).await;
    StateCases::hierarchy(CallerStore::First(MemoryStore::new())).await;
    StateCases::hierarchy(CallerStore::Second(MemoryStore::new())).await;
}

#[tokio::test]
async fn mutations_keep_authored_revisions_and_tombstones_without_partial_visibility() {
    let store = MemoryStore::new();
    let key = StateCases::key("n", "key");
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction.delete(key.clone()).await.unwrap();
    transaction.delete(key.clone()).await.unwrap();
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .unwrap();
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .unwrap();
    assert_eq!(
        transaction
            .load(key.clone())
            .await
            .unwrap()
            .unwrap()
            .revision
            .get(),
        4
    );
    transaction.delete(key.clone()).await.unwrap();
    transaction.commit().await.unwrap();
    assert!(store.load(key.clone()).await.unwrap().is_none());
    StateCases::put(&store, key.clone(), serde_json::json!(2)).await;
    assert_eq!(store.load(key).await.unwrap().unwrap().revision.get(), 6);
}

#[tokio::test]
async fn rollback_drop_and_unpolled_commit_publish_nothing() {
    let store = MemoryStore::new();
    let key = StateCases::key("n", "key");
    for mode in 0..3 {
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!(mode)))
            .await
            .unwrap();
        match mode {
            0 => transaction.rollback().await.unwrap(),
            1 => drop(transaction),
            _ => drop(transaction.commit()),
        }
        assert!(store.load(key.clone()).await.unwrap().is_none());
    }
    StateCases::put(&store, key.clone(), serde_json::json!("first")).await;
    assert_eq!(store.load(key).await.unwrap().unwrap().revision.get(), 1);
}

#[tokio::test]
async fn present_absent_and_tombstoned_read_conflicts_publish_no_partial_receipt() {
    for initial in 0..3 {
        let store = MemoryStore::new();
        let key = StateCases::key("n", "head");
        if initial > 0 {
            StateCases::put(&store, key.clone(), serde_json::json!(0)).await;
        }
        if initial == 2 {
            let mut transaction = store.begin_transaction().await.unwrap();
            transaction.delete(key.clone()).await.unwrap();
            transaction.commit().await.unwrap();
        }
        let mut stale = store.begin_transaction().await.unwrap();
        stale.load(key.clone()).await.unwrap();
        let receipt = StateCases::key("n", "receipt");
        stale
            .put(StateRecord::new(
                receipt.clone(),
                serde_json::json!("stale"),
            ))
            .await
            .unwrap();
        StateCases::put(&store, key, serde_json::json!(1)).await;
        assert!(matches!(
            stale.commit().await,
            Err(StateStoreError::TransactionConflict { .. })
        ));
        assert!(store.load(receipt).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn write_skew_is_rejected_but_store_observation_is_not_a_transaction_fence() {
    let store = MemoryStore::new();
    let first = StateCases::key("n", "first");
    let second = StateCases::key("n", "second");
    StateCases::put(&store, first.clone(), serde_json::json!(true)).await;
    StateCases::put(&store, second.clone(), serde_json::json!(true)).await;
    let mut left = store.begin_transaction().await.unwrap();
    let mut right = store.begin_transaction().await.unwrap();
    left.load(second.clone()).await.unwrap();
    right.load(first.clone()).await.unwrap();
    left.put(StateRecord::new(first.clone(), serde_json::json!(false)))
        .await
        .unwrap();
    right
        .put(StateRecord::new(second.clone(), serde_json::json!(false)))
        .await
        .unwrap();
    left.commit().await.unwrap();
    assert!(matches!(
        right.commit().await,
        Err(StateStoreError::TransactionConflict { .. })
    ));
    let prior = store.load(first.clone()).await.unwrap().unwrap();
    StateCases::put(&store, first.clone(), serde_json::json!("later")).await;
    StateCases::put(&store, first.clone(), serde_json::json!("unconditional")).await;
    assert!(store.load(first).await.unwrap().unwrap().revision > prior.revision);
}

#[tokio::test]
async fn failed_load_put_and_delete_poison_all_prior_staging() {
    for operation in 0..3 {
        let store = MemoryStore::new();
        let key = StateCases::key("n", "key");
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!(1)))
            .await
            .unwrap();
        let error = StateStoreError::unavailable("operation", "scripted failure");
        transaction.fail_next(error.clone());
        let observed = match operation {
            0 => transaction.load(key.clone()).await.map(|_| ()),
            1 => {
                transaction
                    .put(StateRecord::new(key.clone(), serde_json::json!(2)))
                    .await
            }
            _ => transaction.delete(key.clone()).await,
        };
        assert_eq!(observed, Err(error));
        assert!(transaction.delete(key.clone()).await.is_err());
        assert!(transaction.commit().await.is_err());
        assert!(store.load(key).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn cancelled_admitted_operation_poisons_but_unpolled_operation_does_not() {
    for operation in 0..3 {
        let store = MemoryStore::new();
        let key = StateCases::key("n", "key");
        let mut transaction = store.begin_transaction().await.unwrap();
        drop(transaction.delete(key.clone()));
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!(1)))
            .await
            .unwrap();
        transaction.wait_next(Arc::new(Notify::new()));
        {
            let mut pending = std::pin::pin!(async {
                match operation {
                    0 => transaction.load(key.clone()).await.map(|_| ()),
                    1 => {
                        transaction
                            .put(StateRecord::new(key.clone(), serde_json::json!(2)))
                            .await
                    }
                    _ => transaction.delete(key.clone()).await,
                }
            });
            StateCases::pending(pending.as_mut()).await;
        }
        assert!(transaction.commit().await.is_err());
        assert!(store.load(key).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn overflow_invalidates_earlier_writes_without_wrapping_or_publication() {
    let store = MemoryStore::new();
    let boundary = StateCases::key("n", "boundary");
    store.seed_boundary(boundary.clone());
    let receipt = StateCases::key("n", "receipt");
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction
        .put(StateRecord::new(receipt.clone(), serde_json::json!(1)))
        .await
        .unwrap();
    assert!(matches!(
        transaction.delete(boundary.clone()).await,
        Err(StateStoreError::InvalidRequest(_))
    ));
    assert!(transaction.commit().await.is_err());
    assert!(store.load(receipt).await.unwrap().is_none());
    assert_eq!(
        store.load(boundary).await.unwrap().unwrap().revision.get(),
        u64::MAX
    );
}

#[tokio::test]
async fn listing_is_literal_ordered_bounded_and_uses_a_new_view_for_each_page() {
    let store = MemoryStore::new();
    for key in ["a%1", "a%2", "a%4", "A%1", "a_1", "é"] {
        StateCases::put(&store, StateCases::key("n", key), serde_json::json!(key)).await;
    }
    StateCases::put(
        &store,
        StateCases::key("other", "a%0"),
        serde_json::json!(0),
    )
    .await;
    let request = StateListRequest::new("n")
        .unwrap()
        .with_key_prefix(StateKeyPrefix::new("a%").unwrap())
        .with_limit(2)
        .unwrap();
    let first = store.list(request.clone()).await.unwrap();
    assert_eq!(
        first
            .records
            .iter()
            .map(|row| row.record.key.key())
            .collect::<Vec<_>>(),
        ["a%1", "a%2"]
    );
    assert_eq!(first.next_start_after.as_deref(), Some("a%2"));
    let mut delete = store.begin_transaction().await.unwrap();
    delete.delete(StateCases::key("n", "a%2")).await.unwrap();
    delete.commit().await.unwrap();
    StateCases::put(&store, StateCases::key("n", "a%3"), serde_json::json!(3)).await;
    let second = store
        .list(
            request
                .start_after(first.next_start_after.unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        second
            .records
            .iter()
            .map(|row| row.record.key.key())
            .collect::<Vec<_>>(),
        ["a%3", "a%4"]
    );
    assert!(
        second.next_start_after.is_none(),
        "exactly full final page has no cursor"
    );
    assert!(
        store
            .list(StateListRequest::new("absent").unwrap())
            .await
            .unwrap()
            .next_start_after
            .is_none()
    );
}

#[tokio::test]
async fn cloned_record_changes_cannot_mutate_retained_observations() {
    let store = MemoryStore::new();
    let key = StateCases::key("n", "key");
    StateCases::put(&store, key.clone(), serde_json::json!({"value": 1})).await;
    let original = store.load(key.clone()).await.unwrap().unwrap();
    let mut changed = original.record.clone();
    Arc::make_mut(&mut changed.value)["value"] = serde_json::json!(2);
    assert_eq!(original.record.value["value"], 1);
    assert_eq!(store.load(key).await.unwrap().unwrap(), original);
}

#[tokio::test]
async fn identical_errors_can_describe_both_unpublished_and_published_commits() {
    for published in [false, true] {
        let store = MemoryStore::new();
        let key = StateCases::key("n", "receipt");
        let error = StateStoreError::timed_out("commit", 10);
        let mut transaction = store.begin_transaction().await.unwrap();
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!("accepted")))
            .await
            .unwrap();
        transaction.finalize_as(if published {
            CommitMode::After(error.clone())
        } else {
            CommitMode::Before(error.clone())
        });
        assert_eq!(transaction.commit().await, Err(error));
        assert_eq!(store.load(key).await.unwrap().is_some(), published);
    }
}

#[tokio::test]
async fn dropped_commit_waiter_does_not_revoke_admitted_publication() {
    let store = MemoryStore::new();
    let key = StateCases::key("n", "receipt");
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .unwrap();
    transaction.finalize_as(CommitMode::Gated);
    let mut commit = Box::pin(transaction.commit());
    StateCases::pending(commit.as_mut()).await;
    assert_eq!(store.pending(), 1);
    assert!(store.load(key.clone()).await.unwrap().is_none());
    drop(commit);
    store.settle();
    assert_eq!(store.pending(), 0);
    assert!(store.load(key).await.unwrap().is_some());
}

#[tokio::test]
async fn successful_commit_reply_follows_publication_and_owned_finalization() {
    let store = MemoryStore::new();
    let key = StateCases::key("n", "receipt");
    let mut transaction = store.begin_transaction().await.unwrap();
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .unwrap();
    transaction.finalize_as(CommitMode::Gated);
    let mut commit = Box::pin(transaction.commit());
    StateCases::pending(commit.as_mut()).await;
    store.settle();
    tokio::time::timeout(Duration::from_secs(2), commit)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.pending(), 0);
    assert!(store.load(key).await.unwrap().is_some());
}
