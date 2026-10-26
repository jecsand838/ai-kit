use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{
    BooleanArray, Int64Array, LargeBinaryArray, RecordBatch, StringArray, UInt8Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::{
    Dataset,
    dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams},
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateListRequest, StateRecord},
    },
};

#[derive(Clone, Copy, Debug)]
pub(super) enum NativeFault {
    RevisionBoundary,
    Json,
    Schema,
    MissingControl,
    DuplicateControl,
    DuplicateRecord,
    Identity,
    EmptyIdentity,
    MalformedIdentity,
    UppercaseIdentity,
    Target,
}

struct NativeRow {
    kind: u8,
    namespace: &'static str,
    key: &'static str,
    revision: u64,
    present: bool,
    value: Option<Vec<u8>>,
    token: String,
}

impl NativeRow {
    fn control(token: &str) -> Self {
        Self {
            kind: 1,
            namespace: "",
            key: "",
            revision: 0,
            present: false,
            value: None,
            token: token.to_owned(),
        }
    }

    fn record() -> Self {
        Self {
            kind: 0,
            namespace: "fault",
            key: "record",
            revision: 1,
            present: true,
            value: Some(br#""original""#.to_vec()),
            token: String::new(),
        }
    }
}

pub(super) struct NativeFixture {
    directory: tempfile::TempDir,
}

impl NativeFixture {
    pub(super) fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("owned failure fixture"),
        }
    }

    fn config(&self) -> StateStoreConfig {
        StateStoreConfig::Lance(LanceStateStoreConfig::directory(self.directory.path()))
    }

    fn uri(&self) -> String {
        self.directory
            .path()
            .join("__ai_kit_state.lance")
            .to_str()
            .expect("UTF-8 native fixture path")
            .to_owned()
    }

    pub(super) async fn open_native(&self) -> Arc<Dataset> {
        Arc::new(
            Dataset::open(&self.uri())
                .await
                .expect("open fixture native table"),
        )
    }

    async fn stored_json(&self) -> Vec<u8> {
        let dataset = self.open_native().await;
        let mut stream = dataset
            .scan()
            .try_into_stream()
            .await
            .expect("native byte read");
        while let Some(batch) = stream.try_next().await.expect("native byte batch") {
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("kind");
            let values = batch
                .column(5)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("JSON bytes");
            for row in 0..batch.num_rows() {
                if kinds.value(row) == 0 {
                    return values.value(row).to_vec();
                }
            }
        }
        panic!("fixture record must retain persisted bytes");
    }

    pub(super) async fn connect(&self) -> StateStore {
        StateStore::connect(self.config())
            .await
            .expect("supplied Lance store")
    }

    async fn publish(&self, fault: NativeFault) -> u64 {
        self.publish_content(fault, None).await
    }

    pub(super) async fn publish_content(&self, fault: NativeFault, json: Option<Vec<u8>>) -> u64 {
        let dataset = self.open_native().await;
        let target = dataset.version_id() + 1;
        let uuid = if matches!(fault, NativeFault::UppercaseIdentity) {
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_owned()
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        let mut rows = vec![NativeRow::control(&uuid), NativeRow::record()];
        match fault {
            NativeFault::RevisionBoundary => rows[1].revision = u64::MAX,
            NativeFault::Json => rows[1].value = Some(b"{broken JSON".to_vec()),
            NativeFault::MissingControl => {
                rows.remove(0);
            }
            NativeFault::DuplicateControl => rows.push(NativeRow::control(&uuid)),
            NativeFault::DuplicateRecord => rows.push(NativeRow::record()),
            NativeFault::Identity => rows[0].token = uuid::Uuid::new_v4().to_string(),
            NativeFault::EmptyIdentity
            | NativeFault::MalformedIdentity
            | NativeFault::UppercaseIdentity
            | NativeFault::Target
            | NativeFault::Schema => {}
        }
        if let Some(json) = json {
            rows[1].value = Some(json);
        }
        let batch = if matches!(fault, NativeFault::Schema) {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "unrelated",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .expect("incompatible schema fixture")
        } else {
            RecordBatch::try_new(
                Arc::new(Schema::from(dataset.schema())),
                vec![
                    Arc::new(UInt8Array::from_iter_values(
                        rows.iter().map(|row| row.kind),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.namespace),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.key),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        rows.iter().map(|row| row.revision),
                    )),
                    Arc::new(BooleanArray::from(
                        rows.iter().map(|row| row.present).collect::<Vec<_>>(),
                    )),
                    Arc::new(LargeBinaryArray::from_iter(
                        rows.iter().map(|row| row.value.as_deref()),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        rows.iter().map(|row| row.token.as_str()),
                    )),
                ],
            )
            .expect("native failure fixture batch")
        };
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            data_storage_version: Some(
                dataset
                    .manifest()
                    .data_storage_format
                    .lance_file_format()
                    .to_manifest_string()
                    .parse()
                    .expect("retain the exact native storage format"),
            ),
            enable_stable_row_ids: false,
            ..WriteParams::default()
        };
        let mut transaction = InsertBuilder::new(Arc::clone(&dataset))
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .expect("prepare native fault without publication");
        transaction.uuid = match fault {
            NativeFault::EmptyIdentity => String::new(),
            NativeFault::MalformedIdentity => "native-identity-without-uuid-syntax".to_owned(),
            NativeFault::UppercaseIdentity => uuid.to_uppercase(),
            NativeFault::RevisionBoundary
            | NativeFault::Json
            | NativeFault::Schema
            | NativeFault::MissingControl
            | NativeFault::DuplicateControl
            | NativeFault::DuplicateRecord
            | NativeFault::Identity
            | NativeFault::Target => uuid,
        };
        transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            "ai-kit.state.expected-version".to_owned(),
            if matches!(fault, NativeFault::Target) {
                target + 1
            } else {
                target
            }
            .to_string(),
        )])));
        let published = CommitBuilder::new(dataset)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .expect("publish native fault fixture");
        assert_eq!(published.version_id(), target);
        target
    }

    async fn replace_native_format(&self, format: &str, stable_row_ids: bool) {
        self.replace_native_image(format, stable_row_ids, None)
            .await;
    }

    async fn replace_native_image(
        &self,
        format: &str,
        stable_row_ids: bool,
        record: Option<NativeRow>,
    ) {
        let dataset = self.open_native().await;
        let schema = Arc::new(Schema::from(dataset.schema()));
        drop(dataset);
        // Replace only this disposable fixture's table, retaining the already
        // connected public owner so its next fresh read must validate it too.
        std::fs::remove_dir_all(self.directory.path().join("__ai_kit_state.lance"))
            .expect("replace owned native fixture");
        let uuid = uuid::Uuid::new_v4().to_string();
        let mut rows = vec![NativeRow::control(&uuid)];
        rows.extend(record);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt8Array::from_iter_values(
                    rows.iter().map(|row| row.kind),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.namespace),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.key),
                )),
                Arc::new(UInt64Array::from_iter_values(
                    rows.iter().map(|row| row.revision),
                )),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.present).collect::<Vec<_>>(),
                )),
                Arc::new(LargeBinaryArray::from_iter(
                    rows.iter().map(|row| row.value.as_deref()),
                )),
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|row| row.token.as_str()),
                )),
            ],
        )
        .expect("valid disposable native image");
        let params = WriteParams {
            mode: WriteMode::Create,
            data_storage_version: Some(format.parse().expect("known native format")),
            enable_stable_row_ids: stable_row_ids,
            ..WriteParams::default()
        };
        let uri = self.uri();
        let mut transaction = InsertBuilder::new(uri.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .expect("prepare native-format fixture");
        transaction.uuid = uuid;
        transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            "ai-kit.state.expected-version".to_owned(),
            "1".to_owned(),
        )])));
        let native = CommitBuilder::new(uri.as_str())
            .use_stable_row_ids(stable_row_ids)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .expect("publish native-format fixture");
        assert_eq!(native.version_id(), 1);
        assert_eq!(
            native
                .manifest()
                .data_storage_format
                .lance_file_format()
                .to_manifest_string(),
            format
        );
        assert_eq!(native.manifest().uses_stable_row_ids(), stable_row_ids);
        assert_eq!(
            native
                .scan()
                .try_into_batch()
                .await
                .expect("native fixture rows")
                .num_rows(),
            rows.len()
        );
    }
}

struct NestedJsonCase;

#[tokio::test]
async fn native_exact_version_readback_accepts_unvalidated_identity_but_public_state_rejects_it() {
    // Ordinary native corruption fixtures establish what the pinned reader
    // actually returns. This does not call or copy the adapter's private
    // commit-error classifier; supplied reads exercise its existing integrity
    // boundary independently of collision reconciliation.
    for (fault, expected) in [
        (NativeFault::EmptyIdentity, ""),
        (
            NativeFault::MalformedIdentity,
            "native-identity-without-uuid-syntax",
        ),
        (
            NativeFault::UppercaseIdentity,
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        ),
    ] {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        let key = StateKey::new("fault", "record").expect("fixture key");
        assert!(
            store
                .load(key.clone())
                .await
                .expect("warm valid image")
                .is_none()
        );
        let target = fixture.publish(fault).await;
        let native = fixture.open_native().await;
        let observed = native
            .read_transaction_by_version(target)
            .await
            .expect("native exact-version readback")
            .expect("persisted native transaction");
        assert_eq!(observed.uuid, expected);
        assert_eq!(native.version_id(), target);
        assert_eq!(fixture.stored_json().await, br#""original""#.to_vec());
        if matches!(fault, NativeFault::UppercaseIdentity) {
            assert_eq!(
                uuid::Uuid::parse_str(&observed.uuid)
                    .expect("alternate valid UUID spelling")
                    .to_string(),
                "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            );
        } else {
            assert!(uuid::Uuid::parse_str(&observed.uuid).is_err());
        }
        assert!(matches!(
            store.load(key).await,
            Err(StateStoreError::BackendOperation { .. })
        ));
        assert!(matches!(
            store
                .list(StateListRequest::new("fault").expect("namespace"))
                .await,
            Err(StateStoreError::BackendOperation { .. })
        ));
        assert!(matches!(
            store.begin_transaction().await,
            Err(StateStoreError::BackendOperation { .. })
        ));
        drop(store);
        assert!(matches!(
            StateStore::connect(fixture.config()).await,
            Err(StateStoreError::BackendOperation { .. })
        ));
    }
}

impl NativeFixture {
    async fn replace_cache_record(&self, revision: u64, value: &str) {
        let mut record = NativeRow::record();
        record.revision = revision;
        record.value = Some(serde_json::to_vec(value).expect("fixture scalar"));
        self.replace_native_image("2.0", false, Some(record)).await;
    }
}

#[tokio::test]
async fn warm_cache_refreshes_when_the_same_native_version_has_a_different_transaction_identity() {
    // Native replacement is a disposable corruption/identity fixture, never a
    // supported operator repair or a second implementation of StateStore.
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    fixture.replace_cache_record(7, "first identity").await;
    let key = StateKey::new("fault", "record").expect("fixture key");
    let first = store
        .load(key.clone())
        .await
        .expect("warm original image")
        .expect("record");
    assert_eq!(first.revision.get(), 7);
    assert_eq!(first.record.value.as_str(), Some("first identity"));
    let before = fixture.open_native().await;
    let before_uuid = before
        .read_transaction()
        .await
        .expect("original identity")
        .expect("transaction")
        .uuid;
    fixture
        .replace_cache_record(19, "replacement identity")
        .await;
    let replaced = fixture.open_native().await;
    assert_eq!(replaced.version_id(), before.version_id());
    assert_ne!(
        replaced
            .read_transaction()
            .await
            .expect("replacement identity")
            .expect("transaction")
            .uuid,
        before_uuid
    );
    let current = store
        .load(key.clone())
        .await
        .expect("fresh identity lookup")
        .expect("replacement record");
    assert_eq!(current.revision.get(), 19);
    assert_eq!(current.record.value.as_str(), Some("replacement identity"));
    let page = store
        .list(StateListRequest::new("fault").expect("namespace"))
        .await
        .expect("refreshed list");
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].revision.get(), 19);
    let mut transaction = store.begin_transaction().await.expect("fresh transaction");
    assert_eq!(
        transaction
            .load(key)
            .await
            .expect("fresh image")
            .expect("record")
            .revision
            .get(),
        19
    );
    transaction
        .put(StateRecord::new(
            StateKey::new("fault", "progress").expect("key"),
            serde_json::json!(true),
        ))
        .await
        .expect("progress after image replacement");
    transaction
        .commit()
        .await
        .expect("publish against replacement identity");
}

#[tokio::test]
async fn changed_identity_at_the_same_version_is_backend_uncertainty_without_stale_publication() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    fixture.replace_cache_record(7, "original").await;
    let key = StateKey::new("fault", "record").expect("key");
    let receipt = StateKey::new("fault", "stale-receipt").expect("receipt key");
    let mut stale = store.begin_transaction().await.expect("original snapshot");
    assert_eq!(
        stale
            .load(key.clone())
            .await
            .expect("original read")
            .expect("record")
            .revision
            .get(),
        7
    );
    stale
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("stale staging");
    fixture.replace_cache_record(19, "replacement").await;
    assert!(
        matches!(
            stale.commit().await,
            Err(StateStoreError::BackendOperation { .. })
        ),
        "same-version identity replacement is not an ordinary serialization conflict"
    );
    assert!(
        store
            .load(receipt)
            .await
            .expect("no stale receipt")
            .is_none()
    );
    let current = store
        .load(key)
        .await
        .expect("current record")
        .expect("record");
    assert_eq!(current.revision.get(), 19);
    assert_eq!(current.record.value.as_str(), Some("replacement"));
    assert_eq!(
        fixture.open_native().await.version_id(),
        1,
        "stale input publishes no native version"
    );
}

#[tokio::test]
async fn failed_freshness_observation_never_returns_cached_data_and_recovers_after_restoration() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let key = StateKey::new("fault", "record").expect("key");
    let mut transaction = store.begin_transaction().await.expect("seed");
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!("cached value"),
        ))
        .await
        .expect("seed value");
    transaction.commit().await.expect("seed publication");
    assert_eq!(
        store
            .load(key.clone())
            .await
            .expect("warm cache")
            .expect("record")
            .revision
            .get(),
        1
    );
    let table = fixture.directory.path().join("__ai_kit_state.lance");
    let parked = fixture.directory.path().join("fixture-parked-table");
    std::fs::rename(&table, &parked).expect("temporarily obstruct only the disposable fixture");
    let observation = store.load(key.clone()).await;
    std::fs::rename(&parked, &table).expect("restore fixture before asserting the failure");
    assert!(
        matches!(observation, Err(StateStoreError::BackendOperation { .. })),
        "missing current authority cannot fall back to the warm decoded image"
    );
    let current = store
        .load(key.clone())
        .await
        .expect("fresh read after restoration")
        .expect("record");
    assert_eq!(current.revision.get(), 1);
    assert_eq!(current.record.value.as_str(), Some("cached value"));
    let mut next = store
        .begin_transaction()
        .await
        .expect("subsequent transaction");
    next.put(StateRecord::new(key.clone(), serde_json::json!("progress")))
        .await
        .expect("subsequent staging");
    next.commit().await.expect("subsequent publication");
    assert_eq!(
        store
            .load(key)
            .await
            .expect("subsequent read")
            .expect("record")
            .revision
            .get(),
        2
    );
}

#[tokio::test]
async fn advanced_native_corruption_is_rejected_even_with_a_warm_decoded_image() {
    for fault in [
        NativeFault::Json,
        NativeFault::Schema,
        NativeFault::MissingControl,
        NativeFault::DuplicateControl,
        NativeFault::DuplicateRecord,
        NativeFault::Identity,
        NativeFault::Target,
    ] {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        let key = StateKey::new("fault", "record").expect("key");
        let mut transaction = store.begin_transaction().await.expect("seed");
        transaction
            .put(StateRecord::new(
                key.clone(),
                serde_json::json!("warm value"),
            ))
            .await
            .expect("seed value");
        transaction.commit().await.expect("seed publication");
        assert_eq!(
            store
                .load(key.clone())
                .await
                .expect("warm cache")
                .expect("record")
                .revision
                .get(),
            1
        );
        let before = fixture.open_native().await.version_id();
        assert_eq!(fixture.publish(fault).await, before + 1);
        assert!(
            matches!(
                store.load(key).await,
                Err(StateStoreError::BackendOperation { .. })
            ),
            "new corrupt native image cannot use the old cache: {fault:?}"
        );
        assert!(matches!(
            store
                .list(StateListRequest::new("fault").expect("namespace"))
                .await,
            Err(StateStoreError::BackendOperation { .. })
        ));
        assert!(matches!(
            store.begin_transaction().await,
            Err(StateStoreError::BackendOperation { .. })
        ));
    }
}

#[tokio::test]
async fn directory_io_errors_include_a_sanitized_category_without_paths_or_contents() {
    let directory = tempfile::tempdir().expect("owned diagnostic fixture");
    let blocked = directory
        .path()
        .join(format!("private-location-{}", uuid::Uuid::new_v4()));
    let private_contents = "private-record-content-must-not-be-disclosed";
    std::fs::write(&blocked, private_contents)
        .expect("ordinary file obstructs directory configuration");
    let Err(error) = StateStore::connect(StateStoreConfig::Lance(
        LanceStateStoreConfig::directory(&blocked),
    ))
    .await
    else {
        panic!("file cannot open as a Lance state directory");
    };
    let StateStoreError::BackendOperation { operation, message } = error else {
        panic!("directory I/O failure has its owning backend error type");
    };
    assert_eq!(operation, "connect");
    assert!(
        message.contains("AlreadyExists"),
        "sanitized I/O category must remain actionable: {message}"
    );
    assert!(!message.contains(blocked.to_str().expect("fixture path")));
    assert!(!message.contains("private-location-"));
    assert!(!message.contains(private_contents));
}

impl NestedJsonCase {
    fn key() -> StateKey {
        StateKey::new("nested-json", "depth-140").expect("nested JSON key")
    }

    fn value() -> serde_json::Value {
        let mut value = serde_json::json!({"leaf": "retained"});
        for _ in 0..140 {
            value = serde_json::Value::Array(vec![value]);
        }
        value
    }

    async fn verify_read(store: &StateStore) {
        let observed = store
            .load(Self::key())
            .await
            .expect("acknowledged nested JSON remains readable")
            .expect("nested JSON record");
        assert_eq!(observed.revision.get(), 1);
        assert_eq!(observed.record.value.as_ref(), &Self::value());
        let page = store
            .list(StateListRequest::new("nested-json").expect("namespace"))
            .await
            .expect("nested JSON remains listable");
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].record.value.as_ref(), &Self::value());
    }

    async fn verify_roundtrip_and_progress(store: &StateStore) {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("initial transaction");
        transaction
            .put(StateRecord::new(Self::key(), Self::value()))
            .await
            .expect("stage caller-constructed nested JSON");
        assert_eq!(
            transaction
                .load(Self::key())
                .await
                .expect("staged nested read")
                .expect("staged record")
                .record
                .value
                .as_ref(),
            &Self::value()
        );
        transaction.commit().await.expect("commit nested JSON");
        Self::verify_read(store).await;

        let unrelated = StateKey::new("unrelated", "progress").expect("unrelated key");
        let mut next = store.begin_transaction().await.expect("fresh transaction");
        assert_eq!(
            next.load(Self::key())
                .await
                .expect("fresh nested read")
                .expect("nested record")
                .record
                .value
                .as_ref(),
            &Self::value()
        );
        next.put(StateRecord::new(unrelated.clone(), serde_json::json!(true)))
            .await
            .expect("stage unrelated progress");
        next.commit().await.expect("commit unrelated progress");
        assert_eq!(
            store
                .load(unrelated)
                .await
                .expect("unrelated read")
                .expect("unrelated record")
                .revision
                .get(),
            1
        );
        Self::verify_read(store).await;
    }
}

/// A decoder stack overflow must fail its isolated test, never abort the suite.
struct JsonStackProcess {
    directory: tempfile::TempDir,
    child: Option<Child>,
}

impl JsonStackProcess {
    fn spawn(mode: &str) -> Self {
        let directory = tempfile::tempdir().expect("owned JSON stack fixture");
        let output = File::create(directory.path().join("child.log")).expect("child log");
        let child = Command::new("/bin/sh")
            .args(["-c", "ulimit -c 0; exec \"$@\"", "lance-json-stack-child"])
            .arg(std::env::current_exe().expect("integration executable"))
            .args([
                "lancedb_state_failures::owned_json_stack_process_entry",
                "--exact",
                "--nocapture",
            ])
            .env("AI_KIT_LANCE_JSON_STACK_ROOT", directory.path())
            .env("AI_KIT_LANCE_JSON_STACK_MODE", mode)
            .stdin(Stdio::null())
            .stdout(output.try_clone().expect("child stdout"))
            .stderr(output)
            .spawn()
            .expect("owned JSON stack process");
        Self {
            directory,
            child: Some(child),
        }
    }

    fn verify(mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("owned JSON child")
                .try_wait()
                .expect("observe JSON child")
            {
                self.child
                    .take()
                    .expect("completed JSON child")
                    .wait()
                    .expect("reap JSON child");
                let output = std::fs::read_to_string(self.directory.path().join("child.log"))
                    .expect("JSON child log");
                assert!(
                    self.directory.path().join("acknowledged").is_file(),
                    "child did not acknowledge the deep record: {status}\n{output}"
                );
                assert!(
                    status.success(),
                    "acknowledged JSON must decode and allow progress: {status}\n{output}"
                );
                assert!(
                    self.directory.path().join("completed").is_file(),
                    "child must join its store and runtime before completion\n{output}"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "JSON child watchdog elapsed; owned process will be killed and reaped"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn key() -> StateKey {
        StateKey::new("stack-json", "nested").expect("nested key")
    }

    fn assert_shape(value: &serde_json::Value) {
        let mut cursor = value;
        for _ in 0..1536 {
            let array = cursor.as_array().expect("nested array");
            assert_eq!(array.len(), 1);
            cursor = &array[0];
        }
        assert!(cursor.is_null(), "nested leaf remains null");
    }

    async fn read(store: &StateStore) {
        let observed = store
            .load(Self::key())
            .await
            .expect("deep read")
            .expect("deep record");
        assert_eq!(observed.revision.get(), 1);
        Self::assert_shape(observed.record.value.as_ref());
        let page = store
            .list(StateListRequest::new("stack-json").expect("namespace"))
            .await
            .expect("deep list");
        assert_eq!(page.records.len(), 1);
        Self::assert_shape(page.records[0].record.value.as_ref());
        DeepJsonCases::dispose(observed.record.value);
        for record in page.records {
            DeepJsonCases::dispose(record.record.value);
        }
    }

    async fn exercise(root: &Path, mode: &str) {
        let config = match mode {
            "memory" => LanceStateStoreConfig::memory(),
            "disk" => LanceStateStoreConfig::directory(root.join("store")),
            _ => panic!("unknown JSON child mode"),
        };
        let store = StateStore::connect(StateStoreConfig::Lance(config.clone()))
            .await
            .expect("real supplied store");
        let mut value = serde_json::Value::Null;
        for _ in 0..1536 {
            value = serde_json::Value::Array(vec![value]);
        }
        let mut transaction = store.begin_transaction().await.expect("deep transaction");
        transaction
            .put(StateRecord::new(Self::key(), value))
            .await
            .expect("stage deep value");
        transaction.commit().await.expect("acknowledge deep value");
        std::fs::write(root.join("acknowledged"), b"committed").expect("acknowledgment marker");
        Self::read(&store).await;

        let unrelated = StateKey::new("unrelated", "after-deep-json").expect("progress key");
        let mut next = store.begin_transaction().await.expect("fresh transaction");
        next.put(StateRecord::new(unrelated.clone(), serde_json::json!(true)))
            .await
            .expect("stage unrelated progress");
        next.commit().await.expect("commit unrelated progress");
        let retained = match mode {
            "memory" => {
                let retained = store.clone();
                drop(store);
                retained
            }
            "disk" => {
                drop(store);
                StateStore::connect(StateStoreConfig::Lance(config))
                    .await
                    .expect("reopen disk store with deep JSON")
            }
            _ => unreachable!(),
        };
        Self::read(&retained).await;
        assert!(
            retained
                .load(unrelated)
                .await
                .expect("progress read")
                .is_some()
        );
        let mut cleanup = retained
            .begin_transaction()
            .await
            .expect("cleanup transaction");
        cleanup
            .delete(Self::key())
            .await
            .expect("delete nested record");
        cleanup.commit().await.expect("commit nested cleanup");
        assert!(
            retained
                .load(Self::key())
                .await
                .expect("deleted deep read")
                .is_none()
        );
        drop(retained);
        DeepJsonCases::exercise(root, mode).await;
    }
}

impl Drop for JsonStackProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn owned_json_stack_process_entry() {
    let Ok(root) = std::env::var("AI_KIT_LANCE_JSON_STACK_ROOT") else {
        return;
    };
    let mode = std::env::var("AI_KIT_LANCE_JSON_STACK_MODE").expect("selected JSON child mode");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("JSON child caller runtime");
    runtime.block_on(JsonStackProcess::exercise(Path::new(&root), &mode));
    drop(runtime);
    std::fs::write(Path::new(&root).join("completed"), b"owners joined")
        .expect("completion marker");
}

#[test]
fn memory_acknowledged_deep_json_survives_decoding_on_the_owned_worker() {
    JsonStackProcess::spawn("memory").verify();
}

#[test]
fn disk_acknowledged_deep_json_survives_decoding_and_reopen_on_the_owned_worker() {
    JsonStackProcess::spawn("disk").verify();
}

struct FloatingPointJsonCase;

impl FloatingPointJsonCase {
    fn key() -> StateKey {
        StateKey::new("json-numbers", "finite-f64").expect("floating point key")
    }

    fn bits() -> [u64; 5] {
        // The first three finite values exercise serde_json's documented
        // non-roundtripping default-parser cases; signed zero and the smallest
        // positive subnormal also retain their exact public Value semantics.
        [
            0x4049_9fc4_4f1b_2f60,
            0xc057_53ed_7e04_693b,
            0x0b77_7f24_5544_ff83,
            (-0.0_f64).to_bits(),
            1,
        ]
    }

    fn value() -> serde_json::Value {
        serde_json::Value::Array(
            Self::bits()
                .into_iter()
                .map(|bits| serde_json::Value::from(f64::from_bits(bits)))
                .collect(),
        )
    }

    async fn verify_read(store: &StateStore) {
        let observed = store
            .load(Self::key())
            .await
            .expect("committed finite numbers remain readable")
            .expect("finite number record");
        assert_eq!(observed.revision.get(), 1);
        let values = observed.record.value.as_array().expect("number array");
        assert_eq!(values.len(), Self::bits().len());
        for (value, expected) in values.iter().zip(Self::bits()) {
            assert_eq!(
                value.as_f64().expect("finite f64").to_bits(),
                expected,
                "committed JSON numbers must not change their exact value"
            );
        }
    }

    async fn verify_roundtrip(store: &StateStore) {
        let mut transaction = store.begin_transaction().await.expect("number transaction");
        transaction
            .put(StateRecord::new(Self::key(), Self::value()))
            .await
            .expect("stage caller-constructed finite numbers");
        transaction.commit().await.expect("commit finite numbers");
        Self::verify_read(store).await;
    }
}

#[tokio::test]
async fn memory_json_numbers_retain_exact_floating_point_values() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("memory store");
    FloatingPointJsonCase::verify_roundtrip(&store).await;
}

#[tokio::test]
async fn disk_json_numbers_retain_exact_floating_point_values_after_reopen() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    FloatingPointJsonCase::verify_roundtrip(&store).await;
    drop(store);
    FloatingPointJsonCase::verify_read(&fixture.connect().await).await;
}

#[tokio::test]
async fn memory_nested_json_roundtrips_beyond_default_parser_depth() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("memory store");
    NestedJsonCase::verify_roundtrip_and_progress(&store).await;
    let retained = store.clone();
    drop(store);
    NestedJsonCase::verify_read(&retained).await;
}

#[tokio::test]
async fn disk_nested_json_roundtrips_and_reopens_beyond_default_parser_depth() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    NestedJsonCase::verify_roundtrip_and_progress(&store).await;
    drop(store);
    let reopened = fixture.connect().await;
    NestedJsonCase::verify_read(&reopened).await;
    let unrelated = StateKey::new("after-reopen", "progress").expect("unrelated key");
    let mut transaction = reopened
        .begin_transaction()
        .await
        .expect("reopened transaction");
    transaction
        .put(StateRecord::new(unrelated.clone(), serde_json::json!(true)))
        .await
        .expect("stage after reopen");
    transaction.commit().await.expect("progress after reopen");
    assert!(
        reopened
            .load(unrelated)
            .await
            .expect("after reopen read")
            .is_some()
    );
    NestedJsonCase::verify_read(&reopened).await;
}

#[tokio::test]
async fn supplied_tables_use_runtime_owned_encoding_format_for_initialization_and_delta() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let initial = fixture.open_native().await;
    assert_eq!(
        initial
            .manifest()
            .data_storage_format
            .lance_file_format()
            .to_data_file_numbers(),
        (2, 0)
    );
    assert!(!initial.manifest().uses_stable_row_ids());
    let mut transaction = store.begin_transaction().await.expect("delta transaction");
    transaction
        .put(StateRecord::new(
            StateKey::new("format", "delta").expect("key"),
            serde_json::json!({"committed": true}),
        ))
        .await
        .expect("stage delta");
    transaction.commit().await.expect("commit delta");
    let current = fixture.open_native().await;
    assert_eq!(current.version_id(), initial.version_id() + 1);
    assert_eq!(
        current
            .manifest()
            .data_storage_format
            .lance_file_format()
            .to_data_file_numbers(),
        (2, 0)
    );
    assert!(!current.manifest().uses_stable_row_ids());
}

#[tokio::test]
async fn incompatible_native_encoding_or_row_ids_are_rejected_on_every_fresh_open() {
    for (format, stable_row_ids) in [("2.1", false), ("2.0", true)] {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        fixture.replace_native_format(format, stable_row_ids).await;
        let outcome = store
            .load(StateKey::new("format", "missing").expect("key"))
            .await;
        assert!(
            matches!(&outcome, Err(StateStoreError::BackendOperation { .. })),
            "fresh read must reject format={format}, stable_row_ids={stable_row_ids}: {outcome:?}"
        );
        assert!(
            matches!(
                StateStore::connect(fixture.config()).await,
                Err(StateStoreError::BackendOperation { .. })
            ),
            "reconnect must reject format={format}, stable_row_ids={stable_row_ids}"
        );
        assert_eq!(
            fixture.open_native().await.version_id(),
            1,
            "must not migrate or repair foreign encoding"
        );
    }
}

#[tokio::test]
async fn revision_overflow_invalidates_all_earlier_staged_mutations() {
    for delete in [false, true] {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        let version = fixture.publish(NativeFault::RevisionBoundary).await;
        let key = StateKey::new("fault", "record").expect("record key");
        let earlier = StateKey::new("fault", "earlier").expect("earlier key");
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("boundary transaction");
        let observed = transaction
            .load(key.clone())
            .await
            .expect("read")
            .expect("boundary record");
        assert_eq!(observed.revision.get(), u64::MAX);
        transaction
            .put(StateRecord::new(earlier.clone(), serde_json::json!(true)))
            .await
            .expect("stage earlier mutation");
        let failed = if delete {
            transaction.delete(key.clone()).await
        } else {
            transaction
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!("replacement"),
                ))
                .await
        };
        assert!(matches!(failed, Err(StateStoreError::InvalidRequest(_))));
        assert!(
            transaction.commit().await.is_err(),
            "failed mutation poisons commit"
        );
        assert!(store.load(earlier).await.expect("earlier read").is_none());
        let record = store
            .load(key)
            .await
            .expect("boundary read")
            .expect("record");
        assert_eq!(record.revision.get(), u64::MAX);
        assert_eq!(record.record.value.as_ref(), &serde_json::json!("original"));
        assert_eq!(fixture.open_native().await.version_id(), version);
    }
}

#[tokio::test]
async fn malformed_native_state_is_rejected_without_repair_or_empty_success() {
    for fault in [
        NativeFault::Json,
        NativeFault::Schema,
        NativeFault::MissingControl,
        NativeFault::DuplicateControl,
        NativeFault::DuplicateRecord,
        NativeFault::Identity,
        NativeFault::Target,
    ] {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        let version = fixture.publish(fault).await;
        let key = StateKey::new("fault", "record").expect("key");
        assert!(
            matches!(
                store.load(key).await,
                Err(StateStoreError::BackendOperation { .. })
            ),
            "load must reject {fault:?}"
        );
        assert!(
            matches!(
                StateStore::connect(fixture.config()).await,
                Err(StateStoreError::BackendOperation { .. })
            ),
            "reopen must reject {fault:?}"
        );
        assert_eq!(
            fixture.open_native().await.version_id(),
            version,
            "must not heal {fault:?}"
        );
    }
}

#[tokio::test]
async fn read_only_operations_do_not_publish_native_versions() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let version = fixture.open_native().await.version_id();
    let key = StateKey::new("read-only", "absent").expect("key");
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("read-only transaction");
    assert!(
        transaction
            .load(key.clone())
            .await
            .expect("snapshot read")
            .is_none()
    );
    transaction.commit().await.expect("read-only commit");
    assert!(store.load(key).await.expect("standalone read").is_none());
    assert!(
        store
            .list(StateListRequest::new("read-only").expect("namespace"))
            .await
            .expect("page")
            .records
            .is_empty()
    );
    assert_eq!(fixture.open_native().await.version_id(), version);
}

#[tokio::test]
async fn native_metadata_advance_rejects_a_supplied_transactions_entire_overlay() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let changed = StateKey::new("maintenance", "changed").expect("changed key");
    let deleted = StateKey::new("maintenance", "deleted").expect("deleted key");
    let receipt = StateKey::new("maintenance", "receipt").expect("receipt key");
    let mut seed = store.begin_transaction().await.expect("seed transaction");
    for key in [&changed, &deleted] {
        seed.put(StateRecord::new(key.clone(), serde_json::json!("original")))
            .await
            .expect("seed record");
    }
    seed.commit().await.expect("seed commit");

    let mut stale = store.begin_transaction().await.expect("original snapshot");
    assert!(
        stale
            .load(receipt.clone())
            .await
            .expect("receipt read")
            .is_none()
    );
    stale
        .put(StateRecord::new(
            changed.clone(),
            serde_json::json!("replacement"),
        ))
        .await
        .expect("stage replacement");
    stale.delete(deleted).await.expect("stage deletion");
    stale
        .put(StateRecord::new(receipt, serde_json::json!(true)))
        .await
        .expect("stage receipt");

    // This real native maintenance changes only manifest metadata. In
    // particular, the control row is untouched, so row-overlap checking alone
    // cannot prove that the supplied transaction still owns original V+1.
    let mut native = Dataset::open(&fixture.uri())
        .await
        .expect("native maintenance view");
    let original_version = native.version_id();
    let original_rows = native
        .scan()
        .try_into_batch()
        .await
        .expect("original native rows");
    assert_eq!(
        original_rows.num_rows(),
        3,
        "control and two original records"
    );
    native
        .update_metadata([("fixture.native-maintenance", "complete")])
        .await
        .expect("publish native metadata maintenance");
    let maintenance_version = native.version_id();
    assert_eq!(maintenance_version, original_version + 1);
    assert_eq!(
        native
            .scan()
            .try_into_batch()
            .await
            .expect("maintenance rows"),
        original_rows
    );

    let outcome = stale.commit().await;
    assert!(
        matches!(&outcome, Err(StateStoreError::TransactionConflict { .. })),
        "the exact-version guard must reject rebasing across maintenance: {outcome:?}"
    );
    let current = fixture.open_native().await;
    assert_eq!(
        current.version_id(),
        maintenance_version,
        "no stale version was published"
    );
    assert_eq!(
        current
            .metadata()
            .get("fixture.native-maintenance")
            .map(String::as_str),
        Some("complete")
    );
    assert_eq!(
        current
            .scan()
            .try_into_batch()
            .await
            .expect("after rejected overlay"),
        original_rows,
        "the stale put, delete, and receipt must all remain unpublished"
    );
    // The foreign manifest does not carry the supplied format's transaction
    // identity. A newly opened public read must detect it instead of repairing
    // or blessing externally maintained state.
    assert!(matches!(
        store.load(changed).await,
        Err(StateStoreError::BackendOperation { .. })
    ));
    assert_eq!(
        fixture.open_native().await.version_id(),
        maintenance_version
    );
}

#[tokio::test]
async fn equal_puts_and_repeated_tombstone_deletes_each_advance_revisions() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let key = StateKey::new("revision", "repeated-mutation").expect("key");
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("initial transaction");
    for _ in 0..3 {
        transaction
            .delete(key.clone())
            .await
            .expect("delete absent or staged tombstone");
    }
    for _ in 0..2 {
        transaction
            .put(StateRecord::new(key.clone(), serde_json::json!("equal")))
            .await
            .expect("equal-value put");
    }
    assert_eq!(
        transaction
            .load(key.clone())
            .await
            .expect("staged read")
            .expect("present")
            .revision
            .get(),
        5
    );
    transaction.commit().await.expect("initial commit");
    assert_eq!(
        store
            .load(key.clone())
            .await
            .expect("committed read")
            .expect("present")
            .revision
            .get(),
        5
    );

    let mut tombstone = store.begin_transaction().await.expect("delete transaction");
    tombstone.delete(key.clone()).await.expect("delete present");
    tombstone
        .delete(key.clone())
        .await
        .expect("delete staged tombstone");
    tombstone
        .commit()
        .await
        .expect("commit revision-seven tombstone");
    assert!(
        store
            .load(key.clone())
            .await
            .expect("deleted read")
            .is_none()
    );

    let mut recreate = store
        .begin_transaction()
        .await
        .expect("recreate transaction");
    recreate
        .delete(key.clone())
        .await
        .expect("delete retained tombstone");
    for _ in 0..2 {
        recreate
            .put(StateRecord::new(key.clone(), serde_json::json!("equal")))
            .await
            .expect("recreate with equal values");
    }
    assert_eq!(
        recreate
            .load(key.clone())
            .await
            .expect("recreated read")
            .expect("present")
            .revision
            .get(),
        10
    );
    recreate.commit().await.expect("commit recreation");
    let observed = store.load(key).await.expect("final read").expect("present");
    assert_eq!(observed.revision.get(), 10);
    assert_eq!(observed.record.value.as_ref(), &serde_json::json!("equal"));
}

#[tokio::test]
async fn missing_manifests_do_not_reinitialize_existing_state_history() {
    for remove_hint in [false, true] {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("history transaction");
        transaction
            .put(StateRecord::new(
                StateKey::new("history", "retained").expect("key"),
                serde_json::json!("must not silently disappear"),
            ))
            .await
            .expect("stage history");
        transaction.commit().await.expect("publish history");
        drop(store);
        let versions = fixture
            .directory
            .path()
            .join("__ai_kit_state.lance/_versions");
        assert!(
            versions.join("latest_version_hint.json").exists(),
            "native version hint is evidence of prior publication"
        );
        let manifests = std::fs::read_dir(&versions)
            .expect("native manifests directory")
            .map(|entry| entry.expect("manifest entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "manifest")
            })
            .collect::<Vec<_>>();
        assert!(
            manifests.len() >= 2,
            "retain evidence of two committed versions"
        );
        for manifest in manifests {
            std::fs::remove_file(manifest).expect("inject missing authority");
        }
        if remove_hint {
            std::fs::remove_file(versions.join("latest_version_hint.json"))
                .expect("also remove the nonauthoritative hint");
        }
        assert!(
            matches!(
                StateStore::connect(fixture.config()).await,
                Err(StateStoreError::BackendOperation { .. })
            ),
            "missing authority must not bootstrap a new empty state history"
        );
    }
}

struct DeepJsonCases;

impl DeepJsonCases {
    fn value(shape: usize) -> serde_json::Value {
        let mut value = serde_json::json!("leaf\n🦀");
        for level in (0..4096).rev() {
            value = if shape == 0 || (shape == 2 && level % 2 == 0) {
                serde_json::Value::Array(vec![value])
            } else {
                serde_json::Value::Object(serde_json::Map::from_iter([(
                    "nested".to_owned(),
                    value,
                )]))
            };
        }
        value
    }

    fn assert_value(value: &serde_json::Value, shape: usize) {
        let mut cursor = value;
        for level in 0..4096 {
            cursor = if shape == 0 || (shape == 2 && level % 2 == 0) {
                let values = cursor.as_array().expect("deep array");
                assert_eq!(values.len(), 1);
                &values[0]
            } else {
                let values = cursor.as_object().expect("deep object");
                assert_eq!(values.len(), 1);
                values.get("nested").expect("deep member")
            };
        }
        assert_eq!(cursor.as_str(), Some("leaf\n🦀"));
    }

    fn dispose(value: Arc<serde_json::Value>) {
        if let Some(value) = Arc::into_inner(value) {
            let mut pending = vec![value];
            while let Some(value) = pending.pop() {
                match value {
                    serde_json::Value::Array(values) => pending.extend(values),
                    serde_json::Value::Object(values) => {
                        pending.extend(values.into_iter().map(|(_, value)| value))
                    }
                    _ => {}
                }
            }
        }
    }

    async fn exercise(root: &Path, mode: &str) {
        for shape in 0..3 {
            let config = if mode == "memory" {
                LanceStateStoreConfig::memory()
            } else {
                LanceStateStoreConfig::directory(root.join(format!("shape-{shape}")))
            };
            let store = StateStore::connect(StateStoreConfig::Lance(config.clone()))
                .await
                .expect("deep shape store");
            let key = StateKey::new("deep", "shape").expect("shape key");
            let mut unpolled = store
                .begin_transaction()
                .await
                .expect("unpolled deep transaction");
            let unpolled_key = key.clone();
            std::thread::Builder::new()
                .name("small-stack-public-put".to_owned())
                .stack_size(256 * 1024)
                .spawn(move || {
                    drop(unpolled.put(StateRecord::new(unpolled_key, Self::value(shape))));
                    drop(unpolled);
                })
                .expect("owned small-stack caller")
                .join()
                .expect("unpolled public put releases its input safely");
            let mut transaction = store.begin_transaction().await.expect("shape transaction");
            transaction
                .put(StateRecord::new(key.clone(), Self::value(shape)))
                .await
                .expect("shape put");
            transaction.commit().await.expect("shape commit");
            let mut transaction = store.begin_transaction().await.expect("deep snapshot");
            let observed = transaction
                .load(key.clone())
                .await
                .expect("transaction read")
                .expect("shape");
            Self::assert_value(&observed.record.value, shape);
            Self::dispose(observed.record.value);
            transaction
                .put(StateRecord::new(key.clone(), Self::value(shape)))
                .await
                .expect("first replacement");
            transaction
                .put(StateRecord::new(key.clone(), Self::value(shape)))
                .await
                .expect("replace owned overlay");
            transaction.rollback().await.expect("deep rollback");
            let mut abandoned = store.begin_transaction().await.expect("drop snapshot");
            abandoned
                .put(StateRecord::new(key.clone(), Self::value(shape)))
                .await
                .expect("drop overlay");
            drop(abandoned);

            // Poll idle-store observers once; Pending demonstrates admitted work
            // whose private reply may subsequently have no receiving owner.
            let waker = futures::task::noop_waker();
            let mut context = std::task::Context::from_waker(&waker);
            let mut pending_loads = 0;
            let mut pending_lists = 0;
            let mut pending_begins = 0;
            for _ in 0..8 {
                let mut load = Box::pin(store.load(key.clone()));
                match std::future::Future::poll(load.as_mut(), &mut context) {
                    std::task::Poll::Ready(result) => {
                        if let Some(record) = result.expect("load outcome") {
                            Self::dispose(record.record.value);
                        }
                    }
                    std::task::Poll::Pending => pending_loads += 1,
                }
                drop(load);
                let mut list =
                    Box::pin(store.list(StateListRequest::new("deep").expect("list request")));
                match std::future::Future::poll(list.as_mut(), &mut context) {
                    std::task::Poll::Ready(result) => {
                        for record in result.expect("page outcome").records {
                            Self::dispose(record.record.value);
                        }
                    }
                    std::task::Poll::Pending => pending_lists += 1,
                }
                drop(list);
                let mut begin = Box::pin(store.begin_transaction());
                match std::future::Future::poll(begin.as_mut(), &mut context) {
                    std::task::Poll::Ready(result) => drop(result.expect("snapshot outcome")),
                    std::task::Poll::Pending => pending_begins += 1,
                }
                drop(begin);
            }
            assert!(
                pending_loads > 0 && pending_lists > 0 && pending_begins > 0,
                "each private reply owner needs an abandoned Pending observer witness"
            );
            let observed = store
                .load(key.clone())
                .await
                .expect("progress after abandoned replies")
                .expect("shape");
            Self::assert_value(&observed.record.value, shape);
            Self::dispose(observed.record.value);
            if mode == "disk" {
                let reopened = StateStore::connect(StateStoreConfig::Lance(config))
                    .await
                    .expect("deep reopen");
                let observed = reopened
                    .load(key.clone())
                    .await
                    .expect("reopened read")
                    .expect("shape");
                Self::assert_value(&observed.record.value, shape);
                Self::dispose(observed.record.value);
            }
            let mut replacement = store
                .begin_transaction()
                .await
                .expect("replace deep persisted");
            replacement
                .put(StateRecord::new(key.clone(), serde_json::json!(false)))
                .await
                .expect("replace deep");
            replacement.commit().await.expect("replacement commit");
            let mut deletion = store.begin_transaction().await.expect("delete deep");
            deletion
                .put(StateRecord::new(key.clone(), Self::value(shape)))
                .await
                .expect("stage deep again");
            deletion.delete(key).await.expect("drop deep staged value");
            deletion.commit().await.expect("deep deletion commit");
            drop(store);
        }
        Self::native_documents().await;
    }

    async fn native_documents() {
        let fixture = NativeFixture::new();
        let store = fixture.connect().await;
        let key = StateKey::new("fault", "record").expect("native document key");
        let deep = format!("{}null{}", "[".repeat(4096), "]".repeat(4096));
        for bytes in [
            format!("{deep} trailing").into_bytes(),
            format!("{}null", "[".repeat(4096)).into_bytes(),
            format!("{{\"first\":{deep},\"broken\":}}").into_bytes(),
        ] {
            fixture
                .publish_content(NativeFault::Json, Some(bytes))
                .await;
            assert!(matches!(
                store.load(key.clone()).await,
                Err(StateStoreError::BackendOperation { .. })
            ));
        }
        fixture
            .publish_content(
                NativeFault::Json,
                Some(format!("{{\"same\":{deep},\"same\":true}}").into_bytes()),
            )
            .await;
        let observed = store
            .load(key.clone())
            .await
            .expect("duplicate key parse")
            .expect("last key wins");
        assert_eq!(
            observed.record.value.as_ref(),
            &serde_json::json!({"same":true})
        );
        fixture.publish(NativeFault::RevisionBoundary).await;
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("overflow transaction");
        transaction
            .put(StateRecord::new(
                StateKey::new("earlier", "deep").expect("key"),
                Self::value(2),
            ))
            .await
            .expect("earlier deep stage");
        assert!(matches!(
            transaction.put(StateRecord::new(key, Self::value(1))).await,
            Err(StateStoreError::InvalidRequest(_))
        ));
        assert!(matches!(
            transaction.commit().await,
            Err(StateStoreError::InvalidRequest(_))
        ));
        assert!(
            store
                .load(StateKey::new("earlier", "deep").expect("key"))
                .await
                .expect("invalidated overlay")
                .is_none()
        );
    }
}

#[tokio::test]
async fn persisted_json_grammar_matches_pinned_serde_and_preserves_existing_bytes() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let key = StateKey::new("fault", "record").expect("document key");
    let documents = [
        "null",
        "true",
        "false",
        "0",
        "-0.0",
        "18446744073709551615",
        "-9223372036854775808",
        "1e-300",
        "2.2250738585072014e-308",
        "[1,2,3]",
        "{}",
        "[]",
        r#"{"z":1,"a":{"x":[],"y":{}}}"#,
        r#"{"z":1,"a":2,"z":3}"#,
        r#""\"\\\/\b\f\n\r\t\u0000\uD83E\uDD80é""#,
        " \n [true, null, {\"key\":\"value\"}] \t\r",
        "",
        " ",
        "[",
        "{",
        "[1,]",
        "{\"a\":1,}",
        "[1 2]",
        "{\"a\" 1}",
        "[1:2]",
        "true false",
        "01",
        "1.",
        "1e",
        "NaN",
        "\"\\uD800\"",
        "[[]]x",
        "{}[]",
        "{1:true}",
    ];
    for document in documents {
        fixture
            .publish_content(NativeFault::Json, Some(document.as_bytes().to_vec()))
            .await;
        match serde_json::from_str::<serde_json::Value>(document) {
            Ok(expected) => {
                let observed = store
                    .load(key.clone())
                    .await
                    .expect("valid persisted JSON")
                    .expect("value");
                assert_eq!(
                    serde_json::to_vec(observed.record.value.as_ref()).expect("shallow bytes"),
                    serde_json::to_vec(&expected).expect("reference bytes"),
                    "document {document}"
                );
                let mut transaction = store.begin_transaction().await.expect("encode transaction");
                transaction
                    .put(StateRecord::new(key.clone(), expected.clone()))
                    .await
                    .expect("encode staged");
                transaction
                    .commit()
                    .await
                    .expect("iterative encoded commit");
                assert_eq!(
                    fixture.stored_json().await,
                    serde_json::to_vec(&expected).expect("pinned encoded representation")
                );
                drop(observed);
                let reopened = fixture.connect().await;
                assert_eq!(
                    reopened
                        .load(key.clone())
                        .await
                        .expect("reopen JSON")
                        .expect("value")
                        .record
                        .value
                        .as_ref(),
                    &expected,
                    "document {document}"
                );
            }
            Err(_) => assert!(
                matches!(
                    store.load(key.clone()).await,
                    Err(StateStoreError::BackendOperation { .. })
                ),
                "invalid document {document}"
            ),
        }
    }
}

#[tokio::test]
async fn ordinary_filesystem_obstruction_fails_without_publication_and_allows_progress() {
    let fixture = NativeFixture::new();
    let store = fixture.connect().await;
    let key = StateKey::new("filesystem", "value").expect("key");
    let receipt = StateKey::new("filesystem", "receipt").expect("receipt");
    let mut transaction = store.begin_transaction().await.expect("original snapshot");
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!(true)))
        .await
        .expect("stage data");
    transaction
        .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
        .await
        .expect("stage receipt");
    let original_version = fixture.open_native().await.version_id();
    // This disposable native fixture blocks data I/O using ordinary filesystem
    // operations. No permissions, syscall behavior or SDK hooks are changed.
    let data = fixture.directory.path().join("__ai_kit_state.lance/data");
    let saved = fixture.directory.path().join("saved-native-data");
    std::fs::rename(&data, &saved).expect("retain all original data files");
    std::fs::write(&data, b"owned obstruction").expect("block data directory");
    let outcome = transaction.commit().await;
    std::fs::remove_file(&data).expect("remove owned obstruction");
    std::fs::rename(&saved, &data).expect("restore original data files");
    assert!(
        matches!(outcome, Err(StateStoreError::BackendOperation { .. })),
        "I/O failure must remain an uncertain backend error: {outcome:?}"
    );
    assert_eq!(fixture.open_native().await.version_id(), original_version);
    let mut verifier = store
        .begin_transaction()
        .await
        .expect("settled worker can read again");
    assert!(
        verifier
            .load(key.clone())
            .await
            .expect("data read")
            .is_none()
    );
    assert!(
        verifier
            .load(receipt.clone())
            .await
            .expect("receipt read")
            .is_none()
    );
    verifier
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!("recovered"),
        ))
        .await
        .expect("subsequent write");
    verifier.commit().await.expect("subsequent commit");
    let reopened = fixture.connect().await;
    let mut observed = reopened
        .begin_transaction()
        .await
        .expect("fresh atomic verification");
    let recovered = observed.load(key).await.expect("data").expect("value");
    assert_eq!(recovered.revision.get(), 1);
    assert_eq!(
        recovered.record.value.as_ref(),
        &serde_json::json!("recovered")
    );
    assert!(
        observed
            .load(receipt)
            .await
            .expect("no failed receipt")
            .is_none()
    );
    observed.rollback().await.expect("read cleanup");
}
