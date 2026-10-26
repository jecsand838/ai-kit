use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File, FileTimes, OpenOptions},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow_array::{
    BooleanArray, LargeBinaryArray, RecordBatch, RecordBatchIterator, StringArray, UInt8Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use lance::{
    Dataset,
    dataset::{
        CommitBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched,
        WhenNotMatchedBySource, WriteMode, WriteParams,
    },
};
use serde_json::{Value, json};
use storage_sdk::{
    Database, DatabaseBackend, StateStore, StateStoreBackend, StateStoreError,
    StateTransactionBackend,
    database::{
        StorageLocator,
        catalog::{CatalogRequest, CreateCollectionMode, CreateCollectionRequest},
        config::{DatabaseConfig, LanceConfig},
        data::{CountRequest, DataRecord, DataRequest, DataResponse},
        query::{Projection, QueryRequest, ScrollRequest},
    },
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateRecord},
    },
};
use tempfile::TempDir;

struct RecoveryCase {
    directory: TempDir,
}

#[derive(Clone, Copy)]
enum PartialCleanup {
    SuccessorGap,
    RemovedHintTarget,
    MissingHint,
}

struct RecoveryProcess {
    child: Child,
    original_data: BTreeSet<PathBuf>,
    original_manifests: BTreeSet<PathBuf>,
}

impl RecoveryCase {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("owned maintenance recovery fixture"),
        }
    }

    fn table(&self) -> PathBuf {
        self.directory.path().join("__ai_kit_state.lance")
    }

    fn key(key: &str) -> StateKey {
        StateKey::new("maintenance-recovery", key).expect("fixture key")
    }

    async fn connect_at(directory: &Path) -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(directory)
                .with_operation_timeout(Duration::from_secs(120))
                .expect("fixture timeout"),
        ))
        .await
        .expect("supplied Lance StateStore")
    }

    async fn connect(&self) -> StateStore {
        Self::connect_at(self.directory.path()).await
    }

    async fn native(&self) -> Dataset {
        Dataset::open(self.table().to_str().expect("native path"))
            .await
            .expect("native read-only evidence")
    }

    fn shared_lease(&self) -> File {
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(
                self.directory
                    .path()
                    .join("__ai_kit_state.maintenance.lock"),
            )
            .expect("ordinary coordination file");
        lease
            .lock_shared()
            .expect("defer maintenance during fixture preparation");
        lease
    }

    async fn write(store: &StateStore, sequence: u64) {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("fixture transaction");
        transaction
            .put(StateRecord::new(Self::key("head"), json!(sequence)))
            .await
            .expect("stage head");
        transaction.commit().await.expect("publish head");
    }

    fn native_files(&self) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();
        for directory in ["data", "_deletions", "_transactions"] {
            let entries = match fs::read_dir(self.table().join(directory)) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => panic!("owned native inventory failed: {error}"),
            };
            for entry in entries {
                let path = entry.expect("owned native entry").path();
                if path.is_file() {
                    files.insert(path.clone(), fs::read(path).expect("owned native bytes"));
                }
            }
        }
        files
    }

    async fn prepare_orphan_delta(&self, dataset: Arc<Dataset>) {
        let token = uuid::Uuid::new_v4().to_string();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::from(dataset.schema())),
            vec![
                Arc::new(UInt8Array::from(vec![1, 0])),
                Arc::new(StringArray::from(vec!["", "maintenance-recovery"])),
                Arc::new(StringArray::from(vec!["", "kept-a"])),
                Arc::new(UInt64Array::from(vec![0, 2])),
                Arc::new(BooleanArray::from(vec![false, true])),
                Arc::new(LargeBinaryArray::from_iter([
                    None,
                    Some(br#""never published""#.as_slice()),
                ])),
                Arc::new(StringArray::from(vec![token.as_str(), ""])),
            ],
        )
        .expect("uncommitted orphan fixture batch");
        let source = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut merge = MergeInsertBuilder::try_new(
            dataset,
            vec!["kind".to_owned(), "namespace".to_owned(), "key".to_owned()],
        )
        .expect("uncommitted orphan fixture merge");
        merge
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
            .use_index(false);
        let delta = merge
            .try_build()
            .expect("uncommitted orphan fixture plan")
            .execute_uncommitted(source)
            .await
            .expect("create real unpublished native data and deletion files");
        drop(delta);
    }

    async fn publish_metadata_fixture(
        &self,
        dataset: Arc<Dataset>,
        batch: RecordBatch,
        token: String,
        overlay: bool,
    ) -> u64 {
        let target = dataset.version_id() + 1;
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            data_storage_version: Some("2.0".parse().expect("fixed native format")),
            enable_stable_row_ids: false,
            ..WriteParams::default()
        };
        let mut transaction = InsertBuilder::new(Arc::clone(&dataset))
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .expect("prepare disposable metadata fixture");
        if overlay {
            let lance::dataset::transaction::Operation::Overwrite { fragments, .. } =
                &mut transaction.operation
            else {
                panic!("fixture overwrite retains its concrete operation");
            };
            let fragment = fragments.first_mut().expect("native base fragment");
            let mut data_file = fragment.files[0].clone();
            let source = self.table().join("data").join(&data_file.path);
            let mut name = data_file.path.clone().into_bytes();
            let last = name.len() - ".lance".len() - 1;
            name[last] = if name[last] == b'0' { b'1' } else { b'0' };
            data_file.path = String::from_utf8(name).expect("native ASCII data filename");
            let destination = self.table().join("data").join(&data_file.path);
            assert!(!destination.exists(), "owned overlay fixture file is fresh");
            fs::copy(source, destination).expect("copy real native overlay storage");
            fragment
                .overlays
                .push(lance_table::format::overlay::DataOverlayFile {
                    data_file,
                    // Empty coverage does not alter any value, but remains a real
                    // native file reference that reclamation must not overlook.
                    coverage: lance_table::format::overlay::OverlayCoverage::Shared(Arc::default()),
                    committed_version: target,
                });
        }
        transaction.uuid = token;
        transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            "ai-kit.state.expected-version".to_owned(),
            target.to_string(),
        )])));
        let published = CommitBuilder::new(dataset)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .expect("publish disposable metadata fixture");
        assert_eq!(published.version_id(), target);
        target
    }

    fn manifests(&self) -> BTreeSet<PathBuf> {
        fs::read_dir(self.table().join("_versions"))
            .expect("native manifest inventory")
            .map(|entry| entry.expect("manifest entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "manifest")
            })
            .collect()
    }

    fn manifest(&self, version: u64) -> PathBuf {
        let versions = self.table().join("_versions");
        let v1 = versions.join(format!("{version}.manifest"));
        if v1.exists() {
            v1
        } else {
            versions.join(format!("{:020}.manifest", u64::MAX - version))
        }
    }

    async fn settle(&self, store: &StateStore) {
        for _ in 0..35 {
            store
                .load(Self::key("head"))
                .await
                .expect("ordinary progress after maintenance");
            if self.manifests().len() == 1 {
                return;
            }
        }
        assert_eq!(
            self.manifests().len(),
            1,
            "eligible quiescent history must be reclaimed"
        );
    }

    async fn seed_boundaries(&self) {
        let original = self.connect().await;
        drop(original);
        let dataset = Arc::new(self.native().await);
        let target = dataset.version_id() + 1;
        let token = uuid::Uuid::new_v4().to_string();
        let deep = format!("{}\"leaf🦀\"{}", "[".repeat(4096), "]".repeat(4096));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::from(dataset.schema())),
            vec![
                Arc::new(UInt8Array::from(vec![1, 0, 0, 0])),
                Arc::new(StringArray::from(vec![
                    "",
                    "maintenance-recovery",
                    "maintenance-recovery",
                    "maintenance-recovery",
                ])),
                Arc::new(StringArray::from(vec!["", "maximum", "deleted", "deep"])),
                Arc::new(UInt64Array::from(vec![0, u64::MAX, u64::MAX - 1, 17])),
                Arc::new(BooleanArray::from(vec![false, true, false, true])),
                Arc::new(LargeBinaryArray::from_iter([
                    None,
                    Some(b"true".as_slice()),
                    None,
                    Some(deep.as_bytes()),
                ])),
                Arc::new(StringArray::from(vec![token.as_str(), "", "", ""])),
            ],
        )
        .expect("native exact-boundary fixture");
        let source = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut merge = MergeInsertBuilder::try_new(
            Arc::clone(&dataset),
            vec!["kind".to_owned(), "namespace".to_owned(), "key".to_owned()],
        )
        .expect("native fixture merge");
        merge
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
            .use_index(false);
        let mut delta = merge
            .try_build()
            .expect("native fixture plan")
            .execute_uncommitted(source)
            .await
            .expect("prepare exact-boundary fixture");
        delta.transaction.uuid = token;
        delta.transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            "ai-kit.state.expected-version".to_owned(),
            target.to_string(),
        )])));
        let mut commit = CommitBuilder::new(dataset)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true);
        if let Some(affected) = delta.affected_rows {
            commit = commit.with_affected_rows(affected);
        }
        commit
            .execute(delta.transaction)
            .await
            .expect("publish disposable native fixture");
    }

    fn assert_deep(value: &Value) {
        let mut cursor = value;
        for _ in 0..4096 {
            let array = cursor.as_array().expect("retained deep array");
            assert_eq!(array.len(), 1);
            cursor = &array[0];
        }
        assert_eq!(cursor.as_str(), Some("leaf🦀"));
    }

    fn dispose(value: Arc<Value>) {
        if let Some(value) = Arc::into_inner(value) {
            let mut pending = vec![value];
            while let Some(value) = pending.pop() {
                match value {
                    Value::Array(values) => pending.extend(values),
                    Value::Object(values) => {
                        pending.extend(values.into_iter().map(|(_, value)| value))
                    }
                    _ => {}
                }
            }
        }
    }

    fn copy_directory(from: &Path, to: &Path) {
        let mut pending = vec![(from.to_owned(), to.to_owned())];
        while let Some((from, to)) = pending.pop() {
            fs::create_dir_all(&to).expect("cold destination directory");
            for entry in fs::read_dir(from).expect("cold source inventory") {
                let entry = entry.expect("cold file entry");
                let destination = to.join(entry.file_name());
                if entry.file_type().expect("cold file type").is_dir() {
                    pending.push((entry.path(), destination));
                } else {
                    fs::copy(entry.path(), destination).expect("copy complete cold file");
                }
            }
        }
    }

    async fn database(&self) -> Database {
        Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
            self.directory.path(),
        )))
        .await
        .expect("supplied knowledge Database")
    }
}

#[tokio::test]
async fn maintenance_recovery_orphan_only_removes_recent_native_files_and_retains_all_history() {
    let fixture = RecoveryCase::new();
    let store = fixture.connect().await;
    let mut seed = store.begin_transaction().await.expect("seed transaction");
    for key in ["kept-a", "kept-b", "head"] {
        seed.put(StateRecord::new(RecoveryCase::key(key), json!(key)))
            .await
            .expect("seed retained record");
    }
    seed.commit().await.expect("publish retained records");
    RecoveryCase::write(&store, 1).await;
    drop(store);

    let native = Arc::new(fixture.native().await);
    let manifests = fixture.manifests();
    assert!(manifests.len() > 1 && manifests.len() < 64);
    let versions = native.versions().await.expect("all committed versions");
    let mut history = Vec::new();
    let mut oldest_timestamp = u128::MAX;
    for version in &versions {
        let snapshot = native
            .checkout_version(version.version)
            .await
            .expect("retained version before orphan cleanup");
        oldest_timestamp = oldest_timestamp.min(snapshot.manifest().timestamp_nanos);
        let rows = snapshot
            .scan()
            .try_into_batch()
            .await
            .expect("retained historical rows before orphan cleanup");
        history.push((version.version, rows));
    }
    let retained_files = fixture.native_files();
    fixture.prepare_orphan_delta(Arc::clone(&native)).await;
    assert_eq!(fixture.native().await.version_id(), native.version_id());
    assert_eq!(fixture.manifests(), manifests);

    // Copy real transaction bytes to a distinct native transaction filename.
    // No manifest references this disposable file; no codec or commit handler
    // is substituted, and the actual published transaction file stays intact.
    let transaction_file = native
        .manifest()
        .transaction_file
        .as_ref()
        .expect("pinned commits retain a transaction file");
    let orphan_transaction = fixture.table().join("_transactions").join(format!(
        "{}-{}.txn",
        native.version_id(),
        uuid::Uuid::new_v4()
    ));
    fs::copy(
        fixture.table().join("_transactions").join(transaction_file),
        &orphan_transaction,
    )
    .expect("copy disposable unreferenced transaction fixture");
    let orphan_files = fixture
        .native_files()
        .into_keys()
        .filter(|path| !retained_files.contains_key(path))
        .collect::<BTreeSet<_>>();
    assert!(
        orphan_files
            .iter()
            .any(|path| path.extension().is_some_and(|ext| ext == "lance"))
    );
    assert!(orphan_files.iter().any(|path| {
        path.extension()
            .is_some_and(|ext| ext == "arrow" || ext == "bin")
    }));
    assert!(orphan_files.contains(&orphan_transaction));
    for path in &orphan_files {
        let modified = fs::metadata(path)
            .expect("actual orphan metadata")
            .modified()
            .expect("actual orphan timestamp")
            .duration_since(UNIX_EPOCH)
            .expect("post-epoch fixture timestamp")
            .as_nanos();
        assert!(modified > oldest_timestamp, "actual recent orphan witness");
    }
    let unknown = [
        fixture.table().join("data/keep-not-native.lance"),
        fixture.table().join("_deletions/keep-not-native.bin"),
        fixture.table().join("_transactions/keep-not-native.txn"),
        fixture.table().join("data/old-unknown.lance"),
        fixture.table().join("data/nested/keep-not-native.lance"),
        fixture.table().join("_deletions/old-unknown.bin"),
        fixture.table().join("_transactions/old-unknown.txn"),
        fixture.table().join("_indices/unknown-index/payload.bin"),
        fixture.table().join("_versions/.tmp-not-native"),
    ];
    for path in &unknown {
        fs::create_dir_all(path.parent().expect("unknown fixture parent"))
            .expect("unknown fixture directory");
        fs::write(path, b"unknown caller fixture remains untouched")
            .expect("unknown disposable filename");
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("old-"))
        {
            File::open(path)
                .expect("open aged unknown fixture")
                .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1)))
                .expect("age unknown fixture beyond native cleanup threshold");
        }
    }
    drop(native);

    let reopened = fixture.connect().await;
    assert_eq!(
        fixture.manifests(),
        manifests,
        "orphan-only cleanup keeps every committed version"
    );
    for path in &orphan_files {
        assert!(
            !path.exists(),
            "recognized recent orphan must be removed: {path:?}"
        );
    }
    for (path, bytes) in &retained_files {
        assert_eq!(
            &fs::read(path).expect("referenced native file survives"),
            bytes
        );
    }
    for path in &unknown {
        assert_eq!(
            fs::read(path).expect("unknown filename survives"),
            b"unknown caller fixture remains untouched"
        );
    }
    let native = fixture.native().await;
    for (version, rows) in history {
        let snapshot = native
            .checkout_version(version)
            .await
            .expect("every retained version reopens");
        assert_eq!(
            snapshot
                .scan()
                .try_into_batch()
                .await
                .expect("retained historical rows"),
            rows
        );
    }
    drop(native);
    let kept = reopened
        .load(RecoveryCase::key("kept-a"))
        .await
        .expect("retained public record")
        .expect("present record");
    assert_eq!(kept.revision.get(), 1);
    assert_eq!(kept.record.value.as_ref(), &json!("kept-a"));
    RecoveryCase::write(&reopened, 2).await;
    assert_eq!(
        reopened
            .load(RecoveryCase::key("head"))
            .await
            .expect("subsequent progress")
            .expect("head")
            .revision
            .get(),
        3
    );
}

#[tokio::test]
async fn maintenance_recovery_full_cleanup_reclaims_obsolete_files_with_future_timestamps() {
    let fixture = RecoveryCase::new();
    let store = fixture.connect().await;
    let oldest = store
        .begin_transaction()
        .await
        .expect("pin all seeded history");
    let mut seed = store
        .begin_transaction()
        .await
        .expect("mixed fragment seed");
    for key in ["kept-a", "kept-b", "head"] {
        seed.put(StateRecord::new(RecoveryCase::key(key), json!(key)))
            .await
            .expect("seed retained record");
    }
    seed.commit().await.expect("publish retained records");
    for sequence in 1..=63 {
        RecoveryCase::write(&store, sequence).await;
    }
    assert!(
        fixture.manifests().len() >= 64,
        "actual full-cleanup threshold"
    );
    let obsolete = fixture.native_files();
    for extension in ["lance", "txn"] {
        assert!(
            obsolete
                .keys()
                .any(|path| path.extension().is_some_and(|value| value == extension)),
            "native {extension} files must be present"
        );
    }
    assert!(
        obsolete.keys().any(|path| path
            .extension()
            .is_some_and(|value| value == "arrow" || value == "bin")),
        "native partial deletion files must be present"
    );
    let future = SystemTime::now()
        .checked_add(Duration::from_secs(2 * 24 * 60 * 60))
        .expect("bounded future timestamp");
    for path in obsolete.keys() {
        // This is an ordinary, explicitly clock-skewed fixture. Production
        // must compare references without modifying or trusting timestamps.
        File::options()
            .write(true)
            .open(path)
            .expect("owned obsolete native file")
            .set_times(FileTimes::new().set_modified(future))
            .expect("ordinary future timestamp fixture");
        assert!(
            fs::metadata(path)
                .expect("actual modified metadata")
                .modified()
                .expect("actual modification time")
                > SystemTime::now()
        );
    }
    oldest
        .rollback()
        .await
        .expect("release seeded history for full cleanup");
    fixture.settle(&store).await;

    let mut snapshot = store
        .begin_transaction()
        .await
        .expect("atomic post-maintenance view");
    for key in ["kept-a", "kept-b"] {
        let record = snapshot
            .load(RecoveryCase::key(key))
            .await
            .expect("retained record")
            .expect("present record");
        assert_eq!(record.revision.get(), 1);
        assert_eq!(record.record.value.as_ref(), &json!(key));
    }
    let head = snapshot
        .load(RecoveryCase::key("head"))
        .await
        .expect("retained head")
        .expect("head");
    assert_eq!(head.revision.get(), 64);
    assert_eq!(head.record.value.as_ref(), &json!(63));
    snapshot
        .rollback()
        .await
        .expect("release post-maintenance view");
    assert_eq!(fixture.manifests().len(), 1);
    for path in obsolete.keys() {
        assert!(
            !path.exists(),
            "obsolete native file with future mtime must be reclaimed: {path:?}"
        );
    }
    let native = fixture.native().await;
    assert!(!native.manifest().fragments.is_empty());
    for fragment in native.manifest().fragments.iter() {
        for file in &fragment.files {
            let path = fixture.table().join("data").join(&file.path);
            assert!(path.is_file(), "current referenced native data must remain");
            assert!(
                !obsolete.contains_key(&path),
                "full rewrite publishes replacement fragments"
            );
        }
    }
    if let Some(transaction) = &native.manifest().transaction_file {
        assert!(
            fixture
                .table()
                .join("_transactions")
                .join(transaction)
                .is_file(),
            "current native transaction remains readable"
        );
    }
    drop(native);
    drop(store);
    let reopened = fixture.connect().await;
    RecoveryCase::write(&reopened, 64).await;
    assert_eq!(
        reopened
            .load(RecoveryCase::key("head"))
            .await
            .expect("reopened progress")
            .expect("head")
            .revision
            .get(),
        65
    );
}

impl RecoveryCase {
    async fn reject_unsupported_history(overlay: bool) {
        let fixture = RecoveryCase::new();
        let store = fixture.connect().await;
        RecoveryCase::write(&store, 1).await;
        drop(store);
        let native = Arc::new(fixture.native().await);
        let former_manifests = fixture.manifests();
        let former_data = fixture
            .native_files()
            .into_keys()
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "lance")
            })
            .collect::<Vec<_>>();
        assert!(!former_data.is_empty());
        let original = native
            .scan()
            .try_into_batch()
            .await
            .expect("valid state image");
        let unsupported = if overlay {
            original.clone()
        } else {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "unrelated",
                    DataType::UInt8,
                    false,
                )])),
                vec![Arc::new(UInt8Array::from(vec![1]))],
            )
            .expect("unsupported historical schema")
        };
        let bad_version = fixture
            .publish_metadata_fixture(
                native,
                unsupported.clone(),
                uuid::Uuid::new_v4().to_string(),
                overlay,
            )
            .await;
        let bad = Arc::new(fixture.native().await);
        assert_eq!(
            bad.scan()
                .try_into_batch()
                .await
                .expect("unsupported history is independently native-readable"),
            unsupported,
            "the rejection fixture preserves exact native contents"
        );
        let cutoff = bad.manifest().timestamp_nanos;
        let token = uuid::Uuid::new_v4().to_string();
        let kinds = original
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("state kind");
        let mut columns = original.columns().to_vec();
        columns[6] = Arc::new(StringArray::from_iter_values((0..original.num_rows()).map(
            |row| {
                if kinds.value(row) == 1 {
                    token.as_str()
                } else {
                    ""
                }
            },
        )));
        let restored = RecordBatch::try_new(original.schema(), columns)
            .expect("valid later state schema and sentinel");
        let head = fixture
            .publish_metadata_fixture(bad, restored, token, false)
            .await;
        assert!(head > bad_version);
        // Model a prior partial history cleanup using ordinary disposable files.
        // The remaining unsupported historical manifest is native-readable; the
        // valid latest head alone cannot authorize a destructive StateStore sweep.
        for path in former_manifests {
            fs::remove_file(path).expect("dispose earlier fixture manifests");
        }
        for path in &former_data {
            let modified = fs::metadata(path)
                .expect("actual old data metadata")
                .modified()
                .expect("old data modification time")
                .duration_since(UNIX_EPOCH)
                .expect("post-epoch timestamp")
                .as_nanos();
            assert!(
                modified < cutoff,
                "actual file eligible for native timestamp-filtered cleanup"
            );
        }
        let manifests = fixture.manifests();
        assert_eq!(manifests.len(), 2);
        let files = fixture.native_files();
        let result = StateStore::connect(StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(fixture.directory.path()),
        ))
        .await;
        assert!(
            matches!(result, Err(StateStoreError::BackendOperation { .. })),
            "unsupported retained history must fail closed before history-file deletion"
        );
        assert_eq!(fixture.manifests(), manifests);
        assert_eq!(
            fixture.native_files(),
            files,
            "complete validation must precede native cleanup and the supplemental sweep"
        );

        // Removing only the deliberately unsupported historical fixture makes
        // ordinary recovery possible; the supported current image stays exact.
        fs::remove_file(fixture.manifest(bad_version))
            .expect("remove unsupported disposable fixture");
        let recovered = fixture.connect().await;
        let observed = recovered
            .load(RecoveryCase::key("head"))
            .await
            .expect("restored supported history")
            .expect("head");
        assert_eq!(observed.revision.get(), 1);
        assert_eq!(observed.record.value.as_ref(), &json!(1));
    }
}

#[tokio::test]
async fn maintenance_recovery_rejects_unsupported_history_before_history_file_deletion() {
    RecoveryCase::reject_unsupported_history(false).await;
}

#[tokio::test]
async fn maintenance_recovery_rejects_overlay_references_before_any_reclamation() {
    RecoveryCase::reject_unsupported_history(true).await;
}

#[tokio::test]
async fn maintenance_recovery_finds_latest_across_stale_hints_and_partial_manifest_cleanup() {
    for cleanup in [
        PartialCleanup::SuccessorGap,
        PartialCleanup::RemovedHintTarget,
        PartialCleanup::MissingHint,
    ] {
        let fixture = RecoveryCase::new();
        let store = fixture.connect().await;
        RecoveryCase::write(&store, 1).await;
        let hint = fixture.table().join("_versions/latest_version_hint.json");
        let earlier_hint = fs::read(&hint).expect("first committed native hint");
        let earlier_version = fixture.native().await.version_id();
        for sequence in 2..=6 {
            RecoveryCase::write(&store, sequence).await;
        }
        drop(store);
        let latest = fixture.native().await.version_id();
        for version in earlier_version + 1..latest {
            fs::remove_file(fixture.manifest(version)).expect("disposable partial GC fixture");
        }
        match cleanup {
            PartialCleanup::SuccessorGap => {
                fs::write(&hint, &earlier_hint).expect("stale retained hint")
            }
            PartialCleanup::RemovedHintTarget => {
                fs::remove_file(fixture.manifest(earlier_version))
                    .expect("remove hinted old version");
                fs::write(&hint, &earlier_hint).expect("stale missing-target hint");
            }
            PartialCleanup::MissingHint => {
                fs::remove_file(&hint).expect("missing nonauthoritative hint")
            }
        }
        let reopened = fixture.connect().await;
        let head = reopened
            .load(RecoveryCase::key("head"))
            .await
            .expect("recovered head")
            .expect("present head");
        assert_eq!(
            head.revision.get(),
            6,
            "a manifest gap cannot make an older hinted version authoritative"
        );
        assert_eq!(head.record.value.as_ref(), &json!(6));
        RecoveryCase::write(&reopened, 7).await;
        assert_eq!(
            reopened
                .load(RecoveryCase::key("head"))
                .await
                .expect("progress")
                .expect("head")
                .revision
                .get(),
            7
        );
    }
}

#[tokio::test]
async fn maintenance_recovery_retains_maximum_revisions_tombstones_and_deep_json() {
    let fixture = RecoveryCase::new();
    let lease = fixture.shared_lease();
    fixture.seed_boundaries().await;
    let store = fixture.connect().await;
    for sequence in 1..=64 {
        RecoveryCase::write(&store, sequence).await;
    }
    assert!(
        fixture.manifests().len() >= 64,
        "actual retained history before maintenance"
    );
    drop(store);
    drop(lease);
    let store = fixture.connect().await;
    fixture.settle(&store).await;
    let maximum = store
        .load(RecoveryCase::key("maximum"))
        .await
        .expect("load maximum")
        .expect("present maximum");
    assert_eq!(maximum.revision.get(), u64::MAX);
    let deep = store
        .load(RecoveryCase::key("deep"))
        .await
        .expect("load deep")
        .expect("present deep");
    assert_eq!(deep.revision.get(), 17);
    RecoveryCase::assert_deep(&deep.record.value);
    RecoveryCase::dispose(deep.record.value);
    let mut recreation = store
        .begin_transaction()
        .await
        .expect("tombstone continuation");
    recreation
        .put(StateRecord::new(
            RecoveryCase::key("deleted"),
            json!("last revision"),
        ))
        .await
        .expect("reach MAX");
    assert_eq!(
        recreation
            .load(RecoveryCase::key("deleted"))
            .await
            .expect("staged revision")
            .expect("present")
            .revision
            .get(),
        u64::MAX
    );
    recreation.commit().await.expect("commit MAX recreation");
    let mut overflow = store
        .begin_transaction()
        .await
        .expect("overflow transaction");
    overflow
        .put(StateRecord::new(
            RecoveryCase::key("discarded"),
            json!(true),
        ))
        .await
        .expect("earlier staging");
    assert!(matches!(
        overflow.delete(RecoveryCase::key("maximum")).await,
        Err(StateStoreError::InvalidRequest(_))
    ));
    assert!(
        overflow.commit().await.is_err(),
        "overflow invalidates complete overlay"
    );
    assert!(
        store
            .load(RecoveryCase::key("discarded"))
            .await
            .expect("no partial publication")
            .is_none()
    );
    let native = fixture.native().await;
    assert_eq!(
        native
            .manifest()
            .data_storage_format
            .lance_file_format()
            .to_data_file_numbers(),
        (2, 0)
    );
    assert!(!native.manifest().uses_stable_row_ids());
    drop(native);
    drop(store);
    let reopened = fixture.connect().await;
    assert_eq!(
        reopened
            .load(RecoveryCase::key("deleted"))
            .await
            .expect("reopen")
            .expect("recreated")
            .revision
            .get(),
        u64::MAX
    );
    let deep = reopened
        .load(RecoveryCase::key("deep"))
        .await
        .expect("deep reopen")
        .expect("deep");
    RecoveryCase::assert_deep(&deep.record.value);
    RecoveryCase::dispose(deep.record.value);
}

#[tokio::test]
async fn maintenance_recovery_cold_backup_preserves_same_directory_knowledge_and_state() {
    let fixture = RecoveryCase::new();
    let store = fixture.connect().await;
    let oldest = store
        .begin_transaction()
        .await
        .expect("retain seed history until every intended write completes");
    let mut deleted = store
        .begin_transaction()
        .await
        .expect("tombstone transaction");
    deleted
        .delete(RecoveryCase::key("deleted"))
        .await
        .expect("first delete");
    deleted
        .delete(RecoveryCase::key("deleted"))
        .await
        .expect("second delete");
    deleted.commit().await.expect("tombstone publication");
    for sequence in 1..=63 {
        RecoveryCase::write(&store, sequence).await;
    }
    oldest.rollback().await.expect("release seed history");
    fixture.settle(&store).await;
    let database = fixture.database().await;
    let record = DataRecord::without_id().json_field("body", json!("durable knowledge 🦀"));
    database
        .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
            locator: StorageLocator::new("knowledge"),
            schema: None,
            records: vec![record.clone()],
            mode: CreateCollectionMode::Create,
        }))
        .await
        .expect("same-directory knowledge publication");
    drop(database);
    drop(store);
    let restored = RecoveryCase::new();
    RecoveryCase::copy_directory(fixture.directory.path(), restored.directory.path());
    let state = restored.connect().await;
    let head = state
        .load(RecoveryCase::key("head"))
        .await
        .expect("backup head")
        .expect("head");
    assert_eq!(head.revision.get(), 63);
    assert_eq!(head.record.value.as_ref(), &json!(63));
    let mut recreate = state.begin_transaction().await.expect("backup tombstone");
    recreate
        .put(StateRecord::new(
            RecoveryCase::key("deleted"),
            json!("restored"),
        ))
        .await
        .expect("recreate");
    assert_eq!(
        recreate
            .load(RecoveryCase::key("deleted"))
            .await
            .expect("load")
            .expect("present")
            .revision
            .get(),
        3
    );
    recreate
        .commit()
        .await
        .expect("independent restored publication");
    let database = restored.database().await;
    assert_eq!(
        database
            .data(DataRequest::Count(CountRequest {
                locator: StorageLocator::new("knowledge"),
                filter: None,
            }))
            .await
            .expect("restored knowledge count"),
        DataResponse::Count(1)
    );
    let page = database
        .query(QueryRequest::Scroll(ScrollRequest {
            locator: StorageLocator::new("knowledge"),
            filter: None,
            projection: Projection::All,
            limit: Some(10),
            offset: None,
            page_token: None,
        }))
        .await
        .expect("restored knowledge query");
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].record, record);
    let original = fixture.connect().await;
    assert!(
        original
            .load(RecoveryCase::key("deleted"))
            .await
            .expect("source unchanged")
            .is_none()
    );
}

#[tokio::test]
async fn maintenance_recovery_final_drop_does_not_start_deferred_reclamation() {
    let fixture = RecoveryCase::new();
    let lease = fixture.shared_lease();
    let store = fixture.connect().await;
    for sequence in 1..=64 {
        RecoveryCase::write(&store, sequence).await;
    }
    let before = fixture.manifests();
    assert!(before.len() >= 64);
    drop(lease);
    drop(store);
    assert_eq!(
        fixture.manifests(),
        before,
        "final-owner drop joins accepted work without initiating maintenance"
    );
    let reopened = fixture.connect().await;
    fixture.settle(&reopened).await;
    assert_eq!(
        reopened
            .load(RecoveryCase::key("head"))
            .await
            .expect("head")
            .expect("present")
            .revision
            .get(),
        64
    );
}

impl RecoveryProcess {
    fn start(fixture: &RecoveryCase) -> Self {
        let original_data = fs::read_dir(fixture.table().join("data"))
            .expect("original data inventory")
            .map(|entry| entry.expect("original data entry").path())
            .collect();
        let original_manifests = fixture.manifests();
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "lancedb_state_maintenance_recovery::maintenance_recovery_process_entry",
                "--nocapture",
            ])
            .env(
                "AI_KIT_LANCE_MAINTENANCE_RECOVERY_CHILD",
                fixture.directory.path(),
            )
            .spawn()
            .expect("ordinary owned maintenance child");
        Self {
            child,
            original_data,
            original_manifests,
        }
    }

    async fn kill_after_rewrite_witness(&mut self, fixture: &RecoveryCase) -> bool {
        let data = fixture.table().join("data");
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(status) = self.child.try_wait().expect("owned child status") {
                assert!(
                    status.success(),
                    "maintenance child failed before a kill witness: {status}"
                );
                return false;
            }
            assert!(
                Instant::now() < deadline,
                "bounded maintenance witness wait"
            );
            let preparation = fs::read_dir(&data)
                .expect("rewrite data inventory")
                .map(|entry| entry.expect("rewrite data entry").path())
                .any(|path| !self.original_data.contains(&path));
            if preparation
                && fixture.manifests() == self.original_manifests
                && !fixture
                    .directory
                    .path()
                    .join("maintenance-complete")
                    .exists()
            {
                // This is ordinary polling evidence. Publication can race the
                // following kill; no exact instruction or I/O boundary is claimed.
                if let Err(error) = self.child.kill() {
                    if self
                        .child
                        .try_wait()
                        .expect("recheck child completion")
                        .is_some_and(|status| status.success())
                    {
                        return false;
                    }
                    panic!("could not terminate the supervised child: {error}");
                }
                let status = self.child.wait().expect("reap maintenance child");
                // The child can finish between the filesystem observation and
                // termination. A successful exit is a missed witness, not a
                // backend failure; retry within the fixed attempt budget.
                return !status.success();
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

impl Drop for RecoveryProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn maintenance_recovery_process_loss_after_observed_rewrite_keeps_exact_committed_state() {
    for attempt in 1..=3 {
        let fixture = RecoveryCase::new();
        let lease = fixture.shared_lease();
        let store = fixture.connect().await;
        let mut payload = store
            .begin_transaction()
            .await
            .expect("large rewrite fixture");
        payload
            .put(StateRecord::new(
                RecoveryCase::key("payload"),
                Value::String("m".repeat(attempt * 8 * 1024 * 1024)),
            ))
            .await
            .expect("stage retained rewrite payload");
        payload.commit().await.expect("publish retained payload");
        for sequence in 1..=64 {
            RecoveryCase::write(&store, sequence).await;
        }
        drop(store);
        drop(lease);
        let mut child = RecoveryProcess::start(&fixture);
        if !child.kill_after_rewrite_witness(&fixture).await {
            continue;
        }
        let store = fixture.connect().await;
        let mut view = store
            .begin_transaction()
            .await
            .expect("atomic post-crash view");
        let head = view
            .load(RecoveryCase::key("head"))
            .await
            .expect("head")
            .expect("present head");
        let payload = view
            .load(RecoveryCase::key("payload"))
            .await
            .expect("payload")
            .expect("present payload");
        assert_eq!(head.revision.get(), 64);
        assert_eq!(head.record.value.as_ref(), &json!(64));
        assert_eq!(payload.revision.get(), 1);
        let value = payload
            .record
            .value
            .as_str()
            .expect("exact original payload");
        assert_eq!(value.len(), attempt * 8 * 1024 * 1024);
        assert!(value.bytes().all(|byte| byte == b'm'));
        view.rollback().await.expect("release post-crash view");
        fixture.settle(&store).await;
        RecoveryCase::write(&store, 65).await;
        return;
    }
    panic!("missing evidence: no maintenance rewrite witness within three ordinary attempts");
}

#[tokio::test]
async fn maintenance_recovery_process_entry() {
    let Some(directory) = std::env::var_os("AI_KIT_LANCE_MAINTENANCE_RECOVERY_CHILD") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let store = RecoveryCase::connect_at(&directory).await;
    for _ in 0..35 {
        store
            .load(RecoveryCase::key("head"))
            .await
            .expect("child ordinary progress");
    }
    drop(store);
    fs::write(directory.join("maintenance-complete"), b"complete").expect("joined child marker");
}
