use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arrow_array::{
    Array, BooleanArray, Int64Array, LargeBinaryArray, RecordBatch, RecordBatchIterator,
    StringArray, UInt8Array, UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use lance::{
    Dataset, Error,
    dataset::{
        CommitBuilder, InsertBuilder, MergeInsertBuilder, UncommittedMergeInsert, WhenMatched,
        WhenNotMatched, WhenNotMatchedBySource, WriteMode, WriteParams, builder::DatasetBuilder,
        transaction::Transaction,
    },
    io::ObjectStore,
};
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction as ManifestTransaction},
    io::commit::{
        CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation,
        ManifestNamingScheme, ManifestWriter,
    },
};
use object_store::path::Path;
use storage_kit::StateStoreError;
use tempfile::TempDir;

const EXPECTED_VERSION: &str = "storage-kit.native-probe.expected-version";

#[derive(Clone, Copy)]
pub enum CommitPolicy {
    Native,
    ExactVersion,
}

impl CommitPolicy {
    fn handler(self) -> Arc<dyn CommitHandler> {
        match self {
            Self::Native => Arc::new(ConditionalPutCommitHandler),
            Self::ExactVersion => Arc::new(ExactVersionCommit),
        }
    }
}

/// Test-local adapter for the pinned foreign trait; no Kit behavior is mocked.
#[derive(Debug)]
struct ExactVersionCommit;

impl CommitHandler for ExactVersionCommit {
    fn is_version_not_found_definitive(&self) -> bool {
        // This handler resolves only strongly consistent local/memory stores,
        // with no external or eventually consistent manifest authority.
        true
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        // A landed manifest does not erase a failed durability barrier.
        true
    }

    fn commit<'owner, 'manifest, 'path, 'store, 'future>(
        &'owner self,
        manifest: &'manifest mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &'path Path,
        object_store: &'store ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<ManifestTransaction>,
    ) -> Pin<Box<dyn Future<Output = Result<ManifestLocation, CommitError>> + Send + 'future>>
    where
        'owner: 'future,
        'manifest: 'future,
        'path: 'future,
        'store: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let expected = transaction
                .as_ref()
                .and_then(|transaction| {
                    transaction
                        .inner
                        .transaction_properties
                        .get(EXPECTED_VERSION)
                })
                .and_then(|version| version.parse::<u64>().ok())
                .ok_or_else(|| {
                    CommitError::OtherError(Error::invalid_input(
                        "native state probe requires the original target version",
                    ))
                })?;
            if manifest.version != expected {
                return Err(CommitError::OtherError(
                    Error::retryable_commit_conflict_source(
                        expected,
                        format!(
                            "state transaction expected version {expected}, received {}",
                            manifest.version
                        )
                        .into(),
                    ),
                ));
            }
            ConditionalPutCommitHandler
                .commit(
                    manifest,
                    indices,
                    base_path,
                    object_store,
                    manifest_writer,
                    naming_scheme,
                    transaction,
                )
                .await
        })
    }
}

pub struct NativeTable {
    directory: TempDir,
    policy: CommitPolicy,
}

impl NativeTable {
    pub fn new(policy: CommitPolicy) -> Self {
        Self {
            directory: tempfile::tempdir().expect("native probe directory"),
            policy,
        }
    }

    fn uri(&self) -> String {
        self.directory
            .path()
            .join("state.lance")
            .to_str()
            .expect("UTF-8 temporary path")
            .to_owned()
    }

    fn batch(rows: &[(&str, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(key, _)| *key),
                )),
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, value)| *value),
                )),
            ],
        )
        .expect("native probe batch")
    }

    fn state_batch(rows: usize, sequence: u64, token: &str) -> RecordBatch {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("kind", DataType::UInt8, false),
                Field::new("namespace", DataType::Utf8, false),
                Field::new("key", DataType::Utf8, false),
                Field::new("revision", DataType::UInt64, false),
                Field::new("present", DataType::Boolean, false),
                Field::new("value", DataType::LargeBinary, true),
                Field::new("token", DataType::Utf8, false),
            ])
            .with_metadata(HashMap::from([(
                "ai-kit.state.format".to_owned(),
                "1".to_owned(),
            )])),
        );
        let value = serde_json::to_vec(&serde_json::json!({
            "payload": "x".repeat(128), "sequence": sequence,
        }))
        .expect("state-shaped JSON encoding");
        let keys = std::iter::once(String::new())
            .chain((0..rows).map(|index| format!("key-{index:05}")))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt8Array::from_iter_values(
                    std::iter::once(1).chain((0..rows).map(|_| 0)),
                )),
                Arc::new(StringArray::from_iter_values(
                    std::iter::once("").chain((0..rows).map(|_| "rows")),
                )),
                Arc::new(StringArray::from_iter_values(keys.iter())),
                Arc::new(UInt64Array::from_iter_values(
                    std::iter::once(0).chain((0..rows).map(|_| sequence + 1)),
                )),
                Arc::new(BooleanArray::from_iter(
                    std::iter::once(Some(false)).chain((0..rows).map(|_| Some(true))),
                )),
                Arc::new(LargeBinaryArray::from_iter(
                    std::iter::once(None).chain((0..rows).map(|_| Some(value.as_slice()))),
                )),
                Arc::new(StringArray::from_iter_values(
                    std::iter::once(token).chain((0..rows).map(|_| "")),
                )),
            ],
        )
        .expect("seven-column state-shaped native batch")
    }

    fn drain_state_runtime(runtime: tokio::runtime::Runtime) -> Duration {
        let started = Instant::now();
        let deadline = started + Duration::from_secs(30);
        while runtime.metrics().num_alive_tasks() != 0 {
            assert!(
                Instant::now() < deadline,
                "native phase child drain deadline"
            );
            runtime.block_on(tokio::task::yield_now());
        }
        // Format 2.0 keeps the relevant native work on this owned runtime.
        // Destruction also joins its blocking work; no background shutdown.
        drop(runtime);
        started.elapsed()
    }

    async fn assert_state_rows(snapshot: &Dataset, rows: usize, token: &str) {
        assert_eq!(snapshot.version_id(), 27);
        assert_eq!(
            snapshot.versions().await.expect("native versions").len(),
            27
        );
        assert_eq!(
            snapshot
                .manifest()
                .data_storage_format
                .lance_file_format()
                .to_data_file_numbers(),
            (2, 0)
        );
        assert!(!snapshot.manifest().uses_stable_row_ids());
        assert_eq!(
            Schema::from(snapshot.schema()),
            *Self::state_batch(0, 0, token).schema()
        );
        let transaction = snapshot
            .read_transaction_by_version(27)
            .await
            .expect("fresh canonical transaction")
            .expect("native transaction identity");
        assert_eq!(transaction.uuid, token);
        assert_eq!(
            transaction
                .transaction_properties
                .as_ref()
                .and_then(|properties| properties.get(EXPECTED_VERSION))
                .map(String::as_str),
            Some("27")
        );
        let batch = snapshot
            .scan()
            .try_into_batch()
            .await
            .expect("fresh complete native scan");
        assert_eq!(batch.num_rows(), rows + 1);
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("kinds");
        let keys = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keys");
        let seeded = Self::state_batch(rows, 0, token);
        let updated = Self::state_batch(2, 25, token);
        let mut identities = BTreeMap::new();
        for index in 0..batch.num_rows() {
            assert!(!kinds.is_null(index));
            assert!(!keys.is_null(index));
            let key = keys.value(index);
            assert!(
                identities.insert((kinds.value(index), key), ()).is_none(),
                "duplicate row"
            );
            let expected = match kinds.value(index) {
                1 => seeded.slice(0, 1),
                0 => {
                    let row = key
                        .strip_prefix("key-")
                        .expect("public key prefix")
                        .parse::<usize>()
                        .expect("public key index");
                    assert!(row < rows);
                    assert_eq!(key, format!("key-{row:05}"));
                    if row < 2 {
                        updated.slice(row + 1, 1)
                    } else {
                        seeded.slice(row + 1, 1)
                    }
                }
                other => panic!("unexpected record kind {other}"),
            };
            // Row equality checks all seven fields, exact JSON bytes, nulls,
            // revisions and sentinel identity regardless of physical ordering.
            assert_eq!(batch.slice(index, 1), expected);
        }
        assert!(identities.contains_key(&(1, "")));
    }

    /// Native-engine attribution only. This bypasses the supplied adapter's
    /// image, queue, leases, durable ObjectStore wrapper and cleanup ledger.
    pub fn measure_state_phases(&self, rows: usize) {
        assert!(matches!(self.policy, CommitPolicy::ExactVersion));
        assert!([100, 1_000, 10_000].contains(&rows));
        let uri = url::Url::from_file_path(self.uri())
            .expect("native file URI")
            .to_string()
            .replacen("file:", "file-object-store:", 1);
        let create_started = Instant::now();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owned native creation runtime");
        let initial_token = uuid::Uuid::new_v4().to_string();
        let initial = runtime.block_on(async {
            let params = WriteParams {
                mode: WriteMode::Create,
                data_storage_version: Some("2.0".parse().expect("pinned format")),
                enable_stable_row_ids: false,
                commit_handler: Some(self.policy.handler()),
                skip_auto_cleanup: true,
                ..WriteParams::default()
            };
            let mut transaction = InsertBuilder::new(uri.as_str())
                .with_params(&params)
                .execute_uncommitted(vec![Self::state_batch(0, 0, &initial_token)])
                .await
                .expect("prepare state-shaped sentinel");
            transaction.uuid = initial_token;
            Self::retain_original_target(&mut transaction);
            CommitBuilder::new(uri.as_str())
                .use_stable_row_ids(false)
                .with_commit_handler(self.policy.handler())
                .with_max_retries(0)
                .with_skip_auto_cleanup(true)
                .execute(transaction)
                .await
                .expect("publish state-shaped sentinel")
        });
        assert_eq!(initial.version_id(), 1);
        let creation_drain = Self::drain_state_runtime(runtime);
        drop(initial);
        let creation = create_started.elapsed();
        let mut observations = Vec::new();
        let mut last_token = String::new();
        for sequence in 0..=25_u64 {
            let complete_started = Instant::now();
            let runtime_started = Instant::now();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("owned native phase runtime");
            let runtime_creation = runtime_started.elapsed();
            let open_started = Instant::now();
            let original = Arc::new(runtime.block_on(async {
                DatasetBuilder::from_uri(uri.clone())
                    .with_commit_handler(self.policy.handler())
                    .load()
                    .await
                    .expect("fresh original native snapshot")
            }));
            let open = open_started.elapsed();
            assert_eq!(original.version_id(), sequence + 1);
            let batch_started = Instant::now();
            let token = uuid::Uuid::new_v4().to_string();
            let batch = Self::state_batch(if sequence == 0 { rows } else { 2 }, sequence, &token);
            let batch_construction = batch_started.elapsed();
            let preparation_started = Instant::now();
            let mut delta = runtime.block_on(async {
                let schema = batch.schema();
                let source = RecordBatchIterator::new(vec![Ok(batch)], schema);
                let mut builder = MergeInsertBuilder::try_new(
                    Arc::clone(&original),
                    vec!["kind".to_owned(), "namespace".to_owned(), "key".to_owned()],
                )
                .expect("state-shaped merge builder");
                builder
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched(WhenNotMatched::InsertAll)
                    .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
                    .use_index(false);
                builder
                    .try_build()
                    .expect("state-shaped merge job")
                    .execute_uncommitted(source)
                    .await
                    .expect("prepare state-shaped delta")
            });
            let preparation = preparation_started.elapsed();
            assert_eq!(delta.transaction.read_version, original.version_id());
            assert_eq!(
                delta.stats.num_inserted_rows,
                if sequence == 0 { rows as u64 } else { 0 }
            );
            assert_eq!(
                delta.stats.num_updated_rows,
                if sequence == 0 { 1 } else { 3 }
            );
            assert_eq!(delta.stats.num_deleted_rows, 0);
            assert_eq!(delta.stats.num_attempts, 1);
            assert_eq!(delta.stats.num_skipped_duplicates, 0);
            let stats = serde_json::json!({
                "inserted_rows": delta.stats.num_inserted_rows,
                "updated_rows": delta.stats.num_updated_rows,
                "logical_deleted_rows": delta.stats.num_deleted_rows,
                "preparation_attempts": delta.stats.num_attempts,
                "data_file_bytes_written": delta.stats.bytes_written,
                "data_files_written": delta.stats.num_files_written,
                "skipped_source_duplicates": delta.stats.num_skipped_duplicates,
            });
            let publication_started = Instant::now();
            delta.transaction.uuid = token.clone();
            Self::retain_original_target(&mut delta.transaction);
            let published = runtime
                .block_on(Self::commit_delta(Arc::clone(&original), delta))
                .expect("publish exact original-snapshot state delta");
            let publication = publication_started.elapsed();
            assert_eq!(published.version_id(), sequence + 2);
            let drain = Self::drain_state_runtime(runtime);
            // Keep both native snapshots alive through child/runtime joining.
            drop(published);
            drop(original);
            let complete = complete_started.elapsed();
            observations.push(serde_json::json!({
                "sequence": sequence, "seed": sequence == 0, "measured": sequence > 5,
                "native_version": sequence + 2, "batch_us": batch_construction.as_micros(),
                "runtime_creation_us": runtime_creation.as_micros(), "fresh_open_us": open.as_micros(),
                "preparation_us": preparation.as_micros(), "publication_us": publication.as_micros(),
                "owned_drain_and_runtime_drop_us": drain.as_micros(), "complete_us": complete.as_micros(),
                "native_stats": stats,
            }));
            last_token = token;
        }
        let reopen_started = Instant::now();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owned native verification runtime");
        let reopened = runtime.block_on(async {
            DatasetBuilder::from_uri(uri.clone())
                .with_commit_handler(self.policy.handler())
                .load()
                .await
                .expect("native reopen")
        });
        let reopen = reopen_started.elapsed();
        let verification_started = Instant::now();
        runtime.block_on(Self::assert_state_rows(&reopened, rows, &last_token));
        let verification = verification_started.elapsed();
        let final_drain = Self::drain_state_runtime(runtime);
        drop(reopened);
        println!(
            "LANCE_NATIVE_STATE_PHASES {}",
            serde_json::json!({
                "rows": rows, "kind": "native_engine_phase_attribution", "outcome": "ok",
                "scope": "native file-object-store without Kit durable wrapper, image, leases, queue or cleanup ledger; not supplied-adapter evidence",
                "schema_columns": 7, "native_format": "2.0", "stable_row_ids": false,
                "creation_us": creation.as_micros(), "creation_drain_and_runtime_drop_us": creation_drain.as_micros(),
                "warmups": 5, "measured_updates": 20, "mutated_public_keys_per_update": 2,
                "retained_versions": 27, "maintenance": "none; below the adapter's 64-version threshold",
                "observations": observations, "reopen_us": reopen.as_micros(),
                "complete_reopen_validation_us": verification.as_micros(),
                "final_drain_and_runtime_drop_us": final_drain.as_micros(),
                "data_file_stats_scope": "native preparation reports data files only, excluding manifests, transaction files, deletion files and filesystem synchronization",
                "timing_scope": "inclusive wall time; not CPU or actual device I/O; publication includes transaction identity assignment; complete includes measurement assertions",
                "dataset_directory": self.directory.path(),
            })
        );
    }

    fn retain_original_target(transaction: &mut Transaction) {
        let expected = transaction
            .read_version
            .checked_add(1)
            .expect("native probe version fits");
        transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            EXPECTED_VERSION.to_owned(),
            expected.to_string(),
        )])));
    }

    fn state_record(key: &str, revision: u64, value: Option<&[u8]>) -> RecordBatch {
        RecordBatch::try_new(
            Self::state_batch(0, 0, "").schema(),
            vec![
                Arc::new(UInt8Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["rows"])),
                Arc::new(StringArray::from(vec![key])),
                Arc::new(UInt64Array::from(vec![revision])),
                Arc::new(BooleanArray::from(vec![value.is_some()])),
                Arc::new(LargeBinaryArray::from(vec![value])),
                Arc::new(StringArray::from(vec![""])),
            ],
        )
        .expect("exact seven-column state record")
    }

    async fn prepare_state_delta(
        snapshot: Arc<Dataset>,
        rows: Vec<RecordBatch>,
        token: &str,
    ) -> UncommittedMergeInsert {
        let schema = Self::state_batch(0, 0, token).schema();
        let source = RecordBatchIterator::new(rows.into_iter().map(Ok), schema);
        let mut builder = MergeInsertBuilder::try_new(
            snapshot,
            vec!["kind".to_owned(), "namespace".to_owned(), "key".to_owned()],
        )
        .expect("state-shaped fragment merge");
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
            .use_index(false);
        let mut delta = builder
            .try_build()
            .expect("state-shaped merge job")
            .execute_uncommitted(source)
            .await
            .expect("prepare selected state rows");
        delta.transaction.uuid = token.to_owned();
        Self::retain_original_target(&mut delta.transaction);
        delta
    }

    async fn state_rows(snapshot: &Dataset) -> BTreeMap<String, RecordBatch> {
        let batch = snapshot
            .scan()
            .try_into_batch()
            .await
            .expect("scan all native state rows");
        let keys = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("state keys");
        let mut rows = BTreeMap::new();
        for index in 0..batch.num_rows() {
            assert!(
                rows.insert(keys.value(index).to_owned(), batch.slice(index, 1))
                    .is_none()
            );
        }
        rows
    }

    /// Native-engine evidence for the adapter's proposed partial maintenance
    /// merge. Selection is explicit here; this does not test Kit scheduling.
    pub async fn assert_selected_state_fragments(&self, include_sentinel: bool) {
        let token = uuid::Uuid::new_v4().to_string();
        let params = WriteParams {
            mode: WriteMode::Create,
            data_storage_version: Some("2.0".parse().expect("pinned format")),
            enable_stable_row_ids: false,
            commit_handler: Some(self.policy.handler()),
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        let uri = self.uri();
        let mut initial = InsertBuilder::new(uri.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![Self::state_batch(0, 0, &token)])
            .await
            .expect("prepare native state sentinel");
        initial.uuid = token;
        Self::retain_original_target(&mut initial);
        let mut current = self
            .commit_create(initial)
            .await
            .expect("create native state table");
        let mut selected_ids = Vec::new();
        for group in 0..4 {
            let token = uuid::Uuid::new_v4().to_string();
            let rows = vec![
                Self::state_batch(0, 0, &token),
                Self::state_record(
                    &format!("group-{group}-present"),
                    u64::MAX,
                    Some(b"{\"exact\":true}"),
                ),
                Self::state_record(&format!("group-{group}-deleted"), u64::MAX, None),
            ];
            let delta = Self::prepare_state_delta(Arc::clone(&current), rows, &token).await;
            current = Self::commit_delta(current, delta)
                .await
                .expect("seed real native fragment");
            selected_ids.push(
                current
                    .manifest()
                    .fragments
                    .last()
                    .expect("new fragment")
                    .id,
            );
        }
        let cold = serde_json::to_vec(&"cold".repeat(65_536)).expect("cold JSON");
        let token = uuid::Uuid::new_v4().to_string();
        let delta = Self::prepare_state_delta(
            Arc::clone(&current),
            vec![
                Self::state_batch(0, 0, &token),
                Self::state_record("cold", 1, Some(&cold)),
                // The old physical row is deleted, but its fragment still
                // contains the permanent tombstone selected below.
                Self::state_record("group-0-present", u64::MAX, Some(b"{\"replacement\":true}")),
            ],
            &token,
        )
        .await;
        current = Self::commit_delta(current, delta)
            .await
            .expect("introduce physical deletion");
        let cold_fragment = current
            .manifest()
            .fragments
            .last()
            .expect("cold fragment")
            .clone();
        let token = uuid::Uuid::new_v4().to_string();
        let delta = Self::prepare_state_delta(
            Arc::clone(&current),
            vec![
                Self::state_batch(0, 0, &token),
                Self::state_record("tail", 1, Some(b"17")),
            ],
            &token,
        )
        .await;
        current = Self::commit_delta(current, delta)
            .await
            .expect("move current sentinel");
        if include_sentinel {
            selected_ids.push(
                current
                    .manifest()
                    .fragments
                    .last()
                    .expect("sentinel fragment")
                    .id,
            );
        }
        let selected = current
            .manifest()
            .fragments
            .iter()
            .filter(|fragment| selected_ids.contains(&fragment.id))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), if include_sentinel { 5 } else { 4 });
        assert!(
            selected
                .iter()
                .any(|fragment| fragment.deletion_file.is_some())
        );
        let cold_files = cold_fragment
            .files
            .iter()
            .map(|file| {
                let path = self
                    .directory
                    .path()
                    .join("state.lance/data")
                    .join(&file.path);
                let bytes = std::fs::read(&path).expect("cold data file bytes");
                (path, bytes)
            })
            .collect::<Vec<_>>();
        let before = Self::state_rows(&current).await;
        let mut scan = current.scan();
        scan.with_fragments(selected);
        scan.project(&["kind", "namespace", "key"])
            .expect("identity projection");
        let keys = scan
            .try_into_batch()
            .await
            .expect("only selected live identities");
        let names = keys
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("selected keys");
        let token = uuid::Uuid::new_v4().to_string();
        let mut source = vec![Self::state_batch(0, 0, &token)];
        let mut selected_sentinels = 0;
        for index in 0..keys.num_rows() {
            if names.value(index).is_empty() {
                selected_sentinels += 1;
            } else {
                source.push(
                    before
                        .get(names.value(index))
                        .expect("validated source row")
                        .clone(),
                );
            }
        }
        assert_eq!(selected_sentinels, usize::from(include_sentinel));
        let source_rows = source.len();
        let version = current.version_id();
        let delta = Self::prepare_state_delta(Arc::clone(&current), source, &token).await;
        assert_eq!(delta.transaction.read_version, version);
        assert_eq!(delta.stats.num_inserted_rows, 0);
        assert_eq!(delta.stats.num_updated_rows, source_rows as u64);
        assert_eq!(delta.stats.num_deleted_rows, 0);
        let published = Self::commit_delta(Arc::clone(&current), delta)
            .await
            .expect("one guarded partial merge");
        assert_eq!(published.version_id(), version + 1);
        assert!(
            published
                .manifest()
                .fragments
                .iter()
                .all(|fragment| !selected_ids.contains(&fragment.id))
        );
        assert!(
            published
                .manifest()
                .fragments
                .iter()
                .any(|fragment| fragment.id == cold_fragment.id)
        );
        assert!(published.manifest().fragments.len() < current.manifest().fragments.len());
        let after = Self::state_rows(&published).await;
        assert_eq!(after.len(), before.len());
        for (key, row) in &before {
            if !key.is_empty() {
                assert_eq!(after.get(key), Some(row), "unchanged logical row {key}");
            }
        }
        assert_eq!(after.get(""), Some(&Self::state_batch(0, 0, &token)));
        assert_eq!(
            published
                .read_transaction()
                .await
                .expect("published identity")
                .expect("transaction")
                .uuid,
            token
        );
        assert_eq!(
            Self::state_rows(&current).await,
            before,
            "original native snapshot remains readable"
        );
        drop(current);
        published
            .cleanup_with_policy(lance::dataset::cleanup::CleanupPolicy {
                before_version: Some(published.version_id()),
                delete_unverified: true,
                ..Default::default()
            })
            .await
            .expect("reclaim detached selected fragments and history");
        for (path, bytes) in cold_files {
            assert_eq!(std::fs::read(path).expect("retained cold file"), bytes);
        }
        let reopened = self.open().await;
        assert_eq!(reopened.version_id(), version + 1);
        assert_eq!(
            reopened.versions().await.expect("retained history").len(),
            1
        );
        assert_eq!(Self::state_rows(&reopened).await, after);
        assert!(!reopened.manifest().uses_stable_row_ids());
        assert_eq!(
            reopened
                .manifest()
                .data_storage_format
                .lance_file_format()
                .to_data_file_numbers(),
            (2, 0)
        );
    }

    pub async fn prepare_create(&self) -> Transaction {
        let params = WriteParams {
            mode: WriteMode::Create,
            commit_handler: Some(self.policy.handler()),
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        let uri = self.uri();
        let mut transaction = InsertBuilder::new(uri.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![Self::batch(&[("__sentinel", 0)])])
            .await
            .expect("prepare new native table");
        Self::retain_original_target(&mut transaction);
        transaction
    }

    pub async fn commit_create(&self, transaction: Transaction) -> Result<Arc<Dataset>, Error> {
        let uri = self.uri();
        CommitBuilder::new(uri.as_str())
            .with_commit_handler(self.policy.handler())
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .map(Arc::new)
    }

    pub async fn create(&self) -> Arc<Dataset> {
        self.commit_create(self.prepare_create().await)
            .await
            .expect("create native table")
    }

    pub async fn open(&self) -> Arc<Dataset> {
        Arc::new(
            DatasetBuilder::from_uri(self.uri())
                .with_commit_handler(self.policy.handler())
                .load()
                .await
                .expect("fresh native table snapshot"),
        )
    }

    pub async fn prepare_delta(
        snapshot: Arc<Dataset>,
        rows: &[(&str, i64)],
    ) -> UncommittedMergeInsert {
        let batch = Self::batch(rows);
        let schema = batch.schema();
        let source = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut builder = MergeInsertBuilder::try_new(snapshot, vec!["key".to_owned()])
            .expect("native merge builder");
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
            .use_index(false);
        let mut result = builder
            .try_build()
            .expect("native merge job")
            .execute_uncommitted(source)
            .await
            .expect("prepare native delta");
        Self::retain_original_target(&mut result.transaction);
        result
    }

    /// Native evidence only: the iterator uses Lance's supported streaming
    /// input, with a test-local receipt analogous to the supplied adapter.
    /// It does not execute the adapter's private error-selection branch.
    pub async fn assert_lazy_input_error(&self) {
        let original = self.create().await;
        let before = Self::rows(&original).await;
        let batch = Self::batch(&[("__sentinel", 1), ("unpublished", 7)]);
        let schema = batch.schema();
        let receipt = Arc::new(Mutex::new(None));
        let source_receipt = Arc::clone(&receipt);
        let source = RecordBatchIterator::new(
            std::iter::once(Ok(batch)).chain(std::iter::once_with(move || {
                let error = StateStoreError::serialization("native lazy input probe");
                *source_receipt.lock().expect("probe receipt") = Some(error.clone());
                Err(ArrowError::ExternalError(Box::new(error)))
            })),
            schema,
        );
        let mut builder =
            MergeInsertBuilder::try_new(Arc::clone(&original), vec!["key".to_owned()])
                .expect("native merge builder");
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
            .use_index(false);
        let error = match builder
            .try_build()
            .expect("native merge job")
            .execute_uncommitted(source)
            .await
        {
            Ok(_) => panic!("lazy input failure must reject preparation"),
            Err(error) => error,
        };
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut typed_source = None;
        while let Some(current) = cause {
            if let Some(error) = current.downcast_ref::<StateStoreError>() {
                typed_source = Some(error.clone());
                break;
            }
            cause = current.source();
        }
        if let Some(error) = &typed_source {
            assert!(
                matches!(error, StateStoreError::Serialization(message) if message == "native lazy input probe")
            );
        }
        // Some pinned DataFusion paths flatten errors into Execution text.
        // The per-preparation receipt retains the original type independently
        // of that path, without parsing or classifying error strings.
        let retained = receipt.lock().expect("probe receipt").take();
        assert!(
            matches!(retained, Some(StateStoreError::Serialization(message)) if message == "native lazy input probe")
        );
        println!(
            "LANCE_NATIVE_LAZY_ERROR typed_source_preserved={} receipt_preserved=true",
            typed_source.is_some()
        );
        let reopened = self.open().await;
        assert_eq!(reopened.version_id(), original.version_id());
        assert_eq!(Self::rows(&reopened).await, before);
        let progress =
            Self::prepare_delta(Arc::clone(&reopened), &[("__sentinel", 2), ("progress", 1)]).await;
        Self::commit_delta(reopened, progress)
            .await
            .expect("progress after rejected input");
        let recovered_dataset = self.open().await;
        let recovered = Self::rows(&recovered_dataset).await;
        assert_eq!(recovered.get("progress"), Some(&1));
        assert!(!recovered.contains_key("unpublished"));
    }

    pub async fn commit_delta(
        snapshot: Arc<Dataset>,
        delta: UncommittedMergeInsert,
    ) -> Result<Arc<Dataset>, Error> {
        let mut builder = CommitBuilder::new(snapshot)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true);
        if let Some(affected_rows) = delta.affected_rows {
            builder = builder.with_affected_rows(affected_rows);
        }
        builder.execute(delta.transaction).await.map(Arc::new)
    }

    pub async fn prepare_overwrite(snapshot: Arc<Dataset>, rows: &[(&str, i64)]) -> Transaction {
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        let mut transaction = InsertBuilder::new(snapshot)
            .with_params(&params)
            .execute_uncommitted(vec![Self::batch(rows)])
            .await
            .expect("prepare original-snapshot overwrite");
        Self::retain_original_target(&mut transaction);
        transaction
    }

    pub async fn prepare_append(snapshot: Arc<Dataset>, rows: &[(&str, i64)]) -> Transaction {
        let params = WriteParams {
            mode: WriteMode::Append,
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        let mut transaction = InsertBuilder::new(snapshot)
            .with_params(&params)
            .execute_uncommitted(vec![Self::batch(rows)])
            .await
            .expect("prepare native append");
        Self::retain_original_target(&mut transaction);
        transaction
    }

    pub async fn commit(
        snapshot: Arc<Dataset>,
        transaction: Transaction,
    ) -> Result<Arc<Dataset>, Error> {
        CommitBuilder::new(snapshot)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .map(Arc::new)
    }

    pub async fn rows(snapshot: &Dataset) -> BTreeMap<String, i64> {
        let batch = snapshot
            .scan()
            .try_into_batch()
            .await
            .expect("read pinned native snapshot");
        let keys = batch
            .column_by_name("key")
            .expect("key column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string keys");
        let values = batch
            .column_by_name("value")
            .expect("value column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer values");
        let rows = (0..batch.num_rows())
            .map(|index| (keys.value(index).to_owned(), values.value(index)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(rows.len(), batch.num_rows(), "duplicate native probe key");
        rows
    }
}
