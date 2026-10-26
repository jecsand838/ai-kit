//! Explicit, bounded measurements of the real embedded StateStore.
//! Run serially with identical recorded build profiles; these receipts impose no latency SLO.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use storage_sdk::{
    Database, DatabaseBackend, StateStore, StateStoreBackend, StateStoreError,
    StateTransactionBackend,
    database::{
        StorageLocator,
        catalog::{CatalogRequest, CreateCollectionMode, CreateCollectionRequest},
        config::{DatabaseConfig, LanceConfig},
        data::{CountRequest, DataRecord, DataRequest, DataResponse, InsertRequest},
    },
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateListRequest, StateRecord},
    },
};

#[derive(Clone, Copy)]
enum Profile {
    Small,
    Medium,
    Cardinality,
    Payload,
    Large,
    Tombstones,
    Versions,
    Entropy,
}

impl Profile {
    async fn measure_commit_observation(self) {
        let fixture = Workload::new(self.rows(), self.payload());
        let store = fixture.connect().await;
        fixture.seed(&store).await;
        let mut commit = Samples::new();
        let mut first = Samples::new();
        let mut first_load = Samples::new();
        let mut first_transaction = Samples::new();
        let mut warm = Samples::new();
        let mut combined = Samples::new();
        let mut observations = Vec::new();
        let initial_versions = fixture.versions().await;
        let mut previous_versions = initial_versions;
        let mut reclamations = 0;
        let initial_memory = Workload::memory();
        for sequence in 1..=70_u64 {
            let preparation = Instant::now();
            let mut tx = store.begin_transaction().await.expect("measured snapshot");
            let key = Workload::row(sequence as usize % fixture.rows);
            tx.put(StateRecord::new(
                key.clone(),
                serde_json::json!({"payload": fixture.payload, "sequence": sequence}),
            ))
            .await
            .expect("measured update");
            tx.put(StateRecord::new(
                Workload::key("history", &format!("{sequence:08}")),
                serde_json::json!({"sequence": sequence, "payload": fixture.payload}),
            ))
            .await
            .expect("growing history");
            let preparation = preparation.elapsed();
            let combined_started = Instant::now();
            commit
                .observe(tx.commit())
                .await
                .expect("measured publication");
            let transactional = sequence % 2 == 0;
            let value = first
                .observe(async {
                    if transactional {
                        let mut tx = store.begin_transaction().await?;
                        let value = tx.load(key.clone()).await?;
                        tx.rollback().await?;
                        Ok(value)
                    } else {
                        store.load(key.clone()).await
                    }
                })
                .await
                .expect("first observation")
                .expect("published row");
            combined.elapsed.push(combined_started.elapsed());
            if transactional {
                first_transaction
                    .elapsed
                    .push(*first.elapsed.last().expect("sample"));
            } else {
                first_load
                    .elapsed
                    .push(*first.elapsed.last().expect("sample"));
            }
            assert_eq!(value.record.value["sequence"], sequence);
            assert_eq!(value.record.value["payload"], fixture.payload);
            let observed = warm
                .observe(store.load(key))
                .await
                .expect("warm observation")
                .expect("same row");
            assert_eq!(observed.revision, value.revision);
            assert_eq!(observed.record.value, value.record.value);
            let retained_versions = fixture.versions().await;
            reclamations += usize::from(retained_versions < previous_versions);
            previous_versions = retained_versions;
            observations.push(serde_json::json!({
                "sequence": sequence, "transactional_observation": transactional,
                "preparation_us": preparation.as_micros(),
                "commit_us": commit.elapsed.last().expect("sample").as_micros(),
                "first_us": first.elapsed.last().expect("sample").as_micros(),
                "warm_us": warm.elapsed.last().expect("sample").as_micros(),
                "combined_us": combined.elapsed.last().expect("sample").as_micros(),
                "retained_versions": retained_versions,
            }));
        }
        assert!(reclamations > 0, "measure an actual reclamation boundary");
        drop(store);
        let reopened = fixture.connect().await;
        let mut tx = reopened
            .begin_transaction()
            .await
            .expect("reopen verification");
        for sequence in 1..=70_u64 {
            let value = tx
                .load(Workload::key("history", &format!("{sequence:08}")))
                .await
                .expect("history read")
                .expect("all history retained");
            assert_eq!(value.revision.get(), 1);
            assert_eq!(value.record.value["sequence"], sequence);
            assert_eq!(value.record.value["payload"], fixture.payload);
        }
        tx.rollback().await.expect("verification settled");
        drop(reopened);
        Workload::emit(serde_json::json!({
            "kind": "commit_observation", "profile": self.name(),
            "rows": fixture.rows, "payload_bytes": fixture.payload.len(),
            "initial_versions": initial_versions, "final_versions": fixture.versions().await,
            "commit": commit.report(), "first": first.report(), "warm": warm.report(),
            "first_load": first_load.report(), "first_transaction": first_transaction.report(),
            "observed_reclamations": reclamations,
            "combined": combined.report(), "observations": observations,
            "initial_memory": initial_memory, "final_memory": Workload::memory(),
            "disk": fixture.disk(),
        }));
    }

    fn name(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Cardinality => "cardinality",
            Self::Payload => "medium_payload",
            Self::Large => "large_payload",
            Self::Tombstones => "tombstones",
            Self::Versions => "retained_versions",
            Self::Entropy => "less_compressible",
        }
    }

    fn rows(self) -> usize {
        match self {
            Self::Small => 100,
            Self::Large => 128,
            Self::Cardinality => 10_000,
            Self::Medium | Self::Payload | Self::Tombstones | Self::Versions | Self::Entropy => {
                1000
            }
        }
    }

    fn bytes(self) -> usize {
        match self {
            Self::Payload | Self::Entropy => 16 * 1024,
            Self::Large => 256 * 1024,
            Self::Small | Self::Medium | Self::Cardinality | Self::Tombstones | Self::Versions => {
                1024
            }
        }
    }

    fn payload(self) -> String {
        if matches!(self, Self::Entropy) {
            let mut state = 0x4d59_5df4_d0f3_3173_u64;
            (0..self.bytes())
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    char::from(b'!' + (state % 90) as u8)
                })
                .collect()
        } else {
            "x".repeat(self.bytes())
        }
    }

    async fn measure(self) {
        let fixture = Workload::new(self.rows(), self.payload());
        let seed_started = Instant::now();
        let store = fixture.connect().await;
        fixture.seed(&store).await;
        match self {
            Self::Tombstones => {
                for first in (0..10_000).step_by(256) {
                    let mut tx = store
                        .begin_transaction()
                        .await
                        .expect("tombstone seed snapshot");
                    for index in first..(first + 256).min(10_000) {
                        tx.delete(Workload::key("retired", &format!("{index:08}")))
                            .await
                            .expect("authored tombstone");
                    }
                    tx.commit().await.expect("tombstone seed publication");
                }
            }
            Self::Versions => {
                for index in 0..256 {
                    let mut tx = store
                        .begin_transaction()
                        .await
                        .expect("version seed snapshot");
                    tx.put(StateRecord::new(
                        Workload::row(index % 100),
                        serde_json::json!({"payload": fixture.payload, "sequence": 0}),
                    ))
                    .await
                    .expect("version seed row");
                    tx.commit().await.expect("version seed publication");
                }
            }
            Self::Small
            | Self::Medium
            | Self::Cardinality
            | Self::Payload
            | Self::Large
            | Self::Entropy => {}
        }
        let seed_elapsed = seed_started.elapsed();
        let initial_disk = fixture.disk();
        let initial_memory = Workload::memory();
        let mut cold = Samples::new();
        for index in 0..10 {
            let result = cold
                .observe(async {
                    let opened = StateStore::connect(fixture.config()).await?;
                    let value = opened.load(Workload::row(index % fixture.rows)).await?;
                    assert!(value.is_some());
                    drop(opened);
                    Ok(())
                })
                .await;
            if result.is_none() {
                break;
            }
        }

        for index in 0..5 {
            assert!(
                store
                    .load(Workload::row(index % fixture.rows))
                    .await
                    .expect("read warm-up")
                    .is_some()
            );
        }
        let mut loads = Samples::new();
        for index in 0..100 {
            let Some(value) = loads
                .observe(store.load(Workload::row((index * 37) % fixture.rows)))
                .await
            else {
                break;
            };
            assert!(value.is_some());
        }

        for _ in 0..5 {
            store
                .begin_transaction()
                .await
                .expect("transaction warm-up")
                .rollback()
                .await
                .expect("warm-up rollback");
        }
        let mut transactions = Samples::new();
        for index in 0..100 {
            if transactions
                .observe(async {
                    let mut tx = store.begin_transaction().await?;
                    assert!(
                        tx.load(Workload::row((index * 37) % fixture.rows))
                            .await?
                            .is_some()
                    );
                    tx.rollback().await
                })
                .await
                .is_none()
            {
                break;
            }
        }

        let mut pages = Vec::new();
        for limit in [1, 100, 1000] {
            for first in [0, fixture.rows / 2, fixture.rows.saturating_sub(2)] {
                let mut samples = Samples::new();
                for attempt in 0..15 {
                    let mut request = StateListRequest::new("rows")
                        .expect("namespace")
                        .with_limit(limit)
                        .expect("page limit");
                    if first != 0 {
                        request = request
                            .start_after(format!("{:08}", first - 1))
                            .expect("page cursor");
                    }
                    let response = if attempt < 5 {
                        store.list(request).await.expect("list warm-up")
                    } else {
                        let Some(response) = samples.observe(store.list(request)).await else {
                            break;
                        };
                        response
                    };
                    let expected_count = (fixture.rows - first).min(limit);
                    assert_eq!(response.records.len(), expected_count);
                    assert_eq!(
                        response.next_start_after.is_some(),
                        fixture.rows - first > limit
                    );
                    for (offset, record) in response.records.iter().enumerate() {
                        assert_eq!(record.record.key, Workload::row(first + offset));
                    }
                }
                pages.push(serde_json::json!({"limit": limit, "first": first, "latency": samples.report()}));
            }
        }

        let mut single = Samples::new();
        let mut multi = Samples::new();
        let mut evidence = Vec::new();
        let mut warmup = Samples::new();
        for index in 0..5 {
            evidence.push(
                fixture
                    .attempt(
                        &store,
                        format!("warmup/single/{index:04}"),
                        index % fixture.rows,
                        1,
                        &mut warmup,
                    )
                    .await,
            );
        }
        for index in 0..5 {
            evidence.push(
                fixture
                    .attempt(
                        &store,
                        format!("warmup/multi/{index:04}"),
                        index % fixture.rows,
                        fixture.rows.min(129),
                        &mut warmup,
                    )
                    .await,
            );
        }
        for index in 0..30 {
            evidence.push(
                fixture
                    .attempt(
                        &store,
                        format!("single/{index:04}"),
                        index % fixture.rows,
                        1,
                        &mut single,
                    )
                    .await,
            );
        }
        for index in 0..10 {
            evidence.push(
                fixture
                    .attempt(
                        &store,
                        format!("multi/{index:04}"),
                        index % fixture.rows,
                        fixture.rows.min(129),
                        &mut multi,
                    )
                    .await,
            );
        }
        drop(store);
        let verified = fixture.verify(&evidence).await;
        Workload::emit(serde_json::json!({
            "kind": "profile", "profile": self.name(), "rows": fixture.rows,
            "payload_string_bytes": fixture.payload.len(), "seed_ms": seed_elapsed.as_millis(),
            "cold_open_load": cold.report(), "warm_load": loads.report(),
            "begin_load_rollback": transactions.report(), "pages": pages,
            "single_key_mutation": single.report(), "multi_key_mutation": multi.report(),
            "warmup_mutation_attempts": 10, "measured_mutation_attempts": 40,
            "verified_publications": verified, "initial_disk": initial_disk,
            "final_disk": fixture.disk(), "initial_memory": initial_memory,
            "final_memory": Workload::memory(), "retained_versions": fixture.versions().await,
        }));
    }
}

struct Samples {
    elapsed: Vec<Duration>,
    conflicts: usize,
    timeouts: usize,
}

impl Samples {
    fn new() -> Self {
        Self {
            elapsed: Vec::new(),
            conflicts: 0,
            timeouts: 0,
        }
    }

    async fn observe<T>(
        &mut self,
        operation: impl Future<Output = Result<T, StateStoreError>>,
    ) -> Option<T> {
        let started = Instant::now();
        let outcome = operation.await;
        self.elapsed.push(started.elapsed());
        match outcome {
            Ok(value) => Some(value),
            Err(StateStoreError::TransactionConflict { .. }) => {
                self.conflicts += 1;
                None
            }
            Err(StateStoreError::TimedOut { .. }) => {
                self.timeouts += 1;
                None
            }
            Err(error) => panic!("unexpected capacity operation failure: {error:?}"),
        }
    }

    fn report(&self) -> serde_json::Value {
        let mut micros = self
            .elapsed
            .iter()
            .map(Duration::as_micros)
            .collect::<Vec<_>>();
        micros.sort_unstable();
        let percentile = |percent: usize| {
            micros
                .get((micros.len() * percent).div_ceil(100).saturating_sub(1))
                .copied()
        };
        let seconds = self.elapsed.iter().map(Duration::as_secs_f64).sum::<f64>();
        serde_json::json!({
            "samples": micros.len(), "p50_us": percentile(50), "p95_us": percentile(95),
            "p99_us": (micros.len() >= 100).then(|| percentile(99)).flatten(),
            "max_us": micros.last(), "serial_operations_per_second": (seconds > 0.0).then(|| micros.len() as f64 / seconds),
            "conflicts": self.conflicts, "timeouts": self.timeouts,
        })
    }
}

enum Publication {
    Acknowledged,
    Conflict,
    TimedOut,
}

struct Attempt {
    id: String,
    first: usize,
    width: usize,
    publication: Publication,
}

struct Workload {
    directory: tempfile::TempDir,
    rows: usize,
    payload: String,
}

#[derive(Clone, Copy)]
enum FragmentProfile {
    Homogeneous,
    RetainedBacklog,
    MixedCold,
}

#[derive(Clone, Copy)]
enum ImageRetention {
    None,
    Current,
    Original,
}

impl Workload {
    fn new(rows: usize, payload: String) -> Self {
        Self {
            directory: tempfile::tempdir().expect("owned capacity fixture"),
            rows,
            payload,
        }
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn config(&self) -> StateStoreConfig {
        StateStoreConfig::Lance(LanceStateStoreConfig::directory(self.root()))
    }

    async fn connect(&self) -> StateStore {
        StateStore::connect(self.config())
            .await
            .expect("capacity fixture connection")
    }

    fn key(namespace: &str, key: &str) -> StateKey {
        StateKey::new(namespace, key).expect("capacity key")
    }

    fn row(index: usize) -> StateKey {
        Self::key("rows", &format!("{index:08}"))
    }

    async fn seed(&self, store: &StateStore) {
        for first in (0..self.rows).step_by(256) {
            let mut tx = store.begin_transaction().await.expect("seed snapshot");
            for index in first..(first + 256).min(self.rows) {
                tx.put(StateRecord::new(
                    Self::row(index),
                    serde_json::json!({"payload": self.payload, "sequence": 0}),
                ))
                .await
                .expect("seed row");
            }
            tx.commit().await.expect("seed publication");
        }
    }

    async fn measure_fragment_maintenance(&self, profile: FragmentProfile) {
        let started = Instant::now();
        let store = self.connect().await;
        let initial_memory = Self::memory();
        let mut cold = None;
        if matches!(profile, FragmentProfile::MixedCold) {
            let mut random = 0x41bc_197d_957a_029b_u64;
            let value = (0..32 * 1024 * 1024)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    char::from(b'!' + u8::try_from(random % 90).expect("bounded character"))
                })
                .collect::<String>();
            let value = Arc::new(serde_json::Value::String(value));
            let mut tx = store.begin_transaction().await.expect("cold seed snapshot");
            tx.put(StateRecord {
                key: Self::key("cold", "retained"),
                value: Arc::clone(&value),
            })
            .await
            .expect("seed supported oversized cold record");
            tx.commit().await.expect("publish cold record");
            cold = Some(value);
        }
        let table = self.root().join("__ai_kit_state.lance");
        let original = lance::Dataset::open(table.to_str().expect("UTF-8 fixture"))
            .await
            .expect("initial native metadata");
        let cold_fragments = if cold.is_some() {
            original
                .manifest()
                .fragments
                .iter()
                .filter_map(|fragment| {
                    let bytes = fragment
                        .files
                        .iter()
                        .map(|file| {
                            std::fs::metadata(table.join("data").join(&file.path))
                                .expect("cold data bytes")
                                .len()
                        })
                        .sum::<u64>();
                    (bytes > 16 * 1024 * 1024).then_some(fragment.id)
                })
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        assert!(
            cold.is_none() || !cold_fragments.is_empty(),
            "actual cold data exceeds the compaction work budget"
        );
        let mut previous_ids = original
            .manifest()
            .fragments
            .iter()
            .map(|fragment| fragment.id)
            .collect::<HashSet<_>>();
        let mut previous_versions = original.versions().await.expect("initial versions").len();
        drop(original);
        let mut retained = if matches!(profile, FragmentProfile::RetainedBacklog) {
            Some(
                store
                    .begin_transaction()
                    .await
                    .expect("original retained public snapshot"),
            )
        } else {
            None
        };
        let setup = started.elapsed();
        let mut preparation = Samples::new();
        let mut commit = Samples::new();
        let mut first = Samples::new();
        let mut warm = Samples::new();
        let mut complete = Samples::new();
        let mut observations = Vec::new();
        let mut checkpoints = Vec::new();
        let mut original_backlog = HashSet::new();
        let mut release = Duration::ZERO;
        let mut metadata_time = Duration::ZERO;
        for sequence in 1..=512_u64 {
            let payload = if matches!(profile, FragmentProfile::MixedCold) && sequence % 4 == 0 {
                self.payload.repeat(128)
            } else {
                self.payload.clone()
            };
            let complete_started = Instant::now();
            let preparation_started = Instant::now();
            let key = Self::row(sequence as usize);
            let mut tx = store
                .begin_transaction()
                .await
                .expect("single-delta snapshot");
            tx.put(StateRecord::new(
                key.clone(),
                serde_json::json!({"sequence": sequence, "payload": payload}),
            ))
            .await
            .expect("one actual distinct-key update");
            preparation.elapsed.push(preparation_started.elapsed());
            commit
                .observe(tx.commit())
                .await
                .expect("single-delta publication");
            let value = first
                .observe(store.load(key.clone()))
                .await
                .expect("first read")
                .expect("published row");
            assert_eq!(value.revision.get(), 1);
            assert_eq!(value.record.value["sequence"], sequence);
            assert_eq!(value.record.value["payload"], payload);
            let warmed = warm
                .observe(store.load(key))
                .await
                .expect("warm read")
                .expect("same row");
            assert_eq!(warmed.revision, value.revision);
            assert_eq!(warmed.record.value, value.record.value);
            complete.elapsed.push(complete_started.elapsed());
            observations.push(serde_json::json!({
                "sequence": sequence,
                "preparation_us": preparation.elapsed.last().expect("sample").as_micros(),
                "commit_us": commit.elapsed.last().expect("sample").as_micros(),
                "first_us": first.elapsed.last().expect("sample").as_micros(),
                "warm_us": warm.elapsed.last().expect("sample").as_micros(),
                "complete_us": complete.elapsed.last().expect("sample").as_micros(),
            }));
            let inspected = Instant::now();
            let dataset = lance::Dataset::open(table.to_str().expect("UTF-8 fixture"))
                .await
                .expect("read-only native metadata observation");
            let versions = dataset.versions().await.expect("retained versions").len();
            let ids = dataset
                .manifest()
                .fragments
                .iter()
                .map(|fragment| fragment.id)
                .collect::<HashSet<_>>();
            let collected = versions < previous_versions;
            if sequence == 256 && retained.is_some() {
                original_backlog = ids.clone();
            }
            if collected || sequence % 64 == 0 {
                let fragments = dataset.manifest().fragments.iter().map(|fragment| {
                    let bytes = fragment.files.iter().map(|file| {
                        std::fs::metadata(table.join("data").join(&file.path)).expect("active data metadata").len()
                    }).sum::<u64>();
                    serde_json::json!({"id": fragment.id, "data_bytes": bytes, "physical_rows": fragment.physical_rows})
                }).collect::<Vec<_>>();
                checkpoints.push(serde_json::json!({
                    "sequence": sequence, "gc_observed": collected, "snapshot_retained": retained.is_some(),
                    "native_version": dataset.version_id(), "retained_versions": versions,
                    "fragments": fragments,
                    "removed_since_previous_observation": previous_ids.difference(&ids).copied().collect::<Vec<_>>(),
                    "original_backlog_remaining": original_backlog.intersection(&ids).count(),
                    "original_cold_fragments_remaining": cold_fragments.intersection(&ids).count(),
                    "disk": self.disk(), "memory": Self::memory(),
                }));
            }
            metadata_time += inspected.elapsed();
            if sequence == 256
                && let Some(mut original) = retained.take()
            {
                assert!(
                    original
                        .load(Self::row(1))
                        .await
                        .expect("old snapshot remains stable")
                        .is_none()
                );
                assert!(
                    versions >= 257,
                    "the live lease preserved actual native history"
                );
                let release_started = Instant::now();
                original
                    .rollback()
                    .await
                    .expect("release and join retained public snapshot");
                release = release_started.elapsed();
            }
            previous_ids = ids;
            previous_versions = versions;
            drop(dataset);
        }
        if matches!(profile, FragmentProfile::RetainedBacklog) {
            assert!(
                original_backlog.intersection(&previous_ids).count() < original_backlog.len(),
                "continued genuine writes reclaim some original fragment debt after lease release"
            );
        }
        let shutdown_started = Instant::now();
        drop(store);
        let shutdown = shutdown_started.elapsed();
        let reopen_started = Instant::now();
        let reopened = self.connect().await;
        let reopen = reopen_started.elapsed();
        let verification_started = Instant::now();
        let mut view = reopened
            .begin_transaction()
            .await
            .expect("atomic public verification");
        for sequence in 1..=512_u64 {
            let value = view
                .load(Self::row(sequence as usize))
                .await
                .expect("complete final inventory")
                .expect("original row");
            let payload = if matches!(profile, FragmentProfile::MixedCold) && sequence % 4 == 0 {
                self.payload.repeat(128)
            } else {
                self.payload.clone()
            };
            assert_eq!(value.revision.get(), 1);
            assert_eq!(
                value.record.value.as_ref(),
                &serde_json::json!({"sequence": sequence, "payload": payload})
            );
        }
        if let Some(cold) = cold {
            let value = view
                .load(Self::key("cold", "retained"))
                .await
                .expect("cold record")
                .expect("present cold record");
            assert_eq!(value.revision.get(), 1);
            assert_eq!(value.record.value, cold);
        }
        view.rollback()
            .await
            .expect("verification ownership settled");
        let verification = verification_started.elapsed();
        let final_join_started = Instant::now();
        drop(reopened);
        let final_join = final_join_started.elapsed();
        Self::emit(serde_json::json!({
            "kind": "fragment_maintenance", "profile": match profile { FragmentProfile::Homogeneous => "homogeneous", FragmentProfile::RetainedBacklog => "retained_backlog", FragmentProfile::MixedCold => "mixed_cold" },
            "actual_distinct_key_commits": 512, "protected_commits": if matches!(profile, FragmentProfile::RetainedBacklog) { 256 } else { 0 },
            "setup_us": setup.as_micros(), "preparation": preparation.report(), "commit": commit.report(),
            "first": first.report(), "warm": warm.report(), "complete": complete.report(), "observations": observations,
            "metadata_inspection_us": metadata_time.as_micros(), "snapshot_release_us": release.as_micros(),
            "checkpoints": checkpoints, "original_backlog_ids": original_backlog,
            "fixture_retained_expected_cold_bytes": if matches!(profile, FragmentProfile::MixedCold) { 32 * 1024 * 1024 } else { 0 },
            "shutdown_us": shutdown.as_micros(), "reopen_us": reopen.as_micros(),
            "verification_us": verification.as_micros(), "final_join_us": final_join.as_micros(),
            "total_fixture_us": started.elapsed().as_micros(), "initial_memory": initial_memory,
            "final_memory": Self::memory(), "disk": self.disk(),
            "timing_scope": "complete public begin/stage/commit/first/warm; native metadata inspection, setup, release, verification and finalization reported separately",
            "debt_scope": "old fragment identity reclamation and current total fragments are separate; removal of old debt does not imply total debt fell",
            "measurement_caveat": "native metadata opens and version listings outside timers can warm metadata or filesystem caches; this is the identical diagnostic sequence across candidates, not an isolated application benchmark",
        }));
    }

    async fn measure_image_advancement(&self, retention: ImageRetention) {
        let retain_peer = matches!(retention, ImageRetention::Current);
        let retain_history = matches!(retention, ImageRetention::Original);
        let updates = if retain_history { 80_u64 } else { 25 };
        let started = Instant::now();
        let store = self.connect().await;
        // One seed publication keeps ordinary trials below the unchanged
        // 64-version boundary. The original-snapshot trial deliberately holds
        // protection across that boundary and measures reclamation separately.
        let mut seed = store
            .begin_transaction()
            .await
            .expect("image seed snapshot");
        for index in 0..self.rows {
            seed.put(StateRecord::new(
                Self::row(index),
                serde_json::json!({"payload": self.payload, "sequence": 0}),
            ))
            .await
            .expect("image seed row");
        }
        seed.commit().await.expect("image seed publication");
        let original = if retain_history {
            Some(
                store
                    .begin_transaction()
                    .await
                    .expect("original history protection"),
            )
        } else {
            None
        };
        let setup = started.elapsed();
        assert_eq!(self.native_versions().await, (2, 2));
        let initial_disk = self.disk();
        let initial_memory = Self::memory();
        let initial_hint_present = self.hint_present();
        let mut preparation = Samples::new();
        let mut commits = Samples::new();
        let mut first = Samples::new();
        let mut warm = Samples::new();
        let mut complete = Samples::new();
        let mut observations = Vec::new();
        println!(
            "LANCE_STATE_IMAGE rows={} retained_peer={retain_peer} retained_history={retain_history} phase=updates event=enter commits={updates}",
            self.rows
        );
        for sequence in 1..=updates {
            let complete_started = Instant::now();
            let preparation_started = Instant::now();
            // A freshly captured peer must remain alive through this commit.
            // One ancient peer would share only the first committing image.
            let mut peer = if retain_peer {
                Some(
                    store
                        .begin_transaction()
                        .await
                        .expect("retained image peer"),
                )
            } else {
                None
            };
            let peer_preparation = preparation_started.elapsed();
            let mut tx = store
                .begin_transaction()
                .await
                .expect("image update snapshot");
            for index in 0..2 {
                tx.put(StateRecord::new(
                    Self::row(index),
                    serde_json::json!({"payload": self.payload, "sequence": sequence}),
                ))
                .await
                .expect("fixed two-key image update");
            }
            let prepared = preparation_started.elapsed();
            let commit_started = Instant::now();
            tx.commit().await.expect("image update publication");
            let committed = commit_started.elapsed();
            let first_started = Instant::now();
            let observed = store
                .load(Self::row(0))
                .await
                .expect("first image observation")
                .expect("updated image row");
            let first_elapsed = first_started.elapsed();
            let warm_started = Instant::now();
            let repeated = store
                .load(Self::row(1))
                .await
                .expect("warm image observation")
                .expect("second updated image row");
            let warm_elapsed = warm_started.elapsed();
            for row in [observed, repeated] {
                assert_eq!(row.revision.get(), sequence + 1);
                assert_eq!(row.record.value["sequence"], sequence);
                assert_eq!(row.record.value["payload"], self.payload);
            }
            let release_started = Instant::now();
            if let Some(mut peer) = peer.take() {
                for index in 0..2 {
                    let original = peer
                        .load(Self::row(index))
                        .await
                        .expect("retained peer read")
                        .expect("original peer row");
                    assert_eq!(original.revision.get(), sequence);
                    assert_eq!(original.record.value["sequence"], sequence - 1);
                    assert_eq!(original.record.value["payload"], self.payload);
                }
                peer.rollback().await.expect("retained peer settled");
            }
            let peer_release = release_started.elapsed();
            let complete_elapsed = complete_started.elapsed();
            // Filesystem observations are outside every operation timer.
            let hint_present = self.hint_present();
            let measured = sequence > 5;
            if measured {
                preparation.elapsed.push(prepared);
                commits.elapsed.push(committed);
                first.elapsed.push(first_elapsed);
                warm.elapsed.push(warm_elapsed);
                complete.elapsed.push(complete_elapsed);
            }
            observations.push(serde_json::json!({
                "sequence": sequence, "measured": measured,
                "peer_preparation_us": peer_preparation.as_micros(),
                "preparation_us": prepared.as_micros(), "commit_us": committed.as_micros(),
                "first_us": first_elapsed.as_micros(), "warm_us": warm_elapsed.as_micros(),
                "peer_verification_and_release_us": peer_release.as_micros(),
                "complete_us": complete_elapsed.as_micros(),
                "hint_present": hint_present,
            }));
        }
        let protected_versions = self.native_versions().await;
        assert_eq!(protected_versions, (updates + 2, updates as usize + 2));
        let protected_disk = self.disk();
        let settled_memory = Self::memory();
        let release_started = Instant::now();
        if let Some(mut original) = original {
            for index in 0..2 {
                let row = original
                    .load(Self::row(index))
                    .await
                    .expect("original history read")
                    .expect("original history row");
                assert_eq!(row.revision.get(), 1);
                assert_eq!(row.record.value["sequence"], 0);
                assert_eq!(row.record.value["payload"], self.payload);
            }
            original
                .rollback()
                .await
                .expect("original history released");
        }
        let history_release = release_started.elapsed();
        let join_started = Instant::now();
        drop(store);
        let owner_join = join_started.elapsed();
        assert_eq!(self.native_versions().await, protected_versions);
        let final_disk = self.disk();
        let reopen_started = Instant::now();
        let reopened = self.connect().await;
        let reopen = reopen_started.elapsed();
        let mut tx = reopened
            .begin_transaction()
            .await
            .expect("image verification snapshot");
        for index in 0..self.rows {
            let record = tx
                .load(Self::row(index))
                .await
                .expect("reopened image read")
                .expect("all image rows retained");
            let sequence = if index < 2 { updates } else { 0 };
            assert_eq!(record.revision.get(), sequence + 1);
            assert_eq!(record.record.value["sequence"], sequence);
            assert_eq!(record.record.value["payload"], self.payload);
        }
        tx.rollback().await.expect("image verification settled");
        let final_join_started = Instant::now();
        drop(reopened);
        let final_owner_join = final_join_started.elapsed();
        let final_versions = self.native_versions().await;
        if retain_history {
            assert_eq!(final_versions, (updates + 3, 1));
        } else {
            assert_eq!(final_versions, protected_versions);
        }
        Self::emit(serde_json::json!({
            "kind": if retain_history { "retained_history" } else { "image_advancement" },
            "rows": self.rows, "retained_peer": retain_peer, "retained_original_snapshot": retain_history,
            "payload_string_bytes": self.payload.len(), "mutated_keys_per_commit": 2,
            "warmup_commits": 5, "measured_commits": updates - 5,
            "retained_versions": protected_versions.1, "protected_head": protected_versions.0,
            "maintenance_during_updates": false, "maintenance_observed": retain_history,
            "final_head": final_versions.0, "final_retained_versions": final_versions.1,
            "setup_us": setup.as_micros(),
            "preparation": preparation.report(), "commit": commits.report(),
            "first": first.report(), "warm": warm.report(), "complete": complete.report(),
            "complete_scope": "peer and writer preparation through commit, first/warm reads, value assertions, and peer verification/rollback",
            "observations": observations, "owner_join_us": owner_join.as_micros(),
            "reopen_us": reopen.as_micros(), "final_owner_join_us": final_owner_join.as_micros(),
            "reopen_includes_eligible_maintenance": retain_history,
            "original_snapshot_verification_and_release_us": history_release.as_micros(),
            "initial_disk": initial_disk, "final_disk": final_disk,
            "protected_disk": protected_disk, "post_reopen_disk": self.disk(),
            "initial_hint_present": initial_hint_present, "final_hint_present": self.hint_present(),
            "initial_memory": initial_memory, "settled_memory": settled_memory,
            "final_memory": Self::memory(), "dataset_directory": self.root(),
        }));
    }

    async fn measure_maintenance_boundary(&self, structured: bool) {
        let scalar = serde_json::Value::String(self.payload.clone());
        let encoded_bytes = serde_json::to_vec(&scalar).expect("scalar encoding").len();
        let payload = if structured {
            let items = (0..4096_u64)
                .map(|sequence| {
                    serde_json::json!({"sequence": sequence, "receipt": format!("r{sequence:08}"), "settled": true})
                })
                .collect::<Vec<_>>();
            let mut value = serde_json::json!({"items": items, "padding": ""});
            let used = serde_json::to_vec(&value)
                .expect("structured encoding")
                .len();
            assert!(used <= encoded_bytes, "structured rows fit the byte budget");
            value["padding"] = serde_json::json!("x".repeat(encoded_bytes - used));
            assert_eq!(
                serde_json::to_vec(&value).expect("padded encoding").len(),
                encoded_bytes,
                "scalar and structured JSON have exactly equal encoded bytes"
            );
            value
        } else {
            scalar
        };
        let payload = Arc::new(payload);
        assert_eq!(self.rows, 128);
        assert_eq!(
            LanceStateStoreConfig::directory(self.root()).operation_timeout(),
            Duration::from_secs(30)
        );
        let shape = if structured {
            "structured"
        } else {
            "scalar_string"
        };
        let initial_memory = Self::memory();
        let connection_started = Instant::now();
        let store = self.connect().await;
        let connection = connection_started.elapsed();
        let mut phase = "seed";
        let mut phase_started = Instant::now();
        let mut observations = Vec::new();
        let mut serial_commits = Vec::new();
        let mut before_disk = serde_json::Value::Null;
        let body: Result<(), StateStoreError> = async {
            assert_eq!(self.native_versions().await, (1, 1));
            let mut tx = store.begin_transaction().await?;
            for index in 0..self.rows {
                tx.put(StateRecord {
                    key: Self::row(index),
                    value: Arc::clone(&payload),
                })
                .await?;
            }
            tx.commit().await?;
            observations.push(serde_json::json!({"phase": phase, "elapsed_us": phase_started.elapsed().as_micros()}));
            assert_eq!(self.native_versions().await, (2, 2));
            phase = "counter_preparation";
            phase_started = Instant::now();
            for sequence in 1..=61_u64 {
                let preparation_started = Instant::now();
                let mut tx = store.begin_transaction().await?;
                tx.put(StateRecord::new(Self::key("counter", "value"), serde_json::json!(sequence)))
                    .await?;
                let preparation = preparation_started.elapsed();
                let commit_started = Instant::now();
                tx.commit().await?;
                let committed = commit_started.elapsed();
                serial_commits.push(serde_json::json!({
                    "sequence": sequence,
                    "preparation_us": preparation.as_micros(),
                    "commit_us": committed.as_micros(),
                }));
            }
            observations.push(serde_json::json!({"phase": phase, "elapsed_us": phase_started.elapsed().as_micros(), "commits": 61}));
            assert_eq!(self.native_versions().await, (63, 63));
            before_disk = self.disk();
            phase = "warm_before_boundary";
            phase_started = Instant::now();
            let row = store.load(Self::row(0)).await?.expect("seeded row");
            let elapsed = phase_started.elapsed();
            assert_eq!(row.revision.get(), 1);
            assert!(row.record.value == payload, "warm payload remains exact");
            assert_eq!(self.native_versions().await, (63, 63));
            observations.push(serde_json::json!({"phase": phase, "elapsed_us": elapsed.as_micros(), "head": 63, "retained_versions": 63, "hint_present": self.hint_present()}));
            drop(row);

            phase = "boundary_prepare";
            phase_started = Instant::now();
            let mut tx = store.begin_transaction().await?;
            tx.put(StateRecord::new(Self::key("counter", "value"), serde_json::json!(62)))
                .await?;
            observations.push(serde_json::json!({"phase": phase, "elapsed_us": phase_started.elapsed().as_micros()}));
            phase = "boundary_commit";
            phase_started = Instant::now();
            tx.commit().await?;
            let elapsed = phase_started.elapsed();
            assert_eq!(self.native_versions().await, (64, 64));
            observations.push(serde_json::json!({"phase": phase, "elapsed_us": elapsed.as_micros(), "head": 64, "retained_versions": 64, "hint_present": self.hint_present()}));

            for next_phase in ["maintenance_boundary_load", "warm_after_boundary"] {
                phase = next_phase;
                phase_started = Instant::now();
                println!("LANCE_STATE_BOUNDARY shape={shape} phase={phase} event=enter");
                let row = store.load(Self::row(0)).await?;
                let elapsed = phase_started.elapsed();
                let row = row.expect("maintenance preserves the observed row");
                assert_eq!(row.revision.get(), 1);
                assert!(row.record.value == payload, "maintenance preserves exact JSON");
                assert_eq!(self.native_versions().await, (65, 1));
                observations.push(serde_json::json!({"phase": phase, "elapsed_us": elapsed.as_micros(), "head": 65, "retained_versions": 1, "hint_present": self.hint_present()}));
                println!("LANCE_STATE_BOUNDARY shape={shape} phase={phase} event=complete elapsed_us={}", elapsed.as_micros());
            }
            Ok(())
        }
        .await;
        let failure_elapsed = phase_started.elapsed();
        // A timeout does not identify its admission/observation/lease phase or
        // cancel admitted work. Join the sole owner before further native/file
        // measurements; never turn later maintenance completion into success.
        let shutdown_started = Instant::now();
        drop(store);
        let shutdown = shutdown_started.elapsed();
        if let Err(error) = body {
            Self::emit(serde_json::json!({
                "kind": "maintenance_boundary", "shape": shape, "outcome": "error",
                "failed_phase": phase, "failed_phase_elapsed_us": failure_elapsed.as_micros(),
                "error": format!("{error:?}"), "observations": observations,
                "connection_us": connection.as_micros(), "serial_commits": serial_commits,
                "rows": self.rows, "encoded_bytes_per_row": encoded_bytes,
                "dataset_directory": self.root(), "owner_join_us": shutdown.as_micros(),
                "initial_memory": initial_memory, "final_memory": Self::memory(),
            }));
            panic!("maintenance boundary failed at {phase}: {error:?}");
        }
        let after_disk = self.disk();
        let reopen_started = Instant::now();
        let reopened = self.connect().await;
        let reopen = reopen_started.elapsed();
        let mut tx = reopened.begin_transaction().await.expect("reopen snapshot");
        for index in 0..self.rows {
            let row = tx
                .load(Self::row(index))
                .await
                .expect("reopen read")
                .expect("all rows retained");
            assert_eq!(row.revision.get(), 1);
            assert!(
                row.record.value == payload,
                "reopened payload remains exact"
            );
        }
        let counter = tx
            .load(Self::key("counter", "value"))
            .await
            .expect("counter read")
            .expect("counter retained");
        assert_eq!(counter.revision.get(), 62);
        assert_eq!(counter.record.value.as_u64(), Some(62));
        tx.rollback().await.expect("verification settled");
        let page = reopened
            .list(
                StateListRequest::new("rows")
                    .expect("namespace")
                    .with_limit(self.rows + 1)
                    .expect("limit"),
            )
            .await
            .expect("complete payload inventory");
        assert_eq!(page.records.len(), self.rows);
        assert!(page.next_start_after.is_none());
        drop(page);
        let shutdown_started = Instant::now();
        drop(reopened);
        let final_shutdown = shutdown_started.elapsed();
        assert_eq!(self.native_versions().await, (65, 1));
        Self::emit(serde_json::json!({
            "kind": "maintenance_boundary", "shape": shape, "outcome": "ok",
            "rows": self.rows, "encoded_bytes_per_row": encoded_bytes,
            "encoded_payload_bytes": self.rows * encoded_bytes,
            "structured_items_per_row": if structured { 4096 } else { 0 },
            "counter_commits": 62, "counter_revision": 62,
            "public_records": self.rows + 1, "operation_timeout_ms": 30_000,
            "connection_us": connection.as_micros(), "serial_commits": serial_commits,
            "observations": observations, "owner_join_us": shutdown.as_micros(),
            "reopen_us": reopen.as_micros(), "final_owner_join_us": final_shutdown.as_micros(),
            "dataset_directory": self.root(), "before_disk": before_disk, "after_disk": after_disk,
            "final_disk": self.disk(), "initial_memory": initial_memory, "final_memory": Self::memory(),
            "final_hint_present": self.hint_present(),
            "scope": "one directory and handle; serial public operations; native metadata only after successful operations or final owner join; mechanism comparison, not an App dataset replica",
        }));
    }

    async fn mutate(
        &self,
        store: &StateStore,
        id: &str,
        first: usize,
        width: usize,
    ) -> Result<(), StateStoreError> {
        let mut tx = store.begin_transaction().await?;
        for offset in 0..width {
            let key = Self::row((first + offset) % self.rows);
            let before = tx.load(key.clone()).await?.expect("seeded mutation row");
            let sequence = before.record.value["sequence"].as_u64().expect("sequence");
            tx.put(StateRecord::new(
                key,
                serde_json::json!({"payload": self.payload, "sequence": sequence + 1}),
            ))
            .await?;
        }
        for namespace in ["receipts", "companions"] {
            tx.put(StateRecord::new(
                Self::key(namespace, id),
                serde_json::json!(id),
            ))
            .await?;
        }
        tx.commit().await
    }

    async fn attempt(
        &self,
        store: &StateStore,
        id: String,
        first: usize,
        width: usize,
        samples: &mut Samples,
    ) -> Attempt {
        let started = Instant::now();
        let result = self.mutate(store, &id, first, width).await;
        samples.elapsed.push(started.elapsed());
        let publication = match result {
            Ok(()) => Publication::Acknowledged,
            Err(StateStoreError::TransactionConflict { .. }) => {
                samples.conflicts += 1;
                Publication::Conflict
            }
            Err(StateStoreError::TimedOut { .. }) => {
                samples.timeouts += 1;
                Publication::TimedOut
            }
            Err(error) => panic!("unexpected capacity mutation failure: {error:?}"),
        };
        Attempt {
            id,
            first,
            width,
            publication,
        }
    }

    async fn verify(&self, evidence: &[Attempt]) -> usize {
        // Callers drop every writer first, so admitted finalization and its
        // owned worker join have settled before timeout reconciliation.
        let store = self.connect().await;
        let mut tx = store
            .begin_transaction()
            .await
            .expect("one atomic verification view");
        let mut expected = HashMap::<usize, u64>::new();
        let mut publications = 0;
        for attempt in evidence {
            let receipt = tx
                .load(Self::key("receipts", &attempt.id))
                .await
                .expect("receipt reconciliation");
            let companion = tx
                .load(Self::key("companions", &attempt.id))
                .await
                .expect("companion reconciliation");
            assert_eq!(
                receipt.is_some(),
                companion.is_some(),
                "atomic receipt and companion"
            );
            match attempt.publication {
                Publication::Acknowledged => assert!(receipt.is_some()),
                Publication::Conflict => assert!(receipt.is_none()),
                Publication::TimedOut => {}
            }
            if let Some(receipt) = receipt {
                assert_eq!(receipt.record.value.as_str(), Some(attempt.id.as_str()));
                assert_eq!(receipt.revision.get(), 1);
                let companion = companion.expect("paired value");
                assert_eq!(companion.record.value.as_str(), Some(attempt.id.as_str()));
                assert_eq!(companion.revision.get(), 1);
                publications += 1;
                for offset in 0..attempt.width {
                    *expected
                        .entry((attempt.first + offset) % self.rows)
                        .or_default() += 1;
                }
            }
        }
        for index in 0..self.rows {
            let sequence = expected.get(&index).copied().unwrap_or(0);
            let row = tx
                .load(Self::row(index))
                .await
                .expect("sequence read")
                .expect("sequence row");
            assert_eq!(row.record.value["sequence"].as_u64(), Some(sequence));
            assert_eq!(
                row.record.value["payload"].as_str(),
                Some(self.payload.as_str())
            );
        }
        tx.put(StateRecord::new(
            Self::key("progress", "verified"),
            serde_json::json!(true),
        ))
        .await
        .expect("subsequent progress");
        tx.commit().await.expect("progress after reconciliation");
        publications
    }

    fn disk(&self) -> serde_json::Value {
        let mut pending = vec![self.root().to_path_buf()];
        let mut files = 0_u64;
        let mut bytes = 0_u64;
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).expect("owned dataset directory") {
                let entry = entry.expect("dataset entry");
                let metadata = entry.metadata().expect("dataset metadata");
                if metadata.is_dir() {
                    pending.push(entry.path());
                } else {
                    assert!(metadata.is_file());
                    files += 1;
                    bytes += metadata.len();
                }
            }
        }
        serde_json::json!({"files": files, "bytes": bytes, "scope": "complete owned fixture, including native history and optional knowledge tables"})
    }

    fn hint_present(&self) -> bool {
        let path = self
            .root()
            .join("__ai_kit_state.lance/_versions/latest_version_hint.json");
        match std::fs::metadata(path) {
            Ok(metadata) => {
                assert!(metadata.is_file(), "the observed hint is a regular file");
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => panic!("version-hint observation failed: {error}"),
        }
    }

    fn memory() -> serde_json::Value {
        let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
            return serde_json::json!({"available": false, "reason": "optional self-process counters unavailable; no process inspection is required"});
        };
        let mut rss = None;
        let mut peak = None;
        for line in status.lines() {
            let mut words = line.split_whitespace();
            match words.next() {
                Some("VmRSS:") => rss = words.next().and_then(|word| word.parse::<u64>().ok()),
                Some("VmHWM:") => peak = words.next().and_then(|word| word.parse::<u64>().ok()),
                _ => {}
            }
        }
        serde_json::json!({"available": rss.is_some(), "rss_kib": rss, "peak_rss_kib": peak, "scope": "whole test process and native caches"})
    }

    async fn native_versions(&self) -> (u64, usize) {
        let path = self.root().join("__ai_kit_state.lance");
        let dataset = lance::Dataset::open(path.to_str().expect("UTF-8 fixture path"))
            .await
            .expect("settled native measurement view");
        let versions = dataset
            .versions()
            .await
            .expect("retained version measurement");
        (dataset.version_id(), versions.len())
    }

    async fn versions(&self) -> usize {
        let path = self.root().join("__ai_kit_state.lance");
        lance::Dataset::open(path.to_str().expect("UTF-8 fixture path"))
            .await
            .expect("native measurement view")
            .versions()
            .await
            .expect("retained version measurement")
            .len()
    }

    fn emit(mut report: serde_json::Value) {
        report["warmup_policy"] = serde_json::json!(if report["kind"] == "profile" {
            "five discarded warmups per warm read/list/mutation class; cold opens intentionally have none"
        } else if report["kind"] == "image_advancement" || report["kind"] == "retained_history" {
            "five update warmups retained in raw observations but excluded from measured summaries"
        } else {
            "bounded whole-scenario measurement with no discarded warmups"
        });
        report["label"] = serde_json::json!(
            std::env::var("AI_KIT_LANCE_PERF_LABEL").unwrap_or_else(|_| "unlabelled".to_owned())
        );
        report["platform"] =
            serde_json::json!({"os": std::env::consts::OS, "arch": std::env::consts::ARCH});
        report["interpretation"] = serde_json::json!(
            "bounded public-API measurement; no fixed SLO; no device-write or power-loss claim; sample counts and unavailable optional metrics are explicit"
        );
        println!(
            "LANCE_STATE_CAPACITY {}",
            serde_json::to_string(&report).expect("measurement receipt")
        );
    }
}

#[derive(Clone, Copy)]
enum Contention {
    Single,
    Shared,
    Independent,
    Hot,
    Knowledge,
}

impl Contention {
    fn name(self) -> &'static str {
        match self {
            Self::Single => "one_writer",
            Self::Shared => "eight_shared_disjoint",
            Self::Independent => "eight_independent_disjoint",
            Self::Hot => "eight_independent_hot",
            Self::Knowledge => "same_root_knowledge",
        }
    }

    async fn measure(self) {
        let fixture = Workload::new(1000, "x".repeat(1024));
        let store = fixture.connect().await;
        fixture.seed(&store).await;
        let count = if matches!(self, Self::Single | Self::Knowledge) {
            1
        } else {
            8
        };
        let mut writers = Vec::new();
        for _ in 0..count {
            writers.push(if matches!(self, Self::Independent | Self::Hot) {
                fixture.connect().await
            } else {
                store.clone()
            });
        }
        let database = if matches!(self, Self::Knowledge) {
            let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
                fixture.root(),
            )))
            .await
            .expect("same-root knowledge database");
            database
                .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
                    locator: StorageLocator::new("capacity_knowledge"),
                    schema: None,
                    records: vec![
                        DataRecord::without_id()
                            .json_field("body", serde_json::json!({"index": 0, "payload": "seed"})),
                    ],
                    mode: CreateCollectionMode::Create,
                }))
                .await
                .expect("knowledge collection");
            Some(database)
        } else {
            None
        };
        let started = Instant::now();
        let state_work =
            futures::future::join_all(writers.iter().enumerate().map(|(writer, store)| {
                let fixture = &fixture;
                async move {
                    let mut samples = Samples::new();
                    let mut evidence = Vec::new();
                    for attempt in 0..128 / count {
                        let first = if matches!(self, Self::Hot) {
                            0
                        } else {
                            writer * 32 + attempt
                        };
                        evidence.push(
                            fixture
                                .attempt(
                                    store,
                                    format!("writer-{writer}/{attempt:04}"),
                                    first,
                                    1,
                                    &mut samples,
                                )
                                .await,
                        );
                    }
                    (samples.report(), evidence)
                }
            }));
        let knowledge_work = async {
            if let Some(database) = &database {
                for index in 0..128 {
                    database
                        .data(DataRequest::Insert(InsertRequest::append(
                            StorageLocator::new("capacity_knowledge"),
                            vec![DataRecord::without_id().json_field(
                                "body",
                                serde_json::json!({"index": index, "payload": "k".repeat(1024)}),
                            )],
                        )))
                        .await
                        .expect("concurrent knowledge insertion");
                }
            }
        };
        let (results, ()) = tokio::join!(state_work, knowledge_work);
        let observation_elapsed = started.elapsed();
        let mut evidence = Vec::new();
        let mut reports = Vec::new();
        for (report, attempts) in results {
            reports.push(report);
            evidence.extend(attempts);
        }
        drop(writers);
        drop(store);
        let finalization_elapsed = started.elapsed();
        if let Some(database) = &database {
            let count = database
                .data(DataRequest::Count(CountRequest {
                    locator: StorageLocator::new("capacity_knowledge"),
                    filter: None,
                }))
                .await
                .expect("independent knowledge count");
            let DataResponse::Count(count) = count else {
                panic!("knowledge count response");
            };
            assert_eq!(count, 129);
        }
        drop(database);
        let verified = fixture.verify(&evidence).await;
        Workload::emit(
            serde_json::json!({"kind": "contention", "scenario": self.name(), "attempts": evidence.len(), "observation_elapsed_ms": observation_elapsed.as_millis(), "finalization_elapsed_ms": finalization_elapsed.as_millis(), "acknowledged_or_reconciled_publications": verified, "publications_per_second": verified as f64 / finalization_elapsed.as_secs_f64(), "writers": reports, "disk": fixture.disk(), "memory": Workload::memory()}),
        );
    }
}

#[tokio::test]
async fn capacity_fragment_maintenance_homogeneous_512() {
    Workload::new(512, "x".repeat(128))
        .measure_fragment_maintenance(FragmentProfile::Homogeneous)
        .await;
}

#[tokio::test]
async fn capacity_fragment_maintenance_retained_backlog() {
    Workload::new(512, "x".repeat(128))
        .measure_fragment_maintenance(FragmentProfile::RetainedBacklog)
        .await;
}

#[tokio::test]
async fn capacity_fragment_maintenance_mixed_cold() {
    Workload::new(512, "x".repeat(128))
        .measure_fragment_maintenance(FragmentProfile::MixedCold)
        .await;
}

#[tokio::test]
async fn capacity_image_advancement_with_and_without_retained_peers() {
    for rows in [100, 1000, 10_000] {
        for retention in [ImageRetention::None, ImageRetention::Current] {
            Workload::new(rows, "x".repeat(128))
                .measure_image_advancement(retention)
                .await;
        }
    }
}

#[tokio::test]
async fn capacity_retained_snapshot_history_beyond_maintenance_threshold() {
    Workload::new(1000, "x".repeat(128))
        .measure_image_advancement(ImageRetention::Original)
        .await;
}

#[tokio::test]
async fn capacity_maintenance_boundary_scalar() {
    let fixture = Workload::new(128, "x".repeat(256 * 1024 - 2));
    assert_eq!(fixture.payload.len() + 2, 256 * 1024);
    fixture.measure_maintenance_boundary(false).await;
}

#[tokio::test]
async fn capacity_maintenance_boundary_structured() {
    let fixture = Workload::new(128, "x".repeat(256 * 1024 - 2));
    assert_eq!(fixture.payload.len() + 2, 256 * 1024);
    fixture.measure_maintenance_boundary(true).await;
}

#[cfg(unix)]
#[tokio::test]
async fn capacity_directory_depth_pairs() {
    use std::os::unix::fs::MetadataExt;

    let parent = tempfile::tempdir().expect("owned same-filesystem comparison");
    let device = parent.path().metadata().expect("comparison device").dev();
    let space = std::process::Command::new("df")
        .arg("-kP")
        .arg(parent.path())
        .output()
        .expect("ordinary available-space observation");
    assert!(space.status.success(), "available-space command succeeds");
    println!(
        "LANCE_STATE_DEPTH filesystem_device={device} available_space={:?}",
        String::from_utf8(space.stdout).expect("available-space output is UTF-8")
    );
    // Reverse the second pair's order without reusing mutable datasets. Each
    // sample retains the same 64-version boundary and all reopen assertions.
    for (pair, depths) in [[0, 8], [8, 0]].into_iter().enumerate() {
        for depth in depths {
            let mut directory = parent.path().to_path_buf();
            for level in 0..depth {
                directory.push(format!("level-{level:02}"));
            }
            std::fs::create_dir_all(&directory).expect("owned comparison ancestry");
            let fixture = Workload {
                directory: tempfile::Builder::new()
                    .prefix("depth-")
                    .tempdir_in(directory)
                    .expect("independent depth dataset"),
                rows: 128,
                payload: "x".repeat(1024 - 2),
            };
            assert_eq!(fixture.payload.len() + 2, 1024);
            assert_eq!(
                fixture.root().metadata().expect("dataset device").dev(),
                device,
                "paired datasets occupy the same filesystem"
            );
            println!(
                "LANCE_STATE_DEPTH pair={} added_depth={depth} canonical_root={:?} event=enter",
                pair + 1,
                fixture.root().canonicalize().expect("canonical dataset")
            );
            let started = Instant::now();
            fixture.measure_maintenance_boundary(false).await;
            println!(
                "LANCE_STATE_DEPTH pair={} added_depth={depth} event=complete elapsed_us={}",
                pair + 1,
                started.elapsed().as_micros()
            );
        }
    }
}

#[tokio::test]
async fn capacity_small() {
    Profile::Small.measure().await;
}

#[tokio::test]
async fn capacity_commit_followed_by_observation() {
    for profile in [Profile::Small, Profile::Cardinality, Profile::Payload] {
        profile.measure_commit_observation().await;
    }
}
#[tokio::test]
async fn capacity_medium() {
    Profile::Medium.measure().await;
}
#[tokio::test]
async fn capacity_cardinality() {
    Profile::Cardinality.measure().await;
}
#[tokio::test]
async fn capacity_medium_payload() {
    Profile::Payload.measure().await;
}
#[tokio::test]
async fn capacity_large_payload() {
    Profile::Large.measure().await;
}
#[tokio::test]
async fn capacity_tombstones() {
    Profile::Tombstones.measure().await;
}
#[tokio::test]
async fn capacity_retained_versions() {
    Profile::Versions.measure().await;
}
#[tokio::test]
async fn capacity_less_compressible() {
    Profile::Entropy.measure().await;
}
#[tokio::test]
async fn contention_one_writer() {
    Contention::Single.measure().await;
}
#[tokio::test]
async fn contention_shared_writers() {
    Contention::Shared.measure().await;
}
#[tokio::test]
async fn contention_independent_writers() {
    Contention::Independent.measure().await;
}
#[tokio::test]
async fn contention_hot_key() {
    Contention::Hot.measure().await;
}
#[tokio::test]
async fn contention_same_root_knowledge() {
    Contention::Knowledge.measure().await;
}

#[tokio::test]
async fn capacity_retained_transaction_images() {
    let fixture = Workload::new(1000, "x".repeat(4096));
    let store = fixture.connect().await;
    fixture.seed(&store).await;
    let mut reports = Vec::new();
    for count in [1, 8, 32] {
        let mut retained = Vec::new();
        let mut samples = Samples::new();
        for index in 0..count {
            let Some(mut tx) = samples.observe(store.begin_transaction()).await else {
                break;
            };
            assert!(
                tx.load(Workload::row(index))
                    .await
                    .expect("retained read")
                    .is_some()
            );
            retained.push(tx);
        }
        reports.push(serde_json::json!({"requested_transactions": count, "retained_transactions": retained.len(), "begin": samples.report(), "memory": Workload::memory()}));
        for tx in retained {
            tx.rollback().await.expect("retained rollback");
        }
        assert!(
            store
                .load(Workload::row(0))
                .await
                .expect("progress after retention")
                .is_some()
        );
    }
    drop(store);
    Workload::emit(
        serde_json::json!({"kind": "retained_images", "measurements": reports, "final_memory": Workload::memory()}),
    );
}

#[tokio::test]
async fn capacity_fixed_bytes_different_batch_counts() {
    let mut reports = Vec::new();
    for rows in [1, 16, 128, 1024] {
        let fixture = Workload::new(rows, "x".repeat(16 * 1024 * 1024 / rows));
        let store = fixture.connect().await;
        fixture.seed(&store).await;
        let mut samples = Samples::new();
        let mut evidence = Vec::new();
        for attempt in 0..3 {
            evidence.push(
                fixture
                    .attempt(&store, format!("fixed/{attempt}"), 0, rows, &mut samples)
                    .await,
            );
        }
        drop(store);
        let verified = fixture.verify(&evidence).await;
        reports.push(serde_json::json!({"rows": rows, "payload_string_bytes": fixture.payload.len(), "total_payload_string_bytes": rows * fixture.payload.len(), "mutation": samples.report(), "verified_publications": verified, "disk": fixture.disk(), "memory": Workload::memory()}));
    }
    let report = serde_json::json!({"kind": "fixed_authored_bytes", "measurements": reports});
    Workload::emit(report);
}
