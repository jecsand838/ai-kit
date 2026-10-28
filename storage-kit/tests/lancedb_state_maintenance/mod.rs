use std::{
    fs,
    path::Path,
    process::{Child, Command},
    time::{Duration, Instant},
};

use lance::Dataset;
use serde_json::json;
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateRecord},
    },
};
use tempfile::TempDir;

struct MaintenanceCase {
    directory: TempDir,
}

struct MaintenanceProcess {
    child: Child,
}

impl MaintenanceProcess {
    fn spawn(directory: &Path) -> Self {
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "lancedb_state_maintenance::maintenance_process_entry",
                "--nocapture",
            ])
            .env("AI_KIT_LANCE_MAINTENANCE_CHILD", directory)
            .spawn()
            .expect("supervised ordinary child");
        Self { child }
    }

    async fn wait_for(&mut self, path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !path.exists() {
            assert!(
                self.child.try_wait().expect("child status").is_none(),
                "child exited before its marker"
            );
            assert!(Instant::now() < deadline, "bounded child marker wait");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn finish(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Some(status) = self.child.try_wait().expect("child status") {
                assert!(status.success(), "maintenance child failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "bounded child completion");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for MaintenanceProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl MaintenanceCase {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("maintenance fixture"),
        }
    }

    async fn connect(&self) -> StateStore {
        StateStore::connect(StateStoreConfig::lance(
            LanceStateStoreConfig::directory(self.directory.path())
                .with_operation_timeout(Duration::from_secs(120))
                .expect("valid operation timeout"),
        ))
        .await
        .expect("connect supplied state store")
    }

    fn key(name: &str) -> StateKey {
        StateKey::new("maintenance", name).expect("valid fixture key")
    }

    async fn native(&self) -> Dataset {
        Dataset::open(
            self.directory
                .path()
                .join("__ai_kit_state.lance")
                .to_str()
                .expect("fixture path"),
        )
        .await
        .expect("open native fixture for read-only evidence")
    }

    async fn versions(&self) -> usize {
        self.native()
            .await
            .versions()
            .await
            .expect("retained versions")
            .len()
    }

    async fn write(store: &StateStore, sequence: u64) {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("begin fixture mutation");
        transaction
            .put(StateRecord::new(
                Self::key("head"),
                json!({"sequence": sequence}),
            ))
            .await
            .expect("stage head");
        transaction.commit().await.expect("commit fixture mutation");
    }

    async fn settle_maintenance(&self, store: &StateStore) {
        for _ in 0..35 {
            store
                .load(Self::key("head"))
                .await
                .expect("ordinary read permits quiescent maintenance");
            if self.versions().await == 1 {
                return;
            }
        }
        assert_eq!(
            self.versions().await,
            1,
            "quiescent maintenance reclaims obsolete versions"
        );
    }

    fn copy_directory(from: &Path, to: &Path) {
        fs::create_dir_all(to).expect("backup directory");
        for entry in fs::read_dir(from).expect("backup inventory") {
            let entry = entry.expect("backup entry");
            let destination = to.join(entry.file_name());
            if entry.file_type().expect("backup file type").is_dir() {
                Self::copy_directory(&entry.path(), &destination);
            } else {
                fs::copy(entry.path(), destination).expect("copy complete cold file");
            }
        }
    }
}

#[tokio::test]
async fn maintenance_reclaims_history_without_changing_revisions_or_tombstones() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    let oldest = store
        .begin_transaction()
        .await
        .expect("retain seed history until reclamation");
    let mut first = store.begin_transaction().await.expect("first transaction");
    first
        .delete(MaintenanceCase::key("deleted"))
        .await
        .expect("permanent tombstone");
    first.commit().await.expect("commit tombstone");
    for sequence in 1..=63 {
        MaintenanceCase::write(&store, sequence).await;
    }
    oldest.rollback().await.expect("release seed history");
    fixture.settle_maintenance(&store).await;
    let head = store
        .load(MaintenanceCase::key("head"))
        .await
        .expect("load head")
        .expect("present head");
    assert_eq!(head.revision.get(), 63);
    assert_eq!(*head.record.value, json!({"sequence": 63}));
    let mut recreate = store
        .begin_transaction()
        .await
        .expect("recreate transaction");
    recreate
        .put(StateRecord::new(
            MaintenanceCase::key("deleted"),
            json!("back"),
        ))
        .await
        .expect("recreate tombstone");
    assert_eq!(
        recreate
            .load(MaintenanceCase::key("deleted"))
            .await
            .expect("staged load")
            .expect("present")
            .revision
            .get(),
        2
    );
    recreate
        .rollback()
        .await
        .expect("rollback fixture recreation");
    drop(store);
    let reopened = fixture.connect().await;
    assert!(
        reopened
            .load(MaintenanceCase::key("deleted"))
            .await
            .expect("reopened tombstone")
            .is_none()
    );
}

#[tokio::test]
async fn maintenance_defers_for_live_transactions_but_not_idle_connections() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    let idle = fixture.connect().await;
    MaintenanceCase::write(&store, 1).await;
    let mut retained = store
        .begin_transaction()
        .await
        .expect("retained transaction");
    for sequence in 2..=70 {
        MaintenanceCase::write(&store, sequence).await;
    }
    assert!(
        fixture.versions().await >= 64,
        "live transaction retains its original native history"
    );
    assert_eq!(
        retained
            .load(MaintenanceCase::key("head"))
            .await
            .expect("retained read")
            .expect("old record")
            .revision
            .get(),
        1
    );
    retained.rollback().await.expect("release native snapshot");
    fixture.settle_maintenance(&store).await;
    assert_eq!(
        idle.load(MaintenanceCase::key("head"))
            .await
            .expect("idle connection remains fresh")
            .expect("head")
            .revision
            .get(),
        70
    );
}

#[tokio::test]
async fn memory_snapshots_preserve_state_across_maintenance_threshold_and_release() {
    let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory()))
        .await
        .expect("connect one process-local memory owner");
    let writer = store.clone();
    let mut seed = store.begin_transaction().await.expect("seed transaction");
    for (name, value) in [
        ("head", json!({"sequence": 1})),
        ("kept", json!("original")),
    ] {
        seed.put(StateRecord::new(MaintenanceCase::key(name), value))
            .await
            .expect("seed present record");
    }
    seed.delete(MaintenanceCase::key("deleted"))
        .await
        .expect("seed permanent tombstone");
    seed.commit().await.expect("publish seed atomically");

    let mut retained = store.begin_transaction().await.expect("retained snapshot");
    let mut stale = store.begin_transaction().await.expect("stale writer");
    stale
        .put(StateRecord::new(
            MaintenanceCase::key("head"),
            json!({"sequence": 999}),
        ))
        .await
        .expect("stage losing overwrite");
    stale
        .put(StateRecord::new(
            MaintenanceCase::key("losing-receipt"),
            json!(true),
        ))
        .await
        .expect("stage losing receipt");
    stale
        .delete(MaintenanceCase::key("kept"))
        .await
        .expect("stage losing deletion");
    for sequence in 2..=70 {
        MaintenanceCase::write(&writer, sequence).await;
    }
    let original = retained
        .load(MaintenanceCase::key("head"))
        .await
        .expect("read retained original version")
        .expect("original head");
    assert_eq!(original.revision.get(), 1);
    assert_eq!(*original.record.value, json!({"sequence": 1}));
    assert!(
        retained
            .load(MaintenanceCase::key("deleted"))
            .await
            .expect("retained tombstone")
            .is_none()
    );
    assert!(matches!(
        stale.commit().await,
        Err(StateStoreError::TransactionConflict { .. })
    ));
    retained
        .commit()
        .await
        .expect("a consistent read-only snapshot may finish after intervening writes");
    drop(store);

    // Cross the private scheduling threshold while snapshots are retained,
    // then give the idle memory owner enough public jobs to retry after their
    // release. Internal object reclamation is not observable through this API;
    // this case proves stable snapshots, exact state and subsequent progress.
    for _ in 0..35 {
        let head = writer
            .load(MaintenanceCase::key("head"))
            .await
            .expect("read after every retained snapshot has settled")
            .expect("current head");
        assert_eq!(head.revision.get(), 70);
        assert_eq!(*head.record.value, json!({"sequence": 70}));
    }
    let mut progress = writer
        .begin_transaction()
        .await
        .expect("atomic oracle and subsequent mutation");
    assert!(
        progress
            .load(MaintenanceCase::key("losing-receipt"))
            .await
            .expect("losing receipt read")
            .is_none()
    );
    assert!(
        progress
            .load(MaintenanceCase::key("deleted"))
            .await
            .expect("tombstone before recreation")
            .is_none()
    );
    for (name, value, revision) in [
        ("head", json!({"sequence": 70}), 70),
        ("kept", json!("original"), 1),
    ] {
        let record = progress
            .load(MaintenanceCase::key(name))
            .await
            .expect("read complete winning publication")
            .expect("losing overwrite and deletion never published");
        assert_eq!(record.revision.get(), revision);
        assert_eq!(*record.record.value, value);
    }
    for (name, value, revision) in [
        ("head", json!({"sequence": 71}), 71),
        ("deleted", json!("recreated"), 2),
    ] {
        progress
            .put(StateRecord::new(MaintenanceCase::key(name), value))
            .await
            .expect("stage progress without resetting revisions");
        assert_eq!(
            progress
                .load(MaintenanceCase::key(name))
                .await
                .expect("read staged progress")
                .expect("present staged record")
                .revision
                .get(),
            revision
        );
    }
    progress.commit().await.expect("publish after quiescence");
    let mut verified = writer.begin_transaction().await.expect("atomic final view");
    for (name, value, revision) in [
        ("head", json!({"sequence": 71}), 71),
        ("deleted", json!("recreated"), 2),
        ("kept", json!("original"), 1),
    ] {
        let record = verified
            .load(MaintenanceCase::key(name))
            .await
            .expect("read complete final publication")
            .expect("present final record");
        assert_eq!(record.revision.get(), revision);
        assert_eq!(*record.record.value, value);
    }
    verified.rollback().await.expect("settle final snapshot");
    drop(writer);
}

#[tokio::test]
async fn maintenance_startup_removes_owned_orphans_without_pruning_short_history() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    for sequence in 1..=4 {
        MaintenanceCase::write(&store, sequence).await;
    }
    drop(store);
    let retained = fixture.versions().await;
    let data = fixture.directory.path().join("__ai_kit_state.lance/data");
    let staging = data.join(format!(".state-stage-{}", uuid::Uuid::new_v4()));
    let unknown = fixture
        .directory
        .path()
        .join("__ai_kit_state.lance/operator-note.txt");
    fs::write(&staging, b"abandoned Kit staging").expect("orphan fixture");
    fs::write(&unknown, b"unrelated file").expect("unrelated fixture");
    let reopened = fixture.connect().await;
    assert!(
        !staging.exists(),
        "quiescent startup reclaims recognized abandoned staging"
    );
    assert_eq!(
        fs::read(unknown).expect("unknown file retained"),
        b"unrelated file"
    );
    assert_eq!(
        fixture.versions().await,
        retained,
        "orphan-only cleanup preserves committed history"
    );
    assert_eq!(
        reopened
            .load(MaintenanceCase::key("head"))
            .await
            .expect("load")
            .expect("head")
            .revision
            .get(),
        4
    );
}

#[tokio::test]
async fn maintenance_cold_backup_preserves_revision_and_tombstone_continuity() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    MaintenanceCase::write(&store, 1).await;
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("tombstone transaction");
    transaction
        .delete(MaintenanceCase::key("deleted"))
        .await
        .expect("delete");
    transaction.commit().await.expect("commit");
    drop(store);
    let restored = MaintenanceCase::new();
    MaintenanceCase::copy_directory(fixture.directory.path(), restored.directory.path());
    let copy = restored.connect().await;
    let mut transaction = copy
        .begin_transaction()
        .await
        .expect("restored transaction");
    transaction
        .put(StateRecord::new(
            MaintenanceCase::key("deleted"),
            json!("restored"),
        ))
        .await
        .expect("recreate");
    assert_eq!(
        transaction
            .load(MaintenanceCase::key("deleted"))
            .await
            .expect("load")
            .expect("present")
            .revision
            .get(),
        2
    );
    transaction
        .commit()
        .await
        .expect("commit restored mutation");
    let original = fixture.connect().await;
    assert!(
        original
            .load(MaintenanceCase::key("deleted"))
            .await
            .expect("original remains unchanged")
            .is_none()
    );
}

#[tokio::test]
async fn maintenance_shared_lease_wait_is_bounded_and_releases_on_connect_failure() {
    let fixture = MaintenanceCase::new();
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(
            fixture
                .directory
                .path()
                .join("__ai_kit_state.maintenance.lock"),
        )
        .expect("coordination fixture");
    file.try_lock().expect("ordinary exclusive fixture lock");
    let config = LanceStateStoreConfig::directory(fixture.directory.path())
        .with_operation_timeout(Duration::from_millis(50))
        .expect("valid timeout");
    let result = StateStore::connect(StateStoreConfig::lance(config)).await;
    assert!(
        matches!(result, Err(StateStoreError::TimedOut { .. })),
        "exclusive maintenance prevents unprotected initialization"
    );
    drop(file);
    let store = fixture.connect().await;
    MaintenanceCase::write(&store, 1).await;
}

#[tokio::test]
async fn maintenance_retained_transaction_excludes_reclamation_across_processes() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    MaintenanceCase::write(&store, 1).await;
    let mut child = MaintenanceProcess::spawn(fixture.directory.path());
    child
        .wait_for(&fixture.directory.path().join("reader-ready"))
        .await;
    for sequence in 2..=70 {
        MaintenanceCase::write(&store, sequence).await;
    }
    assert!(
        fixture.versions().await >= 64,
        "independent process protects its snapshot"
    );
    fs::write(fixture.directory.path().join("reader-release"), b"release").expect("release child");
    child.finish().await;
    fixture.settle_maintenance(&store).await;
}

#[tokio::test]
async fn maintenance_process_entry() {
    let Some(directory) = std::env::var_os("AI_KIT_LANCE_MAINTENANCE_CHILD") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::directory(
        &directory,
    )))
    .await
    .expect("child supplied store");
    let mut transaction = store.begin_transaction().await.expect("child snapshot");
    fs::write(directory.join("reader-ready"), b"ready").expect("child ready marker");
    let deadline = Instant::now() + Duration::from_secs(120);
    while !directory.join("reader-release").exists() {
        assert!(Instant::now() < deadline, "bounded child release wait");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        transaction
            .load(MaintenanceCase::key("head"))
            .await
            .expect("child old read")
            .expect("child head")
            .revision
            .get(),
        1
    );
    transaction
        .rollback()
        .await
        .expect("child release snapshot");
    drop(store);
}

#[tokio::test]
async fn maintenance_preserves_peer_staging_until_its_snapshot_owner_settles() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    let retained = store.begin_transaction().await.expect("peer protection");
    let data = fixture.directory.path().join("__ai_kit_state.lance/data");
    let staging = data.join(format!(".state-stage-{}", uuid::Uuid::new_v4()));
    let unrelated = data.join(".state-stage-operator-note");
    fs::write(&staging, b"peer-owned staging fixture").expect("recognized staging fixture");
    fs::write(&unrelated, b"unrelated note").expect("unknown staging-like name");
    let peer = fixture.connect().await;
    for _ in 0..35 {
        peer.load(MaintenanceCase::key("head"))
            .await
            .expect("ordinary peer read");
    }
    assert!(
        staging.exists(),
        "a live peer's protection excludes orphan scavenging"
    );
    retained.rollback().await.expect("settle peer snapshot");
    let reopened = fixture.connect().await;
    assert!(!staging.exists());
    assert_eq!(
        fs::read(unrelated).expect("unknown file remains"),
        b"unrelated note"
    );
    drop(reopened);
}

#[tokio::test]
async fn maintenance_retains_an_undelivered_begin_reply_until_observer_drop() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    let oldest = store
        .begin_transaction()
        .await
        .expect("defer rewrite during seeding");
    for sequence in 1..=63 {
        MaintenanceCase::write(&store, sequence).await;
    }
    let mut pending = None;
    for _ in 0..8 {
        let mut observer = Box::pin(store.begin_transaction());
        let polled = futures::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(observer.as_mut(), context))
        })
        .await;
        match polled {
            std::task::Poll::Pending => {
                pending = Some(observer);
                break;
            }
            std::task::Poll::Ready(Ok(transaction)) => transaction
                .rollback()
                .await
                .expect("immediate begin is valid but not a witness"),
            std::task::Poll::Ready(Err(error)) => panic!("begin admission failed: {error}"),
        }
    }
    let observer = pending.expect("bounded attempts require a real Pending admission witness");
    // This subsequent command completes only after Begin has placed its
    // snapshot into the still-unobserved reply channel.
    store
        .load(MaintenanceCase::key("head"))
        .await
        .expect("begin command has settled");
    oldest.rollback().await.expect("release other protection");
    for _ in 0..35 {
        store
            .load(MaintenanceCase::key("head"))
            .await
            .expect("reads with retained reply");
    }
    assert!(
        fixture.versions().await >= 64,
        "undelivered snapshot retains native history"
    );
    drop(observer);
    fixture.settle_maintenance(&store).await;
}

#[tokio::test]
async fn maintenance_failure_is_typed_and_retries_after_thirty_two_further_jobs() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    let oldest = store
        .begin_transaction()
        .await
        .expect("retain seed versions");
    for sequence in 1..=63 {
        MaintenanceCase::write(&store, sequence).await;
    }
    oldest.rollback().await.expect("release seed protection");
    let hint = fixture
        .directory
        .path()
        .join("__ai_kit_state.lance/_versions/latest_version_hint.json");
    match fs::remove_file(&hint) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("prepare owned hint obstruction: {error}"),
    }
    fs::create_dir(&hint).expect("ordinary directory obstruction");
    let failed = store.load(MaintenanceCase::key("head")).await;
    fs::remove_dir(&hint).expect("release owned obstruction");
    assert!(
        matches!(failed, Err(StateStoreError::BackendOperation { ref operation, .. }) if operation == "maintenance")
    );
    for _ in 0..32 {
        let head = store
            .load(MaintenanceCase::key("head"))
            .await
            .expect("settled error permits domain progress")
            .expect("head");
        assert_eq!(head.revision.get(), 63);
        assert!(
            fixture.versions().await >= 64,
            "retry waits for all 32 further completed jobs"
        );
    }
    store
        .load(MaintenanceCase::key("head"))
        .await
        .expect("eligible retry succeeds");
    assert_eq!(fixture.versions().await, 1);
}

#[tokio::test]
async fn maintenance_reclaims_orphans_after_sixty_four_failed_native_preparations() {
    let fixture = MaintenanceCase::new();
    let store = fixture.connect().await;
    let mut transactions = Vec::new();
    for sequence in 0..64 {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("prepare original snapshot");
        transaction
            .put(StateRecord::new(
                MaintenanceCase::key(&format!("failed/{sequence}")),
                json!(sequence),
            ))
            .await
            .expect("stage unique failed receipt");
        transactions.push(transaction);
    }
    let data = fixture.directory.path().join("__ai_kit_state.lance/data");
    let saved = fixture.directory.path().join("saved-private-data");
    fs::rename(&data, &saved).expect("retain native data");
    fs::write(&data, b"ordinary owned obstruction").expect("block preparation");
    for transaction in transactions {
        assert!(matches!(
            transaction.commit().await,
            Err(StateStoreError::BackendOperation { .. })
        ));
    }
    fs::remove_file(&data).expect("remove obstruction");
    fs::rename(&saved, &data).expect("restore native data");
    let staging = data.join(format!(".state-stage-{}", uuid::Uuid::new_v4()));
    fs::write(&staging, b"settled orphan fixture").expect("recognized orphan");
    store
        .load(MaintenanceCase::key("head"))
        .await
        .expect("orphan maintenance trigger");
    assert!(
        !staging.exists(),
        "failed native attempts schedule orphan cleanup"
    );
    assert_eq!(
        fixture.versions().await,
        1,
        "failed preparations publish no versions"
    );
    let mut verifier = store
        .begin_transaction()
        .await
        .expect("atomic failed-receipt oracle");
    for sequence in 0..64 {
        assert!(
            verifier
                .load(MaintenanceCase::key(&format!("failed/{sequence}")))
                .await
                .expect("receipt read")
                .is_none()
        );
    }
    verifier.rollback().await.expect("settle oracle");
    MaintenanceCase::write(&store, 1).await;
}
