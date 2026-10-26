//! Native feasibility probes, not a receipt for the supplied StateStore.
//! Failure injection uses Lance's public native object-store hook around real
//! local I/O, using the native custom-handler policy that preserves a barrier
//! failure even when its manifest landed. The separate built-in tests must
//! prove the shipped adapter; these probes do not describe every native default.

mod lancedb_durability;

use std::{sync::Arc, time::Duration};

use lance::dataset::WriteMode;
use lancedb_durability::{FileRole, NativeFixture};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn object_store_scheme_intercepts_real_data_and_manifest_writes() {
    let fixture = NativeFixture::new();
    let dataset = fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("write through actual durability barriers");
    assert_eq!(dataset.count_rows(None).await.expect("count"), 1);
    assert!(fixture.probe().synchronized(FileRole::Data) > 0);
    assert!(fixture.probe().synchronized(FileRole::Manifest) > 0);
}

#[tokio::test]
async fn ordinary_file_scheme_bypasses_data_write_wrapper() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, false)
        .await
        .expect("ordinary native write");
    assert_eq!(fixture.probe().synchronized(FileRole::Data), 0);
    assert!(fixture.probe().synchronized(FileRole::Manifest) > 0);
}

#[tokio::test]
async fn failed_data_barrier_prevents_native_manifest_publication() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    fixture.probe().fail_next(FileRole::Data);
    let result = fixture.write(2, WriteMode::Append, true).await;
    assert!(
        result.is_err(),
        "native write must propagate failed data sync"
    );
    assert_eq!(fixture.probe().failures(FileRole::Data), 1);
    let observed = fixture.reopen().await;
    assert_eq!(observed.version().version, 1);
    assert_eq!(observed.count_rows(None).await.expect("count"), 1);
}

#[tokio::test]
async fn failed_manifest_barrier_keeps_publication_outcome_uncertain() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    fixture.probe().fail_next(FileRole::Manifest);
    let result = fixture.write(2, WriteMode::Append, true).await;
    assert!(
        result.is_err(),
        "manifest sync failure must not become success"
    );
    assert_eq!(fixture.probe().failures(FileRole::Manifest), 1);
    let observed = fixture.reopen().await;
    assert_eq!(observed.version().version, 2);
    assert_eq!(observed.count_rows(None).await.expect("count"), 2);
}

#[tokio::test]
async fn failed_manifest_barrier_preserves_own_uuid_without_implying_success() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    let (snapshot, transaction) = fixture.prepare_overwrite(2).await;
    let target = snapshot.version_id() + 1;
    let authored_uuid = transaction.uuid.clone();
    fixture.probe().fail_next(FileRole::Manifest);
    let result = NativeFixture::commit(snapshot, transaction).await;
    assert!(
        result.is_err(),
        "own publication must not erase the barrier error"
    );
    assert_eq!(fixture.probe().failures(FileRole::Manifest), 1);

    let observed = fixture
        .open_version(target)
        .await
        .expect("fresh exact version");
    assert_eq!(
        observed
            .read_transaction()
            .await
            .expect("read published transaction")
            .expect("manifest transaction")
            .uuid,
        authored_uuid
    );
    assert_eq!(
        observed.count_rows(None).await.expect("complete overwrite"),
        1
    );
    let progressed = fixture
        .write(3, WriteMode::Append, true)
        .await
        .expect("subsequent native write remains possible");
    assert_eq!(progressed.version_id(), target + 1);
    assert_eq!(progressed.count_rows(None).await.expect("count"), 2);
}

#[tokio::test]
async fn stale_native_writer_readback_identifies_the_competing_transaction() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    let (winner_snapshot, winner) = fixture.prepare_overwrite(2).await;
    let (loser_snapshot, loser) = fixture.prepare_overwrite(3).await;
    let target = winner_snapshot.version_id() + 1;
    let winner_uuid = winner.uuid.clone();
    let loser_uuid = loser.uuid.clone();
    NativeFixture::commit(winner_snapshot, winner)
        .await
        .expect("winner publishes");
    let error = NativeFixture::commit(loser_snapshot, loser)
        .await
        .expect_err("stale original-snapshot overwrite cannot publish");
    assert!(matches!(error, lance::Error::CommitConflict { .. }));

    let observed = fixture
        .open_version(target)
        .await
        .expect("fresh exact version");
    let canonical = observed
        .read_transaction()
        .await
        .expect("read exact published transaction")
        .expect("manifest transaction");
    assert_eq!(canonical.uuid, winner_uuid);
    assert_ne!(canonical.uuid, loser_uuid);
    assert_eq!(fixture.reopen().await.version_id(), target);
}

#[tokio::test]
async fn repeated_transaction_uuid_never_overwrites_an_existing_preparation_file() {
    let fixture = NativeFixture::new();
    fixture.probe().create_only_preparations();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    let (winner_snapshot, winner) = fixture.prepare_overwrite(2).await;
    let (loser_snapshot, mut loser) = fixture.prepare_overwrite(3).await;
    assert_eq!(winner.read_version, loser.read_version);
    let target = winner_snapshot.version_id() + 1;
    let winner_uuid = winner.uuid.clone();
    let filename = format!("{}-{}.txn", winner.read_version, winner_uuid);
    // This is a real native transaction with a deliberately repeated public
    // UUID, not interception of the supplied SDK's private random identity.
    loser.uuid = winner_uuid.clone();
    NativeFixture::commit(winner_snapshot, winner)
        .await
        .expect("winner publishes its transaction file and manifest");
    let path = fixture
        .local_files()
        .into_iter()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name == filename.as_str())
        })
        .expect("the exact winning transaction file exists");
    let original = std::fs::read(&path).expect("retain the winning native transaction bytes");
    assert!(!original.is_empty());

    let result = NativeFixture::commit(loser_snapshot, loser).await;
    assert!(
        result.is_err(),
        "a colliding native preparation cannot succeed"
    );
    let retained = std::fs::read(&path);
    assert!(
        retained.is_ok(),
        "a colliding preparation must not remove the winner's transaction file: {retained:?}"
    );
    assert_eq!(
        retained.expect("checked retained file"),
        original,
        "a colliding preparation must not replace the winner's transaction bytes"
    );

    let observed = fixture
        .open_version(target)
        .await
        .expect("fresh winner readback");
    assert_eq!(
        observed
            .read_transaction()
            .await
            .expect("fresh winning transaction")
            .expect("winning transaction remains referenced")
            .uuid,
        winner_uuid
    );
    let rows = observed
        .scan()
        .try_into_batch()
        .await
        .expect("winning rows");
    assert_eq!(rows.num_rows(), 1);
    assert_eq!(
        rows.column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("native value column")
            .value(0),
        2
    );
    let progressed = fixture
        .write(4, WriteMode::Append, true)
        .await
        .expect("a new native transaction still progresses after the collision");
    assert_eq!(progressed.version_id(), target + 1);
    assert_eq!(progressed.count_rows(None).await.expect("row count"), 2);
}

#[tokio::test]
async fn create_only_native_preparation_puts_preserve_existing_bytes() {
    let fixture = NativeFixture::new();
    fixture.probe().create_only_preparations();
    let (store, base) = fixture.object_store().await;
    // These are native local puts through the supported hook. They establish
    // create-only feasibility, not supplied-adapter classifier coverage.
    for (directory, name) in [
        ("data", "prepared.lance"),
        ("_transactions", "1-repeated.txn"),
        ("_deletions", "1-1-1.arrow"),
        ("_deletions", "1-1-2.bin"),
    ] {
        let path = base.clone().join(directory).join(name);
        store
            .inner
            .put_opts(&path, vec![1, 2, 3].into(), Default::default())
            .await
            .expect("real winning preparation");
        let error = store
            .inner
            .put_opts(&path, vec![4, 5, 6].into(), Default::default())
            .await
            .expect_err("create-only native put must reject an existing destination");
        assert!(matches!(error, object_store::Error::AlreadyExists { .. }));
        assert_eq!(
            store
                .inner
                .get_opts(&path, Default::default())
                .await
                .expect("fresh original preparation")
                .bytes()
                .await
                .expect("original preparation bytes")
                .as_ref(),
            &[1, 2, 3]
        );
    }
    assert_eq!(fixture.probe().synchronized(FileRole::Data), 1);
    assert_eq!(fixture.probe().synchronized(FileRole::Transaction), 1);
    assert_eq!(fixture.probe().synchronized(FileRole::Deletion), 2);

    let contested = base.clone().join("data").join("contested.lance");
    let (first, second) = tokio::join!(
        store
            .inner
            .put_opts(&contested, vec![7].into(), Default::default()),
        store
            .inner
            .put_opts(&contested, vec![8].into(), Default::default())
    );
    let winner = match (first, second) {
        (Ok(_), Err(object_store::Error::AlreadyExists { .. })) => 7,
        (Err(object_store::Error::AlreadyExists { .. }), Ok(_)) => 8,
        results => panic!("competing native creates require one exact winner: {results:?}"),
    };
    assert_eq!(
        store
            .inner
            .get_opts(&contested, Default::default())
            .await
            .expect("fresh competing-create winner")
            .bytes()
            .await
            .expect("winning competing-create bytes")
            .as_ref(),
        &[winner]
    );

    let hint = base.join("_versions").join("latest_version_hint.json");
    for value in [1, 2] {
        store
            .inner
            .put_opts(&hint, vec![value].into(), Default::default())
            .await
            .expect("mutable native hint retains overwrite behavior");
    }
    assert_eq!(
        store
            .inner
            .get_opts(&hint, Default::default())
            .await
            .expect("fresh mutable hint")
            .bytes()
            .await
            .expect("latest hint bytes")
            .as_ref(),
        &[2]
    );
}

#[tokio::test]
async fn conditional_manifest_collision_preserves_a_separate_native_hook_failure() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    let (winner_snapshot, winner) = fixture.prepare_overwrite(2).await;
    let (loser_snapshot, loser) = fixture.prepare_overwrite(3).await;
    let target = winner_snapshot.version_id() + 1;
    let winner_uuid = winner.uuid.clone();
    let loser_uuid = loser.uuid.clone();
    NativeFixture::commit(winner_snapshot, winner)
        .await
        .expect("winner publishes the actual conditional-create target");

    fixture.probe().fail_barrier_after_manifest_collision();
    let error = NativeFixture::commit(loser_snapshot, loser)
        .await
        .expect_err("genuine conditional-create collision remains an error");
    assert!(matches!(error, lance::Error::CommitConflict { .. }));
    fixture.probe().assert_collision_failure_retained();
    assert_eq!(fixture.probe().failures(FileRole::Manifest), 1);

    let observed = fixture
        .open_version(target)
        .await
        .expect("fresh winner readback");
    let canonical = observed
        .read_transaction()
        .await
        .expect("read exact winning transaction")
        .expect("manifest transaction");
    assert_eq!(canonical.uuid, winner_uuid);
    assert_ne!(canonical.uuid, loser_uuid);
    assert_eq!(fixture.reopen().await.version_id(), target);
    // This tests native error propagation and an independent hook receipt.
    // It does not exercise the private SDK staging-unlink/final-file-barrier
    // path or provide a test-local portable error mapper.
    let rows = observed
        .scan()
        .try_into_batch()
        .await
        .expect("read actual winning rows");
    assert_eq!(rows.num_rows(), 1);
    assert_eq!(
        rows.column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("native value column")
            .value(0),
        2
    );
}

#[tokio::test]
async fn unavailable_exact_version_readback_does_not_imply_nonpublication() {
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    let (snapshot, transaction) = fixture.prepare_overwrite(2).await;
    let target = snapshot.version_id() + 1;
    let authored_uuid = transaction.uuid.clone();
    fixture.probe().fail_next(FileRole::Manifest);
    assert!(NativeFixture::commit(snapshot, transaction).await.is_err());
    assert_eq!(fixture.probe().failures(FileRole::Manifest), 1);

    // This is the supported application-level native ObjectStore hook. No
    // debugger, syscall interception, privilege, or SDK production hook is used.
    fixture.probe().fail_next_read(FileRole::Manifest);
    assert!(fixture.open_version(target).await.is_err());
    assert_eq!(fixture.probe().failed_reads(FileRole::Manifest), 1);

    let observed = fixture
        .open_version(target)
        .await
        .expect("independent readback");
    assert_eq!(
        observed
            .read_transaction()
            .await
            .expect("read transaction after transient hook error")
            .expect("published transaction")
            .uuid,
        authored_uuid
    );
}

#[tokio::test]
async fn native_commit_ignores_failed_version_hint_barrier() {
    assert!(
        std::env::var_os("LANCE_USE_VERSION_HINT").is_none(),
        "unset LANCE_USE_VERSION_HINT for the native version-hint feasibility probe"
    );
    let fixture = NativeFixture::new();
    fixture
        .write(1, WriteMode::Create, true)
        .await
        .expect("initial dataset");
    assert!(fixture.probe().synchronized(FileRole::VersionHint) > 0);
    fixture.probe().fail_next(FileRole::VersionHint);
    fixture
        .write(2, WriteMode::Append, true)
        .await
        .expect("native success does not account for best-effort hint failure");
    assert_eq!(fixture.probe().failures(FileRole::VersionHint), 1);
    let observed = fixture.reopen().await;
    assert_eq!(observed.version().version, 2);
    assert_eq!(observed.count_rows(None).await.expect("count"), 2);
}

#[tokio::test]
async fn multipart_completion_reaches_real_file_and_directory_barriers() {
    let fixture = NativeFixture::new();
    let (store, base) = fixture.object_store().await;
    let path = base.join("multipart.lance");
    let mut writer = store.create(&path).await.expect("native writer");
    writer
        .write_all(&fixture.multipart_payload())
        .await
        .expect("multipart native write");
    AsyncWriteExt::shutdown(&mut writer)
        .await
        .expect("native multipart completion");
    drop(writer);
    fixture.probe().assert_multipart_completed();
    assert_eq!(fixture.probe().synchronized(FileRole::Data), 1);
    let files = fixture.local_files();
    assert_eq!(files.len(), 1);
    assert_eq!(
        std::fs::read(&files[0]).expect("native completed file"),
        fixture.multipart_payload()
    );
}

#[tokio::test]
async fn native_writer_ignores_abort_error_but_wrapper_can_retain_it() {
    let fixture = NativeFixture::new();
    fixture.probe().hold_aborts();
    let (store, base) = fixture.object_store().await;
    let path = base.join("failed-abort.lance");
    let mut writer = store.create(&path).await.expect("native writer");
    writer
        .write_all(&fixture.multipart_payload())
        .await
        .expect("start real multipart write");
    drop(writer);
    fixture.probe().wait_for_abort().await;
    let staged_files = fixture.local_files();
    assert_eq!(staged_files.len(), 1, "one actual multipart staging file");
    std::fs::remove_file(&staged_files[0]).expect("remove owned staged file");
    fixture.probe().release_abort();
    fixture.probe().wait_for_cleanup().await;
    fixture.probe().assert_abort_failed();
    assert!(fixture.local_files().is_empty());
}

#[tokio::test]
async fn failed_native_multipart_completion_leaves_staging_without_abort() {
    let fixture = NativeFixture::new();
    let (store, base) = fixture.object_store().await;
    let path = base.join("failed-completion.lance");
    let mut writer = store.create(&path).await.expect("native writer");
    writer
        .write_all(&fixture.multipart_payload())
        .await
        .expect("start real multipart write");
    fixture.occupy_destination(&path);
    assert!(AsyncWriteExt::shutdown(&mut writer).await.is_err());
    drop(writer);
    fixture.probe().wait_for_cleanup().await;
    fixture.probe().assert_unfinished_upload();
    assert_eq!(fixture.local_files().len(), 1, "staging file still exists");
}

#[test]
fn isolated_runtime_drop_alone_does_not_complete_pending_native_abort() {
    let fixture = NativeFixture::new();
    fixture.probe().hold_aborts();
    let probe = Arc::clone(fixture.probe());
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("isolated native runtime");
        runtime.block_on(async {
            let (store, base) = fixture.object_store().await;
            let path = base.join("abandoned.lance");
            let mut writer = store.create(&path).await.expect("native writer");
            writer
                .write_all(&fixture.multipart_payload())
                .await
                .expect("start real multipart write");
            drop(writer);
            fixture.probe().wait_for_abort().await;
        });
        // The upstream abort has started and is held by a deterministic
        // test-local barrier. Runtime shutdown cancels it rather than waiting.
        drop(runtime);
    });
    worker.join().expect("native owner joined");
    probe.assert_shutdown_interrupted_abort();
}

#[test]
fn explicit_abort_drain_settles_native_cleanup_before_runtime_shutdown() {
    let fixture = NativeFixture::new();
    fixture.probe().hold_aborts();
    let probe = Arc::clone(fixture.probe());
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("isolated native runtime");
        runtime.block_on(async {
            let (store, base) = fixture.object_store().await;
            let path = base.join("abandoned.lance");
            let mut writer = store.create(&path).await.expect("native writer");
            writer
                .write_all(&fixture.multipart_payload())
                .await
                .expect("start real multipart write");
            drop(writer);
            fixture.probe().wait_for_abort().await;
            fixture.probe().release_abort();
            fixture.probe().wait_for_cleanup().await;
            assert!(fixture.local_files().is_empty());
        });
        drop(runtime);
    });
    worker.join().expect("native owner joined after cleanup");
    probe.assert_aborted();
}

#[tokio::test]
async fn native_commit_timeout_abandons_landed_work_unless_disabled() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let timed = NativeFixture::new();
        let admitted = NativeFixture::new();
        for fixture in [&timed, &admitted] {
            fixture.write(1, WriteMode::Create, true).await.expect("initial dataset");
        }
        let (timed_snapshot, timed_transaction) = timed.prepare_overwrite(2).await;
        let (admitted_snapshot, admitted_transaction) = admitted.prepare_overwrite(2).await;
        let timed_uuid = timed_transaction.uuid.clone();
        let admitted_uuid = admitted_transaction.uuid.clone();
        timed.probe().hold_commit();
        admitted.probe().hold_commit();
        // Poll the two owned futures directly. The gate is after actual native
        // conditional publication and durability; no task or synthetic write
        // completes either operation.
        let mut timed_commit = Box::pin(NativeFixture::commit_with_timeout(
            timed_snapshot, timed_transaction, Some(Duration::from_secs(1)),
        ));
        let mut admitted_commit = Box::pin(NativeFixture::commit_with_timeout(
            admitted_snapshot, admitted_transaction, None,
        ));
        tokio::select! {
            () = async {
                tokio::join!(timed.probe().wait_for_commit(), admitted.probe().wait_for_commit());
            } => {},
            result = &mut timed_commit => panic!("native timer ended before both publication milestones: {result:?}"),
            result = &mut admitted_commit => panic!("held admitted commit returned before release: {result:?}"),
        }
        let error = tokio::select! {
            result = &mut timed_commit => result.expect_err("native timeout drops the held inner future"),
            result = &mut admitted_commit => panic!("disabled timeout must retain admitted work until release: {result:?}"),
        };
        assert!(matches!(error, lance::Error::Timeout { .. }), "native timeout classification: {error:?}");
        for (fixture, expected_uuid) in [(&timed, timed_uuid), (&admitted, admitted_uuid)] {
            let landed = fixture.reopen().await;
            assert_eq!(landed.version_id(), 2);
            assert_eq!(landed.read_transaction().await.expect("read landed transaction").expect("transaction").uuid, expected_uuid);
            assert_eq!(landed.count_rows(None).await.expect("landed rows"), 1);
            assert_eq!(fixture.probe().completed_held_commits(), 0);
        }
        timed.probe().release_commit();
        admitted.probe().release_commit();
        let completed = admitted_commit.await.expect("explicit release completes admitted work");
        assert_eq!(completed.version_id(), 2);
        assert_eq!(admitted.probe().completed_held_commits(), 1);
        assert_eq!(timed.probe().completed_held_commits(), 0);
        drop(completed);
        drop(timed_commit);
    }).await.expect("bounded native timeout ownership control and cleanup");
}
