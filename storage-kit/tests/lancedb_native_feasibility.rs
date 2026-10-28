mod lancedb_format_lifecycle;
mod lancedb_native;

use std::sync::Arc;

use lance::Error;
use lancedb_native::{CommitPolicy, NativeTable};

#[test]
fn native_state_delta_phase_measurement() {
    for rows in [100, 1_000, 10_000] {
        NativeTable::new(CommitPolicy::ExactVersion).measure_state_phases(rows);
    }
}

#[tokio::test]
async fn selected_state_fragments_merge_with_an_external_sentinel_and_preserve_cold_data() {
    NativeTable::new(CommitPolicy::ExactVersion)
        .assert_selected_state_fragments(false)
        .await;
}

#[tokio::test]
async fn selected_state_fragments_merge_with_their_sentinel_and_preserve_cold_data() {
    NativeTable::new(CommitPolicy::ExactVersion)
        .assert_selected_state_fragments(true)
        .await;
}

#[tokio::test]
async fn lazy_native_input_failure_preserves_typed_receipt_and_never_publishes() {
    NativeTable::new(CommitPolicy::ExactVersion)
        .assert_lazy_input_error()
        .await;
}

#[tokio::test]
async fn full_row_delta_replaces_active_fragments_and_survives_version_cleanup() {
    let table = NativeTable::new(CommitPolicy::ExactVersion);
    let original = table.create().await;
    let delta = NativeTable::prepare_delta(
        Arc::clone(&original),
        &[("__sentinel", 1), ("a", 1), ("b", 2), ("tombstone", 9)],
    )
    .await;
    NativeTable::commit_delta(original, delta)
        .await
        .expect("seed rows");
    let current = table.open().await;
    let partial =
        NativeTable::prepare_delta(Arc::clone(&current), &[("__sentinel", 2), ("a", 3)]).await;
    NativeTable::commit_delta(current, partial)
        .await
        .expect("introduce physical deletions");
    let current = table.open().await;
    let fragments = current
        .manifest()
        .fragments
        .iter()
        .map(|fragment| fragment.id)
        .collect::<Vec<_>>();
    let rows = NativeTable::rows(&current).await;
    let input = rows
        .iter()
        .map(|(key, value)| (key.as_str(), *value))
        .collect::<Vec<_>>();
    let rewrite = NativeTable::prepare_delta(Arc::clone(&current), &input).await;
    NativeTable::commit_delta(current, rewrite)
        .await
        .expect("full-row guarded delta");
    let rewritten = table.open().await;
    assert!(
        rewritten
            .manifest()
            .fragments
            .iter()
            .all(|fragment| !fragments.contains(&fragment.id)),
        "all old active fragments are detached"
    );
    assert_eq!(NativeTable::rows(&rewritten).await, rows);
    rewritten
        .cleanup_with_policy(lance::dataset::cleanup::CleanupPolicy {
            before_version: Some(rewritten.version_id()),
            delete_unverified: true,
            ..Default::default()
        })
        .await
        .expect("quiescent native cleanup");
    let reopened = table.open().await;
    assert_eq!(
        reopened.versions().await.expect("retained versions").len(),
        1
    );
    assert_eq!(NativeTable::rows(&reopened).await, rows);
}

#[tokio::test]
async fn strict_overwrite_uses_the_original_snapshot_and_never_rebases() {
    let table = NativeTable::new(CommitPolicy::Native);
    let original = table.create().await;
    let version = original.version_id();
    let winner =
        NativeTable::prepare_overwrite(Arc::clone(&original), &[("__sentinel", 1), ("winner", 1)])
            .await;
    let loser =
        NativeTable::prepare_overwrite(Arc::clone(&original), &[("__sentinel", 2), ("loser", 1)])
            .await;
    assert_eq!(winner.read_version, version);
    assert_eq!(loser.read_version, version);

    let committed = NativeTable::commit(Arc::clone(&original), winner)
        .await
        .expect("first overwrite commits");
    assert_eq!(committed.version_id(), version + 1);
    let error = NativeTable::commit(Arc::clone(&original), loser)
        .await
        .expect_err("stale strict overwrite must conflict");
    assert!(matches!(error, Error::CommitConflict { .. }));

    let latest = table.open().await;
    assert_eq!(latest.version_id(), version + 1);
    let rows = NativeTable::rows(&latest).await;
    assert_eq!(rows.get("winner"), Some(&1));
    assert!(!rows.contains_key("loser"));
    let old_rows = NativeTable::rows(&original).await;
    assert_eq!(old_rows.len(), 1);
    assert_eq!(old_rows.get("__sentinel"), Some(&0));
}

#[tokio::test]
async fn unguarded_delta_allows_write_skew_across_absent_reads() {
    let table = NativeTable::new(CommitPolicy::Native);
    let original = table.create().await;
    let before = NativeTable::rows(&original).await;
    assert!(!before.contains_key("claim-a"));
    assert!(!before.contains_key("claim-b"));

    // Both authors only claim their key if neither claim exists. Lance does
    // not know these application reads when rebasing disjoint native writes.
    let first = NativeTable::prepare_delta(Arc::clone(&original), &[("claim-a", 1)]).await;
    let second = NativeTable::prepare_delta(Arc::clone(&original), &[("claim-b", 1)]).await;
    assert_eq!(
        first.transaction.read_version,
        second.transaction.read_version
    );
    NativeTable::commit_delta(Arc::clone(&original), first)
        .await
        .expect("first unguarded claim");
    NativeTable::commit_delta(Arc::clone(&original), second)
        .await
        .expect("native disjoint merge rebases despite stale absence reads");

    let latest = table.open().await;
    assert_eq!(latest.version_id(), original.version_id() + 2);
    let rows = NativeTable::rows(&latest).await;
    assert_eq!(rows.get("claim-a"), Some(&1));
    assert_eq!(rows.get("claim-b"), Some(&1));
    assert_eq!(NativeTable::rows(&original).await, before);
}

#[tokio::test]
async fn sentinel_delta_publishes_one_winner_and_preserves_transaction_uuid() {
    let table = NativeTable::new(CommitPolicy::ExactVersion);
    let original = table.create().await;
    let target = original.version_id() + 1;
    let winner =
        NativeTable::prepare_delta(Arc::clone(&original), &[("__sentinel", 1), ("claim-a", 1)])
            .await;
    let winner_uuid = winner.transaction.uuid.clone();
    let loser =
        NativeTable::prepare_delta(Arc::clone(&original), &[("__sentinel", 2), ("claim-b", 1)])
            .await;
    let loser_uuid = loser.transaction.uuid.clone();
    assert_ne!(winner_uuid, loser_uuid);
    NativeTable::commit_delta(Arc::clone(&original), winner)
        .await
        .expect("first guarded delta");
    let error = NativeTable::commit_delta(Arc::clone(&original), loser)
        .await
        .expect_err("second sentinel writer cannot commit");
    assert!(matches!(
        error,
        Error::RetryableCommitConflict { .. }
            | Error::CommitConflict { .. }
            | Error::IncompatibleTransaction { .. }
    ));

    let latest = table.open().await;
    assert_eq!(latest.version_id(), target);
    let canonical = latest
        .read_transaction_by_version(target)
        .await
        .expect("read canonical transaction without writer caches")
        .expect("inline native transaction");
    assert_eq!(canonical.uuid, winner_uuid);
    assert_ne!(canonical.uuid, loser_uuid);
    let rows = NativeTable::rows(&latest).await;
    assert_eq!(rows.get("__sentinel"), Some(&1));
    assert_eq!(rows.get("claim-a"), Some(&1));
    assert!(!rows.contains_key("claim-b"));
}

#[tokio::test]
async fn exact_version_guard_rejects_rebase_after_an_append_that_keeps_the_sentinel() {
    let table = NativeTable::new(CommitPolicy::ExactVersion);
    let original = table.create().await;
    let delta =
        NativeTable::prepare_delta(Arc::clone(&original), &[("__sentinel", 1), ("authored", 1)])
            .await;
    let external_append =
        NativeTable::prepare_append(Arc::clone(&original), &[("external", 1)]).await;
    NativeTable::commit(Arc::clone(&original), external_append)
        .await
        .expect("intervening native append");

    let error = NativeTable::commit_delta(Arc::clone(&original), delta)
        .await
        .expect_err("exact version guard must reject the rebased target");
    assert!(
        matches!(error, Error::RetryableCommitConflict { .. }),
        "exact-version rejection must retain its original classification: {error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("state transaction expected version")
    );
    let latest = table.open().await;
    assert_eq!(latest.version_id(), original.version_id() + 1);
    let rows = NativeTable::rows(&latest).await;
    assert_eq!(rows.get("__sentinel"), Some(&0));
    assert_eq!(rows.get("external"), Some(&1));
    assert!(!rows.contains_key("authored"));
}

#[tokio::test]
async fn exact_version_guard_fails_closed_without_the_original_target() {
    let table = NativeTable::new(CommitPolicy::ExactVersion);
    let original = table.create().await;
    let mut delta =
        NativeTable::prepare_delta(Arc::clone(&original), &[("__sentinel", 1), ("authored", 1)])
            .await;
    delta.transaction.transaction_properties = None;
    let error = NativeTable::commit_delta(Arc::clone(&original), delta)
        .await
        .expect_err("missing original target must prevent publication");
    assert!(
        matches!(error, Error::InvalidInput { .. }),
        "missing-target rejection must retain its original classification: {error:?}"
    );
    let latest = table.open().await;
    assert_eq!(latest.version_id(), original.version_id());
    assert_eq!(
        NativeTable::rows(&latest).await,
        NativeTable::rows(&original).await
    );
}

#[tokio::test]
async fn independently_prepared_initializers_cannot_overwrite_the_winner() {
    let table = NativeTable::new(CommitPolicy::ExactVersion);
    let first = table.prepare_create().await;
    let second = table.prepare_create().await;
    assert_eq!(first.read_version, 0);
    assert_eq!(second.read_version, 0);
    let first_uuid = first.uuid.clone();
    let second_uuid = second.uuid.clone();
    table.commit_create(first).await.expect("first initializer");
    table
        .commit_create(second)
        .await
        .expect_err("stale initializer must not overwrite an existing table");

    let latest = table.open().await;
    assert_eq!(latest.version_id(), 1);
    let transaction = latest
        .read_transaction_by_version(1)
        .await
        .expect("read winning initialization")
        .expect("initialization transaction");
    assert_eq!(transaction.uuid, first_uuid);
    assert_ne!(transaction.uuid, second_uuid);
    let rows = NativeTable::rows(&latest).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.get("__sentinel"), Some(&0));
}
