use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{
    BooleanArray, LargeBinaryArray, RecordBatch, StringArray, UInt8Array, UInt64Array,
};
use arrow_schema::Schema;
use lance::{
    Dataset,
    dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams},
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateRecord},
    },
};

use crate::lancedb_state_reference::SharedStateCases;

struct TransactionFixture {
    directory: tempfile::TempDir,
}

impl TransactionFixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("owned transaction fixture"),
        }
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("independent transaction runtime")
    }

    async fn connect(root: &Path) -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::directory(
            root.join("database"),
        )))
        .await
        .expect("supplied Lance StateStore")
    }

    fn key(namespace: &str, key: &str) -> StateKey {
        StateKey::new(namespace, key).expect("valid transaction key")
    }

    fn committed(result: Result<(), StateStoreError>) -> bool {
        match result {
            Ok(()) => true,
            Err(StateStoreError::TransactionConflict { .. }) => false,
            Err(error) => panic!("unexpected transaction result: {error:?}"),
        }
    }

    async fn revision_boundary(&self, present: bool) {
        let uri = self.root().join("database/__ai_kit_state.lance");
        let dataset = Arc::new(
            Dataset::open(uri.to_str().expect("UTF-8 fixture path"))
                .await
                .expect("native fixture table"),
        );
        let token = uuid::Uuid::new_v4().to_string();
        let target = dataset.version_id() + 1;
        let payload = present.then_some(br#""original""#.as_slice());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::from(dataset.schema())),
            vec![
                Arc::new(UInt8Array::from(vec![1, 0])),
                Arc::new(StringArray::from(vec!["", "boundary"])),
                Arc::new(StringArray::from(vec!["", "record"])),
                Arc::new(UInt64Array::from(vec![0, u64::MAX - 1])),
                Arc::new(BooleanArray::from(vec![false, present])),
                Arc::new(LargeBinaryArray::from_iter([None, payload])),
                Arc::new(StringArray::from(vec![token.as_str(), ""])),
            ],
        )
        .expect("boundary fixture batch");
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
            .expect("prepare boundary fixture");
        transaction.uuid = token;
        transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            "ai-kit.state.expected-version".to_owned(),
            target.to_string(),
        )])));
        CommitBuilder::new(dataset)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .expect("publish boundary fixture");
    }
}

#[tokio::test]
async fn memory_clones_pass_shared_serializable_transaction_cases() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("one memory owner");
    SharedStateCases::new(store.clone(), store.clone(), store)
        .verify()
        .await;
}

#[tokio::test]
async fn staged_max_revision_then_overflow_discards_the_complete_overlay() {
    for present in [false, true] {
        for delete in [false, true] {
            let fixture = TransactionFixture::new();
            let store = TransactionFixture::connect(fixture.root()).await;
            fixture.revision_boundary(present).await;
            let key = TransactionFixture::key("boundary", "record");
            let receipt = TransactionFixture::key("boundary", "unpublished-receipt");
            let mut transaction = store.begin_transaction().await.expect("boundary snapshot");
            let original = transaction.load(key.clone()).await.expect("original value");
            assert_eq!(original.is_some(), present);
            if let Some(record) = original {
                assert_eq!(record.revision.get(), u64::MAX - 1);
            }
            transaction
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!("staged-max"),
                ))
                .await
                .expect("last representable staged revision");
            assert_eq!(
                transaction
                    .load(key.clone())
                    .await
                    .expect("staged read")
                    .expect("staged record")
                    .revision
                    .get(),
                u64::MAX
            );
            transaction
                .put(StateRecord::new(receipt.clone(), serde_json::json!(true)))
                .await
                .expect("stage companion receipt");
            let overflow = if delete {
                transaction.delete(key.clone()).await
            } else {
                transaction
                    .put(StateRecord::new(key.clone(), serde_json::json!("overflow")))
                    .await
            };
            assert!(matches!(overflow, Err(StateStoreError::InvalidRequest(_))));
            assert!(matches!(
                transaction.commit().await,
                Err(StateStoreError::InvalidRequest(_))
            ));
            let mut verifier = store.begin_transaction().await.expect("fresh verifier");
            assert!(verifier.load(receipt).await.expect("receipt").is_none());
            let original = verifier.load(key.clone()).await.expect("retained value");
            assert_eq!(original.is_some(), present);
            if let Some(record) = original {
                assert_eq!(record.revision.get(), u64::MAX - 1);
                assert_eq!(record.record.value.as_ref(), &serde_json::json!("original"));
            }
            verifier
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!("committed-max"),
                ))
                .await
                .expect("failed transaction consumed no revision");
            verifier.commit().await.expect("commit maximum revision");
            let reopened = TransactionFixture::connect(fixture.root()).await;
            let committed = reopened.load(key).await.expect("reopen").expect("record");
            assert_eq!(committed.revision.get(), u64::MAX);
            assert_eq!(
                committed.record.value.as_ref(),
                &serde_json::json!("committed-max")
            );
        }
    }
}

#[tokio::test]
async fn three_way_dependency_cycle_keeps_mixed_write_sets_atomic() {
    let fixture = TransactionFixture::new();
    let first = TransactionFixture::connect(fixture.root()).await;
    let second = TransactionFixture::connect(fixture.root()).await;
    let third = TransactionFixture::connect(fixture.root()).await;
    let decisions = ["cycle-a", "cycle-b", "cycle-c"]
        .map(|namespace| TransactionFixture::key(namespace, "decision"));
    let victims = ["cycle-a", "cycle-b", "cycle-c"]
        .map(|namespace| TransactionFixture::key(namespace, "delete-me"));
    let receipts = ["a", "b", "c"].map(|key| TransactionFixture::key("cycle-receipts", key));
    let mut seed = first.begin_transaction().await.expect("seed cycle");
    for key in decisions.iter().chain(victims.iter()) {
        seed.put(StateRecord::new(key.clone(), serde_json::json!(false)))
            .await
            .expect("seed record");
    }
    seed.commit().await.expect("seed commit");
    let mut transactions = [
        first.begin_transaction().await.expect("first snapshot"),
        second.begin_transaction().await.expect("second snapshot"),
        third.begin_transaction().await.expect("third snapshot"),
    ];
    for (index, transaction) in transactions.iter_mut().enumerate() {
        for key in [&decisions[index], &decisions[(index + 1) % 3]] {
            let observed = transaction
                .load(key.clone())
                .await
                .expect("dependency")
                .expect("decision");
            assert_eq!(observed.record.value.as_ref(), &serde_json::json!(false));
            assert_eq!(observed.revision.get(), 1);
        }
        transaction
            .put(StateRecord::new(
                decisions[index].clone(),
                serde_json::json!(true),
            ))
            .await
            .expect("conditional decision");
        transaction
            .delete(victims[index].clone())
            .await
            .expect("companion deletion");
        transaction
            .put(StateRecord::new(
                receipts[index].clone(),
                serde_json::json!(true),
            ))
            .await
            .expect("companion receipt");
    }
    let [left, middle, right] = transactions;
    let (left, middle, right) = tokio::join!(left.commit(), middle.commit(), right.commit());
    let committed = [left, middle, right].map(TransactionFixture::committed);
    let count = committed.iter().filter(|result| **result).count();
    // All three create a cycle. One or two successful decisions have a serial
    // ordering; conservative conflicts are allowed without picking a winner.
    assert!(
        (1..3).contains(&count),
        "successful decisions must admit a serial ordering"
    );
    let mut verifier = first
        .begin_transaction()
        .await
        .expect("one verification snapshot");
    for index in 0..3 {
        let decision = verifier
            .load(decisions[index].clone())
            .await
            .expect("decision")
            .expect("seeded decision");
        assert_eq!(decision.record.value.as_bool(), Some(committed[index]));
        assert_eq!(decision.revision.get(), 1 + u64::from(committed[index]));
        let victim = verifier
            .load(victims[index].clone())
            .await
            .expect("deleted record");
        assert_eq!(victim.is_none(), committed[index]);
        if let Some(victim) = victim {
            assert_eq!(victim.revision.get(), 1);
        }
        assert_eq!(
            verifier
                .load(receipts[index].clone())
                .await
                .expect("receipt")
                .is_some(),
            committed[index]
        );
        verifier
            .put(StateRecord::new(
                victims[index].clone(),
                serde_json::json!("recreated"),
            ))
            .await
            .expect("recreate victim");
        assert_eq!(
            verifier
                .load(victims[index].clone())
                .await
                .expect("recreated")
                .expect("present")
                .revision
                .get(),
            2 + u64::from(committed[index])
        );
    }
    verifier
        .commit()
        .await
        .expect("progress after mixed conflicts");
}

struct Contender {
    child: Option<Child>,
    root: PathBuf,
    name: String,
}

impl Contender {
    fn spawn(root: &Path, name: &str) -> Self {
        let output = File::create(root.join(format!("{name}.log"))).expect("owned child log");
        let child = Command::new(std::env::current_exe().expect("integration executable"))
            .args([
                "lancedb_state_transactions::absence_process_entry",
                "--exact",
                "--nocapture",
            ])
            .env("AI_KIT_LANCE_ABSENCE_ROOT", root)
            .env("AI_KIT_LANCE_ABSENCE_NAME", name)
            .stdin(Stdio::null())
            .stdout(output.try_clone().expect("child stdout"))
            .stderr(output)
            .spawn()
            .expect("owned transaction child");
        Self {
            child: Some(child),
            root: root.to_owned(),
            name: name.to_owned(),
        }
    }

    fn output(&self) -> String {
        std::fs::read_to_string(self.root.join(format!("{}.log", self.name))).expect("child log")
    }

    fn wait_read(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !self.root.join(format!("{}.read", self.name)).is_file() {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("owned child")
                .try_wait()
                .expect("child status")
            {
                panic!(
                    "child exited before absence read: {status}\n{}",
                    self.output()
                );
            }
            assert!(
                Instant::now() < deadline,
                "child did not reach read barrier\n{}",
                self.output()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn finish(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("owned child")
                .try_wait()
                .expect("child status")
            {
                self.child
                    .take()
                    .expect("completed child")
                    .wait()
                    .expect("reap child");
                assert!(
                    status.success(),
                    "child failed: {status}\n{}",
                    self.output()
                );
                return serde_json::from_slice(
                    &std::fs::read(self.root.join(format!("{}.result", self.name)))
                        .expect("child result"),
                )
                .expect("commit boolean");
            }
            assert!(
                Instant::now() < deadline,
                "child did not complete\n{}",
                self.output()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    async fn run(root: &Path, name: &str) {
        assert!(matches!(name, "left" | "right"));
        let store = TransactionFixture::connect(root).await;
        let mut transaction = store.begin_transaction().await.expect("child snapshot");
        for name in ["left", "right"] {
            assert!(
                transaction
                    .load(TransactionFixture::key("process-absence", name))
                    .await
                    .expect("absence read")
                    .is_none()
            );
        }
        std::fs::write(root.join(format!("{name}.read")), []).expect("observed absence barrier");
        tokio::time::timeout(Duration::from_secs(60), async {
            while !root.join("release").is_file() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent releases child");
        transaction
            .put(StateRecord::new(
                TransactionFixture::key("process-absence", name),
                serde_json::json!(true),
            ))
            .await
            .expect("conditional claim");
        transaction
            .put(StateRecord::new(
                TransactionFixture::key("process-absence-receipts", name),
                serde_json::json!(true),
            ))
            .await
            .expect("claim receipt");
        let committed = TransactionFixture::committed(transaction.commit().await);
        std::fs::write(
            root.join(format!("{name}.result")),
            serde_json::to_vec(&committed).expect("result JSON"),
        )
        .expect("record child result");
        drop(store);
    }
}

impl Drop for Contender {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn absence_process_entry() {
    let Some(root) = std::env::var_os("AI_KIT_LANCE_ABSENCE_ROOT") else {
        return;
    };
    let name = std::env::var("AI_KIT_LANCE_ABSENCE_NAME").expect("child name");
    TransactionFixture::runtime().block_on(Contender::run(&PathBuf::from(root), &name));
}

#[test]
fn independent_processes_protect_absent_and_tombstoned_reads() {
    for tombstone in [false, true] {
        let fixture = TransactionFixture::new();
        TransactionFixture::runtime().block_on(async {
            let store = TransactionFixture::connect(fixture.root()).await;
            if tombstone {
                let mut transaction = store.begin_transaction().await.expect("seed tombstones");
                for name in ["left", "right"] {
                    transaction
                        .delete(TransactionFixture::key("process-absence", name))
                        .await
                        .expect("absent delete");
                }
                transaction.commit().await.expect("commit tombstones");
            }
        });
        let mut left = Contender::spawn(fixture.root(), "left");
        let mut right = Contender::spawn(fixture.root(), "right");
        left.wait_read();
        right.wait_read();
        std::fs::write(fixture.root().join("release"), []).expect("release competing snapshots");
        let committed = [left.finish(), right.finish()];
        assert_eq!(committed.iter().filter(|value| **value).count(), 1);
        TransactionFixture::runtime().block_on(async {
            let store = TransactionFixture::connect(fixture.root()).await;
            let mut verifier = store
                .begin_transaction()
                .await
                .expect("atomic verification snapshot");
            for (index, name) in ["left", "right"].into_iter().enumerate() {
                let key = TransactionFixture::key("process-absence", name);
                let record = verifier.load(key.clone()).await.expect("claim");
                assert_eq!(record.is_some(), committed[index]);
                if let Some(record) = record {
                    assert_eq!(record.revision.get(), 1 + u64::from(tombstone));
                }
                assert_eq!(
                    verifier
                        .load(TransactionFixture::key("process-absence-receipts", name))
                        .await
                        .expect("receipt")
                        .is_some(),
                    committed[index]
                );
                verifier
                    .put(StateRecord::new(key.clone(), serde_json::json!("settled")))
                    .await
                    .expect("follow-up mutation");
                assert_eq!(
                    verifier
                        .load(key)
                        .await
                        .expect("settled")
                        .expect("present")
                        .revision
                        .get(),
                    1 + u64::from(tombstone) + u64::from(committed[index])
                );
            }
            verifier
                .commit()
                .await
                .expect("fresh progress and exact revisions");
        });
    }
}
