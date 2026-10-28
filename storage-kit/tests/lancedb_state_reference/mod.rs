//! A deliberately small whole-image native reference for successful transaction
//! semantics. This is not a production adapter or a cancellation/durability proof.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, LargeBinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use lance::{
    Dataset,
    dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams},
};
use storage_kit::{
    StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::record::{
        StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
        StateRevision,
    },
};

use crate::state_cases::StateCases;

#[derive(Clone)]
struct OverwriteStore {
    directory: Arc<tempfile::TempDir>,
}

#[derive(Clone)]
struct ReferenceRecord {
    revision: u64,
    value: Option<Arc<serde_json::Value>>,
}

impl ReferenceRecord {
    fn snapshot(&self, key: &StateKey) -> Option<StateRecordSnapshot> {
        self.value.as_ref().map(|value| StateRecordSnapshot {
            record: StateRecord {
                key: key.clone(),
                value: Arc::clone(value),
            },
            revision: StateRevision::new(self.revision).expect("positive reference revision"),
        })
    }
}

enum ReferenceState {
    Active {
        dataset: Arc<Dataset>,
        records: HashMap<StateKey, ReferenceRecord>,
        changed: bool,
    },
    Invalid(StateStoreError),
}

struct OverwriteTransaction {
    state: ReferenceState,
    // Keep test-owned files alive if the store itself is dropped first.
    store: OverwriteStore,
}

impl OverwriteStore {
    async fn new() -> Self {
        let store = Self {
            directory: Arc::new(tempfile::tempdir().expect("reference directory")),
        };
        let params = WriteParams {
            mode: WriteMode::Create,
            ..WriteParams::default()
        };
        let uri = store.uri();
        let transaction = InsertBuilder::new(uri.as_str())
            .with_params(&params)
            .execute_uncommitted(vec![Self::batch(&HashMap::new())])
            .await
            .expect("prepare reference table");
        CommitBuilder::new(uri.as_str())
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .expect("publish reference table");
        store
    }

    fn uri(&self) -> String {
        self.directory
            .path()
            .join("reference.lance")
            .to_str()
            .expect("UTF-8 reference path")
            .to_owned()
    }

    async fn dataset(&self) -> Arc<Dataset> {
        Arc::new(
            Dataset::open(&self.uri())
                .await
                .expect("fresh reference snapshot"),
        )
    }

    fn batch(records: &HashMap<StateKey, ReferenceRecord>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("namespace", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("revision", DataType::UInt64, false),
            Field::new("payload", DataType::LargeBinary, true),
        ]));
        // The reference's empty-namespace row keeps an otherwise empty full
        // image representable. Its format is independent from the supplied Kit.
        let mut namespaces = vec![""];
        let mut keys = vec![""];
        let mut revisions = vec![0];
        let mut payloads = vec![None];
        for (key, record) in records {
            namespaces.push(key.namespace().as_str());
            keys.push(key.key());
            revisions.push(record.revision);
            payloads.push(
                record.value.as_ref().map(|value| {
                    serde_json::to_vec(value.as_ref()).expect("encode reference JSON")
                }),
            );
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(namespaces)),
                Arc::new(StringArray::from(keys)),
                Arc::new(UInt64Array::from(revisions)),
                Arc::new(LargeBinaryArray::from_iter(
                    payloads.iter().map(|value| value.as_deref()),
                )),
            ],
        )
        .expect("reference full image")
    }

    async fn records(dataset: &Dataset) -> HashMap<StateKey, ReferenceRecord> {
        let batch = dataset
            .scan()
            .try_into_batch()
            .await
            .expect("read reference full image");
        let namespaces = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("namespaces");
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keys");
        let revisions = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("revisions");
        let payloads = batch
            .column(3)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("payloads");
        let mut records = HashMap::new();
        let mut controls = 0;
        for index in 0..batch.num_rows() {
            if namespaces.value(index).is_empty() {
                assert!(keys.value(index).is_empty());
                assert_eq!(revisions.value(index), 0);
                assert!(payloads.is_null(index));
                controls += 1;
                continue;
            }
            let key =
                StateKey::new(namespaces.value(index), keys.value(index)).expect("reference key");
            let revision = revisions.value(index);
            assert!(revision > 0);
            let value = if payloads.is_null(index) {
                None
            } else {
                Some(Arc::new(
                    serde_json::from_slice(payloads.value(index)).expect("reference JSON"),
                ))
            };
            assert!(
                records
                    .insert(key, ReferenceRecord { revision, value })
                    .is_none()
            );
        }
        assert_eq!(controls, 1);
        records
    }
}

impl StateStoreBackend for OverwriteStore {
    type Transaction = OverwriteTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let dataset = self.dataset().await;
        Ok(Self::records(&dataset)
            .await
            .get(&key)
            .and_then(|record| record.snapshot(&key)))
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let dataset = self.dataset().await;
        let mut records = Self::records(&dataset)
            .await
            .into_iter()
            .filter(|(key, _)| {
                key.namespace() == request.namespace()
                    && request
                        .prefix()
                        .is_none_or(|prefix| key.key().starts_with(prefix))
                    && request
                        .start_after_key()
                        .is_none_or(|cursor| key.key().as_bytes() > cursor.as_bytes())
            })
            .filter_map(|(key, record)| record.snapshot(&key))
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.record
                .key
                .key()
                .as_bytes()
                .cmp(right.record.key.key().as_bytes())
        });
        let next_start_after = if records.len() > request.limit() {
            records.truncate(request.limit());
            records
                .last()
                .map(|record| record.record.key.key().to_owned())
        } else {
            None
        };
        Ok(StateListResponse {
            records,
            next_start_after,
        })
    }

    async fn begin_transaction(&self) -> Result<OverwriteTransaction, StateStoreError> {
        let dataset = self.dataset().await;
        let records = Self::records(&dataset).await;
        Ok(OverwriteTransaction {
            state: ReferenceState::Active {
                dataset,
                records,
                changed: false,
            },
            store: self.clone(),
        })
    }
}

impl OverwriteTransaction {
    fn stage(
        &mut self,
        key: StateKey,
        value: Option<Arc<serde_json::Value>>,
    ) -> Result<(), StateStoreError> {
        match &mut self.state {
            ReferenceState::Invalid(error) => Err(error.clone()),
            ReferenceState::Active {
                records, changed, ..
            } => {
                let previous = records.get(&key).map_or(0, |record| record.revision);
                let Some(revision) = previous.checked_add(1) else {
                    let error = StateStoreError::invalid_request("reference revision overflow");
                    self.state = ReferenceState::Invalid(error.clone());
                    return Err(error);
                };
                records.insert(key, ReferenceRecord { revision, value });
                *changed = true;
                Ok(())
            }
        }
    }
}

impl StateTransactionBackend for OverwriteTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match &self.state {
            ReferenceState::Active { records, .. } => {
                Ok(records.get(&key).and_then(|record| record.snapshot(&key)))
            }
            ReferenceState::Invalid(error) => Err(error.clone()),
        }
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.stage(record.key, Some(record.value))
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.stage(key, None)
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let result = match self.state {
            ReferenceState::Invalid(error) => Err(error),
            ReferenceState::Active {
                dataset,
                records,
                changed,
            } => {
                if !changed {
                    return Ok(());
                }
                let params = WriteParams {
                    mode: WriteMode::Overwrite,
                    ..WriteParams::default()
                };
                let transaction = InsertBuilder::new(Arc::clone(&dataset))
                    .with_params(&params)
                    .execute_uncommitted(vec![OverwriteStore::batch(&records)])
                    .await
                    .expect("prepare original snapshot reference overwrite");
                assert_eq!(transaction.read_version, dataset.version_id());
                match CommitBuilder::new(dataset)
                    .with_max_retries(0)
                    .with_skip_auto_cleanup(true)
                    .execute(transaction)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(lance::Error::CommitConflict { .. }) => {
                        Err(StateStoreError::transaction_conflict(
                            "commit",
                            "reference snapshot changed",
                        ))
                    }
                    Err(_) => Err(StateStoreError::backend_operation(
                        "commit",
                        "reference native operation failed",
                    )),
                }
            }
        };
        drop(self.store);
        result
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        drop(self);
        Ok(())
    }
}

pub(super) struct SharedStateCases<S: StateStoreBackend> {
    first: S,
    second: S,
    verifier: S,
}

impl<S: StateStoreBackend> SharedStateCases<S> {
    pub(super) fn new(first: S, second: S, verifier: S) -> Self {
        Self {
            first,
            second,
            verifier,
        }
    }

    pub(super) async fn verify(&self) {
        self.counter().await;
        for initial in [
            SharedInitial::Absent,
            SharedInitial::Tombstone,
            SharedInitial::Present,
        ] {
            self.write_skew(initial).await;
        }
    }

    fn committed(result: Result<(), StateStoreError>) -> bool {
        match result {
            Ok(()) => true,
            Err(StateStoreError::TransactionConflict { .. }) => false,
            Err(error) => panic!("unexpected shared-case finalization error: {error:?}"),
        }
    }

    async fn counter(&self) {
        let key = StateKey::new("shared-reference", "counter").expect("counter key");
        let mut seed = self.first.begin_transaction().await.expect("counter seed");
        seed.put(StateRecord::new(key.clone(), serde_json::json!(0)))
            .await
            .expect("seed");
        seed.commit().await.expect("seed commit");
        let mut observing = self
            .verifier
            .begin_transaction()
            .await
            .expect("old read view");
        assert_eq!(
            observing
                .load(key.clone())
                .await
                .expect("read")
                .expect("counter")
                .revision
                .get(),
            1
        );
        let mut left = self
            .first
            .begin_transaction()
            .await
            .expect("left increment");
        let mut right = self
            .second
            .begin_transaction()
            .await
            .expect("right increment");
        for transaction in [&mut left, &mut right] {
            let previous = transaction
                .load(key.clone())
                .await
                .expect("read")
                .expect("counter");
            assert_eq!(previous.revision.get(), 1);
            transaction
                .put(StateRecord::new(
                    key.clone(),
                    serde_json::json!(previous.record.value.as_u64().expect("counter value") + 1),
                ))
                .await
                .expect("increment");
        }
        let (left, right) = tokio::join!(left.commit(), right.commit());
        assert_eq!(
            usize::from(Self::committed(left)) + usize::from(Self::committed(right)),
            1
        );
        let old = observing
            .load(key.clone())
            .await
            .expect("repeat old snapshot")
            .expect("counter");
        assert_eq!(old.record.value.as_u64(), Some(0));
        assert_eq!(old.revision.get(), 1);
        observing
            .commit()
            .await
            .expect("readonly serializes before winner");
        let current = self
            .verifier
            .load(key.clone())
            .await
            .expect("fresh read")
            .expect("counter");
        assert_eq!(current.record.value.as_u64(), Some(1));
        assert_eq!(current.revision.get(), 2);
        let mut rolled_back = self.first.begin_transaction().await.expect("rollback view");
        rolled_back
            .put(StateRecord::new(key.clone(), serde_json::json!(99)))
            .await
            .expect("staged rollback");
        rolled_back.rollback().await.expect("rollback");
        assert_eq!(
            self.verifier
                .load(key.clone())
                .await
                .expect("after rollback"),
            Some(current)
        );
        let mut next = self
            .second
            .begin_transaction()
            .await
            .expect("fresh progress");
        next.put(StateRecord::new(key.clone(), serde_json::json!(2)))
            .await
            .expect("next");
        next.commit().await.expect("next commit");
        let final_record = self
            .verifier
            .load(key)
            .await
            .expect("final read")
            .expect("counter");
        assert_eq!(final_record.revision.get(), 3);
        assert_eq!(final_record.record.value.as_u64(), Some(2));
    }

    async fn write_skew(&self, initial: SharedInitial) {
        let suffix = initial.namespace();
        let keys = [
            StateKey::new(format!("shared-left-{suffix}"), "decision").expect("left key"),
            StateKey::new(format!("shared-right-{suffix}"), "decision").expect("right key"),
        ];
        let receipts = [
            StateKey::new(format!("shared-receipts-{suffix}"), "left").expect("receipt"),
            StateKey::new(format!("shared-receipts-{suffix}"), "right").expect("receipt"),
        ];
        let mut seed = self.first.begin_transaction().await.expect("seed view");
        for key in &keys {
            match initial {
                SharedInitial::Absent => {}
                SharedInitial::Tombstone => seed.delete(key.clone()).await.expect("seed tombstone"),
                SharedInitial::Present => seed
                    .put(StateRecord::new(key.clone(), serde_json::json!(false)))
                    .await
                    .expect("seed present"),
            }
        }
        seed.commit().await.expect("seed commit");
        let mut left = self.first.begin_transaction().await.expect("left view");
        let mut right = self.second.begin_transaction().await.expect("right view");
        for key in &keys {
            for transaction in [&mut left, &mut right] {
                let record = transaction
                    .load(key.clone())
                    .await
                    .expect("conditional read");
                assert_eq!(record.is_some(), matches!(initial, SharedInitial::Present));
                if let Some(record) = record {
                    assert_eq!(record.revision.get(), 1);
                    assert_eq!(record.record.value.as_ref(), &serde_json::json!(false));
                }
            }
        }
        for (transaction, index) in [(&mut left, 0), (&mut right, 1)] {
            transaction
                .put(StateRecord::new(
                    keys[index].clone(),
                    serde_json::json!(true),
                ))
                .await
                .expect("decision");
            transaction
                .put(StateRecord::new(
                    receipts[index].clone(),
                    serde_json::json!(true),
                ))
                .await
                .expect("receipt");
        }
        let (left, right) = tokio::join!(left.commit(), right.commit());
        let committed = [Self::committed(left), Self::committed(right)];
        assert_eq!(usize::from(committed[0]) + usize::from(committed[1]), 1);
        for index in 0..2 {
            assert_eq!(
                self.verifier
                    .load(receipts[index].clone())
                    .await
                    .expect("receipt")
                    .is_some(),
                committed[index]
            );
            let current = self
                .verifier
                .load(keys[index].clone())
                .await
                .expect("decision");
            if committed[index] {
                let current = current.expect("committed decision");
                assert_eq!(current.revision.get(), initial.revision() + 1);
                assert_eq!(current.record.value.as_ref(), &serde_json::json!(true));
            } else if matches!(initial, SharedInitial::Present) {
                let current = current.expect("original decision");
                assert_eq!(current.revision.get(), 1);
                assert_eq!(current.record.value.as_ref(), &serde_json::json!(false));
            } else {
                assert!(current.is_none());
            }
        }
        let mut next = self.verifier.begin_transaction().await.expect("next view");
        for key in &keys {
            next.put(StateRecord::new(key.clone(), serde_json::json!("settled")))
                .await
                .expect("next decision");
        }
        next.commit().await.expect("fresh progress");
        for index in 0..2 {
            assert_eq!(
                self.verifier
                    .load(keys[index].clone())
                    .await
                    .expect("settled")
                    .expect("decision")
                    .revision
                    .get(),
                initial.revision() + u64::from(committed[index]) + 1
            );
        }
    }
}

#[derive(Clone, Copy)]
enum SharedInitial {
    Absent,
    Tombstone,
    Present,
}

impl SharedInitial {
    fn namespace(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Tombstone => "tombstone",
            Self::Present => "present",
        }
    }

    fn revision(self) -> u64 {
        match self {
            Self::Absent => 0,
            Self::Tombstone | Self::Present => 1,
        }
    }
}

#[tokio::test]
async fn native_strict_overwrite_reference_passes_portable_record_cases() {
    StateCases::new(OverwriteStore::new().await).verify().await;
}

#[tokio::test]
async fn native_strict_overwrite_reference_passes_shared_concurrent_cases() {
    let store = OverwriteStore::new().await;
    SharedStateCases::new(store.clone(), store.clone(), store)
        .verify()
        .await;
}
