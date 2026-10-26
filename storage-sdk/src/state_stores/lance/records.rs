use std::{
    collections::{BTreeMap, HashMap, hash_map::IntoValues},
    sync::{Arc, Mutex},
};

use super::{coordination::LanceProtection, json::LanceJson};
use arrow_array::{
    Array, BooleanArray, LargeBinaryArray, RecordBatch, StringArray, UInt8Array, UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;

use crate::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot, StateRevision,
    StateStoreError,
};

pub(super) const EXPECTED_VERSION: &str = "ai-kit.state.expected-version";

/// Private persistence DTO. Decoding and transaction staging validate revisions.
#[derive(Clone)]
pub(super) struct StoredRecord {
    pub(super) key: StateKey,
    pub(super) revision: u64,
    pub(super) value: Option<LanceJson>,
}

impl StoredRecord {
    pub(super) fn snapshot(&self) -> Option<StateRecordSnapshot> {
        self.value.as_ref().map(|value| StateRecordSnapshot {
            record: StateRecord {
                key: self.key.clone(),
                value: value.public_value(),
            },
            revision: StateRevision::new(self.revision)
                .expect("decoded and staged state revisions are positive"),
        })
    }
}

pub(super) struct LanceSnapshot {
    dataset: Arc<Dataset>,
    image: Arc<LanceImage>,
    // Field order keeps protection until the native snapshot is released.
    _protection: Arc<LanceProtection>,
}

/// Decoded immutable state can be shared without retaining native resources or
/// preventing maintenance after the last transaction releases its protection.
pub(super) struct LanceImage {
    version: u64,
    control_token: String,
    records: Vec<StoredRecord>,
    by_key: HashMap<StateKey, usize>,
}

impl LanceSnapshot {
    pub(super) fn schema() -> Arc<Schema> {
        Arc::new(
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
        )
    }

    pub(super) fn validate_storage(dataset: &Dataset) -> Result<(), StateStoreError> {
        let manifest = dataset.manifest();
        let version = manifest.data_storage_format.lance_file_format();
        // Format 2.0 runs primitive encoders on the calling native runtime.
        // Structural formats use an independent global CPU executor whose
        // outstanding work cannot be joined by the transaction owner.
        if manifest.data_storage_format.file_format != "lance"
            || version.to_data_file_numbers() != (2, 0)
            || manifest.uses_stable_row_ids()
        {
            return Err(Self::invalid("state table storage format is incompatible"));
        }
        Ok(())
    }

    pub(super) async fn identity(dataset: &Dataset) -> Result<String, StateStoreError> {
        Self::validate_storage(dataset)?;
        let schema = Schema::from(dataset.schema());
        if schema != *Self::schema() {
            return Err(Self::invalid("state table schema is incompatible"));
        }
        let transaction = dataset
            .read_transaction()
            .await
            .map_err(|_| Self::invalid("state transaction identity could not be read"))?
            .ok_or_else(|| Self::invalid("state transaction identity is missing"))?;
        let expected = transaction
            .transaction_properties
            .as_ref()
            .and_then(|properties| properties.get(EXPECTED_VERSION))
            .and_then(|version| version.parse::<u64>().ok());
        if expected != Some(dataset.version_id())
            || uuid::Uuid::parse_str(&transaction.uuid).is_err()
        {
            return Err(Self::invalid("state table was changed outside its owner"));
        }
        Ok(transaction.uuid)
    }

    pub(super) async fn read(
        dataset: Arc<Dataset>,
        protection: Arc<LanceProtection>,
        previous: Option<&LanceImage>,
    ) -> Result<Self, StateStoreError> {
        let identity = Self::identity(&dataset).await?;
        let mut image = LanceImage {
            version: dataset.version_id(),
            control_token: String::new(),
            records: Vec::new(),
            by_key: HashMap::new(),
        };
        let source = Arc::clone(&dataset);
        let scanner = source.scan();
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(|_| Self::invalid("state snapshot could not be read"))?;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|_| Self::invalid("state snapshot could not be read"))?
        {
            image.decode(&batch, previous)?;
            tokio::task::yield_now().await;
        }
        if image.control_token.is_empty() {
            return Err(Self::invalid("state table control record is missing"));
        }
        if identity != image.control_token {
            return Err(Self::invalid("state table was changed outside its owner"));
        }
        Ok(Self::from_image(dataset, Arc::new(image), protection))
    }

    pub(super) fn from_image(
        dataset: Arc<Dataset>,
        image: Arc<LanceImage>,
        protection: Arc<LanceProtection>,
    ) -> Self {
        Self {
            dataset,
            image,
            _protection: protection,
        }
    }

    pub(super) fn image(&self) -> &Arc<LanceImage> {
        &self.image
    }

    pub(super) fn matches_identity(&self, dataset: &Dataset, token: &str) -> bool {
        self.image.matches_identity(dataset, token)
    }

    pub(super) fn record(&self, key: &StateKey) -> Option<&StoredRecord> {
        self.image
            .by_key
            .get(key)
            .map(|index| &self.image.records[*index])
    }

    pub(super) fn list(&self, request: &StateListRequest) -> StoredPage {
        let mut selected = BTreeMap::new();
        for record in &self.image.records {
            if record.value.is_none()
                || record.key.namespace() != request.namespace()
                || request
                    .prefix()
                    .is_some_and(|prefix| !record.key.key().starts_with(prefix))
                || request
                    .start_after_key()
                    .is_some_and(|cursor| record.key.key().as_bytes() <= cursor.as_bytes())
            {
                continue;
            }
            selected.insert(record.key.key().as_bytes(), record);
            if selected.len() > request.limit() + 1 {
                selected.pop_last();
            }
        }
        let more = selected.len() > request.limit();
        if more {
            selected.pop_last();
        }
        let next_start_after = if more {
            selected
                .last_key_value()
                .map(|(_, record)| record.key.key().to_owned())
        } else {
            None
        };
        StoredPage {
            records: selected.into_values().cloned().collect(),
            next_start_after,
        }
    }

    pub(super) fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    pub(super) fn initial_batch(token: &str) -> Result<RecordBatch, StateStoreError> {
        Self::batch(&[], Some(token))
    }

    fn batch(
        records: &[StoredRecord],
        token: Option<&str>,
    ) -> Result<RecordBatch, StateStoreError> {
        let mut kinds = Vec::new();
        let mut namespaces = Vec::new();
        let mut keys = Vec::new();
        let mut revisions = Vec::new();
        let mut present = Vec::new();
        let mut values = Vec::new();
        let mut tokens = Vec::new();
        if let Some(token) = token {
            kinds.push(1);
            namespaces.push("");
            keys.push("");
            revisions.push(0);
            present.push(false);
            values.push(None);
            tokens.push(token);
        }
        for record in records {
            if record.revision == 0 {
                return Err(StateStoreError::invalid_request(
                    "state revision must be positive",
                ));
            }
            kinds.push(0);
            namespaces.push(record.key.namespace().as_str());
            keys.push(record.key.key());
            revisions.push(record.revision);
            present.push(record.value.is_some());
            values.push(
                record
                    .value
                    .as_ref()
                    .map(LanceJson::encode)
                    .transpose()
                    .map_err(|_| {
                        StateStoreError::serialization("state record could not be encoded")
                    })?,
            );
            tokens.push("");
        }
        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt8Array::from(kinds)),
                Arc::new(StringArray::from(namespaces)),
                Arc::new(StringArray::from(keys)),
                Arc::new(UInt64Array::from(revisions)),
                Arc::new(BooleanArray::from(present)),
                Arc::new(LargeBinaryArray::from_iter(values)),
                Arc::new(StringArray::from(tokens)),
            ],
        )
        .map_err(|_| StateStoreError::serialization("state records could not be encoded"))
    }

    fn invalid(message: &str) -> StateStoreError {
        StateStoreError::backend_operation("read", message)
    }
}

impl LanceImage {
    pub(super) fn committed(
        &self,
        overlay: HashMap<StateKey, StoredRecord>,
        version: u64,
        control_token: String,
    ) -> Arc<Self> {
        let mut records = self.records.clone();
        let mut by_key = self.by_key.clone();
        for record in overlay.into_values() {
            if let Some(index) = by_key.get(&record.key) {
                records[*index] = record;
            } else {
                by_key.insert(record.key.clone(), records.len());
                records.push(record);
            }
        }
        Arc::new(Self {
            version,
            control_token,
            records,
            by_key,
        })
    }

    pub(super) fn matches_identity(&self, dataset: &Dataset, token: &str) -> bool {
        self.version == dataset.version_id() && self.control_token == token
    }

    fn decode(
        &mut self,
        batch: &RecordBatch,
        previous: Option<&Self>,
    ) -> Result<(), StateStoreError> {
        if batch.schema().fields() != LanceSnapshot::schema().fields() {
            return Err(LanceSnapshot::invalid(
                "state record schema is incompatible",
            ));
        }
        let kind = batch.column(0).as_any().downcast_ref::<UInt8Array>();
        let namespaces = batch.column(1).as_any().downcast_ref::<StringArray>();
        let keys = batch.column(2).as_any().downcast_ref::<StringArray>();
        let revisions = batch.column(3).as_any().downcast_ref::<UInt64Array>();
        let present = batch.column(4).as_any().downcast_ref::<BooleanArray>();
        let values = batch.column(5).as_any().downcast_ref::<LargeBinaryArray>();
        let tokens = batch.column(6).as_any().downcast_ref::<StringArray>();
        let (
            Some(kind),
            Some(namespaces),
            Some(keys),
            Some(revisions),
            Some(present),
            Some(values),
            Some(tokens),
        ) = (kind, namespaces, keys, revisions, present, values, tokens)
        else {
            return Err(LanceSnapshot::invalid(
                "state record columns are incompatible",
            ));
        };
        for index in 0..batch.num_rows() {
            if kind.is_null(index)
                || namespaces.is_null(index)
                || keys.is_null(index)
                || revisions.is_null(index)
                || present.is_null(index)
                || tokens.is_null(index)
            {
                return Err(LanceSnapshot::invalid(
                    "state record contains invalid nulls",
                ));
            }
            match kind.value(index) {
                1 => {
                    if !self.control_token.is_empty()
                        || !namespaces.value(index).is_empty()
                        || !keys.value(index).is_empty()
                        || revisions.value(index) != 0
                        || present.value(index)
                        || !values.is_null(index)
                        || uuid::Uuid::parse_str(tokens.value(index)).is_err()
                    {
                        return Err(LanceSnapshot::invalid(
                            "state table control record is invalid",
                        ));
                    }
                    self.control_token = tokens.value(index).to_owned();
                }
                0 => {
                    if revisions.value(index) == 0
                        || present.value(index) == values.is_null(index)
                        || !tokens.value(index).is_empty()
                    {
                        return Err(LanceSnapshot::invalid("stored state record is invalid"));
                    }
                    let key = StateKey::new(namespaces.value(index), keys.value(index))
                        .map_err(|_| LanceSnapshot::invalid("stored state key is invalid"))?;
                    if self.by_key.contains_key(&key) {
                        return Err(LanceSnapshot::invalid(
                            "state table contains duplicate records",
                        ));
                    }
                    let value = if present.value(index) {
                        let bytes = values.value(index);
                        // Fresh scanning and every row check precede reuse.
                        // Only the payload parser may be avoided, and only for
                        // the exact bytes of an already validated immutable value.
                        let retained = previous
                            .and_then(|image| {
                                image.by_key.get(&key).map(|index| &image.records[*index])
                            })
                            .filter(|record| record.revision == revisions.value(index))
                            .and_then(|record| record.value.as_ref())
                            .filter(|value| value.matches_encoding(bytes));
                        Some(match retained {
                            Some(value) => value.clone(),
                            None => LanceJson::decode(bytes)?,
                        })
                    } else {
                        None
                    };
                    self.by_key.insert(key.clone(), self.records.len());
                    self.records.push(StoredRecord {
                        key,
                        revision: revisions.value(index),
                        value,
                    });
                }
                _ => return Err(LanceSnapshot::invalid("state record kind is invalid")),
            }
        }
        Ok(())
    }
}

/// Owns authored values until the native writer requests each bounded batch.
/// The receipt retains typed encoding failures if a native error path flattens
/// its source to text; it is scoped to this one preparation attempt.
pub(super) struct LanceBatches {
    state: BatchState,
    failure: Arc<Mutex<Option<StateStoreError>>>,
}

enum BatchState {
    Control { token: String, rows: BatchRows },
    Records(BatchRows),
    Done,
}

enum BatchRows {
    Delta(IntoValues<StateKey, StoredRecord>),
    Image { image: Arc<LanceImage>, next: usize },
}

impl LanceBatches {
    pub(super) fn delta(records: HashMap<StateKey, StoredRecord>, token: String) -> Self {
        Self {
            state: BatchState::Control {
                token,
                rows: BatchRows::Delta(records.into_values()),
            },
            failure: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn image(image: Arc<LanceImage>, token: String) -> Self {
        Self {
            state: BatchState::Control {
                token,
                rows: BatchRows::Image { image, next: 0 },
            },
            failure: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn failure_receipt(&self) -> Arc<Mutex<Option<StateStoreError>>> {
        Arc::clone(&self.failure)
    }
}

impl Iterator for LanceBatches {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match std::mem::replace(&mut self.state, BatchState::Done) {
            BatchState::Control { token, rows } => {
                self.state = BatchState::Records(rows);
                LanceSnapshot::batch(&[], Some(&token))
            }
            BatchState::Records(mut rows) => {
                let result = rows.next_batch();
                self.state = BatchState::Records(rows);
                match result {
                    Ok(Some(batch)) => Ok(batch),
                    Ok(None) => {
                        self.state = BatchState::Done;
                        return None;
                    }
                    Err(error) => Err(error),
                }
            }
            BatchState::Done => return None,
        };
        Some(result.map_err(|error| {
            self.state = BatchState::Done;
            // This mutex only carries an error value; recovering its contents
            // after poisoning cannot authorize publication or suppress failure.
            *self
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
            ArrowError::ExternalError(Box::new(error))
        }))
    }
}

impl BatchRows {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StateStoreError> {
        match self {
            Self::Delta(rows) => {
                let records = rows.by_ref().take(128).collect::<Vec<_>>();
                if records.is_empty() {
                    return Ok(None);
                }
                LanceSnapshot::batch(&records, None).map(Some)
            }
            Self::Image { image, next } => {
                if *next == image.records.len() {
                    return Ok(None);
                }
                let end = next.saturating_add(128).min(image.records.len());
                let batch = LanceSnapshot::batch(&image.records[*next..end], None)?;
                *next = end;
                Ok(Some(batch))
            }
        }
    }
}

/// Replies retain safe value owners until synchronous public conversion.
pub(super) struct StoredPage {
    records: Vec<StoredRecord>,
    next_start_after: Option<String>,
}

impl StoredPage {
    pub(super) fn into_response(self) -> StateListResponse {
        StateListResponse {
            records: self
                .records
                .iter()
                .filter_map(StoredRecord::snapshot)
                .collect(),
            next_start_after: self.next_start_after,
        }
    }
}
