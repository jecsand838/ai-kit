use std::{
    collections::VecDeque,
    ops::Bound,
    sync::{Arc, Mutex},
};

use serde::{Serialize, Serializer, ser::SerializeStruct};

use redb::ReadableDatabase;

use crate::{
    StateKey, StateListRequest, StateListResponse, StateNamespace, StateRecord,
    StateRecordSnapshot, StateRevision, StateStoreError,
};

use super::error::RedbErrorContext;

const REDB_RECORD_SCHEMA_VERSION: u16 = 1;

pub(super) const RECORDS: redb::TableDefinition<&[u8], &[u8]> =
    redb::TableDefinition::new("state_records_v1");

pub(super) enum RedbRecordStatus {
    Deleted { revision: StateRevision },
    Present { revision: StateRevision },
}

pub(super) struct RedbStateKeyCodec;

pub(super) struct RedbEncodedKey {
    bytes: Vec<u8>,
}

impl RedbEncodedKey {
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) struct RedbListRange {
    lower: Bound<Vec<u8>>,
    upper: Vec<u8>,
    limit: usize,
}

impl RedbListRange {
    fn lower(&self) -> Bound<&[u8]> {
        self.lower.as_ref().map(Vec::as_slice)
    }

    fn upper(&self) -> &[u8] {
        &self.upper
    }

    fn limit_with_extra(&self) -> Result<usize, StateStoreError> {
        self.limit
            .checked_add(1)
            .ok_or_else(|| StateStoreError::invalid_request("state list limit overflow"))
    }
}

impl RedbStateKeyCodec {
    pub(super) fn encode_key(key: &StateKey) -> RedbEncodedKey {
        let mut bytes = Self::namespace_prefix(key.namespace());
        bytes.extend_from_slice(key.key().as_bytes());
        RedbEncodedKey { bytes }
    }

    pub(super) fn namespace_prefix(namespace: &StateNamespace) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(namespace.as_str().len() + 1);
        bytes.extend_from_slice(namespace.as_str().as_bytes());
        bytes.push(0);
        bytes
    }

    pub(super) fn request_range(request: &StateListRequest) -> RedbListRange {
        let mut lower = Self::namespace_prefix(request.namespace());
        if let Some(prefix) = request.key_prefix() {
            lower.extend_from_slice(prefix.as_str().as_bytes());
        }
        // The namespace separator makes this nonempty. Its final byte is
        // either zero or UTF-8 from the key prefix, so increment cannot overflow.
        let mut upper = lower.clone();
        upper[lower.len() - 1] += 1;
        let start_after = request.start_after_key().map(|cursor| {
            let mut bytes = Self::namespace_prefix(request.namespace());
            bytes.extend_from_slice(cursor.as_bytes());
            bytes
        });
        let lower = match start_after {
            // Clamp an exhausted prefix to an empty native range.
            Some(cursor) if cursor >= upper => Bound::Included(upper.clone()),
            Some(cursor) if cursor >= lower => Bound::Excluded(cursor),
            _ => Bound::Included(lower),
        };
        RedbListRange {
            lower,
            upper,
            limit: request.limit(),
        }
    }

    pub(super) fn decode_key(
        namespace: &StateNamespace,
        bytes: &[u8],
    ) -> Result<StateKey, StateStoreError> {
        let prefix = Self::namespace_prefix(namespace);
        if !bytes.starts_with(&prefix) {
            return Err(RedbErrorContext::new("list")
                .serialization("encoded Redb key is outside the requested namespace"));
        }
        let suffix = &bytes[prefix.len()..];
        let key = std::str::from_utf8(suffix).map_err(|err| {
            RedbErrorContext::new("list").serialization(format!("invalid Redb key UTF-8: {err}"))
        })?;
        StateKey::new(namespace.as_str().to_owned(), key.to_owned()).map_err(|err| {
            RedbErrorContext::new("list").serialization(format!("invalid decoded Redb key: {err}"))
        })
    }
}

#[derive(Clone)]
pub(super) struct RedbRecordBody {
    revision: u64,
    value: Option<Arc<serde_json::Value>>,
    deleted: bool,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl RedbRecordBody {
    pub(super) fn next_revision(previous: Option<&Self>) -> Result<StateRevision, StateStoreError> {
        match previous {
            Some(body) => match body.status()? {
                RedbRecordStatus::Deleted { revision } | RedbRecordStatus::Present { revision } => {
                    revision.next()
                }
            },
            None => Ok(StateRevision::first()),
        }
    }

    pub(super) fn new_present(
        revision: StateRevision,
        value: Arc<serde_json::Value>,
        now_ms: i64,
        previous: Option<&Self>,
    ) -> Self {
        Self {
            revision: revision.get(),
            value: Some(value),
            deleted: false,
            created_at_ms: previous.map_or(now_ms, |body| body.created_at_ms),
            updated_at_ms: now_ms,
        }
    }

    pub(super) fn new_tombstone(
        revision: StateRevision,
        now_ms: i64,
        previous: Option<&Self>,
    ) -> Self {
        Self {
            revision: revision.get(),
            value: None,
            deleted: true,
            created_at_ms: previous.map_or(now_ms, |body| body.created_at_ms),
            updated_at_ms: now_ms,
        }
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, StateStoreError> {
        let mut value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|err| RedbErrorContext::new("decode").serialization(err))?;
        let object = value.as_object_mut().ok_or_else(|| {
            RedbErrorContext::new("decode").serialization("Redb record body must be an object")
        })?;

        let schema_version = Self::field_u64(object, "schema_version")?;
        if schema_version != u64::from(REDB_RECORD_SCHEMA_VERSION) {
            return Err(RedbErrorContext::new("decode").serialization(format!(
                "unsupported Redb record schema version {schema_version}"
            )));
        }
        let revision = Self::field_u64(object, "revision")?;
        StateRevision::from_backend_u64(revision).map_err(|err| {
            RedbErrorContext::new("decode")
                .serialization(format!("invalid Redb record revision: {err}"))
        })?;
        let deleted = Self::field_bool(object, "deleted")?;
        let raw_value = object.get_mut("value").ok_or_else(|| {
            RedbErrorContext::new("decode").serialization("Redb record body is missing value")
        })?;
        let value = if deleted {
            if !raw_value.is_null() {
                return Err(RedbErrorContext::new("decode")
                    .serialization("deleted Redb record body must store null value"));
            }
            None
        } else {
            Some(Arc::new(raw_value.take()))
        };
        let created_at_ms = Self::field_i64(object, "created_at_ms")?;
        let updated_at_ms = Self::field_i64(object, "updated_at_ms")?;

        Ok(Self {
            revision,
            value,
            deleted,
            created_at_ms,
            updated_at_ms,
        })
    }

    pub(super) fn encode(self) -> Result<Vec<u8>, StateStoreError> {
        if !self.deleted && self.value.is_none() {
            return Err(RedbErrorContext::new("encode")
                .serialization("present Redb record body is missing value"));
        }
        serde_json::to_vec(&self).map_err(|err| RedbErrorContext::new("encode").serialization(err))
    }

    pub(super) fn status(&self) -> Result<RedbRecordStatus, StateStoreError> {
        let revision = StateRevision::from_backend_u64(self.revision).map_err(|err| {
            RedbErrorContext::new("status")
                .serialization(format!("invalid Redb record revision: {err}"))
        })?;
        if self.deleted {
            Ok(RedbRecordStatus::Deleted { revision })
        } else {
            Ok(RedbRecordStatus::Present { revision })
        }
    }

    pub(super) fn into_state_snapshot(
        self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if self.deleted {
            return Ok(None);
        }
        let revision = StateRevision::from_backend_u64(self.revision).map_err(|err| {
            RedbErrorContext::new("decode")
                .serialization(format!("invalid Redb record revision: {err}"))
        })?;
        let value = self.value.ok_or_else(|| {
            RedbErrorContext::new("decode").serialization("present Redb record body missing value")
        })?;
        Ok(Some(StateRecordSnapshot {
            record: StateRecord { key, value },
            revision,
        }))
    }

    fn field_u64(
        object: &serde_json::Map<String, serde_json::Value>,
        name: &str,
    ) -> Result<u64, StateStoreError> {
        object
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                RedbErrorContext::new("decode")
                    .serialization(format!("state record field `{name}` must be a u64"))
            })
    }

    fn field_i64(
        object: &serde_json::Map<String, serde_json::Value>,
        name: &str,
    ) -> Result<i64, StateStoreError> {
        object
            .get(name)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                RedbErrorContext::new("decode")
                    .serialization(format!("state record field `{name}` must be an i64"))
            })
    }

    fn field_bool(
        object: &serde_json::Map<String, serde_json::Value>,
        name: &str,
    ) -> Result<bool, StateStoreError> {
        object
            .get(name)
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| {
                RedbErrorContext::new("decode")
                    .serialization(format!("state record field `{name}` must be a bool"))
            })
    }
}

impl Serialize for RedbRecordBody {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut body = serializer.serialize_struct("RedbRecordBody", 6)?;
        body.serialize_field("schema_version", &REDB_RECORD_SCHEMA_VERSION)?;
        body.serialize_field("revision", &self.revision)?;
        body.serialize_field("value", &self.value.as_deref())?;
        body.serialize_field("deleted", &self.deleted)?;
        body.serialize_field("created_at_ms", &self.created_at_ms)?;
        body.serialize_field("updated_at_ms", &self.updated_at_ms)?;
        body.end()
    }
}

struct RedbRetainedRecord {
    key: Vec<u8>,
    bytes: Vec<u8>,
    body: RedbRecordBody,
}

pub(super) struct RedbReadOperation {
    db: Arc<redb::Database>,
    records: Mutex<VecDeque<RedbRetainedRecord>>,
}

impl RedbReadOperation {
    pub(super) fn new(db: Arc<redb::Database>) -> Self {
        Self {
            db,
            records: Mutex::new(VecDeque::new()),
        }
    }

    pub(super) fn decode_body(
        &self,
        key: &[u8],
        bytes: &[u8],
        remember: bool,
    ) -> Result<RedbRecordBody, StateStoreError> {
        // Callers first obtain these complete bytes through their native read.
        // Revision alone, a caller Value, and a staged put are never cache evidence.
        if let Ok(mut records) = self.records.lock() {
            if let Some(index) = records
                .iter()
                .position(|entry| entry.key == key && entry.bytes == bytes)
            {
                let entry = records.remove(index).expect("located retained body");
                let body = entry.body.clone();
                records.push_back(entry);
                return Ok(body);
            }
            // Keep eligible CS miss decoding and admission under the same guard.
            // The cap counts the complete native envelope, not just its JSON payload.
            if remember && bytes.len() <= 16 * 1024 * 1024 {
                let body = RedbRecordBody::decode(bytes)?;
                if !body.deleted {
                    records.retain(|entry| entry.key != key);
                    while records.len() >= 4
                        || records.iter().map(|entry| entry.bytes.len()).sum::<usize>()
                            + bytes.len()
                            > 16 * 1024 * 1024
                    {
                        records.pop_front();
                    }
                    records.push_back(RedbRetainedRecord {
                        key: key.to_vec(),
                        bytes: bytes.to_vec(),
                        body: body.clone(),
                    });
                }
                return Ok(body);
            }
        }
        // TX misses, oversize bodies and a poisoned memo decode outside its guard.
        RedbRecordBody::decode(bytes)
    }

    pub(super) fn load(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let context = RedbErrorContext::new("load");
        let encoded = RedbStateKeyCodec::encode_key(&key);
        let tx = self.db.begin_read().map_err(|err| context.backend(err))?;
        let table = tx.open_table(RECORDS).map_err(|err| context.backend(err))?;
        match table
            .get(encoded.as_slice())
            .map_err(|err| context.backend(err))?
        {
            Some(body) => self
                .decode_body(encoded.as_slice(), body.value(), true)?
                .into_state_snapshot(key),
            None => Ok(None),
        }
    }

    pub(super) fn list(
        &self,
        request: StateListRequest,
    ) -> Result<StateListResponse, StateStoreError> {
        let context = RedbErrorContext::new("list");
        let range = RedbStateKeyCodec::request_range(&request);
        let tx = self.db.begin_read().map_err(|err| context.backend(err))?;
        let table = tx.open_table(RECORDS).map_err(|err| context.backend(err))?;

        let mut records = Vec::with_capacity(request.limit());
        let limit_with_extra = range.limit_with_extra()?;
        let iterator = table
            .range::<&[u8]>((range.lower(), Bound::Excluded(range.upper())))
            .map_err(|err| context.backend(err))?;
        for entry in iterator {
            let (key_bytes, value_bytes) = entry.map_err(|err| context.backend(err))?;
            let public_key = RedbStateKeyCodec::decode_key(request.namespace(), key_bytes.value())?;
            let body = RedbRecordBody::decode(value_bytes.value())?;
            if let Some(record) = body.into_state_snapshot(public_key)? {
                records.push(record);
                if records.len() == limit_with_extra {
                    break;
                }
            }
        }

        let has_more = records.len() > request.limit();
        if has_more {
            records.truncate(request.limit());
        }
        let next_start_after = if has_more {
            records
                .last()
                .map(|snapshot| snapshot.record.key.key().to_owned())
        } else {
            None
        };
        Ok(StateListResponse {
            records,
            next_start_after,
        })
    }
}
