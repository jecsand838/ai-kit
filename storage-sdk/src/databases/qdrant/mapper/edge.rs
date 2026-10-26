use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::{DataRecord, DenseVector, PageToken, RecordsPage, StorageError, VectorCoercionPolicy};

use super::super::{manager::QdrantManager, point_id};
use super::VectorSelectionPlan;
use super::profile::edge_unnamed_default_vector_unsupported;

pub(crate) fn encode_point_id_for_edge(
    id: &crate::RecordId,
    _db: &QdrantManager,
) -> Result<qdrant_edge::PointId, StorageError> {
    Ok(point_id::encode_native_point_id(id)?.to_edge_id())
}

pub(crate) fn encode_vectors_for_edge(
    _db: &QdrantManager,
    vectors: &BTreeMap<String, DenseVector>,
) -> Result<qdrant_edge::VectorStructPersisted, StorageError> {
    if vectors.keys().any(|name| name.is_empty()) {
        return Err(edge_unnamed_default_vector_unsupported("vector write"));
    }

    let named = vectors
        .iter()
        .map(|(name, vector)| -> Result<_, StorageError> {
            let name = name.clone();
            let value =
                qdrant_edge::VectorPersisted::Dense(vector.to_f32(VectorCoercionPolicy::Lossless)?);
            Ok((name, value))
        })
        .collect::<Result<_, StorageError>>()?;

    Ok(qdrant_edge::VectorStructPersisted::Named(named))
}

pub(crate) fn decode_edge_record_to_record(
    _db: &QdrantManager,
    point: &qdrant_edge::Record,
    vector_selection: &VectorSelectionPlan,
) -> Result<DataRecord, StorageError> {
    let payload_map = point
        .payload
        .as_ref()
        .map(|payload| payload.0.clone())
        .unwrap_or_default();

    let id_json = edge_point_id_json(&point.id);
    let id = point_id::decode_point_id(&id_json)?;

    let mut vectors = BTreeMap::new();
    if let Some(vector) = point.vector.as_ref() {
        match vector {
            qdrant_edge::VectorStructInternal::Single(vector) => {
                if vector_selection.requests_vectors() {
                    return Err(edge_unnamed_default_vector_unsupported("vector projection"));
                }
                vectors.insert(
                    qdrant_edge::DEFAULT_VECTOR_NAME.to_string(),
                    DenseVector::F32(vector.clone()),
                );
            }
            qdrant_edge::VectorStructInternal::MultiDense(_) => {
                return Err(super::profile::VectorProfile::unsupported_native(
                    "edge vector decode",
                ));
            }
            qdrant_edge::VectorStructInternal::Named(map) => {
                for (name, vector) in map {
                    vectors.insert(name.clone(), decode_edge_vector_internal(vector)?);
                }
            }
        }
    }
    vector_selection.retain_decoded_vectors(&mut vectors);

    Ok(DataRecord {
        id,
        vectors,
        fields: payload_map,
    })
}

pub(crate) fn decode_edge_records_page(
    db: &QdrantManager,
    points: &[qdrant_edge::Record],
    vector_selection: &VectorSelectionPlan,
    next_page: Option<PageToken>,
) -> Result<RecordsPage, StorageError> {
    Ok(RecordsPage {
        records: points
            .iter()
            .map(|point| decode_edge_record_to_record(db, point, vector_selection))
            .collect::<Result<Vec<_>, StorageError>>()?,
        total: None,
        next_page,
    })
}

fn edge_point_id_json(id: &qdrant_edge::PointId) -> Value {
    match id {
        qdrant_edge::PointId::NumId(value) => json!(value),
        qdrant_edge::PointId::Uuid(value) => Value::String(value.to_string()),
    }
}

fn decode_edge_vector_internal(
    value: &qdrant_edge::VectorInternal,
) -> Result<DenseVector, StorageError> {
    match value {
        qdrant_edge::VectorInternal::Dense(vector) => Ok(DenseVector::F32(vector.clone())),
        qdrant_edge::VectorInternal::Sparse(_) | qdrant_edge::VectorInternal::MultiDense(_) => Err(
            super::profile::VectorProfile::unsupported_native("edge named-vector decode"),
        ),
    }
}
