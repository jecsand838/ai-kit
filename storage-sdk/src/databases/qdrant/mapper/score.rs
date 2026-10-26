use crate::{ScoreKind, ScoredRecord, StorageError};

use super::super::manager::QdrantManager;
use super::{
    VectorSelectionPlan, edge::decode_edge_record_to_record, point_decode::decode_point_to_record,
};

pub(crate) fn decode_scored_record(
    db: &QdrantManager,
    point: &serde_json::Value,
    vector_selection: &VectorSelectionPlan,
) -> Result<ScoredRecord, StorageError> {
    let record = decode_point_to_record(db, point, vector_selection)?;
    let raw_score = point.get("score").and_then(serde_json::Value::as_f64);
    Ok(ScoredRecord {
        record,
        raw_score,
        score_kind: ScoreKind::Similarity,
    })
}

pub(crate) fn decode_edge_scored_record(
    db: &QdrantManager,
    point: &qdrant_edge::ScoredPoint,
    vector_selection: &VectorSelectionPlan,
) -> Result<ScoredRecord, StorageError> {
    let record = decode_edge_record_to_record(
        db,
        &qdrant_edge::Record {
            id: point.id,
            payload: point.payload.clone(),
            vector: point.vector.clone(),
            shard_key: point.shard_key.clone(),
            order_value: point.order_value,
        },
        vector_selection,
    )?;
    let raw_score = Some(point.score as f64);
    Ok(ScoredRecord {
        record,
        raw_score,
        score_kind: ScoreKind::Similarity,
    })
}
