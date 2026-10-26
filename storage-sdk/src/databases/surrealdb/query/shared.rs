use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    DataRecord, DenseVector, PageToken, QueryResponse, RecordId, ScoreKind, ScoredRecord,
    StorageError, StorageLocator,
};

use super::super::{codec, mapper};
use super::SurrealManager;

pub(super) enum SurrealScoring {
    Unscored(ScoreKind),
    RelevanceEnvelope,
}

impl SurrealScoring {
    pub(super) fn response(
        self,
        rows: Value,
        next_page: Option<PageToken>,
    ) -> Result<QueryResponse, StorageError> {
        let Value::Array(rows) = rows else {
            return Err(StorageError::Serialization(
                "Surreal query response must be an array".to_owned(),
            ));
        };
        let hits = rows
            .into_iter()
            .map(|row| self.decode(row))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(QueryResponse::from_hits(hits, next_page))
    }

    fn decode(&self, row: Value) -> Result<ScoredRecord, StorageError> {
        match self {
            Self::Unscored(kind) => Ok(ScoredRecord {
                record: codec::value_to_record(row)?,
                raw_score: None,
                score_kind: kind.clone(),
            }),
            Self::RelevanceEnvelope => {
                let Value::Object(mut envelope) = row else {
                    return Err(StorageError::Serialization(
                        "Surreal relevance response must contain a record envelope".to_owned(),
                    ));
                };
                let record = envelope.remove("record").ok_or_else(|| {
                    StorageError::Serialization(
                        "Surreal relevance response is missing its record".to_owned(),
                    )
                })?;
                let raw_score = match envelope.remove("score") {
                    None | Some(Value::Null) => None,
                    Some(Value::Number(score)) => Some(score.as_f64().ok_or_else(|| {
                        StorageError::Serialization(
                            "Surreal relevance score is not representable".to_owned(),
                        )
                    })?),
                    Some(_) => {
                        return Err(StorageError::Serialization(
                            "Surreal relevance score must be numeric or absent".to_owned(),
                        ));
                    }
                };
                Ok(ScoredRecord {
                    record: codec::value_to_record(record)?,
                    raw_score,
                    score_kind: ScoreKind::Relevance,
                })
            }
        }
    }
}

pub(super) async fn resolve_seed_vector(
    db: &SurrealManager,
    locator: &StorageLocator,
    id: &RecordId,
    vector_field: &str,
) -> Result<DenseVector, StorageError> {
    let table = db.resolve_table(locator)?;
    let mut binds = BTreeMap::new();
    binds.insert("id".into(), mapper::record_id_to_value(id));
    let sql = format!("SELECT * FROM type::record('{table}', $id);");
    let rows: Value = db.run_query_take(Some(locator), sql, binds, 0).await?;
    let mut page = codec::rows_page_from_value(rows)?;
    let record = page.records.pop().ok_or_else(|| {
        StorageError::backend_op(
            "search_by_id",
            format!("record `{}` was not found", id.as_string_lossy()),
        )
    })?;
    extract_dense_vector(&record, vector_field).ok_or_else(|| {
        StorageError::backend_op(
            "search_by_id",
            format!(
                "record `{}` has no dense vector field `{vector_field}`",
                id.as_string_lossy()
            ),
        )
    })
}

fn extract_dense_vector(record: &DataRecord, vector_field: &str) -> Option<DenseVector> {
    let values = record
        .fields
        .get(vector_field)?
        .as_array()?
        .iter()
        .map(Value::as_f64)
        .collect::<Option<Vec<_>>>()?;
    Some(DenseVector::F64(values))
}

pub(super) fn next_page_token_from_rows(
    rows: &Value,
    limit: Option<usize>,
    effective_offset: Option<usize>,
) -> Result<Option<PageToken>, StorageError> {
    let count = match rows {
        Value::Array(values) => values.len(),
        other => {
            return Err(StorageError::Serialization(format!(
                "Surreal query response must be an array, got {other}"
            )));
        }
    };
    Ok(PageToken::next_offset(limit, effective_offset, count))
}
