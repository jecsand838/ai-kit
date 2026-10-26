use serde_json::{Map, Value, json};

use crate::{FilterExpr, Projection, QueryResponse, StorageError};

use super::super::collection::QdrantCollectionTarget;
use super::super::mapper;

pub(super) struct RemoteQuerySpec<'a> {
    pub projection: &'a Projection,
    pub vector_field: Option<&'a String>,
    pub query: Value,
    pub filter: Option<&'a FilterExpr>,
    pub limit: usize,
    pub resolve_using: bool,
    pub extra: Map<String, Value>,
}

pub(super) async fn execute_remote_query(
    target: &QdrantCollectionTarget<'_>,
    spec: RemoteQuerySpec<'_>,
) -> Result<QueryResponse, StorageError> {
    let vector_selection = target.vector_selection_plan(spec.projection).await?;
    let db = target.db();
    let mut body = Map::from_iter([
        ("query".into(), spec.query),
        ("limit".into(), json!(spec.limit)),
        (
            "with_payload".into(),
            mapper::compile_projection_selector(spec.projection),
        ),
        ("with_vector".into(), vector_selection.with_vector()),
    ]);
    if let Some(filter) = mapper::compile_filter(spec.filter)? {
        body.insert("filter".into(), filter);
    }
    body.extend(spec.extra);
    if let Some(using) = if spec.resolve_using {
        Some(vector_selection.query_using(&db.default_vector_name(spec.vector_field)?))
    } else {
        None
    }
    .flatten()
    {
        body.insert("using".into(), json!(using));
    }
    let path = format!("/collections/{}/points/query", target.collection());
    let raw = db
        .rest_client()?
        .request_read_result_value(reqwest::Method::POST, &path, Some(Value::Object(body)))
        .await?;
    decode_query_hits(db, &raw, &vector_selection)
}

fn decode_query_hits(
    db: &super::QdrantManager,
    raw: &Value,
    vector_selection: &mapper::VectorSelectionPlan,
) -> Result<QueryResponse, StorageError> {
    let points = raw
        .get("points")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            StorageError::Serialization(format!("Qdrant query response is missing `points`: {raw}"))
        })?;
    let hits = points
        .iter()
        .map(|point| mapper::decode_scored_record(db, point, vector_selection))
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(QueryResponse::from_hits(hits, None))
}
