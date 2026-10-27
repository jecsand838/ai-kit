use crate::{Projection, QueryResponse, StorageError};

use super::super::mapper;
use super::QdrantManager;

pub(super) struct EdgeNearestSpec<'a> {
    pub vector: Vec<f32>,
    pub using: String,
    pub filter: Option<&'a crate::FilterExpr>,
    pub exclude_point_id: Option<qdrant_edge::PointId>,
    pub limit: usize,
    pub offset: Option<usize>,
    pub ef: Option<usize>,
    pub indexed_only: bool,
    pub projection: &'a Projection,
    pub vector_selection: &'a mapper::VectorSelectionPlan,
}

impl EdgeNearestSpec<'_> {
    pub(super) fn into_request(self) -> Result<qdrant_edge::QueryRequest, StorageError> {
        let with_payload = mapper::projection_selector_for_edge(self.projection)?;
        let with_vector = self.vector_selection.with_vector_for_edge();
        let filter = self.filter_with_excluded_id()?;
        let params = self.search_params();
        let named_query = if self.using == qdrant_edge::DEFAULT_VECTOR_NAME {
            qdrant_edge::NamedQuery::default_dense(self.vector)
        } else {
            qdrant_edge::NamedQuery::new(
                qdrant_edge::VectorInternal::Dense(self.vector),
                self.using,
            )
        };

        Ok(qdrant_edge::QueryRequest {
            prefetches: Vec::new(),
            query: Some(qdrant_edge::ScoringQuery::Vector(
                qdrant_edge::QueryEnum::Nearest(named_query),
            )),
            filter,
            score_threshold: None,
            limit: self.limit,
            offset: self.offset.unwrap_or(0),
            params,
            with_vector,
            with_payload,
        })
    }

    fn filter_with_excluded_id(&self) -> Result<Option<qdrant_edge::Filter>, StorageError> {
        let mut filter = mapper::compile_filter_for_edge(self.filter)?;
        if let Some(point_id) = self.exclude_point_id {
            filter
                .get_or_insert_with(qdrant_edge::Filter::default)
                .must_not
                .get_or_insert_with(Vec::new)
                .push(qdrant_edge::Condition::HasId(
                    [point_id].into_iter().collect(),
                ));
        }
        Ok(filter)
    }

    fn search_params(&self) -> Option<qdrant_edge::SearchParams> {
        if self.ef.is_none() && !self.indexed_only {
            return None;
        }
        Some(qdrant_edge::SearchParams {
            hnsw_ef: self.ef,
            exact: false,
            quantization: None,
            indexed_only: self.indexed_only,
            acorn: None,
            idf: None,
        })
    }
}

pub(super) fn edge_query(
    db: &QdrantManager,
    shard: &qdrant_edge::EdgeShard,
    request: qdrant_edge::QueryRequest,
    vector_selection: &mapper::VectorSelectionPlan,
) -> Result<crate::QueryResponse, StorageError> {
    let offset = request.offset;
    let limit = request.limit;
    limit.checked_add(offset).ok_or_else(|| {
        StorageError::InvalidRequest("Qdrant query limit plus offset overflows".into())
    })?;
    let results = shard
        .query(request)
        .map_err(|err| db.map_err("edge_query", err))?;
    // Edge applies the requested offset before returning the hits.
    let hits = results
        .iter()
        .take(limit)
        .map(|point| mapper::decode_edge_scored_record(db, point, vector_selection))
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(QueryResponse::from_hits(hits, None))
}

pub(super) fn edge_dense_vector_from_record(
    _db: &QdrantManager,
    record: &qdrant_edge::Record,
    vector_name: &str,
) -> Result<Vec<f32>, StorageError> {
    let vector = record
        .vector
        .as_ref()
        .ok_or_else(|| StorageError::backend_op("edge_search_by_id", "record has no vectors"))?;

    match vector {
        qdrant_edge::VectorStructInternal::Single(_)
        | qdrant_edge::VectorStructInternal::MultiDense(_) => Err(
            mapper::edge_unnamed_default_vector_unsupported("search-by-id"),
        ),
        qdrant_edge::VectorStructInternal::Named(vectors) => match vectors.get(vector_name) {
            Some(qdrant_edge::VectorInternal::Dense(vector)) => Ok(vector.clone()),
            Some(_) => Err(StorageError::unsupported_capability(
                "NON_DENSE_SEARCH_BY_ID",
                format!("edge search-by-id requires a dense vector field, got `{vector_name}`"),
            )),
            None => Err(StorageError::backend_op(
                "edge_search_by_id",
                format!("record does not contain vector field `{vector_name}`"),
            )),
        },
    }
}
