use crate::databases::shared::VectorParamProfile;
use crate::{
    DenseVector, FilterExpr, NearestRequest, Projection, QueryResponse, RecordId,
    SearchByIdRequest, StorageError, StorageLocator, VectorCoercionPolicy, VectorSearchParams,
};

use super::super::{
    collection::{EdgeTarget, QdrantCollectionTarget},
    manager::QdrantConnection,
    mapper,
};
use super::{QdrantManager, edge, shared};

pub(super) async fn nearest(
    db: &QdrantManager,
    request: NearestRequest,
) -> Result<QueryResponse, StorageError> {
    QdrantVectorQuery::from_nearest(request).execute(db).await
}

pub(super) async fn search_by_id(
    db: &QdrantManager,
    request: SearchByIdRequest,
) -> Result<QueryResponse, StorageError> {
    QdrantVectorQuery::from_search_by_id(request)
        .execute(db)
        .await
}

enum VectorSeed {
    Vector(DenseVector),
    PointId(RecordId),
}

struct QdrantVectorQuery {
    locator: StorageLocator,
    vector_field: Option<String>,
    params: VectorSearchParams,
    filter: Option<FilterExpr>,
    projection: Projection,
    seed: VectorSeed,
    exclude_source: bool,
}

impl QdrantVectorQuery {
    fn from_nearest(request: NearestRequest) -> Self {
        Self {
            locator: request.query.scope.locator,
            vector_field: request.query.options.vector_field,
            params: request.query.options.params,
            filter: request.query.scope.filter,
            projection: request.query.scope.projection,
            seed: VectorSeed::Vector(request.vector),
            exclude_source: false,
        }
    }

    fn from_search_by_id(request: SearchByIdRequest) -> Self {
        Self {
            locator: request.query.scope.locator,
            vector_field: request.query.options.vector_field,
            params: request.query.options.params,
            filter: request.query.scope.filter,
            projection: request.query.scope.projection,
            seed: VectorSeed::PointId(request.id),
            exclude_source: true,
        }
    }

    async fn execute(self, db: &QdrantManager) -> Result<QueryResponse, StorageError> {
        self.validate_params(db)?;
        let target = QdrantCollectionTarget::new(db, &self.locator)?;
        match &target {
            QdrantCollectionTarget::Edge(edge_target) => self.execute_edge(db, edge_target),
            QdrantCollectionTarget::RemoteRest(_) => {
                shared::execute_remote_query(&target, self.remote_spec(db)?).await
            }
        }
    }

    fn validate_params(&self, db: &QdrantManager) -> Result<(), StorageError> {
        let support = match db.connection() {
            QdrantConnection::Edge(_) => VectorParamProfile::qdrant_edge(),
            QdrantConnection::RemoteRest(_) => VectorParamProfile::qdrant_remote(),
        };
        support.validate(
            match self.seed {
                VectorSeed::Vector(_) => "Qdrant nearest",
                VectorSeed::PointId(_) => "Qdrant search-by-id",
            },
            &self.params,
        )
    }

    fn execute_edge(
        self,
        db: &QdrantManager,
        target: &EdgeTarget<'_>,
    ) -> Result<QueryResponse, StorageError> {
        let shard = target.shard();
        mapper::ensure_edge_named_dense_vectors(shard, self.operation())?;
        let vector_field = db.default_vector_name(self.vector_field.as_ref())?;
        let exclude_point_id = self.edge_exclude_point_id(db)?;
        let vector_selection = target.vector_selection_plan(&self.projection)?;
        edge::edge_query(
            db,
            shard,
            edge::EdgeNearestSpec {
                vector: self.edge_vector(db, shard, &vector_field)?,
                using: vector_field,
                filter: self.filter.as_ref(),
                exclude_point_id,
                limit: self.params.limit,
                offset: self.params.offset,
                ef: self.params.ef,
                indexed_only: self.params.fast_search,
                projection: &self.projection,
                vector_selection: &vector_selection,
            }
            .into_request()?,
            &vector_selection,
        )
    }

    fn operation(&self) -> &'static str {
        match self.seed {
            VectorSeed::Vector(_) => "nearest search",
            VectorSeed::PointId(_) => "search-by-id",
        }
    }

    fn edge_vector(
        &self,
        db: &QdrantManager,
        shard: &qdrant_edge::EdgeShard,
        vector_field: &str,
    ) -> Result<Vec<f32>, StorageError> {
        match &self.seed {
            VectorSeed::Vector(vector) => vector.to_f32(VectorCoercionPolicy::Lossless),
            VectorSeed::PointId(id) => {
                let record = shard
                    .retrieve(
                        &[mapper::encode_point_id_for_edge(id, db)?],
                        Some(qdrant_edge::WithPayloadInterface::Bool(false)),
                        Some(qdrant_edge::WithVector::Bool(true)),
                    )
                    .map_err(|error| db.map_err("edge_retrieve", error))?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        StorageError::backend_op(
                            "edge_search_by_id",
                            format!("record `{}` was not found", id.as_string_lossy()),
                        )
                    })?;
                edge::edge_dense_vector_from_record(db, &record, vector_field)
            }
        }
    }

    fn edge_exclude_point_id(
        &self,
        db: &QdrantManager,
    ) -> Result<Option<qdrant_edge::PointId>, StorageError> {
        match (&self.seed, self.exclude_source) {
            (VectorSeed::PointId(id), true) => mapper::encode_point_id_for_edge(id, db).map(Some),
            _ => Ok(None),
        }
    }

    fn remote_spec<'a>(
        &'a self,
        db: &'a QdrantManager,
    ) -> Result<shared::RemoteQuerySpec<'a>, StorageError> {
        Ok(shared::RemoteQuerySpec {
            projection: &self.projection,
            vector_field: self.vector_field.as_ref(),
            query: match &self.seed {
                VectorSeed::Vector(vector) => {
                    serde_json::json!(vector.to_f32(VectorCoercionPolicy::Lossless)?)
                }
                VectorSeed::PointId(id) => mapper::encode_point_id(id, db)?,
            },
            filter: self.filter.as_ref(),
            limit: self.params.limit,
            resolve_using: true,
            extra: remote_vector_extra(&self.params),
        })
    }
}

fn remote_vector_extra(params: &VectorSearchParams) -> serde_json::Map<String, serde_json::Value> {
    let mut extra = serde_json::Map::new();
    if let Some(offset) = params.offset {
        extra.insert("offset".to_owned(), serde_json::json!(offset));
    }
    if params.ef.is_some() || params.fast_search {
        let mut search = serde_json::Map::new();
        if let Some(ef) = params.ef {
            search.insert("hnsw_ef".to_owned(), serde_json::json!(ef));
        }
        if params.fast_search {
            search.insert("indexed_only".to_owned(), serde_json::Value::Bool(true));
        }
        extra.insert("params".to_owned(), serde_json::Value::Object(search));
    }
    extra
}
