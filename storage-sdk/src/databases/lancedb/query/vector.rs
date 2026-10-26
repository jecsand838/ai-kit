use crate::{
    DenseVector, FilterExpr, NearestRequest, Projection, QueryResponse, ScoreKind,
    SearchByIdRequest, StorageError, StorageLocator, VectorCoercionPolicy, VectorSearchParams,
};

use super::super::{codec, mapper};
use super::{LanceManager, shared};

pub(super) async fn nearest(
    db: &LanceManager,
    request: NearestRequest,
) -> Result<QueryResponse, StorageError> {
    LanceVectorQuery {
        db,
        vector_column: db
            .default_vector_column(request.query.options.vector_field.as_ref())?
            .to_owned(),
        locator: request.query.scope.locator,
        seed: request.vector,
        filter: request.query.scope.filter,
        projection: request.query.scope.projection,
        params: request.query.options.params,
    }
    .execute()
    .await
}

pub(super) async fn search_by_id(
    db: &LanceManager,
    _request: SearchByIdRequest,
) -> Result<QueryResponse, StorageError> {
    Err(db.unsupported(
        "SEARCH_BY_ID",
        "LanceDB does not expose portable search-by-id without synthetic hidden id storage",
    ))
}

struct LanceVectorQuery<'a> {
    db: &'a LanceManager,
    locator: StorageLocator,
    vector_column: String,
    seed: DenseVector,
    filter: Option<FilterExpr>,
    projection: Projection,
    params: VectorSearchParams,
}

impl LanceVectorQuery<'_> {
    async fn execute(self) -> Result<QueryResponse, StorageError> {
        use futures::TryStreamExt;
        use lancedb::query::ExecutableQuery;

        let table = self.db.table_ops(&self.locator).open().await?;
        let query = table
            .query()
            .nearest_to(self.seed.to_f32(VectorCoercionPolicy::Lossless)?)
            .map_err(|error| self.db.map_err("nearest_to", error))?
            .column(&self.vector_column);
        let schema = table
            .schema()
            .await
            .map_err(|error| self.db.map_err("schema", error))?;
        let filter_sql = mapper::LanceExpressions::new(schema).optional(self.filter.as_ref())?;
        let query = mapper::apply_vector_options(
            query,
            &self.params,
            filter_sql.as_deref(),
            &self.projection,
        )?;
        let batches = query
            .execute()
            .await
            .map_err(|error| self.db.map_err("nearest.execute", error))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| self.db.map_err("nearest.try_collect", error))?;
        let page = codec::decode_batches(&batches)?;
        Ok(shared::records_page_to_query_response(
            page,
            ScoreKind::Distance,
        ))
    }
}
