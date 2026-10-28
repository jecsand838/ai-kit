use std::collections::BTreeMap;

use serde_json::Value;

use crate::{NearestRequest, QueryResponse, ScoreKind, StorageError, VectorSearchParams};

use super::super::mapper;
use super::{SurrealManager, shared};

impl SurrealManager {
    pub(super) async fn nearest(
        &self,
        request: NearestRequest,
    ) -> Result<QueryResponse, StorageError> {
        let scope = self.scope(&request.query.scope.locator)?;
        let vector_field = request.query.options.vector_field.ok_or_else(|| {
            StorageError::unsupported_capability(
                "VECTOR_FIELD_SELECTION",
                "SurrealDB vector queries require a selected vector field",
            )
        })?;
        let projection = mapper::projection_clause(&request.query.scope.projection)?;
        let knn = SurrealKnnPlan::new(&request.query.options.params)?;
        let mut where_terms = vec![format!(
            "{} {} $vector",
            mapper::quote_ident(&vector_field),
            knn.operator()
        )];
        if let Some(filter) = request.query.scope.filter.as_ref() {
            where_terms.push(mapper::compile_filter(filter)?);
        }
        let mut binds = BTreeMap::new();
        binds.insert(
            "vector".into(),
            mapper::dense_vector_to_json(&request.vector),
        );

        let where_sql = format!(" WHERE {}", where_terms.join(" AND "));
        let limit_sql = mapper::limit_offset_clause(None, request.query.options.params.offset);
        let sql = format!(
            "SELECT {projection} FROM {}{where_sql} {limit_sql};",
            scope.table(),
        );
        let rows: Value = scope.run_take(sql, binds, 0).await?;
        shared::SurrealScoring::Unscored(ScoreKind::Distance).response(rows, None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SurrealKnnPlan {
    IndexedHnsw(String),
    BruteForce(String),
}

impl SurrealKnnPlan {
    fn operator(&self) -> &str {
        match self {
            Self::IndexedHnsw(operator) | Self::BruteForce(operator) => operator,
        }
    }
}

impl SurrealKnnPlan {
    fn new(params: &VectorSearchParams) -> Result<Self, StorageError> {
        let VectorSearchParams {
            limit,
            distance,
            ef,
            ..
        } = params;
        match (*distance, *ef) {
            (None, None) => Err(StorageError::unsupported_capability(
                "HNSW_EF",
                "SurrealDB indexed vector queries require VectorSearchParams.ef",
            )),
            (None, Some(ef)) => Ok(Self::IndexedHnsw(format!("<|{limit},{ef}|>"))),
            (Some(_), Some(_)) => Err(StorageError::InvalidConfig(
                "SurrealDB vector queries cannot combine distance with ef".into(),
            )),
            (Some(metric), None) => Ok(Self::BruteForce(format!(
                "<|{},{}|>",
                limit,
                mapper::vector_metric_token(Some(metric))
            ))),
        }
    }
}
