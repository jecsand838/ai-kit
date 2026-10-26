use std::collections::BTreeMap;

use serde_json::Value;

use crate::databases::shared::VectorParamProfile;
use crate::{
    DistanceMetric, NearestRequest, QueryResponse, RecordId, ScoreKind, SearchByIdRequest,
    StorageError, VectorSearchParams,
};

use super::super::mapper;
use super::{SurrealManager, shared};

impl SurrealManager {
    pub(super) async fn nearest(
        &self,
        request: NearestRequest,
    ) -> Result<QueryResponse, StorageError> {
        self.nearest_excluding(request, None).await
    }

    async fn nearest_excluding(
        &self,
        request: NearestRequest,
        excluded_id: Option<RecordId>,
    ) -> Result<QueryResponse, StorageError> {
        let scope = self.scope(&request.query.scope.locator)?;
        let vector_field = request.query.options.vector_field.ok_or_else(|| {
            StorageError::unsupported_capability(
                "VECTOR_FIELD_SELECTION",
                "SurrealDB vector queries require a selected vector field",
            )
        })?;
        VectorParamProfile::surreal()
            .validate("SurrealDB vector query", &request.query.options.params)?;
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
        if let Some(id) = excluded_id {
            // Native `id` is a record value, not its numeric/string key. Use
            // the same typed identity as the seed lookup, inside the indexed
            // predicate so the seed cannot consume a top-k slot.
            where_terms.push("id != type::record($seed_table, $seed_id)".to_owned());
            binds.insert("seed_table".into(), Value::String(scope.table().to_owned()));
            binds.insert("seed_id".into(), mapper::record_id_to_value(&id));
        }
        let where_sql = format!(" WHERE {}", where_terms.join(" AND "));
        let limit_sql = mapper::limit_offset_clause(None, request.query.options.params.offset);
        let sql = format!(
            "SELECT {projection} FROM {}{where_sql} {limit_sql};",
            scope.table(),
        );
        let rows: Value = scope.run_take(sql, binds, 0).await?;
        shared::SurrealScoring::Unscored(ScoreKind::Distance).response(rows, None)
    }

    pub(super) async fn search_by_id(
        &self,
        request: SearchByIdRequest,
    ) -> Result<QueryResponse, StorageError> {
        let vector_field = request
            .query
            .options
            .vector_field
            .as_deref()
            .ok_or_else(|| {
                StorageError::unsupported_capability(
                    "VECTOR_FIELD_SELECTION",
                    "SurrealDB search-by-id requires a selected vector field",
                )
            })?;
        VectorParamProfile::surreal()
            .validate("SurrealDB vector query", &request.query.options.params)?;
        let vector = shared::resolve_seed_vector(
            self,
            &request.query.scope.locator,
            &request.id,
            vector_field,
        )
        .await?;
        self.nearest_excluding(
            NearestRequest {
                query: request.query,
                vector,
            },
            Some(request.id),
        )
        .await
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
            (Some(DistanceMetric::Dot), _) => Err(StorageError::unsupported_capability(
                "DOT_DISTANCE",
                "SurrealDB portable vector queries do not support dot-product distance",
            )),
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
