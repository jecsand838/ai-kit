use serde_json::Value;

use crate::{QueryResponse, RandomSampleRequest, ScoreKind, ScrollRequest, StorageError};

use super::super::mapper;
use super::{SurrealManager, shared};

pub(super) async fn scroll(
    db: &SurrealManager,
    request: ScrollRequest,
) -> Result<QueryResponse, StorageError> {
    let scope = db.scope(&request.locator)?;
    let projection = mapper::projection_clause(&request.projection)?;
    let where_sql = mapper::where_clause(request.filter.as_ref())?
        .map(|sql| format!(" WHERE {sql}"))
        .unwrap_or_default();
    let effective_offset =
        crate::PageToken::effective_offset(request.page_token.as_deref(), request.offset)?;
    let limit_sql = mapper::limit_offset_clause(request.limit, effective_offset);
    let sql = format!(
        "SELECT {projection} FROM {}{where_sql} {limit_sql};",
        scope.table()
    );
    let rows: Value = scope.run_value(sql).await?;
    let next_page = shared::next_page_token_from_rows(&rows, request.limit, effective_offset)?;
    shared::SurrealScoring::Unscored(ScoreKind::Unknown).response(rows, next_page)
}

pub(super) async fn random_sample(
    db: &SurrealManager,
    request: RandomSampleRequest,
) -> Result<QueryResponse, StorageError> {
    let scope = db.scope(&request.locator)?;
    let projection = mapper::projection_clause(&request.projection)?;
    let where_sql = mapper::where_clause(request.filter.as_ref())?
        .map(|sql| format!(" WHERE {sql}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT {projection} FROM {}{where_sql} ORDER BY rand() LIMIT {};",
        scope.table(),
        request.limit
    );
    let rows: Value = scope.run_value(sql).await?;
    shared::SurrealScoring::Unscored(ScoreKind::Unknown).response(rows, None)
}
