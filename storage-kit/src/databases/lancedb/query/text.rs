use crate::{QueryResponse, ScoreKind, StorageError, TextSearchRequest};

use super::super::{codec, mapper};
use super::{LanceManager, shared};

pub(super) async fn text(
    db: &LanceManager,
    request: TextSearchRequest,
) -> Result<QueryResponse, StorageError> {
    use futures::TryStreamExt;
    use lancedb::{
        index::scalar::FullTextSearchQuery,
        query::{ExecutableQuery, QueryBase},
    };

    let table = db.table_ops(&request.query.scope.locator).open().await?;
    let mut text_query = FullTextSearchQuery::new(request.text);
    if let Some(field) = request.query.options.text_field.as_ref() {
        text_query = text_query
            .with_column(field.clone())
            .map_err(|error| db.map_err("fts.with_column", error))?;
    }
    let query = table.query().full_text_search(text_query);
    let schema = table
        .schema()
        .await
        .map_err(|error| db.map_err("schema", error))?;
    let filter_sql =
        mapper::LanceExpressions::new(schema).optional(request.query.scope.filter.as_ref())?;
    let query = mapper::apply_common_query_options(
        query,
        mapper::CommonQueryOptions {
            projection: &request.query.scope.projection,
            filter_sql: filter_sql.as_deref(),
            limit: request.query.options.limit,
            offset: request.query.options.offset,
        },
    );

    let batches = query
        .execute()
        .await
        .map_err(|error| db.map_err("fts.execute", error))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| db.map_err("fts.try_collect", error))?;

    let page = codec::decode_batches(&batches)?;
    Ok(shared::records_page_to_query_response(
        page,
        ScoreKind::Relevance,
    ))
}
