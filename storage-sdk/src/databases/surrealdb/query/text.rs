use std::collections::BTreeMap;

use serde_json::Value;

use crate::{QueryResponse, StorageError, TextSearchRequest};

use super::super::mapper;
use super::{SurrealManager, shared};

pub(super) async fn text(
    db: &SurrealManager,
    request: TextSearchRequest,
) -> Result<QueryResponse, StorageError> {
    let scope = db.scope(&request.query.scope.locator)?;
    let field = request.query.options.text_field.clone().ok_or_else(|| {
        db.unsupported(
            "TEXT_FIELD_SELECTION",
            "SurrealDB text search requires a selected text field",
        )
    })?;
    let projection = mapper::projection_clause(&request.query.scope.projection)?;
    let record = match request.query.scope.projection {
        crate::Projection::All => "$this".to_owned(),
        crate::Projection::Columns(_) => format!("(SELECT {projection} FROM $this)[0]"),
    };
    let where_text = format!("{} @1@ $query", mapper::quote_ident(&field));
    let where_sql = match request.query.scope.filter.as_ref() {
        Some(filter) => format!(
            " WHERE ({where_text}) AND ({})",
            mapper::compile_filter(filter)?
        ),
        None => format!(" WHERE {where_text}"),
    };
    let limit_sql =
        mapper::limit_offset_clause(request.query.options.limit, request.query.options.offset);
    let sql = format!(
        "SELECT * FROM (SELECT VALUE {{ record: {record}, score: search::score(1) }} FROM {}{where_sql}) ORDER BY score DESC {limit_sql};",
        scope.table()
    );

    let mut binds = BTreeMap::new();
    binds.insert("query".into(), Value::String(request.text));
    let rows: Value = scope.run_take(sql, binds, 0).await?;
    shared::SurrealScoring::RelevanceEnvelope.response(rows, None)
}
