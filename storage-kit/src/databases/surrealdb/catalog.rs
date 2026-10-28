use std::{collections::BTreeMap, time::Duration};

use serde_json::Value;
use surrealdb::types::{ErrorDetails, QueryError};

use crate::core::CreateCollectionAction;
use crate::{
    CatalogRequest, CatalogResponse, CreateCollectionRequest, ListCollectionsRequest,
    SchemaEvolutionRequest, StorageError, StorageLocator,
};

use super::{data, manager::SurrealManager, mapper};

pub(super) async fn handle(
    db: &SurrealManager,
    request: CatalogRequest,
) -> Result<CatalogResponse, StorageError> {
    match request {
        CatalogRequest::CreateCollection(request) => create_collection(db, request).await,
        CatalogRequest::OpenCollection { locator } => open_collection(db, locator).await,
        CatalogRequest::ListCollections(request) => list_collections(db, request).await,
        CatalogRequest::DropCollection {
            locator,
            ignore_missing,
        } => drop_collection(db, locator, ignore_missing).await,
        CatalogRequest::AlterCollection(request) => alter_collection(db, request).await,
    }
}

async fn create_collection(
    db: &SurrealManager,
    request: CreateCollectionRequest,
) -> Result<CatalogResponse, StorageError> {
    db.ensure_mutable("create_collection")?;
    db.ensure_namespace_database(Some(&request.locator)).await?;
    let table = db.resolve_table(&request.locator)?;
    let exists = db.table_exists(&request.locator).await?;
    let recreate = match request.mode.resolve(exists) {
        CreateCollectionAction::ErrorExists => {
            return Err(request.already_exists_error("table"));
        }
        CreateCollectionAction::Reuse => {
            return open_collection(db, request.locator).await;
        }
        CreateCollectionAction::Recreate => true,
        CreateCollectionAction::Create => false,
    };

    // Render every field and seed before beginning a destructive replacement.
    let mut definitions = vec![format!("DEFINE TABLE {table} SCHEMALESS")];
    if let Some(schema) = &request.schema {
        for field in &schema.fields {
            let field_name = mapper::sanitize_identifier(&field.name)?;
            let field_type = mapper::field_type_sql(&field.field_type, field.nullable)?;
            definitions.push(format!(
                "DEFINE FIELD IF NOT EXISTS {field_name} ON TABLE {table} TYPE {field_type}"
            ));
        }
    }
    let (seeds, mut bindings) =
        data::SurrealWrite::Append.statements(db, &request.locator, &request.records)?;
    let mut statements = vec!["BEGIN TRANSACTION".to_owned()];
    if recreate {
        statements.push(format!("REMOVE TABLE {table}"));
        statements.push("IF schema::table::exists($guard_table) { DELETE type::record($guard_table, $guard_id) RETURN NONE; }".to_owned());
        bindings.insert(
            "guard_table".to_owned(),
            serde_json::json!(SurrealManager::PARTITION_REPLACEMENT_GUARD_TABLE),
        );
        bindings.insert(
            "guard_id".to_owned(),
            serde_json::json!(mapper::table_name(&request.locator).trim()),
        );
    }
    statements.extend(definitions);
    statements.extend(seeds);
    statements.push("COMMIT TRANSACTION".to_owned());
    // Native schema/type failures roll back the table, contents and indexes.
    db.run_response(
        Some(&request.locator),
        format!("{};", statements.join("; ")),
        bindings,
    )
    .await?;
    open_collection(db, request.locator).await
}

async fn open_collection(
    db: &SurrealManager,
    locator: StorageLocator,
) -> Result<CatalogResponse, StorageError> {
    Ok(CatalogResponse::Collection(
        db.open_collection_info(locator).await?,
    ))
}

async fn list_collections(
    db: &SurrealManager,
    request: ListCollectionsRequest,
) -> Result<CatalogResponse, StorageError> {
    Ok(CatalogResponse::Collections(
        db.list_collection_infos(&request).await?,
    ))
}

async fn drop_collection(
    db: &SurrealManager,
    locator: StorageLocator,
    ignore_missing: bool,
) -> Result<CatalogResponse, StorageError> {
    db.ensure_mutable("drop_collection")?;
    let table = db.resolve_table(&locator)?;
    db.remove_collection_table(&locator, &table, ignore_missing)
        .await?;
    Ok(CatalogResponse::Completed)
}

impl SurrealManager {
    async fn remove_collection_table(
        &self,
        locator: &StorageLocator,
        table: &str,
        ignore_missing: bool,
    ) -> Result<(), StorageError> {
        let remove = if ignore_missing {
            format!("REMOVE TABLE IF EXISTS {table}")
        } else {
            format!("REMOVE TABLE {table}")
        };
        let sql = format!(
            "BEGIN TRANSACTION; {remove}; IF schema::table::exists($guard_table) {{ DELETE type::record($guard_table, $guard_id) RETURN NONE; }}; COMMIT TRANSACTION;"
        );
        let bindings = BTreeMap::from([
            (
                "guard_table".to_owned(),
                serde_json::json!(Self::PARTITION_REPLACEMENT_GUARD_TABLE),
            ),
            (
                "guard_id".to_owned(),
                serde_json::json!(mapper::table_name(locator).trim()),
            ),
        ]);
        self.with_request_timeout(
            "drop_collection",
            "removing a SurrealDB collection",
            async {
                let mut attempt = 0_usize;
                loop {
                    let errors = self
                        .run_consuming_statement_errors(
                            Some(locator),
                            sql.clone(),
                            bindings.clone(),
                        )
                        .await?;
                    if errors.is_empty() {
                        return Ok(());
                    }
                    // SurrealDB 3.3 reports a failed atomic COMMIT as one typed
                    // conflict plus NotExecuted siblings. Index compaction can
                    // cause this rollback even after a successful full-text query.
                    let conflict = errors.iter().any(|(_, error)| {
                        matches!(
                            error.details(),
                            ErrorDetails::Query(Some(QueryError::TransactionConflict))
                        )
                    });
                    let rolled_back = errors.iter().all(|(_, error)| {
                        matches!(
                            error.details(),
                            ErrorDetails::Query(Some(
                                QueryError::TransactionConflict | QueryError::NotExecuted
                            ))
                        )
                    });
                    if attempt >= self.config().max_retries as usize || !conflict || !rolled_back {
                        return Err(self.map_err("drop_collection", format!("{errors:?}")));
                    }
                    // The outer deadline also covers context selection and backoff.
                    // Transport failure and unknown commit outcomes never reach a
                    // replay; only the confirmed rolled-back transaction does.
                    tokio::time::sleep(Duration::from_millis(1_u64 << attempt.min(6))).await;
                    attempt += 1;
                }
            },
        )
        .await
    }
}

async fn alter_collection(
    db: &SurrealManager,
    request: SchemaEvolutionRequest,
) -> Result<CatalogResponse, StorageError> {
    db.ensure_mutable("alter_collection")?;
    match request {
        SchemaEvolutionRequest::AddColumns(request) => {
            if request.read_columns.is_some() {
                return Err(db.unsupported(
                    "SCHEMA_EVOLUTION_READ_COLUMNS",
                    "SurrealDB computed-column source declarations are unsupported",
                ));
            }
            let table = db.resolve_table(&request.locator)?;
            for expression in request.expressions {
                let field = mapper::quote_ident(&expression.name);
                let sql = format!(
                    "DEFINE FIELD IF NOT EXISTS {field} ON TABLE {table} VALUE {}; UPDATE {table} SET {field} = {} RETURN NONE;",
                    expression.expression_sql, expression.expression_sql
                );
                let _: Value = db.run_query_value(Some(&request.locator), sql).await?;
            }
        }
        SchemaEvolutionRequest::AlterColumns(request) => {
            let table = db.resolve_table(&request.locator)?;
            let mut sql = String::from("BEGIN TRANSACTION;");
            for alteration in request.alterations {
                if alteration.cast_to.is_none() && alteration.nullable.is_some() {
                    return Err(db.unsupported(
                        "SCHEMA_NULLABILITY",
                        "SurrealDB nullable-only schema alterations are unsupported",
                    ));
                }
                if alteration.cast_to.is_some() && alteration.nullable.is_none() {
                    return Err(StorageError::InvalidConfig(
                        "SurrealDB casts require an explicit nullable setting".into(),
                    ));
                }
                let field = mapper::sanitize_identifier(&alteration.path)?;
                let renamed = alteration
                    .rename
                    .as_deref()
                    .map(mapper::sanitize_identifier)
                    .transpose()?;
                let field_type = alteration
                    .cast_to
                    .as_ref()
                    .map(|field_type| {
                        mapper::field_type_sql(field_type, alteration.nullable.unwrap_or(false))
                    })
                    .transpose()?;
                match (renamed, field_type) {
                    (Some(renamed), Some(field_type)) => sql.push_str(&format!(
                        " DEFINE FIELD OVERWRITE {renamed} ON TABLE {table} TYPE {field_type}; UPDATE {table} SET {renamed} = <{field_type}> {field} RETURN NONE; REMOVE FIELD {field} ON TABLE {table}; UPDATE {table} UNSET {field} RETURN NONE;"
                    )),
                    (Some(renamed), None) => sql.push_str(&format!(
                        " DEFINE FIELD IF NOT EXISTS {renamed} ON TABLE {table}; UPDATE {table} SET {renamed} = {field} RETURN NONE; REMOVE FIELD {field} ON TABLE {table}; UPDATE {table} UNSET {field} RETURN NONE;"
                    )),
                    (None, Some(field_type)) => sql.push_str(&format!(
                        " DEFINE FIELD OVERWRITE {field} ON TABLE {table} TYPE {field_type};"
                    )),
                    (None, None) => {}
                }
            }
            sql.push_str(" COMMIT TRANSACTION;");
            db.run_response(Some(&request.locator), sql, BTreeMap::new())
                .await?;
        }
        SchemaEvolutionRequest::DropColumns(request) => {
            let table = db.resolve_table(&request.locator)?;
            let fields = request
                .columns
                .iter()
                .map(|column| mapper::sanitize_identifier(column))
                .collect::<Result<Vec<_>, _>>()?;
            if fields.is_empty() {
                return Ok(CatalogResponse::Completed);
            }
            let mut sql = "BEGIN TRANSACTION;".to_owned();
            for field in &fields {
                sql.push_str(&format!(" REMOVE FIELD {field} ON TABLE {table};"));
            }
            sql.push_str(&format!(
                " UPDATE {table} UNSET {} RETURN NONE; COMMIT TRANSACTION;",
                fields.join(", ")
            ));
            db.run_response(Some(&request.locator), sql, BTreeMap::new())
                .await?;
        }
    }
    Ok(CatalogResponse::Completed)
}
