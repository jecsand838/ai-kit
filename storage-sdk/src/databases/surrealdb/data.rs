use std::{collections::BTreeMap, time::Duration};

use serde_json::Value;
use surrealdb::types::{ErrorDetails, QueryError};

use crate::{
    CountRequest, DataRequest, DataResponse, DeleteRequest, InsertRequest, LookupRequest,
    MergeRequest, MutationResult, PageToken, ScanRequest, StorageError, UpdateRequest, WriteMode,
};

use super::{codec, manager::SurrealManager, mapper};

pub(super) async fn handle(
    db: &SurrealManager,
    req: DataRequest,
) -> Result<DataResponse, StorageError> {
    match req {
        DataRequest::Insert(req) => insert(db, req).await,
        DataRequest::Lookup(req) => lookup(db, req).await,
        DataRequest::Scan(req) => scan(db, req).await,
        DataRequest::Update(req) => update(db, req).await,
        DataRequest::Delete(req) => delete(db, req).await,
        DataRequest::Merge(req) => merge(db, req).await,
        DataRequest::ReplacePartition(req) => db.replace_partition(req).await,
        DataRequest::Count(req) => count(db, req).await,
    }
}

pub(super) enum SurrealWrite {
    Append,
    Merge,
    Replace,
}

impl SurrealWrite {
    pub(super) async fn execute(
        self,
        db: &SurrealManager,
        locator: crate::StorageLocator,
        records: Vec<crate::DataRecord>,
    ) -> Result<MutationResult, StorageError> {
        db.ensure_mutable("insert")?;
        if records.is_empty() && !matches!(self, Self::Replace) {
            return Err(db.unsupported(
                "EMPTY_INSERT",
                "SurrealDB empty append and merge requests perform no mutation",
            ));
        }
        let (statements, bindings) = self.statements(db, &locator, &records)?;
        db.ensure_namespace_database(Some(&locator)).await?;
        db.run_response(
            Some(&locator),
            format!(
                "BEGIN TRANSACTION; {}; COMMIT TRANSACTION;",
                statements.join("; ")
            ),
            bindings,
        )
        .await?;
        Ok(MutationResult::no_counts())
    }

    pub(super) fn statements(
        self,
        db: &SurrealManager,
        locator: &crate::StorageLocator,
        records: &[crate::DataRecord],
    ) -> Result<(Vec<String>, BTreeMap<String, Value>), StorageError> {
        reject_payload_id_fields(db, records)?;
        let table = db.resolve_table(locator)?;
        let mut statements = Vec::new();
        if matches!(self, Self::Replace) {
            statements.push(format!("DELETE {table} RETURN NONE"));
        }
        let mut bindings = BTreeMap::new();
        for (index, record) in records.iter().enumerate() {
            let payload_key = format!("payload_{index}");
            bindings.insert(payload_key.clone(), mapper::data_record_to_value(record)?);
            if let Some(id) = &record.id {
                let id_key = format!("id_{index}");
                bindings.insert(id_key.clone(), mapper::record_id_to_value(id));
                let operation = match self {
                    Self::Append => "CREATE",
                    Self::Replace | Self::Merge => "UPSERT",
                };
                statements.push(format!("{operation} type::record('{table}', ${id_key}) CONTENT ${payload_key} RETURN NONE"));
            } else {
                statements.push(format!(
                    "CREATE type::table('{table}') CONTENT ${payload_key} RETURN NONE"
                ));
            }
        }
        Ok((statements, bindings))
    }
}

async fn insert(db: &SurrealManager, req: InsertRequest) -> Result<DataResponse, StorageError> {
    let operation = match req.mode {
        WriteMode::Append => SurrealWrite::Append,
        WriteMode::Overwrite => SurrealWrite::Replace,
    };
    let mutation = operation.execute(db, req.locator, req.records).await?;
    Ok(DataResponse::Mutation(mutation))
}

async fn lookup(db: &SurrealManager, req: LookupRequest) -> Result<DataResponse, StorageError> {
    let table = db.resolve_table(&req.locator)?;
    let projection = mapper::projection_clause(&req.projection)?;
    let mut resources = Vec::new();
    let mut binds = BTreeMap::new();
    for (idx, id) in req.ids.iter().enumerate() {
        let name = format!("id_{idx}");
        binds.insert(name.clone(), mapper::record_id_to_value(id));
        resources.push(format!("type::record('{table}', ${name})"));
    }
    let targets = if resources.is_empty() {
        "[]".to_owned()
    } else {
        resources.join(", ")
    };
    let sql = format!("SELECT {projection} FROM {targets};");
    let rows: Value = db.run_query_take(Some(&req.locator), sql, binds, 0).await?;
    Ok(DataResponse::Records(codec::rows_page_from_value(rows)?))
}

async fn scan(db: &SurrealManager, req: ScanRequest) -> Result<DataResponse, StorageError> {
    let table = db.resolve_table(&req.locator)?;
    let projection = mapper::projection_clause(&req.projection)?;
    let where_sql = mapper::where_clause(req.filter.as_ref())?
        .map(|sql| format!(" WHERE {sql}"))
        .unwrap_or_default();
    let effective_offset = req.effective_offset()?;
    let limit_sql = mapper::limit_offset_clause(req.limit, effective_offset);
    let sql = format!("SELECT {projection} FROM {table}{where_sql} {limit_sql};");
    let rows: Value = db.run_query_value(Some(&req.locator), sql).await?;
    let mut page = codec::rows_page_from_value(rows.clone())?;
    page.next_page = PageToken::next_offset(
        req.limit,
        effective_offset,
        db.affected_row_count(&rows)? as usize,
    );
    Ok(DataResponse::Records(page))
}

async fn update(db: &SurrealManager, req: UpdateRequest) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("update")?;

    let table = db.resolve_table(&req.locator)?;
    let where_sql = mapper::where_clause(req.filter.as_ref())?
        .map(|sql| format!(" WHERE {sql}"))
        .unwrap_or_default();
    let mut binds = BTreeMap::new();
    let assigns = req
        .assignments
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let field = mapper::quote_ident(&item.column);
            let bind = format!("assign_{index}");
            binds.insert(bind.clone(), item.value.clone());
            format!("{field} = ${bind}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("UPDATE {table} SET {assigns}{where_sql} RETURN AFTER;");
    let rows: Value = db.run_query_take(Some(&req.locator), sql, binds, 0).await?;
    Ok(DataResponse::Mutation(affected_rows_mutation(
        db, &rows, false,
    )?))
}

async fn delete(db: &SurrealManager, req: DeleteRequest) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("delete")?;

    let table = db.resolve_table(&req.locator)?;
    let sql = format!(
        "DELETE {table} WHERE {} RETURN BEFORE;",
        mapper::compile_filter(&req.filter)?
    );
    let rows: Value = db.run_query_value(Some(&req.locator), sql).await?;
    Ok(DataResponse::Mutation(affected_rows_mutation(
        db, &rows, true,
    )?))
}

async fn merge(db: &SurrealManager, req: MergeRequest) -> Result<DataResponse, StorageError> {
    reject_payload_id_fields(db, &req.records)?;
    if req.actions != crate::MergeActions::default() {
        return Err(db.unsupported(
            "MERGE_ACTIONS",
            "SurrealDB merge actions beyond logical-id overwrite are not strict-live-proven",
        ));
    }
    if req.on.is_empty() || req.on == ["id".to_string()] {
        if req.records.iter().any(|record| record.id.is_none()) {
            return Err(db.unsupported(
                "LOGICAL_IDS",
                "SurrealDB merge by id requires DataRecord.id; payload `id` is not promoted into a logical id",
            ));
        }
        let mutation = SurrealWrite::Merge
            .execute(db, req.locator, req.records)
            .await?;
        Ok(DataResponse::Mutation(mutation))
    } else {
        Err(db.unsupported(
            "MERGE_COMPLEX",
            "SurrealDB merge is only implemented for logical-id upserts through the common abstraction",
        ))
    }
}

impl SurrealManager {
    async fn replace_partition(
        &self,
        req: crate::ReplacePartitionRequest,
    ) -> Result<DataResponse, StorageError> {
        self.ensure_mutable("replace_partition")?;
        let replacement = SurrealPartitionReplacement::new(self, &req)?;
        self.with_request_timeout(
            "replace_partition",
            "replacing a SurrealDB partition",
            async {
                self.ensure_namespace_database(Some(&req.locator)).await?;
                replacement.execute(self).await
            },
        )
        .await?;
        Ok(DataResponse::Mutation(MutationResult::no_counts()))
    }
}

struct SurrealPartitionReplacement {
    locator: crate::StorageLocator,
    sql: String,
    bindings: BTreeMap<String, Value>,
}

impl SurrealPartitionReplacement {
    fn new(
        db: &SurrealManager,
        req: &crate::ReplacePartitionRequest,
    ) -> Result<Self, StorageError> {
        let payloads = req
            .records
            .iter()
            .map(mapper::data_record_to_value)
            .collect::<Result<Vec<_>, StorageError>>()?;
        let table = db.resolve_table(&req.locator)?;
        let target_table = mapper::table_name(&req.locator).trim().to_owned();
        let partition = mapper::compile_filter(&req.partition)?;
        let identity = mapper::sanitize_identifier(&req.identity_field)?;
        let identities = req
            .records
            .iter()
            .map(|record| {
                record
                    .fields
                    .get(&req.identity_field)
                    .cloned()
                    .ok_or_else(|| {
                        StorageError::InvalidRequest(
                            "replacement record is missing its identity field".to_owned(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut statements = vec![
            "BEGIN TRANSACTION".to_owned(),
            "UPSERT type::record($guard_table, $guard_id) SET generation = rand::uuid() RETURN NONE"
                .to_owned(),
            format!("LET $outside = SELECT VALUE id FROM {table} WHERE {identity} INSIDE $replacement_identities AND NOT ({partition}) LIMIT 1"),
            "IF array::len($outside) > 0 { THROW 'replacement identity already belongs to another partition'; }".to_owned(),
            format!("DELETE {table} WHERE {partition} RETURN NONE"),
        ];
        let mut bindings = BTreeMap::from([
            (
                "guard_table".to_owned(),
                serde_json::json!(SurrealManager::PARTITION_REPLACEMENT_GUARD_TABLE),
            ),
            (
                "guard_id".to_owned(),
                serde_json::json!(target_table.clone()),
            ),
            ("target_table".to_owned(), serde_json::json!(target_table)),
            (
                "replacement_identities".to_owned(),
                Value::Array(identities),
            ),
        ]);
        for (index, payload) in payloads.into_iter().enumerate() {
            let payload_key = format!("payload_{index}");
            bindings.insert(payload_key.clone(), payload);
            statements.push(format!(
                "CREATE type::table($target_table) CONTENT ${payload_key} RETURN NONE"
            ));
        }
        statements.push("COMMIT TRANSACTION".to_owned());

        Ok(Self {
            locator: req.locator.clone(),
            sql: format!("{};", statements.join("; ")),
            bindings,
        })
    }

    async fn execute(&self, db: &SurrealManager) -> Result<(), StorageError> {
        let mut attempt = 0_usize;
        loop {
            let errors = db
                .run_consuming_statement_errors(
                    Some(&self.locator),
                    self.sql.clone(),
                    self.bindings.clone(),
                )
                .await?;
            if errors.is_empty() {
                return Ok(());
            }
            if attempt >= db.common_config().retries() || !Self::can_retry(&errors) {
                return Err(db.map_err("replace_partition", format!("{errors:?}")));
            }

            let backoff_ms = 1_u64 << attempt.min(6);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            attempt += 1;
        }
    }

    fn can_retry(errors: &[(usize, surrealdb::Error)]) -> bool {
        let mut transaction_conflict = false;
        for (_, error) in errors {
            match error.details() {
                ErrorDetails::Query(Some(QueryError::TransactionConflict)) => {
                    transaction_conflict = true;
                }
                ErrorDetails::Query(Some(QueryError::NotExecuted)) => {}
                ErrorDetails::Internal if Self::is_remote_transaction_conflict(error) => {
                    transaction_conflict = true;
                }
                _ => return false,
            }
        }
        transaction_conflict
    }

    fn is_remote_transaction_conflict(error: &surrealdb::Error) -> bool {
        error
            .message()
            .starts_with("Query not executed: Transaction conflict:")
            && error.message().ends_with("This transaction can be retried")
    }
}

async fn count(db: &SurrealManager, req: CountRequest) -> Result<DataResponse, StorageError> {
    if req.filter.is_none() {
        return Ok(DataResponse::Count(db.count_rows(&req.locator).await?));
    }

    let table = db.resolve_table(&req.locator)?;
    let where_sql = mapper::where_clause(req.filter.as_ref())?
        .map(|sql| format!(" WHERE {sql}"))
        .unwrap_or_default();
    let sql = format!("SELECT count() AS count FROM {table}{where_sql} GROUP ALL;");
    let value: Value = db.run_query_value(Some(&req.locator), sql).await?;
    Ok(DataResponse::Count(db.extract_count(&value)?))
}

fn affected_rows_mutation(
    db: &SurrealManager,
    rows: &Value,
    deleted: bool,
) -> Result<MutationResult, StorageError> {
    let matched = db.affected_row_count(rows)?;
    Ok(MutationResult::matched_operation(matched, deleted, true))
}

fn reject_payload_id_fields(
    db: &SurrealManager,
    records: &[crate::DataRecord],
) -> Result<(), StorageError> {
    if records
        .iter()
        .any(|record| record.fields.contains_key("id") || record.vectors.contains_key("id"))
    {
        return Err(db.unsupported(
            "PAYLOAD_ID_FIELD",
            "SurrealDB uses `id` as the record identity; portable payload fields must not be named `id`",
        ));
    }
    Ok(())
}
