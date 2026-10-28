use crate::{DataRequest, DataResponse, StorageError, WriteMode};

use super::{codec, manager::LanceManager, mapper};

pub(super) async fn handle(
    db: &LanceManager,
    req: DataRequest,
) -> Result<DataResponse, StorageError> {
    match req {
        DataRequest::Insert(req) => insert(db, req).await,
        DataRequest::Scan(req) => scan(db, req).await,
        DataRequest::Update(req) => update(db, req).await,
        DataRequest::Delete(req) => delete(db, req).await,
        DataRequest::ReplacePartition(req) => db.replace_partition(req).await,
        DataRequest::Count(req) => count(db, req).await,
    }
}

async fn insert(
    db: &LanceManager,
    req: crate::InsertRequest,
) -> Result<DataResponse, StorageError> {
    use lancedb::table::AddDataMode;

    db.ensure_mutable("insert")?;
    reject_lance_logical_ids(db, "insert", &req.records)?;

    let ops = db.table_ops(&req.locator);
    let table = ops.open().await?;
    if req.mode == WriteMode::Overwrite && req.records.is_empty() {
        let result = table
            .delete("true")
            .await
            .map_err(|error| db.map_err("overwrite", error))?;
        return Ok(DataResponse::Mutation(crate::MutationResult::counts(
            Some(0),
            None,
            Some(result.num_deleted_rows),
            None,
            None,
            true,
        )));
    }
    let schema = table
        .schema()
        .await
        .map_err(|error| db.map_err("schema", error))?;
    let batches = codec::encode_records(&req.records, Some(schema.as_ref()))?;
    let mode = match req.mode {
        WriteMode::Append => AddDataMode::Append,
        WriteMode::Overwrite => AddDataMode::Overwrite,
    };

    table
        .add(
            batches
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| StorageError::Serialization(err.to_string()))?,
        )
        .mode(mode)
        .execute()
        .await
        .map_err(|err| db.map_err("add", err))?;

    Ok(DataResponse::Mutation(crate::MutationResult::no_counts()))
}

async fn scan(db: &LanceManager, req: crate::ScanRequest) -> Result<DataResponse, StorageError> {
    let effective_offset = req.effective_offset()?;
    let page = db
        .table_ops(&req.locator)
        .plain_page(
            req.filter.as_ref(),
            &req.projection,
            req.limit,
            effective_offset,
        )
        .await?;

    Ok(DataResponse::Records(page))
}

async fn update(
    db: &LanceManager,
    req: crate::UpdateRequest,
) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("update")?;

    let table = db.table_ops(&req.locator).open().await?;
    let schema = table
        .schema()
        .await
        .map_err(|error| db.map_err("schema", error))?;
    let predicate = mapper::LanceExpressions::new(schema);
    let mut builder = table.update();

    if let Some(filter) = req.filter.as_ref() {
        builder = builder.only_if(predicate.compile(filter)?);
    }

    for assignment in &req.assignments {
        builder = builder.column(&assignment.column, predicate.assignment(assignment)?);
    }

    let result = builder
        .execute()
        .await
        .map_err(|err| db.map_err("update", err))?;

    Ok(DataResponse::Mutation(crate::MutationResult::counts(
        None,
        Some(result.rows_updated),
        None,
        None,
        None,
        true,
    )))
}

async fn delete(
    db: &LanceManager,
    req: crate::DeleteRequest,
) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("delete")?;

    let table = db.table_ops(&req.locator).open().await?;
    let schema = table
        .schema()
        .await
        .map_err(|error| db.map_err("schema", error))?;
    let filter_sql = mapper::LanceExpressions::new(schema).compile(&req.filter)?;
    let result = table
        .delete(filter_sql.as_str())
        .await
        .map_err(|err| db.map_err("delete", err))?;

    Ok(DataResponse::Mutation(crate::MutationResult::counts(
        None,
        None,
        Some(result.num_deleted_rows),
        None,
        None,
        true,
    )))
}

impl LanceManager {
    async fn replace_partition(
        &self,
        req: crate::ReplacePartitionRequest,
    ) -> Result<DataResponse, StorageError> {
        self.ensure_mutable("replace_partition")?;

        let table = self.table_ops(&req.locator).open().await?;
        let schema = table
            .schema()
            .await
            .map_err(|error| self.map_err("schema", error))?;
        let predicate = mapper::LanceExpressions::new(schema.clone());
        let filter_sql = predicate.compile(&req.partition)?;
        if req.records.is_empty() {
            let result = table
                .delete(filter_sql.as_str())
                .await
                .map_err(|err| self.map_err("replace_partition", err))?;
            return Ok(DataResponse::Mutation(crate::MutationResult::counts(
                Some(0),
                Some(0),
                Some(result.num_deleted_rows),
                Some(0),
                Some(0),
                true,
            )));
        }

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
        let matching_identity = crate::FilterExpr::In {
            field: req.identity_field.clone(),
            values: identities,
        };
        let identity_sql = predicate.compile(&matching_identity)?;
        // A SQL NULL predicate does not select the row for replacement, so it
        // belongs to the protected complement just as a false predicate does.
        let outside_sql = format!("({identity_sql}) AND ({filter_sql}) IS NOT TRUE");
        if table
            .count_rows(Some(outside_sql))
            .await
            .map_err(|error| self.map_err("replace_partition", error))?
            != 0
        {
            return Err(StorageError::InvalidRequest(
                "replacement identity already belongs to another partition".to_owned(),
            ));
        }
        let batches = codec::encode_records(&req.records, Some(schema.as_ref()))?;
        let mut merge = table.merge_insert(&[req.identity_field.as_str()]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(Some(filter_sql));
        let result = merge
            .execute(Box::new(batches))
            .await
            .map_err(|err| self.map_err("replace_partition", err))?;
        let upserted = result
            .num_inserted_rows
            .saturating_add(result.num_updated_rows);

        Ok(DataResponse::Mutation(crate::MutationResult::counts(
            Some(result.num_inserted_rows),
            Some(result.num_updated_rows),
            Some(result.num_deleted_rows),
            Some(upserted),
            Some(result.num_updated_rows),
            true,
        )))
    }
}

async fn count(db: &LanceManager, req: crate::CountRequest) -> Result<DataResponse, StorageError> {
    Ok(DataResponse::Count(
        db.table_ops(&req.locator)
            .count_rows(req.filter.as_ref())
            .await?,
    ))
}

fn reject_lance_logical_ids(
    db: &LanceManager,
    op: &'static str,
    records: &[crate::DataRecord],
) -> Result<(), StorageError> {
    if records.iter().any(|record| record.id.is_some()) {
        return Err(db.unsupported(
            "LOGICAL_IDS",
            format!("LanceDB does not persist portable DataRecord.id values for `{op}`"),
        ));
    }
    Ok(())
}
