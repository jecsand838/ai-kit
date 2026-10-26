use std::time::Duration;

use crate::{
    IndexInfo, IndexRequest, IndexResponse, StorageError, StorageLocator, WaitForIndexRequest,
};

use super::{manager::LanceManager, mapper};

pub(super) async fn handle(
    db: &LanceManager,
    request: IndexRequest,
) -> Result<IndexResponse, StorageError> {
    match request {
        IndexRequest::Create(request) => create_index(db, request).await,
        IndexRequest::List { locator } => list_indexes(db, locator).await,
        IndexRequest::Drop { locator, name } => drop_index(db, locator, name).await,
        IndexRequest::WaitFor(request) => wait_for_index(db, request).await,
    }
}

async fn create_index(
    db: &LanceManager,
    request: crate::CreateIndexRequest,
) -> Result<IndexResponse, StorageError> {
    db.ensure_mutable("create_index")?;
    let table = db.open_table(&request.locator).await?;
    if let crate::IndexKind::Vector { dimension, .. } = &request.kind {
        let schema = table
            .schema()
            .await
            .map_err(|error| db.map_err("create_index.schema", error))?;
        let field = schema
            .field_with_name(&request.columns[0])
            .map_err(|error| {
                StorageError::InvalidRequest(format!(
                    "vector index column `{}` was not found: {error}",
                    request.columns[0]
                ))
            })?;
        match field.data_type() {
            arrow_schema::DataType::FixedSizeList(_, actual)
                if usize::try_from(*actual).ok() == Some(*dimension) => {}
            arrow_schema::DataType::FixedSizeList(_, actual) => {
                return Err(StorageError::InvalidRequest(format!(
                    "vector index dimension {} does not match column `{}` dimension {actual}",
                    dimension, request.columns[0]
                )));
            }
            other => {
                return Err(StorageError::InvalidRequest(format!(
                    "vector index column `{}` must be a dense vector, got {other:?}",
                    request.columns[0]
                )));
            }
        }
    }
    let index = mapper::index_kind(&request.kind)?;
    let columns = request
        .columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut builder = table.create_index(&columns, index);
    if let Some(timeout_ms) = request.wait_timeout_ms {
        builder = builder.wait_timeout(Duration::from_millis(timeout_ms));
    }
    builder
        .execute()
        .await
        .map_err(|error| db.map_err("create_index", error))?;
    Ok(IndexResponse::Completed)
}

async fn list_indexes(
    db: &LanceManager,
    locator: StorageLocator,
) -> Result<IndexResponse, StorageError> {
    let indexes = db
        .open_table(&locator)
        .await?
        .list_indices()
        .await
        .map_err(|error| db.map_err("list_indices", error))?
        .into_iter()
        .map(|index| IndexInfo {
            name: Some(index.name),
            columns: index.columns,
            ready: None,
        })
        .collect();
    Ok(IndexResponse::Indexes(indexes))
}

async fn drop_index(
    db: &LanceManager,
    locator: StorageLocator,
    name: String,
) -> Result<IndexResponse, StorageError> {
    db.ensure_mutable("drop_index")?;
    db.open_table(&locator)
        .await?
        .drop_index(&name)
        .await
        .map_err(|error| db.map_err("drop_index", error))?;
    Ok(IndexResponse::Completed)
}

async fn wait_for_index(
    db: &LanceManager,
    request: WaitForIndexRequest,
) -> Result<IndexResponse, StorageError> {
    let names = request
        .index_names
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    db.open_table(&request.locator)
        .await?
        .wait_for_index(&names, Duration::from_millis(request.timeout_ms))
        .await
        .map_err(|error| db.map_err("wait_for_index", error))?;
    Ok(IndexResponse::Completed)
}
