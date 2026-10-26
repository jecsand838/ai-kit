use crate::{
    DataRequest, DataResponse, DeleteRequest, InsertRequest, LookupRequest, MergeRequest,
    ScanRequest, StorageError, UpdateRequest,
};

use super::{
    collection::{QdrantCollectionTarget, QdrantWrite},
    manager::QdrantManager,
    mapper,
};

pub(super) async fn handle(
    db: &QdrantManager,
    req: DataRequest,
) -> Result<DataResponse, StorageError> {
    match req {
        DataRequest::Insert(req) => insert(db, req).await,
        DataRequest::Lookup(req) => lookup(db, req).await,
        DataRequest::Scan(req) => scan(db, req).await,
        DataRequest::Update(req) => update(db, req).await,
        DataRequest::Delete(req) => delete(db, req).await,
        DataRequest::Merge(req) => merge(db, req).await,
        DataRequest::ReplacePartition(req) => {
            let _ = req;
            Err(db.unsupported(
                "ATOMIC_PARTITION_REPLACE",
                "Qdrant Edge and REST do not provide rollback-capable scoped partition replacement",
            ))
        }
        DataRequest::Count(req) => count(db, req).await,
    }
}

async fn insert(db: &QdrantManager, req: InsertRequest) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("insert")?;
    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    Ok(DataResponse::Mutation(
        target.write_records(&req.records, req.mode.into()).await?,
    ))
}

async fn lookup(db: &QdrantManager, req: LookupRequest) -> Result<DataResponse, StorageError> {
    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    let vector_selection = target.vector_selection_plan(&req.projection).await?;
    Ok(DataResponse::Records(
        target
            .retrieve(&req.ids, &req.projection, &vector_selection)
            .await?,
    ))
}

async fn scan(db: &QdrantManager, req: ScanRequest) -> Result<DataResponse, StorageError> {
    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    let vector_selection = target.vector_selection_plan(&req.projection).await?;
    Ok(DataResponse::Records(
        target.scroll(&req, &vector_selection).await?,
    ))
}

async fn update(db: &QdrantManager, req: UpdateRequest) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("update")?;
    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    let mut payload = serde_json::Map::new();
    for assignment in &req.assignments {
        let (key, value) = mapper::literal_assignment(assignment)?;
        payload.insert(key, value);
    }

    Ok(DataResponse::Mutation(
        target.update_payload(req.filter.as_ref(), payload).await?,
    ))
}

async fn delete(db: &QdrantManager, req: DeleteRequest) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("delete")?;
    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    Ok(DataResponse::Mutation(
        target.delete_filtered(&req.filter).await?,
    ))
}

async fn merge(db: &QdrantManager, req: MergeRequest) -> Result<DataResponse, StorageError> {
    db.ensure_mutable("merge")?;
    if !req.on.is_empty() && req.on != ["id"] {
        return Err(db.unsupported(
            "MERGE_COMPLEX",
            "Qdrant common adapter only supports id-based upsert style merges",
        ));
    }
    if req.actions != crate::MergeActions::default() {
        return Err(db.unsupported(
            "MERGE_ACTIONS",
            "Qdrant merge supports only default id-based upsert actions; custom MergeActions are unsupported",
        ));
    }

    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    Ok(DataResponse::Mutation(
        target
            .write_records(&req.records, QdrantWrite::Merge)
            .await?,
    ))
}

async fn count(db: &QdrantManager, req: crate::CountRequest) -> Result<DataResponse, StorageError> {
    let target = QdrantCollectionTarget::new(db, &req.locator)?;
    Ok(DataResponse::Count(
        target.count(req.filter.as_ref()).await?,
    ))
}
