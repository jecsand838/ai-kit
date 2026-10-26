use crate::core::CreateCollectionAction;
use crate::{
    CatalogRequest, CatalogResponse, CreateCollectionRequest, ListCollectionsRequest,
    QdrantModeConfig, StorageError,
};

use super::{manager::QdrantManager, mapper};

pub(super) async fn handle(
    db: &QdrantManager,
    request: CatalogRequest,
) -> Result<CatalogResponse, StorageError> {
    match request {
        CatalogRequest::CreateCollection(request) => create_collection(db, request).await,
        CatalogRequest::OpenCollection { locator } => Ok(CatalogResponse::Collection(
            db.collection_info(locator).await?,
        )),
        CatalogRequest::ListCollections(request) => list_collections(db, request).await,
        CatalogRequest::DropCollection {
            locator,
            ignore_missing,
        } => {
            db.drop_collection(locator, ignore_missing).await?;
            Ok(CatalogResponse::Completed)
        }
        CatalogRequest::AlterCollection(request) => Err(db.unsupported(
            "SCHEMA_EVOLUTION",
            format!("Qdrant collection schema alteration is not portable: {request:?}"),
        )),
    }
}

async fn create_collection(
    db: &QdrantManager,
    request: CreateCollectionRequest,
) -> Result<CatalogResponse, StorageError> {
    db.ensure_mutable("create_collection")?;
    let collection_name = db.resolve_collection(&request.locator)?;
    let exists = db.collection_exists(&request.locator).await?;
    let recreate = match request.mode.resolve(exists) {
        CreateCollectionAction::ErrorExists => {
            return Err(request.already_exists_error("collection"));
        }
        CreateCollectionAction::Reuse => {
            return Ok(CatalogResponse::Collection(
                db.collection_info(request.locator).await?,
            ));
        }
        CreateCollectionAction::Recreate => true,
        CreateCollectionAction::Create => false,
    };

    let prepared = QdrantCollectionCreation::prepare(db, &request)?;
    if recreate {
        db.drop_collection(request.locator.clone(), false).await?;
    }
    prepared.create(db, collection_name).await?;
    Ok(CatalogResponse::Collection(
        db.collection_info(request.locator).await?,
    ))
}

async fn list_collections(
    db: &QdrantManager,
    request: ListCollectionsRequest,
) -> Result<CatalogResponse, StorageError> {
    Ok(CatalogResponse::Collections(
        db.list_collection_infos(&request).await?,
    ))
}

// Native creation inputs are fully encoded before an existing collection may be
// removed. Runtime/native creation failures still have their native semantics;
// this is not an emulated cross-request transaction.
enum QdrantCollectionCreation {
    Edge {
        config: Box<qdrant_edge::EdgeConfig>,
        points: Vec<qdrant_edge::PointStructPersisted>,
    },
    RemoteRest {
        body: serde_json::Value,
        points: Vec<serde_json::Value>,
    },
}

impl QdrantCollectionCreation {
    fn prepare(
        db: &QdrantManager,
        request: &CreateCollectionRequest,
    ) -> Result<Self, StorageError> {
        let profile = mapper::infer_collection_profile(request.schema.as_ref(), &request.records)?;
        profile.validate_records(&request.records)?;
        match &db.config().mode {
            QdrantModeConfig::Edge { .. } => {
                let config = Box::new(profile.edge_config());
                let points = request
                    .records
                    .iter()
                    .map(|record| {
                        let id = record.id.as_ref().ok_or_else(|| {
                            StorageError::InvalidConfig("Qdrant edge insert requires ids".into())
                        })?;
                        Ok(qdrant_edge::PointStructPersisted {
                            id: mapper::encode_point_id_for_edge(id, db)?,
                            vector: mapper::encode_vectors_for_edge(db, &record.vectors)?,
                            payload: Some(qdrant_edge::Payload(record.fields.clone())),
                        })
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?;
                Ok(Self::Edge { config, points })
            }
            QdrantModeConfig::RemoteRest { .. } => Ok(Self::RemoteRest {
                body: mapper::build_remote_collection_body(&profile)?,
                points: request
                    .records
                    .iter()
                    .map(|record| mapper::encode_point(db, record))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        }
    }

    async fn create(self, db: &QdrantManager, collection: String) -> Result<(), StorageError> {
        match self {
            Self::Edge { config, points } => {
                let edge = db.edge_state()?;
                let path = edge.collection_path(&collection);
                tokio::fs::create_dir_all(&path).await?;
                let shard = qdrant_edge::EdgeShard::new(&path, *config)
                    .map_err(|error| db.map_err("edge_create_collection", error))?;
                if !points.is_empty() {
                    shard
                        .update(qdrant_edge::UpdateOperation::PointOperation(
                            qdrant_edge::PointOperations::UpsertPoints(
                                qdrant_edge::PointInsertOperations::PointsList(points),
                            ),
                        ))
                        .map_err(|error| db.map_err("edge_seed_collection", error))?;
                }
                edge.insert_created(collection, shard)?;
            }
            Self::RemoteRest { body, points } => {
                db.rest_client()?
                    .request_operation_ack(
                        reqwest::Method::PUT,
                        &format!("/collections/{collection}"),
                        Some(body),
                    )
                    .await?;
                if !points.is_empty() {
                    db.rest_client()?
                        .request_applied_mutation(
                            reqwest::Method::PUT,
                            &format!("/collections/{collection}/points"),
                            Some(serde_json::json!({"points": points, "update_mode":"upsert"})),
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }
}
