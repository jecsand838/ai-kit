use serde_json::Value;

use crate::{
    CollectionInfo, IndexInfo, ListCollectionsRequest, QdrantModeConfig, StorageError,
    StorageLocator,
};

use super::{
    manager::{EdgeCollectionHandle, EdgeState, QdrantConnection, QdrantManager},
    rest::RestQdrantClient,
};

impl QdrantManager {
    pub(crate) fn resolve_collection(
        &self,
        locator: &StorageLocator,
    ) -> Result<String, StorageError> {
        if locator.collection.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "Qdrant operations require a collection name".into(),
            ));
        }
        let mut pieces = locator.namespace.clone();
        if let Some(database) = &locator.database {
            pieces.push(database.clone());
        }
        pieces.push(locator.collection.clone());
        Ok(pieces.join("__"))
    }

    pub(crate) fn default_vector_name(
        &self,
        override_name: Option<&String>,
    ) -> Result<String, StorageError> {
        if let Some(name) = override_name {
            if name.trim().is_empty() {
                return Err(self.unsupported(
                    "UNNAMED_DEFAULT_VECTOR",
                    "Qdrant request vector_field cannot honestly address the unnamed default dense vector",
                ));
            }
            return Ok(name.clone());
        }

        self.config().default_vector_name.clone().ok_or_else(|| {
            self.unsupported(
                "VECTOR_FIELD_SELECTION",
                "Qdrant operations that address a named vector require an explicit request vector_field or QdrantConfig.default_vector_name",
            )
        })
    }

    pub(crate) fn rest_client(&self) -> Result<&RestQdrantClient, StorageError> {
        match self.connection() {
            QdrantConnection::RemoteRest(client) => Ok(client),
            QdrantConnection::Edge(_) => Err(self.unsupported(
                "REST_REMOTE",
                format!(
                    "mode `{}` does not expose a REST client",
                    self.backend_mode()
                ),
            )),
        }
    }

    pub(super) fn edge_state(&self) -> Result<&EdgeState, StorageError> {
        match self.connection() {
            QdrantConnection::Edge(state) => Ok(state),
            QdrantConnection::RemoteRest(_) => Err(self.unsupported(
                "EDGE",
                format!(
                    "mode `{}` does not expose an edge shard",
                    self.backend_mode()
                ),
            )),
        }
    }

    pub(super) fn edge_collection(
        &self,
        locator: &StorageLocator,
    ) -> Result<EdgeCollectionHandle, StorageError> {
        self.edge_state()?
            .collection(self.resolve_collection(locator)?)
    }

    pub(crate) async fn collection_exists(
        &self,
        locator: &StorageLocator,
    ) -> Result<bool, StorageError> {
        let collection = self.resolve_collection(locator)?;
        match self.connection() {
            QdrantConnection::Edge(state) => Ok(state.collection_path(&collection).exists()),
            QdrantConnection::RemoteRest(_) => {
                let result = self
                    .rest_client()?
                    .request_read_result_value(
                        reqwest::Method::GET,
                        &format!("/collections/{collection}/exists"),
                        None,
                    )
                    .await?;
                result
                    .get("exists")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| {
                        StorageError::backend_op(
                            "collection_exists",
                            format!(
                                "unexpected response shape from `/collections/{collection}/exists`: {result}"
                            ),
                        )
                    })
            }
        }
    }

    pub(crate) async fn remote_collection_raw(
        &self,
        collection: &str,
    ) -> Result<Value, StorageError> {
        self.rest_client()?
            .request_read_envelope_value(
                reqwest::Method::GET,
                &format!("/collections/{collection}"),
                None,
            )
            .await
    }

    pub(crate) async fn collection_info_json(
        &self,
        locator: &StorageLocator,
    ) -> Result<Value, StorageError> {
        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => Err(self.unsupported(
                "EDGE_COLLECTION_RAW_INFO",
                "qdrant-edge does not expose REST-shaped raw collection info",
            )),
            QdrantModeConfig::RemoteRest { .. } => {
                let collection = self.resolve_collection(locator)?;
                self.remote_collection_raw(&collection).await
            }
        }
    }

    pub(crate) async fn remote_collections_raw(&self) -> Result<Value, StorageError> {
        self.rest_client()?
            .request_read_envelope_value(reqwest::Method::GET, "/collections", None)
            .await
    }

    pub(crate) async fn delete_remote_collection(
        &self,
        collection: &str,
    ) -> Result<(), StorageError> {
        let _ = self
            .rest_client()?
            .request_operation_ack(
                reqwest::Method::DELETE,
                &format!("/collections/{collection}"),
                None,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn collection_info(
        &self,
        locator: StorageLocator,
    ) -> Result<CollectionInfo, StorageError> {
        let physical_name = self.resolve_collection(&locator)?;
        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => {
                let collection = self.edge_collection(&locator)?;
                let info = collection.shard().info();
                Ok(super::mapper::edge_shard_collection_info(
                    locator,
                    physical_name,
                    collection.path(),
                    &info,
                ))
            }
            QdrantModeConfig::RemoteRest { .. } => {
                let raw = self.collection_info_json(&locator).await?;
                Ok(super::mapper::qdrant_collection_info(
                    self,
                    locator,
                    physical_name,
                    &raw,
                ))
            }
        }
    }

    pub(crate) async fn list_collection_infos(
        &self,
        req: &ListCollectionsRequest,
    ) -> Result<Vec<CollectionInfo>, StorageError> {
        if !req.namespace.is_empty() {
            return Err(self.unsupported(
                "NAMESPACES",
                format!(
                    "Qdrant collection listing cannot scope to logical namespace {:?} because flattened physical collection names are not losslessly invertible",
                    req.namespace
                ),
            ));
        }

        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => {
                let root = &self.edge_state()?.root;
                let mut entries = tokio::fs::read_dir(root).await?;
                let mut collections = Vec::new();
                while let Some(entry) = entries.next_entry().await? {
                    if entry.file_type().await?.is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        collections.push(super::mapper::edge_directory_collection_info(
                            &entry.path(),
                            name,
                        ));
                    }
                }
                Ok(req.apply_to_collections(collections))
            }
            QdrantModeConfig::RemoteRest { .. } => {
                let raw = self.remote_collections_raw().await?;
                Ok(req.apply_to_collections(super::mapper::remote_collection_infos(self, &raw)?))
            }
        }
    }

    pub(crate) async fn list_index_infos(
        &self,
        locator: &StorageLocator,
    ) -> Result<Vec<IndexInfo>, StorageError> {
        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => Err(self.unsupported(
                "LIST_INDEXES",
                format!(
                    "Qdrant edge does not expose a portable payload-index inventory for collection `{}`",
                    locator.collection
                ),
            )),
            QdrantModeConfig::RemoteRest { .. } => {
                let collection = self.resolve_collection(locator)?;
                let raw = self.remote_collection_raw(&collection).await?;
                super::mapper::remote_index_infos(self, &raw)
            }
        }
    }

    pub(crate) async fn drop_collection(
        &self,
        locator: StorageLocator,
        ignore_missing: bool,
    ) -> Result<(), StorageError> {
        self.ensure_mutable("drop_collection")?;
        let collection = self.resolve_collection(&locator)?;
        let exists = self.collection_exists(&locator).await?;
        if !exists {
            return if ignore_missing {
                Ok(())
            } else {
                Err(StorageError::backend_op(
                    "drop_collection",
                    format!("collection `{collection}` not found"),
                ))
            };
        }

        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => {
                let state = self.edge_state()?;
                state.invalidate(&collection)?;
                let path = state.collection_path(&collection);
                tokio::fs::remove_dir_all(&path).await?;
            }
            QdrantModeConfig::RemoteRest { .. } => {
                if let Err(err) = self.delete_remote_collection(&collection).await {
                    if ignore_missing && !self.collection_exists(&locator).await? {
                        return Ok(());
                    }
                    return Err(err);
                }
            }
        }

        Ok(())
    }
}
