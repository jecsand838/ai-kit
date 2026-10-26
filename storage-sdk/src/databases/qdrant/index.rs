use std::time::Duration;

use serde_json::{Value, json};

use crate::{
    CreateIndexRequest, IndexResponse, QdrantModeConfig, StorageError, StorageLocator,
    WaitForIndexRequest,
};

use super::{manager::QdrantManager, mapper};

impl QdrantManager {
    pub(super) async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> Result<IndexResponse, StorageError> {
        self.ensure_mutable("create_index")?;
        let collection = self.resolve_collection(&request.locator)?;
        let column = request.columns.first().cloned().ok_or_else(|| {
            StorageError::InvalidConfig("Qdrant index creation requires one field".into())
        })?;
        if request.columns.len() != 1 {
            return Err(StorageError::InvalidConfig(
                "Qdrant index creation requires exactly one field".into(),
            ));
        }
        if matches!(&request.kind, crate::IndexKind::Vector { .. }) {
            return Err(self.unsupported(
                "VECTOR_INDEX",
                "Qdrant vector indexes are configured when the collection is created",
            ));
        }

        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => {
                if matches!(&request.kind, crate::IndexKind::FullText) {
                    return Err(self.unsupported(
                        "FULL_TEXT_INDEX",
                        "qdrant-edge does not expose portable full-text payload indexes",
                    ));
                }
                let field_name = column.parse().map_err(|error| {
                    StorageError::Serialization(format!(
                        "invalid Qdrant edge field `{column}`: {error:?}"
                    ))
                })?;
                let operation = qdrant_edge::UpdateOperation::FieldIndexOperation(
                    qdrant_edge::FieldIndexOperations::CreateIndex(qdrant_edge::CreateIndex {
                        field_name,
                        field_schema: Some(mapper::field_schema_for_edge(&request.kind)?),
                    }),
                );
                self.edge_collection(&request.locator)?
                    .shard()
                    .update(operation)
                    .map_err(|error| self.map_err("edge_create_index", error))?;
            }
            QdrantModeConfig::RemoteRest { .. } => {
                self.rest_client()?
                    .request_applied_mutation(
                        reqwest::Method::PUT,
                        &format!("/collections/{collection}/index"),
                        Some(json!({
                            "field_name": column,
                            "field_schema": mapper::build_remote_field_schema(&request)?,
                        })),
                    )
                    .await?;
            }
        }
        Ok(IndexResponse::Completed)
    }

    pub(super) async fn drop_index(
        &self,
        locator: StorageLocator,
        name: String,
    ) -> Result<IndexResponse, StorageError> {
        self.ensure_mutable("drop_index")?;
        match &self.config().mode {
            QdrantModeConfig::Edge { .. } => Err(self.unsupported(
                "DROP_INDEX",
                "qdrant-edge does not expose payload-index deletion",
            )),
            QdrantModeConfig::RemoteRest { .. } => {
                let collection = self.resolve_collection(&locator)?;
                self.rest_client()?
                    .request_applied_mutation(
                        reqwest::Method::DELETE,
                        &format!("/collections/{collection}/index/{name}"),
                        None,
                    )
                    .await?;
                Ok(IndexResponse::Completed)
            }
        }
    }

    pub(super) async fn wait_for_index(
        &self,
        request: WaitForIndexRequest,
    ) -> Result<IndexResponse, StorageError> {
        tokio::time::timeout(Duration::from_millis(request.timeout_ms), async {
            let missing = match &self.config().mode {
                QdrantModeConfig::Edge { .. } => {
                    let info = self.edge_collection(&request.locator)?.shard().info();
                    request
                        .index_names
                        .iter()
                        .filter(|name| {
                            name.parse::<qdrant_edge::JsonPath>()
                                .ok()
                                .is_none_or(|key| !info.payload_schema.contains_key(&key))
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                }
                QdrantModeConfig::RemoteRest { .. } => {
                    let raw = self.collection_info_json(&request.locator).await?;
                    request
                        .index_names
                        .iter()
                        .filter(|name| {
                            !raw.pointer("/result/payload_schema")
                                .and_then(Value::as_object)
                                .is_some_and(|schema| schema.contains_key(*name))
                        })
                        .cloned()
                        .collect::<Vec<_>>()
                }
            };
            if !missing.is_empty() {
                return Err(StorageError::backend_op(
                    "wait_for_index",
                    format!("indexes are not ready: {missing:?}"),
                ));
            }
            Ok(IndexResponse::Completed)
        })
        .await
        .map_err(|_| {
            self.map_err(
                "wait_for_index",
                format!(
                    "timed out after {} ms waiting for {:?}",
                    request.timeout_ms, request.index_names
                ),
            )
        })?
    }
}
