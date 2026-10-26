use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

use crate::{
    DataRecord, MutationResult, PageToken, Projection, QdrantModeConfig, RecordId, ScanRequest,
    StorageError, StorageLocator, WriteMode,
};

use super::{
    manager::{EdgeCollectionHandle, QdrantManager},
    mapper,
};

pub(super) enum QdrantCollectionTarget<'a> {
    Edge(EdgeTarget<'a>),
    RemoteRest(RemoteRestTarget<'a>),
}

pub(super) struct EdgeTarget<'a> {
    db: &'a QdrantManager,
    handle: EdgeCollectionHandle,
}

pub(super) struct RemoteRestTarget<'a> {
    db: &'a QdrantManager,
    collection: String,
}

impl<'a> QdrantCollectionTarget<'a> {
    pub(super) fn new(
        db: &'a QdrantManager,
        locator: &StorageLocator,
    ) -> Result<Self, StorageError> {
        match &db.config().mode {
            QdrantModeConfig::Edge { .. } => Ok(Self::Edge(EdgeTarget {
                db,
                handle: db.edge_collection(locator)?,
            })),
            QdrantModeConfig::RemoteRest { .. } => Ok(Self::RemoteRest(RemoteRestTarget {
                db,
                collection: db.resolve_collection(locator)?,
            })),
        }
    }

    pub(super) fn db(&self) -> &QdrantManager {
        match self {
            Self::Edge(target) => target.db,
            Self::RemoteRest(target) => target.db,
        }
    }

    pub(super) fn collection(&self) -> &str {
        match self {
            Self::Edge(target) => target.handle.collection(),
            Self::RemoteRest(target) => &target.collection,
        }
    }

    pub(super) async fn write_records(
        &self,
        records: &[DataRecord],
        mode: QdrantWrite,
    ) -> Result<MutationResult, StorageError> {
        match self {
            Self::Edge(target) => target.write_records(records, mode),
            Self::RemoteRest(target) => target.write_records(records, mode).await,
        }
    }

    pub(super) async fn vector_selection_plan(
        &self,
        projection: &Projection,
    ) -> Result<mapper::VectorSelectionPlan, StorageError> {
        match self {
            Self::Edge(target) => target.vector_selection_plan(projection),
            Self::RemoteRest(target) => target.vector_selection_plan(projection).await,
        }
    }

    pub(super) async fn retrieve(
        &self,
        ids: &[RecordId],
        projection: &Projection,
        vector_selection: &mapper::VectorSelectionPlan,
    ) -> Result<crate::RecordsPage, StorageError> {
        match self {
            Self::Edge(target) => target.retrieve(ids, projection, vector_selection),
            Self::RemoteRest(target) => target.retrieve(ids, projection, vector_selection).await,
        }
    }

    pub(super) async fn scroll(
        &self,
        req: &ScanRequest,
        vector_selection: &mapper::VectorSelectionPlan,
    ) -> Result<crate::RecordsPage, StorageError> {
        match self {
            Self::Edge(target) => target.scroll(req, vector_selection),
            Self::RemoteRest(target) => target.scroll(req, vector_selection).await,
        }
    }

    pub(super) async fn count(
        &self,
        filter: Option<&crate::FilterExpr>,
    ) -> Result<u64, StorageError> {
        match self {
            Self::Edge(target) => target.count(filter),
            Self::RemoteRest(target) => target.count(filter).await,
        }
    }

    pub(super) async fn update_payload(
        &self,
        filter: Option<&crate::FilterExpr>,
        payload: Map<String, Value>,
    ) -> Result<MutationResult, StorageError> {
        match self {
            Self::Edge(target) => target.update_payload(filter, payload),
            Self::RemoteRest(target) => target.update_payload(filter, payload).await,
        }
    }

    pub(super) async fn delete_filtered(
        &self,
        filter: &crate::FilterExpr,
    ) -> Result<MutationResult, StorageError> {
        match self {
            Self::Edge(target) => target.delete_filtered(filter),
            Self::RemoteRest(target) => target.delete_filtered(filter).await,
        }
    }
}

impl EdgeTarget<'_> {
    pub(super) fn shard(&self) -> &qdrant_edge::EdgeShard {
        self.handle.shard()
    }

    fn write_records(
        &self,
        records: &[DataRecord],
        mode: QdrantWrite,
    ) -> Result<MutationResult, StorageError> {
        let shard = self.handle.shard();
        mode.edge(self.db, shard, records)?;
        Ok(MutationResult::no_counts())
    }

    pub(super) fn vector_selection_plan(
        &self,
        projection: &Projection,
    ) -> Result<mapper::VectorSelectionPlan, StorageError> {
        edge_vector_selection_plan(self.db, self.handle.shard(), projection)
    }

    fn retrieve(
        &self,
        ids: &[RecordId],
        projection: &Projection,
        vector_selection: &mapper::VectorSelectionPlan,
    ) -> Result<crate::RecordsPage, StorageError> {
        let with_vector = Some(vector_selection.with_vector_for_edge());
        let point_ids = ids
            .iter()
            .map(|id| mapper::encode_point_id_for_edge(id, self.db))
            .collect::<Result<Vec<_>, StorageError>>()?;
        let with_payload = Some(mapper::projection_selector_for_edge(projection)?);
        let records = self
            .handle
            .shard()
            .retrieve(&point_ids, with_payload, with_vector)
            .map_err(|err| self.db.map_err("edge_retrieve", err))?;
        mapper::decode_edge_records_page(self.db, &records, vector_selection, None)
    }

    fn scroll(
        &self,
        req: &ScanRequest,
        vector_selection: &mapper::VectorSelectionPlan,
    ) -> Result<crate::RecordsPage, StorageError> {
        let fetch_limit = req.fetch_limit(50);
        let request = qdrant_edge::ScrollRequest {
            offset: req
                .page_token
                .as_deref()
                .map(mapper::parse_page_token_for_edge)
                .transpose()?,
            limit: Some(fetch_limit),
            filter: mapper::compile_filter_for_edge(req.filter.as_ref())?,
            with_payload: Some(mapper::projection_selector_for_edge(&req.projection)?),
            with_vector: vector_selection.with_vector_for_edge(),
            order_by: None,
        };
        let (records, next_page) = self
            .handle
            .shard()
            .scroll(request)
            .map_err(|err| self.db.map_err("edge_scroll", err))?;
        let records = req.apply_window(records, 50);
        let next_page = next_page.map(|value| match value {
            qdrant_edge::PointId::NumId(number) => PageToken::new(number.to_string()),
            qdrant_edge::PointId::Uuid(uuid) => PageToken::new(uuid.to_string()),
        });
        mapper::decode_edge_records_page(self.db, &records, vector_selection, next_page)
    }

    fn count(&self, filter: Option<&crate::FilterExpr>) -> Result<u64, StorageError> {
        let request = qdrant_edge::CountRequest {
            filter: mapper::compile_filter_for_edge(filter)?,
            exact: true,
        };
        let count = self
            .handle
            .shard()
            .count(request)
            .map_err(|err| self.db.map_err("edge_count", err))?;
        Ok(count as u64)
    }

    fn update_payload(
        &self,
        filter: Option<&crate::FilterExpr>,
        payload: Map<String, Value>,
    ) -> Result<MutationResult, StorageError> {
        let op = qdrant_edge::UpdateOperation::PayloadOperation(
            qdrant_edge::PayloadOps::SetPayload(qdrant_edge::SetPayloadOp {
                payload: qdrant_edge::Payload(payload),
                points: None,
                filter: Some(mapper::compile_filter_for_edge(filter)?.unwrap_or_default()),
                key: None,
            }),
        );
        self.handle
            .shard()
            .update(op)
            .map_err(|err| self.db.map_err("edge_set_payload", err))?;
        Ok(MutationResult::no_counts())
    }

    fn delete_filtered(&self, filter: &crate::FilterExpr) -> Result<MutationResult, StorageError> {
        let filter = mapper::compile_filter_for_edge(Some(filter))?.ok_or_else(|| {
            StorageError::Serialization("Qdrant edge delete requires a compiled filter".into())
        })?;
        let op = qdrant_edge::UpdateOperation::PointOperation(
            qdrant_edge::PointOperations::DeletePointsByFilter(filter),
        );
        self.handle
            .shard()
            .update(op)
            .map_err(|err| self.db.map_err("edge_delete", err))?;
        Ok(MutationResult::no_counts())
    }
}

impl RemoteRestTarget<'_> {
    fn path(&self, suffix: &str) -> String {
        format!("/collections/{}{}", self.collection, suffix)
    }

    async fn write_records(
        &self,
        records: &[DataRecord],
        mode: QdrantWrite,
    ) -> Result<MutationResult, StorageError> {
        mode.remote(self.db, &self.collection, records).await?;
        Ok(MutationResult::no_counts())
    }

    async fn vector_selection_plan(
        &self,
        projection: &Projection,
    ) -> Result<mapper::VectorSelectionPlan, StorageError> {
        let raw = self.db.remote_collection_raw(&self.collection).await?;
        mapper::VectorSelectionPlan::for_remote(self.db, projection, &raw)
    }

    async fn retrieve(
        &self,
        ids: &[RecordId],
        projection: &Projection,
        vector_selection: &mapper::VectorSelectionPlan,
    ) -> Result<crate::RecordsPage, StorageError> {
        let points = self
            .db
            .rest_client()?
            .request_read_result_array(
                reqwest::Method::POST,
                &self.path("/points"),
                Some(json!({
                    "ids": ids.iter().map(|id| mapper::encode_point_id(id, self.db)).collect::<Result<Vec<_>, StorageError>>()?,
                    "with_payload": mapper::compile_projection_selector(projection),
                    "with_vector": vector_selection.with_vector(),
                })),
            )
            .await?;
        mapper::decode_records_page(self.db, &points, vector_selection, None)
    }

    async fn scroll(
        &self,
        req: &ScanRequest,
        vector_selection: &mapper::VectorSelectionPlan,
    ) -> Result<crate::RecordsPage, StorageError> {
        let fetch_limit = req.fetch_limit(50);
        let mut body = json!({
            "filter": mapper::compile_filter(req.filter.as_ref())?,
            "limit": fetch_limit,
            "with_payload": mapper::compile_projection_selector(&req.projection),
            "with_vector": vector_selection.with_vector(),
        });
        if let Some(page_token) = req.page_token.as_deref() {
            body["offset"] = mapper::parse_page_token_value(page_token);
        }
        let result = self
            .db
            .rest_client()?
            .request_read_result_value(
                reqwest::Method::POST,
                &self.path("/points/scroll"),
                Some(body),
            )
            .await?;
        let mut points = remote_scroll_points(&result)?;
        points = req.apply_window(points, 50);
        let next_page = remote_next_page_token(&result)?;
        mapper::decode_records_page(self.db, &points, vector_selection, next_page)
    }

    async fn count(&self, filter: Option<&crate::FilterExpr>) -> Result<u64, StorageError> {
        let result = self
            .db
            .rest_client()?
            .request_read_result_value(
                reqwest::Method::POST,
                &self.path("/points/count"),
                Some(json!({ "filter": mapper::compile_filter(filter)?, "exact": true })),
            )
            .await?;
        remote_count(&result)
    }

    async fn update_payload(
        &self,
        filter: Option<&crate::FilterExpr>,
        payload: Map<String, Value>,
    ) -> Result<MutationResult, StorageError> {
        self.db
            .rest_client()?
            .request_applied_mutation(
                reqwest::Method::POST,
                &self.path("/points/payload"),
                Some(json!({
                    "filter": mapper::compile_filter(filter)?.unwrap_or_else(|| json!({})),
                    "payload": payload,
                })),
            )
            .await?;
        Ok(MutationResult::no_counts())
    }

    async fn delete_filtered(
        &self,
        filter: &crate::FilterExpr,
    ) -> Result<MutationResult, StorageError> {
        self.db
            .rest_client()?
            .request_applied_mutation(
                reqwest::Method::POST,
                &self.path("/points/delete"),
                Some(json!({ "filter": mapper::compile_filter(Some(filter))? })),
            )
            .await?;
        Ok(MutationResult::no_counts())
    }
}

#[derive(Clone, Copy)]
pub(super) enum QdrantWrite {
    Append,
    Merge,
    Replace,
}

impl From<WriteMode> for QdrantWrite {
    fn from(mode: WriteMode) -> Self {
        match mode {
            WriteMode::Append => Self::Append,
            WriteMode::Overwrite => Self::Replace,
        }
    }
}

impl QdrantWrite {
    async fn remote(
        self,
        db: &QdrantManager,
        collection: &str,
        records: &[DataRecord],
    ) -> Result<(), StorageError> {
        // Encode the complete request before any mutation. Schema inspection is
        // read-only and validates dimensions against the existing collection.
        let points = records
            .iter()
            .map(|record| mapper::encode_point(db, record))
            .collect::<Result<Vec<_>, _>>()?;
        let raw = db.remote_collection_raw(collection).await?;
        mapper::VectorProfile::for_remote(&raw)?.validate_records(records)?;
        let ids = records
            .iter()
            .map(|record| {
                let id = record.id.as_ref().ok_or_else(|| {
                    StorageError::InvalidConfig("Qdrant records require ids".into())
                })?;
                mapper::encode_point_id(id, db)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !points.is_empty() {
            let update_mode = match self {
                Self::Append => "insert_only",
                Self::Merge | Self::Replace => "upsert",
            };
            db.rest_client()?
                .request_applied_mutation(
                    reqwest::Method::PUT,
                    &format!("/collections/{collection}/points"),
                    Some(json!({"points": points, "update_mode": update_mode})),
                )
                .await?;
        }
        if matches!(self, Self::Replace) {
            // Never prune after a failed or unconfirmed upsert. This native
            // two-request replacement does not promise transactional rollback.
            let filter = if ids.is_empty() {
                json!({})
            } else {
                json!({"must_not":[{"has_id": ids}]})
            };
            db.rest_client()?
                .request_applied_mutation(
                    reqwest::Method::POST,
                    &format!("/collections/{collection}/points/delete"),
                    Some(json!({"filter": filter})),
                )
                .await?;
        }
        Ok(())
    }

    fn edge(
        self,
        db: &QdrantManager,
        shard: &qdrant_edge::EdgeShard,
        records: &[DataRecord],
    ) -> Result<(), StorageError> {
        mapper::VectorProfile::for_edge(shard)?.validate_records(records)?;
        let points = records
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
        let ids = points
            .iter()
            .map(|point| point.id)
            .collect::<std::collections::HashSet<_>>();
        if !points.is_empty() {
            let points_op = qdrant_edge::PointInsertOperations::PointsList(points);
            let operation = match self {
                Self::Append => qdrant_edge::PointOperations::UpsertPointsConditional(
                    qdrant_edge::ConditionalInsertOperation {
                        points_op,
                        condition: qdrant_edge::Filter::default(),
                        update_mode: Some(qdrant_edge::UpdateMode::InsertOnly),
                    },
                ),
                Self::Merge | Self::Replace => {
                    qdrant_edge::PointOperations::UpsertPoints(points_op)
                }
            };
            shard
                .update(qdrant_edge::UpdateOperation::PointOperation(operation))
                .map_err(|error| db.map_err("edge_write", error))?;
        }
        if matches!(self, Self::Replace) {
            let filter = if ids.is_empty() {
                qdrant_edge::Filter::default()
            } else {
                qdrant_edge::Filter {
                    must_not: Some(vec![qdrant_edge::Condition::HasId(
                        ids.into_iter().collect(),
                    )]),
                    ..Default::default()
                }
            };
            shard
                .update(qdrant_edge::UpdateOperation::PointOperation(
                    qdrant_edge::PointOperations::DeletePointsByFilter(filter),
                ))
                .map_err(|error| db.map_err("edge_replace_prune", error))?;
        }
        Ok(())
    }
}

fn edge_vector_selection_plan(
    db: &QdrantManager,
    shard: &qdrant_edge::EdgeShard,
    projection: &Projection,
) -> Result<mapper::VectorSelectionPlan, StorageError> {
    let config = shard.config();
    let sparse_names = config
        .sparse_vectors
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if mapper::edge_uses_unnamed_default_dense(shard)
        && edge_projection_requests_vectors(db, projection, &sparse_names)
    {
        return Err(mapper::edge_unnamed_default_vector_unsupported(
            "vector projection",
        ));
    }
    let names = config
        .vectors
        .keys()
        .cloned()
        .chain(sparse_names)
        .collect::<BTreeSet<_>>();
    Ok(mapper::VectorSelectionPlan::for_edge(projection, &names))
}

fn edge_projection_requests_vectors(
    db: &QdrantManager,
    projection: &Projection,
    sparse_names: &BTreeSet<String>,
) -> bool {
    match projection {
        Projection::All => true,
        Projection::Columns(columns) => {
            let configured_default = db.config().default_vector_name.as_deref();
            columns.iter().any(|name| {
                name.is_empty()
                    || name == "vector"
                    || configured_default.is_some_and(|default| name == default)
                    || sparse_names.contains(name)
            })
        }
    }
}

fn remote_scroll_points(result: &Value) -> Result<Vec<Value>, StorageError> {
    result
        .get("points")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| {
            StorageError::Serialization(format!(
                "Qdrant scroll response is missing `points`: {result}"
            ))
        })
}

fn remote_next_page_token(result: &Value) -> Result<Option<PageToken>, StorageError> {
    match result.get("next_page_offset") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(PageToken::new(text))),
        Some(Value::Number(number)) => Ok(Some(PageToken::new(number.to_string()))),
        Some(other) => Err(StorageError::Serialization(format!(
            "Qdrant scroll response has unsupported `next_page_offset`: {other}"
        ))),
    }
}

fn remote_count(result: &Value) -> Result<u64, StorageError> {
    result.get("count").and_then(Value::as_u64).ok_or_else(|| {
        StorageError::Serialization(format!(
            "Qdrant count response is missing numeric `count`: {result}"
        ))
    })
}
