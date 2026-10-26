use crate::{
    DataRequest, DataResponse, QdrantModeConfig, QueryResponse, RandomSampleRequest, ScoreKind,
    ScoredRecord, ScrollRequest, StorageError,
};

use super::super::collection::QdrantCollectionTarget;
use super::super::data;
use super::QdrantManager;

pub(super) async fn scroll(
    db: &QdrantManager,
    request: ScrollRequest,
) -> Result<QueryResponse, StorageError> {
    let page = match data::handle(
        db,
        DataRequest::Scan(crate::ScanRequest {
            locator: request.locator,
            filter: request.filter,
            projection: request.projection,
            limit: request.limit,
            offset: request.offset,
            page_token: request.page_token,
        }),
    )
    .await?
    {
        DataResponse::Records(page) => page,
        other => {
            return Err(StorageError::backend_op(
                "scroll_query",
                format!("unexpected scan response: {other:?}"),
            ));
        }
    };

    Ok(QueryResponse::from_hits(
        page.records.into_iter().map(unknown_hit).collect(),
        page.next_page,
    ))
}

pub(super) async fn random_sample(
    db: &QdrantManager,
    request: RandomSampleRequest,
) -> Result<QueryResponse, StorageError> {
    match &db.config().mode {
        QdrantModeConfig::Edge { .. } => Err(StorageError::unsupported_capability(
            "RANDOM_SAMPLE",
            "qdrant-edge does not expose random sampling",
        )),
        QdrantModeConfig::RemoteRest { .. } => {
            let target = QdrantCollectionTarget::new(db, &request.locator)?;
            super::shared::execute_remote_query(
                &target,
                super::shared::RemoteQuerySpec {
                    projection: &request.projection,
                    vector_field: None,
                    query: serde_json::json!({ "sample": "random" }),
                    filter: request.filter.as_ref(),
                    limit: request.limit,
                    resolve_using: false,
                    extra: serde_json::Map::new(),
                },
            )
            .await
        }
    }
}

fn unknown_hit(record: crate::DataRecord) -> ScoredRecord {
    ScoredRecord {
        record,
        raw_score: None,
        score_kind: ScoreKind::Unknown,
    }
}
