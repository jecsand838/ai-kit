mod edge;
mod scan;
mod shared;
mod vector;

use crate::{CapabilitySet, QueryRequest, QueryResponse, StorageError};

use super::manager::QdrantManager;

pub(super) async fn handle(
    db: &QdrantManager,
    request: QueryRequest,
) -> Result<QueryResponse, StorageError> {
    match request {
        QueryRequest::Nearest(request) => vector::nearest(db, request).await,
        QueryRequest::SearchById(request) => vector::search_by_id(db, request).await,
        QueryRequest::Text(request) => {
            db.require_capability(
                CapabilitySet::FULL_TEXT,
                "FULL_TEXT",
                format!(
                    "text search for collection `{}`",
                    request.query.collection()
                ),
            )?;
            Err(db.unsupported(
                "FULL_TEXT",
                "Qdrant payload matching does not provide the retained portable text-search semantics",
            ))
        }
        QueryRequest::Scroll(request) => scan::scroll(db, request).await,
        QueryRequest::RandomSample(request) => {
            db.require_capability(
                CapabilitySet::RANDOM_SAMPLE,
                "RANDOM_SAMPLE",
                format!(
                    "random sampling for collection `{}`",
                    request.locator.collection
                ),
            )?;
            scan::random_sample(db, request).await
        }
    }
}
