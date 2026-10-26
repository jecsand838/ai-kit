mod scan;
mod shared;
mod text;
mod vector;

use crate::{CapabilitySet, QueryRequest, QueryResponse, StorageError};

use super::manager::SurrealManager;

pub(super) async fn handle(
    db: &SurrealManager,
    request: QueryRequest,
) -> Result<QueryResponse, StorageError> {
    match request {
        QueryRequest::Nearest(request) => db.nearest(request).await,
        QueryRequest::SearchById(request) => db.search_by_id(request).await,
        QueryRequest::Text(request) => {
            db.require_capability(
                CapabilitySet::FULL_TEXT,
                "FULL_TEXT",
                format!(
                    "text search for collection `{}`",
                    request.query.collection()
                ),
            )?;
            text::text(db, request).await
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
