mod scan;
mod shared;
mod text;
mod vector;

use crate::{QueryRequest, QueryResponse, StorageError};

use super::manager::LanceManager;

pub(super) async fn handle(
    db: &LanceManager,
    request: QueryRequest,
) -> Result<QueryResponse, StorageError> {
    match request {
        QueryRequest::Nearest(request) => vector::nearest(db, request).await,
        QueryRequest::SearchById(request) => vector::search_by_id(db, request).await,
        QueryRequest::Text(request) => {
            db.require_capability(
                crate::CapabilitySet::FULL_TEXT,
                "FULL_TEXT",
                format!(
                    "text search for collection `{}`",
                    request.query.collection()
                ),
            )?;
            text::text(db, request).await
        }
        QueryRequest::Scroll(request) => scan::scroll(db, request).await,
        QueryRequest::RandomSample(request) => scan::random_sample(db, request).await,
    }
}
