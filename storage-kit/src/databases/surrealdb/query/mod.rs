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
    }
}
