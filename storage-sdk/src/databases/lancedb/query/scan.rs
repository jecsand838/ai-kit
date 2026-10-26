use crate::{QueryResponse, RandomSampleRequest, ScoreKind, ScrollRequest, StorageError};

use super::{LanceManager, shared};

pub(super) async fn scroll(
    db: &LanceManager,
    request: ScrollRequest,
) -> Result<QueryResponse, StorageError> {
    let effective_offset =
        crate::PageToken::effective_offset(request.page_token.as_deref(), request.offset)?;
    let page = db
        .table_ops(&request.locator)
        .plain_page(
            request.filter.as_ref(),
            &request.projection,
            request.limit,
            effective_offset,
        )
        .await?;

    Ok(shared::records_page_to_query_response(
        page,
        ScoreKind::Unknown,
    ))
}

pub(super) async fn random_sample(
    _db: &LanceManager,
    request: RandomSampleRequest,
) -> Result<QueryResponse, StorageError> {
    Err(StorageError::unsupported_capability(
        "RANDOM_SAMPLE",
        format!(
            "random sampling is unavailable for LanceDB collection `{}`",
            request.locator.collection
        ),
    ))
}
