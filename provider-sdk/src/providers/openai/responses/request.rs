use reqwest::Method;
use serde_json::Value;

use crate::SdkError;
use crate::transport::HttpResponse;

use super::super::api::{OPENAI_REQUESTS, OpenAiApiContext};
pub(crate) async fn create_raw_response(
    api: &OpenAiApiContext,
    body: Value,
) -> Result<HttpResponse<Value>, SdkError> {
    OPENAI_REQUESTS
        .json::<Value>(
            "responses",
            api.request(Method::POST, "responses")?.json(&body),
            api.transport().execution(),
        )
        .await
}
