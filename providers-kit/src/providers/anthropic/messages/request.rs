use reqwest::Method;
use serde_json::Value;

use crate::KitError;
use crate::transport::HttpResponse;

use super::super::api::{ANTHROPIC_REQUESTS, AnthropicApiContext};
pub(crate) async fn create_raw_message(
    api: &AnthropicApiContext,
    body: Value,
) -> Result<HttpResponse<Value>, KitError> {
    let execution = api.transport().execution();
    ANTHROPIC_REQUESTS
        .json::<Value>(
            "v1/messages",
            api.request(Method::POST, "v1/messages")?.json(&body),
            execution,
        )
        .await
}
