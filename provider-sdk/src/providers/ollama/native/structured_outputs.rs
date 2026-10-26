use serde_json::Value;

use crate::SdkError;
use crate::generation::ResponseFormat;

pub(crate) fn response_format_to_ollama(
    format: Option<&ResponseFormat>,
) -> Result<Option<Value>, SdkError> {
    match format {
        None | Some(ResponseFormat::Text) => Ok(None),
        Some(ResponseFormat::JsonObject) => Ok(Some(Value::String("json".to_owned()))),
        Some(ResponseFormat::JsonSchema(schema)) => Ok(Some(schema.schema().clone())),
    }
}
