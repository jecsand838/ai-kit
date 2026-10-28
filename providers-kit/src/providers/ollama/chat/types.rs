use serde_json::Value;

use crate::models::ModelId;

#[derive(Clone, Debug)]
pub(crate) struct OllamaBuiltChatBody {
    pub(crate) body: Value,
    pub(crate) model_id: ModelId,
}
