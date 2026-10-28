use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use serde_json::Value;
use std::path::PathBuf;

use crate::{FileRead, KitError};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexFsReadFileResponse {
    data_base64: String,
}

pub(crate) struct CodexFileRead {
    relative_path: PathBuf,
    absolute_path: PathBuf,
}

impl CodexFileRead {
    pub(crate) fn new(relative_path: PathBuf, absolute_path: PathBuf) -> Result<Self, KitError> {
        if !absolute_path.is_absolute() {
            return Err(KitError::invalid_state(
                "file adapter received a relative native path",
            ));
        }
        Ok(Self {
            relative_path,
            absolute_path,
        })
    }

    pub(crate) fn params(&self) -> Result<Value, KitError> {
        let path = self.absolute_path.to_str().ok_or_else(|| {
            KitError::unsupported("selected runtime cannot represent a non-UTF-8 backend-host path")
        })?;
        Ok(serde_json::json!({ "path": path }))
    }

    pub(crate) fn decode(self, value: Value) -> Result<FileRead, KitError> {
        let response: CodexFsReadFileResponse = serde_json::from_value(value)
            .map_err(|_| KitError::protocol("file response is malformed"))?;
        let bytes = STANDARD
            .decode(response.data_base64.as_bytes())
            .map_err(|_| KitError::protocol("file response contains invalid base64"))?;
        FileRead::from_bytes(self.relative_path, bytes)
    }
}
