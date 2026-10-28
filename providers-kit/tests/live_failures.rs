//! Private case failures keep test assertions distinct from Kit and accepted remote failures.
use std::fmt;
use std::io::Write;
use std::path::Path;

use providers_kit::KitError;
use providers_kit::batch::BatchResult;
use providers_kit::error::RemoteFailure;
use providers_kit::generation::GenerateResponse;
use serde_json::{Value, json};

pub enum CaseFailure {
    Assertion(String),
    Kit(KitError),
    Remote(RemoteFailure),
    Owned {
        id: String,
        failure: Box<Self>,
    },
    BatchRow {
        custom_id: String,
        error: RemoteFailure,
    },
    Group {
        label: &'static str,
        failures: Vec<Self>,
    },
}

impl CaseFailure {
    pub fn required_observation_directory(
        directory: Option<std::path::PathBuf>,
    ) -> Result<std::path::PathBuf, Self> {
        if let Some(directory) = directory {
            return Ok(directory);
        }
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../target/providers-kit-required-observations")
            .join(uuid::Uuid::new_v4().to_string());
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).map_err(|error| {
            Self::from(format!(
                "private required-observation directory failed: {:?}",
                error.kind()
            ))
        })?;
        Ok(path)
    }

    /// Projects public observations without touching opaque continuation contents.
    pub fn response_evidence<C>(response: &GenerateResponse<C>) -> Value {
        json!({
            "model": response.model,
            "message": {
                "role": response.message.role(),
                "content": response.message.content(),
                "function_calls": response.message.function_calls(),
                "function_results": response.message.function_results(),
                "continuation_present": response.message.continuation().is_some(),
            },
            "finish_reason": response.finish_reason,
            "citations": response.citations,
            "artifacts": response.artifacts,
            "hosted_tool_outputs": response.hosted_tool_outputs,
            "usage": response.usage,
        })
    }

    pub fn batch_evidence<C>(job_id: &str, rows: &[BatchResult<C>]) -> Value {
        let rows = rows
            .iter()
            .map(|row| match row {
                BatchResult::Succeeded { custom_id, output } => json!({
                    "outcome": "succeeded", "custom_id": custom_id,
                    "response": Self::response_evidence(output),
                }),
                BatchResult::Failed { custom_id, error } => json!({
                    "outcome": "failed", "custom_id": custom_id, "error": error,
                }),
                BatchResult::Cancelled { custom_id } => json!({
                    "outcome": "cancelled", "custom_id": custom_id,
                }),
                BatchResult::Expired { custom_id } => json!({
                    "outcome": "expired", "custom_id": custom_id,
                }),
            })
            .collect::<Vec<_>>();
        json!({"job_id": job_id, "rows": rows})
    }

    /// Required observations fail before export if their redacted proof is too large.
    pub fn write_required_observation(
        path: &Path,
        observation: Value,
        secrets: &[String],
    ) -> std::io::Result<()> {
        const LIMIT: usize = 1024 * 1024;
        let mut evidence = json!({"truncated": false, "observation": observation});
        Self::redact_credentials(&mut evidence, secrets);
        let bytes = serde_json::to_vec(&evidence).map_err(std::io::Error::other)?;
        if bytes.len() > LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "required observation exceeds 1 MiB after credential redaction",
            ));
        }
        Self::write_bytes(path, &bytes)
    }

    /// Explicit opt-in observation export. Truncation never precedes redaction.
    pub fn write_observation(
        path: &Path,
        observation: Value,
        secrets: &[String],
    ) -> std::io::Result<()> {
        const LIMIT: usize = 1024 * 1024;
        let mut evidence = json!({"truncated": false, "observation": observation});
        Self::redact_credentials(&mut evidence, secrets);
        let serialized = serde_json::to_string(&evidence).map_err(std::io::Error::other)?;
        if serialized.len() <= LIMIT {
            return Self::write_bytes(path, serialized.as_bytes());
        }
        // A JSON string can expand each input byte by at most six bytes. Leave
        // space for the explicit envelope and retain a valid UTF-8 prefix.
        let mut end = (LIMIT - 256) / 6;
        while !serialized.is_char_boundary(end) {
            end -= 1;
        }
        let bounded = json!({
            "truncated": true,
            "limit_bytes": LIMIT,
            "serialized_bytes": serialized.len(),
            "json_prefix": &serialized[..end],
        });
        let bytes = serde_json::to_vec(&bounded).map_err(std::io::Error::other)?;
        Self::write_bytes(path, &bytes)
    }

    pub fn with_owner(self, id: impl Into<String>) -> Self {
        Self::Owned {
            id: id.into(),
            failure: Box::new(self),
        }
    }

    pub fn context(self, label: &'static str) -> Self {
        Self::Group {
            label,
            failures: vec![self],
        }
    }

    pub fn with_remote(self, failure: RemoteFailure) -> Self {
        Self::Group {
            label: "assertion and retained remote outcome",
            failures: vec![self, Self::Remote(failure)],
        }
    }

    pub fn with_sdk(self, error: KitError) -> Self {
        Self::Group {
            label: "assertion and retained Kit failure",
            failures: vec![self, Self::Kit(error)],
        }
    }

    pub fn combine(primary: Result<(), Self>, cleanup: Result<(), Self>) -> Result<(), Self> {
        match (primary, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(failure), Ok(())) | (Ok(()), Err(failure)) => Err(failure),
            (Err(primary), Err(cleanup)) => Err(Self::Group {
                label: "primary and cleanup failures",
                failures: vec![primary, cleanup],
            }),
        }
    }

    pub fn collected(label: &'static str, failures: Vec<Self>) -> Result<(), Self> {
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Self::Group { label, failures })
        }
    }

    /// Explicit sensitive-data export, never used by ordinary formatting.
    pub fn evidence(&self) -> Value {
        match self {
            Self::Assertion(message) => json!({"assertion": message}),
            Self::Kit(error) => json!({"sdk": Self::sdk_evidence(error)}),
            Self::Remote(error) => json!({"remote": error}),
            Self::Owned { id, failure } => json!({"owner": id, "failure": failure.evidence()}),
            Self::BatchRow { custom_id, error } => {
                json!({"batch_row": {"custom_id": custom_id, "error": error}})
            }
            Self::Group { label, failures } => json!({
                "label": label, "failures": failures.iter().map(Self::evidence).collect::<Vec<_>>()
            }),
        }
    }

    fn sdk_evidence(error: &KitError) -> Value {
        json!({
            "kind": format!("{:?}", error.kind()), "basis": error.classification_basis(),
            "context": {"operation": format!("{:?}", error.context().operation), "phase": format!("{:?}", error.context().phase), "row": error.context().row}, "http_status": error.http_status(),
            "timeout_ms": error.timeout().map(|value| value.as_millis()),
            "request_target": error.request_target(), "correlation_id": error.correlation_id(),
            "native": error.native_details(), "response": error.response_details(),
            "cause": error.cause().map(Self::sdk_evidence),
            "supplementary": error.supplementary().iter().map(Self::sdk_evidence).collect::<Vec<_>>(),
        })
    }

    pub fn write_evidence(&self, path: &Path, secrets: &[String]) -> std::io::Result<()> {
        let mut evidence = self.evidence();
        Self::redact_credentials(&mut evidence, secrets);
        let bytes = serde_json::to_vec_pretty(&evidence).map_err(std::io::Error::other)?;
        Self::write_bytes(path, &bytes)
    }

    /// Complete private PCM sidecars remain separate from the 1-MiB JSON limit.
    pub fn write_session_pcm(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "session PCM evidence exceeds 8 MiB",
            ));
        }
        Self::write_bytes(path, bytes)
    }

    fn write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(bytes)?;
        file.sync_all()
    }

    pub(crate) fn redact_credentials(value: &mut Value, secrets: &[String]) {
        match value {
            Value::String(text) => {
                for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
                    *text = text.replace(secret, "<credential-redacted>");
                }
            }
            Value::Array(values) => {
                for value in values {
                    Self::redact_credentials(value, secrets);
                }
            }
            Value::Object(values) => {
                for (mut key, mut value) in std::mem::take(values) {
                    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
                        key = key.replace(secret, "<credential-redacted>");
                    }
                    Self::redact_credentials(&mut value, secrets);
                    values.insert(key, value);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
}

impl From<KitError> for CaseFailure {
    fn from(error: KitError) -> Self {
        Self::Kit(error)
    }
}

impl From<String> for CaseFailure {
    fn from(message: String) -> Self {
        Self::Assertion(message)
    }
}

impl From<&str> for CaseFailure {
    fn from(message: &str) -> Self {
        Self::Assertion(message.to_owned())
    }
}

impl fmt::Display for CaseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Assertion(message) => formatter.write_str(message),
            Self::Kit(error) => write!(formatter, "{error:?}"),
            Self::Remote(error) => write!(formatter, "accepted remote failure: {error:?}"),
            Self::Owned { failure, .. } => {
                write!(formatter, "owned resource [redacted]: {failure}")
            }
            Self::BatchRow { error, .. } => {
                write!(formatter, "accepted batch row [redacted]: {error:?}")
            }
            Self::Group { label, failures } => {
                write!(formatter, "{label}")?;
                for failure in failures {
                    write!(formatter, "; {failure}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for CaseFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}
