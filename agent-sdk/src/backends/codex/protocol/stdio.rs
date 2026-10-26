use std::process::Stdio;

use tokio::process::Command;

use crate::SdkError;

use super::jsonrpc::{CodexRuntimeBuilder, CodexTransportRuntime};

pub(crate) struct CodexStdio;

impl CodexStdio {
    pub(crate) fn spawn() -> Result<CodexTransportRuntime, SdkError> {
        let mut command = Command::new("codex");
        command
            .arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|_| SdkError::integration("server process could not be spawned"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SdkError::integration("server process stdin is unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SdkError::integration("server process stdout is unavailable"))?;
        let stderr = child.stderr.take();
        Ok(CodexRuntimeBuilder::json_lines(
            stdout, stdin, child, stderr,
        ))
    }
}
