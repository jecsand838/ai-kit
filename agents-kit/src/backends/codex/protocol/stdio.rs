use std::process::Stdio;

use tokio::process::Command;

use crate::KitError;

use super::jsonrpc::{CodexRuntimeBuilder, CodexTransportRuntime};

pub(crate) struct CodexStdio;

impl CodexStdio {
    pub(crate) fn spawn() -> Result<CodexTransportRuntime, KitError> {
        let mut command = Command::new("codex");
        command
            .arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|_| KitError::integration("server process could not be spawned"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| KitError::integration("server process stdin is unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| KitError::integration("server process stdout is unavailable"))?;
        let stderr = child.stderr.take();
        Ok(CodexRuntimeBuilder::json_lines(
            stdout, stdin, child, stderr,
        ))
    }
}
