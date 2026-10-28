use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tools_kit::declaration::{ToolInfo, ToolName};
use tools_kit::execution::ToolResult;
use tools_kit::local::ToolRegistration;
use tools_kit::schema::JsonSchema;
use tools_kit::{ToolError, Toolset};

/// Caller-owned shell policy used by tests of ordinary Local tool composition.
pub(crate) struct CallerShell;

#[derive(Default)]
pub(crate) struct CallerShellObserver {
    invoked: AtomicUsize,
    started: AtomicUsize,
    reaped: AtomicUsize,
    returned: AtomicUsize,
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub(crate) struct ShellCounters {
    invoked: usize,
    started: usize,
    reaped: usize,
    returned: usize,
}

impl CallerShellObserver {
    pub(crate) fn snapshot(&self) -> ShellCounters {
        ShellCounters {
            invoked: self.invoked.load(Ordering::Acquire),
            started: self.started.load(Ordering::Acquire),
            reaped: self.reaped.load(Ordering::Acquire),
            returned: self.returned.load(Ordering::Acquire),
        }
    }
}

impl CallerShell {
    pub(crate) fn toolset(
        root: PathBuf,
        timeout: Duration,
        max_bytes: usize,
        observer: Option<Arc<CallerShellObserver>>,
    ) -> Toolset {
        let name = ToolName::new("shell").expect("fixture shell name is valid");
        let mut info = ToolInfo::new(
            name,
            JsonSchema::strict(serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string", "minLength": 1},
                    "cwd": {"type": "string", "minLength": 1}
                },
                "required": ["command", "cwd"],
                "additionalProperties": false
            })),
        );
        info.description =
            Some("Runs a command in an explicit directory below the caller's root.".to_owned());
        let registration = ToolRegistration::function(info, move |invocation| {
            let root = root.clone();
            let observer = observer.clone();
            if let Some(observer) = &observer {
                observer.invoked.fetch_add(1, Ordering::AcqRel);
            }
            async move {
                let result = Self::execute(
                    root,
                    timeout,
                    max_bytes,
                    invocation.arguments,
                    observer.as_deref(),
                )
                .await;
                if let Some(observer) = observer {
                    observer.returned.fetch_add(1, Ordering::AcqRel);
                }
                result
            }
        });
        Toolset::local(vec![registration]).expect("caller shell registration must build")
    }

    async fn execute(
        root: PathBuf,
        timeout: Duration,
        max_bytes: usize,
        arguments: serde_json::Value,
        observer: Option<&CallerShellObserver>,
    ) -> Result<ToolResult, ToolError> {
        let cwd = arguments["cwd"]
            .as_str()
            .expect("Local schema validates cwd");
        let cwd = root
            .join(cwd)
            .canonicalize()
            .map_err(|error| Self::error(error.to_string()))?;
        if !cwd.starts_with(&root) {
            return Ok(ToolResult::text("cwd is outside the configured root").with_error(true));
        }
        let command_text = arguments["command"]
            .as_str()
            .expect("Local schema validates command");
        #[cfg(unix)]
        let (program, argument) = ("sh", "-c");
        #[cfg(windows)]
        let (program, argument) = ("cmd", "/C");
        let mut child = tokio::process::Command::new(program)
            .arg(argument)
            .arg(command_text)
            .current_dir(cwd)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| Self::error(error.to_string()))?;
        if let Some(observer) = observer {
            observer.started.fetch_add(1, Ordering::AcqRel);
        }
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        let remaining = Arc::new(AtomicUsize::new(max_bytes));
        let truncated = Arc::new(AtomicBool::new(false));
        let output = tokio::time::timeout(timeout, async {
            tokio::join!(
                Self::capture(stdout, Arc::clone(&remaining), Arc::clone(&truncated)),
                Self::capture(stderr, remaining, Arc::clone(&truncated)),
                child.wait()
            )
        })
        .await;
        let (stdout, stderr, status) = match output {
            Ok((stdout, stderr, status)) => {
                if status.is_ok()
                    && let Some(observer) = observer
                {
                    observer.reaped.fetch_add(1, Ordering::AcqRel);
                }
                (
                    stdout?,
                    stderr?,
                    status.map_err(|error| Self::error(error.to_string()))?,
                )
            }
            Err(_) => {
                child
                    .kill()
                    .await
                    .map_err(|error| Self::error(error.to_string()))?;
                child
                    .wait()
                    .await
                    .map_err(|error| Self::error(error.to_string()))?;
                if let Some(observer) = observer {
                    observer.reaped.fetch_add(1, Ordering::AcqRel);
                }
                return Err(ToolError::Timeout {
                    tool: ToolName::new("shell").expect("fixture shell name is valid"),
                    timeout_secs: timeout.as_secs().max(1),
                });
            }
        };
        Ok(ToolResult::json(serde_json::json!({
            "exit_code": status.code(),
            "stdout": String::from_utf8_lossy(&stdout),
            "stderr": String::from_utf8_lossy(&stderr),
            "truncated": truncated.load(Ordering::Acquire)
        }))
        .with_error(!status.success()))
    }

    async fn capture<R: AsyncRead + Unpin>(
        mut reader: R,
        remaining: Arc<AtomicUsize>,
        truncated: Arc<AtomicBool>,
    ) -> Result<Vec<u8>, ToolError> {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = reader
                .read(&mut buffer)
                .await
                .map_err(|error| Self::error(error.to_string()))?;
            if count == 0 {
                return Ok(output);
            }
            let available = remaining
                .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                    Some(value.saturating_sub(count))
                })
                .expect("update always succeeds");
            let keep = available.min(count);
            output.extend_from_slice(&buffer[..keep]);
            if keep < count {
                truncated.store(true, Ordering::Release);
            }
        }
    }

    fn error(message: String) -> ToolError {
        ToolError::Transport {
            tool: None,
            message,
        }
    }
}
