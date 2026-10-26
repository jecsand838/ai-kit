use std::{
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;

pub fn test_target_executable(target_name: &str) -> PathBuf {
    let output = Command::new(cargo_bin())
        .args([
            "test",
            "--locked",
            "-p",
            "tool-sdk",
            "--test",
            target_name,
            "--features",
            "mcp-server",
            "--no-run",
            "--message-format=json",
        ])
        .current_dir(workspace_dir())
        .output()
        .expect("nested cargo should compile the repo-managed MCP server target");

    if !output.status.success() {
        panic!(
            "failed to compile repo-managed MCP server target `{target_name}`\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    parse_executable_path(target_name, &output.stdout).unwrap_or_else(|| {
        panic!(
            "cargo did not report an executable for repo-managed MCP server target `{target_name}`\nstdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn cargo_bin() -> PathBuf {
    std::env::var_os("CARGO")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("cargo"))
}

fn workspace_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tool-sdk manifest should live under the workspace root")
}

fn parse_executable_path(target_name: &str, stdout: &[u8]) -> Option<PathBuf> {
    let stdout = String::from_utf8_lossy(stdout);
    for line in stdout.lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };

        if message.get("reason").and_then(Value::as_str) != Some("compiler-artifact") {
            continue;
        }
        if message.pointer("/target/name").and_then(Value::as_str) != Some(target_name) {
            continue;
        }
        if let Some(executable) = message.get("executable").and_then(Value::as_str) {
            return Some(PathBuf::from(executable));
        }
    }

    None
}
