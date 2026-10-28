#![cfg(feature = "mcp-server")]

pub mod support;

use rmcp::{ServiceExt, transport::io::stdio};

use support::repo_managed_mcp_service;

const RUN_STANDALONE_ENV: &str = "TOOLS_KIT_MCP_RUN_STANDALONE";
const RUN_STANDALONE_VALUE: &str = "stdio";
const GRACEFUL_CLOSE_MARKER_ENV: &str = "TOOLS_KIT_MCP_GRACEFUL_CLOSE_MARKER";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var(RUN_STANDALONE_ENV).as_deref() != Ok(RUN_STANDALONE_VALUE) {
        return Ok(());
    }

    run_stdio_server_from_env().await
}

pub async fn run_stdio_server_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let service = repo_managed_mcp_service::RepoManagedMcpServer::from_env()
        .serve(stdio())
        .await?;
    service.waiting().await?;
    if let Some(marker) = std::env::var_os(GRACEFUL_CLOSE_MARKER_ENV) {
        std::fs::write(marker, b"closed")?;
    }
    Ok(())
}
