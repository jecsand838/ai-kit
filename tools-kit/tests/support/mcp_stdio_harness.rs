use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Duration,
};

use tools_kit::{
    Toolset,
    mcp::{McpClientConfig, McpConnectionConfig},
};

const STDIO_SERVER_TARGET: &str = "tools_kit_mcp_stdio_server";
const RUN_STANDALONE_ENV: &str = "TOOLS_KIT_MCP_RUN_STANDALONE";
const RUN_STANDALONE_VALUE: &str = "stdio";
const LIST_TOOLS_ERROR_ENV: &str = "TOOLS_KIT_MCP_LIST_TOOLS_ERROR";
const CANCEL_MARKER_ENV: &str = "TOOLS_KIT_MCP_CANCEL_MARKER";

pub fn stdio_source(
    namespace: Option<&str>,
    exit_marker: Option<&Path>,
    request_timeout_secs: u64,
) -> McpClientConfig {
    stdio_source_config(namespace, exit_marker, None, request_timeout_secs, false)
}

pub fn stdio_source_config_with_list_error(
    namespace: Option<&str>,
    exit_marker: Option<&Path>,
    request_timeout_secs: u64,
    fail_list_tools: bool,
) -> McpClientConfig {
    stdio_source_config(
        namespace,
        exit_marker,
        None,
        request_timeout_secs,
        fail_list_tools,
    )
}

pub fn stdio_source_with_cancel_marker(
    namespace: Option<&str>,
    cancel_marker: &Path,
    request_timeout_secs: u64,
) -> McpClientConfig {
    stdio_source_config(
        namespace,
        None,
        Some(cancel_marker),
        request_timeout_secs,
        false,
    )
}

fn stdio_source_config(
    namespace: Option<&str>,
    exit_marker: Option<&Path>,
    cancel_marker: Option<&Path>,
    request_timeout_secs: u64,
    fail_list_tools: bool,
) -> McpClientConfig {
    let mut env = BTreeMap::new();
    if let Some(exit_marker) = exit_marker {
        env.insert(
            "TOOLS_KIT_MCP_EXIT_MARKER".to_owned(),
            exit_marker.display().to_string(),
        );
    }
    if let Some(cancel_marker) = cancel_marker {
        env.insert(
            CANCEL_MARKER_ENV.to_owned(),
            cancel_marker.display().to_string(),
        );
    }
    if fail_list_tools {
        env.insert(LIST_TOOLS_ERROR_ENV.to_owned(), "1".to_owned());
    }
    env.insert(
        RUN_STANDALONE_ENV.to_owned(),
        RUN_STANDALONE_VALUE.to_owned(),
    );

    let config = McpClientConfig::new(McpConnectionConfig::Stdio {
        program: stdio_server_bin().display().to_string(),
        args: Vec::new(),
        cwd: None,
        env,
    })
    .with_request_timeout(Duration::from_secs(request_timeout_secs))
    .expect("test timeout should validate");
    match namespace {
        Some(namespace) => config
            .with_namespace(namespace)
            .expect("test namespace should validate"),
        None => config,
    }
}

pub async fn runtime(namespace: Option<&str>) -> Toolset {
    Toolset::mcp(stdio_source(namespace, None, 2))
        .await
        .expect("repo-managed stdio MCP runtime should build")
}

fn stdio_server_bin() -> PathBuf {
    static STDIO_SERVER_BIN: OnceLock<PathBuf> = OnceLock::new();
    STDIO_SERVER_BIN
        .get_or_init(|| super::mcp_server_binary::test_target_executable(STDIO_SERVER_TARGET))
        .clone()
}
