use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use rmcp::service::{RoleClient, RunningService};

use crate::{
    ToolError, ToolsetBackend,
    declaration::ToolInfo,
    mcp::McpClientConfig,
    name::{McpToolNameContext, ToolName},
};

pub(crate) mod catalog;
pub(crate) mod client;
pub(crate) mod handler;
pub(crate) mod interaction;
#[cfg(feature = "mcp-server")]
pub(crate) mod server;
pub(crate) mod session;

use self::{
    catalog::{McpProtocol, McpToolEntry},
    client::McpClient,
    session::ToolSdkClientHandler,
};

pub(crate) struct McpToolset {
    tools: Arc<BTreeMap<ToolName, Arc<McpToolEntry>>>,
    session: Mutex<Option<RunningService<RoleClient, ToolSdkClientHandler>>>,
}

impl std::fmt::Debug for McpToolset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpToolset")
            .field("tool_count", &self.tools.len())
            .finish_non_exhaustive()
    }
}

impl McpToolset {
    pub(crate) async fn connect(config: McpClientConfig) -> Result<Self, ToolError> {
        let (client, source_tools, session) = McpClient::connect(&config).await?;
        let mut names = match McpToolNameContext::new(config.namespace()) {
            Ok(names) => names,
            Err(error) => return Err(McpClient::close_after_build_failure(session, error).await),
        };
        let mut routes = BTreeMap::new();
        for tool in source_tools {
            let entry =
                match McpProtocol::entry_from_mcp_tool(&config, client.clone(), &mut names, tool) {
                    Ok(entry) => entry,
                    Err(error) => {
                        return Err(McpClient::close_after_build_failure(session, error).await);
                    }
                };
            let name = entry.info().name.clone();
            if routes.insert(name.clone(), Arc::new(entry)).is_some() {
                return Err(McpClient::close_after_build_failure(
                    session,
                    ToolError::DuplicateName { name },
                )
                .await);
            }
        }
        Ok(Self {
            tools: Arc::new(routes),
            session: Mutex::new(Some(session)),
        })
    }

    fn take_session(&self) -> Option<RunningService<RoleClient, ToolSdkClientHandler>> {
        self.session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl ToolsetBackend for McpToolset {
    type Handler = handler::McpToolHandler;

    fn list_tools(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_ {
        std::future::ready(Ok(self
            .tools
            .values()
            .map(|entry| entry.info().clone())
            .collect()))
    }

    fn tool_handler(
        &self,
        name: ToolName,
    ) -> impl std::future::Future<Output = Result<Self::Handler, ToolError>> + Send + '_ {
        let handler = self
            .tools
            .get(&name)
            .map(|entry| handler::McpToolHandler::new(Arc::clone(entry)))
            .ok_or(ToolError::UnknownTool { name });
        std::future::ready(handler)
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        match self.take_session() {
            Some(session) => McpClient::close_session(session).await,
            None => Ok(()),
        }
    }
}
