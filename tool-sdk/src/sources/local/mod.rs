use std::{collections::BTreeMap, sync::Arc};

use crate::{
    ToolError, ToolsetBackend,
    declaration::ToolInfo,
    local::{RegisteredTool, ToolRegistration},
    name::ToolName,
};

pub(crate) mod handler;

#[derive(Debug)]
pub(crate) struct LocalToolset {
    tools: BTreeMap<ToolName, Arc<RegisteredTool>>,
}

impl LocalToolset {
    pub(crate) fn build(registrations: Vec<ToolRegistration>) -> Result<Self, ToolError> {
        let mut tools = BTreeMap::new();
        for registration in registrations {
            let registration = registration.into_registered();
            let name = registration.info().name.clone();
            if tools.insert(name.clone(), Arc::new(registration)).is_some() {
                return Err(ToolError::DuplicateName { name });
            }
        }
        Ok(Self { tools })
    }
}

impl ToolsetBackend for LocalToolset {
    type Handler = handler::LocalToolHandler;

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
            .map(|registration| handler::LocalToolHandler::new(Arc::clone(registration)))
            .ok_or(ToolError::UnknownTool { name });
        std::future::ready(handler)
    }

    fn shutdown(&self) -> impl std::future::Future<Output = Result<(), ToolError>> + Send + '_ {
        std::future::ready(Ok(()))
    }
}
