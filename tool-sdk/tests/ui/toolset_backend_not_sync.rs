use std::cell::Cell;

use tool_sdk::{
    ToolError, ToolHandlerBackend, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput},
};

struct Handler;

impl ToolHandlerBackend for Handler {
    fn step(
        &mut self,
        _input: ToolHandlerInput,
    ) -> impl std::future::Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_ {
        std::future::pending()
    }
}

struct NonSyncToolset {
    state: Cell<u8>,
}

impl ToolsetBackend for NonSyncToolset {
    type Handler = Handler;

    fn list_tools(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_ {
        std::future::ready(Ok(Vec::new()))
    }

    fn tool_handler(
        &self,
        _name: ToolName,
    ) -> impl std::future::Future<Output = Result<Self::Handler, ToolError>> + Send + '_ {
        std::future::ready(Ok(Handler))
    }

    fn shutdown(&self) -> impl std::future::Future<Output = Result<(), ToolError>> + Send + '_ {
        std::future::ready(Ok(()))
    }
}

fn main() {
    let _ = NonSyncToolset {
        state: Cell::new(0),
    };
}
