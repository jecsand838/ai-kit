use tool_sdk::{
    ToolError, ToolHandler, ToolHandlerBackend,
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

fn main() {
    let _handler = ToolHandler::from_backend(Handler);
}
