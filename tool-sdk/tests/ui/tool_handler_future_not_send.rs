use std::rc::Rc;

use tool_sdk::{
    ToolError, ToolHandlerBackend,
    execution::{ToolHandlerInput, ToolHandlerOutput},
};

struct NonSendFutureHandler;

impl ToolHandlerBackend for NonSendFutureHandler {
    fn step(
        &mut self,
        _input: ToolHandlerInput,
    ) -> impl std::future::Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_ {
        async {
            let not_send = Rc::new(());
            std::future::ready(()).await;
            drop(not_send);
            Err(ToolError::Unsupported {
                operation: "compile-fail fixture".to_owned(),
            })
        }
    }
}

fn main() {}
