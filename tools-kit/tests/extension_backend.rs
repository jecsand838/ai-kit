use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde_json::json;
use tools_kit::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    local::ToolRegistration,
    schema::JsonSchema,
};

struct ExternalHandler {
    finished: bool,
}

impl ToolHandlerBackend for ExternalHandler {
    fn step(
        &mut self,
        input: ToolHandlerInput,
    ) -> impl std::future::Future<Output = Result<ToolHandlerOutput, ToolError>> + Send + '_ {
        let result = match input {
            ToolHandlerInput::Invoke(_) if !self.finished => {
                self.finished = true;
                Ok(ToolHandlerOutput::Completed(ToolResult::text("external")))
            }
            ToolHandlerInput::Invoke(_)
            | ToolHandlerInput::Interaction(_)
            | ToolHandlerInput::Cancel { .. } => Err(ToolError::InvalidConfiguration {
                message: "external handler is terminal".to_owned(),
            }),
        };
        std::future::ready(result)
    }
}

struct ExternalToolset {
    info: ToolInfo,
    shutdowns: Arc<AtomicUsize>,
}

impl ToolsetBackend for ExternalToolset {
    type Handler = ExternalHandler;

    fn list_tools(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_ {
        std::future::ready(Ok(vec![self.info.clone()]))
    }

    fn tool_handler(
        &self,
        name: ToolName,
    ) -> impl std::future::Future<Output = Result<Self::Handler, ToolError>> + Send + '_ {
        std::future::ready(if name == self.info.name {
            Ok(ExternalHandler { finished: false })
        } else {
            Err(ToolError::UnknownTool { name })
        })
    }

    fn shutdown(&self) -> impl std::future::Future<Output = Result<(), ToolError>> + Send + '_ {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn associated_external_handler_normalizes_privately() {
    let name = ToolName::new("external_echo").unwrap();
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let toolset = Toolset::from_backend(ExternalToolset {
        info: ToolInfo::new(name.clone(), JsonSchema::strict(json!({"type": "object"}))),
        shutdowns: Arc::clone(&shutdowns),
    });

    let inventory = toolset.list_tools().await.unwrap();
    assert_eq!(inventory[0].name, name);
    let mut handler = toolset.tool_handler(name).await.unwrap();
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("call".to_owned()),
            arguments: json!({}),
        }))
        .await
        .unwrap();
    assert!(matches!(output, ToolHandlerOutput::Completed(_)));
    toolset.shutdown().await.unwrap();
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn local_construction_uses_the_concrete_builtin_path() {
    let name = ToolName::new("ping").unwrap();
    let info = ToolInfo::new(name.clone(), JsonSchema::strict(json!({"type": "object"})));
    let toolset = Toolset::local(vec![ToolRegistration::function(info, |_| async {
        Ok(ToolResult::text("pong"))
    })])
    .unwrap();
    assert_eq!(toolset.list_tools().await.unwrap()[0].name, name);
}
