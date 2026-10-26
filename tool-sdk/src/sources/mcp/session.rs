use rmcp::{
    ClientHandler, ErrorData as McpError,
    model::{
        ClientCapabilities, CreateElicitationRequestParams, CreateElicitationResult,
        ElicitationCapability, FormElicitationCapability, Implementation, InitializeRequestParams,
    },
    service::{RequestContext, RoleClient},
};

use super::interaction::McpInteractionBroker;

#[derive(Clone, Debug)]
pub(crate) struct ToolSdkClientHandler {
    broker: McpInteractionBroker,
}

impl ToolSdkClientHandler {
    pub(crate) fn new(broker: McpInteractionBroker) -> Self {
        Self { broker }
    }
}

impl ClientHandler for ToolSdkClientHandler {
    fn get_info(&self) -> InitializeRequestParams {
        let mut capabilities = ClientCapabilities::default();
        if self.broker.form_enabled() {
            capabilities.elicitation = Some(ElicitationCapability {
                form: Some(FormElicitationCapability {
                    schema_validation: Some(self.broker.schema_validation()),
                }),
                url: None,
            });
        }
        InitializeRequestParams::new(capabilities, {
            let mut implementation = Implementation::new("ai-kit-tool-sdk", "0.1.0");
            implementation.title = Some("ai-kit tool-sdk".to_owned());
            implementation
        })
    }

    async fn create_elicitation(
        &self,
        request: CreateElicitationRequestParams,
        _context: RequestContext<RoleClient>,
    ) -> Result<CreateElicitationResult, McpError> {
        self.broker
            .request_elicitation(request)
            .await
            .map_err(|error| McpError::internal_error(error.to_string(), None))
    }
}
