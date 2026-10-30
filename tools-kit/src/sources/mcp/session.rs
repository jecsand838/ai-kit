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
pub(crate) struct ToolKitClientHandler {
    broker: McpInteractionBroker,
}

impl ToolKitClientHandler {
    pub(crate) fn new(broker: McpInteractionBroker) -> Self {
        Self { broker }
    }
}

impl ClientHandler for ToolKitClientHandler {
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
            let mut implementation = Implementation::new("ai-kit-tools-kit", "0.0.2");
            implementation.title = Some("ai-kit tools-kit".to_owned());
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
