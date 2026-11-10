use rmcp::{
    ClientHandler, ErrorData as McpError,
    model::{
        ClientCapabilities, ClientConfig, ElicitRequestParams, ElicitResult, ElicitationCapability,
        FormElicitationCapability, Implementation, ProtocolVersion,
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
    fn get_info(&self) -> ClientConfig {
        let mut capabilities = ClientCapabilities::default();
        if self.broker.form_enabled() {
            capabilities.elicitation = Some(
                ElicitationCapability::new().with_form(
                    FormElicitationCapability::new()
                        .with_schema_validation(self.broker.schema_validation()),
                ),
            );
        }
        ClientConfig::new(capabilities, {
            let mut implementation = Implementation::new("ai-kit-tools-kit", "0.0.4");
            implementation.title = Some("ai-kit tools-kit".to_owned());
            implementation
        })
        .with_protocol_version(ProtocolVersion::V_2025_11_25)
    }

    async fn create_elicitation(
        &self,
        request: ElicitRequestParams,
        _context: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, McpError> {
        self.broker
            .request_elicitation(request)
            .await
            .map_err(|error| McpError::internal_error(error.to_string(), None))
    }
}
