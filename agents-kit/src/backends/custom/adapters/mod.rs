use crate::error::{ErrorDetail, ErrorKind as AgentErrorKind, ErrorPhase, Operation};
use providers_kit::error::ErrorKind;
use tools_kit::ToolsetBackend;
pub(crate) mod files;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::stream::{self, StreamExt};
use providers_kit::generation::{
    AllowedToolMode, ContentPart, FinishReason, FunctionCall, FunctionResult, GenerateEvent,
    GenerateRequest, GenerateResponse, GenerationConfig, JsonSchema, Message, ReasoningConfig,
    ReasoningEffort as ProviderReasoningEffort, ResponseFormat, ToolChoice,
};
use providers_kit::models::{ModelId, ModelListRequest, ModelPage};
use providers_kit::{ModelBackend, ProviderBackend};
use tokio::sync::watch;
use tools_kit::execution::{
    ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult,
};

use crate::backends::custom::server_requests::{CustomApprovalDecision, CustomServerRequests};
use crate::backends::custom::state_store::records::CustomGeneration;
use crate::backends::custom::tools::CustomTools;
use crate::prompt::PromptOutput;
use crate::{
    ConversationId, CustomLoopLimits, CustomToolApproval, KitError, ModelInfo, ModelSelector,
    PromptResponse, ReasoningEffort, StructuredOutputRequest, ToolSelection, UsageInfo,
};
use storage_kit::{DatabaseBackend, StateStoreBackend};
use tools_kit::ToolHandlerBackend;

const MODEL_PAGE_SIZE: NonZeroUsize = NonZeroUsize::new(256).expect("model page size is nonzero");
const MAX_MODEL_PAGES: usize = 1_024;
const MAX_MODELS: usize = 65_536;

pub(crate) struct CustomProviderAdapter;

pub(crate) struct CustomProviderRuntime<
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
> {
    provider: Arc<P>,
    tools: CustomTools<T>,
    requests: CustomServerRequests<S, D, T>,
    tool_approval: CustomToolApproval,
}

pub(crate) struct CustomProviderCall {
    conversation_id: ConversationId,
    generation: CustomGeneration,
    model: ModelId,
    system: Option<String>,
    tools: ToolSelection,
    limits: CustomLoopLimits,
    response_format: Option<ResponseFormat>,
    reasoning: Option<ReasoningConfig>,
    output: Option<Arc<PromptOutput>>,
}

#[derive(Clone, Copy)]
pub(crate) enum CustomProviderOperation {
    Build,
    Inventory,
    Request,
    Shutdown,
}

impl CustomProviderAdapter {
    pub(crate) async fn models<P: ProviderBackend>(
        provider: &P,
    ) -> Result<Vec<ModelInfo>, KitError> {
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        let mut rows = BTreeMap::new();
        for _ in 0..MAX_MODEL_PAGES {
            let mut request = ModelListRequest::default().with_limit(MODEL_PAGE_SIZE);
            if let Some(next) = cursor.take() {
                request = request
                    .with_cursor(next)
                    .map_err(|error| CustomProviderOperation::Inventory.map_error(error))?;
            }
            let response = provider
                .list_models(request)
                .await
                .map_err(|error| CustomProviderOperation::Inventory.map_error(error))?;
            let ModelPage {
                models,
                next_cursor,
            } = response;
            if models.len() > MODEL_PAGE_SIZE.get()
                || rows.len().saturating_add(models.len()) > MAX_MODELS
            {
                return Err(KitError::protocol(
                    "model inventory exceeds its supported size",
                ));
            }
            for model in models {
                if model
                    .display_name
                    .as_deref()
                    .is_some_and(|name| name.is_empty() || name.trim() != name)
                    || model.description.as_deref().is_some_and(|description| {
                        description.is_empty() || description.trim() != description
                    })
                    || model.context_window_tokens == Some(0)
                {
                    return Err(KitError::protocol("model inventory is malformed"));
                }
                match provider.model(model.id.clone()) {
                    Ok(_) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            ErrorKind::Unsupported | ErrorKind::Configuration
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        return Err(CustomProviderOperation::Inventory.map_error(error));
                    }
                }
                let selector = ModelSelector::new(model.id.as_str().to_owned())
                    .map_err(|_| KitError::protocol("model selector is malformed"))?;
                let info = ModelInfo::new(selector, model.display_name, Vec::new(), false)
                    .map_err(|_| KitError::protocol("model metadata is malformed"))?;
                if rows.insert(model.id.as_str().to_owned(), info).is_some() {
                    return Err(KitError::invalid_state(
                        "model inventory contains a duplicate selectable model",
                    ));
                }
            }
            let Some(next) = next_cursor else {
                return Ok(rows.into_values().collect());
            };
            if next.trim().is_empty() || !cursors.insert(next.clone()) {
                return Err(KitError::protocol(
                    "model inventory pagination is malformed",
                ));
            }
            cursor = Some(next);
        }
        Err(KitError::protocol(
            "model inventory exceeds its supported page count",
        ))
    }

    pub(crate) fn validate_model<P: ProviderBackend>(
        provider: &P,
        selector: &ModelSelector,
        operation: CustomProviderOperation,
    ) -> Result<ModelId, KitError> {
        let model_id = ModelId::new(selector.as_str().to_owned())
            .map_err(|error| operation.map_error(error))?;
        provider
            .model(model_id.clone())
            .map_err(|error| operation.map_error(error))?;
        Ok(model_id)
    }

    pub(crate) fn validate_reasoning(reasoning: Option<&ReasoningEffort>) -> Result<(), KitError> {
        reasoning.map(Self::reasoning).transpose().map(|_| ())
    }

    pub(crate) fn map_shutdown_error(error: providers_kit::KitError) -> KitError {
        CustomProviderOperation::Shutdown.map_error(error)
    }

    fn reasoning(effort: &ReasoningEffort) -> Result<ProviderReasoningEffort, KitError> {
        match effort.as_str() {
            "minimal" => Ok(ProviderReasoningEffort::Minimal),
            "low" => Ok(ProviderReasoningEffort::Low),
            "medium" => Ok(ProviderReasoningEffort::Medium),
            "high" => Ok(ProviderReasoningEffort::High),
            "maximum" => Ok(ProviderReasoningEffort::Maximum),
            _ => Err(KitError::unsupported(
                "requested reasoning effort cannot be represented",
            )),
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend> Clone
    for CustomProviderRuntime<S, D, P, T>
{
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            tools: self.tools.clone(),
            requests: self.requests.clone(),
            tool_approval: self.tool_approval,
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
    CustomProviderRuntime<S, D, P, T>
{
    pub(crate) fn new(
        provider: Arc<P>,
        tools: CustomTools<T>,
        requests: CustomServerRequests<S, D, T>,
        tool_approval: CustomToolApproval,
    ) -> Self {
        Self {
            provider,
            tools,
            requests,
            tool_approval,
        }
    }

    async fn execute_function(
        &self,
        conversation_id: ConversationId,
        call: FunctionCall,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<FunctionResult, KitError> {
        if *cancellation.borrow() {
            return Err(Self::cancelled_error());
        }
        match self.tool_approval {
            CustomToolApproval::Deny => {
                return self.denied_function_result(&call, None);
            }
            CustomToolApproval::Interaction => {
                let approval = tokio::select! {
                    biased;
                    changed = cancellation.changed() => {
                        let _ = changed;
                        return Err(Self::cancelled_error());
                    }
                    approval = self.requests.wait_for_tool_approval(
                        conversation_id.clone(),
                        call.name().to_owned(),
                        call.arguments().clone(),
                    ) => approval?,
                };
                match approval {
                    CustomApprovalDecision::Allow => {}
                    CustomApprovalDecision::Deny { reason } => {
                        return self.denied_function_result(&call, reason);
                    }
                }
            }
            CustomToolApproval::Allow => {}
        }

        if *cancellation.borrow() {
            return Err(Self::cancelled_error());
        }
        let mut handler = tokio::select! {
            biased;
            changed = cancellation.changed() => {
                let _ = changed;
                return Err(Self::cancelled_error());
            }
            handler = self.tools.handler(call.name()) => handler?,
        };
        let mut output = self
            .step_handler(
                &mut handler,
                ToolHandlerInput::Invoke(ToolInvocation {
                    call_id: call.id().map(str::to_owned),
                    arguments: call.arguments().clone(),
                }),
                &mut cancellation,
            )
            .await?;
        loop {
            match output {
                ToolHandlerOutput::Completed(result) => {
                    let is_error = result.is_error;
                    return FunctionResult::new(
                        call.id().map(str::to_owned),
                        call.name(),
                        self.tool_content(result)?,
                        is_error,
                    )
                    .map_err(|_| {
                        KitError::integration("tool result is malformed")
                            .with_detail(ErrorDetail::InvalidToolOutput)
                            .with_context(Operation::Prompt, ErrorPhase::ToolExecution)
                    });
                }
                ToolHandlerOutput::Interaction(request) => {
                    let response = tokio::select! {
                        biased;
                        changed = cancellation.changed() => {
                            let _ = changed;
                            return Err(self.cancel_handler(&mut handler, Self::cancelled_error()).await);
                        }
                        response = self.requests.wait_for_tool_interaction(
                            conversation_id.clone(),
                            request,
                        ) => match response {
                            Ok(response) => response,
                            Err(error) => return Err(self.cancel_handler(&mut handler, error).await),
                        },
                    };
                    output = self
                        .step_handler(
                            &mut handler,
                            ToolHandlerInput::Interaction(response),
                            &mut cancellation,
                        )
                        .await?;
                }
            }
        }
    }

    async fn step_handler(
        &self,
        handler: &mut T::Handler,
        input: ToolHandlerInput,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Result<ToolHandlerOutput, KitError> {
        if *cancellation.borrow() {
            return Err(self.cancel_handler(handler, Self::cancelled_error()).await);
        }
        let result = {
            let step = handler.step(input);
            tokio::pin!(step);
            tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    let _ = changed;
                    None
                }
                output = &mut step => Some(output),
            }
        };
        match result {
            Some(output) => output.map_err(|error| match error {
                tools_kit::ToolError::InvalidArguments { .. } => {
                    KitError::protocol("model tool arguments are invalid")
                        .with_detail(ErrorDetail::InvalidData)
                        .with_context(Operation::Prompt, ErrorPhase::ToolExecution)
                }
                error => KitError::from(error),
            }),
            None => Err(self.cancel_handler(handler, Self::cancelled_error()).await),
        }
    }

    async fn cancel_handler(&self, handler: &mut T::Handler, primary: KitError) -> KitError {
        let cleanup = handler
            .step(ToolHandlerInput::Cancel {
                reason: Some("prompt tool execution ended".to_owned()),
            })
            .await;
        match cleanup {
            Ok(_) => primary,
            Err(error) => {
                primary.with_supplementary(KitError::from(error).at_phase(ErrorPhase::Cleanup))
            }
        }
    }

    fn cancelled_error() -> KitError {
        KitError::cancelled("provider work was cancelled")
    }

    fn denied_function_result(
        &self,
        call: &FunctionCall,
        reason: Option<String>,
    ) -> Result<FunctionResult, KitError> {
        let text = reason
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(|| "tool execution was denied".to_owned());
        FunctionResult::new(
            call.id().map(str::to_owned),
            call.name(),
            vec![ContentPart::Text { text }],
            true,
        )
        .map_err(|_| KitError::invalid_state("tool denial result is malformed"))
    }

    fn tool_content(&self, result: ToolResult) -> Result<Vec<ContentPart>, KitError> {
        let mut content = Vec::with_capacity(result.content.len());
        for part in result.content {
            content.push(match part {
                ToolContent::Text { text, .. } => ContentPart::Text { text },
                ToolContent::Json { value, .. } => ContentPart::Json { value },
                ToolContent::Image {
                    mime_type,
                    data_base64,
                    ..
                } => ContentPart::Image {
                    media_type: mime_type,
                    data: STANDARD.decode(data_base64).map_err(|_| {
                        KitError::integration("tool image content is malformed")
                            .with_detail(ErrorDetail::InvalidToolOutput)
                            .with_context(Operation::Prompt, ErrorPhase::ToolExecution)
                    })?,
                },
                ToolContent::Audio {
                    mime_type,
                    data_base64,
                    ..
                } => ContentPart::Audio {
                    media_type: mime_type,
                    data: STANDARD.decode(data_base64).map_err(|_| {
                        KitError::integration("tool audio content is malformed")
                            .with_detail(ErrorDetail::InvalidToolOutput)
                            .with_context(Operation::Prompt, ErrorPhase::ToolExecution)
                    })?,
                },
                ToolContent::ResourceLink {
                    uri,
                    title,
                    metadata,
                } => ContentPart::Json {
                    value: serde_json::json!({
                        "uri": uri,
                        "title": title,
                        "metadata": metadata,
                    }),
                },
                ToolContent::EmbeddedResource {
                    uri,
                    contents,
                    metadata,
                } => ContentPart::Json {
                    value: serde_json::json!({
                        "uri": uri,
                        "contents": contents,
                        "metadata": metadata,
                    }),
                },
            });
        }
        if let Some(value) = result.structured_content
            && !content.iter().any(
                |part| matches!(part, ContentPart::Json { value: existing } if existing == &value),
            )
        {
            content.push(ContentPart::Json { value });
        }
        if content.is_empty() {
            return Err(
                KitError::integration("tool result omitted model-visible content")
                    .with_detail(ErrorDetail::InvalidToolOutput)
                    .with_context(Operation::Prompt, ErrorPhase::ToolExecution),
            );
        }
        Ok(content)
    }
}

impl CustomProviderCall {
    pub(crate) fn new(
        conversation_id: ConversationId,
        generation: CustomGeneration,
        model: ModelId,
        tools: ToolSelection,
        limits: CustomLoopLimits,
        output: Option<Arc<PromptOutput>>,
    ) -> Self {
        Self {
            conversation_id,
            generation,
            model,
            system: None,
            tools,
            limits,
            response_format: None,
            reasoning: None,
            output,
        }
    }

    pub(crate) fn with_prompt_overrides(
        mut self,
        system: Option<String>,
        structured: Option<&StructuredOutputRequest>,
        reasoning: Option<&ReasoningEffort>,
    ) -> Result<Self, KitError> {
        self.system = system;
        self.response_format = structured
            .map(|request| JsonSchema::new(request.schema().clone()))
            .transpose()
            .map_err(|_| KitError::invalid_state("structured-output contract is malformed"))?
            .map(ResponseFormat::JsonSchema);
        self.reasoning = reasoning
            .map(CustomProviderAdapter::reasoning)
            .transpose()?
            .map(|effort| ReasoningConfig {
                effort: Some(effort),
                ..ReasoningConfig::default()
            });
        Ok(self)
    }

    pub(crate) async fn execute<
        S: StateStoreBackend,
        D: DatabaseBackend,
        P: ProviderBackend,
        T: ToolsetBackend,
    >(
        self,
        runtime: CustomProviderRuntime<S, D, P, T>,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<PromptResponse, KitError> {
        let Self {
            conversation_id,
            generation,
            model,
            system,
            tools,
            limits,
            response_format,
            reasoning,
            output,
        } = self;
        let model = runtime
            .provider
            .model(model)
            .map_err(|error| CustomProviderOperation::Request.map_error(error))?;
        let mut messages = generation
            .into_messages::<<<P as ProviderBackend>::Model as ModelBackend>::Continuation>(
        )?;
        if let Some(system) = system {
            messages.insert(
                0,
                Message::system(vec![ContentPart::Text { text: system }])
                    .map_err(|error| CustomProviderOperation::Request.map_error(error))?,
            );
        }
        let provider_tools = runtime.tools.provider_tools(&tools);
        let mut required_pending = matches!(
            tools,
            ToolSelection::Required | ToolSelection::RequiredOneOf(_)
        );
        let mut used_actions = 0_usize;
        let mut input_tokens = 0_u64;
        let mut output_tokens = 0_u64;
        let mut has_complete_usage = true;

        for _ in 0..limits.max_model_turns() {
            if *cancellation.borrow() {
                return Err(CustomProviderRuntime::<S, D, P, T>::cancelled_error());
            }
            let generation = GenerationConfig::default()
                .with_tool_choice(Self::tool_choice(&tools, required_pending));
            let mut request = GenerateRequest::new(messages.clone())
                .map_err(|_| KitError::invalid_state("model request could not be assembled"))?
                .with_tools(provider_tools.clone())
                .map_err(|_| KitError::invalid_state("model request could not be assembled"))?
                .with_generation(generation)
                .map_err(|_| KitError::invalid_state("model request could not be assembled"))?;
            if let Some(response_format) = response_format.clone() {
                request = request.with_response_format(response_format);
            }
            if let Some(reasoning) = reasoning.clone() {
                request = request.with_reasoning(reasoning);
            }
            let completion = async {
                let Some(output) = output.as_deref() else {
                    return model
                        .generate(&request)
                        .await
                        .map_err(|error| CustomProviderOperation::Request.map_error(error));
                };
                let mut stream = model
                    .generate_stream(&request)
                    .await
                    .map_err(|error| CustomProviderOperation::Request.map_error(error))?;
                loop {
                    tokio::task::consume_budget().await;
                    match stream.next().await {
                        Some(Ok(GenerateEvent::ContentDelta { delta })) => output.fragment(&delta),
                        Some(Ok(GenerateEvent::Completed(response))) => return Ok(*response),
                        Some(Ok(
                            GenerateEvent::Started { .. }
                            | GenerateEvent::ThinkingDelta { .. }
                            | GenerateEvent::HostedTool(_)
                            | GenerateEvent::Usage(_),
                        )) => {}
                        Some(Err(error)) => {
                            return Err(CustomProviderOperation::Request.map_error(error));
                        }
                        None => {
                            return Err(KitError::protocol(
                                "model output ended before its terminal response",
                            )
                            .with_detail(ErrorDetail::IncompleteStream)
                            .with_context(Operation::Prompt, ErrorPhase::CompletionValidation));
                        }
                    }
                }
            };
            let response = tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    let _ = changed;
                    return Err(CustomProviderRuntime::<S, D, P, T>::cancelled_error());
                }
                response = completion => response?,
            };
            Self::validate_completion(&response)?;
            runtime
                .tools
                .validate_calls(&tools, response.message.function_calls())?;
            match (response.usage.input_tokens, response.usage.output_tokens) {
                (Some(input), Some(output)) if has_complete_usage => {
                    input_tokens = input_tokens.saturating_add(input);
                    output_tokens = output_tokens.saturating_add(output);
                }
                _ => has_complete_usage = false,
            }
            let calls = response.message.function_calls().to_vec();
            if !calls.is_empty() {
                required_pending = false;
            }
            messages.push(response.message);
            if calls.is_empty() {
                if required_pending {
                    return Err(KitError::protocol(
                        "model completed before satisfying the required tool selection",
                    )
                    .with_detail(ErrorDetail::InvalidSequence)
                    .with_context(Operation::Prompt, ErrorPhase::CompletionValidation));
                }
                let usage = has_complete_usage.then(|| UsageInfo::new(input_tokens, output_tokens));
                return Self::terminal_response(response_format.as_ref(), messages.last(), usage);
            }
            used_actions = used_actions.saturating_add(calls.len());
            if used_actions > limits.max_tool_actions() {
                return Err(KitError::invalid_state(
                    "model exceeded the configured tool-action limit",
                ));
            }
            let parallel = limits.max_parallel_tool_actions();
            let executions = stream::iter(calls.into_iter().map(|call| {
                let runtime = runtime.clone();
                let conversation_id = conversation_id.clone();
                let cancellation = cancellation.clone();
                async move {
                    runtime
                        .execute_function(conversation_id, call, cancellation)
                        .await
                }
            }))
            .buffered(parallel)
            .collect::<Vec<_>>();
            let results = executions.await;
            if *cancellation.borrow() {
                return Err(CustomProviderRuntime::<S, D, P, T>::cancelled_error());
            }
            messages.push(
                Message::tool(results.into_iter().collect::<Result<Vec<_>, _>>()?)
                    .map_err(|_| KitError::invalid_state("tool results could not be assembled"))?,
            );
        }
        Err(KitError::invalid_state(
            "model exceeded the configured turn limit",
        ))
    }

    fn validate_completion<C>(response: &GenerateResponse<C>) -> Result<(), KitError> {
        let result = match response.finish_reason.as_ref() {
            Some(FinishReason::Stop) => {
                if !response.message.function_calls().is_empty() {
                    return Err(KitError::protocol(
                        "model reported a natural stop with pending function calls",
                    )
                    .with_detail(ErrorDetail::InvalidSequence)
                    .with_context(Operation::Prompt, ErrorPhase::CompletionValidation));
                }
                Ok(())
            }
            Some(FinishReason::ToolCall) => {
                if response.message.function_calls().is_empty() {
                    return Err(KitError::protocol(
                        "model requested function execution without function calls",
                    )
                    .with_detail(ErrorDetail::InvalidSequence)
                    .with_context(Operation::Prompt, ErrorPhase::CompletionValidation));
                }
                Ok(())
            }
            Some(FinishReason::Error) => Err(KitError::integration("model generation failed")),
            Some(FinishReason::Length) => Err(KitError::integration(
                "model generation stopped at its output limit",
            )
            .with_detail(ErrorDetail::OutputLimit)),
            Some(FinishReason::ContentFilter) => {
                Err(KitError::integration("model generation was filtered")
                    .with_detail(ErrorDetail::ContentFiltered))
            }
            Some(FinishReason::Paused) => Err(KitError::not_yet_implemented(
                "provider-managed model continuation is not implemented",
            )
            .with_detail(ErrorDetail::ContinuationRequired)),
            None => Err(
                KitError::protocol("model response omitted its completion reason")
                    .with_detail(ErrorDetail::MissingData),
            ),
        };
        result.map_err(|error| {
            error.with_context(Operation::Prompt, ErrorPhase::CompletionValidation)
        })
    }

    fn tool_choice(selection: &ToolSelection, required_pending: bool) -> ToolChoice {
        match selection {
            ToolSelection::Auto => ToolChoice::Auto,
            ToolSelection::None => ToolChoice::None,
            ToolSelection::Required if required_pending => ToolChoice::Required,
            ToolSelection::Required => ToolChoice::Auto,
            ToolSelection::Allowed(names) => ToolChoice::Allowed {
                mode: AllowedToolMode::Auto,
                names: names.clone(),
            },
            ToolSelection::RequiredOneOf(names) => ToolChoice::Allowed {
                mode: if required_pending {
                    AllowedToolMode::Required
                } else {
                    AllowedToolMode::Auto
                },
                names: names.clone(),
            },
        }
    }

    fn terminal_response<C>(
        response_format: Option<&ResponseFormat>,
        message: Option<&Message<C>>,
        usage: Option<UsageInfo>,
    ) -> Result<PromptResponse, KitError> {
        let message = message.ok_or_else(|| {
            KitError::protocol("model response omitted its assistant message")
                .with_detail(ErrorDetail::MissingData)
                .with_context(Operation::Prompt, ErrorPhase::CompletionValidation)
        })?;
        let output = message
            .content()
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        if output.trim().is_empty() {
            return Err(
                KitError::protocol("model response omitted terminal assistant text")
                    .with_detail(ErrorDetail::MissingData)
                    .with_context(Operation::Prompt, ErrorPhase::CompletionValidation),
            );
        }
        let structured_output = match response_format {
            Some(ResponseFormat::JsonSchema(schema)) => {
                let value = serde_json::from_str::<serde_json::Value>(&output).map_err(|_| {
                    KitError::protocol("structured response is not valid JSON")
                        .with_detail(ErrorDetail::MalformedData)
                        .with_context(Operation::Prompt, ErrorPhase::CompletionValidation)
                })?;
                let validator = jsonschema::draft202012::options()
                    .build(schema.schema())
                    .map_err(|_| {
                        KitError::invalid_state("structured schema could not be compiled")
                    })?;
                if !validator.is_valid(&value) {
                    return Err(KitError::protocol(
                        "structured response does not satisfy the requested schema",
                    )
                    .with_detail(ErrorDetail::StructuredOutputMismatch)
                    .with_context(Operation::Prompt, ErrorPhase::CompletionValidation));
                }
                Some(value)
            }
            Some(ResponseFormat::JsonObject) => {
                let value = serde_json::from_str::<serde_json::Value>(&output).map_err(|_| {
                    KitError::protocol("structured response is not valid JSON")
                        .with_detail(ErrorDetail::MalformedData)
                        .with_context(Operation::Prompt, ErrorPhase::CompletionValidation)
                })?;
                if !value.is_object() {
                    return Err(
                        KitError::protocol("structured response is not a JSON object")
                            .with_detail(ErrorDetail::StructuredOutputMismatch)
                            .with_context(Operation::Prompt, ErrorPhase::CompletionValidation),
                    );
                }
                Some(value)
            }
            Some(ResponseFormat::Text) | None => None,
        };
        Ok(PromptResponse::from_parts(output, structured_output, usage))
    }
}

impl CustomProviderOperation {
    fn map_error(self, error: providers_kit::KitError) -> KitError {
        let cause = KitError::from_provider(&error);
        let kind = match (self, error.kind()) {
            (Self::Build, ErrorKind::Configuration) => AgentErrorKind::InvalidConfig,
            (Self::Build, ErrorKind::InvalidInput) => AgentErrorKind::Unsupported,
            (Self::Inventory, ErrorKind::InvalidInput) => AgentErrorKind::InvalidState,
            (Self::Request, ErrorKind::InvalidInput) => AgentErrorKind::InvalidInput,
            _ => cause.kind(),
        };
        let (operation, phase, message) = match self {
            Self::Build => (
                Operation::Build,
                ErrorPhase::ModelSelection,
                "model selection request failed",
            ),
            Self::Inventory => (
                Operation::ListModels,
                ErrorPhase::InventoryVerification,
                "model inventory request failed",
            ),
            Self::Request => (
                Operation::Prompt,
                ErrorPhase::ModelGeneration,
                "model request failed",
            ),
            Self::Shutdown => (
                Operation::Shutdown,
                ErrorPhase::Cleanup,
                "model runtime shutdown failed",
            ),
        };
        let mut mapped = KitError::new(kind, message).with_context(operation, phase);
        if let Some(detail) = cause.detail() {
            mapped = mapped.with_detail(*detail);
        }
        if let Some(status) = cause.http_status() {
            mapped = mapped.with_http_status(status);
        }
        mapped.with_cause(cause).at_operation(operation)
    }
}
