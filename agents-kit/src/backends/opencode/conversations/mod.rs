use crate::error::ErrorKind;
use crate::error::{ErrorPhase, Operation};
pub(crate) mod runtime;

use crate::backends::opencode::state_store::OpenCodePromptGeneration;
use crate::prompt::{PromptOutput, PromptStream};
use runtime::OpenCodePromptWaiterState;
use std::path::PathBuf;

use crate::backends::opencode::adapters::OpenCodeModelInventory;
use crate::backends::opencode::knowledge_base::OpenCodeKnowledgeBase;
use crate::backends::opencode::protocol::http::OpenCodeMutationError;
use crate::backends::opencode::protocol::requests::{OpenCodeMessageRequest, OpenCodeRouting};
use crate::backends::opencode::server_requests::OpenCodeServerRequests;
use crate::backends::opencode::state_store::OpenCodeConversationMutationDisposition;
use crate::backends::opencode::state_store::records::OpenCodeCleanup;
use crate::conversation::ConversationBackend;
use crate::core::hierarchy::{HierarchyAdmission, HierarchyClaim, HierarchyRuntime};
use crate::core::notification_store::ResolutionSettlement;
use crate::core::state::ConversationRecord;
use crate::{
    ConversationId, ConversationInfo, ConversationNotificationReceiver, FileRead, KitError,
    NotificationId, NotificationResolution, PromptRequest, PromptResponse, ToolSelection,
    WorkspaceDefaults,
};
use storage_kit::{DatabaseBackend, StateStoreBackend};

pub(crate) struct OpenCodeConversation<S: StateStoreBackend, D: DatabaseBackend> {
    record: ConversationRecord,
    hierarchy: HierarchyRuntime,
    claim: HierarchyClaim,
    lifecycle_timeout: std::time::Duration,
    runtime: runtime::OpenCodeConversationRuntime<S>,
    knowledge: OpenCodeKnowledgeBase<S, D>,
}

impl<S: StateStoreBackend, D: DatabaseBackend> OpenCodeConversation<S, D> {
    pub(crate) fn new(
        record: ConversationRecord,
        hierarchy: HierarchyRuntime,
        claim: HierarchyClaim,
        lifecycle_timeout: std::time::Duration,
        runtime: runtime::OpenCodeConversationRuntime<S>,
        knowledge: OpenCodeKnowledgeBase<S, D>,
    ) -> Self {
        Self {
            record,
            hierarchy,
            claim,
            lifecycle_timeout,
            runtime,
            knowledge,
        }
    }

    fn publish_caller_result<T>(
        &self,
        admission: HierarchyAdmission,
        result: Result<T, KitError>,
    ) -> Result<T, KitError> {
        admission.publish(result)
    }

    async fn current(&self) -> Result<ConversationRecord, KitError> {
        self.claim.ensure_open()?;
        let workspace = self
            .runtime
            .state()
            .workspace(self.runtime.agent_id(), self.record.workspace_id())
            .await?;
        if workspace.revision() != self.record.workspace_revision() {
            return Err(KitError::conflict("conversation handle is stale"));
        }
        let record = self
            .runtime
            .state()
            .conversation(self.record.workspace_id(), self.record.id())
            .await?;
        if record.workspace_revision() != workspace.revision() {
            return Err(KitError::invalid_state(
                "conversation revision contradicts its workspace",
            ));
        }
        Ok(record)
    }

    async fn cleanup(&self) -> Result<OpenCodeCleanup, KitError> {
        let root = self.runtime.binding().require_session_id()?.to_owned();
        let sessions = self
            .runtime
            .http()
            .capture_subtree(
                std::slice::from_ref(&root),
                self.runtime.binding().directory(),
            )
            .await?;
        OpenCodeCleanup::conversation(
            self.runtime.agent_id().clone(),
            self.record.id().clone(),
            self.runtime.binding().directory().to_path_buf(),
            root,
            sessions,
        )
    }
    fn prompt_deadline(&self, request: &PromptRequest) -> Option<tokio::time::Instant> {
        request
            .timeout()
            .or(self.runtime.prompt_policy().default_timeout())
            .map(|timeout| tokio::time::Instant::now() + timeout)
    }

    async fn dispatch_prompt(
        &self,
        request: PromptRequest,
        deadline: Option<tokio::time::Instant>,
        waiter: OpenCodePromptWaiterState,
    ) -> Result<OpenCodePromptGeneration<S>, KitError> {
        let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
        let now = tokio::time::Instant::now();
        let lifecycle_deadline = now + self.lifecycle_timeout;
        let state_admission_deadline = deadline.map_or(lifecycle_deadline, |deadline| {
            deadline.min(lifecycle_deadline)
        });
        let preflight = async {
            let record = self.current().await.map_err(|error| (error, false))?;
            let conversation_defaults = record.defaults().map_err(|error| (error, false))?;
            let workspace = self
                .runtime
                .state()
                .workspace(self.runtime.agent_id(), record.workspace_id())
                .await
                .map_err(|error| (error, false))?;
            let workspace_info = workspace.info().map_err(|error| (error, false))?;
            if request.cwd().is_some() {
                return Err((
                    KitError::unsupported("per-prompt working-directory selection is unsupported"),
                    false,
                ));
            }
            let model = request
                .model()
                .or_else(|| {
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::model)
                })
                .or_else(|| workspace_info.defaults().model());
            let reasoning_effort = request
                .reasoning_effort()
                .or_else(|| {
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::reasoning_effort)
                })
                .or_else(|| workspace_info.defaults().reasoning_effort());
            let personality = request
                .personality()
                .or_else(|| {
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::personality)
                })
                .or_else(|| workspace_info.defaults().personality());
            if reasoning_effort.is_some() || personality.is_some() {
                return Err((
                    KitError::unsupported("reasoning and personality selection are unsupported"),
                    false,
                ));
            }
            if request
                .tool_selection()
                .is_some_and(|selection| !matches!(selection, ToolSelection::Auto))
            {
                return Err((
                    KitError::unsupported("non-automatic tool selection is unsupported"),
                    false,
                ));
            }
            let native_routing = tokio::time::timeout_at(
                state_admission_deadline,
                self.runtime.state().workspace_routing(&workspace),
            )
            .await
            .map_err(|_| {
                (
                    KitError::timeout("prompt routing read deadline elapsed")
                        .at_phase(ErrorPhase::StateRead),
                    false,
                )
            })?
            .map_err(|error| (error, false))?;
            self.runtime
                .http()
                .verify_directory(
                    workspace.cwd(),
                    native_routing.directory(),
                    state_admission_deadline,
                )
                .await
                .map_err(|error| (error, false))?;
            let routing = OpenCodeRouting::for_directory(native_routing.directory().to_path_buf())
                .map_err(|error| (error, false))?;
            let selected = if let Some(model) = model {
                let providers = self
                    .runtime
                    .http()
                    .providers(&routing)
                    .await
                    .map_err(|error| (error, false))?;
                let inventory = OpenCodeModelInventory::from_native(providers)
                    .map_err(|error| (error, false))?;
                Some(inventory.select(model).map_err(|error| (error, false))?)
            } else {
                None
            };
            let system = self
                .knowledge
                .render_system(
                    workspace.id(),
                    record.id(),
                    conversation_defaults
                        .as_ref()
                        .and_then(WorkspaceDefaults::knowledge_policy),
                    workspace_info.defaults().knowledge_policy(),
                    &request,
                    request
                        .instructions()
                        .or_else(|| self.runtime.prompt_policy().instructions()),
                )
                .await
                .map_err(|error| (error, true))?;
            let mut native = OpenCodeMessageRequest::new(request.text().to_owned(), system)
                .map_err(|error| (error, false))?;
            if let Some(selected) = &selected {
                native = native.select_model(selected.native().request().clone());
            }
            let structured_schema = request
                .structured_output()
                .map(|structured| structured.schema().clone());
            if let Some(schema) = &structured_schema {
                native = native
                    .require_structured(schema.clone())
                    .map_err(|error| (error, false))?;
            }
            Ok::<_, (KitError, bool)>((native, structured_schema))
        };
        let preflight = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, preflight).await {
                Ok(preflight) => preflight,
                Err(_) => {
                    return self.publish_caller_result(
                        admission,
                        Err(KitError::timeout("caller prompt deadline elapsed")),
                    );
                }
            },
            None => preflight.await,
        };
        let (native, structured_schema) = match preflight {
            Ok(preflight) => preflight,
            Err((error, true)) => return admission.publish(Err(error)),
            Err((error, false)) => return self.publish_caller_result(admission, Err(error)),
        };
        self.runtime.start_prompt(
            native,
            structured_schema,
            deadline,
            state_admission_deadline,
            admission,
            waiter,
        )
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend> ConversationBackend for OpenCodeConversation<S, D> {
    fn id(&self) -> ConversationId {
        self.record.id().clone()
    }

    async fn info(&self) -> Result<ConversationInfo, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
            let result = async { self.current().await?.info() }.await;
            self.publish_caller_result(admission, result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ConversationInfo)
                .in_phase(ErrorPhase::StateRead)
        })
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, KitError> {
        let result = async {
            let deadline = self.prompt_deadline(&request);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let generation = self
                .dispatch_prompt(
                    request,
                    deadline,
                    OpenCodePromptWaiterState::Waiting(sender),
                )
                .await?;
            self.runtime
                .await_prompt(generation, receiver, deadline)
                .await
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::Prompt)
                .in_phase(ErrorPhase::ModelGeneration)
        })
    }

    async fn prompt_stream(&self, request: PromptRequest) -> Result<PromptStream, KitError> {
        let result = async {
            let deadline = self.prompt_deadline(&request);
            let (output, stream) = PromptOutput::channel();
            self.dispatch_prompt(
                request,
                deadline,
                OpenCodePromptWaiterState::Streaming(output),
            )
            .await?;
            Ok(stream)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::Prompt)
                .in_phase(ErrorPhase::ModelGeneration)
        })
    }

    async fn abort(&self) -> Result<(), KitError> {
        let result = async {
            let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
            let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
            let result = async {
                self.current().await?;
                self.runtime.abort(deadline).await
            }
            .await;
            self.publish_caller_result(admission, result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::Abort)
                .in_phase(ErrorPhase::Cancellation)
        })
    }

    async fn read_file(&self, _: PathBuf) -> Result<FileRead, KitError> {
        let result = async {
            let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
            let result = self.current().await.and(Err(KitError::unsupported(
                "byte-exact file reads are unsupported",
            )));
            self.publish_caller_result(admission, result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ReadFile)
                .in_phase(ErrorPhase::InputValidation)
        })
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, KitError> {
        let result = (|| {
            let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
            let result = self.claim.ensure_open().and_then(|()| {
                self.runtime
                    .state()
                    .notifications()
                    .receiver(self.record.workspace_id(), self.record.id())
                    .map(ConversationNotificationReceiver::shared)
            });
            self.publish_caller_result(admission, result)
        })();
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::NotificationStream)
                .in_phase(ErrorPhase::Admission)
        })
    }

    async fn resolve_notification(
        &self,
        id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<(), KitError> {
        let result = async {
            let admission = self.hierarchy.admit_work(self.record.workspace_id())?;
            let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
            let result = async {
                self.current().await?;
                let continuation = self
                    .runtime
                    .state()
                    .continuation(self.record.id(), &id)
                    .await?;
                let session_id = self.runtime.binding().require_session_id()?;
                if continuation.conversation_id() != self.record.id()
                    || continuation.session_id() != session_id
                {
                    return Err(KitError::not_found(
                        "notification continuation does not belong to this conversation",
                    ));
                }
                let requests = OpenCodeServerRequests::for_binding(self.runtime.binding())?;
                let native = requests.native(&continuation, &resolution)?;
                let intent = self
                    .runtime
                    .state()
                    .notifications()
                    .begin_resolution(self.record.id(), id, resolution, deadline)
                    .await?;
                match requests.send(self.runtime.http(), native).await {
                    Ok(()) => {
                        let actual = self
                            .runtime
                            .state()
                            .settle_interaction(
                                &intent,
                                &continuation,
                                ResolutionSettlement::Accepted,
                                deadline,
                            )
                            .await?;
                        if actual == ResolutionSettlement::Accepted {
                            Ok(())
                        } else {
                            Err(KitError::invalid_state(
                                "notification resolution outcome is inconsistent",
                            ))
                        }
                    }
                    Err(error) => {
                        let (settlement, error) = match error {
                            OpenCodeMutationError::Rejected(error) => {
                                (ResolutionSettlement::Rejected, error)
                            }
                            OpenCodeMutationError::Ambiguous(error) => {
                                (ResolutionSettlement::Ambiguous, error)
                            }
                        };
                        let actual = self
                            .runtime
                            .state()
                            .settle_interaction(&intent, &continuation, settlement, deadline)
                            .await
                            .map_err(|settlement_error| {
                                settlement_error.with_supplementary(error.clone())
                            })?;
                        match actual {
                            ResolutionSettlement::Accepted => Ok(()),
                            actual if actual == settlement => Err(error),
                            ResolutionSettlement::Rejected
                            | ResolutionSettlement::Ambiguous
                            | ResolutionSettlement::Cancelled => Err(KitError::invalid_state(
                                "notification resolution outcome is inconsistent",
                            )),
                        }
                    }
                }
            }
            .await;
            self.publish_caller_result(admission, result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::ResolveNotification)
                .in_phase(ErrorPhase::InteractionResolution)
        })
    }

    async fn close(mut self) -> Result<(), KitError> {
        let result = async {
            let deadline = tokio::time::Instant::now() + self.lifecycle_timeout;
            let mut admission = self
                .hierarchy
                .admit_conversation_close(self.record.workspace_id(), self.record.id(), deadline)
                .await?;
            let current = async {
                tokio::time::timeout_at(deadline, self.current())
                    .await
                    .map_err(|_| KitError::timeout("conversation close deadline elapsed"))?
                    .map_err(|error| match error {
                        error if error.kind() == ErrorKind::NotFound => {
                            KitError::closed("conversation was deleted while close was admitted")
                        }
                        error => error,
                    })?;
                tokio::time::timeout_at(
                    deadline,
                    self.runtime
                        .state()
                        .close_requires_interaction_preparation(self.record.id()),
                )
                .await
                .map_err(|_| KitError::timeout("conversation close deadline elapsed"))?
            }
            .await;
            let needs_preparation = match current {
                Ok(needs_preparation) => needs_preparation,
                Err(error) => return admission.publish(Err(error)),
            };
            // An ordinary terminal prompt may still own a native question. Settle
            // that request before abort waits for its continuation to become clear.
            // Active prompts retain the existing abort-first ordering.
            if needs_preparation {
                let (claim, returned) = self
                    .runtime
                    .prepare_close(self.record.clone(), self.claim, admission, deadline)
                    .await?;
                self.claim = claim;
                admission = returned;
            }
            let cleanup = match async {
                self.runtime.abort(deadline).await?;
                tokio::time::timeout_at(deadline, self.cleanup())
                    .await
                    .map_err(|_| KitError::timeout("conversation close deadline elapsed"))?
            }
            .await
            {
                Ok(cleanup) => cleanup,
                Err(error) => return admission.publish(Err(error)),
            };
            // Work accepted before Close can publish another interaction during
            // abort. This second pass uses the same original deadline and claim.
            let (claim, admission) = self
                .runtime
                .prepare_close(self.record.clone(), self.claim, admission, deadline)
                .await?;
            self.claim = claim;
            let result = async {
                if tokio::time::Instant::now() >= deadline {
                    return Err(KitError::timeout("conversation close deadline elapsed"));
                }
                let completion = self
                    .runtime
                    .state()
                    .close_conversation(
                        self.runtime.agent_id(),
                        &self.record,
                        &cleanup,
                        deadline,
                        self.lifecycle_timeout,
                    )
                    .await
                    .map_err(|error| match error {
                        error if error.kind() == ErrorKind::NotFound => {
                            KitError::closed("conversation was deleted while close was admitted")
                        }
                        error => error,
                    })?;
                self.hierarchy
                    .close_conversation(self.record.workspace_id(), self.record.id());
                match completion {
                    OpenCodeConversationMutationDisposition::Settled => Ok(()),
                    OpenCodeConversationMutationDisposition::RecoveryRequired(error) => Err(error),
                }
            }
            .await;
            if result.is_ok() {
                let owner = self
                    .knowledge
                    .conversation_owner(self.record.workspace_id(), self.record.id());
                let cleanup_attempt = async {
                    let _ = tokio::join!(
                        async {
                            if self.runtime.http().settle_cleanup(&cleanup).await.is_ok() {
                                let _ = self.runtime.state().finish_cleanup(&cleanup).await;
                            }
                        },
                        self.knowledge.settle_forget(&owner, deadline),
                    );
                };
                let _ = tokio::time::timeout_at(deadline, cleanup_attempt).await;
            }
            self.publish_caller_result(admission, result)
        }
        .await;
        result.map_err(|error: KitError| {
            error
                .at_operation(Operation::CloseConversation)
                .in_phase(ErrorPhase::Cleanup)
        })
    }
}
