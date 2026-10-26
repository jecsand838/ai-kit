use std::collections::BTreeSet;
use std::path::PathBuf;

use storage_sdk::StateStoreBackend;
use tokio::time::Instant;

use crate::SdkError;
use crate::backends::opencode::agent::events::{
    OpenCodeEventOwner, OpenCodeLiveEvents, OpenCodeRecoveryFailure,
};
use crate::backends::opencode::protocol::inbound::OpenCodeKnownEvent;
use crate::backends::opencode::protocol::requests::OpenCodeRouting;
use crate::backends::opencode::server_requests::OpenCodeServerRequests;
use crate::backends::opencode::state_store::records::{
    OpenCodeContinuation, OpenCodeContinuationRequest, OpenCodeContinuationState,
    OpenCodeSessionBinding,
};
use crate::core::state::ConversationRecord;
use crate::error::ErrorPhase;

impl<S: StateStoreBackend> OpenCodeEventOwner<S> {
    pub(super) async fn cancel_pending_native(
        &mut self,
        live: &mut OpenCodeLiveEvents,
        pending: &mut Vec<(OpenCodeKnownEvent, PathBuf)>,
        native_pending: &mut BTreeSet<(String, String)>,
        closing: Option<&ConversationRecord>,
        deadline: Instant,
    ) -> Result<bool, OpenCodeRecoveryFailure> {
        let mut settled = BTreeSet::new();
        for (event, directory) in pending.iter() {
            let request = match event {
                OpenCodeKnownEvent::PermissionAsked(request) => {
                    OpenCodeContinuationRequest::Permission(request.clone())
                }
                OpenCodeKnownEvent::QuestionAsked(request) => {
                    OpenCodeContinuationRequest::Question(request.clone())
                }
                _ => {
                    return Err(
                        SdkError::invalid_state("pending cleanup received another event").into(),
                    );
                }
            };
            let identity = (request.session_id().to_owned(), request.id().to_owned());
            if settled.contains(&identity) {
                continue;
            }
            let Some(binding) = self
                .binding_for_event(request.session_id(), Some(directory))
                .await?
            else {
                continue;
            };
            let durable = live
                .capture_while(self.state.native_continuations(binding.conversation_id()))
                .await??;
            let Some(original) = durable
                .iter()
                .find(|continuation| continuation.request_id() == request.id())
            else {
                continue;
            };
            if original.request() != &request {
                return Err(SdkError::protocol("native interaction identity was reused").into());
            }
            self.state
                .native_cleanup_failure(original, deadline)
                .map_err(OpenCodeRecoveryFailure::Parked)?;
            let closing = closing.filter(|record| record.id() == binding.conversation_id());
            match original.state() {
                OpenCodeContinuationState::CancellationRequired => {}
                OpenCodeContinuationState::Pending if closing.is_some() => {}
                OpenCodeContinuationState::CancellationDispatched { .. }
                | OpenCodeContinuationState::Closing { .. } => {
                    return Err(SdkError::integration(
                        "native interaction cancellation remains unconfirmed",
                    )
                    .at_phase(ErrorPhase::Reconciliation)
                    .into());
                }
                _ => continue,
            }
            let mut covered = Vec::new();
            for (peer, peer_directory) in pending.iter() {
                let peer = match peer {
                    OpenCodeKnownEvent::PermissionAsked(peer)
                        if original.is_permission()
                            && peer.session_id() == original.session_id() =>
                    {
                        OpenCodeContinuationRequest::Permission(peer.clone())
                    }
                    OpenCodeKnownEvent::QuestionAsked(peer)
                        if !original.is_permission()
                            && peer.session_id() == original.session_id()
                            && peer.id() == original.request_id() =>
                    {
                        OpenCodeContinuationRequest::Question(peer.clone())
                    }
                    _ => continue,
                };
                if peer_directory != binding.directory() {
                    return Err(SdkError::conflict(
                        "native cancellation peer belongs to another directory",
                    )
                    .into());
                }
                let owner = durable
                    .iter()
                    .find(|owner| owner.request_id() == peer.id())
                    .ok_or_else(|| {
                        SdkError::conflict(
                            "native cancellation would affect an unowned interaction",
                        )
                    })?;
                if owner.request() != &peer {
                    return Err(
                        SdkError::protocol("native cancellation peer identity changed").into(),
                    );
                }
                if !(matches!(
                    owner.state(),
                    OpenCodeContinuationState::CancellationRequired
                ) || (closing.is_some()
                    && owner.state() == OpenCodeContinuationState::Pending))
                {
                    return Err(SdkError::conflict(
                        "native cancellation peer has another outcome owner",
                    )
                    .into());
                }
                if closing.is_none() {
                    self.verify_cancelled_generation(live, &binding, owner, deadline)
                        .await?;
                }
                covered.push(owner.clone());
            }
            if Instant::now() >= deadline {
                return Err(SdkError::timeout("interaction cancellation deadline elapsed").into());
            }
            // An uncertain local dispatch commit must not escape into the event
            // recovery loop and acquire a new mutation attempt.
            let reserved = live
                .capture_until_settled(
                    self.state
                        .reserve_native_cancellation(&binding, &covered, closing, deadline),
                )
                .await?
                .map_err(OpenCodeRecoveryFailure::Parked)?;
            if Instant::now() >= deadline {
                return Err(SdkError::timeout(
                    "interaction cancellation dispatch deadline elapsed",
                )
                .into());
            }
            let requests = OpenCodeServerRequests::for_binding(&binding)?;
            let native = requests.cancellation(reserved.first().ok_or_else(|| {
                SdkError::invalid_state("native cancellation covered no requests")
            })?)?;
            let http = self.http.clone();
            // Retain the native receipt separately: event capture can fail while
            // the admitted HTTP operation still completes successfully.
            let mut receipt = None;
            let capture = live
                .capture_until_settled(async {
                    let result = requests
                        .send(&http, native)
                        .await
                        .map_err(|error| error.into_sdk_error());
                    receipt = Some(result.clone());
                    result
                })
                .await;
            let receipt = receipt.ok_or_else(|| {
                SdkError::invalid_state("native cancellation did not settle its admitted request")
            })?;
            if receipt.is_ok() {
                // A selected HTTP acknowledgement settles the preflighted set,
                // even when its event delivery was lost. No second native send.
                self.state
                    .settle_native_cancellation(&binding, &reserved, deadline)
                    .await
                    .map_err(OpenCodeRecoveryFailure::Stop)?;
            }
            // Applying captured acknowledgements must happen outside the owned
            // HTTP future: waiting for their application inside it deadlocks.
            let mut overlap = None;
            let applied = self
                .apply_buffered_overlap(
                    live,
                    &mut overlap,
                    "interaction cancellation retains an event gap",
                    deadline,
                )
                .await;
            let observation_error = match applied {
                Err(error) => Some(error),
                Ok(()) => overlap.map(OpenCodeRecoveryFailure::from),
            };
            if let Some(observation) = observation_error {
                return Err(match receipt {
                    Err(primary) => OpenCodeRecoveryFailure::from(primary.with_supplementary(
                        observation.into_sdk().at_phase(ErrorPhase::Reconciliation),
                    )),
                    Ok(()) => observation,
                });
            }
            if let Err(primary) = receipt {
                let observed = live
                    .capture_while(self.state.native_continuations(binding.conversation_id()))
                    .await;
                let observed = match observed {
                    Ok(Ok(observed)) => observed,
                    Ok(Err(secondary)) => {
                        return Err(primary
                            .with_supplementary(secondary.at_phase(ErrorPhase::Reconciliation))
                            .into());
                    }
                    Err(secondary) => {
                        return Err(primary
                            .with_supplementary(
                                secondary.into_sdk().at_phase(ErrorPhase::Reconciliation),
                            )
                            .into());
                    }
                };
                if !reserved.iter().all(|expected| {
                    observed.iter().any(|actual| {
                        actual.has_same_request(expected)
                            && actual.state() == OpenCodeContinuationState::Cancelled
                    })
                }) {
                    return Err(match capture {
                        Err(secondary) => primary
                            .with_supplementary(
                                secondary.into_sdk().at_phase(ErrorPhase::Reconciliation),
                            )
                            .into(),
                        Ok(_) => primary.into(),
                    });
                }
            }
            capture.map(|_| ())?;
            for continuation in reserved {
                settled.insert((
                    continuation.session_id().to_owned(),
                    continuation.request_id().to_owned(),
                ));
            }
        }
        pending.retain(|(event, _)| {
            let identity = match event {
                OpenCodeKnownEvent::PermissionAsked(request) => {
                    (request.session_id().to_owned(), request.id().to_owned())
                }
                OpenCodeKnownEvent::QuestionAsked(request) => {
                    (request.session_id().to_owned(), request.id().to_owned())
                }
                _ => return true,
            };
            !settled.contains(&identity)
        });
        native_pending.retain(|identity| !settled.contains(identity));
        Ok(!settled.is_empty())
    }

    async fn verify_cancelled_generation(
        &self,
        live: &mut OpenCodeLiveEvents,
        binding: &OpenCodeSessionBinding,
        continuation: &OpenCodeContinuation,
        deadline: Instant,
    ) -> Result<(), OpenCodeRecoveryFailure> {
        let prompt = live
            .capture_while(self.state.native_cancellation_parent(continuation))
            .await??;
        let origin = match continuation.request() {
            OpenCodeContinuationRequest::Permission(request) => request.tool_origin(),
            OpenCodeContinuationRequest::Question(request) => request.tool_origin(),
        }
        .ok_or_else(|| {
            SdkError::conflict("automatic interaction cancellation has no original tool authority")
        })?;
        let routing = OpenCodeRouting::for_directory(binding.directory().to_path_buf())?;
        let http = self.http.clone();
        let assistant = live
            .capture_while(http.message_by_id(
                &routing,
                continuation.session_id(),
                origin.message_id(),
            ))
            .await??;
        let parent = assistant.cancelled_tool_parent(
            continuation.session_id(),
            origin.message_id(),
            origin.call_id(),
        )?;
        if Instant::now() >= deadline {
            return Err(SdkError::timeout("interaction parent proof deadline elapsed").into());
        }
        let user = live
            .capture_while(http.message_by_id(&routing, continuation.session_id(), &parent))
            .await??;
        user.verify_prompt_parent(continuation.session_id(), &parent, prompt.request_part_id())?;
        Ok(())
    }
}
