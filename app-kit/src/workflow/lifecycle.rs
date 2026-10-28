use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::FutureExt;
use serde_json::{Map, Value, json};

use crate::workflow::definition::JsonBoundary;
use crate::workflow::{
    ActionContext, ActionMode, ActionOutcome, Checkpoint, GraphDescription, RunId, RunLineage,
    RunSnapshot, WorkflowFailure, WorkflowState,
};
use crate::{AppError, WorkspaceId};

const MAX_LIFECYCLE_OBSERVATION: Duration = Duration::from_secs(30);

/// Caller-owned lifecycle observation without a transport or exporter owner.
///
/// Both methods default to success. Asynchronous execution calls the synchronous
/// method before the asynchronous method; synchronous execution calls only the
/// synchronous method. Events borrow their authoritative data for the duration
/// of the call. Implementations own any data they choose to retain afterward.
///
/// # Example
///
/// ```
/// use app_kit::{AppError, workflow::{LifecycleEvent, LifecycleHooks}};
/// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
/// struct CountEvents(Arc<AtomicUsize>);
/// impl LifecycleHooks for CountEvents {
///     fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
///         if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
///             self.0.fetch_add(1, Ordering::SeqCst);
///         }
///         Ok(())
///     }
///     fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>)
///         -> impl std::future::Future<Output = Result<(), AppError>> + Send + 'a {
///         async move {
///             if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
///                 self.0.fetch_add(10, Ordering::SeqCst);
///             }
///             Ok(())
///         }
///     }
/// }
/// let run = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
/// let event = LifecycleEvent::ExecuteStarted {
///     run: &run, occurrence: "00000000000040008000000000000001",
/// };
/// let count = Arc::new(AtomicUsize::new(0));
/// let hooks = CountEvents(Arc::clone(&count));
/// hooks.observe(&event)?;
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// runtime.block_on(hooks.observe_async(&event))?;
/// assert_eq!(count.load(Ordering::SeqCst), 11);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait LifecycleHooks: Send + Sync + 'static {
    /// Observes one event synchronously; the default has no effect.
    ///
    /// # Errors
    ///
    /// Return an error to report functional observation failure. App preserves an
    /// earlier execution failure as the structured cause of `WorkflowFailure::Hook`;
    /// a post-commit hook error does not roll back an accepted native commit.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{LifecycleEvent, LifecycleHooks}};
    /// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    /// struct CountEvents(Arc<AtomicUsize>);
    /// impl LifecycleHooks for CountEvents {
    ///     fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
    ///         if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
    ///             self.0.fetch_add(1, Ordering::SeqCst);
    ///         }
    ///         Ok(())
    ///     }
    ///     fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>)
    ///         -> impl std::future::Future<Output = Result<(), AppError>> + Send + 'a {
    ///         async move {
    ///             if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
    ///                 self.0.fetch_add(10, Ordering::SeqCst);
    ///             }
    ///             Ok(())
    ///         }
    ///     }
    /// }
    /// let run = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
    /// let event = LifecycleEvent::ExecuteStarted {
    ///     run: &run, occurrence: "00000000000040008000000000000001",
    /// };
    /// let count = Arc::new(AtomicUsize::new(0));
    /// CountEvents(Arc::clone(&count)).observe(&event)?;
    /// assert_eq!(count.load(Ordering::SeqCst), 1);
    /// # Ok::<(), AppError>(())
    /// ```
    fn observe(&self, _event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        Ok(())
    }

    /// Observes one event asynchronously; the default has no effect.
    ///
    /// # Errors
    ///
    /// Return an error to report functional observation failure. App preserves an
    /// earlier execution failure as the structured cause of `WorkflowFailure::Hook`;
    /// a post-commit hook error does not roll back an accepted native commit.
    ///
    /// # Example
    ///
    /// ```
    /// use app_kit::{AppError, workflow::{LifecycleEvent, LifecycleHooks}};
    /// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    /// struct CountEvents(Arc<AtomicUsize>);
    /// impl LifecycleHooks for CountEvents {
    ///     fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
    ///         if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
    ///             self.0.fetch_add(1, Ordering::SeqCst);
    ///         }
    ///         Ok(())
    ///     }
    ///     fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>)
    ///         -> impl std::future::Future<Output = Result<(), AppError>> + Send + 'a {
    ///         async move {
    ///             if matches!(event, LifecycleEvent::ExecuteStarted { .. }) {
    ///                 self.0.fetch_add(10, Ordering::SeqCst);
    ///             }
    ///             Ok(())
    ///         }
    ///     }
    /// }
    /// let run = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
    /// let event = LifecycleEvent::ExecuteStarted {
    ///     run: &run, occurrence: "00000000000040008000000000000001",
    /// };
    /// let count = Arc::new(AtomicUsize::new(0));
    /// let hooks = CountEvents(Arc::clone(&count));
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// runtime.block_on(hooks.observe_async(&event))?;
    /// assert_eq!(count.load(Ordering::SeqCst), 10);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn observe_async<'a>(
        &'a self,
        _event: &'a LifecycleEvent<'a>,
    ) -> impl Future<Output = Result<(), AppError>> + Send + 'a {
        async { Ok(()) }
    }
}

/// An exhaustive borrowed view of one application lifecycle event.
pub enum LifecycleEvent<'a> {
    /// Registers a run and its graph with optional child or restore lineage.
    Registered {
        /// Workspace containing the run.
        workspace: &'a WorkspaceId,
        /// Registered run identity.
        run: &'a RunId,
        /// Exact admitted execution occurrence.
        occurrence: &'a str,
        /// Graph used by the registered run.
        graph: &'a GraphDescription,
        /// Parent action when this is an admitted child.
        parent: Option<&'a ActionContext>,
        /// Original boundary when this is a restored run.
        lineage: Option<&'a RunLineage>,
    },
    /// Finishes an admitted child under its exact parent action.
    ChildFinished {
        /// Parent action that admitted this child.
        parent: &'a ActionContext,
        /// Authoritative child snapshot.
        snapshot: &'a RunSnapshot,
    },
    /// Begins one execution session.
    ExecuteStarted {
        /// Run being advanced.
        run: &'a RunId,
        /// Exact admitted execution occurrence.
        occurrence: &'a str,
    },
    /// Finishes one execution session.
    ExecuteFinished {
        /// Authoritative run snapshot at session finalization.
        snapshot: &'a RunSnapshot,
        /// Execution failure, when present.
        failure: Option<&'a WorkflowFailure>,
    },
    /// Begins an action attempt against its committed base and resolved inputs.
    StepStarted {
        /// Identity and attempted action ordinal.
        context: &'a ActionContext,
        /// Unchanged committed base state.
        state: &'a WorkflowState,
        /// Resolved transient action inputs.
        inputs: &'a Map<String, Value>,
    },
    /// Finalizes an attempt, including rollback with no completed outcome.
    StepFinished {
        /// Identity and attempted ordinal, including failed attempts.
        context: &'a ActionContext,
        /// Committed result, exact failure, or no outcome on EOF, abort or Drop.
        outcome: Option<Result<&'a ActionOutcome, &'a WorkflowFailure>>,
        /// Committed boundary; failure retains the unchanged prior boundary.
        checkpoint: &'a Checkpoint,
    },
    /// Opens partial-result observation for one action or addressed span.
    StreamStarted {
        /// Action owning this stream.
        context: &'a ActionContext,
        /// Addressed span, or the action root.
        span: Option<&'a str>,
    },
    /// Observes one transient partial result.
    StreamItem {
        /// Action owning this stream.
        context: &'a ActionContext,
        /// Addressed span, or the action root.
        span: Option<&'a str>,
        /// Zero-based index within this stream.
        index: usize,
        /// Transient result fields; these do not commit state.
        result: &'a Map<String, Value>,
    },
    /// Closes partial-result observation exactly once.
    StreamFinished {
        /// Action owning this stream.
        context: &'a ActionContext,
        /// Addressed span, or the action root.
        span: Option<&'a str>,
        /// Number of observed partial items.
        items: usize,
        /// Failure that closed observation, if any.
        failure: Option<&'a WorkflowFailure>,
    },
    /// Opens an explicitly addressed span within one action attempt.
    SpanStarted {
        /// Action owning the span.
        context: &'a ActionContext,
        /// Unique open span identity within this attempt.
        id: &'a str,
        /// Display name; repeated names are permitted.
        name: &'a str,
        /// Open parent span, or the action root.
        parent: Option<&'a str>,
        /// Dependency span identities in caller-authored order.
        dependencies: &'a [String],
    },
    /// Adds an attribute to an open span or the action root.
    SpanAttribute {
        /// Action owning the attribute.
        context: &'a ActionContext,
        /// Open span, or the action root.
        span: Option<&'a str>,
        /// Attribute name.
        key: &'a str,
        /// Attribute value.
        value: &'a Value,
        /// Caller tags whose values are strings.
        tags: &'a Map<String, Value>,
    },
    /// Closes an addressed span.
    SpanFinished {
        /// Action owning the span.
        context: &'a ActionContext,
        /// Exact open span identity.
        id: &'a str,
    },
}

/// Default lifecycle implementation with no observation effects.
///
/// # Example
///
/// ```
/// use app_kit::workflow::{LifecycleEvent, LifecycleHooks, NoHooks};
/// let run = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
/// let event = LifecycleEvent::ExecuteStarted {
///     run: &run, occurrence: "00000000000040008000000000000001",
/// };
/// NoHooks.observe(&event)?;
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// runtime.block_on(NoHooks.observe_async(&event))?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Default)]
pub struct NoHooks;

impl LifecycleHooks for NoHooks {}

impl LifecycleEvent<'_> {
    pub(crate) fn deadline(activation: Option<tokio::time::Instant>) -> tokio::time::Instant {
        let ceiling = tokio::time::Instant::now() + MAX_LIFECYCLE_OBSERVATION;
        activation.map_or(ceiling, |deadline| deadline.min(ceiling))
    }

    pub(crate) fn deliver_sync<H: LifecycleHooks>(&self, hooks: &H) -> Result<(), WorkflowFailure> {
        catch_unwind(AssertUnwindSafe(|| hooks.observe(self)))
            .map_err(|_| WorkflowFailure::Execution("lifecycle callback panicked".into()))?
            .map_err(WorkflowFailure::hook)
    }

    pub(crate) async fn deliver<H: LifecycleHooks>(
        &self,
        hooks: &H,
        mode: ActionMode,
        deadline: tokio::time::Instant,
    ) -> Result<(), WorkflowFailure> {
        if mode == ActionMode::Both {
            return Err(WorkflowFailure::Execution(
                "lifecycle requires one actual mode".into(),
            ));
        }
        self.deliver_sync(hooks)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(WorkflowFailure::Timeout(
                "lifecycle observation expired".into(),
            ));
        }
        if mode == ActionMode::Synchronous {
            return Ok(());
        }
        let source = catch_unwind(AssertUnwindSafe(|| hooks.observe_async(self)))
            .map_err(|_| WorkflowFailure::Execution("lifecycle callback panicked".into()))?;
        let mut source = Box::pin(source);
        let observed = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => {
                Err(WorkflowFailure::Timeout("lifecycle observation expired".into()))
            }
            result = AssertUnwindSafe(poll_fn(|context| source.as_mut().poll(context))).catch_unwind() => {
                match result {
                    Ok(result) => result.map_err(WorkflowFailure::hook),
                    Err(_) => Err(WorkflowFailure::Execution("lifecycle callback panicked".into())),
                }
            }
        };
        // The callback's future is still owned here when its observation ends.
        // Preserve an earlier callback/timeout failure if destruction also fails.
        let destroyed = catch_unwind(AssertUnwindSafe(|| drop(source)))
            .map_err(|_| WorkflowFailure::Execution("lifecycle callback panicked".into()));
        observed.and(destroyed)
    }

    /// Projects this observation into the pinned rurr tracking JSON records.
    ///
    /// This is an explicit content export: inputs, results, state and attributes
    /// may be present. The caller owns transport and samples the observation time.
    /// No record is retained and no hook is invoked. The complete encoded list
    /// is bounded to 1 MiB. Timed records require UTC years 1970 through 9999.
    ///
    /// Execute boundaries, child completion and nonfirst partial items have no
    /// pinned wire row and return an empty list. Step completion serializes its
    /// temporary state projection through the process-global field encoders;
    /// the authoritative checkpoint remains unchanged.
    /// A step without an outcome has no pinned end-entry state and returns an
    /// input error before invoking a field encoder.
    ///
    /// ```
    /// use app_kit::workflow::{LifecycleEvent, RunId};
    /// let run: RunId = "run_8a7c251e-b5b8-4d9e-8f0b-123456789abc".parse()?;
    /// let event = LifecycleEvent::ExecuteStarted {
    /// run: &run, occurrence: "00000000000040008000000000000001",
    /// };
    /// assert!(event.tracking_records(std::time::UNIX_EPOCH)?.is_empty());
    /// # Ok::<(), app_kit::AppError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for a step with no outcome, missing graph source,
    /// unsupported timestamp, malformed span attributes, or exceeded record bounds.
    /// Codec errors propagate from state serialization. This explicit export may
    /// contain caller content; it is separate from safe operational tracing.
    pub fn tracking_records(&self, observed_at: SystemTime) -> Result<Vec<Value>, AppError> {
        let records = match self {
            Self::Registered {
                workspace,
                run,
                occurrence: _,
                graph,
                parent,
                lineage,
            } => {
                let logical_parent = lineage.map(|lineage| {
                    json!({
                        "type":"pointer_data", "app_id":lineage.source,
                        "sequence_id":lineage.action_ordinal, "partition_key":workspace,
                    })
                });
                let spawning_parent = parent.map(|parent| {
                    json!({
                        "type":"pointer_data", "app_id":parent.run,
                        "sequence_id":parent.action_ordinal, "partition_key":parent.workspace,
                    })
                });
                let mut records = vec![
                    graph.tracking_record()?,
                    json!({
                        "type":"application_metadata", "partition_key":workspace,
                        "parent_pointer":logical_parent, "spawning_parent_pointer":spawning_parent,
                    }),
                ];
                if lineage.is_some() || parent.is_some() {
                    let time = Self::tracking_time(observed_at)?;
                    for (kind, ordinal) in lineage
                        .map(|lineage| ("fork", lineage.action_ordinal))
                        .into_iter()
                        .chain(parent.map(|parent| ("spawn_start", Some(parent.action_ordinal))))
                    {
                        records.push(json!({
                            "type":"child_application_data", "event_type":kind,
                            "sequence_id":ordinal, "event_time":time,
                            "child":{"type":"pointer_data", "app_id":run,
                                "sequence_id":null, "partition_key":workspace},
                        }));
                    }
                }
                records
            }
            Self::StepStarted {
                context, inputs, ..
            } => vec![json!({
                "type":"begin_entry", "start_time":Self::tracking_time(observed_at)?,
                "action":context.node, "inputs":inputs, "sequence_id":context.action_ordinal,
            })],
            Self::StepFinished {
                context,
                outcome,
                checkpoint,
            } => {
                let Some(outcome) = outcome else {
                    return Err(AppError::InvalidInput(
                        "step without an outcome has no tracking end-entry state".into(),
                    ));
                };
                // Pinned field encoders see the attempted sequence and committed
                // predecessor, including on failure. They never mutate the
                // authoritative checkpoint or the outer numeric wire ordinal.
                let mut state = checkpoint.state.clone();
                state
                    .values
                    .insert("__SEQUENCE_ID".into(), context.action_ordinal.into());
                match &checkpoint.position {
                    Some(position) => {
                        state
                            .values
                            .insert("__PRIOR_STEP".into(), position.clone().into());
                    }
                    None => {
                        state.values.remove("__PRIOR_STEP");
                    }
                }
                let (result, exception) = match outcome {
                    Ok(outcome) => (Some(&outcome.result), None),
                    Err(failure) => (None, Some(failure.message())),
                };
                vec![json!({
                    "type":"end_entry", "end_time":Self::tracking_time(observed_at)?,
                    "action":context.node, "result":result, "exception":exception,
                    "state":state.serialize()?, "sequence_id":context.action_ordinal,
                })]
            }
            Self::SpanStarted {
                context,
                id,
                name,
                parent,
                dependencies,
            } => vec![json!({
                "type":"begin_span", "start_time":Self::tracking_time(observed_at)?,
                "action_sequence_id":context.action_ordinal, "span_id":id,
                "span_name":name, "parent_span_id":parent, "span_dependencies":dependencies,
            })],
            Self::SpanFinished { context, id } => vec![json!({
                "type":"end_span", "end_time":Self::tracking_time(observed_at)?,
                "action_sequence_id":context.action_ordinal, "span_id":id,
            })],
            Self::SpanAttribute {
                context,
                span,
                key,
                value,
                tags,
            } => {
                if tags.values().any(|value| !value.is_string()) {
                    return Err(AppError::InvalidInput(
                        "tracking attribute tags must be strings".into(),
                    ));
                }
                vec![json!({
                    "type":"attribute", "key":key, "action_sequence_id":context.action_ordinal,
                    "span_id":span, "value":value, "tags":tags,
                    "time_logged":Self::tracking_time(observed_at)?,
                })]
            }
            Self::StreamStarted { context, span } => vec![json!({
                "type":"begin_stream", "action_sequence_id":context.action_ordinal,
                "span_id":span, "stream_init_time":Self::tracking_time(observed_at)?,
            })],
            Self::StreamItem {
                context,
                span,
                index: 0,
                ..
            } => vec![json!({
                "type":"first_item_stream", "action_sequence_id":context.action_ordinal,
                "span_id":span, "first_item_time":Self::tracking_time(observed_at)?,
            })],
            Self::StreamFinished {
                context,
                span,
                items,
                ..
            } => vec![json!({
                "type":"end_stream", "action_sequence_id":context.action_ordinal,
                "span_id":span, "end_time":Self::tracking_time(observed_at)?,
                "items_streamed":items,
            })],
            Self::ChildFinished { .. }
            | Self::ExecuteStarted { .. }
            | Self::ExecuteFinished { .. }
            | Self::StreamItem { .. } => Vec::new(),
        };
        JsonBoundary::validate_size(&records)?;
        Ok(records)
    }

    fn tracking_time(observed_at: SystemTime) -> Result<String, AppError> {
        let elapsed = observed_at
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AppError::InvalidInput("tracking timestamp precedes UNIX_EPOCH".into()))?;
        if elapsed.as_secs() >= 253_402_300_800 {
            return Err(AppError::InvalidInput(
                "tracking timestamp exceeds year 9999".into(),
            ));
        }
        // The checked interval bounds this Gregorian conversion to 8,030 years.
        // There is no platform time API, new dependency or separate clock owner.
        let mut days = elapsed.as_secs() / 86_400;
        let mut year = 1970_u64;
        let leap = loop {
            let leap =
                year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
            let length = 365 + u64::from(leap);
            if days < length {
                break leap;
            }
            days -= length;
            year += 1;
        };
        let months = [
            31,
            28 + u64::from(leap),
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 1;
        for length in months {
            if days < length {
                break;
            }
            days -= length;
            month += 1;
        }
        let time = elapsed.as_secs() % 86_400;
        Ok(format!(
            "{year:04}-{month:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
            days + 1,
            time / 3_600,
            (time / 60) % 60,
            time % 60,
            elapsed.subsec_nanos()
        ))
    }
}

impl WorkflowFailure {
    pub(crate) fn with_cause(self, cause: Option<Self>) -> Self {
        match self {
            Self::Hook {
                message,
                cause: prior,
            } => Self::Hook {
                message,
                cause: prior.or_else(|| cause.map(Box::new)),
            },
            failure => failure,
        }
    }

    fn hook(error: AppError) -> Self {
        Self::Hook {
            message: error.to_string(),
            cause: None,
        }
    }

    pub(crate) fn application_error(&self) -> AppError {
        match self {
            Self::Timeout(message) => AppError::Timeout(message.clone()),
            _ => AppError::Integration(self.message().to_owned()),
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Hook { message, .. } => message,
            Self::Evaluation(message)
            | Self::Action(message)
            | Self::State(message)
            | Self::Agent(message)
            | Self::Child(message)
            | Self::Execution(message)
            | Self::Timeout(message)
            | Self::Limit(message) => message,
        }
    }
}
