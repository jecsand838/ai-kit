//! Real async lifecycle delivery, separate from direct trait and wire projection tests.
//! Pin 005bfe3ece631e64b06dac2d42ef84235f727a4f, rurr-main.zip SHA256
//! 5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e:
//! lifecycle/guard.rs20–71,104–193,206–271; application/step_txn.rs258–276;
//! application/streaming/step_core.rs310–375,401–421,656–678.
//! Includes exact structured Hook cause at completion. Operational subscriber
//! coverage is separate; hook content never enters operational tracing here.

use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, UNIX_EPOCH};

use agent_sdk::Agent;
use app_sdk::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, CheckpointKind,
    CheckpointSelector, ExecutionEvent, ExecutionOptions, Interaction, JsonState, LifecycleEvent,
    LifecycleHooks, NoConditions, Resolution, RunSelector, RunStatus, StateChange,
    StateSerialization, StateTyping, WorkflowDefinition, WorkflowFailure, WorkflowRun,
    WorkflowState,
};
use app_sdk::{App, AppConfig, AppError, Shutdown, WorkspaceConfig};
use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value, json};
use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_sdk::state::record::{
    StateKey, StateKeyPrefix, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Notify;
use tracing::instrument::WithSubscriber;

#[derive(Clone, Copy)]
enum SourceEnding {
    Complete,
    Spans(SpanProgram),
    Error,
    Eof,
    Pending,
    DropPanic,
}

#[derive(Clone, Copy)]
enum SpanProgram {
    Nested,
    OpenComplete,
    OpenError,
    OpenEof,
    OpenPending,
    UnknownParent,
    Duplicate,
    ClosedAttribute,
    NonStringTags,
    UnknownFinish,
    ParentBeforeChild,
    DoubleFinish,
    ClosedReuse,
    Count(usize),
    Depth(usize),
    TextBytes(usize),
    EventBytes(usize),
    RetainedBytes(usize),
    Dependencies(usize),
    Tags(usize),
}

impl SourceEnding {
    fn events(self) -> std::vec::IntoIter<ActionEvent> {
        let mut events = Vec::new();
        if let Self::Spans(program) = self {
            match program {
                SpanProgram::Count(count) | SpanProgram::Depth(count) => {
                    for index in 0..count {
                        events.push(ActionEvent::SpanStarted {
                            id: format!("span-{index}"),
                            name: "bounded".into(),
                            parent: if matches!(program, SpanProgram::Depth(_)) && index > 0 {
                                Some(format!("span-{}", index - 1))
                            } else {
                                None
                            },
                            dependencies: Vec::new(),
                        });
                        if matches!(program, SpanProgram::Count(_)) {
                            // Closed ids remain counted, without making this
                            // case depend on the separate open-depth limit.
                            events.push(ActionEvent::SpanFinished {
                                id: format!("span-{index}"),
                            });
                        }
                    }
                }
                SpanProgram::TextBytes(bytes) => events.push(ActionEvent::SpanStarted {
                    id: "x".repeat(bytes),
                    name: "bounded".into(),
                    parent: None,
                    dependencies: Vec::new(),
                }),
                SpanProgram::EventBytes(bytes) => events.push(ActionEvent::SpanAttribute {
                    span: None,
                    key: "k".into(),
                    // key + encoded string + encoded empty tags = 5 bytes
                    // before the raw value, independent of a private DTO.
                    value: json!("x".repeat(bytes - 5)),
                    tags: Map::new(),
                }),
                SpanProgram::RetainedBytes(bytes) => {
                    let mut remaining = bytes;
                    let mut index = 0;
                    while remaining > 0 {
                        let length = remaining.min(65_536);
                        let id = if length == 1 {
                            "z".into()
                        } else {
                            format!("{index:02x}{}", "x".repeat(length - 2))
                        };
                        events.push(ActionEvent::SpanStarted {
                            id: id.clone(),
                            name: "bounded".into(),
                            parent: None,
                            dependencies: Vec::new(),
                        });
                        events.push(ActionEvent::SpanFinished { id });
                        remaining -= length;
                        index += 1;
                    }
                }
                SpanProgram::Dependencies(count) => events.push(ActionEvent::SpanStarted {
                    id: "bounded".into(),
                    name: "bounded".into(),
                    parent: None,
                    dependencies: vec!["upstream".into(); count],
                }),
                SpanProgram::Tags(count) => events.push(ActionEvent::SpanAttribute {
                    span: None,
                    key: "k".into(),
                    value: Value::Null,
                    tags: (0..count)
                        .map(|index| (format!("tag-{index}"), json!("tag")))
                        .collect(),
                }),
                _ => {
                    events.push(ActionEvent::SpanStarted {
                        id: "parent".into(),
                        name: "repeated".into(),
                        parent: matches!(program, SpanProgram::UnknownParent)
                            .then(|| "missing".into()),
                        dependencies: Vec::new(),
                    });
                    events.push(ActionEvent::SpanStarted {
                        id: if matches!(program, SpanProgram::Duplicate) {
                            "parent"
                        } else {
                            "child"
                        }
                        .into(),
                        name: "repeated".into(),
                        parent: Some("parent".into()),
                        dependencies: vec![
                            "upstream-b".into(),
                            "upstream-a".into(),
                            "upstream-b".into(),
                        ],
                    });
                    if matches!(
                        program,
                        SpanProgram::UnknownFinish | SpanProgram::ParentBeforeChild
                    ) {
                        events.push(ActionEvent::SpanFinished {
                            id: if matches!(program, SpanProgram::UnknownFinish) {
                                "missing"
                            } else {
                                "parent"
                            }
                            .into(),
                        });
                    }
                    for span in [Some("parent"), None] {
                        events.push(ActionEvent::SpanAttribute {
                            span: span.map(str::to_owned),
                            key: "explicit-content".into(),
                            value: json!({"secret":"caller-only span payload"}),
                            tags: Map::from_iter([(
                                "source".into(),
                                if matches!(program, SpanProgram::NonStringTags) {
                                    json!(7)
                                } else {
                                    json!("caller")
                                },
                            )]),
                        });
                    }
                    if !matches!(
                        program,
                        SpanProgram::OpenComplete
                            | SpanProgram::OpenError
                            | SpanProgram::OpenEof
                            | SpanProgram::OpenPending
                    ) {
                        events.push(ActionEvent::SpanFinished { id: "child".into() });
                        events.push(ActionEvent::SpanFinished {
                            id: "parent".into(),
                        });
                    }
                    if matches!(program, SpanProgram::DoubleFinish) {
                        events.push(ActionEvent::SpanFinished { id: "child".into() });
                    }
                    if matches!(program, SpanProgram::ClosedReuse) {
                        events.push(ActionEvent::SpanStarted {
                            id: "child".into(),
                            name: "reused identity".into(),
                            parent: None,
                            dependencies: Vec::new(),
                        });
                    }
                    if matches!(program, SpanProgram::ClosedAttribute) {
                        events.push(ActionEvent::SpanAttribute {
                            span: Some("child".into()),
                            key: "too-late".into(),
                            value: json!(1),
                            tags: Map::new(),
                        });
                    }
                }
            }
        }
        events.push(ActionEvent::Partial {
            result: Map::from_iter([("text".into(), json!("private partial output"))]),
        });
        events.into_iter()
    }
}

#[derive(Clone, Copy)]
enum HookFault {
    None,
    HoldRegistration,
    RejectRegistration,
    HoldExecuteFinished,
    HoldPost {
        fail: bool,
    },
    HoldItem {
        fail: bool,
    },
    HoldAttribute {
        fail: bool,
    },
    SharedCleanupBudget,
    SharedSiblingCleanupBudget,
    OpeningBudget,
    EofRead {
        permanent: bool,
    },
    ChildRegistrationError,
    NativePreError {
        lane: &'static str,
    },
    Error {
        event: &'static str,
        lane: &'static str,
    },
    Panic {
        event: &'static str,
        lane: &'static str,
    },
}

#[derive(Clone, Copy)]
enum CleanupReadFault {
    Recoverable,
    Permanent,
    Held,
    Captured,
    Delayed,
}

#[derive(Clone, Copy)]
enum ExecutionReadPhase {
    FirstPoll,
    BeforePolling,
    AfterPartial,
}

#[derive(Clone, Copy)]
enum OpeningReadPhase {
    FailureReceipt,
    Generation,
    Completed,
}

#[derive(Clone, Copy)]
enum RegistrationReadPhase {
    Authority(CleanupReadFault),
    DroppedReceipt,
    Superseded,
}

#[derive(Clone, Copy)]
enum HookCommit {
    RejectedCancellation,
    SourceFailureStage,
    Retained,
    Prepared,
    Halt,
    ChildAdmission,
    ChildIntent,
    Barrier,
    NativePrefix,
    Stopping,
    Failure,
}

#[derive(Default)]
struct HookEvidence {
    calls: Mutex<Vec<Value>>,
    constructed: AtomicUsize,
    source_dropped: AtomicUsize,
    owner_dropped: AtomicUsize,
    encoded: AtomicUsize,
    rejected: AtomicBool,
    typing_pre: AtomicUsize,
    registration_entered: Notify,
    registration_release: Notify,
    post_entered: Notify,
    post_release: Notify,
    waits_dropped: AtomicUsize,
    source_contexts: Mutex<Vec<Value>>,
    admission_armed: AtomicBool,
    admission_entered: Notify,
    admission_release: Notify,
    admitted_commits: AtomicUsize,
    barrier_armed: Mutex<Option<HookCommit>>,
    barrier_entered: Notify,
    barrier_release: Notify,
    barrier_commits: AtomicUsize,
    cleanup_read: Mutex<Option<(StateKey, CleanupReadFault)>>,
    cleanup_read_faults: AtomicUsize,
    cleanup_read_until_mutation: AtomicBool,
}

impl HookEvidence {
    fn calls(&self) -> Vec<Value> {
        self.calls.lock().unwrap().clone()
    }

    fn names(&self) -> Vec<String> {
        self.calls()
            .iter()
            .map(|event| {
                format!(
                    "{}:{}",
                    event["event"].as_str().unwrap(),
                    event["lane"].as_str().unwrap()
                )
            })
            .collect()
    }

    fn span_endings(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|event| event["event"] == "span-finish")
            .map(|event| {
                format!(
                    "{}:{}",
                    event["id"].as_str().unwrap(),
                    event["lane"].as_str().unwrap()
                )
            })
            .collect()
    }

    fn failure(failure: &WorkflowFailure) -> Value {
        match failure {
            WorkflowFailure::Action(message) => json!({"category":"action","message":message}),
            WorkflowFailure::State(message) => json!({"category":"state","message":message}),
            WorkflowFailure::Execution(message) => {
                json!({"category":"execution","message":message})
            }
            WorkflowFailure::Hook { message, cause } => json!({
                "category":"hook", "message":message,
                "cause":cause.as_deref().map(Self::failure),
            }),
            _ => json!({"category":"other"}),
        }
    }
}

struct RuntimeHooks {
    evidence: Arc<HookEvidence>,
    fault: HookFault,
    inspect_no_outcome: bool,
}

struct HookAdmissionStore {
    inner: StateStore,
    evidence: Arc<HookEvidence>,
}

struct HookAdmissionTransaction {
    inner: StateTransaction,
    evidence: Arc<HookEvidence>,
    admission: bool,
    barrier: bool,
    mutation_receipt: bool,
}

impl StateStoreBackend for HookAdmissionStore {
    type Transaction = HookAdmissionTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let fault = {
            let mut armed = self.evidence.cleanup_read.lock().unwrap();
            if armed.as_ref().is_some_and(|(selected, _)| selected == &key) {
                match armed.as_ref().map(|(_, fault)| *fault) {
                    Some(CleanupReadFault::Delayed) => Some(CleanupReadFault::Delayed),
                    _ => armed.take().map(|(_, fault)| fault),
                }
            } else {
                None
            }
        };
        if let Some(fault) = fault {
            // A public cleanup boundary selects this exact head. Transaction
            // reads and accepted native commit receipts remain untouched.
            self.evidence
                .cleanup_read_faults
                .fetch_add(1, Ordering::SeqCst);
            match fault {
                CleanupReadFault::Permanent => {
                    return Err(StateStoreError::serialization(
                        "cleanup authority is unreadable",
                    ));
                }
                CleanupReadFault::Recoverable => {
                    return Err(StateStoreError::unavailable(
                        "load",
                        "cleanup authority is temporarily unavailable",
                    ));
                }
                CleanupReadFault::Delayed => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                CleanupReadFault::Held => {
                    let _wait = HookWait(Arc::clone(&self.evidence));
                    self.evidence.post_entered.notify_one();
                    self.evidence.post_release.notified().await;
                }
                CleanupReadFault::Captured => {
                    let snapshot = self.inner.load(key).await?;
                    let _wait = HookWait(Arc::clone(&self.evidence));
                    self.evidence.admission_entered.notify_one();
                    self.evidence.admission_release.notified().await;
                    return Ok(snapshot);
                }
            }
        }
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(HookAdmissionTransaction {
            inner: self.inner.begin_transaction().await?,
            evidence: Arc::clone(&self.evidence),
            admission: false,
            barrier: false,
            mutation_receipt: false,
        })
    }
}

impl StateTransactionBackend for HookAdmissionTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        if self.mutation_receipt
            && self
                .evidence
                .cleanup_read_until_mutation
                .load(Ordering::SeqCst)
        {
            let mut armed = self.evidence.cleanup_read.lock().unwrap();
            if armed.as_ref().is_some_and(|(selected, _)| selected == &key) {
                // This native transaction already read its mutation receipt.
                // A read-only snapshot cannot retire the optional pure-read fault.
                armed.take();
                self.evidence
                    .cleanup_read_until_mutation
                    .store(false, Ordering::SeqCst);
            }
        }
        let receipt = key.namespace().as_str() == "app-sdk" && key.key().starts_with("receipt/");
        let result = self.inner.load(key).await;
        if receipt && result.is_ok() {
            self.mutation_receipt = true;
        }
        result
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let reject_cancelled = record.key.namespace().as_str() == "app-sdk"
            && record.key.key().starts_with("history/")
            && record.value.get("workflow_id") == Some(&json!("hooks"))
            && record.value.pointer("/checkpoint/phase/state") == Some(&json!("cancelled"))
            && matches!(
                *self.evidence.barrier_armed.lock().unwrap(),
                Some(HookCommit::RejectedCancellation)
            );
        if reject_cancelled {
            self.evidence.barrier_armed.lock().unwrap().take();
            self.evidence.rejected.store(true, Ordering::SeqCst);
            self.evidence.barrier_entered.notify_one();
            self.evidence.barrier_release.notified().await;
            // This attempted terminal write never enters the native store.
            // The original App transaction must roll back before read recovery.
            return Err(StateStoreError::serialization(
                "selected terminal cancellation was rejected",
            ));
        }
        self.admission |= self.evidence.admission_armed.load(Ordering::SeqCst)
            && record.key.namespace().as_str() == "app-sdk"
            && record.key.key().starts_with("history/")
            && record.value.get("workflow_id") == Some(&json!("hooks"))
            && record.value.pointer("/checkpoint/sequence") == Some(&json!(0));
        self.barrier |= record.key.namespace().as_str() == "app-sdk"
            && record.value.get("workflow_id") == Some(&json!("hooks"))
            && self
                .evidence
                .barrier_armed
                .lock()
                .unwrap()
                .is_some_and(|target| {
                    (record.key.key().starts_with("history/")
                        || matches!(target, HookCommit::Retained)
                            && record.key.key().starts_with("run/"))
                        && match target {
                            HookCommit::RejectedCancellation => false,
                            HookCommit::SourceFailureStage => record
                                .value
                                .pointer("/checkpoint/staged")
                                .and_then(Value::as_array)
                                .is_some_and(|staged| {
                                    staged.iter().any(|row| {
                                        row["node"] == "work" && row["outcome"]["state"] == "failed"
                                    })
                                }),
                            HookCommit::Retained => {
                                record.value.pointer("/retention/state") == Some(&json!("retained"))
                            }
                            HookCommit::ChildAdmission => record
                                .value
                                .pointer("/checkpoint/controls")
                                .and_then(Value::as_object)
                                .is_some_and(|controls| {
                                    controls.values().any(|control| {
                                        control["state"]["kind"] == "child"
                                            && control["state"]["admitted"]
                                                .as_array()
                                                .is_some_and(|children| children.len() == 2)
                                    })
                                }),
                            HookCommit::ChildIntent => record
                                .value
                                .pointer("/checkpoint/controls")
                                .and_then(Value::as_object)
                                .is_some_and(|controls| {
                                    controls.values().any(|control| {
                                        control["state"]["kind"] == "child"
                                            && control["state"]["admitted"]
                                                .as_array()
                                                .is_some_and(Vec::is_empty)
                                    })
                                }),
                            HookCommit::Prepared => {
                                record.value.pointer("/checkpoint/admission/kind")
                                    == Some(&json!("prepared"))
                                    && record.value.pointer("/checkpoint/phase/state")
                                        == Some(&json!("active"))
                                    && record.value.pointer("/checkpoint/staged")
                                        == Some(&json!([]))
                            }
                            HookCommit::Halt => {
                                record.value.pointer("/checkpoint/admission/kind")
                                    == Some(&json!("halted"))
                                    && record.value.pointer("/checkpoint/admission/node")
                                        == Some(&json!("work"))
                            }
                            HookCommit::Barrier => {
                                record.value.pointer("/checkpoint/phase/state")
                                    == Some(&json!("routing"))
                                    && record.value.pointer("/checkpoint/outcomes/0/node")
                                        == Some(&json!("work"))
                            }
                            HookCommit::NativePrefix => {
                                record.value.pointer("/checkpoint/phase/state")
                                    == Some(&json!("active"))
                                    && record.value.pointer("/checkpoint/staged/0/node")
                                        == Some(&json!("a_native"))
                            }
                            HookCommit::Stopping => {
                                record.value.pointer("/checkpoint/phase/state")
                                    == Some(&json!("failing"))
                            }
                            HookCommit::Failure => {
                                record.value.pointer("/checkpoint/phase/state")
                                    == Some(&json!("failed"))
                            }
                        }
                });
        self.inner.put(record).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        self.inner.commit().await?;
        if self.admission && self.evidence.admission_armed.swap(false, Ordering::SeqCst) {
            // The immutable checkpoint and its head are already durable; the
            // retained App operation alone still awaits this real receipt.
            self.evidence
                .admitted_commits
                .fetch_add(1, Ordering::SeqCst);
            self.evidence.admission_entered.notify_one();
            self.evidence.admission_release.notified().await;
        }
        if self.barrier && self.evidence.barrier_armed.lock().unwrap().take().is_some() {
            self.evidence.barrier_commits.fetch_add(1, Ordering::SeqCst);
            self.evidence.barrier_entered.notify_one();
            self.evidence.barrier_release.notified().await;
        }
        Ok(())
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

impl RuntimeHooks {
    fn record(&self, event: &LifecycleEvent<'_>, lane: &str) -> Result<(), AppError> {
        let mut row = match event {
            LifecycleEvent::Registered {
                run,
                graph,
                parent,
                lineage,
                ..
            } => json!({
                "event":"registered", "run":run, "graph":serde_json::to_value(graph).unwrap(),
                "parent":parent.is_some(), "lineage":lineage.is_some(),
                "parent_run":parent.map(|context| &context.run),
                "parent_node":parent.map(|context| &context.node)
            }),
            LifecycleEvent::ExecuteStarted { run, .. } => {
                json!({"event":"execute-start","run":run})
            }
            LifecycleEvent::ExecuteFinished { snapshot, failure } => json!({
                "event":"execute-finish","run":snapshot.id,"state":snapshot.state.values(),
                "ordinal":snapshot.last_action_ordinal,"failure":failure.map(HookEvidence::failure)
            }),
            LifecycleEvent::StepStarted {
                context,
                state,
                inputs,
            } => json!({
                "event":"step-start","run":context.run,"workflow":context.workflow,
                "node":context.node,"ordinal":context.action_ordinal,"checkpoint":context.checkpoint,
                "state":state.values(),"inputs":inputs,"asynchronous":context.mode == ActionMode::Asynchronous
            }),
            LifecycleEvent::StepFinished {
                context,
                outcome,
                checkpoint,
            } => {
                let outcome = match outcome {
                    Some(Ok(outcome)) => {
                        json!({"kind":"success","node":outcome.node,"ordinal":outcome.ordinal,"result":outcome.result})
                    }
                    Some(Err(failure)) => {
                        json!({"kind":"failure","failure":HookEvidence::failure(failure)})
                    }
                    None => Value::Null,
                };
                let before = self.evidence.encoded.load(Ordering::SeqCst);
                let projection_rejected = self.inspect_no_outcome
                    && outcome.is_null()
                    && event.tracking_records(UNIX_EPOCH).is_err();
                json!({"event":"step-finish","run":context.run,"workflow":context.workflow,
                    "node":context.node,"ordinal":context.action_ordinal,"checkpoint":context.checkpoint,
                    "state":checkpoint.state.values(),"committed_ordinal":checkpoint.action_ordinal,
                    "outcome":outcome,"projection_rejected":projection_rejected,
                    "projection_codec_calls":self.evidence.encoded.load(Ordering::SeqCst)-before})
            }
            LifecycleEvent::StreamStarted { context, span } => {
                json!({"event":"stream-start","node":context.node,"span":span})
            }
            LifecycleEvent::StreamItem {
                context,
                span,
                index,
                result,
            } => {
                json!({"event":"stream-item","node":context.node,"span":span,"index":index,"result":result})
            }
            LifecycleEvent::StreamFinished {
                context,
                span,
                items,
                failure,
            } => {
                json!({"event":"stream-finish","node":context.node,"span":span,"items":items,"failure":failure.map(HookEvidence::failure)})
            }
            LifecycleEvent::ChildFinished { parent, snapshot } => json!({
                "event":"child-finish","run":snapshot.id,"parent_run":parent.run,
                "parent_node":parent.node,"state":snapshot.state.values(),
                "succeeded":matches!(snapshot.status, RunStatus::Succeeded)
            }),
            LifecycleEvent::SpanStarted {
                context,
                id,
                name,
                parent,
                dependencies,
            } => json!({
                "event":"span-start", "node":context.node, "ordinal":context.action_ordinal,
                "id":id, "name":name, "parent":parent, "dependencies":dependencies,
            }),
            LifecycleEvent::SpanAttribute {
                context,
                span,
                key,
                value,
                tags,
            } => json!({
                "event":"span-attribute", "node":context.node, "ordinal":context.action_ordinal,
                "span":span, "key":key, "value":value, "tags":tags,
            }),
            LifecycleEvent::SpanFinished { context, id } => json!({
                "event":"span-finish", "node":context.node, "ordinal":context.action_ordinal, "id":id,
            }),
        };
        row["lane"] = lane.into();
        let name = row["event"].as_str().unwrap().to_owned();
        let child_registration = row["event"] == "registered" && row["parent"] == true;
        let later_native_pre = row["event"] == "step-start" && row["node"] == "b_native";
        self.evidence.calls.lock().unwrap().push(row);
        let (selected, panics) = match self.fault {
            HookFault::None
            | HookFault::HoldRegistration
            | HookFault::RejectRegistration
            | HookFault::HoldExecuteFinished
            | HookFault::HoldPost { .. }
            | HookFault::HoldItem { .. }
            | HookFault::HoldAttribute { .. }
            | HookFault::SharedCleanupBudget
            | HookFault::SharedSiblingCleanupBudget
            | HookFault::OpeningBudget
            | HookFault::EofRead { .. } => (false, false),
            HookFault::ChildRegistrationError => (child_registration && lane == "sync", false),
            HookFault::NativePreError {
                lane: selected_lane,
            } => (later_native_pre && lane == selected_lane, false),
            HookFault::Error {
                event,
                lane: selected_lane,
            } => (event == name && selected_lane == lane, false),
            HookFault::Panic {
                event,
                lane: selected_lane,
            } => (event == name && selected_lane == lane, true),
        };
        if selected && !self.evidence.rejected.swap(true, Ordering::SeqCst) {
            assert!(!panics, "private hook panic payload");
            return Err(AppError::InvalidInput(
                "explicit caller hook context".into(),
            ));
        }
        Ok(())
    }
}

impl LifecycleHooks for RuntimeHooks {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError> {
        self.record(event, "sync")
    }

    async fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>) -> Result<(), AppError> {
        tokio::task::yield_now().await;
        self.record(event, "async")?;
        if matches!(self.fault, HookFault::OpeningBudget)
            && matches!(event, LifecycleEvent::ExecuteStarted { .. })
        {
            // Cleanup after a rejected start retains the same original
            // observation budget instead of granting a second full ceiling.
            tokio::time::sleep(Duration::from_secs(20)).await;
            return Err(AppError::InvalidInput(
                "explicit caller hook context".into(),
            ));
        }
        if matches!(
            self.fault,
            HookFault::HoldRegistration | HookFault::RejectRegistration
        ) && matches!(event, LifecycleEvent::Registered { .. })
        {
            let _wait = HookWait(Arc::clone(&self.evidence));
            self.evidence.registration_entered.notify_one();
            self.evidence.registration_release.notified().await;
            if matches!(self.fault, HookFault::RejectRegistration) {
                return Err(AppError::InvalidInput(
                    "explicit caller hook context".into(),
                ));
            }
        }
        if matches!(self.fault, HookFault::HoldExecuteFinished)
            && matches!(event, LifecycleEvent::ExecuteFinished { .. })
        {
            let _wait = HookWait(Arc::clone(&self.evidence));
            self.evidence.post_entered.notify_one();
            self.evidence.post_release.notified().await;
        }
        if let HookFault::HoldPost { fail } = self.fault
            && matches!(event, LifecycleEvent::StepFinished { .. })
        {
            let _wait = HookWait(Arc::clone(&self.evidence));
            self.evidence.post_entered.notify_one();
            self.evidence.post_release.notified().await;
            if fail {
                return Err(AppError::InvalidInput(
                    "explicit caller hook context".into(),
                ));
            }
        }
        if let HookFault::HoldItem { fail } = self.fault
            && matches!(event, LifecycleEvent::StreamItem { .. })
        {
            let _wait = HookWait(Arc::clone(&self.evidence));
            self.evidence.post_entered.notify_one();
            self.evidence.post_release.notified().await;
            if fail {
                return Err(AppError::InvalidInput(
                    "explicit caller hook context".into(),
                ));
            }
        }
        if let HookFault::HoldAttribute { fail } = self.fault
            && matches!(
                event,
                LifecycleEvent::SpanAttribute {
                    span: Some("parent"),
                    ..
                }
            )
        {
            let _wait = HookWait(Arc::clone(&self.evidence));
            self.evidence.post_entered.notify_one();
            self.evidence.post_release.notified().await;
            if fail {
                return Err(AppError::InvalidInput(
                    "explicit caller hook context".into(),
                ));
            }
        }
        if let HookFault::EofRead { permanent } = self.fault
            && let LifecycleEvent::StepFinished {
                context,
                outcome: None,
                ..
            } = event
        {
            // The original successful async EOF callback arms its next owned
            // read only after the caller source has ended. This guard succeeds.
            *self.evidence.cleanup_read.lock().unwrap() = Some((
                StateKey::new(
                    "app-sdk",
                    format!(
                        "run/{}/{}/{}/{}",
                        context.workspace, context.workflow, context.run, context.occurrence,
                    ),
                )
                .unwrap(),
                if permanent {
                    CleanupReadFault::Permanent
                } else {
                    CleanupReadFault::Recoverable
                },
            ));
        }
        if matches!(self.fault, HookFault::SharedSiblingCleanupBudget)
            && let LifecycleEvent::StepFinished { context, .. } = event
        {
            match context.node.as_str() {
                "a" => tokio::time::sleep(Duration::from_secs(20)).await,
                "b" => {
                    let _wait = HookWait(Arc::clone(&self.evidence));
                    self.evidence.post_entered.notify_one();
                    self.evidence.post_release.notified().await;
                }
                _ => {}
            }
        }
        if matches!(self.fault, HookFault::SharedCleanupBudget) {
            match event {
                LifecycleEvent::StepFinished { .. } => {
                    tokio::time::sleep(Duration::from_secs(20)).await
                }
                LifecycleEvent::StreamFinished { .. } => {
                    let _wait = HookWait(Arc::clone(&self.evidence));
                    self.evidence.post_entered.notify_one();
                    self.evidence.post_release.notified().await;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

struct HookWait(Arc<HookEvidence>);

impl Drop for HookWait {
    fn drop(&mut self) {
        self.0.waits_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for RuntimeHooks {
    fn drop(&mut self) {
        self.evidence.owner_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct HookAction {
    declaration: ActionDeclaration,
    evidence: Arc<HookEvidence>,
    ending: SourceEnding,
}

struct HookSource {
    local: Rc<()>,
    evidence: Arc<HookEvidence>,
    ending: SourceEnding,
    events: std::vec::IntoIter<ActionEvent>,
    terminal: bool,
}

impl ActionBackend for HookAction {
    type Stream = HookSource;
    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }
    fn start(&self, request: ActionRequest) -> Self::Stream {
        assert_eq!(request.state.values()["count"], 0);
        assert_eq!(request.inputs["prompt"], "private caller input");
        self.evidence.source_contexts.lock().unwrap().push(json!({
            "node":request.context.node,"checkpoint":request.context.checkpoint,
            "ordinal":request.context.action_ordinal,
        }));
        self.evidence.constructed.fetch_add(1, Ordering::SeqCst);
        HookSource {
            local: Rc::new(()),
            evidence: Arc::clone(&self.evidence),
            ending: self.ending,
            events: self.ending.events(),
            terminal: false,
        }
    }
}

impl Stream for HookSource {
    type Item = Result<ActionEvent, AppError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        assert_eq!(Rc::strong_count(&self.local), 1);
        assert!(!self.terminal, "source cannot be polled after terminal/EOF");
        if let Some(event) = self.events.next() {
            return Poll::Ready(Some(Ok(event)));
        }
        match self.ending {
            SourceEnding::Pending
            | SourceEnding::DropPanic
            | SourceEnding::Spans(SpanProgram::OpenPending) => Poll::Pending,
            SourceEnding::Eof | SourceEnding::Spans(SpanProgram::OpenEof) => {
                self.terminal = true;
                Poll::Ready(None)
            }
            SourceEnding::Error | SourceEnding::Spans(SpanProgram::OpenError) => {
                self.terminal = true;
                Poll::Ready(Some(Err(AppError::Integration(
                    "private action error payload".into(),
                ))))
            }
            SourceEnding::Complete | SourceEnding::Spans(_) => {
                self.terminal = true;
                Poll::Ready(Some(Ok(ActionEvent::Completed {
                    result: Map::from_iter([("answer".into(), json!("private terminal output"))]),
                    writes: vec![StateChange::Assign {
                        values: Map::from_iter([("count".into(), json!(1))]),
                    }],
                })))
            }
        }
    }
}

impl Drop for HookSource {
    fn drop(&mut self) {
        self.evidence.source_dropped.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.ending, SourceEnding::DropPanic),
            "private source destructor panic"
        );
    }
}

struct HookTyping {
    evidence: Arc<HookEvidence>,
}

impl StateTyping for HookTyping {
    type Data = Map<String, Value>;

    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError> {
        JsonState.construct_data(state)
    }

    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError> {
        JsonState.construct_state(data)
    }

    fn validate_pre(&self, _state: &WorkflowState, _change: &StateChange) -> Result<(), AppError> {
        self.evidence.typing_pre.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct HookCase {
    app: App<StateStore, Agent, HookAction, NoConditions, HookTyping, RuntimeHooks>,
    run: WorkflowRun<StateStore, Agent, HookAction, NoConditions, HookTyping, RuntimeHooks>,
    evidence: Arc<HookEvidence>,
}

struct HookCompletionDispatch {
    released: Arc<Notify>,
}

impl tracing::Subscriber for HookCompletionDispatch {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        unreachable!("completion observation disables spans")
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

impl Drop for HookCompletionDispatch {
    fn drop(&mut self) {
        self.released.notify_one();
    }
}

impl HookCase {
    async fn terminal_completion_failure(background: bool) {
        let evidence = Arc::new(HookEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = Self::app(
            store,
            vec![Self::native_definition()],
            &evidence,
            SourceEnding::Complete,
            HookFault::Error {
                event: "execute-finish",
                lane: "async",
            },
            false,
        )
        .await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            // The first background driver starts a worker whose dispatcher is
            // retained until App shutdown. Warm it outside the operation-scoped
            // completion observer so only this run's handoff owns that observer.
            evidence.rejected.store(true, Ordering::SeqCst);
            let warm = workflow.start(ExecutionOptions::default()).await.unwrap();
            assert!(matches!(warm.wait().await.unwrap().status, RunStatus::Succeeded));
            drop(warm);
            evidence.rejected.store(false, Ordering::SeqCst);
            let typed_before = evidence.typing_pre.load(Ordering::SeqCst);
            let released = Arc::new(Notify::new());
            let dispatch = tracing::Dispatch::new(HookCompletionDispatch { released: Arc::clone(&released) });
            let (selector, checkpoint) = async {
                let mut run = if background { workflow.start(ExecutionOptions::default()).await.unwrap() }
                    else { workflow.prepare(WorkflowState::default()).await.unwrap() };
                let failure = if background { run.wait().await }
                    else { run.execution(ExecutionOptions::default()).await.unwrap().finish().await };
                assert!(failure.err().unwrap().to_string().contains("explicit caller hook context"));
                let terminal = run.snapshot().await.unwrap();
                assert!(matches!(terminal.status, RunStatus::Succeeded),
                    "this completion error follows an actually terminal durable checkpoint");
                assert_eq!(terminal.state.values()["count"], 1);
                let selector = RunSelector::Exact { id: run.id().clone(), occurrence: terminal.occurrence };
                drop(run);
                (selector, terminal.checkpoint)
            }.with_subscriber(dispatch).await;
            tokio::time::timeout(Duration::from_secs(3), released.notified()).await
                .expect("all original accepted tasks complete supervision before a new observer attaches");
            let mut run = workflow.open_run(selector.clone()).await.unwrap();
            for _ in 0..2 {
                assert!(run.wait().await.err().unwrap().to_string().contains("explicit caller hook context"),
                    "terminal completion authority survives zero public and task observers");
            }
            let invalid = ExecutionOptions { state: WorkflowState::new(Map::from_iter([
                ("count".into(), json!(99)),
            ])).unwrap(), ..ExecutionOptions::default() };
            assert!(matches!(run.execution(invalid).await, Err(AppError::InvalidInput(_))));
            assert!(run.wait().await.err().unwrap().to_string().contains("explicit caller hook context"));
            drop(run);
            let mut run = workflow.open_run(selector).await.unwrap();
            let terminal = run.execution(ExecutionOptions::default()).await.unwrap().finish().await.unwrap();
            assert!(matches!(terminal.status, RunStatus::Succeeded));
            assert_eq!(terminal.checkpoint, checkpoint, "successful reacquisition never replays terminal work");
            assert!(matches!(run.wait().await.unwrap().status, RunStatus::Succeeded));
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), typed_before + 1);
        })).catch_unwind().await;
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("terminal completion failure observation remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed.expect("terminal completion owners join").unwrap();
    }

    async fn rejects_span_finish(program: SpanProgram) {
        let mut case = Self::open(SourceEnding::Spans(program), HookFault::None, false).await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let execution = case.run.execution(Self::options()).await.unwrap();
            let _ = execution.finish().await;
            let snapshot = case.run.snapshot().await.unwrap();
            assert!(matches!(
                snapshot.status,
                RunStatus::Failed(WorkflowFailure::Execution(_))
            ));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
            assert_eq!(
                case.evidence.span_endings(),
                ["child:sync", "child:async", "parent:sync", "parent:async"],
                "invalid finish cannot fabricate an extra end or close a parent before its child"
            );
            let calls = case.evidence.calls();
            assert_eq!(
                calls
                    .iter()
                    .filter(|row| row["event"] == "span-start")
                    .count(),
                4,
                "a closed identity cannot start a second span"
            );
            assert!(!calls.iter().any(|row| row["event"] == "stream-item"));
            assert!(
                !calls
                    .iter()
                    .any(|row| row["event"] == "span-finish" && row["id"] == "missing")
            );
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        }))
        .catch_unwind()
        .await;
        case.close(checked).await;
    }

    async fn assert_eof_read_disposition(permanent: bool) {
        let evidence = Arc::new(HookEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let definition = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
            "entry":"work","nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],
            "routes":{}
        }).to_string()).unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: inner.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![definition],
            &evidence,
            SourceEnding::Eof,
            HookFault::EofRead { permanent },
            false,
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new(
                "app-sdk",
                format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence),
            )
            .unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            let initial = inner.load(key.clone()).await.unwrap().unwrap();
            let eof = tokio::time::timeout(Duration::from_secs(3), execution.next())
                .await
                .expect("the original EOF observation remains bounded");
            assert_eq!(
                evidence.cleanup_read_faults.load(Ordering::SeqCst),
                1,
                "the real successful EOF guard selects exactly one authority-read failure"
            );
            assert!(evidence.cleanup_read.lock().unwrap().is_none());
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            if permanent {
                assert!(
                    eof.is_err(),
                    "unreadable authority cannot invent successful EOF settlement"
                );
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        match app
                            .list_workspaces(None, NonZeroUsize::new(1).unwrap())
                            .await
                        {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected global closure observation: {error}"),
                        }
                    }
                })
                .await
                .expect("Permanent EOF authority loss closes the App before another effect");
                let retained = inner.load(key.clone()).await.unwrap().unwrap();
                assert_eq!(
                    retained.record.value, initial.record.value,
                    "failed cleanup authority cannot synthesize a rollback or failure checkpoint"
                );
                drop(execution);
                assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
            } else {
                assert!(
                    eof.expect("Recoverable EOF read retains the original successful guard")
                        .is_none()
                );
                let finished = execution.finish().await.unwrap();
                assert!(matches!(finished.status, RunStatus::Paused));
                assert_eq!(finished.state.values()["count"], 0);
                assert!(finished.last_action_ordinal.is_none() && finished.outcomes.is_empty());
                assert_eq!(
                    evidence.constructed.load(Ordering::SeqCst),
                    1,
                    "read recovery and finish cannot replay the EOF source"
                );
                assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            }
            let head = inner.load(key).await.unwrap().unwrap();
            let boundary = inner
                .load(
                    StateKey::new(
                        "app-sdk",
                        format!(
                            "history/{}/checkpoint/{:020}",
                            head.record.key.key().strip_prefix("run/").unwrap(),
                            head.record.value["sequence"].as_u64().unwrap(),
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(boundary.record.value["checkpoint"]["state"]["count"], 0);
            assert_eq!(boundary.record.value["checkpoint"]["position"], Value::Null);
            assert_eq!(boundary.record.value["checkpoint"]["outcomes"], json!([]));
        }))
        .catch_unwind()
        .await;
        evidence.cleanup_read.lock().unwrap().take();
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("EOF authority proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed
            .expect("EOF cleanup does not panic")
            .expect("EOF cleanup joins");
        if permanent {
            assert!(
                matches!(closed, Err(AppError::Integration(_))),
                "fatal cleanup retains the original authority failure"
            );
        } else {
            closed.unwrap();
        }
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
        let calls = evidence.calls();
        for lane in ["sync", "async"] {
            for event in ["step-start", "step-finish", "stream-start", "stream-finish"] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|row| row["event"] == event && row["lane"] == lane)
                        .count(),
                    1,
                    "cleanup read disposition does not replay a completed original guard"
                );
            }
        }
        for step in calls.iter().filter(|row| row["event"] == "step-finish") {
            assert!(step["outcome"].is_null() && step["committed_ordinal"].is_null());
            assert_eq!(step["state"]["count"], 0);
        }
    }

    async fn assert_execution_read_disposition(fault: CleanupReadFault, phase: ExecutionReadPhase) {
        let evidence = Arc::new(HookEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let definition = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
            "entry":"work","nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],
            "routes":{}
        }).to_string()).unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: inner.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![definition],
            &evidence,
            SourceEnding::Pending,
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let started = matches!(phase, ExecutionReadPhase::AfterPartial);
        let sources = usize::from(started);
        let held = matches!(fault, CleanupReadFault::Held);
        let fatal = !matches!(fault, CleanupReadFault::Recoverable);
        let checked = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(if held { 36 } else { 8 }), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence)).unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            if started {
                assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
            }
            let initial = inner.load(key.clone()).await.unwrap().unwrap();
            *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), fault));
            let aborted = {
                let mut closing = Box::pin(async move {
                    if matches!(phase, ExecutionReadPhase::FirstPoll) {
                        let observed = execution.next().await;
                        drop(execution);
                        match observed {
                            Err(error) => Err(error),
                            Ok(_) => panic!("unreadable initial authority cannot yield execution work"),
                        }
                    } else {
                        execution.abort().await
                    }
                });
                if held {
                    tokio::select! {
                        _ = evidence.post_entered.notified() => {},
                        result = &mut closing => panic!("execution did not enter its selected authority read: {}", result.is_ok()),
                    }
                    assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 0);
                    assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), sources);
                    assert_eq!(inner.load(key.clone()).await.unwrap().unwrap(), initial);
                }
                tokio::time::timeout(Duration::from_secs(if held { 33 } else { 3 }), closing)
                    .await.expect("execution authority remains inside its original observation budget")
            };
            assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);
            assert!(evidence.cleanup_read.lock().unwrap().is_none());
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), sources);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), sources);
            if held {
                assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1,
                    "fatal expiry joins destruction of the original read");
            }
            if fatal {
                assert!(aborted.is_err(), "unreadable cleanup authority cannot report a successful abort");
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected closure observation: {error}"),
                        }
                    }
                }).await.expect("Permanent or exhausted abort authority closes the App");
                assert_eq!(inner.load(key.clone()).await.unwrap().unwrap(), initial,
                    "unreadable authority cannot synthesize a checkpoint");
                assert!(matches!(run.snapshot().await, Err(AppError::Closed(_))));
            } else {
                let finished = aborted.expect("Recoverable read retains the same abort publication");
                assert!(matches!(finished.status, RunStatus::Paused));
                assert_eq!(finished.state.values()["count"], 0);
                assert!(finished.last_action_ordinal.is_none() && finished.outcomes.is_empty());
            }
            let head = inner.load(key).await.unwrap().unwrap();
            let boundary = inner.load(StateKey::new("app-sdk", format!(
                "history/{}/checkpoint/{:020}", head.record.key.key().strip_prefix("run/").unwrap(),
                head.record.value["sequence"].as_u64().unwrap(),
            )).unwrap()).await.unwrap().unwrap();
            assert_eq!(boundary.record.value["checkpoint"]["state"]["count"], 0);
            assert_eq!(boundary.record.value["checkpoint"]["position"], Value::Null);
            assert_eq!(boundary.record.value["checkpoint"]["outcomes"], json!([]));
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence.post_release.notify_one();
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("abort authority proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed
            .expect("abort cleanup does not panic")
            .expect("abort cleanup joins");
        if fatal {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        } else {
            closed.unwrap();
        }
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), sources);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), sources);
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
        let calls = evidence.calls();
        if !started {
            assert!(
                calls.iter().all(|row| row["event"] == "registered"),
                "unstarted execution cannot fabricate execution, step, or stream guards"
            );
            for lane in ["sync", "async"] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|row| row["event"] == "registered" && row["lane"] == lane)
                        .count(),
                    1
                );
            }
            return;
        }
        for lane in ["sync", "async"] {
            for event in ["step-start", "stream-start"] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|row| row["event"] == event && row["lane"] == lane)
                        .count(),
                    1,
                    "the original source and its guards are never replayed"
                );
            }
            if !fatal {
                for event in ["step-finish", "stream-finish", "execute-finish"] {
                    assert_eq!(
                        calls
                            .iter()
                            .filter(|row| row["event"] == event && row["lane"] == lane)
                            .count(),
                        1,
                        "successful abort closes each original guard once"
                    );
                }
            }
        }
    }

    fn options() -> ExecutionOptions {
        ExecutionOptions {
            inputs: Map::from_iter([("prompt".into(), json!("private caller input"))]),
            ..ExecutionOptions::default()
        }
    }

    async fn open(ending: SourceEnding, fault: HookFault, no_outcome: bool) -> Self {
        let evidence = Arc::new(HookEvidence::default());
        let mut state = json!({"count":0,"private_state":"private durable value"});
        if no_outcome {
            let key = format!("hook_{}", uuid::Uuid::new_v4().simple());
            state[&key] = json!("codec probe");
            let captured = Arc::clone(&evidence);
            StateSerialization::global()
                .register_field(
                    key,
                    move |value| {
                        captured.encoded.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &'static str>(value.clone())
                    },
                    |value| Ok::<_, &'static str>(value.clone()),
                )
                .unwrap();
        }
        let reducers: Map<String, Value> = state
            .as_object()
            .unwrap()
            .keys()
            .map(|key| (key.clone(), json!("reject_concurrent")))
            .collect();
        let definition = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1", "state":state,
            "required":["count"],"reducers":reducers, "entry":"work",
            "nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],"routes":{}
        }).to_string()).unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = Self::app(
            store,
            vec![definition],
            &evidence,
            ending,
            fault,
            no_outcome,
        )
        .await;
        let prepared =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                workspace
                    .workflow("hooks".parse().unwrap())
                    .await
                    .unwrap()
                    .prepare(WorkflowState::default())
                    .await
                    .unwrap()
            }))
            .catch_unwind()
            .await;
        match prepared {
            Ok(Ok(run)) => Self { app, run, evidence },
            failed => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await;
                match failed {
                    Err(panic) => std::panic::resume_unwind(panic),
                    _ => panic!("hook fixture preparation timed out"),
                }
            }
        }
    }

    async fn app(
        store: StateStore,
        definitions: Vec<WorkflowDefinition>,
        evidence: &Arc<HookEvidence>,
        ending: SourceEnding,
        fault: HookFault,
        no_outcome: bool,
    ) -> App<StateStore, Agent, HookAction, NoConditions, HookTyping, RuntimeHooks> {
        App::open(Self::config(
            store,
            definitions,
            evidence,
            ending,
            fault,
            no_outcome,
        ))
        .await
        .unwrap()
    }

    fn config<CS: StateStoreBackend>(
        store: CS,
        definitions: Vec<WorkflowDefinition>,
        evidence: &Arc<HookEvidence>,
        ending: SourceEnding,
        fault: HookFault,
        no_outcome: bool,
    ) -> AppConfig<CS, Agent, HookAction, NoConditions, HookTyping, RuntimeHooks> {
        let config: AppConfig<CS, Agent> = AppConfig::new(store, Vec::new(), definitions);
        config
            .with_actions(vec![HookAction {
                declaration: ActionDeclaration {
                    key: "local".into(),
                    revision: "r1".into(),
                    reads: vec!["count".into()],
                    writes: vec!["count".into()],
                    required_inputs: vec!["prompt".into()],
                    optional_inputs: Vec::new(),
                    bound_inputs: Map::new(),
                    default_inputs: Map::new(),
                    tags: Vec::new(),
                    source: Some("local hook fixture".into()),
                    mode: ActionMode::Asynchronous,
                },
                evidence: Arc::clone(evidence),
                ending,
            }])
            .with_state_typing(HookTyping {
                evidence: Arc::clone(evidence),
            })
            .with_hooks(RuntimeHooks {
                evidence: Arc::clone(evidence),
                fault,
                inspect_no_outcome: no_outcome,
            })
    }

    fn native_prefix_definition() -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
                "state":{"count":0,"native":"not an integer"},
                "required":["count","native"],
                "reducers":{"count":"reject_concurrent","native":"reject_concurrent"},
                "entry":"fork","nodes":[
                    {"id":"fork","step":{"kind":"state","mutations":[]}},
                    {"id":"a_native","step":{"kind":"state","mutations":[
                        {"kind":"increment","target":"native","value":{"kind":"literal","value":1}}
                    ]}},
                    {"id":"b_native","step":{"kind":"state","mutations":[
                        {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
                    ]}}
                ],"routes":{"fork":{"mode":"broadcast","branches":[
                    {"target":{"kind":"node","id":"b_native"}},
                    {"target":{"kind":"node","id":"a_native"}}
                ]}}
            })
            .to_string(),
        )
        .unwrap()
    }

    fn native_definition() -> WorkflowDefinition {
        WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
                "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
                "entry":"work","nodes":[{"id":"work","step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
                ]}}],"routes":{}
            })
            .to_string(),
        )
        .unwrap()
    }

    fn map_definitions() -> Vec<WorkflowDefinition> {
        let parent = WorkflowDefinition::parse(&json!({
        "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
        "state":{"items":[10,20],"collected":[]},"required":["items","collected"],
        "reducers":{"items":"reject_concurrent","collected":"collect"},"entry":"map",
        "nodes":[{"id":"map","step":{"kind":"map","workflow":"leaf",
            "items":{"kind":"state","path":"items"},"input_key":"item","max_parallel":2,
            "reductions":[{"source":"item","target":"collected","reducer":"collect"}]}}],"routes":{}
    }).to_string()).unwrap();
        let child = WorkflowDefinition::parse(&json!({
        "schema":"app-sdk.workflow/v2","id":"leaf","revision":"r1",
        "state":{"item":0},"required":["item"],"reducers":{"item":"reject_concurrent"},"entry":"add",
        "nodes":[{"id":"add","step":{"kind":"state","mutations":[
            {"kind":"increment","target":"item","value":{"kind":"literal","value":1}}
        ]}}],"routes":{}
    }).to_string()).unwrap();
        vec![parent, child]
    }

    async fn native(fault: HookFault, background: bool) -> Self {
        Self::from_definitions(vec![Self::native_definition()], fault, background).await
    }

    async fn from_definitions(
        definitions: Vec<WorkflowDefinition>,
        fault: HookFault,
        background: bool,
    ) -> Self {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        Self::from_source(
            store,
            definitions,
            SourceEnding::Complete,
            fault,
            background,
        )
        .await
    }

    async fn from_source(
        store: StateStore,
        definitions: Vec<WorkflowDefinition>,
        ending: SourceEnding,
        fault: HookFault,
        background: bool,
    ) -> Self {
        let evidence = Arc::new(HookEvidence::default());
        let mut app = Self::app(store, definitions, &evidence, ending, fault, false).await;
        let admitted =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(4), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
                if background {
                    workflow.start(ExecutionOptions::default()).await.unwrap()
                } else {
                    workflow.prepare(WorkflowState::default()).await.unwrap()
                }
            }))
            .catch_unwind()
            .await;
        match admitted {
            Ok(Ok(run)) => Self { app, run, evidence },
            failed => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await;
                match failed {
                    Err(panic) => std::panic::resume_unwind(panic),
                    _ => panic!("native hook fixture admission timed out"),
                }
            }
        }
    }

    async fn close(
        mut self,
        checked: std::thread::Result<Result<(), tokio::time::error::Elapsed>>,
    ) {
        let callbacks_before_cleanup = self.evidence.names();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            self.app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(self.run);
        drop(self.app);
        match checked {
            Ok(result) => result.expect("hook scenario remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("hook App cleanup is bounded")
            .expect("hook App cleanup joins");
        assert_eq!(self.evidence.owner_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(
            self.evidence.names(),
            callbacks_before_cleanup,
            "App cleanup cannot repeat or upgrade closed execution hook lanes"
        );
    }
}

#[tokio::test]
async fn caller_span_events_preserve_nesting_targets_content_and_both_hook_lanes() {
    let mut case = HookCase::open(
        SourceEnding::Spans(SpanProgram::Nested),
        HookFault::None,
        false,
    )
    .await;
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            let calls = case.evidence.calls();
            let spans = calls
                .iter()
                .filter(|row| row["event"].as_str().unwrap().starts_with("span-"))
                .collect::<Vec<_>>();
            assert_eq!(
                spans.len(),
                12,
                "all six source observations invoke sync then async exactly once"
            );
            for pair in spans.as_chunks::<2>().0 {
                assert_eq!(pair[0]["lane"], "sync");
                assert_eq!(pair[1]["lane"], "async");
                let mut first = pair[0].clone();
                first["lane"] = json!("async");
                assert_eq!(first, *pair[1]);
                assert_eq!(pair[0]["node"], "work");
                assert_eq!(pair[0]["ordinal"], 0);
            }
            assert_eq!(spans[0]["id"], "parent");
            assert!(spans[0]["parent"].is_null());
            assert_eq!(spans[2]["id"], "child");
            assert_eq!(spans[2]["parent"], "parent");
            assert_eq!(spans[0]["name"], spans[2]["name"]);
            assert_eq!(
                spans[2]["dependencies"],
                json!(["upstream-b", "upstream-a", "upstream-b"])
            );
            assert_eq!(
                spans[4]["span"], "parent",
                "parent-targeted attribute stays on the parent while child is open"
            );
            assert!(spans[6]["span"].is_null());
            for index in [4, 6] {
                assert_eq!(
                    spans[index]["value"],
                    json!({"secret":"caller-only span payload"})
                );
                assert_eq!(spans[index]["tags"], json!({"source":"caller"}));
            }
            assert_eq!(spans[8]["id"], "child");
            assert_eq!(spans[10]["id"], "parent");
            let snapshot = execution.finish().await.unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(
                case.evidence
                    .names()
                    .iter()
                    .filter(|name| name.starts_with("span-"))
                    .count(),
                12
            );
        }))
        .catch_unwind()
        .await;
    case.close(checked).await;
}

#[tokio::test]
async fn remaining_caller_spans_close_before_step_on_completion_source_error_and_eof() {
    for program in [
        SpanProgram::OpenComplete,
        SpanProgram::OpenError,
        SpanProgram::OpenEof,
    ] {
        let mut case = HookCase::open(SourceEnding::Spans(program), HookFault::None, false).await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            assert!(case.evidence.span_endings().is_empty());
            let snapshot = execution.finish().await.unwrap();
            match program {
                SpanProgram::OpenComplete => {
                    assert!(matches!(snapshot.status, RunStatus::Succeeded));
                    assert_eq!(snapshot.state.values()["count"], 1);
                    assert_eq!(snapshot.last_action_ordinal, Some(0));
                    assert_eq!(snapshot.outcomes.len(), 1);
                }
                SpanProgram::OpenError => {
                    assert!(matches!(
                        snapshot.status,
                        RunStatus::Failed(WorkflowFailure::Action(_))
                    ));
                    assert_eq!(snapshot.state.values()["count"], 0);
                    assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
                }
                SpanProgram::OpenEof => {
                    assert!(matches!(snapshot.status, RunStatus::Paused));
                    assert_eq!(snapshot.state.values()["count"], 0);
                    assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
                }
                _ => unreachable!(),
            }
            assert_eq!(
                case.evidence.span_endings(),
                ["child:sync", "child:async", "parent:sync", "parent:async"]
            );
            let calls = case.evidence.calls();
            let last_span = calls
                .iter()
                .rposition(|row| row["event"] == "span-finish")
                .unwrap();
            let first_step = calls
                .iter()
                .position(|row| row["event"] == "step-finish")
                .unwrap();
            assert!(last_span < first_step);
            assert!(
                calls
                    .iter()
                    .filter(|row| row["event"] == "stream-item")
                    .all(|row| row["span"].is_null()),
                "an unaddressed partial stays action-scoped while explicit spans are open"
            );
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        }))
        .catch_unwind()
        .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn caller_span_end_hook_failure_rejects_explicit_and_automatic_completion_before_commit() {
    for program in [
        SpanProgram::Nested,
        SpanProgram::OpenComplete,
        SpanProgram::OpenError,
    ] {
        for lane in ["sync", "async"] {
            let mut case = HookCase::open(
                SourceEnding::Spans(program),
                HookFault::Error {
                    event: "span-finish",
                    lane,
                },
                false,
            )
            .await;
            let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                let execution = case.run.execution(HookCase::options()).await.unwrap();
                let _ = execution.finish().await;
                let snapshot = case.run.snapshot().await.unwrap();
                if matches!(program, SpanProgram::OpenError) {
                    assert!(
                        matches!(
                            snapshot.status,
                            RunStatus::Failed(WorkflowFailure::Action(_))
                        ),
                        "a later span end error cannot replace the authoritative action failure"
                    );
                } else {
                    assert!(matches!(
                        snapshot.status,
                        RunStatus::Failed(WorkflowFailure::Hook { .. })
                    ));
                }
                assert_eq!(snapshot.state.values()["count"], 0);
                assert!(
                    snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty(),
                    "automatic span completion cannot publish the caller's successful result first"
                );
                let expected = if lane == "sync" {
                    vec!["child:sync", "parent:sync", "parent:async"]
                } else {
                    vec!["child:sync", "child:async", "parent:sync", "parent:async"]
                };
                assert_eq!(
                    case.evidence.span_endings(),
                    expected,
                    "a failed end is not retried, and enclosing async scope cleanup still finishes"
                );
                let calls = case.evidence.calls();
                if matches!(program, SpanProgram::OpenError) {
                    let finishes = calls
                        .iter()
                        .filter(|row| row["event"] == "execute-finish")
                        .collect::<Vec<_>>();
                    assert_eq!(finishes.len(), 2);
                    for row in finishes {
                        assert_eq!(row["failure"]["category"], "hook");
                        assert_eq!(row["failure"]["cause"]["category"], "action");
                    }
                    assert!(
                        calls
                            .iter()
                            .filter(|row| row["event"] == "step-finish")
                            .all(|row| row["outcome"]["failure"]["category"] == "action")
                    );
                }
                let last_span = calls
                    .iter()
                    .rposition(|row| row["event"] == "span-finish")
                    .unwrap();
                let first_step = calls
                    .iter()
                    .position(|row| row["event"] == "step-finish")
                    .unwrap();
                assert!(last_span < first_step);
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
                assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            }))
            .catch_unwind()
            .await;
            case.close(checked).await;
        }
    }
}

#[tokio::test]
async fn caller_span_abort_and_owner_drop_close_remaining_children_in_their_own_lanes() {
    for owner_drop in [false, true] {
        let mut case = HookCase::open(
            SourceEnding::Spans(SpanProgram::OpenPending),
            HookFault::None,
            false,
        )
        .await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            assert!(case.evidence.span_endings().is_empty());
            let snapshot = if owner_drop {
                drop(execution);
                assert_eq!(
                    case.evidence.span_endings(),
                    ["child:sync", "parent:sync"],
                    "local Drop closes already observed spans synchronously"
                );
                case.run.wait().await.unwrap()
            } else {
                execution.abort().await.unwrap()
            };
            assert!(matches!(snapshot.status, RunStatus::Paused));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
            let expected = if owner_drop {
                vec!["child:sync", "parent:sync"]
            } else {
                vec!["child:sync", "child:async", "parent:sync", "parent:async"]
            };
            assert_eq!(case.evidence.span_endings(), expected);
            let calls = case.evidence.calls();
            assert!(
                calls
                    .iter()
                    .rposition(|row| row["event"] == "span-finish")
                    .unwrap()
                    < calls
                        .iter()
                        .position(|row| row["event"] == "step-finish")
                        .unwrap()
            );
            assert!(
                calls
                    .iter()
                    .filter(|row| row["event"] == "step-finish")
                    .all(|row| row["outcome"].is_null())
            );
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        }))
        .catch_unwind()
        .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn cancelled_next_retains_the_exact_caller_span_attribute_receipt() {
    let mut case = HookCase::open(
        SourceEnding::Spans(SpanProgram::OpenComplete),
        HookFault::HoldAttribute { fail: false },
        false,
    )
    .await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = case.evidence.post_entered.notified() => {},
            result = &mut next => panic!("span attribute did not enter its held callback: {}", result.is_ok()),
        }
        drop(next);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 0);
        assert!(case.evidence.span_endings().is_empty());
        case.evidence.post_release.notify_one();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
        let snapshot = execution.finish().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert_eq!(snapshot.outcomes.len(), 1);
        assert_eq!(case.evidence.span_endings(), ["child:sync", "child:async", "parent:sync", "parent:async"]);
        let calls = case.evidence.calls();
        let attributes = calls.iter().filter(|row| row["event"] == "span-attribute" && row["span"] == "parent").collect::<Vec<_>>();
        assert_eq!(attributes.len(), 2);
        assert_eq!(attributes[0]["lane"], "sync");
        assert_eq!(attributes[1]["lane"], "async");
        assert_eq!(attributes[1]["value"], json!({"secret":"caller-only span payload"}));
        assert_eq!(attributes[1]["ordinal"], 0);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn owner_drop_retains_caller_span_attribute_failure_and_defers_sync_closure() {
    for fail in [false, true] {
        let mut case = HookCase::open(
            SourceEnding::Spans(SpanProgram::OpenPending),
            HookFault::HoldAttribute { fail },
            false,
        )
        .await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            let mut next = Box::pin(execution.next());
            tokio::select! {
                _ = case.evidence.post_entered.notified() => {},
                result = &mut next => panic!("span attribute did not enter its held callback: {}", result.is_ok()),
            }
            drop(next);
            drop(execution);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 0);
            assert!(case.evidence.span_endings().is_empty(), "the admitted attribute settles before span closure");
            assert!(!case.evidence.names().iter().any(|name| name.starts_with("step-finish") || name.starts_with("execute-finish")));
            assert!(matches!(case.run.execution(HookCase::options()).await, Err(AppError::Busy(_))));
            case.evidence.post_release.notify_one();
            let snapshot = case.run.wait().await.unwrap();
            assert!(if fail { matches!(snapshot.status, RunStatus::Failed(WorkflowFailure::Hook { .. })) }
                else { matches!(snapshot.status, RunStatus::Paused) });
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
            assert_eq!(case.evidence.span_endings(), ["child:sync", "parent:sync"]);
            let calls = case.evidence.calls();
            assert_eq!(calls.iter().filter(|row| row["event"] == "span-attribute").count(), 2,
                "only the accepted attribute may finish after owner loss");
            assert!(!calls.iter().any(|row| row["event"] == "stream-item"));
            assert!(calls.iter().filter(|row| row["event"] == "span-finish" || row["event"] == "step-finish"
                || row["event"] == "stream-finish" || row["event"] == "execute-finish").all(|row| row["lane"] == "sync"));
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        })).catch_unwind().await;
        case.evidence.post_release.notify_one();
        case.close(checked).await;
    }
}

#[tokio::test]
async fn unknown_caller_span_finish_is_rejected_without_a_fabricated_end() {
    HookCase::rejects_span_finish(SpanProgram::UnknownFinish).await;
}

#[tokio::test]
async fn caller_parent_span_cannot_finish_while_its_child_is_open() {
    HookCase::rejects_span_finish(SpanProgram::ParentBeforeChild).await;
}

#[tokio::test]
async fn closed_caller_span_cannot_finish_twice() {
    HookCase::rejects_span_finish(SpanProgram::DoubleFinish).await;
}

#[tokio::test]
async fn caller_span_identity_cannot_be_reused_after_close() {
    HookCase::rejects_span_finish(SpanProgram::ClosedReuse).await;
}

#[tokio::test]
async fn caller_span_resource_boundaries_accept_exact_limits_and_reject_the_next_event() {
    for programs in [
        [
            SpanProgram::Count(4_095),
            SpanProgram::Count(4_096),
            SpanProgram::Count(4_097),
        ],
        [
            SpanProgram::Depth(63),
            SpanProgram::Depth(64),
            SpanProgram::Depth(65),
        ],
        [
            SpanProgram::TextBytes(65_535),
            SpanProgram::TextBytes(65_536),
            SpanProgram::TextBytes(65_537),
        ],
        [
            SpanProgram::EventBytes(1_048_575),
            SpanProgram::EventBytes(1_048_576),
            SpanProgram::EventBytes(1_048_577),
        ],
        [
            SpanProgram::RetainedBytes(1_048_575),
            SpanProgram::RetainedBytes(1_048_576),
            SpanProgram::RetainedBytes(1_048_577),
        ],
        [
            SpanProgram::Dependencies(4_095),
            SpanProgram::Dependencies(4_096),
            SpanProgram::Dependencies(4_097),
        ],
        [
            SpanProgram::Tags(4_095),
            SpanProgram::Tags(4_096),
            SpanProgram::Tags(4_097),
        ],
    ] {
        for (boundary, program) in programs.into_iter().enumerate() {
            let (label, starts, attributes) = match program {
                SpanProgram::Count(value) => ("distinct ids including closed", value.min(4_096), 0),
                SpanProgram::Depth(value) => ("open ancestry depth", value.min(64), 0),
                SpanProgram::TextBytes(_) => ("individual id bytes", usize::from(boundary < 2), 0),
                SpanProgram::EventBytes(_) => ("whole event bytes", 0, usize::from(boundary < 2)),
                SpanProgram::RetainedBytes(_) => ("retained id bytes including closed", 16, 0),
                SpanProgram::Dependencies(_) => {
                    ("dependency entries", usize::from(boundary < 2), 0)
                }
                SpanProgram::Tags(_) => ("tag entries", 0, usize::from(boundary < 2)),
                _ => unreachable!(),
            };
            let mut case =
                HookCase::open(SourceEnding::Spans(program), HookFault::None, false).await;
            // The count case crosses 8,000 real metadata receipts and 16,000
            // callbacks. Give that work its own proportional outer budget;
            // a Timeout is never accepted as the required Execution failure.
            let budget = Duration::from_secs(10 + (starts as u64).div_ceil(64));
            let checked = AssertUnwindSafe(tokio::time::timeout(budget, async {
                let execution = case.run.execution(HookCase::options()).await.unwrap();
                let _ = execution.finish().await;
                let snapshot = case.run.snapshot().await.unwrap();
                if boundary < 2 {
                    assert!(
                        matches!(snapshot.status, RunStatus::Succeeded),
                        "{label}, boundary {boundary}"
                    );
                    assert_eq!(
                        snapshot.state.values()["count"],
                        1,
                        "{label}, boundary {boundary}"
                    );
                    assert_eq!(snapshot.last_action_ordinal, Some(0));
                    assert_eq!(snapshot.outcomes.len(), 1);
                } else {
                    assert!(
                        matches!(
                            snapshot.status,
                            RunStatus::Failed(WorkflowFailure::Execution(_))
                        ),
                        "{label} must reject the first over-limit event as Execution"
                    );
                    assert_eq!(snapshot.state.values()["count"], 0, "{label}");
                    assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
                }
                let calls = case.evidence.calls();
                for (event, expected) in [
                    ("span-start", starts * 2),
                    ("span-finish", starts * 2),
                    ("span-attribute", attributes * 2),
                    ("stream-item", if boundary < 2 { 2 } else { 0 }),
                ] {
                    assert_eq!(
                        calls.iter().filter(|row| row["event"] == event).count(),
                        expected,
                        "{label}, boundary {boundary}, event {event}"
                    );
                }
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
                assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            }))
            .catch_unwind()
            .await;
            case.close(checked).await;
        }
    }
}

#[tokio::test]
async fn invalid_caller_span_identity_order_and_tags_fail_before_action_commit() {
    for program in [
        SpanProgram::UnknownParent,
        SpanProgram::Duplicate,
        SpanProgram::ClosedAttribute,
        SpanProgram::NonStringTags,
    ] {
        let mut case = HookCase::open(SourceEnding::Spans(program), HookFault::None, false).await;
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                let execution = case.run.execution(HookCase::options()).await.unwrap();
                // Ordinary source failures are authoritative on the snapshot;
                // this does not invent a separate outward error rule for spans.
                let _ = execution.finish().await;
                let snapshot = case.run.snapshot().await.unwrap();
                assert!(matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Execution(_))
                ));
                assert_eq!(snapshot.state.values()["count"], 0);
                assert_eq!(snapshot.last_action_ordinal, None);
                assert!(snapshot.outcomes.is_empty());
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
                assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
                let calls = case.evidence.calls();
                assert!(!calls.iter().any(|row| row["event"] == "stream-item"));
                assert!(
                    !calls
                        .iter()
                        .any(|row| row["event"] == "span-attribute" && row["key"] == "too-late")
                );
                assert!(
                    !calls
                        .iter()
                        .any(|row| row["event"] == "span-attribute" && row["tags"]["source"] == 7)
                );
                if matches!(program, SpanProgram::UnknownParent) {
                    assert!(!calls.iter().any(|row| row["event"] == "span-start"));
                }
                if matches!(program, SpanProgram::Duplicate) {
                    assert_eq!(
                        calls
                            .iter()
                            .filter(|row| row["event"] == "span-start")
                            .count(),
                        2
                    );
                }
            }))
            .catch_unwind()
            .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn async_hooks_bracket_real_streaming_with_committed_state_and_exact_lane_order() {
    let mut case = HookCase::open(SourceEnding::Complete, HookFault::None, false).await;
    let checked = std::panic::AssertUnwindSafe(async {
        assert_eq!(
            case.evidence.names(),
            ["registered:sync", "registered:async"]
        );
        let registered = case.evidence.calls();
        assert!(
            !registered[0]["parent"].as_bool().unwrap()
                && !registered[0]["lineage"].as_bool().unwrap()
        );
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        assert_eq!(
            case.evidence.names().len(),
            2,
            "execution admission stays lazy"
        );
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
        assert!(matches!(
            execution.next().await.unwrap(),
            Some(ExecutionEvent::Partial { .. })
        ));
        let partial = case.evidence.calls();
        assert_eq!(partial.last().unwrap()["event"], "stream-item");
        assert_eq!(
            partial.last().unwrap()["result"]["text"],
            "private partial output"
        );
        let snapshot = execution.finish().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert_eq!(
            case.evidence.names(),
            [
                "registered:sync",
                "registered:async",
                "execute-start:sync",
                "execute-start:async",
                "step-start:sync",
                "step-start:async",
                "stream-start:sync",
                "stream-start:async",
                "stream-item:sync",
                "stream-item:async",
                "step-finish:sync",
                "step-finish:async",
                "stream-finish:sync",
                "stream-finish:async",
                "execute-finish:sync",
                "execute-finish:async",
            ]
        );
        let calls = case.evidence.calls();
        for row in calls.iter().filter(|row| row["event"] == "step-start") {
            assert_eq!(row["state"]["count"], 0);
            assert_eq!(row["inputs"]["prompt"], "private caller input");
            assert_eq!(row["ordinal"], 0);
            assert_eq!(row["asynchronous"], true);
        }
        for row in calls.iter().filter(|row| row["event"] == "step-finish") {
            assert_eq!(row["state"]["count"], 1);
            assert_eq!(row["committed_ordinal"], 0);
            assert_eq!(row["outcome"]["kind"], "success");
            assert_eq!(
                row["outcome"]["result"]["answer"],
                "private terminal output"
            );
        }
        for row in calls.iter().filter(|row| row["event"] == "stream-finish") {
            assert_eq!(row["items"], 1);
            assert!(row["failure"].is_null());
        }
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
    });
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
            .catch_unwind()
            .await;
    case.close(checked).await;
}

#[tokio::test]
async fn failed_action_hooks_borrow_the_prior_state_and_redacted_failure_without_a_result() {
    let mut case = HookCase::open(SourceEnding::Error, HookFault::None, false).await;
    let checked = std::panic::AssertUnwindSafe(async {
        let execution = case.run.execution(HookCase::options()).await.unwrap();
        let _ = execution.finish().await;
        let snapshot = case.run.wait().await.unwrap();
        assert!(matches!(
            snapshot.status,
            RunStatus::Failed(WorkflowFailure::Action(_))
        ));
        let calls = case.evidence.calls();
        let post: Vec<_> = calls
            .iter()
            .filter(|row| row["event"] == "step-finish")
            .collect();
        assert_eq!(post.len(), 2);
        for row in post {
            assert_eq!(row["state"]["count"], 0);
            assert!(row["committed_ordinal"].is_null());
            assert_eq!(row["outcome"]["kind"], "failure");
            assert_eq!(row["outcome"]["failure"]["category"], "action");
            assert!(
                !row["outcome"]["failure"]
                    .to_string()
                    .contains("private action error payload")
            );
        }
        assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
    });
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
            .catch_unwind()
            .await;
    case.close(checked).await;
}

#[tokio::test]
async fn pre_hook_failure_stops_dispatch_and_a_sync_error_skips_its_async_callback() {
    for lane in ["sync", "async"] {
        let mut case = HookCase::open(
            SourceEnding::Complete,
            HookFault::Error {
                event: "step-start",
                lane,
            },
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let execution = case.run.execution(HookCase::options()).await.unwrap();
            let result = execution.finish().await;
            assert!(result.is_err(), "pre hook failure is functional");
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("explicit caller hook context")
            );
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
            let names = case.evidence.names();
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "step-start:sync")
                    .count(),
                1
            );
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "step-start:async")
                    .count(),
                usize::from(lane == "async")
            );
            assert!(
                !names
                    .iter()
                    .any(|name| name.starts_with("stream-") || name.starts_with("step-finish"))
            );
            let snapshot = case.run.snapshot().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        });
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
                .catch_unwind()
                .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn stream_item_hook_failure_discards_output_and_uncommitted_state() {
    for lane in ["sync", "async"] {
        let mut case = HookCase::open(
            SourceEnding::Complete,
            HookFault::Error {
                event: "stream-item",
                lane,
            },
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(
                execution.next().await.is_err(),
                "a rejected item must not escape"
            );
            let _ = execution.finish().await;
            let snapshot = case.run.snapshot().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
            let calls = case.evidence.calls();
            assert_eq!(
                calls
                    .iter()
                    .filter(|row| row["event"] == "step-finish")
                    .count(),
                2
            );
            for row in calls.iter().filter(|row| row["event"] == "step-finish") {
                assert_eq!(row["outcome"]["kind"], "failure");
                assert_eq!(row["state"]["count"], 0);
                assert!(row["committed_ordinal"].is_null());
            }
            assert_eq!(
                calls
                    .iter()
                    .filter(|row| row["event"] == "stream-item" && row["lane"] == "async")
                    .count(),
                usize::from(lane == "async")
            );
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        });
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
                .catch_unwind()
                .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn post_hook_failure_preserves_the_commit_and_never_replays_the_completed_action() {
    for lane in ["sync", "async"] {
        let mut case = HookCase::open(
            SourceEnding::Complete,
            HookFault::Error {
                event: "step-finish",
                lane,
            },
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let execution = case.run.execution(HookCase::options()).await.unwrap();
            let result = execution.finish().await;
            assert!(result.is_err());
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("explicit caller hook context")
            );
            let snapshot = case.run.snapshot().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert_eq!(snapshot.outcomes.len(), 1);
            let names = case.evidence.names();
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "step-finish:sync")
                    .count(),
                1
            );
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "step-finish:async")
                    .count(),
                usize::from(lane == "async")
            );
            let replacement = case.run.execution(HookCase::options()).await.unwrap();
            let result = replacement.finish().await.unwrap();
            assert!(matches!(result.status, RunStatus::Succeeded));
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        });
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
                .catch_unwind()
                .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn pre_hook_panics_are_contained_and_redacted_before_source_construction() {
    for lane in ["sync", "async"] {
        let mut case = HookCase::open(
            SourceEnding::Complete,
            HookFault::Panic {
                event: "step-start",
                lane,
            },
            false,
        )
        .await;
        let checked = std::panic::AssertUnwindSafe(async {
            let execution = case.run.execution(HookCase::options()).await.unwrap();
            let observed = std::panic::AssertUnwindSafe(execution.finish())
                .catch_unwind()
                .await;
            assert!(
                observed.is_ok(),
                "hook panic must not escape the execution owner"
            );
            let _ = observed.unwrap();
            let snapshot = case.run.wait().await.unwrap();
            let RunStatus::Failed(WorkflowFailure::Execution(message)) = snapshot.status else {
                panic!("hook panic must be Execution failure")
            };
            assert!(!message.contains("private hook panic payload"));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty());
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
        });
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
                .catch_unwind()
                .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn eof_abort_and_drop_report_no_outcome_with_no_fabricated_tracking_record() {
    for ending in ["eof", "abort", "drop"] {
        let mode = if ending == "eof" {
            SourceEnding::Eof
        } else {
            SourceEnding::Pending
        };
        let mut case = HookCase::open(mode, HookFault::None, true).await;
        let checked = std::panic::AssertUnwindSafe(async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(matches!(
                execution.next().await.unwrap(),
                Some(ExecutionEvent::Partial { .. })
            ));
            let before = case.evidence.names().len();
            match ending {
                "drop" => drop(execution),
                "abort" => {
                    let snapshot = execution.abort().await.unwrap();
                    assert_eq!(snapshot.state.values()["count"], 0);
                }
                _ => {
                    assert!(execution.next().await.unwrap().is_none());
                    execution
                        .finish()
                        .await
                        .expect("ordinary EOF is not a failure");
                }
            }
            let calls = case.evidence.calls();
            let post: Vec<_> = calls
                .iter()
                .filter(|row| row["event"] == "step-finish")
                .collect();
            assert_eq!(post.len(), if ending == "drop" { 1 } else { 2 });
            for row in post {
                assert!(row["outcome"].is_null());
                assert_eq!(row["state"]["count"], 0);
                assert!(row["committed_ordinal"].is_null());
                assert_eq!(row["projection_rejected"], true);
                assert_eq!(row["projection_codec_calls"], 0);
            }
            for row in calls.iter().filter(|row| row["event"] == "stream-finish") {
                assert!(row["failure"].is_null());
                assert_eq!(row["items"], 1);
            }
            let suffix: Vec<_> = case.evidence.names().into_iter().skip(before).collect();
            if ending == "drop" {
                assert_eq!(
                    suffix,
                    [
                        "step-finish:sync",
                        "stream-finish:sync",
                        "execute-finish:sync"
                    ]
                );
            } else {
                assert_eq!(
                    suffix,
                    [
                        "step-finish:sync",
                        "step-finish:async",
                        "stream-finish:sync",
                        "stream-finish:async",
                        "execute-finish:sync",
                        "execute-finish:async"
                    ]
                );
            }
            let snapshot = case.run.snapshot().await.unwrap();
            assert!(!matches!(snapshot.status, RunStatus::Failed(_)));
            assert_eq!(snapshot.state.values()["count"], 0);
            assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        });
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
                .catch_unwind()
                .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn post_hook_errors_retain_exact_structured_primary_cause_at_execution_completion() {
    for ending in [SourceEnding::Error, SourceEnding::Complete] {
        for lane in ["sync", "async"] {
            let mut case = HookCase::open(
                ending,
                HookFault::Error {
                    event: "step-finish",
                    lane,
                },
                false,
            )
            .await;
            let checked = std::panic::AssertUnwindSafe(async {
                let execution = case.run.execution(HookCase::options()).await.unwrap();
                let error = execution
                    .finish()
                    .await
                    .err()
                    .expect("post-hook error surfaces");
                assert!(error.to_string().contains("explicit caller hook context"));
                let completed = matches!(ending, SourceEnding::Complete);
                let snapshot = case.run.snapshot().await.unwrap();
                assert_eq!(snapshot.state.values()["count"], usize::from(completed));
                assert_eq!(snapshot.outcomes.len(), usize::from(completed));
                assert_eq!(snapshot.last_action_ordinal, completed.then_some(0));
                let calls = case.evidence.calls();
                let finished: Vec<_> = calls
                    .iter()
                    .filter(|row| row["event"] == "execute-finish")
                    .collect();
                assert_eq!(
                    finished.len(),
                    2,
                    "explicit finish runs both completion lanes"
                );
                for row in finished {
                    let failure = &row["failure"];
                    assert_eq!(failure["category"], "hook");
                    assert!(
                        failure["message"]
                            .as_str()
                            .unwrap()
                            .contains("explicit caller hook context")
                    );
                    if completed {
                        assert!(
                            failure["cause"].is_null(),
                            "success invents no primary failure"
                        );
                    } else {
                        assert_eq!(failure["cause"]["category"], "action");
                        assert!(
                            !failure["cause"]
                                .to_string()
                                .contains("private action error payload")
                        );
                    }
                }
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
                assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            });
            let checked =
                std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), checked))
                    .catch_unwind()
                    .await;
            case.close(checked).await;
        }
    }
}

#[tokio::test]
async fn native_state_pre_hooks_precede_typing_and_staging_in_both_execution_paths() {
    for background in [false, true] {
        for lane in ["sync", "async"] {
            let mut case = HookCase::native(
                HookFault::Error {
                    event: "step-start",
                    lane,
                },
                background,
            )
            .await;
            let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                if background {
                    let snapshot = case.run.wait().await.unwrap();
                    assert!(matches!(snapshot.status, RunStatus::Failed(WorkflowFailure::Hook { .. })));
                } else {
                    let execution = case.run.execution(ExecutionOptions::default()).await.unwrap();
                    assert!(execution.finish().await.is_err(), "native pre-hook error is functional");
                }
                assert_eq!(case.evidence.typing_pre.load(Ordering::SeqCst), 0,
                    "pre-hook failure must prevent native primitive validation, not merely discard its result");
                let snapshot = case.run.snapshot().await.unwrap();
                assert_eq!(snapshot.state.values()["count"], 0);
                assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
                let names = case.evidence.names();
                assert_eq!(names.iter().filter(|name| name.as_str() == "step-start:sync").count(), 1);
                assert_eq!(names.iter().filter(|name| name.as_str() == "step-start:async").count(), usize::from(lane == "async"));
                assert!(!names.iter().any(|name| name.starts_with("stream-") || name.starts_with("step-finish")));
            })).catch_unwind().await;
            case.close(checked).await;
        }
    }
}

#[tokio::test]
async fn native_state_has_complete_nonstreaming_hooks_in_foreground_and_background() {
    for background in [false, true] {
        let mut case = HookCase::native(HookFault::None, background).await;
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                let snapshot = if background {
                    case.run.wait().await.unwrap()
                } else {
                    case.run
                        .execution(ExecutionOptions::default())
                        .await
                        .unwrap()
                        .finish()
                        .await
                        .unwrap()
                };
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.state.values()["count"], 1);
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(snapshot.outcomes.len(), 1);
                assert_eq!(case.evidence.typing_pre.load(Ordering::SeqCst), 1);
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
                assert_eq!(
                    case.evidence.names(),
                    [
                        "registered:sync",
                        "registered:async",
                        "execute-start:sync",
                        "execute-start:async",
                        "step-start:sync",
                        "step-start:async",
                        "step-finish:sync",
                        "step-finish:async",
                        "execute-finish:sync",
                        "execute-finish:async",
                    ]
                );
                for event in case
                    .evidence
                    .calls()
                    .iter()
                    .filter(|event| event["event"] == "step-start")
                {
                    assert_eq!(event["state"]["count"], 0);
                    assert_eq!(event["inputs"], json!({}));
                    assert_eq!(event["asynchronous"], true);
                    assert_eq!(event["ordinal"], 0);
                }
                for event in case
                    .evidence
                    .calls()
                    .iter()
                    .filter(|event| event["event"] == "step-finish")
                {
                    assert_eq!(event["state"]["count"], 1);
                    assert_eq!(event["outcome"]["kind"], "success");
                    assert_eq!(event["outcome"]["result"], json!({}));
                }
            }))
            .catch_unwind()
            .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn rejected_registration_retains_one_failed_run_and_does_not_replay_on_reopen() {
    for background in [false, true] {
        for lane in ["sync", "async"] {
            for panics in [false, true] {
                let evidence = Arc::new(HookEvidence::default());
                let store =
                    StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                        .await
                        .unwrap();
                let fault = if panics {
                    HookFault::Panic {
                        event: "registered",
                        lane,
                    }
                } else {
                    HookFault::Error {
                        event: "registered",
                        lane,
                    }
                };
                let mut app = HookCase::app(
                    store.clone(),
                    vec![HookCase::native_definition()],
                    &evidence,
                    SourceEnding::Complete,
                    fault,
                    false,
                )
                .await;
                let mut reopened = None;
                let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(
                    Duration::from_secs(6),
                    async {
                        let workspace = app
                            .create_workspace(WorkspaceConfig {
                                name: None,
                                root: std::env::current_dir().unwrap(),
                            })
                            .await
                            .unwrap();
                        let workspace_id = workspace.id().clone();
                        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
                        let admitted = if background {
                            workflow.start(ExecutionOptions::default()).await
                        } else {
                            workflow.prepare(WorkflowState::default()).await
                        };
                        let failure = match admitted {
                            Ok(_) => {
                                panic!("registration failure must reject its admission observation")
                            }
                            Err(error) => error,
                        };
                        if panics {
                            assert!(!failure.to_string().contains("private hook panic payload"));
                        } else {
                            assert!(failure.to_string().contains("explicit caller hook context"));
                        }
                        let runs = workflow
                            .list_runs(None, NonZeroUsize::new(10).unwrap())
                            .await
                            .unwrap();
                        assert_eq!(
                            runs.len(),
                            1,
                            "the proven admission remains discoverable after a hook rejects"
                        );
                        let snapshot = &runs[0];
                        assert_eq!(snapshot.state.values()["count"], 0);
                        assert!(
                            snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none()
                        );
                        if panics {
                            assert!(matches!(
                                &snapshot.status,
                                RunStatus::Failed(WorkflowFailure::Execution(_))
                            ));
                        } else {
                            assert!(matches!(
                                &snapshot.status,
                                RunStatus::Failed(WorkflowFailure::Hook { cause: None, .. })
                            ));
                        }
                        let saved_id = snapshot.id.clone();
                        let calls = evidence.names();
                        assert_eq!(
                            calls,
                            if lane == "sync" {
                                vec!["registered:sync"]
                            } else {
                                vec!["registered:sync", "registered:async"]
                            }
                        );
                        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
                        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
                        app.shutdown(Shutdown::Drain {
                            timeout: Duration::from_secs(3),
                        })
                        .await
                        .unwrap();
                        reopened = Some(
                            HookCase::app(
                                store.clone(),
                                vec![HookCase::native_definition()],
                                &evidence,
                                SourceEnding::Complete,
                                HookFault::None,
                                false,
                            )
                            .await,
                        );
                        let reopened_app = reopened.as_ref().unwrap();
                        {
                            let reopened_workspace =
                                reopened_app.workspace(workspace_id).await.unwrap();
                            let run = reopened_workspace
                                .workflow("hooks".parse().unwrap())
                                .await
                                .unwrap()
                                .open_run(RunSelector::Latest(saved_id))
                                .await
                                .unwrap();
                            let snapshot = run.snapshot().await.unwrap();
                            assert!(matches!(snapshot.status, RunStatus::Failed(_)));
                            assert_eq!(
                                evidence.names(),
                                calls,
                                "reattachment must not replay registration"
                            );
                        }
                    },
                ))
                .catch_unwind()
                .await;
                let reopened_closed = match reopened.as_mut() {
                    Some(reopened) => Some(
                        tokio::time::timeout(
                            Duration::from_secs(5),
                            reopened.shutdown(Shutdown::CancelAndDrain {
                                timeout: Duration::from_secs(3),
                            }),
                        )
                        .await,
                    ),
                    None => None,
                };
                drop(reopened);
                let closed = tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await;
                drop(app);
                match checked {
                    Ok(result) => result.expect("registration failure remains bounded"),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
                if let Some(closed) = reopened_closed {
                    closed.expect("reopened cleanup bounded").unwrap();
                }
                closed.expect("registration cleanup bounded").unwrap();
                assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 2);
            }
        }
    }
}

#[tokio::test]
async fn lost_admission_observer_keeps_registration_owned_before_background_effects() {
    let evidence = Arc::new(HookEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = HookCase::app(
        store,
        vec![HookCase::native_definition()],
        &evidence,
        SourceEnding::Complete,
        HookFault::HoldRegistration,
        false,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
        let mut admission = Box::pin(workflow.start(ExecutionOptions::default()));
        tokio::select! {
            _ = evidence.registration_entered.notified() => {},
            result = &mut admission => panic!("admission returned before held registration: {}", result.is_ok()),
        }
        drop(admission);
        assert_eq!(evidence.names(), ["registered:sync", "registered:async"]);
        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
        let runs = workflow.list_runs(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
        assert_eq!(runs.len(), 1, "registration follows proven, discoverable admission");
        assert_eq!(runs[0].state.values()["count"], 0);
        assert!(runs[0].outcomes.is_empty());
        let mut competing = workflow.open_run(RunSelector::Latest(runs[0].id.clone())).await.unwrap();
        assert!(matches!(competing.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))),
            "visible admission remains exclusively owned until registration settles");
        evidence.registration_release.notify_one();
        let run = workflow.open_run(RunSelector::Latest(runs[0].id.clone())).await.unwrap();
        let snapshot = run.wait().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.names().iter().filter(|name| name.starts_with("registered:")).count(), 2);
    })).catch_unwind().await;
    evidence.registration_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("held registration observation remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("held registration owner cleanup bounded")
        .unwrap();
    assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_map_hooks_relate_each_child_without_imposing_a_sibling_schedule() {
    let case = HookCase::from_definitions(HookCase::map_definitions(), HookFault::None, true).await;
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), async {
            let snapshot = case.run.wait().await.unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["collected"], json!([11, 21]));
            assert_eq!(snapshot.outcomes.len(), 1);
            assert_eq!(snapshot.outcomes[0].node, "map");
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
            let rows = case.evidence.calls();
            assert!(
                !rows
                    .iter()
                    .any(|row| row["event"].as_str().unwrap().starts_with("stream-"))
            );
            let parent_id = serde_json::to_value(case.run.id()).unwrap();
            let parent_rows = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row["run"] == parent_id && row["lane"] == "async")
                .collect::<Vec<_>>();
            assert_eq!(
                parent_rows
                    .iter()
                    .map(|(_, row)| row["event"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                [
                    "registered",
                    "execute-start",
                    "step-start",
                    "step-finish",
                    "execute-finish"
                ]
            );
            let parent_start = parent_rows[2].0;
            let parent_finish = parent_rows[3].0;
            let registrations = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    row["event"] == "registered" && row["lane"] == "async" && row["parent"] == true
                })
                .collect::<Vec<_>>();
            assert_eq!(registrations.len(), 2);
            let mut initial_items = Vec::new();
            for (registered_at, registration) in registrations {
                assert_eq!(registration["parent_run"], parent_id);
                assert_eq!(registration["parent_node"], "map");
                assert!(parent_start < registered_at);
                let child_id = &registration["run"];
                let child_rows = rows
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| &row["run"] == child_id && row["lane"] == "async")
                    .collect::<Vec<_>>();
                assert_eq!(
                    child_rows
                        .iter()
                        .map(|(_, row)| row["event"].as_str().unwrap())
                        .collect::<Vec<_>>(),
                    [
                        "registered",
                        "execute-start",
                        "step-start",
                        "step-finish",
                        "execute-finish",
                        "child-finish"
                    ]
                );
                let initial = child_rows[2].1["state"]["item"].as_i64().unwrap();
                initial_items.push(initial);
                assert_eq!(child_rows[2].1["workflow"], "leaf");
                assert_eq!(child_rows[2].1["inputs"], json!({}));
                assert_eq!(child_rows[2].1["asynchronous"], true);
                assert_eq!(child_rows[2].1["ordinal"], 0);
                assert_eq!(child_rows[3].1["state"]["item"], initial + 1);
                assert_eq!(child_rows[5].1["state"]["item"], initial + 1);
                assert_eq!(child_rows[5].1["parent_run"], parent_id);
                assert_eq!(child_rows[5].1["succeeded"], true);
                assert!(child_rows[5].0 < parent_finish);
                for (_, row) in child_rows {
                    let matching = rows
                        .iter()
                        .filter(|other| {
                            other["event"] == row["event"] && other["run"] == row["run"]
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        matching
                            .iter()
                            .map(|event| event["lane"].as_str().unwrap())
                            .collect::<Vec<_>>(),
                        ["sync", "async"],
                        "each child boundary has exact sync-then-async delivery"
                    );
                }
            }
            initial_items.sort();
            assert_eq!(initial_items, [10, 20]);
        }))
        .catch_unwind()
        .await;
    case.close(checked).await;
}

#[tokio::test]
async fn post_step_error_closes_the_unfinished_stream_in_only_its_sync_drop_lane() {
    for lane in ["sync", "async"] {
        let mut case = HookCase::open(
            SourceEnding::Complete,
            HookFault::Error {
                event: "step-finish",
                lane,
            },
            false,
        )
        .await;
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
                assert!(
                    case.run
                        .execution(HookCase::options())
                        .await
                        .unwrap()
                        .finish()
                        .await
                        .is_err()
                );
                let names = case.evidence.names();
                assert_eq!(
                    names
                        .iter()
                        .filter(|name| name.starts_with("stream-finish:"))
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    ["stream-finish:sync"],
                    "StepGuard failure drops the still-open StreamGuard synchronously"
                );
                assert_eq!(
                    names
                        .iter()
                        .filter(|name| name.starts_with("execute-finish:"))
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    ["execute-finish:sync", "execute-finish:async"]
                );
                let snapshot = case.run.snapshot().await.unwrap();
                assert_eq!(snapshot.state.values()["count"], 1);
                assert_eq!(snapshot.outcomes.len(), 1);
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            }))
            .catch_unwind()
            .await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn child_registration_rejection_settles_the_entire_admitted_group_without_orphans() {
    let case = HookCase::from_definitions(
        HookCase::map_definitions(),
        HookFault::ChildRegistrationError,
        true,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), async {
        assert!(matches!(case.run.wait().await.unwrap().status, RunStatus::Failed(_)));
        let registrations = case.evidence.calls().into_iter().filter(|row| row["event"] == "registered"
            && row["parent"] == true).collect::<Vec<_>>();
        assert!(!registrations.is_empty(), "a real admitted child reached registration");
        let rejected_id = registrations[0]["run"].clone();
        let parent = case.run.snapshot().await.unwrap();
        assert_eq!(parent.state.values()["collected"], json!([]));
        assert!(parent.outcomes.is_empty());
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0,
            "no caller Action exists in this native graph");
        let workspace = case.app.list_workspaces(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
        assert_eq!(workspace.len(), 1);
        let workspace = case.app.workspace(workspace[0].id.clone()).await.unwrap();
        let children = workspace.workflow("leaf".parse().unwrap()).await.unwrap()
            .list_runs(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
        assert_eq!(children.len(), 2, "both structurally admitted child identities remain discoverable");
        assert_eq!(children.iter().filter(|child| serde_json::to_value(&child.id).unwrap() == rejected_id).count(), 1,
            "the rejected Registered.run must name exactly one actual admitted child");
        for child in &children {
            assert!(matches!(child.status, RunStatus::Succeeded | RunStatus::Failed(_) | RunStatus::Cancelled),
                "registration rejection must settle later admitted siblings, not strand their capacity");
            if serde_json::to_value(&child.id).unwrap() == rejected_id {
                assert!(matches!(&child.status, RunStatus::Failed(WorkflowFailure::Hook { .. })));
                assert!(child.outcomes.is_empty() && child.last_action_ordinal.is_none());
                assert!([json!(10), json!(20)].contains(&child.state.values()["item"]));
            }
        }
        assert!(!case.evidence.calls().iter().any(|row| row["run"] == rejected_id && row["event"] == "step-start"));
    })).catch_unwind().await;
    case.close(checked).await;
}

#[tokio::test]
async fn sibling_source_and_hook_contexts_share_the_immutable_wave_checkpoint() {
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"collect"},"entry":"fork",
            "nodes":[{"id":"fork","step":{"kind":"state","mutations":[]}},
                {"id":"a","step":{"kind":"action","action":"local","revision":"r1"}},
                {"id":"b","step":{"kind":"action","action":"local","revision":"r1"}}],
            "routes":{"fork":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"b"}},{"target":{"kind":"node","id":"a"}}
            ]}}
        })
        .to_string(),
    )
    .unwrap();
    let mut case = HookCase::from_definitions(vec![definition], HookFault::None, false).await;
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            let base = match execution.step().await.unwrap() {
                Some(ExecutionEvent::Committed { checkpoint, .. }) => checkpoint,
                _ => panic!("the fork supplies the immutable base for both siblings"),
            };
            let snapshot = execution.finish().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], json!([1, 1]));
            let contexts = case.evidence.source_contexts.lock().unwrap().clone();
            assert_eq!(
                contexts,
                [
                    json!({"node":"a","ordinal":1,"checkpoint":base}),
                    json!({"node":"b","ordinal":2,"checkpoint":base})
                ],
                "control/staging progress cannot replace an activation's immutable base checkpoint"
            );
            for row in case.evidence.calls().iter().filter(|row| {
                row["event"] == "step-start" && [json!("a"), json!("b")].contains(&row["node"])
            }) {
                assert_eq!(row["checkpoint"], base);
            }
            assert_eq!(
                case.evidence
                    .calls()
                    .iter()
                    .filter(|row| row["event"] == "step-start"
                        && [json!("a"), json!("b")].contains(&row["node"]))
                    .count(),
                4
            );
        }))
        .catch_unwind()
        .await;
    case.close(checked).await;
}

#[tokio::test]
async fn earlier_native_staged_failure_remains_primary_when_later_item_or_span_hook_fails() {
    for (ending, fault, event) in [
        (
            SourceEnding::Complete,
            HookFault::HoldItem { fail: true },
            "stream-item",
        ),
        (
            SourceEnding::Spans(SpanProgram::OpenComplete),
            HookFault::HoldAttribute { fail: true },
            "span-attribute",
        ),
    ] {
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
                "state":{"count":0,"native":"not an integer"},
                "required":["count","native"],
                "reducers":{"count":"reject_concurrent","native":"reject_concurrent"},
                "entry":"fork","nodes":[
                    {"id":"fork","step":{"kind":"state","mutations":[]}},
                    {"id":"a_native","step":{"kind":"state","mutations":[
                        {"kind":"increment","target":"native","value":{"kind":"literal","value":1}}
                    ]}},
                    {"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}
                ],"routes":{"fork":{"mode":"broadcast","branches":[
                    {"target":{"kind":"node","id":"work"}},
                    {"target":{"kind":"node","id":"a_native"}}
                ]}}
            })
            .to_string(),
        )
        .unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut case =
            HookCase::from_source(store.clone(), vec![definition], ending, fault, false).await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
            let workspaces = case.app.list_workspaces(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
            assert_eq!(workspaces.len(), 1);
            let workspace = case.app.workspace(workspaces[0].id.clone()).await.unwrap();
            let observer = workspace.workflow("hooks".parse().unwrap()).await.unwrap()
                .open_run(RunSelector::Latest(case.run.id().clone())).await.unwrap();
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            let base = match execution.step().await.unwrap() {
                Some(ExecutionEvent::Committed { checkpoint, outcomes }) => {
                    assert_eq!(outcomes.len(), 1);
                    assert_eq!(outcomes[0].node, "fork");
                    checkpoint
                }
                _ => panic!("the fork must commit before the failing sibling wave"),
            };
            let mut next = Box::pin(execution.next());
            tokio::select! {
                _ = case.evidence.post_entered.notified() => {}
                result = &mut next => panic!("later {event} callback never reached its held rejection: {}", result.is_ok()),
            }
            drop(next);
            let held = observer.snapshot().await.unwrap();
            assert_eq!(held.state.values(), &Map::from_iter([
                ("count".into(), json!(0)), ("native".into(), json!("not an integer")),
            ]));
            assert_eq!(held.last_action_ordinal, Some(0));
            let records = store.list(StateListRequest::new("app-sdk").unwrap().with_limit(100).unwrap()).await.unwrap();
            assert!(records.next_start_after.is_none());
            let current = records.records.iter().filter(|row| {
                row.record.key.key().starts_with("history/")
                    && row.record.value["id"] == json!(observer.id())
                    && row.record.value["checkpoint"]["sequence"] == held.checkpoint
            }).collect::<Vec<_>>();
            assert_eq!(current.len(), 1, "observe the exact durable row while the later hook is held");
            let checkpoint = &current[0].record.value["checkpoint"];
            let mut frontier = checkpoint["frontier"].as_array().unwrap().iter()
                .map(|activation| activation["node"].as_str().unwrap()).collect::<Vec<_>>();
            frontier.sort();
            assert_eq!(frontier, ["a_native", "work"], "both ready siblings belong to the same wave");
            let staged = checkpoint["staged"].as_array().unwrap();
            assert_eq!(staged.len(), 1);
            assert_eq!(staged[0]["node"], "a_native");
            assert_eq!(staged[0]["outcome"]["state"], "failed");
            assert_eq!(staged[0]["outcome"]["category"], "state");
            let primary_message = staged[0]["outcome"]["message"].as_str().unwrap().to_owned();
            assert_eq!(case.evidence.typing_pre.load(Ordering::SeqCst), 1,
                "the native state operation actually attempted its invalid increment");
            let calls = case.evidence.calls();
            for node in ["a_native", "work"] {
                let starts = calls.iter().filter(|row| row["event"] == "step-start" && row["node"] == node).collect::<Vec<_>>();
                assert_eq!(starts.len(), 2);
                assert!(starts.iter().all(|row| row["checkpoint"] == base && row["ordinal"] == 1));
            }
            assert_eq!(calls.iter().filter(|row| row["event"] == event && row["lane"] == "async").count(), 1);
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 0);
            case.evidence.post_release.notify_one();
            let returned = execution.finish().await;
            assert!(matches!(returned, Err(AppError::Integration(ref message)) if message.contains("explicit caller hook context")),
                "the later callback really returned its selected Hook error");
            let snapshot = observer.snapshot().await.unwrap();
            assert!(matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::State(message)) if message == &primary_message),
                "the earlier canonical native failure must stay authoritative");
            assert_eq!(snapshot.state.values(), held.state.values());
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert!(snapshot.outcomes == held.outcomes);
            let failed = observer.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
            assert!(matches!(failed.kind, CheckpointKind::Failed { ref node, ordinal: 1 } if node == "a_native"),
                "equal attempted ordinals cannot move attribution to the later caller node");
            let calls = case.evidence.calls();
            let completions = calls.iter().filter(|row| row["event"] == "execute-finish").collect::<Vec<_>>();
            assert_eq!(completions.len(), 2);
            for completion in completions {
                assert_eq!(completion["failure"]["category"], "hook");
                assert_eq!(completion["failure"]["cause"]["category"], "state");
                assert_eq!(completion["failure"]["cause"]["message"], primary_message);
            }
            let native_ends = calls.iter().filter(|row| row["event"] == "step-finish" && row["node"] == "a_native").collect::<Vec<_>>();
            assert!(!native_ends.is_empty());
            assert!(native_ends.iter().all(|row| row["outcome"]["failure"]["category"] == "state"));
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        })).catch_unwind().await;
        case.evidence.post_release.notify_one();
        case.close(checked).await;
    }
}

#[tokio::test]
async fn earlier_native_prefix_failure_remains_primary_when_later_native_pre_hook_fails() {
    let mut primary_message = None;
    for rejected_lane in [None, Some("sync"), Some("async")] {
        let definition = HookCase::native_prefix_definition();
        let fault =
            rejected_lane.map_or(HookFault::None, |lane| HookFault::NativePreError { lane });
        let mut case = HookCase::from_definitions(vec![definition], fault, false).await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            let (base, base_outcomes) = match execution.step().await.unwrap() {
                Some(ExecutionEvent::Committed { checkpoint, outcomes }) => {
                    assert_eq!(outcomes.len(), 1);
                    assert_eq!(outcomes[0].node, "fork");
                    assert_eq!(outcomes[0].ordinal, 0);
                    (checkpoint, outcomes)
                }
                _ => panic!("the fork must commit before either native sibling starts"),
            };
            let returned = execution.finish().await;
            if rejected_lane.is_some() {
                assert!(matches!(returned, Err(AppError::Integration(ref message))
                    if message.contains("explicit caller hook context")),
                    "the later native pre-hook must return its selected error");
            } else {
                returned.expect("the control observes an ordinary durable State failure");
            }

            // These public observations must establish both the actual earlier
            // mutation and the later callback before testing failure attribution.
            assert_eq!(case.evidence.typing_pre.load(Ordering::SeqCst),
                if rejected_lane.is_some() { 1 } else { 2 },
                "a_native really attempts the invalid increment; rejected b_native never mutates");
            let calls = case.evidence.calls();
            let earlier_starts = calls.iter().filter(|row|
                row["event"] == "step-start" && row["node"] == "a_native"
            ).collect::<Vec<_>>();
            assert_eq!(earlier_starts.len(), 2);
            assert_eq!(earlier_starts[0]["lane"], "sync");
            assert_eq!(earlier_starts[1]["lane"], "async");
            let later_starts = calls.iter().filter(|row|
                row["event"] == "step-start" && row["node"] == "b_native"
            ).collect::<Vec<_>>();
            assert_eq!(later_starts.len(), if rejected_lane == Some("sync") { 1 } else { 2 });
            assert_eq!(later_starts[0]["lane"], "sync");
            if rejected_lane != Some("sync") {
                assert_eq!(later_starts[1]["lane"], "async");
            }
            let initial = Map::from_iter([
                ("count".into(), json!(0)), ("native".into(), json!("not an integer")),
            ]);
            for start in earlier_starts.iter().chain(&later_starts) {
                assert_eq!(start["checkpoint"], base);
                assert_eq!(start["ordinal"], 1,
                    "a failed sibling does not consume a successful action ordinal");
                assert_eq!(start["state"], json!(initial));
            }
            let earlier_end = calls.iter().position(|row|
                row["event"] == "step-start" && row["node"] == "a_native" && row["lane"] == "async"
            ).unwrap();
            let later_begin = calls.iter().position(|row|
                row["event"] == "step-start" && row["node"] == "b_native"
            ).unwrap();
            assert!(earlier_end < later_begin);
            assert_eq!(case.evidence.rejected.load(Ordering::SeqCst), rejected_lane.is_some());
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 0);
            assert!(!calls.iter().any(|row| row["event"].as_str().unwrap().starts_with("stream-")));

            let snapshot = case.run.snapshot().await.unwrap();
            let RunStatus::Failed(WorkflowFailure::State(message)) = &snapshot.status else {
                panic!("the earlier native State failure must remain primary after {rejected_lane:?} rejection");
            };
            match &primary_message {
                Some(primary) => assert_eq!(message, primary,
                    "the later pre-hook cannot replace the original State message"),
                None => primary_message = Some(message.clone()),
            }
            assert_eq!(snapshot.state.values(), &initial);
            assert_eq!(snapshot.last_action_ordinal, Some(0));
            assert!(snapshot.outcomes == base_outcomes);
            let failed = case.run.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
            assert!(matches!(failed.kind, CheckpointKind::Failed { ref node, ordinal: 1 } if node == "a_native"),
                "equal proposed ordinals cannot move failure history to b_native");
            let completions = calls.iter().filter(|row| row["event"] == "execute-finish").collect::<Vec<_>>();
            assert_eq!(completions.len(), 2);
            for completion in completions {
                let failure = &completion["failure"];
                if rejected_lane.is_some() {
                    assert_eq!(failure["category"], "hook");
                    assert!(failure["message"].as_str().unwrap().contains("explicit caller hook context"));
                    assert_eq!(failure["cause"]["category"], "state");
                    assert_eq!(failure["cause"]["message"], message.as_str());
                } else {
                    assert_eq!(failure["category"], "state");
                    assert_eq!(failure["message"], message.as_str());
                }
            }
            let earlier_finishes = calls.iter().filter(|row|
                row["event"] == "step-finish" && row["node"] == "a_native"
            ).collect::<Vec<_>>();
            assert_eq!(earlier_finishes.len(), if rejected_lane.is_some() { 1 } else { 2 });
            assert_eq!(earlier_finishes[0]["lane"], "sync");
            if rejected_lane.is_none() {
                assert_eq!(earlier_finishes[1]["lane"], "async");
            }
            for finished in earlier_finishes {
                assert_eq!(finished["outcome"]["failure"]["category"], "state");
                assert_eq!(finished["outcome"]["failure"]["message"], message.as_str());
            }
            if rejected_lane.is_some() {
                assert!(!calls.iter().any(|row|
                    row["event"] == "step-finish" && row["node"] == "b_native"
                ), "a rejected pre-hook never opened b_native's terminal guard");
            }
        })).catch_unwind().await;
        case.close(checked).await;
    }
}

#[tokio::test]
async fn held_registration_obeys_its_single_thirty_second_observation_ceiling() {
    let evidence = Arc::new(HookEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = HookCase::app(
        store,
        vec![HookCase::native_definition()],
        &evidence,
        SourceEnding::Complete,
        HookFault::HoldRegistration,
        false,
    )
    .await;
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(35), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let started = tokio::time::Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(33),
                workflow.start(ExecutionOptions::default()),
            )
            .await;
            assert!(
                matches!(result, Ok(Err(AppError::Timeout(_)))),
                "held Registered must terminate through its own observation ceiling"
            );
            assert!(started.elapsed() >= Duration::from_secs(29));
            assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
            let runs = workflow
                .list_runs(None, NonZeroUsize::new(10).unwrap())
                .await
                .unwrap();
            assert_eq!(runs.len(), 1);
            assert!(matches!(
                runs[0].status,
                RunStatus::Failed(WorkflowFailure::Timeout(_))
            ));
            assert!(runs[0].outcomes.is_empty() && runs[0].last_action_ordinal.is_none());
        }))
        .catch_unwind()
        .await;
    evidence.registration_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("registration ceiling proof bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed.expect("registration timeout cleanup joins").unwrap();
}

#[tokio::test]
async fn post_step_stream_and_execute_share_one_cleanup_observation_budget() {
    let mut case = HookCase::open(
        SourceEnding::Complete,
        HookFault::SharedCleanupBudget,
        false,
    )
    .await;
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(35), async {
            let execution = case.run.execution(HookCase::options()).await.unwrap();
            let started = tokio::time::Instant::now();
            let result = tokio::time::timeout(Duration::from_secs(33), execution.finish()).await;
            assert!(
                matches!(result, Ok(Err(AppError::Timeout(_)))),
                "post-stream cannot start a fresh30s budget after post-step spent20s"
            );
            assert!(started.elapsed() >= Duration::from_secs(29));
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
            let snapshot = case.run.snapshot().await.unwrap();
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.outcomes.len(), 1);
            assert_eq!(
                case.evidence
                    .names()
                    .iter()
                    .filter(|name| name.as_str() == "execute-finish:async")
                    .count(),
                0,
                "expired cleanup may close synchronously but cannot admit another asynchronous lane"
            );
        }))
        .catch_unwind()
        .await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn terminal_execute_finish_failure_survives_all_handle_drops_and_supervision() {
    for background in [false, true] {
        HookCase::terminal_completion_failure(background).await;
    }
}

#[tokio::test]
async fn post_commit_failure_reaches_waiters_until_successful_reacquisition_and_is_transient_on_reopen()
 {
    for background in [false, true] {
        for reopen in [false, true] {
            let evidence = Arc::new(HookEvidence::default());
            let store =
                StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
                    .await
                    .unwrap();
            let mut app = HookCase::app(
                store.clone(),
                vec![HookCase::native_definition()],
                &evidence,
                SourceEnding::Complete,
                HookFault::HoldPost { fail: true },
                false,
            )
            .await;
            let mut fresh = None;
            let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
                let workspace = app.create_workspace(WorkspaceConfig { name: None,
                    root: std::env::current_dir().unwrap() }).await.unwrap();
                let workspace_id = workspace.id().clone();
                let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
                let mut run = if background { workflow.start(ExecutionOptions::default()).await.unwrap() }
                    else { workflow.prepare(WorkflowState::default()).await.unwrap() };
                let run_id = run.id().clone();
                let observer = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
                let mut competing = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
                let mut running = Box::pin(async {
                    if background { run.wait().await }
                    else { run.execution(ExecutionOptions::default()).await.unwrap().finish().await }
                });
                tokio::select! {
                    _ = evidence.post_entered.notified() => {},
                    result = &mut running => panic!("execution returned before its held post-hook: {}", result.is_ok()),
                }
                let committed = observer.snapshot().await.unwrap();
                assert_eq!(committed.state.values()["count"], 1);
                assert_eq!(committed.outcomes.len(), 1);
                let mut early_waiter = Box::pin(observer.wait());
                assert!(tokio::time::timeout(Duration::from_millis(25), &mut early_waiter).await.is_err(),
                    "a waiter must rendezvous with the still-owned post-hook, not return a premature public boundary");
                assert!(matches!(competing.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))));
                evidence.post_release.notify_one();
                assert!(running.await.is_err());
                let failure = early_waiter.await.err().expect("early waiter receives completed hook failure");
                assert!(failure.to_string().contains("explicit caller hook context"));
                for _ in 0..2 {
                    assert!(observer.wait().await.err().unwrap().to_string().contains("explicit caller hook context"));
                }
                let rejected = ExecutionOptions { state: WorkflowState::new(Map::from_iter([
                    ("count".into(), json!(99)),
                ])).unwrap(), ..ExecutionOptions::default() };
                assert!(matches!(competing.execution(rejected).await, Err(AppError::InvalidInput(_))));
                assert!(observer.wait().await.is_err(), "failed acquisition cannot clear a completed execution failure");
                let parked = observer.snapshot().await.unwrap();
                assert!(matches!(parked.status, RunStatus::Paused));
                assert_eq!(parked.state.values()["count"], 1);
                assert_eq!(parked.last_action_ordinal, Some(0));
                assert!(parked.outcomes == committed.outcomes);
                let completions = evidence.calls().into_iter().filter(|row| row["event"] == "execute-finish").collect::<Vec<_>>();
                assert_eq!(completions.len(), 2);
                for completion in completions {
                    assert_eq!(completion["failure"]["category"], "hook");
                    assert!(completion["failure"]["cause"].is_null());
                }
                // Completion errors remain authoritative in this App even
                // when no public run observer retains the original cell.
                drop(run);
                drop(observer);
                drop(competing);
                let observer = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
                let mut competing = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
                for _ in 0..2 {
                    assert!(observer.wait().await.err().unwrap().to_string().contains("explicit caller hook context"),
                        "dropping every observer must not discard the completed hook error");
                }
                let rejected = ExecutionOptions { state: WorkflowState::new(Map::from_iter([
                    ("count".into(), json!(99)),
                ])).unwrap(), ..ExecutionOptions::default() };
                assert!(matches!(competing.execution(rejected).await, Err(AppError::InvalidInput(_))));
                assert!(observer.wait().await.err().unwrap().to_string().contains("explicit caller hook context"),
                    "rejected acquisition after reopening a handle preserves the original failure");
                if reopen {
                    fresh = Some(HookCase::app(store.clone(), vec![HookCase::native_definition()], &evidence,
                        SourceEnding::Complete, HookFault::None, false).await);
                    let fresh_workspace = fresh.as_ref().unwrap().workspace(workspace_id).await.unwrap();
                    let fresh_workflow = fresh_workspace.workflow("hooks".parse().unwrap()).await.unwrap();
                    let mut resumed = fresh_workflow.open_run(RunSelector::Latest(run_id)).await.unwrap();
                    assert!(matches!(resumed.wait().await.unwrap().status, RunStatus::Paused),
                        "reopen retains the parked boundary without reconstructing transient hook failure");
                    let result = resumed.execution(ExecutionOptions::default()).await.unwrap().finish().await.unwrap();
                    assert!(matches!(result.status, RunStatus::Succeeded));
                    assert_eq!(result.state.values()["count"], 1);
                } else {
                    let resumed = competing.execution(ExecutionOptions::default()).await.unwrap();
                    assert!(matches!(observer.wait().await.unwrap().status, RunStatus::Paused),
                        "only successful acquisition consumes the prior transient completion");
                    let result = resumed.finish().await.unwrap();
                    assert!(matches!(result.status, RunStatus::Succeeded));
                    assert_eq!(result.state.values()["count"], 1);
                }
                assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1, "post-hook failure never replays committed native work");
            })).catch_unwind().await;
            evidence.post_release.notify_one();
            let superseded = fresh.is_some();
            let fresh_closed = match fresh.as_mut() {
                Some(app) => Some(
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        app.shutdown(Shutdown::CancelAndDrain {
                            timeout: Duration::from_secs(3),
                        }),
                    )
                    .await,
                ),
                None => None,
            };
            drop(fresh);
            let closed = tokio::time::timeout(
                Duration::from_secs(5),
                app.shutdown(Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(3),
                }),
            )
            .await;
            drop(app);
            match checked {
                Ok(result) => result.expect("post-hook completion observation remains bounded"),
                Err(panic) => std::panic::resume_unwind(panic),
            }
            if let Some(closed) = fresh_closed {
                closed.expect("fresh App cleanup joins").unwrap();
            }
            match closed.expect("original App cleanup joins") {
                Ok(()) => {}
                Err(AppError::Closed(_)) if superseded => {}
                Err(error) => panic!("original cleanup failed: {error}"),
            }
            assert_eq!(
                evidence.owner_dropped.load(Ordering::SeqCst),
                if reopen { 2 } else { 1 }
            );
        }
    }
}

#[tokio::test]
async fn applied_admission_receipt_retains_exclusive_registration_ownership() {
    let evidence = Arc::new(HookEvidence::default());
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = App::open(HookCase::config(
        HookAdmissionStore {
            inner,
            evidence: Arc::clone(&evidence),
        },
        vec![HookCase::native_definition()],
        &evidence,
        SourceEnding::Complete,
        HookFault::None,
        false,
    ))
    .await
    .unwrap();
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(6),
        async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            evidence.admission_armed.store(true, Ordering::SeqCst);
            let mut admission = Box::pin(workflow.start(ExecutionOptions::default()));
            tokio::select! {
                _ = evidence.admission_entered.notified() => {},
                result = &mut admission => panic!("admission returned before its held native receipt: {}", result.is_ok()),
            }
            assert_eq!(evidence.admitted_commits.load(Ordering::SeqCst), 1);
            assert!(evidence.calls().is_empty(), "Registered follows the proven admission receipt");
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
            let runs = workflow.list_runs(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
            assert_eq!(runs.len(), 1, "the native commit made this exact admission discoverable");
            assert_eq!(runs[0].checkpoint, 0);
            assert_eq!(runs[0].state.values()["count"], 0);
            assert!(runs[0].outcomes.is_empty());
            let mut competing = workflow.open_run(RunSelector::Latest(runs[0].id.clone())).await.unwrap();
            assert!(matches!(competing.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))),
                "an applied admission remains exclusively owned before its receipt and Registered callback");
            evidence.admission_release.notify_one();
            let admitted = admission.await.unwrap();
            assert_eq!(admitted.id(), &runs[0].id);
            let snapshot = admitted.wait().await.unwrap();
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.names().iter().filter(|name| name.starts_with("registered:")).count(), 2);
        },
    )).catch_unwind().await;
    evidence.admission_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("admission receipt observation remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("accepted admission owner joins during cleanup")
        .unwrap();
    assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
}

impl HookCase {
    // Explicit App-native control lifetime; this is not a pinned rurr Input oracle.
    async fn control_lifecycle(dropped: bool, cleanup_fault: Option<CleanupReadFault>) {
        let evidence = Arc::new(HookEvidence::default());
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
                "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
                "entry":"ask","nodes":[{"id":"ask","step":{"kind":"input",
                    "message":[{"kind":"text","text":"Provide the count"}],
                    "target":"count","schema":{"type":"integer"}}}],"routes":{}
            })
            .to_string(),
        )
        .unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: store.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![definition],
            &evidence,
            SourceEnding::Complete,
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let held = matches!(cleanup_fault, Some(CleanupReadFault::Held));
        let fatal = matches!(
            cleanup_fault,
            Some(CleanupReadFault::Permanent | CleanupReadFault::Held)
        );
        let checked =
            std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(if held { 36 } else { 8 }), async {
                let workspace = app
                    .create_workspace(WorkspaceConfig {
                        name: None,
                        root: std::env::current_dir().unwrap(),
                    })
                    .await
                    .unwrap();
                let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
                let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
                let id = run.id().clone();
                let observer = workflow.open_run(RunSelector::Latest(id.clone())).await.unwrap();
                let occurrence = run.snapshot().await.unwrap().occurrence;
                let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), id, occurrence)).unwrap();
                let mut original = None;
                let mut execution = run.execution(ExecutionOptions::default()).await.unwrap();
                let waiting = if dropped {
                    assert!(execution.next().await.unwrap().is_none());
                    let waiting = observer.snapshot().await.unwrap();
                    if let Some(fault) = cleanup_fault {
                        original = store.load(key.clone()).await.unwrap();
                        assert!(original.is_some());
                        *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), fault));
                    }
                    drop(execution);
                    waiting
                } else {
                    execution.finish().await.unwrap()
                };
                let [Interaction::Input { sequence, generation, .. }] = waiting.interactions.as_slice() else {
                    panic!("the native activation must retain its actual input control");
                };
                assert_eq!(waiting.state.values()["count"], 0);
                assert!(waiting.last_action_ordinal.is_none() && waiting.outcomes.is_empty());
                let mut expected = vec![
                    "registered:sync",
                    "registered:async",
                    "execute-start:sync",
                    "execute-start:async",
                    "step-start:sync",
                    "step-start:async",
                ];
                if dropped {
                    expected.push("step-finish:sync");
                }
                expected.push("execute-finish:sync");
                if !dropped {
                    expected.push("execute-finish:async");
                }
                assert_eq!(
                    evidence.names(),
                    expected,
                    "an actionable native control separates execution and activation lifetimes"
                );
                if dropped {
                    let closed = evidence
                        .calls()
                        .into_iter()
                        .find(|row| row["event"] == "step-finish")
                        .unwrap();
                    assert!(closed["outcome"].is_null());
                    assert_eq!(closed["state"]["count"], 0);
                    assert!(closed["committed_ordinal"].is_null());
                }
                if cleanup_fault.is_some() {
                    if held {
                        evidence.post_entered.notified().await;
                        assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 0);
                        assert_eq!(store.load(key.clone()).await.unwrap(), original);
                    }
                    if fatal {
                        tokio::time::timeout(Duration::from_secs(if held { 33 } else { 2 }), async {
                            loop {
                                match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                                    Err(AppError::Closed(_)) => break,
                                    Ok(_) => tokio::task::yield_now().await,
                                    Err(error) => panic!("unexpected App closure observation: {error}"),
                                }
                            }
                        }).await.expect("completed-guard cleanup cannot retain unreadable execution authority");
                        assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);
                        assert_eq!(store.load(key.clone()).await.unwrap(), original,
                            "cleanup cannot fabricate a changed Input checkpoint");
                        assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), usize::from(held),
                            "the original held read is destroyed before fatal publication");
                        assert_eq!(evidence.names(), expected,
                            "completed synchronous guards never open an asynchronous lane on read failure");
                        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
                        return;
                    }
                    tokio::time::timeout(Duration::from_secs(2), async {
                        while evidence.cleanup_read_faults.load(Ordering::SeqCst) == 0 {
                            tokio::task::yield_now().await;
                        }
                    }).await.expect("the retained cleanup owner encounters its recoverable outage");
                    assert_eq!(evidence.names(), expected);
                }
                let resolved = observer
                    .resolve(*sequence, *generation, Resolution::Input(json!(1)))
                    .await
                    .unwrap();
                assert_eq!(
                    resolved.state.values()["count"],
                    0,
                    "control settlement stages its value until explicit advancement"
                );
                assert!(resolved.outcomes.is_empty());
                let snapshot = loop {
                    let mut candidate = workflow.open_run(RunSelector::Latest(id.clone())).await.unwrap();
                    let attempt = candidate.execution(ExecutionOptions::default()).await;
                    match attempt {
                        Ok(execution) => break execution.finish().await.unwrap(),
                        Err(AppError::Busy(_)) => {
                            assert_eq!(
                                evidence.names(),
                                expected,
                                "a rejected acquisition cannot open another lifecycle session"
                            );
                            tokio::task::yield_now().await;
                        }
                        Err(error) => panic!("explicit native continuation failed: {error}"),
                    }
                };
                assert!(matches!(snapshot.status, RunStatus::Succeeded));
                assert_eq!(snapshot.state.values()["count"], 1);
                assert_eq!(snapshot.last_action_ordinal, Some(0));
                assert_eq!(snapshot.outcomes.len(), 1);
                assert_eq!(
                    snapshot.outcomes[0].result,
                    Map::from_iter([("value".into(), json!(1))])
                );
                expected.extend(["execute-start:sync", "execute-start:async"]);
                if !dropped {
                    expected.extend(["step-finish:sync", "step-finish:async"]);
                }
                expected.extend(["execute-finish:sync", "execute-finish:async"]);
                assert_eq!(
                    evidence.names(),
                    expected,
                    "the same native control never reopens an already-started or dropped guard"
                );
                let rows = evidence.calls();
                for row in rows.iter().filter(|row| {
                    matches!(row["event"].as_str(), Some("step-start" | "step-finish"))
                }) {
                    assert_eq!(row["node"], "ask");
                    assert_eq!(row["ordinal"], 0);
                    assert_eq!(row["checkpoint"], 0);
                }
                if !dropped {
                    for row in rows.iter().filter(|row| row["event"] == "step-finish") {
                        assert_eq!(row["outcome"]["kind"], "success");
                        assert_eq!(row["outcome"]["result"], json!({"value":1}));
                        assert_eq!(row["state"]["count"], 1);
                        assert_eq!(row["committed_ordinal"], 0);
                    }
                }
                assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            }))
            .catch_unwind()
            .await;
        let calls = evidence.names();
        evidence.cleanup_read.lock().unwrap().take();
        evidence.post_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("native control lifecycle remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed.expect("native control owner cleanup joins");
        if fatal {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        } else {
            closed.unwrap();
        }
        assert_eq!(
            evidence.names(),
            calls,
            "cleanup does not reopen a closed activation guard"
        );
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_control_observation_survives_successful_execution_finish_until_resolution() {
    HookCase::control_lifecycle(false, None).await;
}

#[tokio::test]
async fn dropped_native_control_observation_closes_synchronously_without_reopening_on_resolution() {
    HookCase::control_lifecycle(true, None).await;
}

#[tokio::test]
async fn owner_drop_retains_an_admitted_item_hook_and_its_failure_before_sync_guards() {
    let mut case = HookCase::open(
        SourceEnding::Pending,
        HookFault::HoldItem { fail: true },
        false,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = case.evidence.post_entered.notified() => {},
            result = &mut next => panic!("item observation returned before its held callback: {}", result.is_ok()),
        }
        drop(next);
        drop(execution);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        assert!(!case.evidence.names().iter().any(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")),
            "Drop cannot close guards before the admitted item hook settles");
        case.evidence.post_release.notify_one();
        let snapshot = case.run.wait().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Failed(WorkflowFailure::Hook { .. })),
            "the callback failure survives loss of its receiver");
        assert_eq!(snapshot.state.values()["count"], 0);
        assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        let suffix = case.evidence.names().into_iter().filter(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")).collect::<Vec<_>>();
        assert_eq!(suffix, ["step-finish:sync", "stream-finish:sync", "execute-finish:sync"],
            "the retained task closes dropped-owner guards without starting asynchronous lanes");
        let calls = case.evidence.calls();
        let step = calls.iter().find(|row| row["event"] == "step-finish").unwrap();
        assert_eq!(step["outcome"]["kind"], "failure");
        let stream = calls.iter().find(|row| row["event"] == "stream-finish").unwrap();
        assert_eq!(stream["failure"]["category"], "hook");
        assert_eq!(stream["items"], 0);
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn owner_drop_after_native_barrier_keeps_guards_with_the_real_receipt() {
    let evidence = Arc::new(HookEvidence::default());
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let definition = WorkflowDefinition::parse(&json!({
        "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1", "state":{"count":0},
        "required":["count"],"reducers":{"count":"reject_concurrent"},"entry":"work",
        "nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],"routes":{}
    }).to_string()).unwrap();
    let mut app = App::open(HookCase::config(
        HookAdmissionStore {
            inner,
            evidence: Arc::clone(&evidence),
        },
        vec![definition],
        &evidence,
        SourceEnding::Complete,
        HookFault::None,
        false,
    ))
    .await
    .unwrap();
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let workspace = app.create_workspace(WorkspaceConfig { name: None,
            root: std::env::current_dir().unwrap() }).await.unwrap();
        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut competing = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut execution = run.execution(HookCase::options()).await.unwrap();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
        *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::Barrier);
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = evidence.barrier_entered.notified() => {},
            result = &mut next => panic!("barrier returned before its held native receipt: {}", result.is_ok()),
        }
        assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
        let committed = observer.snapshot().await.unwrap();
        assert_eq!(committed.state.values()["count"], 1);
        assert_eq!(committed.outcomes.len(), 1);
        drop(next);
        drop(execution);
        assert!(!evidence.names().iter().any(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")),
            "unobserved native commit receipt retains its guards after owner Drop");
        assert!(matches!(competing.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))));
        evidence.barrier_release.notify_one();
        let snapshot = observer.wait().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Paused));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert!(snapshot.outcomes == committed.outcomes);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        let suffix = evidence.names().into_iter().filter(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")).collect::<Vec<_>>();
        assert_eq!(suffix, ["step-finish:sync", "stream-finish:sync", "execute-finish:sync"]);
        let calls = evidence.calls();
        let step = calls.iter().find(|row| row["event"] == "step-finish").unwrap();
        assert_eq!(step["outcome"]["kind"], "success");
        assert_eq!(step["state"]["count"], 1);
        assert_eq!(step["committed_ordinal"], 0);
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    evidence.barrier_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("held barrier lifecycle observation is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed.expect("held barrier publication joins").unwrap();
    assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn returned_committed_boundary_does_not_charge_caller_idle_time_to_cleanup() {
    let mut case = HookCase::open(SourceEnding::Complete, HookFault::None, false).await;
    let checked =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(38), async {
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(matches!(
                execution.step().await.unwrap(),
                Some(ExecutionEvent::Committed { .. })
            ));
            let names = case.evidence.names();
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "step-finish:async")
                    .count(),
                1
            );
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "stream-finish:async")
                    .count(),
                1
            );
            assert!(!names.iter().any(|name| name.starts_with("execute-finish")));
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            tokio::time::sleep(Duration::from_secs(31)).await;
            let snapshot = execution
                .finish()
                .await
                .expect("a returned post-action chain does not time out caller idle time");
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert_eq!(snapshot.state.values()["count"], 1);
            assert_eq!(snapshot.outcomes.len(), 1);
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            let names = case.evidence.names();
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "execute-finish:sync")
                    .count(),
                1
            );
            assert_eq!(
                names
                    .iter()
                    .filter(|name| name.as_str() == "execute-finish:async")
                    .count(),
                1
            );
        }))
        .catch_unwind()
        .await;
    case.close(checked).await;
}

#[tokio::test]
async fn held_successful_item_keeps_source_destructor_failure_after_owner_drop() {
    let mut case = HookCase::open(
        SourceEnding::DropPanic,
        HookFault::HoldItem { fail: false },
        false,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = case.evidence.post_entered.notified() => {},
            result = &mut next => panic!("item returned before held callback: {}", result.is_ok()),
        }
        drop(next);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(execution))).is_ok(),
            "source destructor panic must remain contained");
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        assert!(!case.evidence.names().iter().any(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")));
        case.evidence.post_release.notify_one();
        let snapshot = case.run.wait().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Failed(WorkflowFailure::Execution(_))),
            "the retained callback cannot lose the source destructor failure");
        assert_eq!(snapshot.state.values()["count"], 0);
        assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
        let calls = case.evidence.calls();
        let step = calls.iter().find(|row| row["event"] == "step-finish").unwrap();
        assert_eq!(step["outcome"]["kind"], "failure");
        assert_eq!(step["outcome"]["failure"]["category"], "execution");
        assert!(!step.to_string().contains("private source destructor panic"));
        let suffix = case.evidence.names().into_iter().filter(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")).collect::<Vec<_>>();
        assert_eq!(suffix, ["step-finish:sync", "stream-finish:sync", "execute-finish:sync"]);
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn permanent_eof_cleanup_read_closes_without_replaying_successful_guards() {
    HookCase::assert_eof_read_disposition(true).await;
}

#[tokio::test]
async fn recoverable_eof_cleanup_read_retains_the_original_successful_guards() {
    HookCase::assert_eof_read_disposition(false).await;
}

#[tokio::test]
async fn cancelled_next_retains_successful_eof_cleanup_before_explicit_retry() {
    for retry in [false, true] {
        let mut case = HookCase::open(
            SourceEnding::Eof,
            HookFault::HoldPost { fail: false },
            false,
        )
        .await;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let workspaces = case.app.list_workspaces(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
            let workspace = case.app.workspace(workspaces[0].id.clone()).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut observer = workflow.open_run(RunSelector::Latest(case.run.id().clone())).await.unwrap();
            let initial = observer.snapshot().await.unwrap();
            let mut execution = case.run.execution(HookCase::options()).await.unwrap();
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
            let mut next = Box::pin(execution.next());
            tokio::select! {
                _ = case.evidence.post_entered.notified() => {}
                result = &mut next => panic!("EOF returned before its admitted successful StepFinished settled: {}", result.is_ok()),
            }
            drop(next);
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 0);
            assert!(matches!(observer.execution(HookCase::options()).await, Err(AppError::Busy(_))));
            let held = observer.snapshot().await.unwrap();
            assert_eq!(held.state, initial.state);
            assert!(held.last_action_ordinal.is_none() && held.outcomes.is_empty());
            let names = case.evidence.names();
            assert_eq!(names.iter().filter(|name| name.as_str() == "step-finish:async").count(), 1);
            assert!(!names.iter().any(|name| name.starts_with("stream-finish") || name.starts_with("execute-finish")),
                "later guards cannot overtake the accepted EOF callback");
            case.evidence.post_release.notify_one();
            assert!(execution.next().await.unwrap().is_none(),
                "resuming the cancelled waiter must return the retained EOF before constructing another source");
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
            let names = case.evidence.names();
            for name in ["step-finish:sync", "step-finish:async", "stream-finish:sync", "stream-finish:async"] {
                assert_eq!(names.iter().filter(|observed| observed.as_str() == name).count(), 1);
            }
            assert!(!names.iter().any(|name| name.starts_with("execute-finish")),
                "successful EOF closes its action while the same Execution remains owned");
            if retry {
                assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })),
                    "only this separate explicit advancement may retry the saved position");
                assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 2);
                // A stored permit lets the second EOF's actual callback settle
                // inside finish without adding a second driving future.
                case.evidence.post_release.notify_one();
            }
            let finished = execution.finish().await.unwrap();
            assert!(matches!(finished.status, RunStatus::Paused));
            assert_eq!(finished.state, initial.state);
            assert!(finished.last_action_ordinal.is_none() && finished.outcomes.is_empty());
            let attempts = if retry { 2 } else { 1 };
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), attempts,
                "finish after an already observed EOF cannot silently replay the source");
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), attempts);
            assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), attempts);
            let calls = case.evidence.calls();
            for lane in ["sync", "async"] {
                for event in ["step-start", "step-finish", "stream-start", "stream-finish"] {
                    assert_eq!(calls.iter().filter(|row| row["event"] == event && row["lane"] == lane).count(), attempts);
                }
                for event in ["execute-start", "execute-finish"] {
                    assert_eq!(calls.iter().filter(|row| row["event"] == event && row["lane"] == lane).count(), 1);
                }
            }
            for step in calls.iter().filter(|row| row["event"] == "step-finish") {
                assert!(step["outcome"].is_null());
                assert_eq!(step["state"], json!(initial.state.values()));
                assert!(step["committed_ordinal"].is_null());
            }
            let contexts = case.evidence.source_contexts.lock().unwrap().clone();
            assert_eq!(contexts.len(), attempts);
            assert!(contexts.iter().all(|context| context["node"] == "work" && context["ordinal"] == 0));
        })).catch_unwind().await;
        case.evidence.post_release.notify_one();
        case.close(checked).await;
    }
}

#[tokio::test]
async fn background_eof_pauses_without_replay_and_allows_explicit_fresh_execution() {
    let evidence = Arc::new(HookEvidence::default());
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],
            "reducers":{"count":"reject_concurrent"},"entry":"work",
            "nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],
            "routes":{}
        })
        .to_string(),
    )
    .unwrap();
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = HookCase::app(
        store,
        vec![definition],
        &evidence,
        SourceEnding::Eof,
        HookFault::None,
        false,
    )
    .await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: std::env::current_dir().unwrap(),
            })
            .await
            .unwrap();
        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
        let run = workflow.start(HookCase::options()).await.unwrap();
        let paused = tokio::time::timeout(Duration::from_secs(3), run.wait())
            .await
            .expect("background no-output EOF must become observable without replay")
            .expect("successful EOF is not a failure");
        assert!(matches!(paused.status, RunStatus::Paused));
        assert_eq!(
            paused.state.values(),
            &Map::from_iter([("count".into(), json!(0))])
        );
        assert!(paused.last_action_ordinal.is_none() && paused.outcomes.is_empty());
        assert!(matches!(
            run.checkpoint(CheckpointSelector::Action(0)).await,
            Err(AppError::NotFound(_))
        ));
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
        let first = evidence.calls();
        for lane in ["sync", "async"] {
            for event in [
                "execute-start",
                "execute-finish",
                "step-start",
                "step-finish",
                "stream-start",
                "stream-finish",
            ] {
                assert_eq!(
                    first
                        .iter()
                        .filter(|row| row["event"] == event && row["lane"] == lane)
                        .count(),
                    1
                );
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let resumed = loop {
            let mut candidate = workflow
                .open_run(RunSelector::Latest(run.id().clone()))
                .await
                .unwrap();
            let acquired = candidate.execution(HookCase::options()).await;
            match acquired {
                Ok(mut execution) => {
                    assert!(
                        matches!(
                            execution.next().await.unwrap(),
                            Some(ExecutionEvent::Partial { .. })
                        ),
                        "a fresh explicit execution can retry the uncommitted position"
                    );
                    assert_eq!(evidence.constructed.load(Ordering::SeqCst), 2);
                    break execution.finish().await.unwrap();
                }
                Err(AppError::Busy(_)) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "completed EOF owner must release"
                    );
                    let observed = run.snapshot().await.unwrap();
                    assert_eq!(observed.checkpoint, paused.checkpoint);
                    assert_eq!(observed.state, paused.state);
                    assert!(observed.last_action_ordinal.is_none() && observed.outcomes.is_empty());
                    assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
                    assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected explicit EOF reacquisition failure: {error}"),
            }
        };
        assert!(matches!(resumed.status, RunStatus::Paused));
        assert_eq!(resumed.state, paused.state);
        assert!(resumed.last_action_ordinal.is_none() && resumed.outcomes.is_empty());
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 2);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 2);
        let calls = evidence.calls();
        for lane in ["sync", "async"] {
            for event in [
                "execute-start",
                "execute-finish",
                "step-start",
                "step-finish",
                "stream-start",
                "stream-finish",
            ] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|row| row["event"] == event && row["lane"] == lane)
                        .count(),
                    2
                );
            }
        }
        for step in calls.iter().filter(|row| row["event"] == "step-finish") {
            assert!(step["outcome"].is_null());
            assert_eq!(step["state"], json!(paused.state.values()));
            assert!(step["committed_ordinal"].is_null());
        }
    }))
    .catch_unwind()
    .await;
    let closed = AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    ))
    .catch_unwind()
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("background EOF and explicit resumption are bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("background EOF cleanup does not panic")
        .expect("background EOF cleanup is bounded")
        .expect("background EOF cleanup joins");
    assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn caller_eof_closes_the_uncommitted_native_sibling_before_same_owner_retry() {
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0,"native":0},"required":["count","native"],
            "reducers":{"count":"reject_concurrent","native":"reject_concurrent"},
            "entry":"fork","nodes":[
                {"id":"fork","step":{"kind":"state","mutations":[]}},
                {"id":"a_native","step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"native","value":{"kind":"literal","value":1}}
                ]}},
                {"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}
            ],"routes":{"fork":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"work"}},
                {"target":{"kind":"node","id":"a_native"}}
            ]}}
        })
        .to_string(),
    )
    .unwrap();
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut case = HookCase::from_source(
        store,
        vec![definition],
        SourceEnding::Eof,
        HookFault::None,
        false,
    )
    .await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(10), async {
        let workspaces = case.app.list_workspaces(None, NonZeroUsize::new(10).unwrap()).await.unwrap();
        assert_eq!(workspaces.len(), 1);
        let workspace = case.app.workspace(workspaces[0].id.clone()).await.unwrap();
        let observer = workspace.workflow("hooks".parse().unwrap()).await.unwrap()
            .open_run(RunSelector::Latest(case.run.id().clone())).await.unwrap();
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        let base = match execution.step().await.unwrap() {
            Some(ExecutionEvent::Committed { checkpoint, outcomes }) => {
                assert_eq!(outcomes.len(), 1);
                assert_eq!(outcomes[0].node, "fork");
                checkpoint
            }
            _ => panic!("the fork must commit before the uncommitted sibling wave"),
        };
        let prior = observer.snapshot().await.unwrap();
        for attempt in 1..=2 {
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { ref node, .. }) if node == "work"));
            assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), attempt);
            assert_eq!(case.evidence.typing_pre.load(Ordering::SeqCst), attempt,
                "the native mutation must actually run again on an explicit retry");
            let active = observer.snapshot().await.unwrap();
            assert!(!matches!(active.status, RunStatus::Failed(_)));
            assert_eq!(active.state.values(), prior.state.values());
            assert_eq!(active.last_action_ordinal, Some(0));
            assert!(active.outcomes == prior.outcomes);
            let started = case.evidence.calls();
            for (node, ordinal) in [("a_native", 1), ("work", 2)] {
                for lane in ["sync", "async"] {
                    let selected = started.iter().filter(|row| row["event"] == "step-start" && row["node"] == node && row["lane"] == lane).collect::<Vec<_>>();
                    assert_eq!(selected.len(), attempt);
                    assert!(selected.iter().all(|row| row["checkpoint"] == base && row["ordinal"] == ordinal));
                }
            }
            let source_contexts = case.evidence.source_contexts.lock().unwrap().clone();
            assert_eq!(source_contexts.len(), attempt);
            assert!(source_contexts.iter().all(|context| context == &json!({
                "node":"work", "ordinal":2, "checkpoint":base,
            })), "the later source observes the preceding completed native activation");
            assert!(execution.next().await.unwrap().is_none());
            let paused = observer.snapshot().await.unwrap();
            assert!(matches!(paused.status, RunStatus::Paused));
            assert_eq!(paused.state.values(), prior.state.values());
            assert_eq!(paused.last_action_ordinal, Some(0));
            assert!(paused.outcomes == prior.outcomes);
            assert!(matches!(observer.checkpoint(CheckpointSelector::Action(1)).await, Err(AppError::NotFound(_))));
            assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), attempt);
            let calls = case.evidence.calls();
            for (node, ordinal) in [("a_native", 1), ("work", 2)] {
                for lane in ["sync", "async"] {
                    for event in ["step-start", "step-finish"] {
                        let selected = calls.iter().filter(|row| row["event"] == event && row["node"] == node && row["lane"] == lane).collect::<Vec<_>>();
                        assert_eq!(selected.len(), attempt, "each uncommitted sibling closes before retry");
                        assert!(selected.iter().all(|row| row["checkpoint"] == base && row["ordinal"] == ordinal));
                        if event == "step-finish" {
                            assert!(selected.iter().all(|row| row["outcome"].is_null()
                                && row["state"] == json!(prior.state.values()) && row["committed_ordinal"] == 0));
                        }
                    }
                }
            }
            for lane in ["sync", "async"] {
                for event in ["stream-start", "stream-finish"] {
                    let selected = calls.iter().filter(|row| row["event"] == event && row["lane"] == lane).collect::<Vec<_>>();
                    assert_eq!(selected.len(), attempt);
                    assert!(selected.iter().all(|row| row["node"] == "work"));
                    if event == "stream-finish" {
                        assert!(selected.iter().all(|row| row["failure"].is_null() && row["items"] == 1));
                    }
                }
            }
            assert!(!calls.iter().any(|row| row["event"] == "execute-finish"));
        }
        let finished = execution.finish().await.unwrap();
        assert!(matches!(finished.status, RunStatus::Paused));
        assert_eq!(finished.state.values(), prior.state.values());
        assert_eq!(finished.last_action_ordinal, Some(0));
        assert!(finished.outcomes == prior.outcomes);
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 2);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 2);
        let calls = case.evidence.calls();
        for lane in ["sync", "async"] {
            for event in ["execute-start", "execute-finish"] {
                assert_eq!(calls.iter().filter(|row| row["event"] == event && row["lane"] == lane).count(), 1);
            }
        }
    })).catch_unwind().await;
    case.close(checked).await;
}

#[tokio::test]
async fn returned_eof_does_not_charge_caller_idle_time_to_execution_cleanup() {
    let mut case = HookCase::open(SourceEnding::Eof, HookFault::None, false).await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(36), async {
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        assert!(matches!(
            execution.next().await.unwrap(),
            Some(ExecutionEvent::Partial { .. })
        ));
        assert!(
            execution.next().await.unwrap().is_none(),
            "observe actual successful source EOF"
        );
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        let before_idle = case.evidence.calls();
        for lane in ["sync", "async"] {
            for event in ["step-finish", "stream-finish"] {
                assert_eq!(
                    before_idle
                        .iter()
                        .filter(|row| row["event"] == event && row["lane"] == lane)
                        .count(),
                    1,
                    "EOF must settle the source guards before the caller idle interval"
                );
            }
        }
        assert!(
            !before_idle
                .iter()
                .any(|row| row["event"] == "execute-finish")
        );
        tokio::time::sleep(Duration::from_secs(31)).await;
        assert_eq!(
            case.evidence.calls(),
            before_idle,
            "caller idle time cannot dispatch more callbacks or an automatic source retry"
        );
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        let finished = tokio::time::timeout(Duration::from_secs(3), execution.finish())
            .await
            .expect("finish after caller idle has its own bounded observation")
            .expect("an already completed EOF chain cannot expire later execution cleanup");
        assert!(matches!(finished.status, RunStatus::Paused));
        assert_eq!(finished.state.values()["count"], 0);
        assert!(finished.last_action_ordinal.is_none() && finished.outcomes.is_empty());
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        let calls = case.evidence.calls();
        for lane in ["sync", "async"] {
            for event in [
                "execute-start",
                "execute-finish",
                "step-start",
                "step-finish",
                "stream-start",
                "stream-finish",
            ] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|row| row["event"] == event && row["lane"] == lane)
                        .count(),
                    1
                );
            }
        }
        for step in calls.iter().filter(|row| row["event"] == "step-finish") {
            assert!(step["outcome"].is_null());
            assert!(step["committed_ordinal"].is_null());
            assert_eq!(step["state"]["count"], 0);
        }
    }))
    .catch_unwind()
    .await;
    case.close(checked).await;
}

#[tokio::test]
async fn background_declaration_errors_surface_without_orphaning_unstarted_work() {
    for bound_override in [true, false] {
        let evidence = Arc::new(HookEvidence::default());
        let definition = WorkflowDefinition::parse(
            &json!({
                "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
                "state":{"count":0},"required":["count"],
                "reducers":{"count":"reject_concurrent"},"entry":"work",
                "nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],
                "routes":{}
            })
            .to_string(),
        )
        .unwrap();
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let config = HookCase::config(
            store,
            vec![definition],
            &evidence,
            SourceEnding::Complete,
            HookFault::None,
            false,
        )
        .with_actions(vec![HookAction {
            declaration: ActionDeclaration {
                key: "local".into(),
                revision: "r1".into(),
                reads: vec!["count".into()],
                writes: vec!["count".into()],
                required_inputs: vec!["prompt".into()],
                optional_inputs: Vec::new(),
                bound_inputs: if bound_override {
                    Map::from_iter([("prompt".into(), json!("private caller input"))])
                } else {
                    Map::new()
                },
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: if bound_override {
                    ActionMode::Asynchronous
                } else {
                    ActionMode::Synchronous
                },
            },
            evidence: Arc::clone(&evidence),
            ending: SourceEnding::Complete,
        }]);
        let mut app = App::open(config).await.unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            // Both options are valid JSON and reach the action declaration
            // after the original durable admission and registration complete.
            let run = workflow.start(HookCase::options()).await.unwrap();
            let observed = tokio::time::timeout(Duration::from_secs(3), run.wait())
                .await
                .expect("a rejected background declaration cannot strand Running work")
                .expect("background declaration rejection has durable failure authority");
            let message = match observed.status {
                RunStatus::Failed(WorkflowFailure::Execution(message)) => message,
                _ => panic!("background declaration rejection must retain an Execution failure"),
            };
            assert_eq!(
                message,
                if bound_override {
                    "runtime input cannot override a bound action input"
                } else {
                    "action does not support the selected execution mode"
                }
            );
            let snapshot = run.snapshot().await.unwrap();
            assert!(
                matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::Execution(saved))
                if saved == &message),
                "snapshot retains the exact background failure"
            );
            assert_eq!(
                snapshot.state.values(),
                &Map::from_iter([("count".into(), json!(0))])
            );
            assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
            assert!(
                matches!(
                    run.checkpoint(CheckpointSelector::Action(0)).await,
                    Err(AppError::NotFound(_))
                ),
                "declaration rejection cannot fabricate an attempted action boundary"
            );
            let calls = evidence.calls();
            for lane in ["sync", "async"] {
                assert_eq!(
                    calls
                        .iter()
                        .filter(|row| row["event"] == "registered" && row["lane"] == lane)
                        .count(),
                    1
                );
            }
            assert!(
                !calls
                    .iter()
                    .any(|row| row["event"].as_str().unwrap().starts_with("step-")
                        || row["event"].as_str().unwrap().starts_with("stream-")),
                "invalid declaration must be rejected before step hooks and source dispatch"
            );
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
            let no_source_context = evidence.source_contexts.lock().unwrap().is_empty();
            assert!(no_source_context);
        }))
        .catch_unwind()
        .await;
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("background declaration scenario is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("background declaration cleanup does not panic")
            .expect("background declaration cleanup is bounded")
            .expect("background declaration cleanup joins");
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn owning_drop_during_admitted_post_hook_closes_only_remaining_sync_guards() {
    let mut case = HookCase::open(
        SourceEnding::Complete,
        HookFault::HoldPost { fail: false },
        false,
    )
    .await;
    let checked = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(7), async {
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = case.evidence.post_entered.notified() => {},
            result = &mut next => panic!("commit returned before held post callback: {}", result.is_ok()),
        }
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 1);
        drop(next);
        drop(execution);
        assert!(!case.evidence.names().iter().any(|name| name.starts_with("stream-finish") || name.starts_with("execute-finish")),
            "an admitted step post-hook settles before subsequent guards");
        case.evidence.post_release.notify_one();
        let snapshot = case.run.wait().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Paused));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(snapshot.outcomes.len(), 1);
        let suffix = case.evidence.names().into_iter().filter(|name| name.starts_with("step-finish")
            || name.starts_with("stream-finish") || name.starts_with("execute-finish")).collect::<Vec<_>>();
        assert_eq!(suffix, ["step-finish:sync", "step-finish:async", "stream-finish:sync", "execute-finish:sync"],
            "Drop retains the already admitted async callback and closes later guards synchronously once");
        let calls = case.evidence.calls();
        for step in calls.iter().filter(|row| row["event"] == "step-finish") {
            assert_eq!(step["outcome"]["kind"], "success");
            assert_eq!(step["state"]["count"], 1);
            assert_eq!(step["committed_ordinal"], 0);
        }
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
    })).catch_unwind().await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn permanent_abort_cleanup_read_closes_without_publishing_from_stale_authority() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Permanent,
        ExecutionReadPhase::AfterPartial,
    )
    .await;
}

#[tokio::test]
async fn recoverable_abort_cleanup_read_retains_the_original_source_and_publication() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Recoverable,
        ExecutionReadPhase::AfterPartial,
    )
    .await;
}

#[tokio::test]
async fn held_abort_cleanup_read_expires_and_drops_inside_the_original_cleanup_budget() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Held,
        ExecutionReadPhase::AfterPartial,
    )
    .await;
}

#[tokio::test]
async fn sibling_post_steps_reuse_the_original_wave_cleanup_budget() {
    let definition = WorkflowDefinition::parse(
        &json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"collect"},"entry":"fork",
            "nodes":[{"id":"fork","step":{"kind":"state","mutations":[]}},
                {"id":"a","step":{"kind":"action","action":"local","revision":"r1"}},
                {"id":"b","step":{"kind":"action","action":"local","revision":"r1"}}],
            "routes":{"fork":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"b"}},{"target":{"kind":"node","id":"a"}}
            ]}}
        })
        .to_string(),
    )
    .unwrap();
    let mut case = HookCase::from_definitions(
        vec![definition],
        HookFault::SharedSiblingCleanupBudget,
        false,
    )
    .await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(35), async {
        let mut execution = case.run.execution(HookCase::options()).await.unwrap();
        assert!(matches!(execution.step().await.unwrap(), Some(ExecutionEvent::Committed { .. })));
        let mut finished = Box::pin(execution.finish());
        tokio::select! {
            _ = case.evidence.post_entered.notified() => {},
            result = &mut finished => panic!("the second committed sibling guard was not held: {}", result.is_ok()),
        }
        // The first sibling spent 20s of the same wave's 30s cleanup ceiling.
        // A new ceiling for the second sibling would exceed this total bound.
        let result = tokio::time::timeout(Duration::from_secs(13), &mut finished).await;
        assert!(matches!(result, Ok(Err(AppError::Timeout(_)))),
            "later sibling cleanup cannot reset the original wave budget");
        drop(finished);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
        let snapshot = case.run.snapshot().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Paused));
        assert_eq!(snapshot.state.values()["count"], json!([1, 1]));
        assert_eq!(snapshot.last_action_ordinal, Some(2));
        assert_eq!(snapshot.outcomes.len(), 2);
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 2);
        assert_eq!(case.evidence.source_dropped.load(Ordering::SeqCst), 2);
        let calls = case.evidence.calls();
        for node in ["a", "b"] {
            for lane in ["sync", "async"] {
                assert_eq!(calls.iter().filter(|row| row["event"] == "step-finish"
                    && row["node"] == node && row["lane"] == lane).count(), 1);
            }
        }
        assert!(!calls.iter().any(|row| row["event"] == "execute-finish" && row["lane"] == "async"),
            "the exhausted original budget cannot admit another asynchronous closing guard");
    })).catch_unwind().await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn terminal_capacity_stays_owned_until_execute_finished_returns() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut case = HookCase::from_source(
        store.clone(),
        vec![HookCase::native_definition()],
        SourceEnding::Complete,
        HookFault::HoldExecuteFinished,
        false,
    )
    .await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(6), async {
        let workspaces = case.app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
        let workspace = &workspaces[0].id;
        let occurrence = case.run.snapshot().await.unwrap().occurrence;
        let scope = format!("{workspace}/hooks/{}/{}", case.run.id(), occurrence);
        let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker_key = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        let app_key = StateKey::new("app-sdk", "app").unwrap();
        let execution = case.run.execution(ExecutionOptions::default()).await.unwrap();
        let mut finished = Box::pin(execution.finish());
        tokio::select! {
            _ = case.evidence.post_entered.notified() => {},
            result = &mut finished => panic!("terminal execution returned before its held closing guard: {}", result.is_ok()),
        }
        let head = store.load(head_key.clone()).await.unwrap().unwrap();
        let checkpoint = store.load(StateKey::new("app-sdk", format!(
            "history/{scope}/checkpoint/{:020}", head.record.value["sequence"].as_u64().unwrap(),
        )).unwrap()).await.unwrap().unwrap();
        assert_eq!(checkpoint.record.value["checkpoint"]["phase"]["state"], "succeeded",
            "the held ExecuteFinished observes the actual durable terminal checkpoint");
        assert_eq!(checkpoint.record.value["checkpoint"]["state"]["count"], 1,
            "the successful native state transition is already durable before the terminal hook");
        assert_eq!(head.record.value.pointer("/retention/state"), Some(&json!("active")),
            "a terminal checkpoint cannot release capacity while ExecuteFinished still owns cleanup");
        assert!(store.load(marker_key.clone()).await.unwrap().is_some());
        assert_eq!(store.load(app_key.clone()).await.unwrap().unwrap().record.value["run_count"], 1);
        let calls = case.evidence.calls();
        for lane in ["sync", "async"] {
            let terminal = calls.iter().filter(|row| row["event"] == "execute-finish" && row["lane"] == lane)
                .collect::<Vec<_>>();
            assert_eq!(terminal.len(), 1);
            assert_eq!(terminal[0]["state"]["count"], 1);
            assert!(terminal[0]["failure"].is_null());
        }
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 0);
        case.evidence.post_release.notify_one();
        let snapshot = finished.await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.state.values()["count"], 1);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(snapshot.outcomes.len(), 1);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let head = store.load(head_key.clone()).await.unwrap().unwrap();
                if head.record.value.pointer("/retention/state") == Some(&json!("retained")) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("capacity releases after its terminal cleanup guard completes");
        assert!(store.load(marker_key).await.unwrap().is_none());
        assert_eq!(store.load(app_key).await.unwrap().unwrap().record.value["run_count"], 0);
        assert_eq!(case.evidence.waits_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(case.evidence.constructed.load(Ordering::SeqCst), 0,
            "this native graph needs no Action source to expose terminal cleanup ownership");
        for lane in ["sync", "async"] {
            assert_eq!(case.evidence.calls().iter().filter(|row|
                row["event"] == "execute-finish" && row["lane"] == lane).count(), 1);
        }
    })).catch_unwind().await;
    case.evidence.post_release.notify_one();
    case.close(checked).await;
}

#[tokio::test]
async fn terminal_open_reconciles_a_real_capacity_release_during_observation() {
    let evidence = Arc::new(HookEvidence::default());
    let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = App::open(HookCase::config(
        HookAdmissionStore {
            inner: native.clone(),
            evidence: Arc::clone(&evidence),
        },
        vec![HookCase::native_definition()],
        &evidence,
        SourceEnding::Complete,
        HookFault::HoldExecuteFinished,
        false,
    ))
    .await
    .unwrap();
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None,
            root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let occurrence = run.snapshot().await.unwrap().occurrence;
        let selector = RunSelector::Exact { id: run.id().clone(), occurrence: occurrence.clone() };
        let scope = format!("{}/hooks/{}/{}", workspace.id(), run.id(), occurrence);
        let head_key = StateKey::new("app-sdk", format!("run/{scope}")).unwrap();
        let marker_key = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
        let execution = run.execution(ExecutionOptions::default()).await.unwrap();
        let mut finished = Box::pin(execution.finish());
        tokio::select! {
            _ = evidence.post_entered.notified() => {},
            result = &mut finished => panic!("terminal cleanup gate was not reached: {}", result.is_ok()),
        }
        let before = native.load(head_key.clone()).await.unwrap().unwrap();
        assert_eq!(before.record.value.pointer("/retention/state"), Some(&json!("active")));
        let checkpoint_key = StateKey::new("app-sdk", format!(
            "history/{scope}/checkpoint/{:020}", before.record.value["sequence"].as_u64().unwrap(),
        )).unwrap();
        let checkpoint = native.load(checkpoint_key.clone()).await.unwrap().unwrap();
        *evidence.cleanup_read.lock().unwrap() = Some((marker_key.clone(), CleanupReadFault::Captured));
        let mut opening = Box::pin(workflow.open_run(selector));
        tokio::select! {
            _ = evidence.admission_entered.notified() => {},
            result = &mut opening => panic!("terminal capacity read gate was not reached: {}", result.is_ok()),
        }
        assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);
        evidence.post_release.notify_one();
        let terminal = finished.await.unwrap();
        assert!(matches!(terminal.status, RunStatus::Succeeded));
        let retained = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let head = native.load(head_key.clone()).await.unwrap().unwrap();
                if head.record.value.pointer("/retention/state") == Some(&json!("retained")) {
                    break head;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("the real retention owner completes while only the observer read is held");
        assert_ne!(retained.revision, before.revision);
        assert_eq!(retained.record.value["sequence"], before.record.value["sequence"]);
        assert!(native.load(marker_key).await.unwrap().is_none());
        assert_eq!(native.load(StateKey::new("app-sdk", "app").unwrap()).await.unwrap().unwrap().record.value["run_count"], 0);
        evidence.admission_release.notify_one();
        let observer = opening.await.expect("one terminal open must reconcile the real capacity-release race");
        let snapshot = observer.snapshot().await.unwrap();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.id, terminal.id);
        assert_eq!(snapshot.occurrence, terminal.occurrence);
        assert_eq!(snapshot.checkpoint, terminal.checkpoint);
        assert_eq!(snapshot.state, terminal.state);
        assert_eq!(snapshot.last_action_ordinal, terminal.last_action_ordinal);
        assert_eq!(snapshot.outcomes.len(), terminal.outcomes.len());
        assert_eq!(native.load(checkpoint_key).await.unwrap().unwrap(), checkpoint);
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 2);
    })).catch_unwind().await;
    evidence.cleanup_read.lock().unwrap().take();
    evidence.post_release.notify_one();
    evidence.admission_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    closed.expect("all original terminal owners join").unwrap();
    match checked {
        Ok(result) => result.expect("the gated terminal observation remains bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
async fn permanent_abort_read_before_first_poll_closes_without_fabricating_execution() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Permanent,
        ExecutionReadPhase::BeforePolling,
    )
    .await;
}

#[tokio::test]
async fn recoverable_abort_read_before_first_poll_retains_the_same_settlement() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Recoverable,
        ExecutionReadPhase::BeforePolling,
    )
    .await;
}

#[tokio::test]
async fn held_abort_read_before_first_poll_expires_and_joins_without_opening_hooks() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Held,
        ExecutionReadPhase::BeforePolling,
    )
    .await;
}

impl HookCase {
    async fn opening_failure_receipt(phase: OpeningReadPhase) {
        let generation_read = matches!(phase, OpeningReadPhase::Generation);
        let delayed_completion = matches!(phase, OpeningReadPhase::Completed);
        let evidence = Arc::new(HookEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(HookCase::config(
            HookAdmissionStore {
                inner: inner.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![HookCase::native_definition()],
            &evidence,
            SourceEnding::Complete,
            if generation_read {
                HookFault::OpeningBudget
            } else {
                HookFault::Error {
                    event: "execute-start",
                    lane: "async",
                }
            },
            false,
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(if delayed_completion { 39 } else if generation_read { 36 } else { 8 }), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let occurrence = run.snapshot().await.unwrap().occurrence;
        let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence)).unwrap();
        let mut execution = run.execution(HookCase::options()).await.unwrap();
        *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::Failure);
        let mut next = Box::pin(execution.next());
        tokio::select! {
            _ = evidence.barrier_entered.notified() => {},
            result = &mut next => panic!("rejected start returned before its native failure receipt: {}", result.is_ok()),
        }
        assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
        let failed = observer.snapshot().await.unwrap();
        assert!(matches!(&failed.status, RunStatus::Failed(WorkflowFailure::Hook { cause: None, .. })));
        assert_eq!(failed.state.values()["count"], 0);
        assert!(failed.outcomes.is_empty() && failed.last_action_ordinal.is_none());
        assert_eq!(evidence.names(), ["registered:sync", "registered:async",
            "execute-start:sync", "execute-start:async"]);
        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
        let failed_head = inner.load(key.clone()).await.unwrap().unwrap();
        drop(next);
        // The actual native failure is already durable. A recoverable read
        // outage after its receipt must not replace the original Hook failure.
        if !delayed_completion {
            *evidence.cleanup_read.lock().unwrap() = Some(if generation_read {
                (StateKey::new("app-sdk", "app").unwrap(), CleanupReadFault::Held)
            } else {
                (key.clone(), CleanupReadFault::Recoverable)
            });
        }
        evidence.barrier_release.notify_one();
        let failure = {
            let mut next = Box::pin(execution.next());
            if generation_read {
                tokio::select! {
                    _ = evidence.post_entered.notified() => {},
                    result = &mut next => panic!("cleanup did not enter its selected generation read: {}", result.is_ok()),
                }
                assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 0);
                tokio::time::timeout(Duration::from_secs(13), next).await
                    .expect("generation authority shares the remaining original opening budget")
            } else {
                next.await
            }
        }.err().expect("the original rejection is retained");
        assert!(failure.to_string().contains("explicit caller hook context"));
        if delayed_completion {
            let expected = ["registered:sync", "registered:async",
                "execute-start:sync", "execute-start:async", "execute-finish:sync", "execute-finish:async"];
            assert_eq!(evidence.names(), expected);
            // This is caller idle time after a completed error observation,
            // not part of the later owning Drop's authority observation.
            tokio::time::sleep(Duration::from_secs(31)).await;
            app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            let checkpoint_key = StateKey::new("app-sdk", format!(
                "history/{}/hooks/{}/{}/checkpoint/{:020}", workspace.id(), failed.id, failed.occurrence, failed.checkpoint,
            )).unwrap();
            let original = inner.load(checkpoint_key.clone()).await.unwrap().unwrap();
            // Every actual read of this exact head still returns real valid
            // authority, after Pending. Receipt reuse may remove reads entirely.
            *evidence.cleanup_read.lock().unwrap() = Some((key, CleanupReadFault::Delayed));
            drop(execution);
            app.shutdown(Shutdown::Drain { timeout: Duration::from_secs(4) }).await
                .expect("later Drop and public Drain cannot reuse an expired completed budget");
            evidence.cleanup_read.lock().unwrap().take();
            assert_eq!(inner.load(checkpoint_key).await.unwrap().unwrap(), original);
            assert_eq!(evidence.names(), expected);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
            return;
        }
        drop(execution);
        if generation_read {
            assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1,
                "fatal failure destroys the original held generation read before returning");
            assert!(matches!(app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await,
                Err(AppError::Closed(_))));
            assert_eq!(inner.load(key).await.unwrap().unwrap(), failed_head,
                "unreadable generation cannot authorize capacity mutation");
            assert_eq!(evidence.names(), ["registered:sync", "registered:async",
                "execute-start:sync", "execute-start:async"]);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            return;
        }
        let snapshot = observer.wait().await.unwrap();
        assert!(matches!(&snapshot.status, RunStatus::Failed(WorkflowFailure::Hook { cause: None, .. })));
        assert_eq!(snapshot.checkpoint, failed.checkpoint);
        assert_eq!(snapshot.state, failed.state);
        assert!(snapshot.outcomes == failed.outcomes);
        assert_eq!(snapshot.last_action_ordinal, failed.last_action_ordinal);
        assert_eq!(evidence.names(), ["registered:sync", "registered:async",
            "execute-start:sync", "execute-start:async", "execute-finish:sync", "execute-finish:async"]);
        let rows = evidence.calls();
        for row in rows.iter().filter(|row| row["event"] == "execute-finish") {
            assert_eq!(row["failure"]["category"], "hook");
            assert!(row["failure"]["message"].as_str().unwrap().contains("explicit caller hook context"));
        }
        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
        // Receipt reuse may remove the faulted read entirely; the assertion is
        // stable authority and lifecycle, not a mandated follow-up read.
        assert!(evidence.cleanup_read_faults.load(Ordering::SeqCst) <= 1);
        app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
    })).catch_unwind().await;
        evidence.barrier_armed.lock().unwrap().take();
        evidence.cleanup_read.lock().unwrap().take();
        evidence.barrier_release.notify_one();
        evidence.post_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("rejected execution start remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed.expect("native failure receipt and lifecycle cleanup join");
        if generation_read {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        } else {
            closed.unwrap();
        }
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn rejected_execution_start_retains_its_native_failure_receipt_across_lost_observation() {
    HookCase::opening_failure_receipt(OpeningReadPhase::FailureReceipt).await;
}

#[tokio::test]
async fn rejected_execution_start_generation_read_keeps_its_original_opening_deadline() {
    HookCase::opening_failure_receipt(OpeningReadPhase::Generation).await;
}

#[tokio::test]
async fn dropped_control_retains_completed_guards_during_recoverable_cleanup_read() {
    HookCase::control_lifecycle(true, Some(CleanupReadFault::Recoverable)).await;
}

#[tokio::test]
async fn dropped_control_permanent_cleanup_read_closes_without_replaying_completed_guards() {
    HookCase::control_lifecycle(true, Some(CleanupReadFault::Permanent)).await;
}

#[tokio::test]
async fn dropped_control_held_cleanup_read_is_bounded_after_synchronous_guards() {
    HookCase::control_lifecycle(true, Some(CleanupReadFault::Held)).await;
}

#[tokio::test]
async fn returned_hook_error_does_not_charge_caller_idle_time_to_later_drop_cleanup() {
    HookCase::opening_failure_receipt(OpeningReadPhase::Completed).await;
}

impl HookCase {
    async fn native_prefix_failure_receipt(boundary: HookCommit) {
        let evidence = Arc::new(HookEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: inner.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![Self::native_prefix_definition()],
            &evidence,
            SourceEnding::Complete,
            HookFault::NativePreError { lane: "async" },
            false,
        ))
        .await
        .unwrap();
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(8), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence)).unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            let base_outcomes = match execution.step().await.unwrap() {
                Some(ExecutionEvent::Committed { outcomes, .. }) => {
                    assert_eq!(outcomes.len(), 1);
                    assert_eq!(outcomes[0].node, "fork");
                    assert_eq!(outcomes[0].ordinal, 0);
                    outcomes
                }
                _ => panic!("the preceding fork commits before native sibling failure"),
            };
            *evidence.barrier_armed.lock().unwrap() = Some(boundary);
            let mut next = Box::pin(execution.next());
            tokio::select! {
                _ = evidence.barrier_entered.notified() => {},
                result = &mut next => panic!("native failure cleanup did not reach the selected receipt: {}", result.is_ok()),
            }
            assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
            let head = inner.load(key.clone()).await.unwrap().unwrap();
            let checkpoint_key = StateKey::new("app-sdk", format!(
                "history/{}/checkpoint/{:020}", head.record.key.key().strip_prefix("run/").unwrap(),
                head.record.value["sequence"].as_u64().unwrap(),
            )).unwrap();
            let native = inner.load(checkpoint_key.clone()).await.unwrap().unwrap();
            let checkpoint = &native.record.value["checkpoint"];
            let phase = match boundary {
                HookCommit::NativePrefix => "active",
                HookCommit::Stopping => "failing",
                HookCommit::Failure => "failed",
                HookCommit::RejectedCancellation | HookCommit::SourceFailureStage | HookCommit::Barrier | HookCommit::ChildIntent | HookCommit::ChildAdmission | HookCommit::Halt | HookCommit::Prepared | HookCommit::Retained => unreachable!(),
            };
            assert_eq!(checkpoint["phase"]["state"], phase);
            assert_eq!(checkpoint["state"], json!({"count":0,"native":"not an integer"}));
            assert_eq!(checkpoint["outcomes"].as_array().unwrap().len(), 1);
            assert_eq!(checkpoint["outcomes"][0]["node"], "fork");
            let primary = if matches!(boundary, HookCommit::NativePrefix) {
                let staged = checkpoint["staged"].as_array().unwrap();
                assert_eq!(staged.len(), 1);
                assert_eq!(staged[0]["node"], "a_native");
                assert_eq!(staged[0]["outcome"]["state"], "failed");
                assert_eq!(staged[0]["outcome"]["category"], "state");
                staged[0]["outcome"]["message"].as_str().unwrap().to_owned()
            } else {
                assert_eq!(checkpoint["phase"]["category"], "state");
                checkpoint["phase"]["message"].as_str().unwrap().to_owned()
            };
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1);
            assert!(evidence.rejected.load(Ordering::SeqCst));
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            drop(next);
            // Only the waiter is gone. Native commit authority and the exact
            // preceding State failure remain owned by the original publication.
            *evidence.cleanup_read.lock().unwrap() = Some((key, CleanupReadFault::Recoverable));
            evidence.barrier_release.notify_one();
            let error = execution.next().await.err().expect("the original later Hook rejection remains observable");
            assert!(error.to_string().contains("explicit caller hook context"));
            drop(execution);
            let observed = observer.wait().await.err().expect("completed Hook context remains observable");
            assert!(observed.to_string().contains("explicit caller hook context"),
                "temporary pure-read failure cannot replace retained Hook context with closure");
            let failed = observer.snapshot().await.unwrap();
            assert!(matches!(&failed.status, RunStatus::Failed(WorkflowFailure::State(message)) if message == &primary));
            assert_eq!(failed.state.values(), &Map::from_iter([
                ("count".into(), json!(0)), ("native".into(), json!("not an integer")),
            ]));
            assert_eq!(failed.last_action_ordinal, Some(0));
            assert!(failed.outcomes == base_outcomes);
            let attributed = observer.checkpoint(CheckpointSelector::Action(1)).await.unwrap();
            assert!(matches!(attributed.kind, CheckpointKind::Failed { ref node, ordinal: 1 } if node == "a_native"));
            let calls = evidence.calls();
            for node in ["a_native", "b_native"] {
                assert_eq!(calls.iter().filter(|row| row["event"] == "step-start" && row["node"] == node).count(), 2);
            }
            let completions = calls.iter().filter(|row| row["event"] == "execute-finish").collect::<Vec<_>>();
            assert_eq!(completions.len(), 2);
            for completion in completions {
                assert_eq!(completion["failure"]["category"], "hook");
                assert_eq!(completion["failure"]["cause"]["category"], "state");
                assert_eq!(completion["failure"]["cause"]["message"], primary);
            }
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
            assert_eq!(inner.load(checkpoint_key).await.unwrap().unwrap(), native);
            // Legitimate receipt reuse may remove the pure read completely.
            evidence.cleanup_read.lock().unwrap().take();
            app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
        })).catch_unwind().await;
        evidence.barrier_armed.lock().unwrap().take();
        evidence.cleanup_read.lock().unwrap().take();
        evidence.barrier_release.notify_one();
        evidence.post_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("native failure cleanup remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed
            .expect("native prefix failure cleanup joins")
            .unwrap();
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_prefix_stage_receipt_preserves_primary_failure_after_lost_observer() {
    HookCase::native_prefix_failure_receipt(HookCommit::NativePrefix).await;
}

#[tokio::test]
async fn native_prefix_stopping_receipt_preserves_primary_failure_after_lost_observer() {
    HookCase::native_prefix_failure_receipt(HookCommit::Stopping).await;
}

#[tokio::test]
async fn native_prefix_terminal_receipt_preserves_primary_failure_after_lost_observer() {
    HookCase::native_prefix_failure_receipt(HookCommit::Failure).await;
}

impl HookCase {
    async fn post_hook_authority_read(fault: CleanupReadFault, shared_budget: bool) {
        let evidence = Arc::new(HookEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: inner.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![Self::native_definition()],
            &evidence,
            SourceEnding::Complete,
            HookFault::HoldPost { fail: true },
            false,
        ))
        .await
        .unwrap();
        let held = matches!(fault, CleanupReadFault::Held);
        let fatal = !matches!(fault, CleanupReadFault::Recoverable);
        let checked = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(if held { 37 } else { 8 }), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let run_id = run.id().clone();
            let observer = workflow.open_run(RunSelector::Latest(run_id.clone())).await.unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), run_id, occurrence)).unwrap();
            let execution = run.execution(ExecutionOptions::default()).await.unwrap();
            let mut finish = Box::pin(execution.finish());
            tokio::select! {
                _ = evidence.post_entered.notified() => {},
                result = &mut finish => panic!("original post-hook remains held: {}", result.is_ok()),
            }
            let committed = observer.snapshot().await.unwrap();
            assert_eq!(committed.state.values()["count"], 1);
            assert_eq!(committed.last_action_ordinal, Some(0));
            assert_eq!(committed.outcomes.len(), 1);
            if shared_budget { tokio::time::sleep(Duration::from_secs(20)).await; }
            // Independent cancellation must survive either a fresh observation
            // or native reconciliation of the original barrier's Pause intent.
            observer.cancel(committed.checkpoint).await.unwrap();
            let cancelled = inner.load(key.clone()).await.unwrap().unwrap();
            let cancelled_row = inner.load(StateKey::new("app-sdk", format!(
                "history/{}/checkpoint/{:020}", cancelled.record.key.key().strip_prefix("run/").unwrap(),
                cancelled.record.value["sequence"].as_u64().unwrap(),
            )).unwrap()).await.unwrap().unwrap();
            assert_eq!(cancelled_row.record.value["checkpoint"]["phase"]["state"], "cancelling");
            *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), fault));
            evidence.cleanup_read_until_mutation.store(true, Ordering::SeqCst);
            evidence.post_release.notify_one();
            let completed = if held {
                tokio::select! {
                    _ = evidence.post_entered.notified() => {
                        assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1,
                            "original failed hook is dropped before its fresh read");
                        assert_eq!(inner.load(key.clone()).await.unwrap().unwrap(), cancelled);
                        None
                    },
                    result = &mut finish => Some(result),
                }
            } else { None };
            let result = match completed {
                Some(result) => result,
                None => tokio::time::timeout(Duration::from_secs(
                    if shared_budget { 13 } else if held { 33 } else { 3 },
                ), &mut finish).await.expect("post-hook authority shares the original cleanup budget"),
            };
            let error = result.err().expect("original failed callback remains the primary execution result");
            assert!(error.to_string().contains("explicit caller hook context"),
                "authority observation cannot erase the original callback error: {error}");
            drop(finish);
            let read_faults = evidence.cleanup_read_faults.load(Ordering::SeqCst);
            assert!(read_faults <= 1);
            assert!(evidence.cleanup_read.lock().unwrap().is_none());
            if read_faults == 0 {
                assert!(!evidence.cleanup_read_until_mutation.load(Ordering::SeqCst),
                    "only actual native mutation admission retires the optional read fault");
            }
            if held {
                assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1 + read_faults,
                    "every original observed hook/read is destroyed before the result");
            }
            if fatal && read_faults != 0 {
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                            Err(AppError::Closed(_)) => break,
                            Ok(_) => tokio::task::yield_now().await,
                            Err(error) => panic!("unexpected post-hook authority disposition: {error}"),
                        }
                    }
                }).await.expect("unreadable post-hook authority closes the App");
                assert_eq!(inner.load(key.clone()).await.unwrap().unwrap(), cancelled,
                    "unreadable authority cannot replace the accepted cancellation with stale barrier state");
            } else {
                let terminal = tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        let snapshot = observer.snapshot().await.unwrap();
                        if matches!(snapshot.status, RunStatus::Cancelled) { break snapshot; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the original native cancellation survives recoverable freshness");
                assert_eq!(terminal.state.values()["count"], 1);
                assert_eq!(terminal.last_action_ordinal, Some(0));
                assert!(terminal.outcomes == committed.outcomes);
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            }
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1,
                "freshness retry cannot repeat the already committed native action");
            for lane in ["sync", "async"] {
                assert_eq!(evidence.calls().iter().filter(|row| row["event"] == "step-finish"
                    && row["lane"] == lane).count(), 1,
                    "freshness retry cannot repeat the original post-hook");
            }
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence
            .cleanup_read_until_mutation
            .store(false, Ordering::SeqCst);
        evidence.post_release.notify_one();
        let closed = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("post-hook authority proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let closed = closed
            .expect("post-hook cleanup does not panic")
            .expect("post-hook cleanup joins");
        if fatal && evidence.cleanup_read_faults.load(Ordering::SeqCst) != 0 {
            assert!(matches!(closed, Err(AppError::Integration(_))));
        } else {
            closed.unwrap();
        }
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn optional_hook_read_fault_survives_read_only_transactions_until_mutation_preparation() {
    // This is fixture-control coverage with real Lance transactions, not an
    // assertion that App must perform an optional pure read before mutation.
    let evidence = Arc::new(HookEvidence::default());
    let store = HookAdmissionStore {
        inner: StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap(),
        evidence: Arc::clone(&evidence),
    };
    let key = StateKey::new("hook-fixture", "head").unwrap();
    *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), CleanupReadFault::Recoverable));
    evidence
        .cleanup_read_until_mutation
        .store(true, Ordering::SeqCst);
    let mut observation = store.begin_transaction().await.unwrap();
    let snapshot = observation.load(key.clone()).await.unwrap();
    observation.commit().await.unwrap();
    let ordinary = store.load(key.clone()).await;
    assert!(snapshot.is_none());
    assert!(
        matches!(ordinary, Err(StateStoreError::Unavailable { .. })),
        "read-only transaction completion must leave the ordinary-read fault available"
    );
    assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);

    *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), CleanupReadFault::Recoverable));
    evidence
        .cleanup_read_until_mutation
        .store(true, Ordering::SeqCst);
    let mut mutation = store.begin_transaction().await.unwrap();
    let receipt = mutation
        .load(StateKey::new("app-sdk", "receipt/hook-fixture").unwrap())
        .await
        .unwrap();
    let before = mutation.load(key.clone()).await.unwrap();
    mutation
        .put(StateRecord::new(key.clone(), json!({"published":true})))
        .await
        .unwrap();
    mutation.commit().await.unwrap();
    let published = store.load(key).await.unwrap().unwrap();
    assert!(receipt.is_none() && before.is_none());
    assert_eq!(published.record.value.as_ref(), &json!({"published":true}));
    assert_eq!(
        evidence.cleanup_read_faults.load(Ordering::SeqCst),
        1,
        "mutation preparation retires only the optional ordinary-read fault"
    );
    assert!(evidence.cleanup_read.lock().unwrap().is_none());
    assert!(!evidence.cleanup_read_until_mutation.load(Ordering::SeqCst));
}

#[tokio::test]
async fn recoverable_post_hook_read_preserves_the_original_failure_and_concurrent_cancellation() {
    HookCase::post_hook_authority_read(CleanupReadFault::Recoverable, false).await;
}

#[tokio::test]
async fn post_hook_cleanup_keeps_receipts_or_rejects_permanent_authority() {
    HookCase::post_hook_authority_read(CleanupReadFault::Permanent, false).await;
}

#[tokio::test]
async fn post_hook_cleanup_keeps_receipts_or_bounds_a_held_authority_read() {
    HookCase::post_hook_authority_read(CleanupReadFault::Held, false).await;
}

#[tokio::test]
async fn post_hook_cleanup_keeps_receipts_or_the_original_read_budget() {
    HookCase::post_hook_authority_read(CleanupReadFault::Held, true).await;
}

#[tokio::test]
async fn cancelled_post_hook_timeout_retains_its_native_cleanup_receipt() {
    let evidence = Arc::new(HookEvidence::default());
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let mut app = HookCase::app(
        store,
        vec![HookCase::native_definition()],
        &evidence,
        SourceEnding::Complete,
        HookFault::HoldPost { fail: true },
        false,
    )
    .await;
    let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(37), async {
        let workspace = app.create_workspace(WorkspaceConfig {
            name: None, root: std::env::current_dir().unwrap(),
        }).await.unwrap();
        let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
        let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
        let observer = workflow.open_run(RunSelector::Latest(run.id().clone())).await.unwrap();
        let mut finish = Box::pin(run.execution(ExecutionOptions::default()).await.unwrap().finish());
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = evidence.post_entered.notified() => {},
                result = &mut finish => panic!("original post-hook remains held: {}", result.is_ok()),
            }
        }).await.expect("the actual committed action reaches its original post-hook");
        let committed = observer.snapshot().await.unwrap();
        assert_eq!(committed.state.values()["count"], 1);
        assert_eq!(committed.last_action_ordinal, Some(0));
        assert_eq!(committed.outcomes.len(), 1);
        observer.cancel(committed.checkpoint).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(33), &mut finish)
            .await.expect("the original callback has one bounded observation budget");
        assert!(matches!(result, Err(AppError::Timeout(_))),
            "the original callback timeout remains the primary failure");
        drop(finish);
        assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1,
            "the expired original callback is destroyed before its returned result");
        let terminal = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = observer.snapshot().await.unwrap();
                if matches!(snapshot.status, RunStatus::Cancelled) { break snapshot; }
                tokio::task::yield_now().await;
            }
        }).await.expect("original native cancellation settles after expired callback cleanup");
        assert_eq!(terminal.state.values()["count"], 1);
        assert_eq!(terminal.last_action_ordinal, Some(0));
        assert!(terminal.outcomes == committed.outcomes);
        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1);
        for lane in ["sync", "async"] {
            assert_eq!(evidence.calls().iter().filter(|row| row["event"] == "step-finish"
                && row["lane"] == lane).count(), 1,
                "cleanup cannot replay the already observed original post-hook");
        }
        app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
    })).catch_unwind().await;
    evidence.post_release.notify_one();
    let closed = tokio::time::timeout(
        Duration::from_secs(5),
        app.shutdown(Shutdown::CancelAndDrain {
            timeout: Duration::from_secs(3),
        }),
    )
    .await;
    drop(app);
    match checked {
        Ok(result) => result.expect("cancelled post-hook timeout proof is bounded"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
    closed
        .expect("original hook and cancellation owners join")
        .unwrap();
    assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn held_initial_root_authority_read_is_bounded_before_execution_hooks_or_effects() {
    HookCase::assert_execution_read_disposition(
        CleanupReadFault::Held,
        ExecutionReadPhase::FirstPoll,
    )
    .await;
}

impl HookCase {
    async fn registration_authority(phase: RegistrationReadPhase) {
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![Self::native_definition()],
            &evidence,
            SourceEnding::Complete,
            HookFault::RejectRegistration,
            false,
        ))
        .await
        .unwrap();
        let mut fresh = None;
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(38), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut admission = Some(Box::pin(workflow.prepare(WorkflowState::default())));
            tokio::select! {
                _ = evidence.registration_entered.notified() => {},
                result = admission.as_mut().unwrap() => panic!("accepted registration returned before its original callback: {}", result.is_ok()),
            }
            assert_eq!(evidence.names(), ["registered:sync", "registered:async"]);
            let id = evidence.calls()[0]["run"].as_str().unwrap().to_owned();
            let heads = native.list(StateListRequest::new("app-sdk").unwrap()
                .with_key_prefix(StateKeyPrefix::new(format!("run/{}/hooks/{id}", workspace.id())).unwrap())
                .with_limit(2).unwrap()).await.unwrap();
            assert_eq!(heads.records.len(), 1);
            let original = heads.records.into_iter().next().unwrap().record;
            assert_eq!(original.value["sequence"], 0);
            let scope = original.key.key().strip_prefix("run/").unwrap();
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", 0)).unwrap())
                .await.unwrap().unwrap().record;
            assert_eq!(checkpoint.value["checkpoint"]["state"]["count"], 0);
            assert!(checkpoint.value["checkpoint"]["position"].is_null());
            assert!(checkpoint.value["checkpoint"]["outcomes"].as_array().unwrap().is_empty());
            match phase {
                RegistrationReadPhase::Authority(fault) => {
                    *evidence.cleanup_read.lock().unwrap() = Some((original.key.clone(), fault));
                    evidence.cleanup_read_until_mutation.store(true, Ordering::SeqCst);
                }
                RegistrationReadPhase::DroppedReceipt => {
                    *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::Failure);
                }
                RegistrationReadPhase::Superseded => {
                    fresh = Some(Self::app(native.clone(), vec![Self::native_definition()], &evidence,
                        SourceEnding::Complete, HookFault::None, false).await);
                }
            }
            evidence.registration_release.notify_one();
            if matches!(phase, RegistrationReadPhase::DroppedReceipt) {
                tokio::select! {
                    _ = evidence.barrier_entered.notified() => {},
                    result = admission.as_mut().unwrap() => panic!("registration cleanup returned before its accepted native failure receipt: {}", result.is_ok()),
                }
                drop(admission.take());
                assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
                let mut competing = workflow.open_run(RunSelector::Exact { id: id.parse().unwrap(), occurrence: original.value["creation"].as_str().unwrap().to_owned() }).await.unwrap();
                assert!(matches!(competing.execution(ExecutionOptions::default()).await, Err(AppError::Busy(_))),
                    "observer Drop cannot release the accepted registration failure receipt");
                evidence.barrier_release.notify_one();
            } else {
                let early = if matches!(phase, RegistrationReadPhase::Authority(CleanupReadFault::Held)) {
                    tokio::select! {
                        _ = evidence.post_entered.notified() => None,
                        result = admission.as_mut().unwrap() => Some(result),
                    }
                } else { None };
                let result = match early {
                    Some(result) => result,
                    None => tokio::time::timeout(Duration::from_secs(33), admission.as_mut().unwrap()).await
                        .expect("rejected registration retains one bounded authority owner"),
                };
                let error = result.err().expect("the original registration rejection remains primary");
                assert!(error.to_string().contains("explicit caller hook context"));
                drop(admission.take());
            }
            let selected = evidence.cleanup_read_faults.load(Ordering::SeqCst);
            assert!(selected <= 1);
            fatal = selected == 1 && matches!(phase, RegistrationReadPhase::Authority(CleanupReadFault::Permanent | CleanupReadFault::Held));
            assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1 + usize::from(selected == 1 && matches!(phase, RegistrationReadPhase::Authority(CleanupReadFault::Held))),
                "the original callback and any selected held read are destroyed before returning");
            if fatal {
                assert!(matches!(app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await, Err(AppError::Closed(_))));
                assert_eq!(native.load(original.key.clone()).await.unwrap().unwrap().record.value, original.value);
            } else if let Some(fresh) = &fresh {
                let current = fresh.workspace(workspace.id().clone()).await.unwrap()
                    .workflow("hooks".parse().unwrap()).await.unwrap()
                    .open_run(RunSelector::Exact { id: id.parse().unwrap(), occurrence: original.value["creation"].as_str().unwrap().to_owned() }).await.unwrap().snapshot().await.unwrap();
                assert!(matches!(current.status, RunStatus::Paused));
                assert_eq!(current.checkpoint, 0);
                assert_eq!(native.load(original.key.clone()).await.unwrap().unwrap().record.value, original.value,
                    "the old rejected callback cannot publish through a superseded generation");
            } else {
                let runs = workflow.list_runs(None, NonZeroUsize::new(2).unwrap()).await.unwrap();
                assert_eq!(runs.len(), 1);
                assert!(matches!(&runs[0].status, RunStatus::Failed(WorkflowFailure::Hook { cause: None, message }) if message.contains("explicit caller hook context")));
                assert_eq!(runs[0].state.values()["count"], 0);
                assert!(runs[0].outcomes.is_empty() && runs[0].last_action_ordinal.is_none());
                if matches!(phase, RegistrationReadPhase::Authority(_)) && selected == 0 {
                    assert!(!evidence.cleanup_read_until_mutation.load(Ordering::SeqCst),
                        "an unused optional pure read must hand authority directly to the native exact-head mutation");
                    assert!(evidence.cleanup_read.lock().unwrap().is_none());
                }
            }
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            assert_eq!(evidence.names(), ["registered:sync", "registered:async"],
                "registration failure cannot invent execute/action/stream guards or replay registration");
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence
            .cleanup_read_until_mutation
            .store(false, Ordering::SeqCst);
        evidence.registration_release.notify_one();
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let fresh_closed = match fresh.as_mut() {
            Some(app) => Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await,
            ),
            None => None,
        };
        let superseded = fresh.is_some();
        drop(fresh);
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("registration authority case remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(closed) = fresh_closed {
            closed.expect("replacement owner joins").unwrap();
        }
        match closed.expect("original registration owner joins") {
            Ok(()) if !fatal => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(AppError::Integration(_)) if fatal => {}
            outcome => panic!("registration cleanup retained the wrong disposition: {outcome:?}"),
        }
        assert_eq!(
            evidence.owner_dropped.load(Ordering::SeqCst),
            if superseded { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn rejected_registration_retries_required_authority_without_losing_original_failure() {
    HookCase::registration_authority(RegistrationReadPhase::Authority(
        CleanupReadFault::Recoverable,
    ))
    .await;
}

#[tokio::test]
async fn rejected_registration_keeps_receipt_or_fatally_rejects_permanent_authority() {
    HookCase::registration_authority(RegistrationReadPhase::Authority(
        CleanupReadFault::Permanent,
    ))
    .await;
}

#[tokio::test]
async fn rejected_registration_keeps_receipt_or_bounds_held_authority() {
    HookCase::registration_authority(RegistrationReadPhase::Authority(CleanupReadFault::Held))
        .await;
}

#[tokio::test]
async fn dropped_registration_observer_preserves_original_native_failure_receipt() {
    HookCase::registration_authority(RegistrationReadPhase::DroppedReceipt).await;
}

#[tokio::test]
async fn rejected_registration_cannot_publish_after_a_new_app_generation() {
    HookCase::registration_authority(RegistrationReadPhase::Superseded).await;
}

#[derive(Clone, Copy)]
enum SchedulingReceiptCase {
    HeldRead,
    Cancelled,
}

impl HookCase {
    async fn scheduling_receipt(case: SchedulingReceiptCase) {
        let evidence = Arc::new(HookEvidence::default());
        let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let native = inner.clone();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner,
                evidence: Arc::clone(&evidence),
            },
            Self::map_definitions(),
            &evidence,
            SourceEnding::Complete,
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(40), async {
            let workspace = app.create_workspace(WorkspaceConfig { name:None, root:std::env::current_dir().unwrap() }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let leaves = workspace.workflow("leaf".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let parent_id = run.id().to_string();
            *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::ChildIntent);
            let mut finish = Box::pin(run.execution(ExecutionOptions::default()).await.unwrap().finish());
            tokio::select! {
                _ = evidence.barrier_entered.notified() => {},
                result = &mut finish => panic!("original Child intent receipt did not remain owned: {}", result.is_ok()),
            }
            let page = native.list(StateListRequest::new("app-sdk").unwrap()
                .with_key_prefix(StateKeyPrefix::new(format!("run/{}/hooks/{parent_id}", workspace.id())).unwrap())
                .with_limit(2).unwrap()).await.unwrap();
            assert_eq!(page.records.len(), 1);
            let head = page.records.into_iter().next().unwrap().record;
            let observer = workflow.open_run(RunSelector::Exact {
                id: parent_id.parse().unwrap(),
                occurrence: head.value["creation"].as_str().unwrap().parse().unwrap(),
            }).await.unwrap();
            let sequence = head.value["sequence"].as_u64().unwrap();
            let scope = head.key.key().strip_prefix("run/").unwrap();
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{sequence:020}")).unwrap())
                .await.unwrap().unwrap().record;
            let controls = checkpoint.value.pointer("/checkpoint/controls").unwrap().as_object().unwrap();
            assert_eq!(controls.len(), 1);
            let control = controls.values().next().unwrap();
            assert_eq!(control["state"]["kind"], "child");
            assert!(control["state"]["admitted"].as_array().unwrap().is_empty());
            assert_eq!(checkpoint.value.pointer("/checkpoint/state/collected").unwrap(), &json!([]));
            assert!(checkpoint.value.pointer("/checkpoint/outcomes").unwrap().as_array().unwrap().is_empty());
            assert!(leaves.list_runs(None, NonZeroUsize::new(3).unwrap()).await.unwrap().is_empty());
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
            match case {
                SchedulingReceiptCase::HeldRead => {
                    *evidence.cleanup_read.lock().unwrap() = Some((head.key.clone(), CleanupReadFault::Held));
                    evidence.cleanup_read_until_mutation.store(true, Ordering::SeqCst);
                }
                SchedulingReceiptCase::Cancelled => {
                    let cancelled = observer.cancel(sequence).await.unwrap();
                    assert!(matches!(cancelled.status, RunStatus::Cancelling | RunStatus::Cancelled));
                    assert_eq!(cancelled.state.values()["collected"], json!([]));
                }
            }
            evidence.barrier_release.notify_one();
            let returned = tokio::time::timeout(Duration::from_secs(33), async {
                tokio::select! {
                    result = &mut finish => Some(result),
                    _ = async {
                        loop {
                            match app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await {
                                Err(AppError::Closed(_)) => break,
                                Ok(_) => tokio::task::yield_now().await,
                                Err(error) => panic!("unexpected scheduling observation: {error:?}"),
                            }
                        }
                    } => None,
                }
            }).await.expect("an accepted native Child receipt cannot strand its owner on a scheduling read");
            fatal = evidence.cleanup_read_faults.load(Ordering::SeqCst) != 0;
            if fatal {
                assert!(matches!(case, SchedulingReceiptCase::HeldRead));
                assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1,
                    "the selected original read must be destroyed before fatal observation");
                if let Some(result) = returned { assert!(matches!(result, Err(AppError::Closed(_) | AppError::Integration(_)))); }
                assert!(matches!(app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await, Err(AppError::Closed(_))));
                assert_eq!(native.load(head.key.clone()).await.unwrap().unwrap().record.value, head.value);
                assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
            } else {
                let (snapshot, dropped) = match (case, returned.expect("unread fault cannot close the App")) {
                    (SchedulingReceiptCase::Cancelled, Err(AppError::Conflict(_))) => {
                        // The original Park was accepted before independent
                        // cancellation advanced its native fence. Its stale
                        // follow-on may reject while that cancellation settles.
                        let snapshot = tokio::time::timeout(Duration::from_secs(3), observer.wait()).await
                            .expect("accepted cancellation remains owned after the stale receipt returns").unwrap();
                        (snapshot, true)
                    }
                    (_, result) => (result.unwrap(), false),
                };
                assert!(evidence.cleanup_read.lock().unwrap().is_none(),
                    "only a real native mutation may retire the optional pre-admission read fault");
                match case {
                    SchedulingReceiptCase::HeldRead => {
                        assert!(matches!(snapshot.status, RunStatus::Succeeded));
                        assert_eq!(snapshot.state.values()["collected"], json!([11,21]));
                        assert_eq!(snapshot.last_action_ordinal, Some(0));
                        assert_eq!(snapshot.outcomes.len(), 1);
                        assert!(evidence.typing_pre.load(Ordering::SeqCst) > 0);
                        assert_eq!(evidence.calls().iter().filter(|row| row["event"] == "child-finish").count(), 4);
                    }
                    SchedulingReceiptCase::Cancelled => {
                        assert!(matches!(snapshot.status, RunStatus::Cancelled));
                        assert_eq!(snapshot.state.values()["collected"], json!([]));
                        assert!(snapshot.last_action_ordinal.is_none() && snapshot.outcomes.is_empty());
                        assert!(leaves.list_runs(None, NonZeroUsize::new(3).unwrap()).await.unwrap().is_empty());
                        assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
                    }
                }
                let rows = evidence.calls();
                for lane in ["sync", "async"] {
                    for event in ["registered", "execute-start", "execute-finish"] {
                        let expected = usize::from(!(dropped && event == "execute-finish" && lane == "async"));
                        assert_eq!(rows.iter().filter(|row| row["run"] == parent_id && row["event"] == event && row["lane"] == lane).count(), expected,
                            "original {event}/{lane} observation: {rows:?}");
                    }
                }
                app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await.unwrap();
            }
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        })).catch_unwind().await;
        *evidence.cleanup_read.lock().unwrap() = None;
        evidence
            .cleanup_read_until_mutation
            .store(false, Ordering::SeqCst);
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let cleanup = AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(6),
            app.shutdown(if checked.as_ref().is_ok_and(Result::is_ok) && !fatal {
                Shutdown::Drain {
                    timeout: Duration::from_secs(4),
                }
            } else {
                Shutdown::CancelAndDrain {
                    timeout: Duration::from_secs(4),
                }
            }),
        ))
        .catch_unwind()
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("native scheduling receipt case stays bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        let cleanup = cleanup
            .expect("scheduling cleanup cannot panic")
            .expect("terminal scheduling reaches joined cleanup without spinning");
        if fatal {
            assert!(matches!(cleanup, Err(AppError::Integration(_))));
        } else {
            cleanup.unwrap();
            assert_eq!(
                native
                    .load(StateKey::new("app-sdk", "app").unwrap())
                    .await
                    .unwrap()
                    .unwrap()
                    .record
                    .value["run_count"],
                0
            );
        }
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn original_child_intent_receipt_does_not_wait_unboundedly_on_scheduling_authority() {
    HookCase::scheduling_receipt(SchedulingReceiptCase::HeldRead).await;
}

#[tokio::test]
async fn cancelled_child_intent_receipt_preserves_no_effect_and_terminal_driver_quiescence() {
    HookCase::scheduling_receipt(SchedulingReceiptCase::Cancelled).await;
}

impl HookCase {
    async fn sibling_registration_authority(fault: CleanupReadFault) {
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            Self::map_definitions(),
            &evidence,
            SourceEnding::Complete,
            HookFault::ChildRegistrationError,
            false,
        ))
        .await
        .unwrap();
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(39), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let leaves = workspace.workflow("leaf".parse().unwrap()).await.unwrap();
            *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::ChildAdmission);
            let run = workflow.start(ExecutionOptions::default()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), evidence.barrier_entered.notified())
                .await
                .expect("the original native transaction admits both children before registration");
            let page = native
                .list(
                    StateListRequest::new("app-sdk")
                        .unwrap()
                        .with_key_prefix(
                            StateKeyPrefix::new(format!("run/{}/leaf/", workspace.id())).unwrap(),
                        )
                        .with_limit(3)
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(page.records.len(), 2);
            let mut children = Vec::new();
            for head in page.records {
                assert_eq!(head.record.value["sequence"], 0);
                let scope = head.record.key.key().strip_prefix("run/").unwrap();
                let checkpoint = native
                    .load(
                        StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", 0))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .record;
                assert_eq!(checkpoint.value["parent"]["run_id"], run.id().to_string());
                assert!(checkpoint.value["checkpoint"]["position"].is_null());
                assert!(
                    checkpoint.value["checkpoint"]["outcomes"]
                        .as_array()
                        .unwrap()
                        .is_empty()
                );
                children.push((head.record, checkpoint));
            }
            children.sort_by_key(|(_, checkpoint)| {
                checkpoint.value["parent"]["index"].as_u64().unwrap()
            });
            assert_eq!(children[0].1.value["parent"]["index"], 0);
            assert_eq!(children[1].1.value["parent"]["index"], 1);
            assert_eq!(children[0].1.value["checkpoint"]["state"]["item"], 10);
            assert_eq!(children[1].1.value["checkpoint"]["state"]["item"], 20);
            assert!(
                !evidence
                    .calls()
                    .iter()
                    .any(|row| row["event"] == "registered" && row["parent"] == true)
            );
            *evidence.cleanup_read.lock().unwrap() = Some((children[1].0.key.clone(), fault));
            evidence
                .cleanup_read_until_mutation
                .store(true, Ordering::SeqCst);
            evidence.barrier_release.notify_one();
            let returned = tokio::time::timeout(Duration::from_secs(33), async {
                loop {
                    match app
                        .list_workspaces(None, NonZeroUsize::new(1).unwrap())
                        .await
                    {
                        Err(AppError::Closed(_)) => break false,
                        Ok(_) => {}
                        Err(error) => panic!(
                            "sibling registration authority returned unexpected error: {error:?}"
                        ),
                    }
                    // Inner native observation cannot consume the selected
                    // pure-read fault while the original cleanup still owns it.
                    let head = native
                        .load(children[1].0.key.clone())
                        .await
                        .unwrap()
                        .unwrap()
                        .record;
                    if head.value["retention"]["state"] == "retained" {
                        break true;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the later reserved sibling has one bounded cleanup authority owner");
            let selected = evidence.cleanup_read_faults.load(Ordering::SeqCst);
            assert!(selected <= 1);
            fatal = selected == 1
                && matches!(fault, CleanupReadFault::Permanent | CleanupReadFault::Held);
            assert_eq!(
                returned, !fatal,
                "recoverable sibling authority cannot discard original registration cleanup"
            );
            assert_eq!(
                evidence.waits_dropped.load(Ordering::SeqCst),
                usize::from(selected == 1 && matches!(fault, CleanupReadFault::Held)),
                "the held sibling read is destroyed before fatal closure"
            );
            let first_head = native
                .load(children[0].0.key.clone())
                .await
                .unwrap()
                .unwrap()
                .record;
            let first_scope = first_head.key.key().strip_prefix("run/").unwrap();
            let first = native
                .load(
                    StateKey::new(
                        "app-sdk",
                        format!(
                            "history/{first_scope}/checkpoint/{:020}",
                            first_head.value["sequence"].as_u64().unwrap()
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .record;
            assert_eq!(first.value["checkpoint"]["phase"]["state"], "failed");
            assert_eq!(first.value["checkpoint"]["phase"]["category"], "hook");
            assert!(
                first.value["checkpoint"]["phase"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("explicit caller hook context"),
                "later sibling cleanup cannot replace the original registration Hook failure"
            );
            if fatal {
                assert!(matches!(
                    app.list_workspaces(None, NonZeroUsize::new(1).unwrap())
                        .await,
                    Err(AppError::Closed(_))
                ));
                assert_eq!(
                    native
                        .load(children[1].0.key.clone())
                        .await
                        .unwrap()
                        .unwrap()
                        .record
                        .value,
                    children[1].0.value
                );
            } else {
                let parent = tokio::time::timeout(Duration::from_secs(3), run.wait())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(matches!(parent.status, RunStatus::Failed(_)));
                assert_eq!(parent.state.values()["collected"], json!([]));
                assert!(parent.outcomes.is_empty() && parent.last_action_ordinal.is_none());
                let settled = leaves
                    .list_runs(None, NonZeroUsize::new(3).unwrap())
                    .await
                    .unwrap();
                assert_eq!(settled.len(), 2);
                let later = settled
                    .iter()
                    .find(|child| {
                        child.id.to_string() == children[1].1.value["id"].as_str().unwrap()
                    })
                    .unwrap();
                assert!(matches!(
                    later.status,
                    RunStatus::Failed(WorkflowFailure::Child(_))
                ));
                assert_eq!(later.state.values()["item"], 20);
                assert!(later.outcomes.is_empty() && later.last_action_ordinal.is_none());
                if selected == 0 {
                    assert!(evidence.cleanup_read.lock().unwrap().is_none());
                    assert!(
                        !evidence.cleanup_read_until_mutation.load(Ordering::SeqCst),
                        "only original native settlement may retire an unused authority read fault"
                    );
                }
            }
            for (head, checkpoint) in &children {
                assert_eq!(
                    native
                        .load(checkpoint.key.clone())
                        .await
                        .unwrap()
                        .unwrap()
                        .record
                        .value,
                    checkpoint.value
                );
                let id = checkpoint.value["id"].as_str().unwrap();
                let rows = evidence
                    .calls()
                    .into_iter()
                    .filter(|row| row["run"] == id)
                    .collect::<Vec<_>>();
                if head.key == children[0].0.key {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0]["event"], "registered");
                    assert_eq!(rows[0]["lane"], "sync");
                } else {
                    assert!(
                        rows.is_empty(),
                        "an unregistered sibling cannot invent lifecycle callbacks"
                    );
                }
            }
            assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 0);
        }))
        .catch_unwind()
        .await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence
            .cleanup_read_until_mutation
            .store(false, Ordering::SeqCst);
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("sibling registration cleanup is bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed.expect("the original sibling cleanup joins") {
            Ok(()) if !fatal => {}
            Err(AppError::Integration(_)) if fatal => {}
            result => {
                panic!("sibling registration cleanup retained the wrong disposition: {result:?}")
            }
        }
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn later_child_registration_retries_authority_without_replacing_the_original_hook_failure() {
    HookCase::sibling_registration_authority(CleanupReadFault::Recoverable).await;
}

#[tokio::test]
async fn later_child_registration_permanent_authority_closes_before_any_sibling_effect() {
    HookCase::sibling_registration_authority(CleanupReadFault::Permanent).await;
}

#[tokio::test]
async fn later_child_registration_held_authority_is_destroyed_before_fatal_closure() {
    HookCase::sibling_registration_authority(CleanupReadFault::Held).await;
}

#[derive(Clone, Copy)]
enum HaltReceiptCase {
    HeldRead,
    Superseded,
}

impl HookCase {
    async fn halt_receipt_authority(case: HaltReceiptCase) {
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let document = json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
            "entry":"work","nodes":[
                {"id":"work","step":{"kind":"action","action":"local","revision":"r1"}},
                {"id":"after","step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"count","value":{"kind":"literal","value":100}}
                ]}}
            ],"routes":{"work":{"mode":"first","branches":[{"target":{"kind":"node","id":"after"}}]}}
        })
        .to_string();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![WorkflowDefinition::parse(&document).unwrap()],
            &evidence,
            SourceEnding::Complete,
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let mut fresh = None;
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(38), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name: None, root: std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let id = run.id().clone();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{id}/{occurrence}", workspace.id())).unwrap();
            let mut options = Self::options();
            options.halt_after.push("work".into());
            let mut execution = run.execution(options).await.unwrap();
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
            let committed = execution.next().await.unwrap().expect("the original action commits before Halt");
            let ExecutionEvent::Committed { checkpoint: action_checkpoint, outcomes } = committed else {
                panic!("the original action must publish its real committed result");
            };
            assert_eq!(outcomes.len(), 1);
            assert_eq!(outcomes[0].node, "work");
            assert_eq!(outcomes[0].ordinal, 0);
            *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::Halt);
            let mut next = Box::pin(execution.next());
            tokio::select! {
                _ = evidence.barrier_entered.notified() => {},
                result = &mut next => panic!("Halt returned before its original native receipt: {}", result.is_ok()),
            }
            assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 1);
            let head = native.load(key.clone()).await.unwrap().unwrap().record;
            let scope = key.key().strip_prefix("run/").unwrap();
            let halted = native.load(StateKey::new("app-sdk", format!(
                "history/{scope}/checkpoint/{:020}", head.value["sequence"].as_u64().unwrap(),
            )).unwrap()).await.unwrap().unwrap().record;
            assert!(halted.value["checkpoint"]["sequence"].as_u64().unwrap() > action_checkpoint);
            assert_eq!(halted.value["checkpoint"]["admission"]["kind"], "halted");
            assert_eq!(halted.value["checkpoint"]["admission"]["node"], "work");
            assert_eq!(halted.value["checkpoint"]["admission"]["result"], json!(outcomes[0].result));
            assert_eq!(halted.value["checkpoint"]["state"]["count"], 1);
            assert_eq!(halted.value["checkpoint"]["position"]["ordinal"], 0);
            assert_eq!(halted.value["checkpoint"]["outcomes"], json!(outcomes));
            let original_hooks = evidence.names();
            assert!(!original_hooks.iter().any(|name| name.starts_with("execute-finish")));
            drop(next);
            // Only the observer is cancelled; the already committed Halt keeps
            // its original native receipt and execution lease.
            match case {
                HaltReceiptCase::HeldRead => {
                    *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), CleanupReadFault::Held));
                }
                HaltReceiptCase::Superseded => {
                    fresh = Some(Self::app(native.clone(),
                        vec![WorkflowDefinition::parse(&document).unwrap()], &evidence,
                        SourceEnding::Complete, HookFault::None, false).await);
                }
            }
            evidence.barrier_release.notify_one();
            let mut next = Box::pin(execution.next());
            let returned = if matches!(case, HaltReceiptCase::HeldRead) {
                tokio::select! {
                    _ = evidence.post_entered.notified() => None,
                    result = &mut next => Some(result),
                }
            } else { None };
            let result = match returned {
                Some(result) => result,
                None => tokio::time::timeout(Duration::from_secs(33), &mut next).await
                    .expect("the original Halt receipt cannot strand its owner on a pure read"),
            };
            drop(next);
            let selected = evidence.cleanup_read_faults.load(Ordering::SeqCst);
            assert!(selected <= 1);
            fatal = selected == 1;
            assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), selected,
                "a selected original read is destroyed before its publication returns");
            assert_eq!(evidence.names(), original_hooks,
                "observing the original Halt cannot replay guards or start the successor");
            assert_eq!(native.load(key.clone()).await.unwrap().unwrap().record.value, head.value);
            assert_eq!(native.load(halted.key.clone()).await.unwrap().unwrap().record.value, halted.value);
            if matches!(case, HaltReceiptCase::Superseded) {
                assert!(matches!(result, Err(AppError::Closed(_))),
                    "the original native result does not authorize a superseded App observer");
                let current = fresh.as_ref().unwrap().workspace(workspace.id().clone()).await.unwrap()
                    .workflow("hooks".parse().unwrap()).await.unwrap().open_run(RunSelector::Exact { id: id.clone(), occurrence: occurrence.clone() }).await.unwrap()
                    .snapshot().await.unwrap();
                assert!(matches!(current.status, RunStatus::Halted { .. }));
                assert_eq!(current.checkpoint, head.value["sequence"].as_u64().unwrap());
                assert_eq!(current.state.values()["count"], 1);
                assert!(current.outcomes == outcomes);
                drop(execution);
            } else if fatal {
                assert!(result.is_err(), "unreadable authority cannot fabricate an observed Halt");
                assert!(matches!(app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await,
                    Err(AppError::Closed(_))));
                drop(execution);
            } else {
                assert!(matches!(result, Ok(Some(ExecutionEvent::Halted { node, result }))
                    if node == "work" && result.as_ref() == Some(&outcomes[0].result)));
                // The actual immutable Halt and exact returned result above
                // authorize retiring an unused optional pure-read fault before
                // independent finish/snapshot operations may observe the run.
                let unused = evidence.cleanup_read.lock().unwrap().take();
                assert!(matches!(unused, Some((selected, CleanupReadFault::Held)) if selected == key));
                let finished = execution.finish().await.unwrap();
                assert!(matches!(finished.status, RunStatus::Halted { .. }));
                assert_eq!(finished.checkpoint, head.value["sequence"].as_u64().unwrap());
                assert_eq!(finished.state.values()["count"], 1);
                assert_eq!(finished.last_action_ordinal, Some(0));
                assert!(finished.outcomes == outcomes);
            }
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), 1);
            for lane in ["sync", "async"] {
                for event in ["registered", "execute-start", "step-start", "step-finish", "stream-start", "stream-finish"] {
                    assert_eq!(evidence.calls().iter().filter(|row| row["event"] == event && row["lane"] == lane).count(), 1,
                        "the original committed action and its guards stay once-only");
                }
            }
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let superseded = fresh.is_some();
        let fresh_closed = match fresh.as_mut() {
            Some(app) => Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await,
            ),
            None => None,
        };
        drop(fresh);
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("the Halt authority case remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(closed) = fresh_closed {
            closed.expect("replacement owner joins").unwrap();
        }
        match closed.expect("original Halt publication joins") {
            Ok(()) if !fatal => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(AppError::Integration(_)) if fatal => {}
            outcome => panic!("Halt cleanup retained the wrong disposition: {outcome:?}"),
        }
        assert_eq!(
            evidence.owner_dropped.load(Ordering::SeqCst),
            if superseded { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn native_halt_receipt_keeps_its_result_or_bounds_the_required_authority_read() {
    HookCase::halt_receipt_authority(HaltReceiptCase::HeldRead).await;
}

#[tokio::test]
async fn native_halt_receipt_does_not_authorize_a_superseded_app_observer() {
    HookCase::halt_receipt_authority(HaltReceiptCase::Superseded).await;
}

#[derive(Clone, Copy)]
enum PublicationHandoff {
    Retry,
    EndSource,
    Stopping,
    Release,
}

#[derive(Clone, Copy)]
enum PublicationObservation {
    HeldRead,
    Superseded,
    DelayedReceipt,
}

impl HookCase {
    async fn publication_handoff_authority(
        boundary: PublicationHandoff,
        observation: PublicationObservation,
    ) {
        let superseded = matches!(observation, PublicationObservation::Superseded);
        let delayed = matches!(observation, PublicationObservation::DelayedReceipt);
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let document = json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0,"native":0},"required":["count","native"],
            "reducers":{"count":"reject_concurrent","native":"reject_concurrent"},
            "entry":"fork","nodes":[
                {"id":"fork","step":{"kind":"state","mutations":[]}},
                {"id":"a_native","step":{"kind":"state","mutations":[
                    {"kind":"increment","target":"native","value":{"kind":"literal","value":1}}
                ]}},
                {"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}
            ],"routes":{"fork":{"mode":"broadcast","branches":[
                {"target":{"kind":"node","id":"work"}},
                {"target":{"kind":"node","id":"a_native"}}
            ]}}
        })
        .to_string();
        let ending = match boundary {
            PublicationHandoff::Retry => SourceEnding::Error,
            PublicationHandoff::EndSource => SourceEnding::Eof,
            PublicationHandoff::Stopping | PublicationHandoff::Release => SourceEnding::Pending,
        };
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![WorkflowDefinition::parse(&document).unwrap()],
            &evidence,
            ending,
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let mut fresh = None;
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(40), async {
            let workspace = app.create_workspace(WorkspaceConfig {
                name:None, root:std::env::current_dir().unwrap(),
            }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let id = run.id().clone();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{id}/{occurrence}", workspace.id())).unwrap();
            let mut retry_run = workflow.open_run(RunSelector::Exact { id: id.clone(), occurrence: occurrence.clone() }).await.unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            let initial = execution.step().await.unwrap();
            let Some(ExecutionEvent::Committed { outcomes: prefix, .. }) = initial else {
                panic!("the original fork must commit before its mixed wave");
            };
            assert_eq!(prefix.len(), 1);
            assert_eq!(prefix[0].node, "fork");
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
            let staged_head = native.load(key.clone()).await.unwrap().unwrap().record;
            let scope = key.key().strip_prefix("run/").unwrap();
            let staged = native.load(StateKey::new("app-sdk", format!(
                "history/{scope}/checkpoint/{:020}", staged_head.value["sequence"].as_u64().unwrap(),
            )).unwrap()).await.unwrap().unwrap().record;
            assert!(staged.value["checkpoint"]["staged"].as_array().unwrap().iter()
                .any(|activation| activation["node"] == "a_native"),
                "the real native sibling is staged before rollback/readmission");
            let mut execution = match boundary {
                PublicationHandoff::Retry => {
                    let _ = execution.finish().await;
                    let failed = run.snapshot().await.unwrap();
                    assert!(matches!(failed.status, RunStatus::Failed(WorkflowFailure::Action(_))));
                    assert_eq!(failed.state.values()["native"], 0);
                    assert!(failed.outcomes == prefix);
                    assert!(native.load(StateKey::new("app-sdk", format!("active/{scope}")).unwrap())
                        .await.unwrap().is_none(), "original failed capacity settles before explicit retry");
                    None
                }
                PublicationHandoff::EndSource | PublicationHandoff::Stopping | PublicationHandoff::Release => Some(execution),
            };
            let original_hooks = evidence.calls();
            let original_typed = evidence.typing_pre.load(Ordering::SeqCst);
            if matches!(boundary, PublicationHandoff::Stopping | PublicationHandoff::Release) {
                let cancelling = retry_run.cancel(staged_head.value["sequence"].as_u64().unwrap()).await.unwrap();
                assert!(matches!(cancelling.status, RunStatus::Cancelling));
            }
            *evidence.barrier_armed.lock().unwrap() = Some(if matches!(boundary, PublicationHandoff::Stopping | PublicationHandoff::Release) {
                HookCommit::Retained
            } else { HookCommit::Prepared });
            // Each future keeps its original public owner. The Halt fixture
            // separately proves cancellation of only a publication waiter.
            let mut observed = Box::pin(async {
                match boundary {
                    PublicationHandoff::Retry => retry_run.execution(Self::options()).await.map(Some),
                    PublicationHandoff::Release => {
                        execution.take().unwrap().abort().await.map(|_| None)
                    }
                    PublicationHandoff::EndSource | PublicationHandoff::Stopping => {
                        assert!(execution.as_mut().unwrap().next().await?.is_none(),
                            "EOF or cancellation returns without another action");
                        Ok(None)
                    }
                }
            });
            tokio::select! {
                _ = evidence.barrier_entered.notified() => {},
                result = &mut observed => panic!("handoff did not retain its selected native receipt: {}", result.is_ok()),
            }
            let head = native.load(key.clone()).await.unwrap().unwrap().record;
            let checkpoint = native.load(StateKey::new("app-sdk", format!(
                "history/{scope}/checkpoint/{:020}", head.value["sequence"].as_u64().unwrap(),
            )).unwrap()).await.unwrap().unwrap().record;
            assert!(head.value["sequence"].as_u64().unwrap() > staged_head.value["sequence"].as_u64().unwrap());
            assert_eq!(checkpoint.value["checkpoint"]["admission"]["kind"], "prepared");
            assert_eq!(checkpoint.value["checkpoint"]["phase"]["state"],
                if matches!(boundary, PublicationHandoff::Stopping | PublicationHandoff::Release) { "cancelled" } else { "active" });
            if matches!(boundary, PublicationHandoff::Stopping | PublicationHandoff::Release) {
                assert_eq!(head.value.pointer("/retention/state"), Some(&json!("retained")));
                assert!(native.load(StateKey::new("app-sdk", format!("active/{scope}")).unwrap())
                    .await.unwrap().is_none());
                assert_eq!(native.load(StateKey::new("app-sdk", "app").unwrap()).await.unwrap().unwrap().record.value["run_count"], 0);
                for lane in ["sync", "async"] {
                    assert_eq!(evidence.calls().iter().filter(|row|
                        row["event"] == "execute-finish" && row["lane"] == lane).count(), 1,
                        "the original terminal callback finishes before its actual retention receipt");
                }
            }
            assert_eq!(checkpoint.value["checkpoint"]["staged"], json!([]));
            assert_eq!(checkpoint.value["checkpoint"]["state"], json!({"count":0,"native":0}));
            assert_eq!(checkpoint.value["checkpoint"]["outcomes"], json!(prefix));
            assert_eq!(checkpoint.value["checkpoint"]["position"]["ordinal"], 0);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            let committed_hooks = evidence.calls();
            if matches!(boundary, PublicationHandoff::Release) {
                // Abort has already admitted its terminal cleanup publication.
                // Cancel only this consuming observer while that original
                // native receipt is held. Its later public snapshot can never
                // run and therefore cannot consume this pure-publication fault.
                drop(observed);
                assert!(execution.is_none());
                if superseded {
                    fresh = Some(Self::app(native.clone(), vec![WorkflowDefinition::parse(&document).unwrap()],
                        &evidence, ending, HookFault::None, false).await);
                } else {
                    *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), CleanupReadFault::Held));
                }
                evidence.barrier_release.notify_one();
                // The native witness above has zero active inventory and
                // completed original lifecycle callbacks. Drain must still
                // join the accepted publication whose observer was dropped.
                let drained = tokio::time::timeout(Duration::from_secs(35), app.shutdown(
                    Shutdown::Drain { timeout:Duration::from_secs(33) },
                )).await.expect("the original Release publication cannot outlive its pure-read bound");
                let selected = evidence.cleanup_read_faults.load(Ordering::SeqCst);
                assert!(selected <= 1);
                fatal = selected == 1;
                assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), selected,
                    "a selected Release read is destroyed before joined shutdown");
                match drained {
                    Ok(()) if !fatal && !superseded => {},
                    Err(AppError::Closed(_)) if superseded => {},
                    Err(AppError::Integration(_)) if fatal => {},
                    result => panic!("the original Release publication retained the wrong cleanup result: {result:?}"),
                }
                assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
                assert_eq!(native.load(key.clone()).await.unwrap().unwrap().record.value, head.value);
                assert_eq!(evidence.calls(), committed_hooks);
                assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), original_typed);
                assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
                if !superseded && !fatal {
                    let unused = evidence.cleanup_read.lock().unwrap().take();
                    assert!(matches!(unused, Some((selected, CleanupReadFault::Held)) if selected == key),
                        "the native terminal receipt and joined original publication authorize retiring an eliminated read");
                }
                return;
            }
            if superseded {
                fresh = Some(Self::app(native.clone(), vec![WorkflowDefinition::parse(&document).unwrap()],
                    &evidence, ending, HookFault::None, false).await);
            } else if delayed {
                // This original native commit already applied. Its retained
                // receipt may outlive the earlier callback/read cleanup pass.
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(31)) => {},
                    result = &mut observed => panic!("accepted native receipt stopped being owned: {}", result.is_ok()),
                }
                assert_eq!(evidence.calls(), committed_hooks);
                assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            } else {
                *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), CleanupReadFault::Held));
            }
            evidence.barrier_release.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(if superseded {3} else {33}), &mut observed)
                .await.expect("the original publication result cannot be stranded by a post-native pure read");
            drop(observed);
            let selected = evidence.cleanup_read_faults.load(Ordering::SeqCst);
            assert!(selected <= 1);
            fatal = selected == 1;
            assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), selected,
                "the selected original read is destroyed before its owner returns");
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            assert_eq!(native.load(key.clone()).await.unwrap().unwrap().record.value, head.value);
            assert_eq!(evidence.calls(), committed_hooks,
                "returning the native result cannot open another action or replay closed guards");
            assert_eq!(evidence.typing_pre.load(Ordering::SeqCst), original_typed);
            assert!(original_hooks.len() <= committed_hooks.len());
            if superseded {
                assert!(matches!(result, Err(AppError::Closed(_))), "old native authority cannot cross a real App generation claim");
                let current = fresh.as_ref().unwrap().workspace(workspace.id().clone()).await.unwrap()
                    .workflow("hooks".parse().unwrap()).await.unwrap().open_run(RunSelector::Exact { id, occurrence: occurrence.clone() }).await.unwrap()
                    .snapshot().await.unwrap();
                assert!(if matches!(boundary, PublicationHandoff::Stopping | PublicationHandoff::Release) {
                    matches!(current.status, RunStatus::Cancelled)
                } else { matches!(current.status, RunStatus::Paused) });
                assert_eq!(current.checkpoint, head.value["sequence"].as_u64().unwrap());
                assert!(current.outcomes == prefix);
            } else if fatal {
                assert!(result.is_err(), "unreadable authority cannot invent successful EOF or retry admission");
                assert!(matches!(app.list_workspaces(None, NonZeroUsize::new(1).unwrap()).await, Err(AppError::Closed(_))));
            } else {
                let execution = result.expect("the original native handoff remains usable")
                    .or_else(|| execution.take()).unwrap();
                let unused = evidence.cleanup_read.lock().unwrap().take();
                if delayed {
                    assert!(unused.is_none(), "native receipt latency does not require a synthetic read fault");
                } else {
                    assert!(matches!(unused, Some((selected, CleanupReadFault::Held)) if selected == key),
                        "only the witnessed original native result may retire an eliminated read");
                }
                let finished = execution.abort().await.unwrap();
                assert!(if matches!(boundary, PublicationHandoff::Stopping | PublicationHandoff::Release) {
                    matches!(finished.status, RunStatus::Cancelled)
                } else { matches!(finished.status, RunStatus::Paused) });
                assert_eq!(finished.state.values()["count"], 0);
                assert_eq!(finished.state.values()["native"], 0);
                assert!(finished.outcomes == prefix);
            }
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            for node in ["fork", "a_native", "work"] {
                for lane in ["sync", "async"] {
                    assert_eq!(evidence.calls().iter().filter(|row|
                        row["event"] == "step-start" && row["node"] == node && row["lane"] == lane).count(), 1);
                    assert!(evidence.calls().iter().filter(|row|
                        row["event"] == "step-finish" && row["node"] == node && row["lane"] == lane).count() <= 1,
                        "failure cleanup may close a remaining guard once, never replay it");
                }
            }
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence.barrier_armed.lock().unwrap().take();
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let fresh_closed = match fresh.as_mut() {
            Some(app) => Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await,
            ),
            None => None,
        };
        drop(fresh);
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("publication handoff proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(closed) = fresh_closed {
            closed.expect("replacement App joins").unwrap();
        }
        match closed.expect("original publication and source cleanup join") {
            Ok(()) if !fatal => {}
            Err(AppError::Closed(_)) if superseded => {}
            Err(AppError::Integration(_)) if fatal => {}
            result => panic!("publication cleanup retained the wrong disposition: {result:?}"),
        }
        assert_eq!(
            evidence.owner_dropped.load(Ordering::SeqCst),
            if superseded { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn native_failed_retry_handoff_preserves_its_receipt_or_bounds_its_read() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::Retry,
        PublicationObservation::HeldRead,
    )
    .await;
}

#[tokio::test]
async fn native_eof_handoff_preserves_its_receipt_or_bounds_its_read() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::EndSource,
        PublicationObservation::HeldRead,
    )
    .await;
}

#[tokio::test]
async fn native_failed_retry_handoff_rejects_a_superseding_app_generation() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::Retry,
        PublicationObservation::Superseded,
    )
    .await;
}

#[tokio::test]
async fn native_eof_handoff_rejects_a_superseding_app_generation() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::EndSource,
        PublicationObservation::Superseded,
    )
    .await;
}

#[tokio::test]
async fn native_cancelled_retention_handoff_preserves_its_receipt_or_bounds_its_read() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::Stopping,
        PublicationObservation::HeldRead,
    )
    .await;
}

#[tokio::test]
async fn native_cancelled_retention_handoff_rejects_a_superseding_app_generation() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::Stopping,
        PublicationObservation::Superseded,
    )
    .await;
}

#[tokio::test]
async fn dropped_abort_observer_keeps_its_native_release_receipt_or_bounds_its_read() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::Release,
        PublicationObservation::HeldRead,
    )
    .await;
}

#[tokio::test]
async fn superseded_dropped_abort_receipt_joins_original_and_replacement_owners() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::Release,
        PublicationObservation::Superseded,
    )
    .await;
}

#[tokio::test]
async fn native_eof_receipt_latency_does_not_expire_its_later_generation_observation() {
    HookCase::publication_handoff_authority(
        PublicationHandoff::EndSource,
        PublicationObservation::DelayedReceipt,
    )
    .await;
}

#[derive(Clone, Copy)]
enum SettlementReadBoundary {
    DroppedSource,
    DroppedSourceMarker,
    DroppedNativeStage,
    BufferedAbort,
    PendingStageAbort,
}

impl HookCase {
    async fn settlement_read_authority(boundary: SettlementReadBoundary) {
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let native_failure = matches!(boundary, SettlementReadBoundary::DroppedNativeStage);
        let buffered = matches!(
            boundary,
            SettlementReadBoundary::BufferedAbort | SettlementReadBoundary::PendingStageAbort
        );
        let pending_stage = matches!(boundary, SettlementReadBoundary::PendingStageAbort);
        let definition = if native_failure {
            Self::native_prefix_definition()
        } else {
            WorkflowDefinition::parse(&json!({
                "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
                "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
                "entry":"work","nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],"routes":{}
            }).to_string()).unwrap()
        };
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![definition],
            &evidence,
            if pending_stage {
                SourceEnding::Error
            } else {
                SourceEnding::DropPanic
            },
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(39), async {
            let workspace = app.create_workspace(WorkspaceConfig { name:None, root:std::env::current_dir().unwrap() }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence)).unwrap();
            let scope = key.key().strip_prefix("run/").unwrap();
            let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            let prefix = if native_failure {
                let Some(ExecutionEvent::Committed { outcomes, .. }) = execution.step().await.unwrap() else {
                    panic!("original fork must commit before native failure");
                };
                assert_eq!(outcomes.len(), 1);
                assert_eq!(outcomes[0].node, "fork");
                *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::NativePrefix);
                let mut next = Box::pin(execution.next());
                tokio::select! {
                    _ = evidence.barrier_entered.notified() => {},
                    result = &mut next => panic!("native Stage receipt did not remain owned: {}", result.is_ok()),
                }
                drop(next);
                outcomes
            } else {
                assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
                Vec::new()
            };
            *evidence.barrier_armed.lock().unwrap() = Some(if buffered { HookCommit::SourceFailureStage } else { HookCommit::Failure });
            if pending_stage {
                // Cancel only this waiter after the original source error's
                // Stage has committed. Abort must join that same publication.
                let mut next = Box::pin(execution.next());
                tokio::select! {
                    _ = evidence.barrier_entered.notified() => {},
                    result = &mut next => panic!("source failure Stage must retain its native receipt: {}", result.is_ok()),
                }
                drop(next);
            }
            let mut closing = if buffered { Some(Box::pin(execution.abort())) } else {
                assert!(std::panic::catch_unwind(AssertUnwindSafe(|| drop(execution))).is_ok());
                None
            };
            if native_failure { evidence.barrier_release.notify_one(); }
            if let Some(closing) = closing.as_mut() {
                if !pending_stage { tokio::select! {
                    _ = evidence.barrier_entered.notified() => {},
                    result = closing => panic!("buffered failure Stage receipt did not remain owned: {}", result.is_ok()),
                } } else {
                    std::future::poll_fn(|cx| {
                        assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending(),
                            "abort retains the original pending Stage receipt");
                        Poll::Ready(())
                    }).await;
                }
            } else {
                evidence.barrier_entered.notified().await;
            }
            let head = native.load(key.clone()).await.unwrap().unwrap().record;
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", head.value["sequence"].as_u64().unwrap())).unwrap())
                .await.unwrap().unwrap().record;
            let saved = &checkpoint.value["checkpoint"];
            let category = if native_failure { "state" } else if pending_stage { "action" } else { "execution" };
            if buffered {
                assert!(saved["staged"].as_array().unwrap().iter().any(|row|
                    row["node"] == "work" && row["outcome"]["state"] == "failed"
                        && row["outcome"]["category"] == category));
            } else {
                assert_eq!(saved["phase"]["state"], "failed");
                assert_eq!(saved["phase"]["category"], category);
            }
            assert_eq!(saved["state"]["count"], 0);
            assert_eq!(saved["outcomes"], json!(prefix));
            assert_eq!(saved["position"], if native_failure { json!({"node":"fork","ordinal":0}) } else { Value::Null });
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), usize::from(!native_failure));
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), usize::from(!native_failure));
            let selected_key = if matches!(boundary, SettlementReadBoundary::DroppedSourceMarker) {
                marker.clone()
            } else { key.clone() };
            *evidence.cleanup_read.lock().unwrap() = Some((selected_key.clone(), CleanupReadFault::Held));
            // If close reuses its actual Stage receipt, the next native Failed
            // mutation retires this optional pre-mutation read. The consuming
            // caller's later public snapshot can never receive the test fault.
            evidence.cleanup_read_until_mutation.store(buffered, Ordering::SeqCst);
            evidence.barrier_release.notify_one();
            if let Some(closing) = closing.take() {
                let result = tokio::time::timeout(Duration::from_secs(33), closing).await
                    .expect("buffered close cannot strand the original native failure on a pure read");
                fatal = evidence.cleanup_read_faults.load(Ordering::SeqCst) != 0;
                if fatal { assert!(result.is_err()); } else {
                    let final_row = result.expect("completed source failure remains observable");
                    assert!(if pending_stage {
                        matches!(final_row.status, RunStatus::Failed(WorkflowFailure::Action(_)))
                    } else {
                        matches!(final_row.status, RunStatus::Failed(WorkflowFailure::Execution(_)))
                    });
                    assert_eq!(final_row.state.values()["count"], 0);
                    assert!(final_row.outcomes.is_empty() && final_row.last_action_ordinal.is_none());
                }
            } else {
                // Watch native capacity only: a public snapshot or shutdown must
                // not steal the fault from the dropped Execution's cleanup.
                tokio::time::timeout(Duration::from_secs(33), async {
                    loop {
                        if evidence.cleanup_read_faults.load(Ordering::SeqCst) != 0 { fatal = true; break; }
                        if native.load(marker.clone()).await.unwrap().is_none() { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.expect("the original failure cleanup reaches a read or retained capacity");
            }
            if !fatal {
                let unused = evidence.cleanup_read.lock().unwrap().take();
                if buffered {
                    assert!(unused.is_none() && !evidence.cleanup_read_until_mutation.load(Ordering::SeqCst),
                        "the original Failed native mutation retires an eliminated Stage read before the public snapshot");
                    let final_head = native.load(key.clone()).await.unwrap().unwrap().record;
                    let final_record = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", final_head.value["sequence"].as_u64().unwrap())).unwrap()).await.unwrap().unwrap().record;
                    assert_eq!(final_record.value["checkpoint"]["phase"]["state"], "failed");
                    assert_eq!(final_record.value["checkpoint"]["phase"]["category"], category);
                    for field in ["state", "outcomes", "position"] {
                        assert_eq!(final_record.value["checkpoint"][field], saved[field],
                            "Failed publication preserves the original uncommitted boundary");
                    }
                } else {
                    assert!(matches!(unused, Some((selected, CleanupReadFault::Held)) if selected == selected_key),
                        "only original failure completion may retire an eliminated read");
                }
            }
            let closed = tokio::time::timeout(Duration::from_secs(35), app.shutdown(Shutdown::Drain { timeout:Duration::from_secs(33) }))
                .await.expect("failure cleanup retains a finite pure-read boundary");
            match closed { Ok(()) if !fatal => {}, Err(AppError::Integration(_)) if fatal => {},
                other => panic!("failure cleanup returned the wrong disposition: {other:?}") }
            assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), usize::from(fatal));
            assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), usize::from(fatal), "joined cleanup destroys the original selected read");
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            if !buffered || fatal {
                let final_head = native.load(key.clone()).await.unwrap().unwrap().record;
                assert_eq!(final_head.value["sequence"], head.value["sequence"],
                    "retention cannot replace the selected Failed result, and fatal Stage read loss cannot advance it");
            }
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), usize::from(!native_failure));
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), usize::from(!native_failure));
            let calls = evidence.calls();
            for lane in ["sync", "async"] {
                assert!(calls.iter().filter(|row| row["lane"] == lane && row["event"] == "execute-finish").count() <= 1,
                    "the original execution completion cannot replay");
                let nodes = if native_failure { vec!["fork", "a_native", "b_native"] } else { vec!["work"] };
                for node in nodes {
                    for event in ["step-start", "step-finish", "stream-start", "stream-finish"] {
                        assert!(calls.iter().filter(|row| row["lane"] == lane && row["event"] == event && row["node"] == node).count() <= 1,
                            "original native and source guards cannot replay after their failure receipt");
                    }
                }
            }
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence
            .cleanup_read_until_mutation
            .store(false, Ordering::SeqCst);
        evidence.barrier_armed.lock().unwrap().take();
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("settlement read proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed.expect("original failure owner joins") {
            Ok(()) if !fatal => {}
            Err(AppError::Integration(_)) if fatal => {}
            other => panic!("failure owner did not retain cleanup disposition: {other:?}"),
        }
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn dropped_source_failure_retains_native_result_or_bounds_cleanup_read() {
    HookCase::settlement_read_authority(SettlementReadBoundary::DroppedSource).await;
}

#[tokio::test]
async fn dropped_native_stage_failure_retains_native_result_or_bounds_cleanup_read() {
    HookCase::settlement_read_authority(SettlementReadBoundary::DroppedNativeStage).await;
}

#[tokio::test]
async fn buffered_source_failure_abort_retains_stage_or_bounds_cleanup_read() {
    HookCase::settlement_read_authority(SettlementReadBoundary::BufferedAbort).await;
}

#[tokio::test]
async fn dropped_source_failure_bounds_required_capacity_marker_observation() {
    HookCase::settlement_read_authority(SettlementReadBoundary::DroppedSourceMarker).await;
}

impl HookCase {
    async fn background_terminal_generation(superseded: bool) {
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let document = json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
            "entry":"sleep","nodes":[{"id":"sleep","deadline_ms":100,
                "step":{"kind":"sleep","delay_ms":1000,"message":"wait for the native deadline"}}],"routes":{}
        }).to_string();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![WorkflowDefinition::parse(&document).unwrap()],
            &evidence,
            SourceEnding::Complete,
            HookFault::HoldExecuteFinished,
            false,
        ))
        .await
        .unwrap();
        let mut fresh = None;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(39), async {
            let workspace = app
                .create_workspace(WorkspaceConfig {
                    name: None,
                    root: std::env::current_dir().unwrap(),
                })
                .await
                .unwrap();
            let run = workspace
                .workflow("hooks".parse().unwrap())
                .await
                .unwrap()
                .start(Self::options())
                .await
                .unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            evidence.post_entered.notified().await;
            let key = StateKey::new(
                "app-sdk",
                format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence),
            )
            .unwrap();
            let scope = key.key().strip_prefix("run/").unwrap();
            let head = native.load(key.clone()).await.unwrap().unwrap().record;
            let checkpoint = native
                .load(
                    StateKey::new(
                        "app-sdk",
                        format!(
                            "history/{scope}/checkpoint/{:020}",
                            head.value["sequence"].as_u64().unwrap()
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .record;
            let saved = &checkpoint.value["checkpoint"];
            assert_eq!(saved["phase"]["state"], "failed");
            assert_eq!(saved["phase"]["category"], "timeout");
            assert_eq!(saved["state"], json!({"count":0}));
            assert_eq!(saved["outcomes"], json!([]));
            assert_eq!(saved["position"], Value::Null);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
            for lane in ["sync", "async"] {
                assert_eq!(
                    evidence
                        .calls()
                        .iter()
                        .filter(|row| row["event"] == "execute-finish" && row["lane"] == lane)
                        .count(),
                    1
                );
            }
            if superseded {
                fresh = Some(
                    Self::app(
                        native.clone(),
                        vec![WorkflowDefinition::parse(&document).unwrap()],
                        &evidence,
                        SourceEnding::Complete,
                        HookFault::None,
                        false,
                    )
                    .await,
                );
            } else {
                *evidence.cleanup_read.lock().unwrap() = Some((
                    StateKey::new("app-sdk", "app").unwrap(),
                    CleanupReadFault::Held,
                ));
            }
            evidence.post_release.notify_one();
            if !superseded {
                // Observe the selected original owner before any public App
                // operation can consume its generation-read fault.
                evidence.post_entered.notified().await;
                assert_eq!(
                    evidence.waits_dropped.load(Ordering::SeqCst),
                    1,
                    "the original callback finished before this read began"
                );
            }
            let closed = tokio::time::timeout(
                Duration::from_secs(35),
                app.shutdown(Shutdown::Drain {
                    timeout: Duration::from_secs(33),
                }),
            )
            .await
            .expect("background terminal observation and original cleanup must join");
            if superseded {
                assert!(matches!(closed, Err(AppError::Closed(_))));
                let observer = fresh
                    .as_ref()
                    .unwrap()
                    .workspace(workspace.id().clone())
                    .await
                    .unwrap()
                    .workflow("hooks".parse().unwrap())
                    .await
                    .unwrap()
                    .open_run(RunSelector::Exact {
                        id: run.id().clone(),
                        occurrence: occurrence.clone(),
                    })
                    .await
                    .unwrap();
                let snapshot = observer.snapshot().await.unwrap();
                assert!(matches!(
                    snapshot.status,
                    RunStatus::Failed(WorkflowFailure::Timeout(_))
                ));
                assert_eq!(snapshot.state.values()["count"], 0);
                assert!(snapshot.outcomes.is_empty() && snapshot.last_action_ordinal.is_none());
            } else {
                assert!(
                    matches!(closed, Err(AppError::Integration(_))),
                    "required unreadable terminal authority must not silently complete"
                );
                assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);
                assert_eq!(
                    evidence.waits_dropped.load(Ordering::SeqCst),
                    2,
                    "joined fatal cleanup destroys the actual post-callback read"
                );
            }
            assert_eq!(
                native
                    .load(checkpoint.key.clone())
                    .await
                    .unwrap()
                    .unwrap()
                    .record
                    .value,
                checkpoint.value
            );
            assert_eq!(
                native
                    .load(key.clone())
                    .await
                    .unwrap()
                    .unwrap()
                    .record
                    .value["sequence"],
                head.value["sequence"]
            );
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 0);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 0);
        }))
        .catch_unwind()
        .await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence.post_release.notify_one();
        let fresh_closed = match fresh.as_mut() {
            Some(app) => Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    app.shutdown(Shutdown::CancelAndDrain {
                        timeout: Duration::from_secs(3),
                    }),
                )
                .await,
            ),
            None => None,
        };
        drop(fresh);
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("background generation proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        if let Some(closed) = fresh_closed {
            closed.expect("replacement generation joins").unwrap();
        }
        match closed.expect("original background execution joins") {
            Err(AppError::Closed(_)) if superseded => {}
            Err(AppError::Integration(_)) if !superseded => {}
            other => panic!("background cleanup returned the wrong disposition: {other:?}"),
        }
        assert_eq!(
            evidence.owner_dropped.load(Ordering::SeqCst),
            if superseded { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn background_terminal_callback_requires_bounded_generation_observation() {
    HookCase::background_terminal_generation(false).await;
}

#[tokio::test]
async fn background_terminal_callback_and_superseding_generation_join_original_owners() {
    HookCase::background_terminal_generation(true).await;
}
#[tokio::test]
async fn pending_source_failure_stage_abort_retains_original_receipt_or_bounds_cleanup_read() {
    HookCase::settlement_read_authority(SettlementReadBoundary::PendingStageAbort).await;
}

impl HookCase {
    async fn rejected_stopping_read_authority() {
        let evidence = Arc::new(HookEvidence::default());
        let native = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let definition = WorkflowDefinition::parse(&json!({
            "schema":"app-sdk.workflow/v2","id":"hooks","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
            "entry":"work","nodes":[{"id":"work","step":{"kind":"action","action":"local","revision":"r1"}}],"routes":{}
        }).to_string()).unwrap();
        let mut app = App::open(Self::config(
            HookAdmissionStore {
                inner: native.clone(),
                evidence: Arc::clone(&evidence),
            },
            vec![definition],
            &evidence,
            SourceEnding::Pending,
            HookFault::None,
            false,
        ))
        .await
        .unwrap();
        let mut fatal = false;
        let checked = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(39), async {
            let workspace = app.create_workspace(WorkspaceConfig { name:None, root:std::env::current_dir().unwrap() }).await.unwrap();
            let workflow = workspace.workflow("hooks".parse().unwrap()).await.unwrap();
            let mut run = workflow.prepare(WorkflowState::default()).await.unwrap();
            let occurrence = run.snapshot().await.unwrap().occurrence;
            let observer = workflow.open_run(RunSelector::Exact { id: run.id().clone(), occurrence: occurrence.clone() }).await.unwrap();
            let key = StateKey::new("app-sdk", format!("run/{}/hooks/{}/{}", workspace.id(), run.id(), occurrence)).unwrap();
            let scope = key.key().strip_prefix("run/").unwrap();
            let marker = StateKey::new("app-sdk", format!("active/{scope}")).unwrap();
            let mut execution = run.execution(Self::options()).await.unwrap();
            assert!(matches!(execution.next().await.unwrap(), Some(ExecutionEvent::Partial { .. })));
            let before = native.load(key.clone()).await.unwrap().unwrap().record;
            let cancelling = observer.cancel(before.value["sequence"].as_u64().unwrap()).await.unwrap();
            assert!(matches!(cancelling.status, RunStatus::Cancelling));
            let head = native.load(key.clone()).await.unwrap().unwrap().record;
            let checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", head.value["sequence"].as_u64().unwrap())).unwrap())
                .await.unwrap().unwrap().record;
            let saved = &checkpoint.value["checkpoint"];
            assert_eq!(saved["phase"]["state"], "cancelling");
            assert_eq!(saved["controls"], json!({}));
            assert_eq!(saved["state"]["count"], 0);
            assert_eq!(saved["outcomes"], json!([]));
            assert!(saved["position"].is_null());
            *evidence.barrier_armed.lock().unwrap() = Some(HookCommit::RejectedCancellation);
            let mut closing = Box::pin(execution.abort());
            tokio::select! {
                _ = evidence.barrier_entered.notified() => {},
                result = &mut closing => panic!("original cancellation must reach its terminal native write: {}", result.is_ok()),
            }
            assert!(evidence.rejected.load(Ordering::SeqCst));
            assert_eq!(evidence.barrier_commits.load(Ordering::SeqCst), 0,
                "the held terminal candidate has no applied native receipt");
            assert_eq!(native.load(key.clone()).await.unwrap().unwrap().record.value, head.value);
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            *evidence.cleanup_read.lock().unwrap() = Some((key.clone(), CleanupReadFault::Held));
            evidence.barrier_release.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(33), &mut closing).await
                .expect("a rejected terminal write cannot strand its original cancellation on a pure read");
            drop(closing);
            fatal = evidence.cleanup_read_faults.load(Ordering::SeqCst) != 0;
            assert!(result.is_err(), "definitely rejected or unreadable terminal authority cannot report successful abort");
            if fatal {
                assert_eq!(evidence.cleanup_read_faults.load(Ordering::SeqCst), 1);
                assert_eq!(evidence.waits_dropped.load(Ordering::SeqCst), 1);
                assert_eq!(native.load(key.clone()).await.unwrap().unwrap().record.value, head.value,
                    "unreadable stopping authority cannot publish a fabricated terminal result");
            } else {
                // A future reduction may propagate the original rejected write
                // directly. Only real completed cancellation can retire the
                // optional observation before an independent public snapshot.
                tokio::time::timeout(Duration::from_secs(3), async {
                    while native.load(marker.clone()).await.unwrap().is_some() { tokio::task::yield_now().await; }
                }).await.expect("the original cancellation still owns terminal capacity cleanup");
                let final_head = native.load(key.clone()).await.unwrap().unwrap().record;
                let final_checkpoint = native.load(StateKey::new("app-sdk", format!("history/{scope}/checkpoint/{:020}", final_head.value["sequence"].as_u64().unwrap())).unwrap()).await.unwrap().unwrap().record;
                assert_eq!(final_checkpoint.value["checkpoint"]["phase"]["state"], "cancelled");
                for field in ["state", "outcomes", "position"] {
                    assert_eq!(final_checkpoint.value["checkpoint"][field], saved[field]);
                }
                assert!(matches!(evidence.cleanup_read.lock().unwrap().take(), Some((selected, CleanupReadFault::Held)) if selected == key));
            }
            assert_eq!(native.load(checkpoint.key.clone()).await.unwrap().unwrap().record.value, checkpoint.value);
            let closed = tokio::time::timeout(Duration::from_secs(35), app.shutdown(Shutdown::CancelAndDrain { timeout:Duration::from_secs(33) }))
                .await.expect("rejected cancellation cleanup joins original native and read owners");
            match closed { Ok(()) if !fatal => {}, Err(AppError::Integration(_)) if fatal => {},
                other => panic!("rejected cancellation retained the wrong authority disposition: {other:?}") }
            assert_eq!(evidence.constructed.load(Ordering::SeqCst), 1);
            assert_eq!(evidence.source_dropped.load(Ordering::SeqCst), 1);
            for lane in ["sync", "async"] {
                let calls = evidence.calls();
                assert!(calls.iter().filter(|row| row["event"] == "execute-finish" && row["lane"] == lane).count() <= 1);
                for event in ["step-start", "step-finish", "stream-start", "stream-finish"] {
                    assert!(calls.iter().filter(|row| row["event"] == event && row["node"] == "work" && row["lane"] == lane).count() <= 1);
                }
            }
        })).catch_unwind().await;
        evidence.cleanup_read.lock().unwrap().take();
        evidence.barrier_armed.lock().unwrap().take();
        evidence.post_release.notify_one();
        evidence.barrier_release.notify_one();
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            app.shutdown(Shutdown::CancelAndDrain {
                timeout: Duration::from_secs(3),
            }),
        )
        .await;
        drop(app);
        match checked {
            Ok(result) => result.expect("rejected stopping authority proof remains bounded"),
            Err(panic) => std::panic::resume_unwind(panic),
        }
        match closed.expect("the original stopping owner joins") {
            Ok(()) if !fatal => {}
            Err(AppError::Integration(_)) if fatal => {}
            other => panic!("stopping owner did not retain cleanup disposition: {other:?}"),
        }
        assert_eq!(evidence.owner_dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn rejected_terminal_cancellation_bounds_original_settlement_read() {
    HookCase::rejected_stopping_read_authority().await;
}
