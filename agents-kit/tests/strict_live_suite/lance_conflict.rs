use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use agents_kit::config::{AgentConfig, OpenCodeConfig};
use agents_kit::prompt::{PromptEvent, PromptRequest};
use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use storage_kit::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_kit::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tokio::sync::Semaphore;

use super::fixture::StrictFixture;
use crate::native_cleanup::NativeCleanup;
use crate::opencode_question_evidence::QuestionExpectation;

const PROMPTS: &str = "agents_kit_opencode_prompts_v1";
const EVENTS: &str = "agents_kit_opencode_event_progress_v1";

struct LanceOverlap {
    armed: AtomicBool,
    held: AtomicBool,
    event_entered: Semaphore,
    release: Semaphore,
    event_commits: AtomicUsize,
    foreground_loads: AtomicUsize,
    foreground_claimed: AtomicBool,
    foreground_entered: Semaphore,
    foreground_release: Semaphore,
    event_committed: Semaphore,
    foreground_conflicts: AtomicUsize,
    prepared_attempts: std::sync::Mutex<Vec<String>>,
    prepared_commits: std::sync::Mutex<BTreeSet<(String, String)>>,
}

impl LanceOverlap {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            held: AtomicBool::new(false),
            event_entered: Semaphore::new(0),
            release: Semaphore::new(0),
            event_commits: AtomicUsize::new(0),
            foreground_loads: AtomicUsize::new(0),
            foreground_claimed: AtomicBool::new(false),
            foreground_entered: Semaphore::new(0),
            foreground_release: Semaphore::new(0),
            event_committed: Semaphore::new(0),
            foreground_conflicts: AtomicUsize::new(0),
            prepared_attempts: std::sync::Mutex::new(Vec::new()),
            prepared_commits: std::sync::Mutex::new(BTreeSet::new()),
        }
    }

    fn release(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.release.add_permits(1);
        self.foreground_release.add_permits(1);
    }
}

#[derive(Clone)]
struct OverlappingLance {
    inner: StateStore,
    overlap: Arc<LanceOverlap>,
}

struct OverlappingTransaction {
    inner: StateTransaction,
    overlap: Arc<LanceOverlap>,
    event: bool,
    held: bool,
    prepared: Vec<(String, String)>,
    foreground: bool,
}

impl StateStoreBackend for OverlappingLance {
    type Transaction = OverlappingTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }
    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }
    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(OverlappingTransaction {
            inner: self.inner.begin_transaction().await?,
            overlap: Arc::clone(&self.overlap),
            event: false,
            held: false,
            prepared: Vec::new(),
            foreground: false,
        })
    }
}

impl StateTransactionBackend for OverlappingTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.event |= key.namespace().as_str() == EVENTS;
        let foreground = !self.event
            && key.namespace().as_str() == PROMPTS
            && self.overlap.held.load(Ordering::SeqCst);
        if foreground {
            self.overlap.foreground_loads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let prompt = record.key.namespace().as_str() == PROMPTS;
        let prepared = if prompt && record.value["phase"]["phase"] == "prepared" {
            let generation = record.value["message_id"].as_str().unwrap().to_owned();
            self.overlap
                .prepared_attempts
                .lock()
                .unwrap()
                .push(generation.clone());
            Some((
                generation,
                record.value["request_part_id"].as_str().unwrap().to_owned(),
            ))
        } else {
            None
        };
        self.inner.put(record).await?;
        self.foreground |= prompt && !self.event && self.overlap.held.load(Ordering::SeqCst);
        if let Some(generation) = prepared {
            self.prepared.push(generation);
        }
        if prompt && self.event && self.overlap.armed.swap(false, Ordering::SeqCst) {
            self.held = true;
            self.overlap.held.store(true, Ordering::SeqCst);
            self.overlap.event_entered.add_permits(1);
            self.overlap.release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }
    async fn commit(self) -> Result<(), StateStoreError> {
        let foreground =
            self.foreground && !self.overlap.foreground_claimed.swap(true, Ordering::SeqCst);
        if foreground {
            self.overlap.foreground_entered.add_permits(1);
            self.overlap
                .foreground_release
                .acquire()
                .await
                .unwrap()
                .forget();
        }
        let result = self.inner.commit().await;
        if foreground && matches!(&result, Err(StateStoreError::TransactionConflict { .. })) {
            self.overlap
                .foreground_conflicts
                .fetch_add(1, Ordering::SeqCst);
        }
        if result.is_ok() {
            self.overlap
                .prepared_commits
                .lock()
                .unwrap()
                .extend(self.prepared);
        }
        if self.held {
            self.overlap.held.store(false, Ordering::SeqCst);
            if result.is_ok() {
                self.overlap.event_commits.fetch_add(1, Ordering::SeqCst);
                self.overlap.event_committed.add_permits(1);
            }
        }
        result
    }
    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opencode_lance_foreground_conflict_recomputes_after_native_event_commit() {
    let marker = format!("LANCE_CONFLICT_{}", uuid::Uuid::new_v4().simple());
    let prompt_text = format!(
        "This is a literal text copy check. Do not call any tools. Return only this opaque token, preserving every character exactly: {marker}. Do not add tags, markup, quotes, whitespace, explanations, or any other prefix or suffix."
    );
    super::StrictLiveEnvironment::require_all();
    let fixture = StrictFixture::new().await;
    let overlap = Arc::new(LanceOverlap::new());
    let mut agent = None;
    let mut workspace = None;
    let mut conversation = None;
    let address = StrictFixture::available_loopback_address();
    let observed = NativeCleanup::observe(tokio::time::Instant::now() + Duration::from_secs(240), async {
        let config = LanceStateStoreConfig::directory(fixture.directory())
            .with_operation_timeout(Duration::from_secs(15)).unwrap();
        let store = OverlappingLance {
            inner: StateStore::connect(StateStoreConfig::Lance(config)).await.unwrap(),
            overlap: Arc::clone(&overlap),
        };
        let native = OpenCodeConfig::spawn(PathBuf::from("opencode"), Vec::new(), address, Duration::from_secs(30), Duration::from_secs(120), Duration::from_secs(30)).unwrap();
        let config = AgentConfig::new(native.into(), Duration::from_secs(120), std::num::NonZeroUsize::new(1).unwrap()).unwrap();
        agent = Some(AgentBuilder::create(config, AgentStorage::new(store)).build().await.unwrap());
        let agent = agent.as_ref().unwrap();
        let configured = StrictFixture::required("TEST_OLLAMA_MODEL_OPENCODE_DEFAULT");
        let inventory = agent.list_models().await.unwrap();
        let selected: Vec<_> = inventory.iter().filter(|model| model.display_name() == Some(configured.as_str())).collect();
        assert_eq!(selected.len(), 1, "configured native model must be unambiguous");
        let model = selected[0].selector().clone();
        workspace = Some(agent.new_workspace(StrictFixture::workspace("lance overlap")).await.unwrap());
        conversation = Some(workspace.as_ref().unwrap().new_conversation(StrictFixture::conversation("native Lance overlap")).await.unwrap());
        let conversation = conversation.as_ref().unwrap();
        overlap.armed.store(true, Ordering::SeqCst);
        let prompt_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        // Native HTTP completion waits for inference. Reserve one configured
        // StateStore operation budget for commit, recomputation and publication;
        // model latency never substitutes for two staged native transactions.
        let overlap_deadline = prompt_deadline - Duration::from_secs(15);
        let mut stream = conversation.prompt_stream(PromptRequest::new(prompt_text.clone()).unwrap().with_model(model).with_timeout(Duration::from_secs(120)).unwrap()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(30), overlap.event_entered.acquire()).await.expect("real native event must enter the gated Lance transaction").unwrap().forget();
        tokio::time::timeout_at(overlap_deadline, overlap.foreground_entered.acquire()).await
            .expect("real foreground commit must stage against the held event snapshot before the original deadline reserve")
            .unwrap().forget();
        assert!(overlap.foreground_loads.load(Ordering::SeqCst) > 0);
        overlap.release.add_permits(1);
        tokio::time::timeout_at(overlap_deadline, overlap.event_committed.acquire()).await
            .expect("the native event commit must publish before the stale foreground commit is released")
            .unwrap().forget();
        overlap.foreground_release.add_permits(1);
        let completed = loop {
            match stream.recv().await {
                Some(Ok(PromptEvent::Completed(response))) => break response,
                Some(Ok(_)) => {},
                Some(Err(error)) => panic!("real Lance conflict must recompute without losing the native response: {error:?}"),
                None => panic!("native generation must reach an authoritative completion"),
            }
        };
        let fused = stream.recv().await.is_none();
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        let endpoint = format!("http://{address}");
        let sessions: serde_json::Value = client.get(format!("{endpoint}/session")).query(&[("directory", env!("CARGO_MANIFEST_DIR"))]).send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
        let sessions = sessions.as_array().unwrap();
        assert_eq!(sessions.len(), 1);
        let session = sessions[0]["id"].as_str().unwrap();
        let messages: serde_json::Value = client.get(format!("{endpoint}/session/{session}/message")).query(&[("directory", env!("CARGO_MANIFEST_DIR"))]).send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
        let native_users: Vec<_> = messages.as_array().unwrap().iter().filter(|message| message["info"]["role"] == "user").collect();
        let native_user_ids: Vec<_> = native_users.iter().map(|message| message["info"]["id"].as_str().unwrap().to_owned()).collect();
        let native_user_parts: Vec<_> = native_users.iter().flat_map(|message| message["parts"].as_array().unwrap()).collect();
        let native_part_ids: Vec<_> = native_user_parts.iter().map(|part| part["id"].as_str().unwrap()).collect();
        let prepared_attempts = overlap.prepared_attempts.lock().unwrap().clone();
        let prepared_commits: Vec<_> = overlap.prepared_commits.lock().unwrap().iter().cloned().collect();
        eprintln!("native generation evidence: prepared_attempts={prepared_attempts:?}, committed_generation_parts={prepared_commits:?}, native_user_ids={native_user_ids:?}, native_part_ids={native_part_ids:?}, exact_marker={}, fused={fused}, event_commits={}, first_foreground_conflicts={}", completed.output() == marker, overlap.event_commits.load(Ordering::SeqCst), overlap.foreground_conflicts.load(Ordering::SeqCst));
        // The already-fetched history supplies independent persisted native
        // evidence. Capture failure cannot replace the original exact assertion.
        let primary = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_eq!(completed.output(), marker);
        }));
        let capture = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            QuestionExpectation::new(session, &prompt_text, &configured)
                .prompt_output(&messages, "ollama", &marker, completed.output())
        }));
        let context = match capture {
            Ok(Ok((mut context, native))) => {
                let bounded: String = native.chars().take(4096).collect();
                context["native_text"] = serde_json::json!(bounded);
                context["native_text_truncated"] = serde_json::json!(native.chars().count() > 4096);
                context
            }
            Ok(Err(reason)) => serde_json::json!({"capture":"incomplete","reason":reason,
                "foreground_http_body_observed":false}),
            Err(_) => serde_json::json!({"capture":"incomplete","reason":"analysis_panic",
                "foreground_http_body_observed":false}),
        };
        eprintln!("AGENTS_KIT_LANCE_NATIVE_EVIDENCE={context}");
        if let Err(primary) = primary { std::panic::resume_unwind(primary); }

        assert!(fused);
        assert_eq!(overlap.event_commits.load(Ordering::SeqCst), 1);
        assert_eq!(overlap.foreground_conflicts.load(Ordering::SeqCst), 1, "the staged foreground commit must report a real native transaction conflict");
        assert_eq!(prepared_commits.len(), 1, "a conflict must not reserve a second committed native generation");
        assert_eq!(native_user_ids.len(), 1, "stock OpenCode must receive exactly one user prompt");
        // Durable generation IDs and native message IDs have different owners.
        // The request's private part ID provides the exact cross-boundary proof.
        let matching_parts: Vec<_> = native_user_parts.iter().filter(|part| part["id"].as_str() == Some(prepared_commits[0].1.as_str())).collect();
        assert_eq!(matching_parts.len(), 1, "the committed generation must own exactly one native input part");
        assert_eq!(matching_parts[0]["messageID"].as_str(), Some(native_user_ids[0].as_str()));
        assert_eq!(matching_parts[0]["sessionID"].as_str(), Some(session));
        assert_eq!(matching_parts[0]["type"].as_str(), Some("text"));
        assert_ne!(matching_parts[0]["synthetic"].as_bool(), Some(true));
        assert_eq!(matching_parts[0]["text"].as_str(), Some(prompt_text.as_str()));
        assert_eq!(context["capture"], "complete", "native evidence must be complete");
        assert_eq!(context["native_equals_portable"], true, "native terminal text must be preserved");
    }).await;
    if !matches!(&observed, Ok(Ok(()))) {
        eprintln!(
            "native overlap failure counters: held={}, event_commits={}, foreground_loads={}, first_foreground_conflicts={}, prepared_put_attempts={}",
            overlap.held.load(Ordering::SeqCst),
            overlap.event_commits.load(Ordering::SeqCst),
            overlap.foreground_loads.load(Ordering::SeqCst),
            overlap.foreground_conflicts.load(Ordering::SeqCst),
            overlap.prepared_attempts.lock().unwrap().len(),
        );
    }
    overlap.release();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(150));
    if let Some(conversation) = conversation {
        cleanup.settle(conversation.close()).await;
    }
    if let Some(workspace) = workspace {
        cleanup.settle(workspace.close()).await;
    }
    if let Some(agent) = agent {
        cleanup.settle(agent.shutdown()).await;
    }
    cleanup.finish(observed);
}
