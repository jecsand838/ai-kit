//! Bounded public-API performance workloads; no lower Agent or service is built.
//!
//! Build once in release mode, then invoke the executable directly:
//! `performance linear --root /absolute/new-directory`
//! `performance seed-history --root /absolute/new-directory --rows 1000`
//! `performance seed-history --root /absolute/another-new-directory --rows 10000`
//! `performance reopen-history --root /absolute/seed-directory --page-size 128 --open-only`
//! `performance reopen-history --root /absolute/seed-directory --page-size 128`
//! `performance ready32 --root /absolute/new-directory`
//! `performance map4096 --root /absolute/new-directory`
//! `performance repeated100 --root /absolute/new-directory`
//! `performance nested-cancel --root /absolute/new-directory`
//! `performance delayed-observer --root /absolute/new-directory`
//!
//! Seed commands refuse existing roots. Reopen runs in a separate invocation;
//! the consumer never opens a second App in its seed process. Its v2 manifest
//! records the exact run occurrence; earlier seed manifests must be regenerated.
//! An external runner
//! records executable/source digests, toolchain, host hardware, cache ordering
//! and process RSS. Internal timings exclude process startup and build time.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use agents_kit::Agent;
use app_kit::workflow::{
    ActionBackend, ActionDeclaration, ActionEvent, ActionMode, ActionRequest, CheckpointKind,
    ExecutionEvent, ExecutionOptions, RunId, RunSelector, RunSnapshot, RunStatus, StateChange,
    WorkflowDefinition, WorkflowRun, WorkflowState,
};
use app_kit::{App, AppConfig, AppError, Shutdown, WorkspaceConfig, WorkspaceId};
use futures_util::{FutureExt, Stream, StreamExt, stream};
use serde_json::{Map, Value, json};
use storage_kit::StateStore;
use storage_kit::state::config::{LanceStateStoreConfig, StateStoreConfig};
use tokio::sync::Notify;

const WORKLOAD_BUDGET: Duration = Duration::from_secs(30 * 60);
const CLEANUP_BUDGET: Duration = Duration::from_secs(2 * 60);
const CLEANUP_OBSERVATION: Duration = Duration::from_secs(10);
const NODE_DEADLINE_MS: u64 = 60_000;
const PAGE_SIZE: usize = 128;
const MANIFEST_SCHEMA: &str = "app-kit-performance/v2";

struct Performance {
    command: Command,
    root: PathBuf,
}

enum Command {
    Linear,
    SeedHistory { rows: u64 },
    ReopenHistory { open_only: bool },
    Ready,
    Map,
    Repeated,
    NestedCancel,
    DelayedObserver,
}

impl Performance {
    fn parse() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut args = std::env::args().skip(1);
        let command = args
            .next()
            .ok_or("expected a documented performance command")?;
        let mut root = None;
        let mut rows = None;
        let mut page_size = None;
        let mut open_only = false;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--root" if root.is_none() => {
                    root = Some(PathBuf::from(args.next().ok_or("--root needs a path")?));
                }
                "--rows" if rows.is_none() => {
                    rows = Some(args.next().ok_or("--rows needs a count")?.parse::<u64>()?);
                }
                "--page-size" if page_size.is_none() => {
                    page_size = Some(
                        args.next()
                            .ok_or("--page-size needs a count")?
                            .parse::<usize>()?,
                    );
                }
                "--open-only" if !open_only => open_only = true,
                _ => return Err(format!("unknown or repeated argument: {arg}").into()),
            }
        }
        let command = match (command.as_str(), rows, page_size, open_only) {
            ("linear", None, None, false) => Command::Linear,
            ("ready32", None, None, false) => Command::Ready,
            ("map4096", None, None, false) => Command::Map,
            ("repeated100", None, None, false) => Command::Repeated,
            ("nested-cancel", None, None, false) => Command::NestedCancel,
            ("delayed-observer", None, None, false) => Command::DelayedObserver,
            ("seed-history", Some(rows @ (1_000 | 10_000)), None, false) => {
                Command::SeedHistory { rows }
            }
            ("reopen-history", None, Some(PAGE_SIZE), open_only) => {
                Command::ReopenHistory { open_only }
            }
            _ => {
                return Err(
                    "invalid command options; see the example's invocation documentation".into(),
                );
            }
        };
        let root = root.ok_or("--root is required")?;
        if !root.is_absolute() {
            return Err("--root must be absolute and its parent must already exist".into());
        }
        Ok(Self { command, root })
    }

    fn settings(actions: u64) -> Value {
        json!({
            "runtime":"tokio-current-thread", "backend":"LanceDB directory",
            "storage_operation_timeout_ms":30_000,
            "node_deadline_ms":NODE_DEADLINE_MS, "workload_timeout_seconds":WORKLOAD_BUDGET.as_secs(),
            "cleanup_timeout_seconds":CLEANUP_BUDGET.as_secs(), "history_page_size":PAGE_SIZE,
            "history_row_ceiling":actions * 16 + 16
        })
    }

    fn definition(linear: bool, actions: u64) -> Result<(WorkflowDefinition, usize), AppError> {
        let mut nodes = Vec::new();
        let mut routes = Map::new();
        for index in 0..if linear { actions } else { 1 } {
            let node = if linear {
                format!("n{index:04}")
            } else {
                "count".to_owned()
            };
            nodes.push(json!({"id":node,"deadline_ms":NODE_DEADLINE_MS,
            "step":{"kind":"state","mutations":[
                {"kind":"increment","target":"count","value":{"kind":"literal","value":1}}
            ]}}));
            if linear && index + 1 < actions {
                routes.insert(
                    node,
                    json!({"mode":"first","branches":[
                        {"target":{"kind":"node","id":format!("n{:04}",index + 1)}}
                    ]}),
                );
            } else if !linear {
                routes.insert(
                    node,
                    json!({"mode":"first","branches":[
                        {"when":{"kind":"less_than","left":{"kind":"state","path":"count"},
                            "right":{"kind":"literal","value":actions}},
                            "target":{"kind":"node","id":"count"}},
                        {"target":{"kind":"end"}}
                    ]}),
                );
            }
        }
        let source = json!({
            "schema":"app-kit.workflow/v2","id":"performance","revision":"r1",
            "state":{"count":0},"required":["count"],"reducers":{"count":"reject_concurrent"},
            "entry":if linear {"n0000"} else {"count"}, "nodes":nodes,"routes":routes
        })
        .to_string();
        assert!(
            source.len() <= 1_048_576,
            "generated definition must fit the real source bound"
        );
        Ok((WorkflowDefinition::parse(&source)?, source.len()))
    }

    fn read_manifest(&self) -> Result<Value, Box<dyn Error + Send + Sync>> {
        // Reopen must never silently create a replacement empty store. Reject
        // links and missing directories before passing the caller-owned path to LanceDB.
        for name in [".performance-root", "manifest.json"] {
            if !fs::symlink_metadata(self.root.join(name))?
                .file_type()
                .is_file()
            {
                return Err(format!("performance root needs a regular {name} file").into());
            }
        }
        let mut marker = String::new();
        File::open(self.root.join(".performance-root"))?
            .take(MANIFEST_SCHEMA.len() as u64 + 1)
            .read_to_string(&mut marker)?;
        if marker != MANIFEST_SCHEMA {
            return Err("root is not owned by this performance consumer".into());
        }
        if !fs::symlink_metadata(self.root.join("state"))?
            .file_type()
            .is_dir()
            || fs::read_dir(self.root.join("state"))?
                .next()
                .transpose()?
                .is_none()
        {
            return Err("performance state directory is absent or empty".into());
        }
        let mut encoded = String::new();
        File::open(self.root.join("manifest.json"))?
            .take(16_385)
            .read_to_string(&mut encoded)?;
        if encoded.len() > 16_384 {
            return Err("performance manifest exceeds its bounded size".into());
        }
        let manifest: Value = serde_json::from_str(&encoded)?;
        if manifest["schema"] != MANIFEST_SCHEMA
            || manifest["dataset"] != "history"
            || manifest["cleanup_joined"] != true
        {
            return Err("unsupported performance manifest".into());
        }
        let actions = manifest["actions"]
            .as_u64()
            .ok_or("manifest has no action count")?;
        let rows = manifest["retained_rows"]
            .as_u64()
            .ok_or("manifest has no retained row count")?;
        if !matches!(rows, 1_000 | 10_000)
            || actions != (rows - 4) / 3
            || manifest["terminal_checkpoint"] != rows - 1
            || manifest["settings"] != Self::settings(actions)
        {
            return Err("manifest workload or settings do not match this consumer".into());
        }
        Ok(manifest)
    }

    fn samples(mut samples: Vec<Duration>) -> Value {
        samples.sort_unstable();
        let length = samples.len();
        if length == 0 {
            return json!({"count":0,"p50_ns":null,"p95_ns":null,"max_ns":null});
        }
        json!({"count":length,
            "p50_ns":samples[(length * 50).div_ceil(100) - 1].as_nanos(),
            "p95_ns":samples[(length * 95).div_ceil(100) - 1].as_nanos(),
            "max_ns":samples[length - 1].as_nanos(),
            "percentile":"nearest rank: ceil(p * n), one-based"})
    }

    fn verify_snapshot(snapshot: &RunSnapshot, actions: u64) {
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert!(snapshot.interactions.is_empty());
        assert_eq!(
            snapshot.state.values(),
            &Map::from_iter([("count".into(), json!(actions))])
        );
        assert_eq!(snapshot.last_action_ordinal, Some(actions - 1));
        assert_eq!(snapshot.outcomes.len(), 1);
        assert_eq!(snapshot.outcomes[0].ordinal, actions - 1);
        assert!(snapshot.outcomes[0].result.is_empty());
    }

    async fn history(
        &self,
        run: &WorkflowRun<StateStore, Agent, WorkloadAction>,
        actions: u64,
        linear: bool,
        terminal_sequence: u64,
    ) -> Result<Value, AppError> {
        let ceiling = actions * 16 + 16;
        let mut cursor = None;
        let mut prior_sequence = None;
        let mut rows = 0_u64;
        let mut observed_actions = 0_u64;
        let mut initial = 0;
        let mut terminal = 0;
        let mut controls = 0;
        let mut halts = 0;
        let mut samples = Vec::new();
        let started = Instant::now();
        loop {
            assert!(
                samples.len() < ceiling.div_ceil(PAGE_SIZE as u64) as usize + 1,
                "history page ceiling exhausted without cursor completion"
            );
            let page_started = Instant::now();
            let page = run
                .history(cursor, NonZeroUsize::new(PAGE_SIZE).unwrap())
                .await?;
            samples.push(page_started.elapsed());
            assert!(page.checkpoints.len() <= PAGE_SIZE);
            assert!(
                !page.checkpoints.is_empty() || page.next.is_none(),
                "history cursor made no progress"
            );
            for row in &page.checkpoints {
                assert_eq!(&row.run, run.id());
                assert!(prior_sequence.is_none_or(|prior| row.sequence > prior));
                prior_sequence = Some(row.sequence);
                rows += 1;
                assert!(
                    rows <= ceiling,
                    "declared retained-row workload ceiling exceeded"
                );
                match row.kind {
                    CheckpointKind::Initial => {
                        initial += 1;
                        assert_eq!(observed_actions, 0);
                        assert_eq!(row.sequence, 0);
                    }
                    CheckpointKind::Action => {
                        assert_eq!(row.outcomes.len(), 1);
                        let outcome = &row.outcomes[0];
                        assert_eq!(outcome.ordinal, observed_actions);
                        assert_eq!(
                            outcome.node,
                            if linear {
                                format!("n{observed_actions:04}")
                            } else {
                                "count".into()
                            }
                        );
                        assert!(outcome.result.is_empty());
                        observed_actions += 1;
                        assert_eq!(row.action_ordinal, Some(observed_actions - 1));
                    }
                    CheckpointKind::Terminal => {
                        terminal += 1;
                        assert!(matches!(row.status, RunStatus::Succeeded));
                    }
                    CheckpointKind::Control => {
                        assert!(row.outcomes.is_empty());
                        controls += 1;
                        if let RunStatus::Halted { node, result } = &row.status {
                            assert!(!linear);
                            assert_eq!(row.sequence, 4);
                            assert_eq!(node, "count");
                            assert_eq!(result, &Some(Map::new()));
                            halts += 1;
                        }
                    }
                    CheckpointKind::Failed { .. } => {
                        panic!("workload history contains a failed action")
                    }
                }
                assert_eq!(
                    row.state.values(),
                    &Map::from_iter([("count".into(), json!(observed_actions))])
                );
                if !linear && matches!(row.sequence, 4 | 5) {
                    assert!(matches!(row.kind, CheckpointKind::Control));
                    assert_eq!(observed_actions, 1);
                    assert_eq!(row.action_ordinal, Some(0));
                    assert_eq!(row.position.as_deref(), Some("count"));
                    if row.sequence == 5 {
                        assert!(matches!(row.status, RunStatus::Paused));
                    }
                }
            }
            cursor = page.next;
            // Each complete Checkpoint/WorkflowState is released with this page.
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(initial, 1);
        assert_eq!(terminal, 1);
        assert_eq!(observed_actions, actions);
        assert_eq!(prior_sequence, Some(terminal_sequence));
        if !linear {
            assert_eq!(rows, actions * 3 + 4);
            assert_eq!(terminal_sequence, rows - 1);
            assert_eq!(controls, actions * 2 + 2);
            assert_eq!(halts, 1);
            assert_eq!(samples.len(), (rows as usize).div_ceil(PAGE_SIZE));
        }
        let elapsed = started.elapsed();
        let first = samples.first().copied().unwrap_or_default();
        let subsequent: Duration = samples.iter().skip(1).copied().sum();
        Ok(
            json!({"rows":rows,"action_rows":observed_actions,"pages":samples.len(),
            "elapsed_ns":elapsed.as_nanos(),"first_page_ns":first.as_nanos(),
            "subsequent_pages_total_ns":subsequent.as_nanos(),
            "rows_per_second":rows as f64 / elapsed.as_secs_f64(),"page_latency":Self::samples(samples)}),
        )
    }

    async fn seed(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        actions: u64,
        linear: bool,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: Some("performance consumer".into()),
                root: self.root.join("workspace"),
            })
            .await?;
        let workflow = workspace.workflow("performance".parse()?).await?;
        let mut run = workflow.prepare(WorkflowState::default()).await?;
        let mut execution = run
            .execution(ExecutionOptions {
                halt_after: if linear {
                    Vec::new()
                } else {
                    vec!["count".into()]
                },
                ..ExecutionOptions::default()
            })
            .await?;
        let mut samples = Vec::with_capacity(actions as usize);
        let started = Instant::now();
        for ordinal in 0..actions {
            let step_started = Instant::now();
            let event = execution.step().await?;
            samples.push(step_started.elapsed());
            let Some(ExecutionEvent::Committed { outcomes, .. }) = event else {
                panic!("each serial workload step must commit one action");
            };
            assert_eq!(outcomes.len(), 1);
            assert_eq!(outcomes[0].ordinal, ordinal);
            assert_eq!(
                outcomes[0].node,
                if linear {
                    format!("n{ordinal:04}")
                } else {
                    "count".into()
                }
            );
            assert!(outcomes[0].result.is_empty());
            if !linear && ordinal == 0 {
                // One public halt and explicit resume add two control rows to
                // Initial + three checkpoints per action + Terminal.
                let halted = execution.finish().await?;
                assert!(matches!(&halted.status, RunStatus::Halted { node, result }
                    if node == "count" && result == &Some(Map::new())));
                assert_eq!(halted.checkpoint, 4);
                assert_eq!(
                    halted.state.values(),
                    &Map::from_iter([("count".into(), json!(1))])
                );
                assert_eq!(halted.last_action_ordinal, Some(0));
                assert!(halted.outcomes == outcomes);
                assert!(halted.interactions.is_empty());
                execution = run.execution(ExecutionOptions::default()).await?;
            }
        }
        let snapshot = execution.finish().await?;
        Self::verify_snapshot(&snapshot, actions);
        let execution_elapsed = started.elapsed();
        let history = self
            .history(&run, actions, linear, snapshot.checkpoint)
            .await?;
        if let Command::SeedHistory { rows } = self.command {
            assert_eq!(history["rows"], rows);
        }
        Ok(
            json!({"dataset":if linear {"linear"} else {"history"},"actions":actions,
            "workspace":workspace.id().to_string(),"run":run.id().to_string(),
            "occurrence":snapshot.occurrence,
            "expected_state":snapshot.state.values(),"last_action_ordinal":snapshot.last_action_ordinal,
            "terminal_checkpoint":snapshot.checkpoint,"retained_rows":history["rows"],
            "execution_ns":execution_elapsed.as_nanos(),
            "actions_per_second":actions as f64 / execution_elapsed.as_secs_f64(),
            "step_latency":Self::samples(samples),"history":history}),
        )
    }

    async fn reopen(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        manifest: &Value,
        actions: u64,
        open_only: bool,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let started = Instant::now();
        let workspace: WorkspaceId = manifest["workspace"]
            .as_str()
            .ok_or("missing workspace ID")?
            .parse()?;
        let run_id: RunId = manifest["run"].as_str().ok_or("missing run ID")?.parse()?;
        let occurrence = manifest["occurrence"]
            .as_str()
            .ok_or("missing run occurrence")?
            .to_owned();
        let workspace = app.workspace(workspace).await?;
        let workflow = workspace.workflow("performance".parse()?).await?;
        let run = workflow
            .open_run(RunSelector::Exact {
                id: run_id,
                occurrence: occurrence.clone(),
            })
            .await?;
        let snapshot = run.snapshot().await?;
        Self::verify_snapshot(&snapshot, actions);
        assert_eq!(snapshot.occurrence, occurrence);
        assert_eq!(json!(snapshot.checkpoint), manifest["terminal_checkpoint"]);
        assert_eq!(json!(snapshot.state.values()), manifest["expected_state"]);
        assert_eq!(
            json!(snapshot.last_action_ordinal),
            manifest["last_action_ordinal"]
        );
        let lookup = started.elapsed();
        if open_only {
            return Ok(
                json!({"dataset":"history","actions":actions,"mode":"open-only",
                "lookup_and_snapshot_ns":lookup.as_nanos(),
                "seed_retained_rows":manifest["retained_rows"],"observed_history_rows":null}),
            );
        }
        let history = self
            .history(&run, actions, false, snapshot.checkpoint)
            .await?;
        assert_eq!(history["rows"], manifest["retained_rows"]);
        Ok(
            json!({"dataset":"history","actions":actions,"mode":"full-history",
            "lookup_and_snapshot_ns":lookup.as_nanos(),"history":history}),
        )
    }

    fn definitions(&self, actions: u64) -> Result<(Vec<WorkflowDefinition>, usize), AppError> {
        if matches!(
            self.command,
            Command::Linear | Command::SeedHistory { .. } | Command::ReopenHistory { .. }
        ) {
            let (definition, bytes) =
                Self::definition(matches!(self.command, Command::Linear), actions)?;
            return Ok((vec![definition], bytes));
        }
        let leaf = json!({
            "schema":"app-kit.workflow/v2","id":"performance-leaf","revision":"r1",
            "state":{"value":0,"result":0},"required":["value","result"],
            "reducers":{"value":"reject_concurrent","result":"reject_concurrent"},
            "entry":"work","nodes":[{"id":"work","deadline_ms":NODE_DEADLINE_MS,
                "step":{"kind":"action","action":"performance-action","revision":"r1"}}],"routes":{}
        });
        let documents = match self.command {
            Command::Ready => {
                let mut nodes = vec![json!({"id":"fork","step":{"kind":"state","mutations":[]}})];
                let mut branches = Vec::new();
                for index in 0..32 {
                    let node = format!("n{index:02}");
                    nodes.push(json!({"id":node,"deadline_ms":NODE_DEADLINE_MS,
                        "step":{"kind":"action","action":"performance-action","revision":"r1"}}));
                    branches.push(json!({"target":{"kind":"node","id":node}}));
                }
                vec![json!({
                    "schema":"app-kit.workflow/v2","id":"performance","revision":"r1",
                    "state":{"value":0,"result":0},"required":["value","result"],
                    "reducers":{"value":"reject_concurrent","result":"collect"},
                    "entry":"fork","nodes":nodes,
                    "routes":{"fork":{"mode":"broadcast","branches":branches}}
                })]
            }
            Command::Map | Command::NestedCancel => {
                let nested = matches!(self.command, Command::NestedCancel);
                let items = if nested { 2 } else { 4_096 };
                let mut documents = vec![
                    json!({
                        "schema":"app-kit.workflow/v2","id":"performance","revision":"r1",
                        "state":{"value":0,"result":0},"required":["value","result"],
                        "reducers":{"value":"reject_concurrent","result":"sum_integer"},
                        "entry":"map","nodes":[{"id":"map","deadline_ms":if nested {NODE_DEADLINE_MS} else {WORKLOAD_BUDGET.as_millis() as u64},
                            "step":{"kind":"map","workflow":if nested {"performance-mid"} else {"performance-leaf"},
                            "items":{"kind":"literal","value":(0..items).collect::<Vec<_>>()},
                            "input_key":"value","max_parallel":if nested {2} else {8},
                            "reductions":[{"source":"result","target":"result","reducer":"sum_integer"}]}}],"routes":{}
                    }),
                    leaf,
                ];
                if nested {
                    documents.push(json!({
                        "schema":"app-kit.workflow/v2","id":"performance-mid","revision":"r1",
                        "state":{"value":0,"result":0},"required":["value","result"],
                        "reducers":{"value":"reject_concurrent","result":"sum_integer"},
                        "entry":"map","nodes":[{"id":"map","deadline_ms":NODE_DEADLINE_MS,
                            "step":{"kind":"map","workflow":"performance-leaf",
                            "items":{"kind":"literal","value":(0..8).collect::<Vec<_>>()},
                            "input_key":"value","max_parallel":8,
                            "reductions":[{"source":"result","target":"result","reducer":"sum_integer"}]}}],"routes":{}
                    }));
                }
                documents
            }
            Command::Repeated | Command::DelayedObserver => vec![leaf],
            Command::Linear | Command::SeedHistory { .. } | Command::ReopenHistory { .. } => {
                unreachable!()
            }
        };
        let mut bytes = 0;
        let mut definitions = Vec::with_capacity(documents.len());
        for document in documents {
            let source = document.to_string();
            assert!(source.len() <= 1_048_576);
            bytes += source.len();
            definitions.push(WorkflowDefinition::parse(&source)?);
        }
        Ok((definitions, bytes))
    }

    async fn ready(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        probe: &WorkloadProbe,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: self.root.join("workspace"),
            })
            .await?;
        let workflow = workspace.workflow("performance".parse()?).await?;
        let mut waves = Vec::with_capacity(32);
        let mut releases = Vec::with_capacity(32 * 31);
        let mut barriers = Vec::with_capacity(32);
        let started = Instant::now();
        for permutation in 0..32 {
            probe.assert_idle();
            let mut run = workflow.prepare(WorkflowState::default()).await?;
            let observer = workflow
                .open_run(RunSelector::Latest(run.id().clone()))
                .await?;
            let mut execution = run.execution(ExecutionOptions::default()).await?;
            let Some(ExecutionEvent::Committed { outcomes, .. }) = execution.step().await? else {
                panic!("the native fork must commit before the ready wave");
            };
            assert_eq!(outcomes.len(), 1);
            assert_eq!(
                (outcomes[0].node.as_str(), outcomes[0].ordinal),
                ("fork", 0)
            );
            let wave = Instant::now();
            let mut finish = Box::pin(execution.finish());
            tokio::select! {
                _ = probe.wait_for(&probe.constructed, (permutation + 1) * 32) => {}
                result = &mut finish => panic!("gated ready wave finished before admission: {}", result.is_ok()),
            }
            assert_eq!(probe.active.load(Ordering::SeqCst), 32);
            let order = (0..32)
                .map(|offset| (permutation + 31 - offset) % 32)
                .collect::<Vec<_>>();
            for (released, index) in order.iter().take(31).enumerate() {
                let releasing = Instant::now();
                probe.release(*index);
                tokio::select! {
                    _ = probe.wait_for(&probe.completed, permutation * 32 + released + 1) => {}
                    result = &mut finish => panic!("a partial ready wave committed: {}", result.is_ok()),
                }
                releases.push(releasing.elapsed());
                let visible = observer.snapshot().await?;
                assert_eq!(
                    visible.state.values()["result"],
                    0,
                    "staged siblings cannot expose a partial barrier"
                );
                assert_eq!(visible.last_action_ordinal, Some(0));
            }
            let barrier = Instant::now();
            probe.release(order[31]);
            let snapshot = finish.await?;
            barriers.push(barrier.elapsed());
            waves.push(wave.elapsed());
            assert!(matches!(snapshot.status, RunStatus::Succeeded));
            assert!(snapshot.interactions.is_empty());
            assert_eq!(
                snapshot.state.values()["result"],
                json!((1..=32).collect::<Vec<_>>())
            );
            assert_eq!(snapshot.last_action_ordinal, Some(32));
            assert_eq!(snapshot.outcomes.len(), 32);
            for (index, outcome) in snapshot.outcomes.iter().enumerate() {
                assert_eq!(outcome.node, format!("n{index:02}"));
                assert_eq!(outcome.ordinal, index as u64 + 1);
                assert_eq!(
                    outcome.result,
                    Map::from_iter([("value".into(), json!(index + 1))])
                );
            }
            assert_eq!(std::mem::take(&mut *probe.order.lock().unwrap()), order);
            assert_eq!(
                probe.completed.load(Ordering::SeqCst),
                (permutation + 1) * 32
            );
            probe.assert_idle();
        }
        assert_eq!(probe.peak.load(Ordering::SeqCst), 32);
        let elapsed = started.elapsed();
        Ok(
            json!({"dataset":"ready32","permutations":32,"ready_actions_per_wave":32,
            "caller_action_commits":1024,"native_fork_commits":32,
            "execution_ns":elapsed.as_nanos(),"caller_actions_per_second":1024.0 / elapsed.as_secs_f64(),
            "wave_latency_including_snapshot_probes":Self::samples(waves),
            "release_to_source_completion_latency":Self::samples(releases),
            "last_release_to_committed_barrier_latency":Self::samples(barriers)}),
        )
    }

    async fn mapped(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        probe: &WorkloadProbe,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: self.root.join("workspace"),
            })
            .await?;
        let workflow = workspace.workflow("performance".parse()?).await?;
        let mut run = workflow.prepare(WorkflowState::default()).await?;
        let observer = workflow
            .open_run(RunSelector::Latest(run.id().clone()))
            .await?;
        let started = Instant::now();
        let mut finish = Box::pin(run.execution(ExecutionOptions::default()).await?.finish());
        tokio::select! {
            _ = probe.wait_for(&probe.constructed, 8) => {}
            result = &mut finish => panic!("map finished before its first eight blocked sources: {}", result.is_ok()),
        }
        assert_eq!(probe.active.load(Ordering::SeqCst), 8);
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 8);
        let unchanged = observer.snapshot().await?;
        assert_eq!(unchanged.state.values()["result"], 0);
        assert!(unchanged.last_action_ordinal.is_none());
        probe.release_all();
        let snapshot = finish.await?;
        let elapsed = started.elapsed();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert!(snapshot.interactions.is_empty());
        assert_eq!(snapshot.state.values()["result"], 4_096_u64 * 4_097 / 2);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(snapshot.outcomes.len(), 1);
        assert_eq!(snapshot.outcomes[0].node, "map");
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 4_096);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 4_096);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 8);
        probe.assert_idle();
        let child = workspace.workflow("performance-leaf".parse()?).await?;
        let mut cursor = None;
        let mut values = BTreeSet::new();
        let verifying = Instant::now();
        loop {
            let page = child
                .list_runs(cursor, NonZeroUsize::new(PAGE_SIZE).unwrap())
                .await?;
            assert!(page.len() <= PAGE_SIZE);
            if page.is_empty() {
                break;
            }
            for row in &page {
                assert!(matches!(row.status, RunStatus::Succeeded));
                let value = row.state.values()["value"].as_u64().unwrap();
                assert!(
                    value < 4_096 && values.insert(value),
                    "every mapped input has exactly one retained child"
                );
                assert_eq!(row.state.values()["result"], value + 1);
                assert_eq!(row.last_action_ordinal, Some(0));
                assert_eq!(row.outcomes.len(), 1);
                assert_eq!(row.outcomes[0].node, "work");
            }
            let last = page.last().unwrap();
            cursor = Some(RunSelector::Exact {
                id: last.id.clone(),
                occurrence: last.occurrence.clone(),
            });
            assert!(values.len() <= 4_096);
        }
        assert_eq!(values.len(), 4_096);
        Ok(json!({"dataset":"map4096","items":4096,"max_parallel":8,
            "parent_node_deadline_ms":WORKLOAD_BUDGET.as_millis(),
            "execution_ns":elapsed.as_nanos(),"child_actions_per_second":4096.0 / elapsed.as_secs_f64(),
            "map_completion_latency":Self::samples(vec![elapsed]),
            "child_inventory_verification_ns":verifying.elapsed().as_nanos(),"verified_children":values.len()}))
    }

    async fn repeated(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        probe: &WorkloadProbe,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: self.root.join("workspace"),
            })
            .await?;
        let workflow = workspace.workflow("performance-leaf".parse()?).await?;
        let started = Instant::now();
        // Admit one run at a time, then directly poll at most 16 original
        // executions together. No consumer task is spawned or detached.
        let mut runs = std::pin::pin!(
            stream::iter(0..100_u64)
                .then(|value| {
                    let workflow = &workflow;
                    async move {
                        let started = Instant::now();
                        let run = workflow
                            .prepare(WorkflowState::new(Map::from_iter([(
                                "value".into(),
                                json!(value),
                            )]))?)
                            .await?;
                        Ok::<_, AppError>((value, run, started))
                    }
                })
                .map(|prepared| async move {
                    let (value, mut run, started) = prepared?;
                    let snapshot = run
                        .execution(ExecutionOptions::default())
                        .await?
                        .finish()
                        .await?;
                    assert!(matches!(snapshot.status, RunStatus::Succeeded));
                    assert!(snapshot.interactions.is_empty());
                    assert_eq!(snapshot.state.values()["value"], value);
                    assert_eq!(snapshot.state.values()["result"], value + 1);
                    assert_eq!(snapshot.last_action_ordinal, Some(0));
                    assert_eq!(snapshot.outcomes.len(), 1);
                    Ok::<_, AppError>((run.id().clone(), started.elapsed()))
                })
                .buffer_unordered(16)
        );
        tokio::select! {
            _ = probe.wait_for(&probe.constructed, 16) => {}
            result = runs.next() => panic!(
                "run finished before the first 16 blocked sources: {:?}",
                result.map(|result| result.map(|_| ())),
            ),
        }
        assert_eq!(probe.active.load(Ordering::SeqCst), 16);
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 16);
        probe.release_all();
        let mut ids = BTreeSet::new();
        let mut samples = Vec::with_capacity(100);
        while let Some(result) = runs.next().await {
            let (id, elapsed) = result?;
            assert!(ids.insert(id));
            samples.push(elapsed);
        }
        assert_eq!(ids.len(), 100);
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 100);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 100);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 16);
        probe.assert_idle();
        let rows = workflow
            .list_runs(None, NonZeroUsize::new(128).unwrap())
            .await?;
        assert_eq!(rows.len(), 100);
        assert_eq!(
            rows.iter()
                .map(|row| row.id.clone())
                .collect::<BTreeSet<_>>(),
            ids
        );
        assert!(
            rows.iter()
                .all(|row| matches!(row.status, RunStatus::Succeeded))
        );
        let elapsed = started.elapsed();
        Ok(
            json!({"dataset":"repeated100","runs":100,"caller_in_flight_limit":16,"admission":"sequential",
            "production_active_limit":"unchanged; this measures only the caller's 16-journey envelope",
            "execution_ns":elapsed.as_nanos(),"runs_per_second":100.0 / elapsed.as_secs_f64(),
            "run_latency_including_admission":Self::samples(samples),"indefinite_stability_proof":false}),
        )
    }

    async fn nested_cancel(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        probe: &WorkloadProbe,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: self.root.join("workspace"),
            })
            .await?;
        let workflow = workspace.workflow("performance".parse()?).await?;
        let run = workflow.start(ExecutionOptions::default()).await?;
        tokio::select! {
            _ = probe.wait_for(&probe.constructed, 16) => {}
            result = run.wait() => panic!("nested run ended before its held leaf sources: {}", result.is_ok()),
        }
        assert_eq!(probe.active.load(Ordering::SeqCst), 16);
        let cancelling = Instant::now();
        let snapshot = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let before = run.snapshot().await?;
                assert_eq!(before.state.values()["result"], 0);
                assert!(before.outcomes.is_empty() && before.last_action_ordinal.is_none());
                match run.cancel(before.checkpoint).await {
                    Ok(_) => break,
                    Err(AppError::Conflict(_)) => tokio::task::yield_now().await,
                    Err(error) => return Err(error),
                }
            }
            run.wait().await
        })
        .await
        .map_err(|_| {
            AppError::Timeout("nested cancellation exceeded its declared 30-second grace".into())
        })??;
        let elapsed = cancelling.elapsed();
        assert!(matches!(snapshot.status, RunStatus::Cancelled));
        assert!(snapshot.interactions.is_empty() && snapshot.outcomes.is_empty());
        assert!(snapshot.last_action_ordinal.is_none());
        assert_eq!(snapshot.state.values()["result"], 0);
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 16);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        probe.assert_idle();
        for (id, count) in [
            ("performance", 1),
            ("performance-mid", 2),
            ("performance-leaf", 16),
        ] {
            let child = workspace.workflow(id.parse()?).await?;
            let rows = child
                .list_runs(None, NonZeroUsize::new(128).unwrap())
                .await?;
            assert_eq!(rows.len(), count);
            for row in rows {
                assert!(matches!(row.status, RunStatus::Cancelled));
                assert!(row.outcomes.is_empty() && row.last_action_ordinal.is_none());
                assert_eq!(row.state.values()["result"], 0);
            }
        }
        Ok(
            json!({"dataset":"nested-cancel","depth":2,"intermediate_children":2,"leaf_children":16,
            "cooperative_cancel_grace_ms":30_000,"cancellation_latency":Self::samples(vec![elapsed]),
            "source_replays":0,"caller_sources_dropped":16,"agent_unknown_replay":"not applicable: no Agent attempt"}),
        )
    }

    async fn delayed_observer(
        &self,
        app: &App<StateStore, Agent, WorkloadAction>,
        probe: &WorkloadProbe,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let workspace = app
            .create_workspace(WorkspaceConfig {
                name: None,
                root: self.root.join("workspace"),
            })
            .await?;
        let workflow = workspace.workflow("performance-leaf".parse()?).await?;
        let run = workflow.start(ExecutionOptions::default()).await?;
        let observer = workflow
            .open_run(RunSelector::Latest(run.id().clone()))
            .await?;
        // This public observation is deliberately never polled while the source
        // produces its 256 partials and commits. It owns no execution work.
        let delayed = observer.wait();
        let produced = run.wait().await?;
        assert!(matches!(produced.status, RunStatus::Succeeded));
        assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
        probe.assert_idle();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let observing = Instant::now();
        let snapshot = delayed.await?;
        let elapsed = observing.elapsed();
        assert!(matches!(snapshot.status, RunStatus::Succeeded));
        assert_eq!(snapshot.checkpoint, produced.checkpoint);
        assert_eq!(snapshot.state.values()["result"], 1);
        assert_eq!(snapshot.last_action_ordinal, Some(0));
        assert_eq!(snapshot.outcomes.len(), 1);
        assert_eq!(
            snapshot.outcomes[0].result,
            Map::from_iter([("value".into(), json!(1))])
        );
        assert_eq!(probe.constructed.load(Ordering::SeqCst), 1);
        assert_eq!(probe.partials.load(Ordering::SeqCst), 256);
        probe.assert_idle();
        let terminal = run.snapshot().await?;
        assert_eq!(terminal.checkpoint, snapshot.checkpoint);
        assert_eq!(terminal.state, snapshot.state);
        let mut cursor = None;
        let mut actions = 0;
        let mut rows = 0;
        loop {
            let page = run
                .history(cursor, NonZeroUsize::new(PAGE_SIZE).unwrap())
                .await?;
            for row in &page.checkpoints {
                rows += 1;
                assert!(rows <= 32);
                if matches!(row.kind, CheckpointKind::Action) {
                    actions += 1;
                    assert_eq!(row.action_ordinal, Some(0));
                    assert_eq!(row.outcomes.len(), 1);
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            actions, 1,
            "partials cannot become extra durable action commits"
        );
        Ok(
            json!({"dataset":"delayed-observer","unobserved_partial_events":256,"delay_after_terminal_confirmation_ms":2_000,
            "observation_latency":Self::samples(vec![elapsed]),"retained_rows":rows,
            "memory_scope":"external process RSS; this consumer retains no partial payloads"}),
        )
    }

    async fn close(
        app: &mut App<StateStore, Agent, WorkloadAction>,
        success: bool,
    ) -> Result<(), AppError> {
        let deadline = Instant::now() + CLEANUP_BUDGET;
        let mut first_failure = None;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(first_failure.unwrap_or_else(|| {
                    AppError::Timeout("performance cleanup budget exhausted".into())
                }));
            }
            let policy = if success {
                Shutdown::Drain {
                    timeout: CLEANUP_OBSERVATION,
                }
            } else {
                Shutdown::CancelAndDrain {
                    timeout: CLEANUP_OBSERVATION,
                }
            };
            let result =
                tokio::time::timeout(remaining.min(CLEANUP_OBSERVATION), app.shutdown(policy))
                    .await;
            match result {
                Ok(Ok(())) => return first_failure.map_or(Ok(()), Err),
                Ok(Err(error @ AppError::Timeout(_))) => {
                    first_failure.get_or_insert(error);
                }
                Err(_) => {
                    first_failure.get_or_insert_with(|| {
                        AppError::Timeout("performance cleanup observation timed out".into())
                    });
                }
                Ok(Err(error)) => return Err(first_failure.unwrap_or(error)),
            }
            // A timed-out observer reattaches to its original finalizer.
        }
    }

    async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let manifest = if matches!(self.command, Command::ReopenHistory { .. }) {
            self.root = self.root.canonicalize()?;
            Some(self.read_manifest()?)
        } else {
            fs::create_dir(&self.root)?;
            self.root = self.root.canonicalize()?;
            fs::create_dir(self.root.join("workspace"))?;
            let mut marker = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.root.join(".performance-root"))?;
            marker.write_all(MANIFEST_SCHEMA.as_bytes())?;
            marker.sync_all()?;
            None
        };
        let actions = match self.command {
            Command::Linear => 1_000,
            Command::SeedHistory { rows } => (rows - 4) / 3,
            Command::Ready => 32 * 32,
            Command::Map => 4_096,
            Command::Repeated => 100,
            Command::NestedCancel => 16,
            Command::DelayedObserver => 1,
            Command::ReopenHistory { .. } => {
                manifest.as_ref().unwrap()["actions"].as_u64().unwrap()
            }
        };
        let linear = matches!(self.command, Command::Linear);
        let (definitions, definition_bytes) = self.definitions(actions)?;
        let probe = match self.command {
            Command::Ready => Some(WorkloadProbe::new(SourceMode::Permuted, 1_024)),
            Command::Map | Command::Repeated => {
                Some(WorkloadProbe::new(SourceMode::Parallel, actions as usize))
            }
            Command::NestedCancel => Some(WorkloadProbe::new(SourceMode::Held, 16)),
            Command::DelayedObserver => Some(WorkloadProbe::new(SourceMode::Partials, 1)),
            Command::Linear | Command::SeedHistory { .. } | Command::ReopenHistory { .. } => None,
        };
        if let Some(manifest) = &manifest {
            assert_eq!(manifest["definition_bytes"], json!(definition_bytes));
        }
        let mut app = None;
        let started = Instant::now();
        let observed = AssertUnwindSafe(tokio::time::timeout(WORKLOAD_BUDGET, async {
            let connecting = Instant::now();
            let config = LanceStateStoreConfig::directory(self.root.join("state"))
                .with_operation_timeout(Duration::from_secs(30))?;
            let store = StateStore::connect(StateStoreConfig::Lance(config)).await?;
            let connect = connecting.elapsed();
            let opening = Instant::now();
            app = Some(
                App::open(
                    AppConfig::<_, Agent>::new(store, Vec::new(), definitions).with_actions(
                        probe
                            .as_ref()
                            .map(WorkloadAction::new)
                            .into_iter()
                            .collect(),
                    ),
                )
                .await?,
            );
            let open = opening.elapsed();
            let mut result = match self.command {
                Command::Linear | Command::SeedHistory { .. } => {
                    self.seed(app.as_ref().unwrap(), actions, linear).await?
                }
                Command::Ready => {
                    self.ready(app.as_ref().unwrap(), probe.as_ref().unwrap())
                        .await?
                }
                Command::Map => {
                    self.mapped(app.as_ref().unwrap(), probe.as_ref().unwrap())
                        .await?
                }
                Command::Repeated => {
                    self.repeated(app.as_ref().unwrap(), probe.as_ref().unwrap())
                        .await?
                }
                Command::NestedCancel => {
                    self.nested_cancel(app.as_ref().unwrap(), probe.as_ref().unwrap())
                        .await?
                }
                Command::DelayedObserver => {
                    self.delayed_observer(app.as_ref().unwrap(), probe.as_ref().unwrap())
                        .await?
                }
                Command::ReopenHistory { open_only } => {
                    self.reopen(
                        app.as_ref().unwrap(),
                        manifest.as_ref().unwrap(),
                        actions,
                        open_only,
                    )
                    .await?
                }
            };
            result["store_connect_ns"] = json!(connect.as_nanos());
            result["app_open_ns"] = json!(open.as_nanos());
            result["workload_elapsed_ns"] = json!(started.elapsed().as_nanos());
            Ok::<_, Box<dyn Error + Send + Sync>>(result)
        }))
        .catch_unwind()
        .await;
        let success = matches!(&observed, Ok(Ok(Ok(_))));
        let closing = Instant::now();
        let cleanup = if let Some(mut app) = app.take() {
            let closed = AssertUnwindSafe(Self::close(&mut app, success))
                .catch_unwind()
                .await;
            drop(app);
            Some(closed)
        } else {
            None
        };
        if !success {
            eprintln!(
                "performance workload failed; cleanup completed={}",
                matches!(&cleanup, Some(Ok(Ok(()))))
            );
        }
        let mut result = match observed {
            Ok(Ok(result)) => result?,
            Ok(Err(_)) => {
                return Err(
                    AppError::Timeout("performance workload budget exhausted".into()).into(),
                );
            }
            Err(panic) => resume_unwind(panic),
        };
        match cleanup {
            Some(Ok(result)) => result?,
            Some(Err(panic)) => resume_unwind(panic),
            None => return Err("successful workload did not retain its App cleanup owner".into()),
        }
        // All run/workflow/App owners and the caller-created store have dropped.
        // A cleanup timeout remains a failure even if reattachment later joined.
        result["schema"] = json!(MANIFEST_SCHEMA);
        result["settings"] = Self::settings(actions);
        if let Some(probe) = &probe {
            probe.assert_idle();
            result["caller_sources"] = probe.report();
            result["settings"]
                .as_object_mut()
                .unwrap()
                .remove("history_row_ceiling");
            result["settings"]["private_task_counts"] =
                json!("not exposed; App shutdown proves joined ownership");
            result["settings"]["lower_agents_and_tool_handlers"] = json!("none constructed");
        }
        result["definition_bytes"] = json!(definition_bytes);
        result["process_id"] = json!(std::process::id());
        result["os"] = json!(std::env::consts::OS);
        result["architecture"] = json!(std::env::consts::ARCH);
        result["package_version"] = json!(env!("CARGO_PKG_VERSION"));
        result["cleanup_joined"] = json!(true);
        result["cleanup_ns"] = json!(closing.elapsed().as_nanos());
        let encoded = serde_json::to_vec(&result)?;
        assert!(
            encoded.len() <= 16_384,
            "result stays within the manifest bound"
        );
        if matches!(self.command, Command::SeedHistory { .. }) {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.root.join("manifest.json"))?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        println!("{}", std::str::from_utf8(&encoded)?);
        Ok(())
    }
}

// These counters observe only this consumer's concrete sources. They expose no
// runtime internals and do not claim to count App tasks or lower tool handlers.
#[derive(Clone, Copy)]
enum SourceMode {
    Permuted,
    Parallel,
    Held,
    Partials,
}

struct WorkloadProbe {
    mode: SourceMode,
    expected: usize,
    open: AtomicBool,
    constructed: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    completed: AtomicUsize,
    dropped: AtomicUsize,
    partials: AtomicUsize,
    permitted: Mutex<BTreeSet<usize>>,
    wakers: Mutex<BTreeMap<usize, Waker>>,
    order: Mutex<Vec<usize>>,
    lifetimes: Mutex<Vec<Duration>>,
    changed: Notify,
}

impl WorkloadProbe {
    fn new(mode: SourceMode, expected: usize) -> Arc<Self> {
        Arc::new(Self {
            mode,
            expected,
            open: AtomicBool::new(false),
            constructed: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            partials: AtomicUsize::new(0),
            permitted: Mutex::new(BTreeSet::new()),
            wakers: Mutex::new(BTreeMap::new()),
            order: Mutex::new(Vec::new()),
            lifetimes: Mutex::new(Vec::new()),
            changed: Notify::new(),
        })
    }

    async fn wait_for(&self, counter: &AtomicUsize, expected: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            changed.await;
        }
    }

    fn release(&self, index: usize) {
        assert!(self.permitted.lock().unwrap().insert(index));
        let waker = self.wakers.lock().unwrap().remove(&index);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn release_all(&self) {
        self.open.store(true, Ordering::SeqCst);
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap());
        for (_, waker) in wakers {
            waker.wake();
        }
    }

    fn assert_idle(&self) {
        assert_eq!(
            self.active.load(Ordering::SeqCst),
            0,
            "original caller sources must be dropped"
        );
        assert_eq!(
            self.dropped.load(Ordering::SeqCst),
            self.constructed.load(Ordering::SeqCst)
        );
        assert!(self.wakers.lock().unwrap().is_empty());
        assert!(self.permitted.lock().unwrap().is_empty());
    }

    fn report(&self) -> Value {
        json!({"constructed":self.constructed.load(Ordering::SeqCst),
            "completed":self.completed.load(Ordering::SeqCst),
            "dropped":self.dropped.load(Ordering::SeqCst),
            "active_after_cleanup":self.active.load(Ordering::SeqCst),
            "peak_live_sources":self.peak.load(Ordering::SeqCst),
            "partial_events":self.partials.load(Ordering::SeqCst),
            "construction_to_drop_latency":Performance::samples(std::mem::take(&mut *self.lifetimes.lock().unwrap()))})
    }
}

struct WorkloadAction {
    declaration: ActionDeclaration,
    probe: Arc<WorkloadProbe>,
}

impl WorkloadAction {
    fn new(probe: &Arc<WorkloadProbe>) -> Self {
        Self {
            declaration: ActionDeclaration {
                key: "performance-action".into(),
                revision: "r1".into(),
                reads: vec!["value".into()],
                writes: vec!["result".into()],
                required_inputs: Vec::new(),
                optional_inputs: Vec::new(),
                bound_inputs: Map::new(),
                default_inputs: Map::new(),
                tags: Vec::new(),
                source: None,
                mode: ActionMode::Asynchronous,
            },
            probe: Arc::clone(probe),
        }
    }
}

impl ActionBackend for WorkloadAction {
    type Stream = WorkloadSource;

    fn declaration(&self) -> &ActionDeclaration {
        &self.declaration
    }

    fn start(&self, request: ActionRequest) -> Self::Stream {
        let value = match self.probe.mode {
            SourceMode::Permuted => request
                .context
                .node
                .strip_prefix('n')
                .unwrap()
                .parse()
                .unwrap(),
            SourceMode::Parallel | SourceMode::Held | SourceMode::Partials => {
                usize::try_from(request.state.values()["value"].as_u64().unwrap()).unwrap()
            }
        };
        assert!(
            self.probe.constructed.fetch_add(1, Ordering::SeqCst) < self.probe.expected,
            "a duplicate source construction exceeds this workload's exact bound"
        );
        let active = self.probe.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.probe.peak.fetch_max(active, Ordering::SeqCst);
        self.probe.changed.notify_waiters();
        WorkloadSource {
            probe: Arc::clone(&self.probe),
            value,
            started: Instant::now(),
            state: SourceState::Polling(0),
        }
    }
}

struct WorkloadSource {
    probe: Arc<WorkloadProbe>,
    value: usize,
    started: Instant,
    state: SourceState,
}

enum SourceState {
    Polling(usize),
    Done,
}

impl Stream for WorkloadSource {
    type Item = Result<ActionEvent, AppError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let source = self.get_mut();
        let SourceState::Polling(polls) = &mut source.state else {
            return Poll::Ready(None);
        };
        *polls += 1;
        let polls = *polls;
        match source.probe.mode {
            SourceMode::Held => return Poll::Pending,
            SourceMode::Permuted | SourceMode::Parallel => {
                // The bounded map contains only sources currently waiting on
                // this harness gate, never every historical run or child.
                source
                    .probe
                    .wakers
                    .lock()
                    .unwrap()
                    .insert(source.value, cx.waker().clone());
                let ready = match source.probe.mode {
                    SourceMode::Permuted => {
                        source.probe.permitted.lock().unwrap().remove(&source.value)
                    }
                    SourceMode::Parallel => source.probe.open.load(Ordering::SeqCst),
                    SourceMode::Held | SourceMode::Partials => unreachable!(),
                };
                if !ready {
                    return Poll::Pending;
                }
                source.probe.wakers.lock().unwrap().remove(&source.value);
                if matches!(source.probe.mode, SourceMode::Parallel) && polls == 1 {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
            SourceMode::Partials if polls <= 256 => {
                source.probe.partials.fetch_add(1, Ordering::SeqCst);
                return Poll::Ready(Some(Ok(ActionEvent::Partial {
                    result: Map::from_iter([("progress".into(), json!(polls))]),
                })));
            }
            SourceMode::Partials => {}
        }
        source.state = SourceState::Done;
        if matches!(source.probe.mode, SourceMode::Permuted) {
            source.probe.order.lock().unwrap().push(source.value);
        }
        source.probe.completed.fetch_add(1, Ordering::SeqCst);
        source.probe.changed.notify_waiters();
        Poll::Ready(Some(Ok(ActionEvent::Completed {
            result: Map::from_iter([("value".into(), json!(source.value + 1))]),
            writes: vec![StateChange::Assign {
                values: Map::from_iter([("result".into(), json!(source.value + 1))]),
            }],
        })))
    }
}

impl Drop for WorkloadSource {
    fn drop(&mut self) {
        self.probe.wakers.lock().unwrap().remove(&self.value);
        self.probe.active.fetch_sub(1, Ordering::SeqCst);
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
        self.probe
            .lifetimes
            .lock()
            .unwrap()
            .push(self.started.elapsed());
        self.probe.changed.notify_waiters();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    Performance::parse()?.run().await
}
