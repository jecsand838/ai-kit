# Run a workflow and reopen its result

Start with the complete
[history and recovery example](../examples/history_and_recovery.rs). It runs
locally without an LLM, external service or credentials:

```sh
cargo run --locked -p app-sdk --example history_and_recovery
```

Run the command from the ai-kit workspace root using its pinned Rust toolchain.
App has no Cargo features. The example constructs storage-sdk's supplied LanceDB
StateStore in caller code. LanceDB is included in storage-sdk's base dependency
set and needs no additional feature. A caller implementing `StateStoreBackend`
directly needs no App feature or supplied store implementation.

The example prints a source result of 2, a copied initial value of 40 and a
resumed result of 41. It then opens a new App generation over the same store
and verifies the retained result and source lineage. Its in-memory store is
deliberate: it demonstrates App reopen, while persistence across process exit
requires a persistent caller-selected store.

## Build the application

`HistoryExample::open_app` contains the full workflow document and construction
code. The two native State nodes increment `count` in sequence. The document
declares `app-sdk.workflow/v2`, a logical workflow ID and revision, initial state,
required keys, reducers, entrypoint, nodes and routes. Parsing validates the
document before App admission.

`AppConfig::new` receives the already-built store, Agent inventory and parsed
definitions. This example has no Agent nodes, so its Agent inventory is empty.
The explicit `Agent` type annotation resolves the unused static parameter;
no Agent instance is created. Applications with agents instead pass ready
`AgentSpec<A>` values. Their caller-chosen concrete `A: AgentBackend` and its
associated Workspace and Conversation types remain intact throughout App.

Providers, tools and managed knowledge are configured below Agent. App does
not construct them or require their supplied implementations. A workflow node
selects an Agent by its logical key. Different nodes can select different
configured agents.

## Create and advance a run

`App::create_workspace` creates coordination and any required lower
materializations. The example uses the caller's current directory as its
absolute workspace root. Keep the returned `WorkspaceId` for later lookup;
looking up an existing workspace uses `App::workspace` rather than creating
another one.

`Workspace::workflow` selects a configured workflow. `Workflow::prepare`
admits its initial checkpoint without executing the entry node. The example
then acquires `run.execution(ExecutionOptions::default())` and calls `finish`
to advance the two State nodes. `Workflow::start` is the background-admission
alternative. Initial durable state and transient execution inputs are distinct.

Inspect the returned status as well as its state. An Input or Agent
interaction can pause a workflow instead of producing a terminal result.
Resolve a live interaction with its exact sequence and target generation from
current public observation. The run checkpoint fences cancellation, not target
resolution; a stale fence is not permission to retry an uncertain external effect.

## Retain, restore and reopen

The example selects the first action using `CheckpointSelector::Action(0)`.
Its state has `count = 1`. Restoring that boundary with an override of 40 and
`ResumePosition::AfterCompleted` creates a fresh paused run. Explicit execution
performs only the remaining increment, producing 41.

The source is explicitly deleted before the fork executes. The copied fork
remains valid and preserves its source lineage. Terminal results do not expire
automatically. Checkpoint sequences, action ordinals and storage revisions have
different purposes; use the public selector appropriate to the operation.

To reopen, rebuild the App configuration with the same store and required
definition/binding revisions, then look up the saved WorkspaceId and workflow.
Use `RunSelector::Exact` with the saved RunId and snapshot occurrence to reopen
the same run. The example does this after joining the first App. A new App
generation fences old handles; reopening a run does not replay completed actions.

## Finish owned work

The example always awaits `Shutdown::CancelAndDrain` before propagating an
operation failure. A shutdown timeout ends that caller's observation, while
accepted finalization retains its work. Keep the Tokio runtime driven until
cleanup settles; another shutdown observer can reattach.

Ordinary workflow/run handles are observers, so dropping them does not cancel
a run. An execution-owning stream has different drop semantics: it revokes
uncommitted advancement and preserves any already-accepted effect uncertainty.
Use explicit finish, abort, cancellation and shutdown according to the owner
you hold.

Continue with [workflow authoring](WORKFLOW_LANGUAGE.md),
[state and parallelism](STATE_AND_PARALLELISM.md), and
[persistence and recovery](PERSISTENCE_AND_RECOVERY.md). The exact public
contract is maintained in [SPECIFICATIONS.md](../specs/SPECIFICATIONS.md).
