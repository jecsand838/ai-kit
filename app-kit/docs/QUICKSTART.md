# Run a workflow and reopen its result

Start with the complete
[history and recovery example](../examples/history_and_recovery.rs). It runs
locally without an LLM, external service or credentials. You can first reopen
a result within one process, then retain that result across separate processes.

## Prerequisites and first run

Install Git and Rust through rustup. This checkout selects Rust **1.99.0** and
its components through [rust-toolchain.toml](../../rust-toolchain.toml). Native
dependencies also require a C/C++ compiler, libclang, CMake, Protocol Buffers,
pkg-config, and TLS development libraries. Follow the platform setup in
[build prerequisites](../../CONTRIBUTING.md#get-started) before running the
example. Allow time and disk space for the first native build.

Clone the repository and run from its root. For an existing checkout, use that
directory instead of cloning again:

```sh
git clone https://github.com/jecsand838/ai-kit.git
cd ai-kit
cargo run --locked -p app-kit --example history_and_recovery
```

App has no Cargo features. The example constructs storage-kit's supplied embedded
Lance StateStore in caller code. Lance is included in storage-kit's base dependency
set and needs no additional feature. A caller implementing `StateStoreBackend`
directly needs no App feature or supplied store implementation; the base native
dependencies still compile.

After compilation, the example prints (`<run-id>` represents a generated ID):

```text
Source count: 2; copied count: 40; completed fork count: 41
Reopened fork <run-id> at count 41; source lineage remains readable
```

The source runs two increments. A restored fork overrides the checkpoint after
the first increment with `40`, then executes only the remaining increment to
reach `41`. After deleting the source, the example opens a new App generation
and verifies the fork's result and source lineage. This default mode uses an
in-memory store; its state lasts only within this process.

## Reopen after process exit

Use a fresh directory for the persistent variant. The following shell commands
run the same executable twice; the first process exits before the second starts:

```sh
demo_root="$(mktemp -d)"
cargo run --locked -p app-kit --example history_and_recovery -- create "$demo_root/history"
cargo run --locked -p app-kit --example history_and_recovery -- reopen "$demo_root/history"
```

The `create` invocation prints:

```text
Source count: 2; copied count: 40; completed fork count: 41
Saved fork <run-id> at count 41
```

The `reopen` invocation prints the same fork ID:

```text
Reopened fork <run-id> at count 41; source lineage remains readable
```

`create DIRECTORY` requires a directory that does not already exist. It builds
caller-owned Lance storage in `DIRECTORY/app-state`, completes the fork, and
joins App cleanup before saving public lookup identities in `DIRECTORY/run.json`.
Keep the receipt and storage together: the receipt identifies the workspace,
workflow, source run and fork run, including their exact occurrences. It contains
no native storage records or lower credentials.

`reopen DIRECTORY` reads that receipt, reconstructs the same workflow definition,
and looks up the exact completed fork. It verifies the result and source lineage
without creating a new workspace or run, or executing workflow steps. Repeat
the `reopen` command to read the same result again. Missing or malformed receipts,
missing storage, and an occupied `create` destination fail without resetting
existing data. The example leaves the selected directory in place on success
and failure. After both processes exit, you can remove the test directory created
by the snippet; retain it if you want to reopen the result later.

This demonstrates persistence of a completed result after a normal process
exit. It does not interrupt unfinished work or establish whether an external
effect completed before a crash. Such uncertainty needs explicit caller
resolution; see [effect uncertainty](PERSISTENCE_AND_RECOVERY.md#effect-uncertainty).

## Build the application

`HistoryExample::open_app` contains the full workflow document and construction
code. The two native State nodes increment `count` in sequence. The document
declares `app-kit.workflow/v2`, a logical workflow ID and revision, initial state,
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
the same run. Memory mode does this after joining the first App; persistent mode
also reconnects the store in a new process. A new App generation fences old
handles; reopening a run does not replay completed actions.

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
