# app-kit architecture

`app-kit` owns application workspaces and durable orchestration. Agent
execution stays in `agents-kit`; one private app persistence receiver uses the
caller-built `storage-kit::StateStoreBackend` implementation directly.

## Public boundary

The crate root exposes App, Workspace, configuration, identity, and errors.
`workflow` is the sole functional public module and owns the opaque definition,
Workflow, WorkflowRun, execution owners, state/history values, snapshots, status,
interactions, and resolution.

Its five App-owned behavior traits are `ActionBackend`, `ConditionBackend`,
`StateTyping`, `LifecycleHooks`, and `ChildExecutor`. Their implementations
remain concrete. `ChildExecutor` alone uses a generic associated future so
local non-Send child futures retain their exact types. `StateSerialization`
alone privately erases process-global field/tag codecs; no action, condition,
typing, hook, child executor or lower backend enters that registry.

Execution streams, retained-history selection and restored runs are exposed
through the workflow handles. There is no public mutable Rust graph AST,
scheduler policy, generalized runtime registry, lower-crate reexport or runtime
control handle. The exact allowlist is
[`specs/SPECIFICATIONS.md`](specs/SPECIFICATIONS.md).

## Private owners

- `app`: configuration validation, Agent ownership, admission, and one
  mutex-owned `AppLifecycle`; `AppInventory` validates recovery input,
  `AppTasks` supervises only tracked task lifetime, `RunRegistry` owns shared
  run cells, and `WorkspaceOperations` owns exact closed-intent plus fence
  reattachment. A separately owned cancellation-safe finalizer drives durable
  shutdown, with one narrow publication gate linearizing a final mode
  escalation against the durable Closed transition.
- `workspace`: materialization, revision fencing, compensation, and deletion.
- `workflow/definition`: versioned schema parsing, typed compilation,
  topology/state validation, and canonical inventory.
- `workflow/execution`: deterministic ready sets, isolated sibling staging,
  BSP barriers, reducers, routing, child/map behavior, and capacity.
- `workflow/interaction`: Agent conversations, notifications, external-effect
  intent/evidence, and unknown reconciliation.
- `workflow/run`: actor control, checkpoints, waits, reservations,
  cancellation, recovery, retention, and snapshots.
- `storage`: one direct `AppStore`, keys, versioned codecs, closed app
  mutations, transaction execution, receipts, and exact reads.

Immutable run rows retain identity, pins and one checkpoint. Readers attach a
freshly observed predecessor for receipt reconciliation. Each checkpoint owns
state, frontier, a control map and one phase for execution, cancellation, failure
or terminal status. The compact run head owns retention and deletion progress.
There is no parallel status, control, or event-log model.

## Ordering and recovery

Every external effect commits intent before dispatch and terminal-or-unknown
evidence afterward. Lost storage receipts are reconciled through exact reads.
AppStore batches only proved-Applied receipt retirement in pages of 256,
using a separate best-effort transaction. Its partial idle page is bounded;
reopening sweeps any remainder or failed cleanup without replaying mutations.
A retry requires persisted authoritative proof that the prior lower prompt is
no longer live. Raw errors never establish that proof.

Child identity, parent relation, interaction, definition, materialization,
checkpoint zero, and capacity commit before activation. Cancellation stops new
work, propagates to descendants, and terminalizes only after accepted effects
are reconciled. History remains retained until explicit, bounded, resumable
deletion; terminal capacity release is independent. Mutations preserve their
native revision and missing-record fences. App-owned deletion and shutdown work
survives observer drop and resumes after reopen without blocking App construction.

`AppStore<CS>` performs StateStore reads, scans, and transactions on the active
async runtime. It owns the caller CS directly and consumes `CS::Transaction` through its
public trait for commit or rollback. There is no secondary store or transaction
dispatch layer or built-in storage construction. AppStore owns no OS thread, secondary Tokio runtime,
queue, command actor, oneshot proxy, or shutdown protocol. Backend affinity
and transaction actors remain private storage-kit behavior. App mutations form
a closed enum over only the legal app/workspace/run atomic write shapes; there
is no generic batch or arbitrary fence/change builder.

Run advancement, child admission and failed-run readmission retain their observed
predecessor as a private typed run record with its nonpersisted predecessor link
cleared. `RecordCodec` counts its complete logical inline-equivalent encoding,
including settlement evidence, before applying existing reserve and key checks.
The mutation freshly reads and validates its predecessor inside the StateStore
transaction and compares exact ordered logical evidence before publication.

The private v7 physical projection stores immutable occurrence-owned settlement
pages and a bounded descriptor in each checkpoint. The codec prepares only
changed pages; the existing mutation publishes them with the checkpoint, head,
indexes and receipt. One hydrated run/checkpoint model serves normal execution,
recovery and migration. Restores clear settlement evidence and start independent
page ownership. The migration-only v6 entrypoint is never a fallback for an
invalid v7 record.

The private restore operation pins its concrete future in a box so its retained
hydrated observations do not enlarge the caller's future stack frame. Admission
still starts on polling, and the caller directly owns and awaits restoration;
there is no erased future, spawned task, or additional cancellation owner.

AppStore retains at most four decoded run rows. Reuse requires allocation identity
for the freshly observed checkpoint and every freshly observed referenced page;
matching only the checkpoint is insufficient. Each logical row is bounded by the
unchanged four-MiB inline-equivalent limit and passes occurrence, prospective
reserve and local settlement validation before reuse. Physical root/page metadata,
typed allocations and caller-held outputs are additional memory. Supplied stores
need not preserve allocation identity; fresh allocations miss and repeat checks.
The memo stores no predecessor or contextual recovery result. Existing key,
epoch, head, definition, generation, parent, clock and predecessor-successor checks
still run. Mutation transactions only look up; reads may admit validated rows.
The memo mutex never spans an await. A separate transaction-local vector reuses
at most 128 freshly loaded page snapshots during current/predecessor hydration;
its eviction affects reuse only and imposes no operation or record limit.

Latest-run recovery reads its head, checkpoint and predecessor through one
read-only StateStore transaction. It commits that observation before returning
absence or checking the head again through a fresh ordinary read. The existing
bounded head-change loop also discards and retries a snapshot whose read-only
commit reports a transaction conflict. Other errors, mutation authority, and
every contextual recovery fence remain unchanged.
Failed reads or validation consume rollback; failed rollback preserves the primary
public error and vetoes retry, with sanitized secondary tracing.

Storage transactions contain only transaction-owned awaits. Lower calls occur
outside storage mutation authority. Runtime resources are bounded and owned;
no task, thread, channel, lock, or lower handle is persisted or detached.

## Lower boundaries

`AgentSpec<A>` consumes one fully built `A: AgentBackend`, retained under
`Arc<A>` without requiring `A: Clone`. App uses `A::Workspace` and
`<A::Workspace as WorkspaceBackend>::Conversation` directly. It never erases
or normalizes that hierarchy, reconstructs lower configuration, or adds an
independent child parameter. Heterogeneous inventories use caller-owned matching enums.
`AppConfig<CS, A>` consumes one app-only concrete `StateStoreBackend`, which
moves directly into `AppStore<CS>`; app-kit neither erases it nor wraps it in a second
concurrency runtime nor reimplements backend shutdown. `App`, `Workspace`,
`Workflow`, and `WorkflowRun` preserve `<CS, A>` statically. App owns eventual
consuming `A::shutdown`. Workspace replacement performs lower fan-out and
compensation before publishing app coordination state. App has no direct
providers-kit or tools-kit dependency.

Ready private app records use `app/v7`. `AppStore` performs the specified
one-way cold-cutover migration from `app/v6` before generation claim. One
canonical hydrated run and one codec own immutable settlement pages and logical
limits; legacy decoding exists only during migration. Other epochs and corrupt
records fail explicitly. No second cache, lifecycle worker, or runtime
dual-schema fallback is introduced.
