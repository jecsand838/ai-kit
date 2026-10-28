# Operate an App and its workspaces

Keep the Tokio runtime driven until App cleanup has finished. App owns its
accepted workflow tasks and transferred Agents; the caller owns the service
configuration, App StateStore, and the configuration needed to reopen lower
products. Use explicit shutdown before ending a server process. The
[quickstart](QUICKSTART.md) shows cleanup on both success and operation failure.

## Replace a workspace with an exact fence

Read `workspace.snapshot()` and pass its `revision` to `workspace.replace`.
Replacement takes a complete `WorkspaceConfig`, including its root and optional
name. It is a full replacement rather than a patch. Validation happens before
lower changes, and active workflow work can prevent replacement.

App records each lower materialization's public workspace ID and revision.
It checks readiness, applies updates in canonical Agent key/revision order,
then publishes the new App workspace revision. The logical workspace revision
and each lower Agent workspace revision are different fences. Never copy a
lower revision into an App operation.

If a later lower update fails, compensation attempts to restore earlier
materializations using the exact revisions App recorded after its own writes.
Another caller's update remains a conflict even when it happens to contain the
desired configuration. App cannot adopt that caller's newer fence and overwrite
their work. An unresolved journal prevents ordinary workspace use until it can
be reconciled; the original operation error and current durable state both
matter when diagnosing it.

Accepted workspace operations outlive a cancelled observer. Keep the workspace
identity and operation intent so a caller can reattach to the same operation.
A second, different intent does not acquire the first operation's authority.
Do not retry by constructing a new lower Agent workspace yourself.

## Delete a workspace or retained run

Workspace deletion also requires the exact App workspace revision. It seals new
admission, cancels and drains accepted runs, then removes the lower workspace
materializations and App coordination record. Each lower deletion uses its
recorded revision. Deletion does not authorize unrelated provider resources,
toolsets, caller storage, or other App workspaces to be removed.

Retained workflow history has separate ownership. Deleting workspace
coordination leaves retained results readable through their historical scope,
but that scope cannot admit a new run or restore a fork. Use a run's explicit
`delete(checkpoint)` to remove its retained rows. The supplied checkpoint is an
exact fence, not a request to delete whichever result is current when the
operation eventually runs.

Run deletion seals its history before bounded page removal. An accepted deletion
continues after its observer disappears and can resume after App reopen. A
completed physical delete and delivery of the deletion result are separate
events; graceful App shutdown waits for the accepted operation to publish its
result. See [history and recovery](PERSISTENCE_AND_RECOVERY.md) for selection,
restore, lineage, and source deletion.

## Choose and observe shutdown

| Policy | Effect |
|---|---|
| `Shutdown::RejectActive` | Reject closure if accepted work remains; a rejection leaves the App usable. |
| `Shutdown::Drain { timeout }` | Seal new admission and wait for accepted work to finish. |
| `Shutdown::CancelAndDrain { timeout }` | Seal admission, request cancellation, and wait for owned cleanup. |

The timeout bounds this caller's observation. It does not detach the finalizer
or make lower cleanup successful. A later shutdown call reattaches to retained
completion. Cancellation can escalate a draining App; a later drain request
does not reverse cancellation. Once finalization owns cleanup, it preserves its
primary failure while still attempting the remaining owned shutdowns.

Graceful completion includes accepted operations that have not yet created a
durable run and operations whose durable record has already been deleted.
Zero active run capacity alone is therefore insufficient evidence of quiescence.
App tracks those task owners through publication and join before shutting down
the transferred Agents. Each transferred Agent key/revision is consumed once
in canonical order; a backend's own shared-resource policy still belongs to
that backend.

Ordinary Workflow and WorkflowRun handle Drop does not request cancellation.
[Owning Execution Drop](STREAMING.md#separate-observation-loss-from-owning-drop)
revokes uncommitted caller work while retaining accepted native child evidence
for later explicit collection, without advancing parent state or action ordinal.
Dropping the App root requests its owned cleanup, but Drop
cannot await arbitrary user async code. Keep the runtime alive and prefer an
observed shutdown result when the service must know cleanup has completed.
Do not create a fresh runtime inside Drop or shut down the Tokio runtime while
an accepted App finalizer still needs it.

## Use one current App generation per state domain

App control records occupy one logical domain in the supplied StateStore. A
successful `App::open` claims a new generation and fences the previous App.
This is recovery and replacement of one owner; it is not an active-active
distributed scheduler. Two processes sharing that domain must coordinate which
one opens and owns it. Use separately scoped caller stores for independent Apps.

Reopening requires exact workflow, action, condition, and Agent bindings needed
by readable history. Configuration revisions are caller-owned identities, not
serialized constructors. App reads required pins before its generation claim
and before new lower effects. Ready records use `app/v7`; only the specified
cold `app/v6` cutover is supported. Stop older clients, settle owners and take
a whole-store physical backup before opening with the new Kit. Interrupted
conversion remains non-runnable and resumes before generation claim; it never
replenishes persisted deadlines. Unsupported epochs fail. Keep lower Agent durability
separate from App durability and reopen both when recovering lower work.

Adding an Agent revision preserves existing materializations and journals new
ones before exposing the reopened App. An interrupted extension may contain
proven Applied old pairs and Pending new pairs. Recovery can validate an old
run against those Applied pairs while completing the extension; ordinary
workspace reads remain unavailable during reconciliation. Pending or Unknown
materialization evidence never grants a run access to that Agent pair.

## Plan around bounded inputs and work

These are rejection limits, not measured throughput or memory guarantees.
The limits apply together; satisfying one does not bypass another.

| Resource | Current bound |
|---|---|
| Configured Agent key/revision pairs | 256 |
| Configured workflow definitions | 1,024 entries; total canonical inventory at most 2 MiB |
| Configured action or condition bindings | 4,096 each |
| App workspaces | 10,000 |
| Active runs, including child runs | 100,000 |
| A workflow's nodes / initial state keys | 1,024 each |
| Ready frontier / map items | 4,096 each |
| Child depth below a root | 64; the root has depth zero |
| Workflow source / runtime state boundary | 1 MiB each |
| JSON nesting | 64 levels |
| A workflow's supersteps | 1,000,000 |
| Public list/history page request | At most 1,000 entries |
| Workspace root / workspace name | 4,096 / 256 UTF-8 bytes |

Expression collections, strings, revisions, action operations, retries,
durations, storage records and output queues have additional bounded contracts.
Consult [workflow authoring](WORKFLOW_LANGUAGE.md),
[state and parallelism](STATE_AND_PARALLELISM.md), and the owning
[App](../specs/APP.md), [workflow](../specs/workflows/WORKFLOW.md), and
[StateStore](../specs/STATE_STORE.md) contracts when sizing those values. The
limits are fixed implementation policy; App does not expose a general limit
configuration object.

Node deadlines cover lower preparation and observation, not just the prompt
itself. Accepted effects and storage receipts still require truthful settlement
when a deadline expires. Lifecycle observation has a separate private
30-second ceiling. Synchronous callbacks must cooperate: async timeouts cannot
preempt blocking caller code. Keep blocking I/O out of callbacks and preserve
the primary failure when cleanup also fails.

Measure representative frontier sizes, retained history, backend latency,
process-reopen time, and shutdown under load on the actual deployment. Include
delayed observers and unknown-effect recovery in that workload. An admission
limit of 100,000 active runs is not a claim that the current test envelope has
benchmarked that capacity. Follow the [testing contract](../specs/workflows/TESTING.md)
and retain the hardware, backend, dataset, and exact command with each result.
