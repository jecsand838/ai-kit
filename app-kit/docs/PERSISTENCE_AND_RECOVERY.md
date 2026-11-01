# Persistence, retained history and recovery

App control records belong to the `StateStoreBackend` value supplied to
`AppConfig::new`. Construct that value in the caller. Agent storage is a
separate ownership domain: reopening the App does not reconstruct Agents,
providers, toolsets or credentials. Supply ready Agent values again with the
logical keys and revisions required by the stored configuration.

An App generation fences prior owners. Keep the App store durable when runs
must survive process exit. Retain the workspace and workflow IDs, RunId and
`RunSnapshot.occurrence` for exact lookup. Pass
`RunSelector::Exact { id, occurrence }` to `Workflow::open_run` after looking up
the workspace and workflow. These operations do not start a new run. App and
lower stores can share a physical service only when their namespaces and
lifecycle ownership remain independent.

The complete [history and recovery example](../examples/history_and_recovery.rs)
constructs embedded Lance storage in the caller with an empty Agent inventory.
It executes a two-step counter, restores a parked copy with an override, deletes
the source, then finishes the fork at `41`. Its no-argument mode reopens the
result within one process using memory storage.

For separate processes, use `create DIRECTORY` followed by `reopen DIRECTORY`.
The first invocation stores the completed fork in `DIRECTORY/app-state` and
saves public lookup identities in `DIRECTORY/run.json` after App cleanup. The
second reconstructs the required configuration and opens that exact fork without
executing the workflow again. The [quickstart](QUICKSTART.md#reopen-after-process-exit)
contains prerequisites, full commands, and expected output. Keep the store and
receipt together; the receipt is a caller-owned lookup aid, not a replacement
for the persisted App records.

These paths demonstrate checkpoint restoration and reopening completed results.
They do not interrupt unfinished work or resolve uncertainty about external
effects; those decisions follow [effect uncertainty](#effect-uncertainty).

A logical RunId can have independent occurrences, each with its own state,
controls and history. `RunSelector::Latest(id)` opens the occurrence whose App
checkpoint was saved last, including initial admission. A returned run handle
stays bound to that occurrence even if another later saves. Use `Exact` for a
saved response or history reference; occurrence tokens are opaque identities,
not timestamps or checkpoint sequences.

## Checkpoint identity

Admission creates checkpoint sequence zero atomically with run capacity.
Every later committed transition advances the checkpoint sequence, including
state-preserving control transitions. An action ordinal is a zero-based
execution position; successful actions advance it. It is not a storage revision
or a checkpoint sequence. A failed attempt records its proposed ordinal while
preserving the previous successful position and user state.

`WorkflowRun::checkpoint` accepts three selectors:

| Selector | Selected boundary |
| --- | --- |
| `Latest` | Latest committed boundary at the captured run head. |
| `Sequence(n)` | Exact immutable checkpoint sequence `n`. |
| `Action(n)` | Latest successful or failed observation of action ordinal `n`. |

A successful parallel wave commits as one boundary. Selecting any of its
successful sibling ordinals returns that whole boundary and its ordered
outcomes. A failed wave keeps its base state and identifies the canonical
failed attempt; it does not fabricate successful sibling outcomes.

`Checkpoint` includes the original run and workflow IDs, occurrence, exact
definition revision, user state, kind, lifecycle, position, outcomes and optional restore
lineage. Action results remain separate from user state: reading an outcome
does not apply it as a write. Historical lifecycle projections do not recreate
actionable lower notification targets. Use the current snapshot and normal
wait/resolve operations when controlling a live run.

## Read pages without changing execution

`WorkflowRun::history(None, limit)` starts a view at the current committed
head. Pass its returned `HistoryCursor` to read the next page. Limits range
from one through 1,000. Rows are ordered by increasing sequence, and the cursor
keeps the original cutoff even if execution appends more checkpoints.

The cursor can be serialized, but it is scoped to one workspace, workflow, run
occurrence and App generation. A different run or reopened App rejects it.
Start a fresh traversal after reopening. Do not substitute native storage
cursors or infer authority from the cursor's serialized fields.

History reads never run actions, state-typing callbacks, field codecs or lower
Agent recovery. They validate stored records and the captured head. Missing
or explicitly deleted selections return `NotFound`; corrupt retained records
return `Integration`. Deletion admitted during observation may return `Closed`
or `NotFound`. A cursor does not keep data alive.

`Workflow::list_runs` pages over occurrences in `(RunId, occurrence)` order.
Continue with `Some(RunSelector::Exact { id: last.id.clone(),
occurrence: last.occurrence.clone() })` from the last returned snapshot.
Using `Latest(id)` as the page boundary skips every occurrence through that
logical ID; it is not a substitute for an exact pagination cursor.

## Restore into a fresh identity

`Workflow::restore(RestoreOptions)` copies a selected clean boundary into a new
paused run. It leaves the source untouched. `RestoreOptions.source` accepts
`Exact` to constrain the source occurrence or `Latest(id)` to search retained
occurrences for the latest matching saved boundary. Supply `overrides` as an
explicit `WorkflowState`, and choose the resume position:

| Position | Meaning |
| --- | --- |
| `AfterCompleted` | Continue after the selected successful action or complete wave. |
| `AtSaved` | Retry the saved action or complete uncommitted wave. |
| `Entrypoint` | Begin at the definition entrypoint using the copied state. |

For restore, `CheckpointSelector::Latest` selects the latest actual Action or
Failed observation; ordinary control and initial checkpoints do not create an
action save. An explicit `Sequence(0)` selects an initial checkpoint. This
differs from `WorkflowRun::checkpoint(Latest)`, which reads the bound
occurrence's latest App checkpoint. An implicit latest restore cannot skip
in-flight or Unknown work: `Exact` guards its named occurrence, while an
aggregate source guards all retained occurrences with that RunId. An explicit
older Action or Sequence may select a clean historical boundary without
settling the original uncertain work.

Restore validates the copied state before admission. It does not invoke actions
or reissue lower effects. Explicitly acquire `execution` and advance the new
run when ready. A failed parallel boundary retries the whole uncommitted wave,
not just the representative failure shown in history.

Source selection and new admission use the exact source fence. If another
writer changes that fence, reconciliation rereads the already-selected sequence;
it does not silently select a newer `Latest` boundary. A fork owns a clean
copy of its state, so deleting its source does not invalidate it. Its lineage
retains both the source RunId and `source_occurrence`.

`fallback` is used only when the requested source or selection is absent.
It is a complete initial state: definition defaults and restore overrides are
not merged into it. It creates no invented source lineage or action ordinal.
Invalid, corrupt or conflicting source data does not trigger fallback.

## Retention and deletion

Terminal results and immutable history remain until explicit deletion.
Finishing a run releases active execution capacity independently of history
retention. `Workflow::list_runs` includes retained terminal runs; waiting one
second does not remove them.

Call `WorkflowRun::delete(expected_checkpoint)` on a terminal run with its
current checkpoint fence. It deletes only the occurrence bound to that handle.
Deletion first seals the history, then removes it through bounded resumable
pages. Dropping the caller's deletion future does
not undo accepted deletion or detach its native commit. Recovery continues a
sealed deletion under the new App generation. A child still referenced by a
nonterminal parent cannot be deleted until that parent releases it.

A read already in progress can return `Closed` if deletion removes its selected
run while it reads checkpoint history. App remains usable; a new lookup returns
`NotFound`. Missing checkpoint data beneath a readable run head is still an
`Integration` error.

Deleting workspace coordination does not erase retained run history. A
readable retained head can establish a history-only Workspace/Workflow lookup.
It cannot recreate lower workspaces, appear in `App::list_workspaces`, supply a
configuration snapshot, or admit `start`, `prepare` or `restore`. Active or
deleting orphaned heads do not grant this access. Explicit run deletion still
invalidates subsequent reads through an already-returned handle.

## Definition revisions and deployment

A workflow revision is an opaque caller-chosen identifier for one exact
definition. Supply multiple required revisions in `AppConfig`: for each
workflow ID, the last supplied revision is selected for new runs. There is no
lexical or semantic-version ordering. Existing runs and their future children
use their recorded selections. Restores preserve those selections.

Keep required old definitions and exact action/condition bindings available
while active or retained runs reference them. Missing required bindings reject
reopen before effects. An unused historical catalog revision can be omitted,
but reintroducing the same ID/revision with different content is a conflict.
Deleting all dependent history allows callers to stop supplying that old
revision. Agent revisions declare caller-owned compatibility: never reuse one
for incompatible lower configuration. App does not inspect lower products to
discover or infer that compatibility.

Documents use `app-kit.workflow/v2`; ready private records use `app/v7`.
The sole supported upgrade is the cold, one-way `app/v6` migration in the
[StateStore specification](../specs/STATE_STORE.md). Stop older clients, settle
all App and lower storage owners, and take a complete physical backup before
cutover. Opening validates legacy history before claiming migration, completes
already sealed deletions, and atomically converts each checkpoint with its
immutable settlement pages. Interrupted conversion is resumable and cannot run
workflows. Only its explicit root permits mixed v6/v7 records. Normal generation
claim follows the final audit; persisted deadlines retain their original times.

Unsupported epochs and corrupt records fail without repair or fallback. There
is no in-place downgrade or destructive reset. The cold backup is the rollback
path; a logical list/put copy would lose native revisions and tombstones.

## Effect uncertainty

Persisted intent is not proof of a completed external effect. Reopen uses
public lower workspace/conversation IDs and authoritative public evidence.
It cannot infer an old prompt result from conversation metadata or replay an
uncertain action automatically. An unresolved effect remains `Unknown` until
the caller supplies a supported accept, fail or proven-safe retry decision.

Transient execution inputs are separate from checkpoints. Resupply required
values when explicitly resuming execution; do not expect secrets supplied to
an earlier session to survive a crash unless the workflow explicitly wrote them
to user state. App rollback does not undo caller-owned external side effects.

The owning contracts are [checkpoints](../specs/workflows/CHECKPOINTS.md),
[workspace lifecycle](../specs/WORKSPACE.md) and
[interactions](../specs/workflows/INTERACTIONS.md). Executable behavior coverage
is in [checkpoint history](../tests/checkpoint_history.rs),
[revision recovery](../tests/revision_contract.rs),
[deletion](../tests/deletion_contract.rs) and
[workspace history](../tests/workspace_contract.rs).
