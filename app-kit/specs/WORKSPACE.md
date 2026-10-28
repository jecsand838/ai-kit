# Workspace Coordination

[`SPECIFICATIONS.md`](SPECIFICATIONS.md) owns the exact public shape. This
file owns application workspace validation, lower materialization, revision
fencing, replacement compensation, deletion, and recovery.

## Identity And Configuration

`WorkspaceId` uses the canonical `appws_<lowercase-hyphenated-uuid-v4>` form.
`WorkspaceConfig` contains only an optional name and one absolute lexical
root. The root is persisted losslessly as UTF-8, is 1 through 4,096 bytes, and
contains no NUL or control character. Validation is lexical and does not touch
the filesystem, resolve symlinks, or create directories.

A present name is trimmed, nonempty, control-free, and at most 256 UTF-8
bytes. `WorkspaceSnapshot` contains only id, nonzero revision, and the full
config. Revisions begin at one and increment with checked arithmetic.

## Lower Materialization

Every configured caller-supplied `A: AgentBackend` key/revision pair receives one lower
`A::Workspace`. Its associated Conversation remains
`<A::Workspace as WorkspaceBackend>::Conversation`. app-kit lowers the same
root/cwd, one read-write root, full access, network enabled, and otherwise
neutral lower defaults. A missing name lowers to the canonical WorkspaceId
without mutating caller-visible config. app-kit never constructs `A` itself.

Private materialization state contains only logical agent key/revision, public lower
WorkspaceId, and last committed lower revision. It contains no lower handle,
Agent id, backend/native id, conversation, protocol envelope, provider/tool
identity, or lower storage key.

Runtime use reacquires a lower Workspace through public typed-id operations
on the already-live Agent after every committed replacement. Failure to
reacquire after the app commit revokes that live projection and becomes
recovery work; it cannot retroactively change committed success into an error.

Workflow preparation shares one fair admission queue per App workspace and
exact logical Agent key/revision pair. Creating or reacquiring an associated Conversation holds
that admission only while the lower Workspace lease is needed; the Workspace
is dropped before admission is released. Conversation execution then proceeds
independently. Different Agent key/revision pairs never share this queue, even when
their opaque lower identifiers happen to be equal. Recovery and abort paths
that reacquire a lower Workspace use the same admission rule.

Queue waits remain inside the existing activation or cleanup bound. After
admission, exact-load and revalidate the current App generation and activation
control before any lower lookup or creation. A cancelled or expired queued
activation must not perform late conversation creation or prompt submission.
The existing run registry retains only bounded weak queue references, prunes
inactive entries, and uses the existing run capacity bound. It retains neither
a lower Workspace nor another revision authority. Acquiring an owned queue
reference and deciding whether to create that queue are one registry operation,
so dropping the previous last permit cannot create two queues for one key.

## Creation

Creation validates before persistence or lower calls, generates one
WorkspaceId, and commits an unpublished intent with the complete canonical
agent-key/revision set. For each pair, Pending first persists Unknown and then dispatches
exactly once. Unknown never redispatches; recovery compares public lower
inventory against the exact config and all lower ids already claimed by any
other workspace lifecycle for that exact caller Agent pair. Equal public lower
WorkspaceIds from different pairs do not establish shared lower ownership.

One returned or uniquely observed exact unclaimed workspace may settle the
target. A missing or foreign candidate is retained Unknown, while multiple or
already-claimed candidates conflict. Publishing a claim fences the workspace
revision and writes the unchanged App record in the same transaction so two
stale claim inventories cannot adopt one lower id.

The workspace becomes active in one app transaction only after all lower
materializations are proven. Partial creation remains private resumable
progress.

Opening with new Agent revisions extends each active workspace through the same
creation journal: already proven pairs enter as Applied and new pairs as
Pending. Existing replacement/compensation/creation work settles first; deletion
does not acquire new materializations. Re-read the workspace after claiming the
App generation and admit the extension with its exact storage revision. Keep
the workspace's logical revision and all prior Applied identities/revisions;
adding a distinct Agent cannot invalidate a prepared run's old pair. Unknown
creation is reconciled with that pair's public inventory and never redispatched.
No separate deployment worker, lower factory or alternate creation journal is
introduced.

## Lookup And Listing

`App::workspace` exact-loads one active workspace or the minimum sealed
projection needed to converge deletion and observe retained runs. Listing
returns active snapshots in ascending WorkspaceId order after an exclusive
typed cursor. Unknown cursors are `NotFound`; caller limits above the private
maximum are `InvalidInput`, never silently clamped.

Every configured current workflow is available in every active workspace.
Retained run history may keep a history-only Workflow handle after workspace
deletion, but that handle cannot admit work. When coordination is absent,
`App::workspace` requires a readable retained head in that exact workspace;
`Workspace::workflow` additionally requires one in the requested workflow.
The selected head and its terminal checkpoint must agree, and the current App
generation is checked before and after observation. Active or deleting heads
cannot establish this history-only scope, and storage errors are not treated
as absent coordination. No workspace configuration or lower handle is
reconstructed. Snapshot, replacement, start, prepare and restore still require
live coordination; listing workspaces never includes these history-only scopes.
Returned handles do not pin retention: later explicit deletion remains
authoritative for each history read.

## Full Replacement

Replacement is complete, not patch-based:

1. validate the expected revision and full new config;
2. exact-load the current app record and materializations;
3. reject stale revision or a nonterminal run pinned to the current revision;
4. derive complete lower replacement requests and retain prior full values;
5. perform advisory lower preflight for every agent before mutation;
6. apply lower updates in canonical key/revision order outside app transactions;
7. checkpoint every returned lower revision;
8. atomically publish the new app config, revisions, and mutation receipt; and
9. reacquire invalidated lower handles.

Preflight is never a reservation. Every lower mutation rechecks its expected
revision. Apply the recorded preflight/materialization revision for the
first forward mutation and the exact returned revision for its compensation.
A new lower info read is diagnostic only and must not upgrade either write
fence. In particular, own r1→r2 followed by foreign r2→r3 must preserve r3:
compensation attempts expected r2 and conflicts, even if r3's config happens
to equal the prior config. Reopen retains each leg's exact fence.

After advisory preflight, replacement captures the current App revision before
rechecking active runs. It retains that same revision through replacement-intent
publication. A run admitted before the scan is rejected by its presence; one
admitted afterward changes the App revision and invalidates the transaction.
The early active-run check remains a fast rejection, not the final reservation.
Creation, materialization and deletion keep their existing admission rules.

If one lower update fails after earlier updates committed, app state remains
on the prior revision. Compensation runs in reverse order using the exact
returned revision and prior full value, and its progress is durable. Partial
fan-out always returns `Conflict`; even apparently complete compensation does
not report the requested replacement as success.

Replacing persists only the canonical unprocessed key/revision suffix. The applied
prefix is derived from the full materialization inventory; compensation walks
that prefix in reverse. No parallel applied-key journal is stored.

Uncompensated progress blocks snapshot, replacement, and admission until
recovery converges it. No stale lower handle is retried and no lower workspace
is recreated as compensation.

## Deletion

Deletion:

1. exact-loads and revision-fences the active record;
2. atomically seals it against replacement and new admission;
3. durably requests cancellation of every nonterminal run;
4. waits until each run is terminal or truthfully unknown;
5. refuses lower deletion while any run remains nonterminal or unknown;
6. commits a zero-run cleanup authorization;
7. deletes lower workspaces in canonical key/revision order with expected revisions,
   checkpointing success or intent-backed absence; and
8. removes active app records only after every lower target converges.

The seal and cleanup authorization are monotonic. Restart resumes the exact
remaining target set. Retained terminal history keeps only the private scope
index required for lookup and never recreates a workspace.

Workspace lifecycle admission uses APP.md's shared 1,024 caller-operation
limit. Exact same-intent, same-fence observers of an already registered deletion
reattach without another slot or lower call; conflicting registered intents and
stale fences retain `Conflict`. A new lifecycle owner must reserve capacity
before storage or lower effects. Reopen reattaches a durable workspace-deletion
owner without waiting for its lower effects to converge. This differs from
sealed run-history cleanup, which App open observes to completion before
publication.

## Ownership Rules

One workspace lifecycle owner serializes creation, replacement, deletion,
open reconciliation, and admission fences. Run admission itself belongs to
the run/storage boundary. No lower call, capacity wait, timer, channel send,
or unrelated storage operation occurs while a StateTransaction or synchronous
guard is live. Durable intent precedes lower side effects; exact evidence
follows them.

Workspace handle drop performs no lower close or delete. App shutdown revokes
handles, and successful deletion invalidates the deleting handle.
