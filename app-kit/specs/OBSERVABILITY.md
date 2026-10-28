# Operational tracing and core lifecycle observation

Operational tracing, opt-in lifecycle hooks and retained history are distinct
boundaries. They derive from the same execution/checkpoint truth; none adds a
second workflow engine or independent authoritative log. Exact hook/event
signatures live only in SPECIFICATIONS.md. Server/UI/CLI and exporter clients
remain excluded; core graph/step/stream/child observation remains required.

## Guaranteed safe tracing

The host owns tracing subscribers, sampling, filtering, export, flush and
shutdown. App reads no environment and installs no subscriber/client/telemetry
service. It emits these spans for the corresponding admitted operations:
`app.open`, `app.operation`, `app.workflow.activate`,
`app.workflow.superstep`, `app.workflow.attempt`, `app.workflow.checkpoint`,
`app.workflow.interaction`. An inapplicable operation need not emit a span;
a performed operation must not silently omit its promised span. `app.operation`
covers an admitted owned operation task, including workspace, workflow-driver,
control, publication and finalization work; it does not require a separate span
for every read accessor. `app.workflow.superstep` observes the admitted barrier
publication, rather than promising a whole-wave duration. Each accepted task
or driver carries its admitting host dispatcher through its existing owner;
App does not select a process-global subscriber or preserve the first driver's
context for subsequent unrelated callers.

Committed status, barrier, control creation/settlement, Unknown, cancellation
and explicit deletion transitions emit metadata events only after durable
commit is proven. Failed uncommitted attempt spans may report a safe failure
category but cannot claim a committed transition. Instrumentation is poll-scoped;
entered span guards never cross await. Span construction, filtering, event
emission, entry, exit and destruction isolate synchronous subscriber panics.
Each underlying workflow poll executes once: its exact Poll is preserved, and
an actual workflow panic resumes after observation exits. This also applies when
cancellation drops the span. App does not clone or record into its spans or query
the subscriber's current span. Host subscriber construction, installation,
export, flush and shutdown remain host operations. Recovery may repeat or lose
transient tracing; it never drives retries, state, admission or cancellation.
Subscriber failure cannot roll back state or construct a workflow failure.

Allowed fields are static operation/transition/category, app-owned public ids,
checkpoint/superstep/action/attempt/control ordinals, bounded counts and durations.
Every run-correlated span and event carries both stable `run_id` and the exact
public `occurrence` token; control/interaction generations are local to that
occurrence and cannot disambiguate repeated task identities on their own.
These are correlation values, never metric dimensions. Pinned tracking keeps
its logical app/task identity mapping and does not replace it with occurrence. No prompts, state, outputs,
notifications, schemas, resolutions, tool/provider payloads, credentials,
headers/endpoints, environment, config Debug, raw lower errors, storage keys/
records/receipts, paths, workspace names, lower ids or arbitrary metadata enters
these callsites. Invalid time arithmetic omits the observation rather than
changing execution. Captured-subscriber tests prove spans, committed ordering,
redaction and no span-guard leakage across async task polls.

Committed interaction events use `event = app.workflow.interaction`, the App
run id and occurrence, proven checkpoint, public target `control` sequence and `generation`,
static `category` (`input`, `inputs`, `notification`, `unknown`) and static
`status` (`pending`, `intended`, `applied`, `uncertain`). They derive from changed
settlement entries in the same proven checkpoint, including an applied target
whose runnable control was removed. Lower notification identities and decision
payloads never become fields. Unknown is an explicit target category; it is not
inferred from a generic running/park checkpoint.

Cancellation uses the existing checkpoint event's `cancel_intent`/`cancelled`
transition and phase status. Deletion emits one `app.workflow.delete` event with
App run id, exact occurrence and `transition = completed` only after the existing bounded deletion
owner proves the sealed head and retained history are absent. Sealing alone is
not deletion completion. A separate event for every deletion page is not
required. Explicit deletion and child delete-after-reduction use that same
completion owner; transient completion events may repeat during recovery.

## Core lifecycle hooks

A caller-supplied concrete H implements one exhaustive event vocabulary for
graph/app/child registration, execute begin/end, step begin/end, stream
begin/item/end and action spans/attributes. Async execution invokes both methods
in fixed sync-then-async order. SyncExecution invokes only synchronous hooks,
including normal completion and explicit finish/abort; internally awaiting a
StateStore does not change the public execution lane. The event explicitly
identifies staged partial data versus committed state. Payload references are
borrowed for the call; a hook may copy them under caller policy but cannot
replace engine state or issue workflow commands through a callback facade.

Hooks are an explicit content boundary distinct from default tracing. No
Vec<dyn Hook>, per-event registration, backend erasure, general registry,
exporter client, global observer runtime or hidden network telemetry exists.
Callers compose concrete hook owners/exhaustive enums and choose external
storage/transport. Global state serialization is the sole callable-erasure
exception and cannot store hook implementations.

Hook errors are functional. Pre-hook failure prevents action dispatch;
this includes native state typing, condition evaluation and staging for the
selected activation. Registered follows proven durable admission and precedes
background work. Registration failure settles that discoverable run without
an action outcome or ordinal; reattachment never repeats registration. Losing
the admission observer does not cancel the accepted registration owner.
The existing run owner is reserved before the admission mutation can become
visible and remains exclusive through its receipt and registration. Failed
registration releases that owner only after terminal settlement and capacity
retention are proven. The required current-authority read after a rejected
Registered callback uses one finite pure-read pass on that original owner;
recoverable failure retries only that observation. Unreadable cleanup authority
uses existing whole-App fatal finalization while preserving the original hook
error to its observer; a proven superseded generation remains an ordinary fence.
The admitted failure mutation retains its separate native receipt ownership.
If one child registration rejects, each later reserved sibling retains that
same bounded cleanup observation policy before its native failure settlement.
The original registration error stays primary; settling an unregistered sibling
does not invent registration, execution or action callbacks.
Native actionable controls have an explicit App-native lifetime distinction.
A successful Execution finish at such a control emits that session's execution
completion callbacks while retaining the unfinished activation observation.
Resolution followed by a new execution session owes one total StepStarted and
StepFinished for that activation. Owning Execution Drop instead closes its
unfinished native observation synchronously with no outcome; later settlement
of the same control does not reopen or repeat that guard. The existing bounded
observation collection retains a closed entry only until that exact control
settles. This is not a permanent history marker, and reopen need not reconstruct
transient observations. These native-control rules are App extensions, not
claims about pinned rurr Input behavior.
One private 30-second ceiling bounds lifecycle observation. Registered and
ExecuteStarted each receive one event budget shared by their sync and async
lanes. A rejected ExecuteStarted retains that same original budget for its
post-receipt generation observation and execution-closing guards. The accepted
native failure mutation remains separately owned even if that observation
budget expires; its exact receipt is not replaced by a redundant head read.
Pre-step and stream-item observation use the smaller of that ceiling
and the activation's remaining, once-sampled deadline. Post-step, post-stream
and any execution completion reached in that same finalization or unwind share
one 30-second cleanup deadline for that chain; a later event or lane never
resets it. Authority reads needed to complete active cleanup consume that same
remaining budget. Recoverable read observation retains the original owner;
permanent unreadability or exhausted read observation invokes existing fatal
App cleanup before stale authority can publish a result. Admitted native writes
keep their existing reconciliation owner and are not cancelled by this read
budget. Explicit abort before the first execution poll owns one authority-read
budget under that same ceiling, without opening execution or action hooks.
Synchronous Drop guards and their retained pure authority read share one
closing budget. A completed error returned to the caller ends that chain:
later owning cleanup samples a new budget, excluding intervening caller idle.
A completed chain does not impose an execution-wide deadline on
subsequent actions. Synchronous callbacks remain
cooperative and cannot be preempted; elapsed time is checked before entering
a later asynchronous callback. Owning Drop invokes no asynchronous callbacks.
This finite observation bound is an explicit App extension to the pin.
An admitted stream-item observer owns publication of its failure before returning
its receipt; losing that receipt cannot discard the failure. Its terminal guards
may run while the caller still owns the source, as in the pin's retained
`AsyncStepExecution.source` (`application/streaming/step_core.rs:497–538` and
`240–272`). This does not change source destruction before publication of an
actual caller Action terminal result.
Stream-item failure rejects staged state; completion/post-hook failure surfaces
the hook error in rurr order while retaining any primary execution failure as
structured cause. Committed actions never replay because a post-commit hook
failed. Cleanup errors do not discard either cause. Panics enter supervised
failure handling at the corresponding possible-effect boundary.

A successful action followed by a post-hook error keeps its committed result,
state and Routing phase and parks advancement as Prepared. The existing run
owner publishes the execution's transient completion failure against its exact
generation and checkpoint before releasing/waking observers. A waiting observer
cannot return the parked boundary early and miss that failure. Repeated wait
calls report it until a new Execution successfully acquires the run; rejected
input or a Busy acquisition does not clear it. Explicit execution resumes the
retained boundary without replaying the committed action. Reopening reports
Paused and does not reconstruct a transient hook error from durable history.
No failed action observation or synthetic Failed/Halted status is added for
this post-commit hook error. Cleanup retains the committed barrier receipt
for its revision-fenced Pause intent rather than requiring another head read
after an exhausted callback. Only a definitively rejected empty Pause may
reconcile against independent cancellation/control progress on that same
committed base while the original execution lifecycle still owns it. An
Unknown native receipt keeps its existing exact reconciliation; it never
rebases on a merely paused or stopping row.

An admitted callback or checkpoint publication retains its final guards until
its actual result is known, even when the Execution owner is dropped. Drop does
not project a pre-receipt cached checkpoint as the result of that publication.
The existing retained task closes dropped-owner guards in only the synchronous
lane after its accepted callback or receipt settles, preserving an item-hook
failure or an already committed action outcome. It admits no later source poll.
Owning Execution drop invokes only synchronous completion/abort observation,
with panic containment and no async I/O. Explicit async finish/abort runs the async
lane while the source remains owned. Dropping local non-Send streams cannot
promise asynchronous user-code destruction. Tests assert exact order, failure
precedence, partial/state distinction, late receipt loss, sync drop versus
explicit async finalization and nested span/attribute behavior.

StepFinished distinguishes a committed result, an action failure, and no outcome.
Normal source EOF, explicit abort and owning execution drop with no completed or
failed action use the last case. Post-step observation precedes post-stream
observation, then execute completion; each asynchronous boundary invokes its
synchronous callback before its asynchronous callback. Drop invokes only the
synchronous lane. These rules follow the fixed archive's
`lifecycle/guard.rs:150–172`, `application/streaming/step_core.rs:240–257,310–332,656–678`.
The accompanying checkpoint still exposes App's authoritative committed base.
Its presence must not be interpreted as a successful or failed action result.

Caller span events use the activation's original context and the same retained
source-event receipt as partial observation. Cancelling one `next` wait leaves
that receipt intact. Owning Execution Drop cannot close spans while an admitted
attribute callback is still running; the original owner retains its result,
publishes any precommit hook failure and then closes remaining guards in only
the synchronous lane. No later source event or asynchronous callback begins
because that receipt was lost. A span becomes open only after its start hooks
succeed, and an end is marked closed before invoking its hooks, so an end-hook
error cannot replay the end. These boundaries correspond to the pin's
`visibility/factory/session.rs:84–137` and
`visibility/factory/handle.rs:119–197`.

Explicit finish must address an open span with no open children. Automatic
completion, source error, EOF and abort close all remaining spans in reverse
start order before step/stream/execute completion. Each span closes once;
enclosing spans still close if an inner end hook fails, preserving the primary
failure and the shared cleanup deadline. Explicit asynchronous cleanup invokes
both lanes for each remaining span even after an earlier span's hook error;
owning Drop overrides all later lanes to synchronous. This models structured
scope cleanup (`visibility/trace_api.rs:165–183,237–255`), while reverse start
order and synchronous Drop closure are explicit App-native rules. The raw
`TraceSpanHandle` has no Drop finalizer. A successful caller Completed event
must finish these spans before its result enters staging, so a span end-hook
failure leaves committed state and action ordinal unchanged. Spans are transient
observations and are neither checkpoint state nor restored history.
One pre-stage span-close pass shares a cleanup deadline. It does not carry that
deadline through intervening BSP sibling actions into the later post-barrier
step/stream/execute chain. Explicit source span events use the smaller of the
activation's remaining deadline and the observation ceiling. Span closure
immediately preceding step completion during EOF, failure or abort shares that
existing finalization deadline.

## Retained reader outcomes and tracking content

History exposes committed checkpoint/action state, results, position, exact
workflow revision and lineage through immutable rows plus one head. A reopened
run's old definition must remain identifiable after a newer revision is selected
for new runs. The attempted action ordinal in lifecycle context is independent
of control-checkpoint sequence; a failed attempt still identifies its action
without inventing a successful outcome. Registration identifies the admitted run,
its App workspace, its graph and both restore lineage and spawning parent action.
This is sufficient to relate empty/unstarted children and failures to their
parents before a child produces its first successful step.

GraphDescription.tracking_record and LifecycleEvent.tracking_records are pure,
bounded serialization methods on the existing projection/event owners. They
produce the pinned rurr graph, metadata, log and children JSON object shapes.
This is an explicit opt-in content boundary; App-native control records remain
the only durability authority and never adopt Burr's wire format internally.
There is no public tracking owner, serializer trait, export configuration,
filesystem/S3 adapter or second log state machine. The caller chooses the
destination and adds JSONL framing; transport layout and service indexing remain
outside App. An empty record list means that the event has no pinned log entry,
not that a promised entry was dropped after a serialization error.

Tracking timestamps use UTC with exactly nine fractional digits and a trailing
`Z`, following pinned persistence/wire.rs. The checked supported interval is
UNIX_EPOCH through 9999-12-31T23:59:59.999999999Z; out-of-range time returns
InvalidInput rather than clamping, wrapping or panicking. This makes the pinned
default time representation's finite year range explicit. Events that produce
no timed record do not validate or render an unused observation time. The
complete returned JSON list uses the existing 1 MiB encoded-byte bound. Opt-in
input/result/attribute content retains natural JSON keys; it is not revalidated
as App workflow-state syntax. Attribute tags remain string-valued.

The exact source schema is the checksum-pinned archive's
`crates/rurr-core/src/tracking/models/{graph,metadata,log,children}.rs`, with
`tracking/lifecycle_events.rs` governing record construction and
`goldens/schema/tracking.log_entry.schema.json` governing log validation:

| Existing projection/event | Required serialized content |
|---|---|
| GraphDescription | `application` with entrypoint, ordered `action` declarations and `transition` records; preserve reads, writes, required/optional inputs, source and condition display text. Missing required source/binding metadata fails explicitly. |
| Registered | `application_metadata` plus graph; restore lineage becomes the logical parent pointer and the parent action becomes the spawning pointer. A child/fork emits the corresponding `child_application_data` relationship. App RunId maps to app_id, WorkspaceId to partition_key, semantic action ordinal to sequence_id. |
| StepStarted/StepFinished | `begin_entry`/`end_entry`, including explicit runtime inputs at the opt-in hook boundary, actual result or exception, and committed user state. StepFinished borrows the committed Checkpoint so the last successful action is explicit. Exported `__SEQUENCE_ID` is the attempted action ordinal from ActionContext; `__PRIOR_STEP` names the last successful action from the checkpoint. A failing B after successful A exports B's ordinal and prior A, with unchanged user state. Reserved metadata is projected only into the export and never mutates user state. |
| SpanStarted/SpanFinished/SpanAttribute | `begin_span`/`end_span`/`attribute`, retaining stable span id, name, parent, ordered dependencies and string-valued tags. Action-scoped attributes use a null span id. Repeated display names and parent-targeted attributes while a child is open remain distinct. |
| StreamStarted/first StreamItem/StreamFinished | `begin_stream`/`first_item_stream`/`end_stream`, preserving owning action/span, the sampled time and exact emitted-item count. Later partials remain available to hooks but do not invent additional pinned first-item records. |
| ChildFinished | No tracking record. The hook retains the child's identity and actual terminal snapshot, but the pinned child-record schema and registration emitters define only `fork` and `spawn_start`. |

The earlier proposed terminal child tracking relationship is removed. The pinned
children model's broad documentation mentions completion, but its checked wire
schema and actual client/registration.rs and s3_artifact.rs emission paths have
no completion record. This corrects an unsupported App export promise; it does
not remove a pinned core capability or the ChildFinished lifecycle event.

StepFinished injects attempted `__SEQUENCE_ID` and committed `__PRIOR_STEP` into
a temporary copy before invoking the existing state serializer once. A missing
committed predecessor removes any user-supplied prior marker. Registered field
encoders, including encoders for these reserved keys, observe those projected
values through the same immutable registry snapshot and may transform them.
The outer numeric `sequence_id` and authoritative checkpoint are unchanged.
Codec errors and invalid encoded state remain errors; no alternate metadata
codec path or fallback success is permitted. This follows pinned step_txn.rs,
tracking/lifecycle_events.rs and state.rs field dispatch.

A StepFinished with no outcome cannot be serialized as an `end_entry`.
Pinned `tracking/lifecycle_events.rs:651–658` requires post-step state supplied
by an actual outcome; normal rollback carries none. App therefore returns a
typed projection error before metadata injection or any codec callback, even
though its event also carries the authoritative checkpoint for other callers.
It neither invents a result/exception nor silently emits an empty record list.

The caller samples observed_at once per event and passes it to serialization;
the same event/time produces the same records. Timestamp formatting and optional
fields follow the pinned wire helpers, without reading a clock inside the pure
serializer. Inputs and partials remain transient unless the caller explicitly
persists this opt-in export. Safe tracing never receives these payloads.

Tests consume the real public events and exported JSON, validate the pinned
schema, and compare graph/metadata/log/children goldens after only the documented
identity/time normalization. They cover failed-step post hooks, sync-only hooks
through SyncExecution, async failure/drop ordering, nested/repeated-name spans,
attributes addressed to a parent, zero/one/many partials, child/fork association,
old definition revision after reopen and callback/serialization errors. The
caller-writer example routes these exact objects without private imports or a
second workflow/tracking runtime. Returning a merely App-native object cannot
satisfy the approved serialization/golden requirement.

Partial stream events and operational tracing are transient; committed results
and checkpoints are retained until explicit deletion. A reader cursor captures
an immutable head cutoff. Default tracing never logs durable user content;
explicit history retrieval and opt-in caller hooks have their documented content
policy. Documentation includes a portable caller-writer example using public
DTOs and identifies exactly which observations may repeat or be lost.
