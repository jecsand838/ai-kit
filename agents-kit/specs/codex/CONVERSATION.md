# Codex Conversation Domain

This document owns the Codex `conversations/` domain behind the opaque root
`Conversation`. It coordinates one native root thread attachment, one
foreground prompt generation, file reads, portable notification delivery,
interaction resolution, managed knowledge context, dynamic tools, recovery,
and terminal close.

Exact public operations and DTO membership remain solely in
[`../API.md`](../API.md). Shared prompt, abort, file, receiver, and lifecycle
semantics remain in [`../RUNTIME.md`](../RUNTIME.md) and
[`../NOTIFICATIONS.md`](../NOTIFICATIONS.md); this file specifies the Codex
ownership needed to honor them.

## Functional Ownership

The conversation domain owns the collaboration among:

- one durable Kit Conversation and workspace revision;
- one private bound Codex root thread and its verified descendant closure;
- the shared long-lived protocol connection and a conversation-scoped route;
- one current prompt owner and terminal waiter disposition;
- conversation-scoped StateStore, notification, and server-request state;
- installed dynamic-tool runtime state; and
- conversation knowledge references and Codex prompt rendering.

Thread, turn, request, item, JSON-RPC, tool-call, and storage identifiers remain
private. Native descendants do not become additional portable Conversations.
Their facts route to the bound root only after sourced ancestry validation.

The concrete Codex payload implements the private root Conversation dispatch
contract once. It does not add another public Codex conversation API or split
one operation across duplicate compatibility implementations.

## Open And Reopen

Opening accepts an already materialized and durably bound native thread plus
validated workspace revision, defaults, managed-source references, installed
tools, and scoped runtime collaborators. The live route is published only when
the conversation can accept typed inbound work and persist its consequences.

After the selected native name update, creation verifies a full-history read
before committing its portable binding. The read must confirm the exact created
binding, source token and empty history; failure preserves the existing bounded
creation cleanup obligations. This check makes no extra read-side flushing
promise and does not replace actual cold-reopen qualification.

Reopen validates complete durable ownership before native resume. It verifies
the bound root thread, exact effective workspace and foreground-only policy,
managed registration compatibility, managed source integrity, and current
prompt/interaction/cleanup state. It never constructs a replacement thread or
uses resume overrides to rewrite fixed attachment policy.

The selected native cold-resume path restores thread-owned permission settings,
including unnamed profiles, only when explicit permission overrides are absent.
Kit resume continues to send the existing explicit permission selection, cwd,
workspace roots, and configuration with `excludeTurns: true`; native restoration
does not replace these fixed requirements. Reject a mismatched effective sandbox,
network policy, or active permission profile before publishing the reopened
handle, and retain foreground feature verification. This is distinct from a live
claim that the Kit exercises native cold-resume restoration without overrides.

Open failure cleans only resources acquired by that attempt. If native or
durable acceptance cannot be reverted, recovery evidence remains authoritative
and no partial handle is returned. Opening does not rebuild the agent protocol
connection or create a second transport owner.

## Foreground Prompt Admission

Prompt is async foreground work, not a blocking thread operation. It validates
the public request, open lifecycle, workspace revision, agent-wide capacity,
and the conversation's single prompt slot before managed retrieval or native
acceptance. `Busy` is returned without starting backend work when either
capacity is unavailable.

The request resolves complete persisted conversation defaults once, then
applies the field-specific precedence in [`../RUNTIME.md`](../RUNTIME.md).
The effective cwd is validated relative to the active backend-host workspace
context. Model and reasoning values are verified against Codex selection
semantics before `turn/start`. Explicit personality selections, including
inherited defaults and previously persisted native intent, are `Unsupported`
before native mutation because the selected release retires personality.
They are never rewritten into instructions or user text. A prompt-level
instruction override remains the honest Codex limitation specified by
[`BACKEND.md`](BACKEND.md); it is not simulated by rewriting user text.

Codex native model and reasoning overrides are sticky. The
conversation compares each resolved portable value with its private last
native selection and uses an exact sourced reset value when one exists. If an
omitted portable selection would otherwise inherit a prior explicit native
override and the exact native reset is unavailable, prompt admission returns
`Unsupported` before StateStore prompt mutation or `turn/start`. It never
inherits the stale override or asks the caller for backend-specific reset
context.

Managed retrieval runs after admission and before native acceptance.
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md) renders typed results as private Codex
context while preserving the caller's prompt text. A structured request maps
its already-compiled schema directly to `TurnStartParams.output_schema`.
Absent or `Auto` tool selection leaves the complete managed dynamic-tool
catalog eligible. `None`, `Required`, `Allowed`, and `RequiredOneOf` are
`Unsupported` before StateStore mutation or JSON-RPC send because selected
`turn/start` has no per-turn tool availability or choice field; Codex never
simulates them with persistent thread config. Unsupported prompt media and
other surfaces absent from the API ledger fail at the same preacceptance
boundary.

## Native Acceptance And Terminalization

One private prompt generation fences native turn acceptance, abort, terminal
events, timeout, caller-future loss, and later prompt admission. The StateStore
owner records enough pre-acceptance intent to distinguish a definite local
failure from ambiguous native acceptance. A timed-out or disconnected
`turn/start` is never automatically replayed.

The portable prompt deadline uses the same generation owner. If it expires
after native acceptance, Codex records timeout intent and dispatches at most
one `turn/interrupt`; the caller receives `Timeout`, and the slot remains
`Busy` until sourced terminal/reconciliation facts settle the fence. The
transport request timeout remains a distinct ambiguity boundary.
Returning a stream does not end that deadline: it also covers pending durable
preparation and the original native start request. Timeout observation cannot
wait behind a transaction owned by the same suspended prompt future. The one
retained task continues polling its original work while recording timeout
intent; it never starts a replacement native request. Expired preparation that
has not dispatched is removed without native cancellation.
Retryable timeout-intent failures use the lifecycle retry bound. Exhaustion or
a definite failure invalidates live runtime observation without waiting for
native completion, retains the unresolved durable generation for recovery, and
does not authorize an interrupt whose cancellation intent failed to commit.

After acceptance, the conversation runtime—not the caller future or protocol
reader—owns progress to one durable terminal. Dropping the caller future keeps
the prompt generation admitted and runtime-owned. The terminal
`turn/completed` fact is validated against the bound thread/turn and combined
with completion-ordered typed item facts under the exact output rules in
[`BACKEND.md`](BACKEND.md).

Native interruption acknowledgment and authoritative history readback may precede
terminal-event delivery. After a settled prompt is compacted, an exact delayed
terminal is historical evidence only: complete native history and a fresh
current-owner check are required, no new result is emitted, and no current
prompt is rewritten. This preserves admission after receipt reconciliation
without retaining an unbounded receipt ledger or waiting for a native event.
Contradictory facts and missing state for a still-active owner remain errors;
repeated identical historical events are not separately counted after compaction.

For a structured request, the selected exact final AgentMessage text is also
parsed as JSON and locally validated against the original schema before
publication. The exact untrimmed native text remains `PromptResponse.output`;
the parsed value becomes `structured_output`. Missing, malformed, or
nonconforming output is `Protocol` with no partial response.

Interrupted turns may carry a typed native error. Unary results, stream
termination, durable failure snapshots, and full-history recovery retain outer
`Cancelled` with cancellation context and the normalized model-generation cause.
Matching historical terminals compare that cause as well as status. Cleanup
failure remains supplementary and cannot erase the primary cancellation/cause.
Completed turns with errors and failed turns without errors remain `Protocol`.

Durable terminal state commits before the live waiter observes success,
cancellation, or failure. No terminal fact yields a second public result. If
the waiter is gone, exact output is durably discarded after terminal commit as
specified by [`../STATE.md`](../STATE.md); it is never reconstructed during
reopen.

`prompt_stream` uses this same prompt session, durable generation, and cleanup
path. The selected `item/agentMessage/delta` event supplies actual text with
private thread, turn, and item identity; only fragments belonging to the current
accepted generation reach its bounded observer. Reasoning, tool, plan, and
other native deltas are excluded. Partial text does not determine the final
item's phase or replace completion-ordered final-output selection. The terminal
`PromptResponse`, including structured output, remains authoritative.

The selected server can emit a delta before returning its start receipt, and
that delta contains no client-message correlation. If the existing durable
generation cannot yet prove its exact accepted turn, its existing bounded output
queue retains the fragments behind a private admission signal. The original
request receipt must match the bound root thread and every observed turn before
any text becomes readable. A mismatch or overflow ends observation with
`Integration` and discards unproven text. The reader keeps draining and the
original request remains owned. Cancelling recv retains the admission wait;
terminal failure and runtime revocation cannot wait behind that signal. This
does not adopt the first observed turn, replay the request, or add a second
pending-output queue. Descendant-thread output with a matching turn string does
not belong to the bound root stream. These cases have dedicated protocol proofs.

The existing live prompt context owns its structured validator and optional
output observer together. It never persists fragments or exposes native ids.
Stream delivery settles the existing terminal waiter as discarded, then clears
the prompt before publishing Completed. Observer drop, cancelled recv, and
overflow do not request native cancellation; explicit abort, prompt deadline,
and hierarchy close use the existing generation cancellation path. A malformed
selected native delta terminates affected observation with Protocol, and a
transport interruption cannot claim complete incremental output.
Terminal claim, waiter settlement, and prompt removal share one lifecycle
cleanup deadline. Definite ownership failures and exhausted storage retries
produce an error, preserving an existing execution error. The deadline bounds
observation and further retry admission; it does not cancel an admitted lower
storage operation. The same task retains that operation through completion,
including timeout-intent commits while original prompt work advances. Failed
cleanup retains durable reconciliation evidence; it cannot publish Completed
or leave its observer waiting forever.

The native runtime retains and joins every foreground prompt session task.
Shutdown seals prompt-task admission, observes task failures, and bounds cleanup
using the existing lifecycle deadline. Last runtime drop revokes tracked tasks;
the Kit does not leave detached prompt tasks or spawn a second stream driver.

Protocol readers remain responsive while prompt, tool, Database, and durable
work proceeds. No prompt or state guard is held across a JSON-RPC, tool,
notification, storage, or waiter await.

## Abort And Cancellation

Abort captures only the currently active private prompt generation. Concurrent
abort calls join that target, and at most one `turn/interrupt` is dispatched
for it. With no active prompt, abort succeeds without native work.

Natural completion may win. Abort succeeds only after that generation reaches
a valid durable terminal and cancellation dispatch is settled or proven
unnecessary. A timeout or ambiguous dispatch retains the generation fence and
keeps later admission `Busy`; delayed cancellation must never strike a later
prompt.

Conversation close and parent cascade use the same generation-owned native
cancellation mechanism while retaining their distinct durable lifecycle
meaning. Pending interactions belonging to the prompt settle through their
one durable terminal transition. Cancellation never clears the prompt merely
because the JSON-RPC interrupt response arrived.

## File Reads

File read validates workspace-relative syntax without cleanup or normalization, current workspace
revision, and effective policy before network I/O. The selected `fs/readFile`
route bypasses the native sandbox, so exact reads are supported only for a
verified FullAccess workspace. A restricted workspace returns `Unsupported`
before the request rather than weakening its roots/access policy. For
FullAccess, the adapter resolves the backend-host absolute path, calls the
selected exact-byte route, decodes base64, and returns an opaque `FileRead`
that preserves the accepted relative path spelling exactly.

Valid UTF-8 is exposed exactly through `text` without trimming or newline
normalization; other bytes remain exact and are exposed through `bytes`.
Malformed successful response or invalid
base64 is `Protocol`. The selected App Server collapses otherwise definite
filesystem failures into an untyped JSON-RPC rejection, so the Codex operation
returns `Integration` with a safe portable detail selected only from the typed
JSON-RPC code. Native server prose is never copied into the error. It does
not parse that message to manufacture `NotFound`, expose raw error data, or
retry the read automatically. These selected-version semantics are recorded in
[`BACKEND.md`](BACKEND.md) and [`PROTOCOL.md`](PROTOCOL.md).

## Notifications And Interaction Resolution

Receiver acquisition delegates to the Codex notification domain and returns
the one portable exclusive receiver. The conversation owns sequencing between
durable insertion/delivery state and public publication. Protocol and
notification classification do not acknowledge items directly.

Resolution validates Kit notification ownership and portable content, then
durably enters resolving before executing the one private native continuation
outside storage/synchronization critical sections. Proven rejection may return
to pending. Proven or ambiguous native acceptance is never resent. Success
requires accepted continuation plus durable terminalization.

[`SERVER_REQUESTS.md`](SERVER_REQUESTS.md) owns native response mapping;
[`CATALOG.md`](CATALOG.md) owns dynamic handler execution; and
[`NOTIFICATIONS.md`](NOTIFICATIONS.md) owns typed event conversion. The
conversation is the side-effect coordinator and prevents any one of those
domains from becoming a second lifecycle owner.

## Close

Close consumes the public handle, seals new conversation work, and attempts
all independent responsibilities within the root lifecycle deadline:

- cancel or reconcile the active prompt generation;
- settle pending/resolving interactions according to acceptance evidence;
- stop dynamic tool continuations;
- preserve required final notification order and transition delivery to its
  terminal drain behavior;
- remove the live route without losing already accepted typed facts;
- record native thread deletion and managed-knowledge cleanup obligations;
  and
- release conversation-owned tasks and runtime resources.

Durable terminal close makes the conversation nonreopenable and begins owner
record cleanup. Native deletion is postcommit cleanup and cannot undo the
portable close boundary. Parent cascade does not invoke this terminal close;
it detaches quiescent runtime and preserves the record for reopen.

## Required Boundaries

- Protocol owns directional decode and typed routing, not waiters, StateStore
  mutations, notification publication, or interaction lifecycle.
- Notification and server-request domains do not execute each other's side
  effects or reach into conversation fields.
- Active prompt and interaction state use data-carrying states on their honest
  owners, not suffix-carrier families or option/boolean soup.
- No generic command bus, actor, callback registry, free helper graph, macro,
  item alias, or compatibility wrapper replaces direct typed ownership.
- Runtime fields remain private; passive protocol and persistence DTOs expose
  fields only within their intentional boundary.

## Validation Obligations

Strict-live coverage with the selected real App Server proves create/reopen,
default precedence, foreground prompt acceptance, exact terminal output,
caller-future loss, concurrent abort, natural-completion races, parent close,
timeout and ambiguous acceptance, generation fencing, exact text/binary file
reads and safe rejection context, notification replay, interaction resolution,
dynamic tools, process loss, and terminal close. Static contracts protect the
dispatch/public boundaries without freezing private conversation topology.

## Selected Item And Recovery Rules

Live and full-history reconciliation select only ordinary completed assistant
messages, in completion order. Async delivery or non-null questions is
Unsupported, including `final_answer` messages, because the foreground owner
cannot preserve those continuation semantics. Async text is not published as a
passive Message. Function-call outputs remain private non-assistant data. No
eligible ordinary answer means the existing missing-output failure, never
substitute async success.

Interrupted turns may carry an error. They remain Cancelled at
Prompt/Cancellation with the normalized Integration Prompt/ModelGeneration
cause, subject to existing primary timeout/cancellation/failure precedence.
Malformed terminal status/error combinations remain Protocol. Error details and
misalignment steering prose remain private. Recovery uses full `thread/read`
history for both paginated and legacy native storage, without a storage-mode
override or a new paging API.
