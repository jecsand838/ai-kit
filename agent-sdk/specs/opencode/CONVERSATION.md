# OpenCode Conversation Domain

This file defines the OpenCode implementation behind the opaque root
`Conversation`. [`../API.md`](../API.md) is the sole public operation and type
inventory. One OpenCode conversation owns one SDK conversation id, one bound
root native session and its validated descendant closure, one prompt-generation
state machine, one exclusive portable notification receiver, private
interaction state, managed-knowledge retrieval context, native catalog
binding, recovery sequencing, and terminal close state.

Native session, message, part, permission, question, provider, tool, aggregate,
and event ids remain backend-private. The conversation never exposes route
paths, raw SSE or HTTP payloads, native errors, or backend identity through the
portable surface.

## Opening And Ownership

The workspace supplies a committed conversation record, complete conversation
and workspace defaults, verified native binding, catalog binding, knowledge
state, StateStore owner, protocol owner, Agent-owned event handle, prompt
policy, and hierarchy admission. Opening reconciles native status, event
high-water, pending interactions, prompt generation, and cleanup obligations
before registering one owner-checked live route.

There is no per-conversation SSE stream, second HTTP client, parallel polling
receiver, or side-channel interaction API. The agent's sole global stream is
typed and routed through the validated native ownership closure. A foreign
aggregate or session is an isolation contradiction, never a candidate to
adopt.

## Prompt Admission And Defaults

Foreground prompt admission validates the complete request, conversation
lifecycle, current workspace revision, active-generation policy, model,
required absence of reasoning and personality, cwd policy, knowledge policy,
effective prompt timeout, and configured tool compatibility before dispatch.

Effective defaults resolve exactly in this order:

```text
prompt override
-> complete conversation defaults
-> complete workspace defaults
-> backend/default behavior
```

The persisted values are loaded and validated as complete defaults; no
one-field persistence accessor or native-session inference may become a second
authority. Explicit prompt cwd is `Unsupported`. A selected model maps through
the private provider/model pair. Any non-absent effective reasoning or
personality value returns backend-neutral `Unsupported` before native mutation;
arbitrary native variant and agent names never become portable selectors.
Absent effective reasoning and personality fields are omitted so config-owned
server behavior remains private and honest. When model is absent at every
portable precedence level, the request also omits it. The selected server may
then reuse the provider/model selection persisted from a prior accepted
explicit prompt as backend/default behavior. That sticky native selection is
never imported into portable defaults, accepted as backend context, or exposed
through the public API; a later explicit portable model still wins.

Managed knowledge is retrieved according to the effective policy before native
dispatch and rendered as backend-private context according to
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md). Caller instructions remain distinct
from user text and map to native `system`; absence leaves server-owned
instruction behavior intact.

Only one foreground generation may own the native session at a time. Rejecting
concurrency returns `Busy` before native mutation. Replacement or parent close
uses the same generation-fenced cancellation owner; the implementation must
not create a second cancellation path or allow delayed cancellation to strike
a later prompt.

The effective caller prompt timeout resolves from prompt override to Agent
default to no caller deadline. It is distinct from per-request transport and
lifecycle-cleanup deadlines. Expiry before native acceptance returns `Timeout`
with no accepted work. Expiry after possible or definite acceptance durably
records timeout intent and invokes the same generation-fenced native abort;
the prompt is never replayed, no success is fabricated, and later admission
remains `Busy` until terminal or reconciliation evidence proves a delayed
cancellation cannot affect a later generation.

## Foreground Dispatch And Correlation

The selected foreground HTTP response is terminal authority for exact prompt
output. SSE supplies notifications, interactions, status, recovery facts and
transient incremental assistant text, but cannot fabricate or independently
reconstruct that terminal response.

A completed foreground transport future is retired as soon as its result is
observed, including when that result is ambiguous and cancellation continues.
The owned result and native abort outcome remain available for settlement;
cancellation never polls the completed transport again.

Each dispatch uses three distinct identities:

- an SDK-private prompt generation for cancellation and terminal publication;
- server-allocated user and Assistant message ids for native ownership; and
- one private transport-only text-part id for request/response correlation.

The SDK generation is never serialized as a native message id. The request
omits native `messageID`, leaves `noReply` false, and includes the private part
id. A plain prompt omits `format`; a structured prompt supplies exactly
`format: { type: "json_schema", schema, retryCount: 0 }`. Zero structured-output
retries preserves the requested format-validation bound; it does not disable the
server's separate native transport/model retry policy.
The returned value must be a terminal Assistant message for the bound session
and name a valid distinct user parent. Lexical identity order is not evidence
of ownership; the native loop selects by creation time and exact parent match.
For a plain prompt, the adapter reads that
exact parent and verifies the same session, User role, root-parent shape, and
private part id. For a structured prompt, the selected exact-message route
cannot encode the persisted User format after database hydration. The
conversation instead joins the returned parent id with private typed
`message.updated` and `message.part.updated` observations from the sole live
stream or bounded `/sync/history` recovery. Publication requires exactly the
same session, User root shape, returned parent id, and private request-part id.
Missing, conflicting, or multiply owned evidence is `InvalidState`; the prompt
is never replayed and no event supplies terminal output.

When live evidence is incomplete, the conversation sends one bounded
structured-parent synchronization request to the Agent's existing event owner.
The request carries the operation's existing absolute deadline. The event
owner reads history while capturing its current healthy SSE response; the
conversation neither opens a second stream nor grants a fresh timeout. If the
response fails, the event owner drops it, synchronizes a sole replacement, and
only then completes the request.
If the operation deadline expires while that response remains healthy, the
request returns `Timeout` and the event owner resumes live delivery on the
same response.

Malformed wire identities, unknown roles, missing required parent identity,
or malformed selected payloads are `Protocol`. Schema-valid wrong ownership,
wrong role, nonterminal state, unavailable claimed parent, missing correlation
part, or contradictory part ownership is `InvalidState`. A selected abort
error maps to `Cancelled`; other Assistant errors map to `Integration` with
normalized safe details, except selected structured-output failure, which is
`Protocol`. Authentication, output/context limits, filtering, and remote API
status remain distinguishable without retaining native messages or identities.
Remote API cancellation is distinct from the selected abort terminal and does
not establish durable Agent cancellation. None of these errors returns partial
output as a successful response.

Successful output is the exact concatenation, without trimming or inserted
delimiter, of non-synthetic text parts in native `PartID` order. No qualifying
part yields an empty output. Because native counters describe only the last
assistant step rather than the complete foreground operation, portable usage
is `None`.

For a structured prompt, the completed Assistant must also contain its typed
`structured` value. The adapter locally validates that value against the
original compiled schema and publishes it without reserializing it into text;
the exact text-part output may therefore be empty. Missing or nonconforming
structured state is `Protocol` with no partial response.

Absent or `Auto` portable tool selection leaves OpenCode's complete
server-native catalog under its native planner. Every other selection variant
is `Unsupported` before native acceptance. The deprecated prompt `tools` map
is not used: the selected implementation converts it into persistent session
permission mutations and exposes no general per-turn `toolChoice`, so a
snapshot/restore shim would be racy and dishonest.

Accepted work is owned by the runtime, not by the caller's prompt future.
Dropping that future cannot drop an accepted HTTP operation or its durable
terminalization. The waiter receives the first valid generation-matching
terminal result; terminal output is compacted only after delivery or durable
discard and required cleanup have settled.

## Incremental Prompt Observation

`prompt_stream` uses the same request validation, foreground HTTP dispatch,
generation reservation and tracked prompt task as `prompt`. It returns a
bounded observation of that accepted owner. It does not start another event
connection or a forwarding task. The shared portable queue and fragment bounds
belong to [`../RUNTIME.md`](../RUNTIME.md).

The sole live event owner may publish text from a newly applied normal
`message.part.delta` only after identifying the generation's bound root
session, native root User message carrying its exact request-part id, an
Assistant whose parent is that User message, and a nonsynthetic, nonignored
text part owned by that Assistant. Only the exact `text` field is observable.
User text, reasoning, tool content, metadata, unknown parts, unrelated messages
and descendant-session output are excluded. Updates establish ownership;
their accumulated text is never split into fabricated deltas. Duplicate event
identities do not repeat fragments. Removal of the observed Assistant, its
request-parent message, or an owned text/request part interrupts observation
before later deltas can reuse the removed owner. Unrelated removals do not
interrupt the accepted observation. Private ownership inventories are bounded;
exhaustion interrupts observation explicitly instead of growing without limit.

Native deltas are not durable events. A disconnect, sequence gap, or interrupted
event application ends affected active observations with a safe error, even
when normal event/history recovery later succeeds. History never replays
transient text. An explicit same-connection synchronization command that
preserves complete live buffering alone is not a gap.

Dropping the stream, dropping a pending `recv` future, or overflowing its queue
does not lose the accepted HTTP waiter, mark the generation timed out, or
request abort. The runtime still settles the exact terminal result and its
durable delivery/discard state. A stream's `Completed` uses the authoritative
HTTP response after that settlement and may differ from concatenated deltas;
the selected native text-end path permits plugin transformation. Deadline,
abort, close and shutdown use the existing generation-fenced cancellation and
tracked task cleanup. An interrupted observation cannot resume on another
generation or claim successful completion from partial text.
The existing event owner also revokes the tracked prompt tasks when the final
runtime owner drops. Explicit shutdown drains those tasks before stopping the
event owner; drop supplies no asynchronous native-abort or durable-cleanup
guarantee, and a later reopen still reconciles retained generation state.

The stream observer's prompt deadline is published without first waiting for a
StateStore transaction. The existing tracked prompt task continues polling the
foreground work and its exact-generation timeout intent together, including
when the foreground owns a transaction that must finish before the intent can
be admitted. This retains native commit authority and avoids parking a
transaction behind its own timeout mutation. Retryable timeout-intent failures
use one lifecycle retry bound. An intent that never commits cannot authorize a
native abort: its existing foreground HTTP operation remains owned until its
actual response or configured transport deadline, and unresolved durable state
remains available for reconciliation. The intent and foreground are never
replayed or detached.

Persisting foreground dispatch, native admission, cancellation intent,
cancellation claim, cancellation dispatch, and the foreground owner's native
abort outcome each use one lifecycle retry deadline. Pre-dispatch cleanup and
live idle response-loss repair use the same bounded persistence path. Each
stage samples its deadline once and stops admitting retries at the bound.
The existing cancellation and recovery paths retain their transient integration
retry policy. The six pure generation mutations (dispatch, admission, caller
loss, response loss, delivery claim and settlement), cancellation reservation
and dispatch, and persistence of an already-observed native abort outcome also
retry definite StateStore transaction conflicts by opening a fresh transaction and recomputing
the same generation-fenced transition. Unchanged records roll back without
rewriting. A failed rollback remains supplementary to the original failure and
prevents another attempt. Non-conflict commit failures retry only exact record
observation under the same deadline; an unchanged, absent, or different record
does not confirm the intended commit or authorize another mutation. No state
retry resends a native prompt or tool effect. An existing execution error remains
authoritative if pre-dispatch cleanup fails or conflicts. A dispatch commit admitted
before that deadline remains owned after observation times out; when it finally
commits, the expired dispatch stage settles without sending a native prompt.
Foreground and public recovered Abort outcome persistence use the same
exact-record mutation rules: eligible precommit failures require confirmed
rollback before a fresh generation-fenced attempt; definite consuming-commit
conflicts also permit a fresh attempt. Failed rollback vetoes retry, and an
uncertain commit permits exact receipt observation only. Reopen does not
retry a definite outcome conflict; it may retry eligible precommit transient
failures under its original construction bound. Already-recorded outcomes roll
back without consuming another revision.
Retrying an abort outcome only persists the result of the original native abort
and never sends another abort request. A persistently unavailable outcome keeps
the exact durable cancellation record available for reconciliation and cannot
be reported as successfully settled cancellation. When foreground ownership has
ended, the existing tracked recovery-abort task uses the abort operation's
original lifecycle deadline for outcome persistence. Caller timeout stops
observation; the task retains an admitted commit, stops subsequent retries at
the bound, and never resends the native abort. Both live and recovered abort
outcome persistence use the same generation-fenced transaction authority,
including confirmed rollback and exact observation of an uncertain commit.
The recovered observer retains the last normalized persistence failure even
when its deadline ends observation before the task returns its final result.

Cancellation intent owns its precommit retry loop under that original deadline.
Every failed staging attempt must finish rollback before a definite conflict or
eligible transient failure can open a fresh transaction. Failed rollback is
preserved and stops retry. Initial missing or foreign generations, terminal
releasable records and already-recorded cancellation remain no-ops, with their
read transactions explicitly settled. A non-conflict commit failure permits
only observation: the same generation's recorded cancellation or legal later
cancellation state confirms intent. A missing, foreign or still-unrequested
record does not confirm it and cannot authorize another intent write. The
foreground retains this owner's admitted future; public abort delegates once
without adding an outer mutation retry. If its observer is dropped, admitted
native commit authority remains governed by the StateStore contract. This
persistence never sends native work.

Recovered cancellation of a prepared generation owns its transaction attempts
under the Abort operation's original deadline. Every initial missing, foreign,
or nonprepared read explicitly rolls back before releasing or retaining the
reservation. Failed reads or deletion staging must finish rollback before a
definite conflict or eligible precommit failure can retry; failed rollback
remains attached and stops recomputation. Definite consuming commit conflicts
permit a fresh attempt. A non-conflict deletion commit failure permits only
readback: exact absence confirms the deletion, while an unchanged, foreign, or
malformed present record cannot confirm it or authorize another delete. The
original commit error survives failed observation; deadline exhaustion before
commit retains the last failed attempt instead. The caller delegates once,
and losing its observer does not revoke the StateStore's admitted commit
authority. This cleanup never sends a native prompt or abort.

Terminal-result persistence and ambiguous-response-loss persistence each have
one lifecycle retry bound, including when no caller prompt timeout is set.
Their deadline may end observation while retaining an admitted native commit.
Terminal persistence owns its transaction retry: a failed staging attempt must
finish rollback before rereading the same generation and its continuations;
definite commit conflicts permit recomputation. Once commit succeeds or becomes
uncertain, only exact terminal and continuation observation may continue. A
failed observation never restarts terminal writes. Cancellation reservation
uses the same exact-record mutation authority as the pure generation changes,
including no-op rollback and readback of a lost commit receipt. An unconfirmed
reservation cannot itself authorize native cancellation. If the foreground
owner ends, a still-pending explicit abort may recover that same generation
under a fresh owner after verified native Active observations and a confirmed
Dispatching commit. This is distinct from replaying the failed owner's
unconfirmed mutation. Cancellation dispatch itself follows the same exact
transaction receipt rules; both primary and supplementary rollback cleanup
failures prohibit an automatic retry by that owner.
A known response error takes precedence over either persistence failure.
Ambiguous-response delivery uses the same bounded claim/settlement path as
terminal delivery, while retaining its unresolved native record. There is no
independent response-loss retry loop or success synthesized from that record.
If the original native abort is definitely accepted, its exact durable
cancellation owner can terminalize the response-lost generation as Cancelled
and complete cleanup. This does not permit other terminal results to be
invented after losing the foreground response; the observer's earlier
execution error remains authoritative.

Terminal delivery uses one lifecycle deadline across claim, delivery settlement,
and record cleanup. Definite ownership failures end observation with the
portable error; exhausted transient retries end with `Timeout`. Neither may
publish `Completed`, and an earlier execution error remains authoritative.
After waiter settlement, a definite `TransactionConflict` from the consuming
prompt-record deletion commit permits a fresh generation-fenced cleanup attempt
under that same deadline. This retry requires no primary or supplementary
cleanup failure and rechecks the durable record's generation and clearability;
it never repeats the native prompt or an accepted interaction. This rule adds
no retry authority for precommit conflicts or other commit errors.
No retry begins after the deadline. An already admitted native transaction is
still polled by the same owner after observation ends, so its eventual commit
is not cancelled or misreported. These bounds govern observation and admission
of new attempts; they do not forcibly preempt a caller-owned StateStore future
already admitted by that owner. Failed cleanup retains the exact durable
record for subsequent reconciliation. Stream errors preserve the same complete
normalized error as ordinary conversation operations, including operation,
phase, cause, and supplementary cleanup failures. An observation interruption
does not replace that error with a generic integration failure.

HTTP and SSE can arrive in different orders. A successful HTTP response remains
durable terminal authority; its stream waits for the same Assistant's live
`message.updated` completion marker before reporting `Completed`, so earlier
native deltas still in transit can be observed. An already observed marker is
reused; another message's marker cannot satisfy the wait. This observation wait
uses the remaining prompt deadline when one exists, otherwise one existing
event lifecycle deadline. A missing marker ends only observation with `Timeout`
and never replays or aborts already completed HTTP work. Disconnect or recovery
interruption ends the wait with its existing observation error. History cannot
supply the missing live marker or recreate transient fragments.

## Abort And Ambiguous Outcomes

Abort records generation-matching intent before dispatching the selected
native abort route. Concurrent abort callers join the same generation outcome.
The prompt and abort requests are never automatically replayed.

Recovered Abort locally retries a definite transaction conflict only after an
opened transaction has confirmed rollback, or after a consuming commit reports
that exact conflict. Unchanged transactions also require successful rollback.
A rollback failure vetoes retry and retains its diagnostic. Recovered waiter
updates confirm the exact intended record; deletion confirms exact absence.
Other commit failures retain their original StateCommit evidence and permit
only bounded receipt observation, never another write by that attempt. Reopen's
initial prompt reconciliation keeps its one-attempt definite-conflict behavior
under the construction bound. If that reconciliation requires native abort,
persisting the already-observed abort outcome follows the bounded
generation-fenced retry rules above without dispatching another native request.
Reopen also requires confirmed rollback of an unchanged prompt read before
releasing admission, publishing a runtime, or starting a separate repair
transaction. A rollback failure remains a Cleanup error. If a deadline or an
integrity failure already determines the outcome, that failure stays primary
and retains the rollback failure as supplementary evidence.

An elapsed precommit retry deadline or an independent abort or reopen observer
deadline remains the primary timeout. The last observed storage failure is
supplementary evidence, including any nested rollback failure; it does not
become the source of the elapsed deadline. After an uncertain consuming commit,
that StateCommit error remains primary and a receipt-observation deadline is
supplementary evidence. Both paths preserve the original bound and cancellation
authority.

A definite rejected abort retains the prompt as recovery-owned unless another
typed terminal fact is available. A definitely accepted abort is distinct from
a rejection even if its HTTP response is later lost; durable state must remain
capable of reconciling the accepted cancellation rather than leaving every
future prompt permanently `Busy`. Timeout or connection loss after request
dispatch remains ambiguous and is reconciled from verified session status,
typed events, and durable generation state.

Native prompt observation is an explicit Active or Idle fact. Abort progress
retains a prospective deletion outcome before its consuming commit. That
attempt is not terminal publication: an exact durable deletion receipt permits
Settling(exact outcome); required continuation confirmation permits
Settled(exact outcome) and slot release. A participating observer's loss leaves
the prospective outcome and any observed commit failure on the existing
generation reservation; it cannot authorize another deletion or turn a repaired
outcome into ordinary success. Definite rejected commits clear only the matching
still-prospective attempt. Present, foreign, malformed, or unavailable receipt
observations do not confirm deletion. Receipt and continuation observation use
the original deadline. Absence of native status does not synthesize Idle, and a
repaired-invalid-state outcome cannot be collapsed into ordinary completion.

Successful event recovery and confirmed interaction reconciliation wake the
owning foreground to re-observe native status, including a quiet reconnect with
unchanged history and no pending interaction. That wakeup carries no Idle or
terminal authority: the existing foreground owner must obtain a fresh typed
status response, retain the generation on Active or observation failure, and
check the original continuation and cancellation facts before idle repair.
A failed status observation cannot make a live response-loss owner depend on a
future native status event that the recovered process may never emit.

A cancelled native tool can leave its permission or question pending after the
session becomes idle. Portable notification cancellation alone does not settle
that native obligation. The original generation and request-part identity stay
retained until selected interaction cleanup has authoritative acknowledgement.
This also applies when malformed or lost foreground response bytes have already
ended observation: the original response-loss record and cancellation facts
remain intact while cleanup proceeds, so later reconciliation preserves the
existing complete versus repaired-invalid-state distinction. Reopen also retains
an admitted generation after an accepted abort until its owned pending native
interactions settle, before publishing the reopened Agent. Accepted cancellation
allows preparation under the existing recovery owner; rejection still requires
the exact original request and matching cancelled native tool/message evidence.
Idle status alone never authorizes rejecting an interaction.

Generation-owned event synchronization reconciles the owning conversation.
An uncertain interaction cleanup in another conversation must not become this
prompt's failure or invalidate its otherwise healthy event connection. Complete
native inventories still participate in binding, duplicate identity, and
permission collateral-effect validation. Startup and deliberate global recovery
retain their full reconciliation obligations.

A normalized execution error remains owned before asynchronous persistence and
interaction cleanup begin. A later event-stream interruption cannot replace it;
losing the last hierarchy handle retains that complete known error together with
the owner-stop cleanup failure, subject to the existing publication boundary.
An interruption before a complete execution error exists keeps its ordinary
observation-error behavior.

Natural prompt completion racing abort uses one durable terminal winner. A
late abort result cannot overwrite a completed response, and a late response
cannot publish success for a cancelled generation. Lost exact response bytes
are never reconstructed from history.

## Notifications And Interactions

All passive and actionable delivery uses the exclusive portable receiver
described by [`NOTIFICATIONS.md`](NOTIFICATIONS.md). Permission and question
requests use the same portable Interaction notification and resolution path
described by [`SERVER_REQUESTS.md`](SERVER_REQUESTS.md). Conversation code
orders StateStore transitions, response dispatch, and public publication after
domain locks are released; protocol and notification decoders do not perform
those side effects directly.

## File Capability

The selected OpenCode release cannot satisfy portable file reads. Its file route
trims successful text and returns the same empty text result for a missing file
and a genuinely empty file. The conversation therefore returns `Unsupported`
without calling that route. It does not list before reading, inspect the local
filesystem, trim caller data, or synthesize `NotFound`.

## Close

Conversation close is terminal. It seals prompt and read admission, dispatches
generation-fenced abort when required, cancels pending permission and question
requests through their selected routes, allows already-resolving interactions
to retain their first terminal outcome, waits for accepted work and delivery
to settle, synchronizes the sole event owner before StateStore transaction
admission, records the native subtree cleanup obligation, terminally commits the
conversation close, removes the live route, and closes the receiver.
The event-command capacity reservation and synchronization completion share
that close operation's one lifecycle deadline. Bounded command pressure waits
in FIFO order and expires as `Timeout`; it is not hierarchy `Busy`. The close
application owner checks the same deadline before durable admission but does
not cancel an admitted transaction. An applied or
exact-confirmed close publishes receiver and hierarchy terminal effects before
returning success, including when commit observation finishes after that
deadline. An ambiguous commit starts a separate receiver-owned, read-only
terminal-reconciliation episode; it cannot readmit or replay the close mutation
or extend application authority. Its StateStore owner always makes at least one
exact observation attempt and bounds transient-read retries with one deadline
derived from the existing lifecycle policy. If durable truth remains
unobservable, it ends local delivery and hierarchy ownership before returning
the original commit error so reopen alone can resume recovery.

Close never exposes or returns a native id. A receiver remains pending after a
prompt terminal while the conversation is open and returns terminal end only
after close and durable drain. Dropping a conversation handle alone is not
terminal close.

## Required Invariants

- One conversation owns one native root closure and one active generation.
- The foreground HTTP response alone owns exact prompt output.
- Server message identities, SDK generation, and transport correlation remain
  distinct.
- No lock or transaction crosses HTTP, SSE, database, timer, or cancellation
  awaits.
- Every accepted operation reaches one durable generation-matching terminal
  outcome or remains explicitly recovery-owned.
- Structured-parent recovery uses the Agent's sole current event stream and
  the caller's existing absolute deadline.
- OpenCode's lossy file route is never presented as portable exact-byte I/O.
