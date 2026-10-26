# Codex Server-Request Domain

This document owns the Codex `server_requests/` domain for JSON-RPC requests
initiated by App Server. Exact selected method names and wire shapes remain
source authority in [`PROTOCOL.md`](PROTOCOL.md). Public callers see only the
portable notification and resolution surface in [`../API.md`](../API.md).

## Inventory And Direction

Directional JSON-RPC decoding determines that a frame is an App Server
request before method-specific handling. A request always receives one
JSON-RPC response; it is not converted into a notification frame merely
because handling may require a portable Interaction.

The selected inventory groups into these typed behaviors:

- command execution, file change, and permission approval;
- tool user input and MCP form elicitation;
- dynamic tool invocation;
- ChatGPT auth-token refresh;
- attestation generation; and
- thread-scoped current-time reading.

The exact methods are the pinned list in the protocol contract. Known payloads
must decode to their selected typed DTOs. Deprecated methods, grouped approval
catch-alls, unknown variants, wildcard request variants, generated tables, and
generic success paths are forbidden. Malformed known input is `Protocol`.

Raw JSON-RPC identifiers remain private and are preserved exactly for the
response. A stable internal key may correlate durable continuation state but
never replaces the original string-or-integer id on the wire or enters the
public API.

## Scope And Route Ownership

Thread/turn-scoped requests route only after durable binding and descendant
ownership validation. Connection-scoped auth or attestation requests remain
connection-owned and never select an arbitrary conversation. A missing,
foreign, stale, or ambiguous thread binding fails explicitly without leaking
the actual owner.

Protocol owns routing and the response writer. The server-request domain owns
typed request semantics and one private continuation lifecycle. The
conversation coordinates durable state, notification publication, native
response execution, and resolution. These responsibilities do not collapse
into one manager or command bus.

## Portable Interaction Mapping

Command (including terminal input), file, and permission approvals publish
approval Interactions. Tool user input and MCP elicitation publish
schema-backed Interactions whose
portable schema accurately describes the selected response. Backend-specific
question ids, permission scopes, form DTOs, and response variants remain in
the private continuation.

Resolution validates both the root portable rules and selected request shape
before native response. Allow grants only the least-privilege one-shot native
acceptance; Deny grants nothing. Submit maps validated content to the exact
question/form response. Dismiss is permitted only for a dismissible request
and uses the selected cancellation shape.

Command approval `kind` defaults to `command` when omitted. `writeStdin`
presents terminal input through the same approval Interaction and existing
continuation. Unknown, null, or non-string kinds are `Protocol`. Allow sends
only `accept`; Deny chooses an offered `decline`, otherwise an offered `cancel`.
Dismiss requires offered `cancel`. Session approval and policy amendment are
never substituted. Distinct callbacks may share an item identity: exact raw
string/integer RPC IDs, not item IDs, distinguish their one-shot responses.

User-input `isBlocking` defaults to true when omitted or null. Blocking
requests retain complete multiline question text and exact option labels.
`autoResolutionMs` is deprecated: validate its optional unsigned-integer shape
but never use its presence to decide policy. A false `isBlocking` receives one
safe `Unsupported` response and no actionable durable notification. Upstream
still waits for a response; its automatic-response UI policy has no portable
equivalent. The SDK supplies no countdown, guessed choice, or lost responder.

Selected permission paths use wire strings that can carry foreign executor
locations. Malformed values remain `Protocol`; valid URI or foreign absolute
locations outside the local workspace representation are `Unsupported` before
publication. No remote filesystem support or relaxed local restriction follows.
The selected `LegacyAppPathString` convention treats any two leading slash or
backslash characters as Windows syntax, including forward and mixed-separator
UNC paths. On POSIX hosts these remain foreign and cannot become actionable
local permission notifications merely because host path parsing calls them
absolute. Host-native paths still require the existing absolute-path validation.

Close cancellation is deterministic and conservative. It never fabricates
approval, silently ignores required input, or invents a success response. If
the selected source lacks a true cancellation response, the backend uses only
the explicitly specified safe representation and records terminal
cancellation.

## One Continuation Lifecycle

Each actionable request has one data-carrying private owner that preserves:

- typed request kind and exact raw JSON-RPC id;
- SDK conversation/notification ownership;
- the native response continuation;
- pending or resolving state with one private per-attempt correlation;
- definite rejection versus proven or ambiguous response acceptance; and
- one resolved or cancelled terminal outcome.

It is not split into pending/responded/resolved carrier structs, and delivery
does not create a second lifecycle. Durable transition to resolving precedes
the non-idempotent response outside storage and synchronization critical
sections. Definite pre-acceptance failure may return to pending. Proven or
ambiguous acceptance never does and is never resent.

Publication atomically observes the prompt and inserts the notification and
its native continuation. A competing passive-notification acknowledgment can
conflict on the same journal. Before any response enqueue, only an exact
StateStore transaction conflict permits another publication attempt, bounded
by the existing lifecycle deadline. Each attempt rereads durable ownership
and callback identity; failed staging explicitly rolls back before retry.
Rollback failure remains supplementary to the original error and prevents
retry. Other storage errors and ambiguous commits are never retried here.
The same deadline governs admission to each new publication stage and commit.
Expiry after staging rolls back instead of admitting a commit; a commit
admitted before expiry retains its native authority and is awaited terminally.
Duplicate callback verification reloads the listed continuation inside that
transaction: a stale Pending inventory cannot override a committed ambiguous
or terminal continuation, even if the notification journal remains resolving.
Successful publication has one durable interaction and one continuation with
the original JSON-RPC id; native response ambiguity remains separately fenced.

The shared root resolution contract owns ambiguous intent-commit proof: only
an exact read of this notification with the same unconfirmed private attempt
may continue to the native response, and settlement by that intent requires
the same attempt. The correlation never replaces the raw JSON-RPC id.

Lifecycle cancellation checks its existing absolute deadline before admitting
each resolution. Once intent, native response, or settlement work is admitted,
the cancellation owner awaits that work terminally and checks the same deadline
afterward; an outer timer never drops an admitted durable transaction. The first
resolution or lifecycle failure remains authoritative while all spawned work is
still joined.

For the selected server, local response enqueue is not native acceptance.
`serverRequest/resolved` contains only thread and request identity and is
emitted after the response callback is consumed or removed by lifecycle
cleanup. It therefore proves only that the callback is no longer pending; it
does not prove which cause won and never promotes a resolving continuation to
accepted.

After one response has been enqueued, the SDK durably retains ambiguous
acceptance, returns `Conflict`, and never resends. A matching
`serverRequest/resolved` may record responder disappearance but cannot by
itself clear that ambiguity, acknowledge the portable Interaction, or report
success. Only separate authoritative lifecycle evidence may terminally cancel
the continuation. Duplicate, late, foreign, wrong-kind, or contradictory
reconciliation cannot resurrect or retarget it. Process/connection loss
terminally cancels a request whose exact responder no longer exists; reopen
never presents it as resolvable. Handler-local dynamic-tool interactions remain
governed by their direct continuation acceptance because the server emits no
`serverRequest/resolved` for them.

## Dynamic Tool Requests

`item/tool/call` delegates execution to [`CATALOG.md`](CATALOG.md). This domain
validates native request identity and owns the one-shot responder, but does not
execute `ToolHandler`, inspect tool source identity, or map tool-sdk internals
to public API.

A completed handler result maps to the sourced dynamic-tool response. A
handler execution error maps to a failed tool result rather than JSON-RPC
transport failure. A handler-requested caller interaction uses the same
portable notification path, resumes the same handler after resolution, and
terminalizes if its continuation is lost. App Server provides no separate
`serverRequest/resolved` fact for this handler-local interaction, so accepted
handler continuation resolution commits terminal directly rather than waiting
for a nonexistent acknowledgement.

## Immediate Connection And Thread Requests

ChatGPT auth-token refresh is answered as `Unsupported`; attestation generation
is answered as `NotYetImplemented`. They create no public notification and do
not gain a fabricated conversation owner.

`currentTime/read` is thread-scoped but needs no caller interaction. After
binding validation, the SDK process returns its wall clock in whole Unix
seconds through the private immediate response. It writes no durable request
or notification state. Pre-epoch, out-of-range, or contradictory ownership
fails explicitly.

## Close And Recovery

Conversation close settles pending and resolving server requests before
delivery seals. Pending requests receive their safe cancellation response when
the continuation still exists; accepted or ambiguous requests reconcile from
evidence and are not resent. Public actionable notifications reach the same
first durable terminal outcome.

Recovery uses durable continuation/acceptance facts plus selected native
reconciliation. It does not reconstruct lost JSON-RPC responders, replay
responses, infer acceptance from notification absence, or expose native ids.

## Required Boundaries

- No public Codex request/response DTO, method enum, JSON-RPC id, dynamic-tool
  payload, or backend-specific resolution API.
- No protocol route owns durable interaction state, ToolHandler execution,
  portable notification insertion, or close sequencing.
- No standalone response helpers, generated tables, macros, wildcard routes,
  aliases, or generic command bus replace typed receiver ownership.
- No lock or transaction crosses responder, tool, notification, cancellation,
  or timer awaits.
- Safe public errors contain no method name, route, native id, raw payload,
  secret, or lower-crate error value.

## Validation Obligations

Static contracts prove exact selected inventory coverage and deprecated/catch-
all absence without freezing private enum/helper names. Real strict-live tests
prove every selected behavior, malformed payload rejection, portable approval
and schema interactions, least-privilege mapping, dynamic tool success/error
and suspended interaction, current-time response, auth/attestation policy,
response acceptance ambiguity, duplicate/late reconciliation, process loss,
and close-time cancellation against the selected App Server.

## Selected Approval And Input Fields

Command approval `kind` defaults to `command` only when omitted. Null, wrong
types, and unknown kinds are Protocol. `writeStdin` uses the existing Interaction
with title `Terminal input approval`; ordinary commands retain `Command
approval`. Stdin presentation uses a supplied nonempty reason or `Approve
terminal input?`, never the raw synthetic command containing a process ID.
Raw JSON-RPC request ID alone correlates the response: callback approvalId and
parent itemId are distinct, and multiple callbacks may share one item. Stdin
approval does not publish a second Process Running. Acceptance is one-shot;
advertised denial/cancellation remain authoritative, without session grants or
policy amendments. Response enqueue ambiguity and resolved-callback semantics
are unchanged.

Tool user input defaults `isBlocking` to true for omission or null. True keeps
the durable schema-backed interaction. False is Unsupported because the portable
contract has no nonblocking/default-answer policy; the native server still
awaits a response and does not enforce a client countdown. Wrong types are
Protocol. The deprecated autoResolutionMs is type-checked but does not decide
support. Rejected requests create neither timers, answers nor interactions.

MCP `openaiForm`, `openai/form`, `openai/userVerification`, and URL elicitation
are privately typed and answered once with Unsupported. Ordinary forms retain
exact integer schema/value handling. No proof challenge, credential or native
verification payload enters a portable notification. Initialization advertises
none of these extended capabilities.

Approval paths use native LegacyAppPathString wire semantics. Non-string fields
are Protocol, including when another field has unsupported path context. Local
absolute paths retain supported behavior. Relative or foreign execution-context
strings are Unsupported rather than locally resolved or called malformed. No
SDK cwd conversion or remote-path API is introduced. Permission grants remain
one-turn with the existing denial behavior.
