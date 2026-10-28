# OpenCode Interaction Request Domain

This file defines the backend-private permission and question continuations
that become portable Interaction notifications. The root behavior and public
resolution forms remain owned by [`../NOTIFICATIONS.md`](../NOTIFICATIONS.md)
and [`../API.md`](../API.md). OpenCode request ids, session/message/tool-call
ids, native route names, payloads, and response envelopes never cross that
boundary.

## Selected Permission Shape

A selected permission request contains a branded native request id, an owning
session id, a permission string, an ordered pattern list, metadata, an ordered
set of native always-rules, and optional tool origin containing native message
and call identities. Required fields must be present with the selected JSON
types. Plain source-owned strings are not trimmed or given invented nonempty
constraints.

After ownership validation, a pending permission becomes a portable approval
Interaction with no response schema. The title and body contain safe,
backend-neutral human context derived from the typed permission and patterns;
metadata and native ids remain private. The approval is nondismissible.

Portable resolution maps as follows:

- `Allow` sends the least-privilege native one-shot reply;
- `Deny` sends native reject with safe reason context when supported; and
- Submit or Dismiss is `InvalidInput` for this nondismissible approval.

Native persistent `always` acceptance may be decoded as observed server state,
but portable Allow never fabricates or requests persistent approval.

## Selected Question Shape

A selected question request contains a branded native request id, owning
session id, an ordered question list, and optional tool origin. Each question
contains its prompt, header, ordered options, and optional multiple/custom
flags. Options contain source-owned label and description strings. Required
presence and JSON types are enforced; source-valid empty strings are retained.

The portable Interaction publishes one self-contained JSON Schema document.
Its `answers` array has one prefix item per ordered question, forbids additional
items, and sets both minimum and maximum length to the exact question count.
Each answer group accepts the selected single or multiple string structure
without adding content restrictions absent from the wire contract.
The question Interaction is dismissible because the selected reject route
provides a definite native dismissal operation.

Zero questions are valid selected structure. They produce an exact zero-length
answers schema, and `{ "answers": [] }` is a valid response. An empty string,
an empty group for one question, and an empty complete response for zero
questions remain representable. Wrong JSON types or wrong outer cardinality
are `InvalidInput`; the adapter must not reject source-valid empty content.

Submit maps the validated ordered values to the selected native array-of-array
answer body. Dismiss maps to the selected question reject route. Allow and Deny
are `InvalidInput` for a structured question.

## Durable Lifecycle

Each request has one durable owner associated with one portable notification:

```text
Pending -> Resolving(private attempt, acceptance evidence) -> Terminal(outcome)
```

Live asked events and pending-list snapshots enter that same owner. Snapshot
reconciliation converts the existing typed pending DTO directly; it creates no
synthetic event id, fake normal/sync envelope, separate event family, or
high-water update. Repeated discovery is idempotent and does not publish a
second notification. The private continuation retains the original typed native
request, including its optional tool origin and fields omitted from portable
rendering. Reused identities must match that original request and binding;
compacted notification text or a bounded event cache cannot substitute for this
authority. Records lacking sufficient original evidence fail closed for
automatic cancellation.

Resolution validates notification ownership, pending state, interaction kind,
schema, session ownership, and native binding, then durably enters Resolving
before releasing storage and synchronization and sending the non-idempotent
response. Only an exact durable read of the same unconfirmed private attempt
may recover an error returned by that intent commit, and intent-owned
settlement requires the same attempt.

Native event acceptance can race the caller's settlement transaction. An exact
StateStore transaction conflict at begin, read, write, or commit retries the
complete local settlement with the same intent and existing lifecycle deadline.
The first durable terminal outcome remains authoritative. This never resends
the native response. Other storage failures are not retried; an ambiguous
commit retains exact read-only outcome reconciliation.

- Proven pre-acceptance failure returns to Pending and leaves the Interaction
  unresolved.
- Proven native acceptance records terminal acceptance and never resends.
- Ambiguous post-dispatch loss records recovery-required acceptance evidence
  and never blindly resends.
  The no-redirect/no-reqwest-retry HTTP policy preserves actual initial
  connection refusal as preacceptance evidence. A selected HTTP400 validation
  rejection remains preacceptance; HTTP404, other failed responses, malformed
  acknowledgements, and lost response receipts do not prove the resolution was
  rejected. Native cancellation retains its existing durable dispatch fence
  even when the immediate transport reports preacceptance failure.
- A typed native replied/rejected event or pending-list absence may reconcile
  a resolving request only when its selected evidence is sufficient.

If durable settlement fails after an observed native failure, retain the
complete native failure as supplementary evidence on the owning settlement
error. A lost receipt never authorizes another native dispatch; a settlement
deadline retains its own authority and original deadline.

Duplicate, stale, foreign, terminal, wrong-kind, and wrong-conversation
resolution attempts fail without a native request. A late backend terminal
event is idempotent only when it agrees with the durable request identity and
outcome; contradictory evidence is `InvalidState` or `Protocol` according to
its source.

## Close And Recovery

Conversation open and deliberate reconnect perform one bounded pending-list
reconciliation within the verified native process incarnation. Inventory loss
after native restart is not acceptance evidence or a second delivery path.

Close rejects pending permissions and questions through their selected routes
when possible, then records one terminal cancellation outcome. It must not
relabel an already Resolving request as cancelled: known or ambiguous native
acceptance decides that owner's recovery. Parent cascade preserves durable
state for typed-id reopen, while terminal conversation close follows the root
terminal cleanup rule.

Automatic cleanup of a cancelled generation verifies the exact original native
request, session and tool call against the completed aborted Assistant and the
original user's supplied request-part identity. It retains the prompt generation
until every required native obligation settles. A terminal successful parent is
not automatic cancellation authority. The live foreground owner prepares late
Pending requests from the same generation against its retained, clearable
Cancelled record before releasing that generation; the terminal outcome remains
unchanged. An explicit consuming Close has separate
authority over an unchanged owned Pending request, including a source-valid
request without tool origin or a Kit prompt generation. It retains the actual
conversation claim and operation admission through queued work, native response,
and settlement even when the caller drops its observer. This excludes a competing
Kit reopen while admitted cleanup remains owned; it is not an irreversible
terminal marker or an atomic fence against external native clients. Active prompt
work is aborted first; a terminal prepass and the final post-abort reconciliation
share the original Close deadline.

Selected permission rejection affects every pending permission in the native
session. Before sending, cleanup validates the complete covered same-session
inventory against its exact owned requests and directories. Foreign, changed,
unowned, or separately resolving peers prevent the native action. The selected
successful HTTP acknowledgement confirms the covered set. When that response is
lost, each exact typed rejection event confirms only its own request; one peer's
settlement cannot clear another peer's uncertain attempt. Question rejection
remains exact-request scoped. A missing request, a bare 404, or idle status alone
does not prove this owner's rejection.
After confirmed native cancellation and application of buffered events,
reconciliation refreshes pending inventories under the same deadline before
using absence to settle another continuation; the earlier snapshot cannot
cancel a request admitted during that overlap.

Native cleanup reserves its exact durable attempt before dispatch. Definite
transaction conflicts may recompute only after confirmed transaction cleanup;
an uncertain commit admits only receipt observation. The native operation is
never automatically resent after an uncertain dispatch. A known successful HTTP
acknowledgement remains authoritative across event-stream interruption, while
stream and persistence failures retain their existing diagnostic precedence.
Rejected events for ordinary caller Deny or Dismiss still settle that caller's
accepted resolution; internal cleanup must not relabel such an owner. A later
separately admitted Close revalidates its own authority, and cannot replay an
already committed native cleanup attempt without sufficient settlement evidence.

No request-domain lock or StateStore transaction crosses an HTTP, SSE, timer,
or receiver await. The request owner validates and transitions; conversation
runtime sequences durable mutation, protocol response, portable notification
resolution, recovery, and cleanup.

## Required Invariants

- One native request maps to one durable Interaction owner and notification.
- Permission Allow is one-shot; persistent native approval is never fabricated.
- Question cardinality and source-valid empty values are represented exactly.
- Known or ambiguous acceptance is never replayed as Pending.
- Pending-list recovery fabricates no events and advances no event cursor.
- No request id, native origin, route, or wire DTO becomes public state.
