# Portable Errors

`KitError` is the sole public error. [`API.md`](API.md) owns membership; this
document owns category meaning and mutation success boundaries.

## Categories

`KitError` is an opaque, cloneable, nonserializable owner. Its thirteen broad
categories live in `error::ErrorKind`; `error::Operation`, `error::ErrorPhase`,
`error::ErrorContext`, `error::ErrorDetail`, and `error::DeadlineKind` provide
closed, portable inspection. [`API.md`](API.md) owns exact membership.
Causes and supplementary failures are normalized Agent errors, never raw
lower-crate sources. Kit-owned messages contain no provider/native identity,
route, key, secret, raw payload, lower-layer prose, or retry token.

The operation identifies the caller request and the phase identifies the
detecting step. Outer publication preserves precise inner phases and complete
error values. A dependency cancellation, deadline or conflict does not prove
durable Agent cancellation, local deadline expiry or transaction conflict.
The observed HTTP status is evidence, not an instruction to retry.
When an independent reconciliation deadline expires, the last observed recovery
failure is supplementary evidence, not the timer's cause. Preserve that failure's
own causal and cleanup evidence within its supplementary subtree.
When hierarchy invalidation rejects publication, that rejection remains primary
in the `Publication` phase; an already observed backend failure remains
supplementary evidence.

Error trees retain at most eight levels and 32 nodes, including the root, with
the primary and causal chain preceding supplementary failures in insertion
order. Safe messages are limited to 4 KiB each and 64 KiB collectively at UTF-8
boundaries. `diagnostics_truncated()` reports omitted evidence. Ordinary
formatting and `std::error::Error::source()` remain safe.

| Condition | Category |
|---|---|
| Invalid passive config, missing required build integration, incompatible new-build inputs, or effective integration configuration that prevents a selected operation | `InvalidConfig` |
| Malformed caller runtime data, oversized caller payload, invalid path syntax, or invalid resolution | `InvalidInput` |
| Missing or foreign typed id, missing notification, or a backend-mediated workspace-relative file proven missing by selected typed evidence | `NotFound` |
| Stale revision or handle, reopen attachment/config/tool/knowledge mismatch, or ambiguous non-idempotent acceptance | `Conflict` |
| Sealed runtime or terminal conversation | `Closed` |
| Live prompt, interaction, admitted policy-sensitive operation, setup, close, or recovery blocks progress | `Busy` |
| Accepted work durably reached cancellation | `Cancelled` |
| Any configured operation deadline elapsed | `Timeout` |
| Selected backend/config cannot preserve the portable semantics | `Unsupported` |
| Semantics are possible but the selected adapter is incomplete | `NotYetImplemented` |
| Malformed external wire data violates its sourced schema/framing, or a selected backend/provider success violates the requested structured-output contract | `Protocol` |
| Owned durable or runtime facts contradict themselves independently of alternate caller input | `InvalidState` |
| Provider, storage, tool, HTTP, process, IO, or other dependency execution failure | `Integration` |

Busy may clear after lifecycle progress; Closed will not accept work in that
runtime. Timeout does not invent a terminal: accepted unresolved work remains
busy/recovery-owned. Rich lower-layer sources stay private.

Custom classifies complete normalized model responses before continuing its
loop, as specified in [`custom/CONVERSATION.md`](custom/CONVERSATION.md).
Failed, truncated, and filtered completions and empty completed tool results
are `Integration`; an unimplemented paused-turn continuation is
`NotYetImplemented`; missing or contradictory completion classifications are
`Protocol`. These outcomes retain safe messages without embedding provider
text, tool output, or lower-crate error values.

`Unsupported` describes one source-proven operation, field, selected-version,
or policy mismatch. It cannot stand in for deleting an otherwise required
backend Agent runtime, Workspace/Conversation hierarchy, durable state,
notification/request path, managed knowledge integration, tool catalog, model
inventory, recovery owner, or cleanup domain. `NotYetImplemented` likewise
identifies a finite adapter gap and must not become a permanent backend-wide
capability contraction.

## Success Boundaries

Before an owning durable success boundary, failure returns an error and leaves
portable state unchanged or explicitly recovery-owned.

Workspace update/delete commit is success. Postcommit invalidation, native
cleanup, or cleanup retry cannot turn it into error. Notification resolution
succeeds only when native acceptance and durable terminalization agree;
ambiguous acceptance is not resent blindly.

Consuming close/shutdown may error after attempting cleanup. The handle is not
restored; durable state owns later reconciliation. Secondary cleanup failures
do not replace the primary error.

## Caller Use

InvalidConfig/Input require correction. NotFound/Closed normally end the
addressed flow. Conflict requires fresh information. Busy may be retried after
settlement. Timeout requires reconciliation before repeating non-idempotent
work. Protocol/InvalidState are integrity failures. Integration retry depends
on operation idempotency, never message parsing.

Display and tracing redact secrets/private identity.
