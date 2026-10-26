# Portable Conversation Domain

This document owns caller-visible conversation metadata, foreground prompt,
abort, exact file read, notification entry, recovery, and terminal close.
Exact public types and operations come only from [`API.md`](API.md).
[`RUNTIME.md`](RUNTIME.md), [`NOTIFICATIONS.md`](NOTIFICATIONS.md), and
[`STATE.md`](STATE.md) own their cross-cutting invariants.

## Durable Conversation

A Conversation is bound to one Agent, one Workspace, one observed Workspace
revision, and one backend-private attachment. It owns one foreground prompt
generation at a time, one exclusive durable notification receiver lease, one
portable interaction lifecycle per actionable notification, and terminal
close.

Complete optional conversation defaults are durable and validated as one
value. Reopen never reconstructs them from a native session or field-specific
fallback records. Lost nonresumable work is reconciled before a live handle is
published.

## Foreground Prompt

`prompt` is async foreground work. It validates the complete request and
effective model, personality, reasoning, cwd, workspace policy, timeout,
instructions, knowledge policy, structured schema, and tool selection before
native/provider acceptance. It admits against the Agent-wide limit and the
Conversation's single slot.

After acceptance, runtime—not the caller future—owns terminalization. Dropping
the future records waiter loss and discards exact terminal output only after a
durable terminal transition; it does not cancel or free the slot early. Prompt
and abort are never blindly replayed after ambiguous acceptance.

A caller prompt deadline is distinct from transport and lifecycle deadlines.
After accepted timeout it records timeout intent and uses generation-fenced
cancellation, while later prompt admission stays `Busy` until reconciliation
proves no delayed cancellation can strike it.

Success returns exact backend/provider-authored text, a separately validated
structured value only when requested, and complete-operation usage only when
sourceable. The SDK does not trim, synthesize, reserialize parsed output into
text, or reconstruct lost exact output.

## Abort

Abort is an idle no-op or joins cancellation of exactly the active generation.
Cancellation dispatch occurs at most once. Natural terminalization may win,
and only the prompt waiter receives the prompt outcome. Timeout/ambiguity
retains the generation fence and durable recovery ownership.

Parent close and shutdown use the same backend cancellation owner with their
own lifecycle meanings; they do not create a second cancellation path.

## File Reads

File reads validate input relative to the admitted current
`WorkspaceContext.cwd` without cleanup or normalization, require the resolved
target to remain within a readable declared root, and validate the current
Workspace revision/policy before I/O. Empty paths, NUL, either root form,
ASCII drive prefixes, empty, dot, or parent components, and repeated or
trailing separators are rejected. Successful bytes are exact, the accepted
path spelling is preserved, and opaque `FileRead` text/binary content is
observed only through its accessors. A selected route that cannot preserve
this contract is honestly unsupported. A
definite but untyped remote filesystem rejection follows its backend's
documented portable category and is never parsed to fabricate `NotFound`.

## Notifications And Interactions

The Conversation exposes only the one exclusive durable receiver and one
resolution operation. Codex events/requests, OpenCode SSE and
permission/question routes, and Custom server/provider/tool facts all enter
that same order. No polling or backend-specific public path exists.

Prompt terminal output and errors remain on the prompt return path. Passive
and actionable facts use notifications. Resolution commits intent before its
non-idempotent backend/provider/tool continuation and never resends known or
ambiguous acceptance.

## Terminal Close

Close is consuming and terminal. It seals admission, cancels/reconciles active
work, terminalizes interactions, commits final durable notification state,
releases backend/tool/provider resources, removes conversation knowledge, and
records cleanup obligations before the root drain/deletion boundary.

No close report exposes private cleanup steps. An error consumes the handle and
leaves durable reconciliation state; it does not restore a partially closed
Conversation.
