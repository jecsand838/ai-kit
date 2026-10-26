# Backend Domain Architecture

This document owns backend-domain responsibilities across Codex, OpenCode, and
Custom. Exact public signatures remain in [`API.md`](API.md).

## Complete Backends

Every built-in implements the public `Agent -> Workspace -> Conversation`
hierarchy through associated child types. Caller implementations remain
concrete or use caller-owned exhaustive enums. Supplied handles dispatch only
Codex, OpenCode, and Custom without erasure or normalization.

Codex, OpenCode, and Custom each own durable state, hierarchy lifecycle,
foreground prompting and cancellation, notification delivery and resolution,
private managed knowledge, tool inventory, recovery, and deterministic
cleanup. A selected-version limitation applies only to the operation it
prevents; it does not authorize fabricated success or silent field loss.

## Required Private Domains

Each backend keeps clear private ownership for:

- agent construction, live resources, reopen, and shutdown;
- workspace revision, attachment, conversation inventory, update, and close;
- conversation prompt admission, terminal publication, cancellation, reopen,
  and terminal close;
- typed native fact classification and durable notification delivery;
- actionable continuation ownership and portable resolution;
- backend-private records and reconciliation over `AgentStorage<S>`;
- managed-knowledge prompt adaptation;
- tool installation, inventory, invocation adaptation, and teardown.

Codex and OpenCode additionally own selected protocol transport and typed wire
models. Custom instead owns a private prompt loop. File layout is not a public
contract: domains may be split or folded when ownership remains clear.

## Custom Ingress

Custom receives a direct `P: provider_sdk::ProviderBackend`, resolves the
selected `P::Model`, and advances each prompt through the public model
contract. Its Model-associated `Continuation` stays typed and run-local while
the returned assistant message advances that prompt; it is neither a durable
record nor an independent Agent parameter. The Model-associated Session also
remains provider-owned and is not an independent Agent parameter. Custom—not provider-sdk—owns transcript
advancement, tool execution, approvals, loop limits, shell-as-tool behavior,
cancellation, durable notifications, and final-response detection.

Provider and tool protocol envelopes stay within their owning crates. The
Custom runtime is not a public service, serialized pseudo-protocol, generic
driver, mount registry, or compatibility layer.

## State And Abstraction Rules

Shared agent code may own only truly backend-neutral validation, hierarchy,
notification storage, and managed-knowledge mechanics. It must not absorb
concrete protocol, tool, attachment, recovery, or prompt-correlation state.
It propagates `S: StateStoreBackend`, `D: DatabaseBackend`, and
`P: ProviderBackend` statically and never erases, normalizes, or identifies a
Provider or storage implementation.

Domain owners use private fields, data-carrying state enums, receiver methods,
and bounded owned tasks. DTOs remain passive. No synchronization guard or live
StateStore transaction crosses provider, tool, database, notification,
cancellation, process, or timer waits.

Public behavior is limited to `AgentBackend`, `WorkspaceBackend`, and
`ConversationBackend`. Do not introduce public knowledge behavior, factories,
conformance owners, child ingress constructors, duplicate private behavior
traits, macro dispatch, generic repositories, callback registries, or shared
lifecycle frameworks.

## Verification Boundary

Static contracts prove public exports and privacy, associated-child shape,
opaque handles implementing their traits, direct non-Clone caller hierarchies,
closed built-in behavior, and absence of erased ingress or retired APIs. They
do not lock private helper names, exact file topology, or source spelling.

Deterministic tests prove lifecycle, cancellation, conflict reconciliation,
redaction, notification ownership, and cleanup. Strict-live tests prove real
Codex, OpenCode, Custom-provider, tool, process, and persistence behavior.
