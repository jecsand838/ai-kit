# Codex Backend

Codex adapts one long-lived App Server connection. [`../API.md`](../API.md)
owns public surface; root specs own shared behavior and [`PROTOCOL.md`](PROTOCOL.md) owns wire facts.

## Domain Contract Map

The Codex backend retains the same functional domain partition as the other
agent backends. These documents assign durable ownership without prescribing
private Rust type names, leaf files, helper order, or synchronization syntax:

| Domain contract | Ownership |
|---|---|
| [`AGENT.md`](AGENT.md) | Agent construction, reopen, connection ownership, inventories, and shutdown |
| [`WORKSPACE.md`](WORKSPACE.md) | Workspace attachment, revision observation, conversation factories, and cascade close |
| [`CONVERSATION.md`](CONVERSATION.md) | Foreground prompt, abort, file, notification, resolution, and terminal close coordination |
| [`CATALOG.md`](CATALOG.md) | Executable tool validation, native dynamic-tool installation, execution, inventory, and teardown |
| [`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md) | Codex knowledge adaptation and prompt-context rendering over shared managed knowledge |
| [`NOTIFICATIONS.md`](NOTIFICATIONS.md) | Typed event classification and ordered portable publication |
| [`SERVER_REQUESTS.md`](SERVER_REQUESTS.md) | App Server request lifecycle, portable interactions, and dynamic-tool continuations |
| [`STATE_STORE.md`](STATE_STORE.md) | Codex-private attachment, prompt, interaction, notification, recovery, and cleanup facts |
| [`PROTOCOL.md`](PROTOCOL.md) | Selected transport, wire DTO, method, framing, and routing facts |

Each domain must have one honest backend owner and collaborate through typed
receiver operations. A private implementation may split a domain further, but
must not collapse protocol transport, passive persistence, runtime lifecycle,
or caller interaction ownership into one undifferentiated manager. Shared
agent core remains backend-blind. Backend-local adapters translate between
typed protocol DTOs, managed knowledge, tool-sdk values, and these domain
owners; they do not form another lifecycle or persistence owner.

## Build, Identity, And Context

The runtime owns initialization, negotiation, transport, and any SDK-spawned
process. Initialization verifies the reported server identity against the
release selected by [`../SPECIFICATIONS.md`](../SPECIFICATIONS.md) and
unconditionally negotiates experimental API support before an Agent is
published. Descendant inventory, background-terminal cleanup, and effective
feature verification require that capability even when no executable tools
are registered; rejection is `Unsupported`. A version mismatch has no
compatibility mode. SDK ids remain authority; a conversation keeps a private
binding to one Codex thread. Create commits that binding before publishing the
handle. Reopen checks durable attachment compatibility and verifies the bound
thread; it never substitutes a new thread for a missing one.

Default spawn owns the server process. TCP and Unix attachment require
exclusive server and thread ownership for this Agent runtime; sharing either
with another active client is `InvalidConfig` because native requests and
per-connection capabilities would have competing authorities.

Codex maps the complete `WorkspaceContext` to selected App Server cwd, runtime
workspace roots, sandbox, writable-root, and network policy. Paths name the App
Server host filesystem; remote TCP attachment is valid only when the caller can
represent those paths honestly. ReadOnly requires every root to be ReadOnly and
maps its selected network flag. WorkspaceWrite is directly representable only
when every declared root is ReadWrite: the selected built-in workspace profile
materializes every runtime workspace root as writable, so any mixed-access
WorkspaceWrite context is `Unsupported`. It requires cwd to lie in a declared
root and enables both selected temporary-directory exclusions so the native
sandbox does not add writable paths outside the portable policy. FullAccess
with enabled network maps to `DangerFullAccess`. FullAccess with disabled
network, or any other combination the selected server cannot preserve exactly,
is `Unsupported` before native creation.

The native `SandboxPolicy` constrains model shell execution; it does not by
itself confine client dynamic-tool callbacks, MCP calls, or web search. Any
mounted executable Toolset therefore requires `FullAccess` with network
`Enabled`. A ReadOnly or WorkspaceWrite context, or any context with network
Disabled, requires the complete paged MCP server-status inventory to be empty;
an ambient configured MCP server is `Unsupported` because agent-sdk cannot
prove its filesystem/network confinement. Network Disabled also supplies the
selected thread config override that disables web search. If selected policy
requirements reject that override or the backend cannot prove the required
effective state, workspace creation/reopen is `Unsupported`. Native transport
network needed to reach App Server is outside this model-visible policy.

A workspace update invalidates every prior handle after durable commit; reopen
creates state from the new revision. Conversation create and reopen read the
effective native policy and require it to match the complete portable policy.
A platform or managed-policy downgrade is `Unsupported`; an accepted native
create is cleaned up or retained as a recovery obligation rather than exposed
under broader or narrower semantics.

Each prompt's validated workspace-relative cwd resolves to a backend-host
absolute path and is sent through native `TurnStartParams.cwd` on every turn.
Native conversation creation sets `ephemeral = false` and proves the durable
thread attachment before publishing the Conversation. It also disables native
active `multi_agent` and `multi_agent_v2` features so a completed root turn is the portable
foreground terminal authority rather than the start of hidden descendant work;
that policy is part of attachment compatibility on reopen. Before publication
or reopen, the backend pages `experimentalFeature/list` for the loaded thread
and requires all three to report disabled. An enabled feature is `Unsupported`;
a missing or malformed effective-state entry is `Protocol`.
Agent-config instructions are fixed when the bound thread is created. The
native create request supplies developer instructions, and the private
attachment records their compatibility fingerprint. Reopen requires the same
instruction policy and exclusive caller ownership of the bound thread;
configuration mismatch is `Conflict`, while an unprovable effective policy is
`InvalidState`. A loaded `thread/resume` may ignore developer-instruction
overrides, so resume neither changes nor proves that policy. A prompt-level
instructions override is `Unsupported` because the selected turn request has
no equivalent field; it is never simulated by rewriting user text.

## Prompt, Files, Models, And Tools

Prompt starts one `turn/start`; the matching `turn/completed` fact is terminal
authority. Abort uses `turn/interrupt`. Terminal state commits before the
waiter observes a result, and transport timeout does not invent completion.

A structured prompt supplies the validated Draft 2020-12 schema through
`TurnStartParams.output_schema`. The selected server renders that contract
with its fixed native output-schema name and strict mode. Codex performs no
hidden structured retry. Terminal text is parsed and locally revalidated as
specified by the Conversation domain. Selected `turn/start` exposes no
per-turn tool-choice or availability field, so only absent or `Auto` portable
tool selection is supported; every narrowing or requirement variant is
`Unsupported` before native acceptance.

For `Completed`, output is the exact text of the last completion-ordered
non-whitespace `ThreadItem::AgentMessage` whose phase is `FinalAnswer` or
absent. `Commentary` and `PartialAnswer` are never terminal prompt output, and
selected text is not trimmed.
Candidate absence returns `NotYetImplemented` instead of inventing output.
`Interrupted` is `Cancelled`, with an optional normalized `TurnError` cause;
`Failed` requires `TurnError` and is `Integration` with safe context and a typed
portable detail normalized from
`codexErrorInfo`. The same normalization applies to authoritative `thread/read`
recovery. HTTP status is retained only when that typed native variant supplies
it. Native messages, additional details, and misalignment explanation, steering,
and review-target data stay private and are never persisted or executed. An
`error` notification with `willRetry` remains nonterminal; no retry advisory
authorizes SDK replay.
`InProgress` at terminal, missing failure
data, or an error on `Completed` is `Protocol`. The selected
notifications do not source whole-operation token totals, so usage is `None`.

`fs/readFile` returns exact bytes but applies no native sandbox. Codex therefore
implements portable file read after effective full-access verification. The
adapter maps valid UTF-8 without trimming and uses
`application/octet-stream` for binary data whose MIME is unknown.

The selected server returns successful bytes exactly but collapses definite
non-input filesystem failures into an untyped JSON-RPC rejection. The adapter
therefore retains safe JSON-RPC code classification in `Integration` and drops
the native server message; it does not inspect message text to manufacture `NotFound`,
expose raw error data or native identity, or retry the read automatically.
Malformed successful response shape or base64 remains `Protocol`.

Model inventory exhausts native `model/list` pagination, maps only selectable
models, and preserves sourced default flags subject to the root at-most-one
invariant. Multiple sourced defaults are `Protocol`; with none, every portable
entry has `is_default = false` rather than adopting the server's first-entry
fallback. Portable `ModelSelector` uses the
native `Model.model` slug used by selection/default checks; the catalog preset
`id` remains private. Reasoning effort remains an open validated value;
unsupported values fail before `turn/start`. The selected server retires
personality selection and reports `supportsPersonality=false`. All Codex model rows expose
an empty personality inventory, even if a contradictory deprecated flag is true.
Every explicit personality, including `none`, is `Unsupported` before native
creation, resume mutation, or prompt start. Persisted personality intent is
rejected rather than silently discarded; no migration or public API change is
introduced.

The selected `turn/start` model and reasoning-effort overrides are sticky for
that turn and subsequent turns. The backend tracks only private native
selection/reset evidence needed to preserve portable precedence. After an
explicit override, omitting a portable field may omit the native field only
when that omission restores the same effective value. If no exact reset value
is sourced, the prompt is `Unsupported` before native acceptance.

Agent-message `delivery=async` or non-null `questions` is `Unsupported` in
both live items and full history. The SDK cannot manufacture a responder or
publish these questions as successful final output. Malformed fields remain
`Protocol`. `FunctionCallOutput` is validated private data, never an assistant
answer or a second tool invocation.

Nonempty executable toolsets use the already-required experimental API:
thread creation supplies `dynamicTools`, and `item/tool/call` invokes the
installed tool runtime. Tool inventory returns only SDK-installed dynamic
registrations. Configured MCP and implicit Codex tools remain server-native
and are outside the portable agent-sdk-managed inventory.

App Server restores persisted native dynamic tools from session metadata on
resume, but the wire does not expose them for direct inspection. Agent-sdk
therefore compares its durable managed registration descriptor with the
integrations supplied for reopen before resuming; mismatch is `Conflict`.
Later native behavior contradicting that recorded contract is `Protocol` or
`InvalidState`, never silent tool substitution.

Managed knowledge uses the supplied Database and backend-neutral retrieval.
Codex formats the resulting private context into its native prompt.

## Events, Recovery, And Cleanup

Typed protocol scope and the single known-event classification decide every
inbound fact. Thread/turn events use durable bindings. Agent/workspace routing
requires sourced aggregate evidence. Connection-global input never acquires an
invented conversation.

A bound root thread and its sourced descendant closure have one portable
Conversation owner. Descendants remain private and acquire no public
Conversation; their events and interactions route through the root only after
native ancestry validation. Threads outside every bound closure are foreign.

Approvals, input requests, MCP elicitation, and dynamic-tool interactions use
the root interaction lifecycle. Codex request ids and response DTOs remain
private; accepted or ambiguous responses are never resent. A server request
lost with its JSON-RPC connection is not reconstructable and is terminally
cancelled during reconciliation rather than presented again as resolvable.

Reconciliation uses sourced thread reads and terminal/request facts. Active or
ambiguous native work remains `Busy`; contradictory durable/native bindings
are `InvalidState`, while malformed sourced responses are `Protocol`. It never
infers prompt success from thread existence or notification history.

Interrupt does not terminate background terminals. Any such terminal in the
bound native thread closure is busy descendant work, so workspace update or
delete returns `Busy` before commit. Workspace close and caller-managed Agent
shutdown request background-terminal cleanup for that closure. The clean
request is accepted before cleanup is necessarily complete, so the backend
exhausts native inspection until every thread is verified empty, then uses
`thread/unsubscribe` to release this connection without deleting the thread.
Failure remains a durable cleanup obligation and reopen returns `Busy` until
verification succeeds. Termination of an SDK-owned spawned process may itself
prove shutdown cleanup.

Workspace deletion records cleanup at portable commit, then sends one
`thread/delete` per bound root thread. Native descendant cascade prevents
redundant child deletes. Later `thread/deleted` broadcasts invalidate matching
private bindings and create no required public notification.

Terminal conversation close records the same cleanup obligation for that
conversation's one bound root thread before reclaiming its portable record.

Shutdown seals the runtime, settles descendants through the root lifecycle,
closes the transport, and terminates only an SDK-owned process. Failed
post-commit cleanup remains a durable obligation.
