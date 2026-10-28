# OpenCode Backend

OpenCode adapts one HTTP client and the selected global SSE stream.
[`../API.md`](../API.md) owns public surface; root specs own shared behavior and [`PROTOCOL.md`](PROTOCOL.md) owns wire facts.

## Domain Ownership

OpenCode preserves the same broad private domain architecture as the other
agent backends. The domain contracts are:

- [`AGENT.md`](AGENT.md) for connection, compatibility, workspace registry,
  model inventory, and shutdown;
- [`WORKSPACE.md`](WORKSPACE.md) for revision-valid workspace ownership,
  conversation lifecycle, catalog projection, and workspace close;
- [`CONVERSATION.md`](CONVERSATION.md) for foreground prompt, abort,
  notification receiver, file capability, recovery, and terminal close;
- [`CATALOG.md`](CATALOG.md) for discovery and binding of the server-owned
  built-in, custom, and MCP tool catalog;
- [`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md) for shared managed-knowledge
  composition and OpenCode-native prompt rendering;
- [`NOTIFICATIONS.md`](NOTIFICATIONS.md) for sole-stream typed event routing and
  portable delivery;
- [`SERVER_REQUESTS.md`](SERVER_REQUESTS.md) for permission and question
  continuation state;
- [`STATE_STORE.md`](STATE_STORE.md) for backend-private durable bindings,
  generations, interactions, event progress, and cleanup obligations; and
- `adapters/` owns narrow typed conversions between selected OpenCode facts and
  portable values without becoming another lifecycle or protocol owner.

These files specify semantic ownership rather than exact private file, type,
field, helper, or import spelling. They do not expand the portable API.

The production backend must keep those owners recognizable as dedicated
directory-backed domains: agent, workspace, conversations, protocol ingress,
notifications/events, server requests/interactions, state store, managed
knowledge, tools/catalog, and adapters. A thin backend root composes them. Exact nested
files, private type names, helper decomposition, records, channels, and call
syntax remain implementation-owned, but the responsibilities may not be
collapsed into one backend file or moved into a backend-aware shared runtime.

## Build, Identity, And Context

Spawned mode owns readiness and process shutdown; external mode owns neither.
Both modes validate the health-reported version against the release selected
by [`../SPECIFICATIONS.md`](../SPECIFICATIONS.md) before publishing an Agent;
only spawned readiness is retried, and mismatch has no compatibility route.
Spawned mode forces background subagents off through the selected native flag;
both modes verify `backgroundSubagents == false` before Agent publication or
reopen. A server that reports true is `Unsupported`, because its native work
can outlive the selected foreground response and re-enter the parent outside
the portable one-prompt lifecycle.
Kit ids remain authority, while private bindings associate workspaces with
routing context and conversations with native sessions. Create commits the
binding before handle publication. Reopen verifies it through the selected
session lookup and never creates a replacement session.

Before native session creation, the workspace-routed effective configuration
must report disabled sharing; missing, `manual`, or `auto` is `InvalidConfig`
and creates no session. Spawned mode also sets `OPENCODE_AUTO_SHARE=false`.
These remain Kit attachment requirements even though the selected native
server has discontinued sharing: creation delegates directly to Session,
sharing fails, and unsharing only clears the local link.
External attachment's exclusive-server contract includes stable effective
configuration throughout the check and create operation. Created and reopened
sessions must have no native share binding. A present binding is contradictory
native state; that contradictory native state is `InvalidState`. The backend
seals the runtime, publishes no portable handle, retains cleanup responsibility,
and never exposes or logs the native share URL.

OpenCode maps `WorkspaceContext` to unrestricted native access. Paths always
name the backend host filesystem. External attachment may be
cross-host only when those path semantics honestly represent the workspace;
otherwise workspace creation is `Unsupported`. A portable workspace is
Kit-owned directory context, not an OpenCode experimental-workspace record.
The selected unrestricted final rule represents only full-access,
network-enabled portable execution policy. Read-only, workspace-write, or
network-disabled context, including any root-level access restriction, is
rejected as `Unsupported` before native creation or durable update; normalized
unrestricted roots remain durable context but are not claimed to constrain an
explicitly full-access native runtime.
Native session creation appends
`{ permission: "*", pattern: "*", action: "allow" }` as the final session
permission rule; reopen verifies that exact effective rule before publishing a
handle. Missing or changed policy is `InvalidState`, never a
restricted runtime presented as unrestricted.
Durable update invalidates old handles; changing cwd while any conversation
remains is `Unsupported` because the selected protocol cannot migrate its
native session. With no conversations, or with unchanged cwd, reopen constructs
a runtime from the committed revision.
An explicit prompt cwd is also `Unsupported`: session routing uses the bound
session directory and cannot represent a per-prompt override.

OpenCode server-native tools and MCP state are preconfigured outside
agents-kit. Build discovers and binds the server-owned catalog but exposes no
hosted/MCP/header/OAuth config family. Caller executable `Toolset` input is
`Unsupported` because the selected protocol has no client-executed callback.
Catalog completeness and MCP limitations are defined by
[`CATALOG.md`](CATALOG.md).

## Prompt, Files, Models, And Tools

The selected foreground message HTTP response is prompt-terminal authority.
SSE supplies notification, interaction, and reconciliation facts but cannot
complete the prompt independently. A configured response deadline returns
`Timeout`; transport or connection loss returns `Integration`. Either can
leave possibly accepted work recovery-owned and later prompts `Busy` until
reconciliation proves it terminal or cancelled. Because a lost exact response
is unavailable, recovery discards any output instead of fabricating success.
Abort uses the selected native route and no mutating request is replayed after
ambiguity.

The effective caller prompt timeout is distinct from the HTTP transport
deadline and lifecycle timeout. Expiry after possible native acceptance records
generation-fenced timeout cancellation and retains `Busy` until delayed abort
can no longer strike a later generation. Expiry never authorizes prompt replay
or reconstruction of a lost response.

The request omits the optional native message id so the selected server
allocates both user and Assistant identities. The Kit retains a separate
private prompt-generation fence that is never serialized to OpenCode. It
supplies one private transport-only text-part
id, which the selected server preserves on the created user message. The
request leaves `noReply` false. Plain prompts omit `format`; structured prompts
send the validated JSON Schema format with `retryCount = 0` and require the
typed Assistant `structured` result. Its response must be a completed Assistant
for that session with a valid, distinct server-owned user parent. Native latest
selection uses creation time with identity as a tie-breaker; the adapter proves
exact request ownership without imposing lexical or timestamp ordering. Plain
prompts prove that ownership by reading the exact parent and requiring the same
session, User role, root-parent shape, and private request-part id. For a
structured prompt, the selected exact-message route cannot response-encode the
persisted User format after database hydration. The adapter instead requires
private typed `message.updated` and `message.part.updated` evidence from the
sole event/recovery path for the exact session, User root shape, returned
parent id, and private request-part id. Bounded `/sync/history` recovery fills
a missed live observation without replaying the prompt. The POST Assistant
remains the sole typed output authority in both cases.
Malformed shape is `Protocol`; a schema-valid wrong owner, unavailable claimed
parent, missing correlation part, or nonterminal response is `InvalidState`.
`MessageAbortedError` remains the selected cancellation authority. Other
Assistant errors retain normalized authentication, output-limit, context-limit,
filtering, or remote API rejection details; structured-output failure is
`Protocol`. Error messages, provider identifiers, response bodies, and headers
remain private. HTTP rejection status is retained separately without assuming
retry safety: an unqualified 429 proves resource exhaustion, not a particular
rate or quota cause. No error publishes a partial successful response.
Success output is the exact concatenation without
trimming or a delimiter of non-synthetic `TextPart.text` values in `PartID`
order; no such part yields an empty string. Response tokens describe the last
assistant step rather than the complete foreground operation, so usage is
`None`.

Structured output is locally revalidated and returned separately while exact
text output remains unchanged and may be empty. Only absent or `Auto` portable
tool selection is supported. The selected prompt `tools` map persists session
permissions rather than expressing one-turn choice, so narrowing or requiring
tool use is `Unsupported` before dispatch and is never emulated by session
mutation.

The selected file route trims valid text and maps missing files to empty text,
so it cannot satisfy byte-exact portable file semantics. OpenCode returns
`Unsupported` for `read_file`.

OpenCode model discovery uses the selected workspace-routed provider inventory
route from a stable agent-level context. `list_models` projects every model of
connected providers with an exact private provider/model selector and
empty reasoning-effort and personality inventories. Native model variants and
native agent names are arbitrary config-owned behavior with no sourced portable
semantic mapping and never become public selectors. Configurable native
`reasoning_options` remain backend-private; their effort, toggle, and token-budget
forms establish no portable effort semantics. Results are deterministic
and complete for the supported provider response. The selected route does not
prove one stable agent-wide default, so every portable `is_default` value is
false. Prompt execution revalidates an effective selected model in the bound
workspace context. Every non-absent effective reasoning or personality value
returns backend-neutral `Unsupported` before native mutation; complete absence
omits those fields and uses the server's private config/default selection.
[`AGENT.md`](AGENT.md) and
[`CONVERSATION.md`](CONVERSATION.md) own the complete behavior.

The selected server persists an accepted explicit prompt model on the bound
native session. When prompt, conversation, and workspace model selections are
all absent on a later prompt, omission intentionally permits that persisted
selection to participate in OpenCode's backend/default behavior. This is a
selected-version delta, not a portable default, caller configuration input,
or public backend context. A later explicit portable model still takes
precedence and is revalidated before dispatch.

The selected foreground payload maps the effective prompt/agent instruction
string to native `system`. Absence leaves server-owned instruction policy in
effect; instructions are never appended to user text.

OpenCode accepts no agents-kit-managed executable tool registration. Its
built-in, custom, and MCP planner remains server-native, but the Kit must not
therefore report an unconditional empty inventory. The selected experimental
tool routes support typed built-in/custom discovery. When connected MCP state
could contribute tools, the selected server lacks a complete tool-definition
inventory route, so `list_tools` returns `NotYetImplemented` rather than a
known-partial vector. [`CATALOG.md`](CATALOG.md) owns discovery, completeness,
binding, and lifecycle. Built-in/custom projection also requires a concrete
revalidated Workspace model because the selected description route is
provider/model-sensitive; absence is `NotYetImplemented` for `list_tools` but
does not block native prompt execution.

Managed knowledge storage and retrieval stay shared; OpenCode owns only scope
coordination and bounded final native context rendering as specified by
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md).

## Events, Recovery, And Cleanup

Reopen verifies the bound session before reading `GET /session/status`. A map
entry retains the execution as active/recovery-owned. Absence is sourced native
idle for that verified live server instance, but it does not invent a lost HTTP
result: durable `running` with no status entry is `InvalidState` and reconciles
to a non-success terminal recovery fact under the root state contract.

Spawned mode selects a stable Kit-private per-agent OpenCode database while
preserving the server's normal data home for credentials and configuration.
External attachment requires an exclusively owned server and OpenCode
database; endpoint locality alone does not prove isolation. Initial history
and bounded-complete session inventory must contain no foreign ownership, and
any later unknown aggregate violates that contract. Such a violation seals the
runtime against new work and leaves it recovery-required; prior durable owned
commits remain valid but the live Agent cannot continue under disproven global
ownership. The bounded recovery flow in `PROTOCOL.md` owns per-aggregate
progress and dedup compaction.
Portable delivery and interaction ownership remain the single paths defined by
[`NOTIFICATIONS.md`](NOTIFICATIONS.md) and
[`SERVER_REQUESTS.md`](SERVER_REQUESTS.md); native events and pending snapshots
do not create parallel public surfaces.

Workspace deletion commits portable removal and records each bounded native
subtree before requesting removal of its root session. A successful HTTP
response is not proof because the selected path can remove session inventory
and then absorb aggregate-event cleanup failure. The obligation remains until
global bounded-complete inventory proves every recorded id absent and bounded
sync-history recovery proves their event aggregates absent.

Terminal conversation close records and verifies the same single-root subtree
obligation for its bound session.

Shutdown seals admission and keeps the stream and HTTP owner available while
accepted work, interactions, descendants, and cleanup obligations settle.
After durable reconciliation completes or exhausts its reserved settlement
budget, it stops the stream, releases HTTP state, and terminates only an
Kit-owned server process.

A valid native session whose binding commit fails remains owned by its durable
`Creating` correlation for reopen; it is not deleted. An invalid native
session moves from that exact `Creating` record to the existing durable cleanup
record in one transaction before native deletion is attempted. Exact unchanged
truth leaves `Creating`; exact applied truth leaves cleanup. A still-unobservable
handoff invalidates the current runtime, performs no deletion, and leaves
whichever atomic durable state committed as the sole reopen authority; cleanup
and creation authorities are never published concurrently. Reopen that confirms
the handoff settles its staged
knowledge forget and native cleanup, finishes the cleanup record, and continues
that same construction under the original construction deadline.

One absolute Agent shutdown deadline owns that sequence. A phase cutoff derived
once from that deadline reserves its remaining budget for runtime teardown; it
does not create or extend a deadline. Exhausting the earlier settlement budget
never bypasses runtime teardown: the Agent concurrently invokes event-owner
and process/output-drain shutdown with the unchanged final deadline, including
the event task's abort-and-join path and the bounded process termination and
drain-join paths. The first shutdown failure remains primary; a later cleanup
or runtime-teardown failure does not replace it.
