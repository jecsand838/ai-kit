# OpenCode Protocol Contract

This file owns selected OpenCode wire facts; parent specs own root behavior,
[`../API.md`](../API.md) owns public surface, and [`BACKEND.md`](BACKEND.md)
owns capability differences. Domain sequencing belongs to
[`AGENT.md`](AGENT.md), [`WORKSPACE.md`](WORKSPACE.md),
[`CONVERSATION.md`](CONVERSATION.md), [`CATALOG.md`](CATALOG.md),
[`NOTIFICATIONS.md`](NOTIFICATIONS.md),
[`SERVER_REQUESTS.md`](SERVER_REQUESTS.md), and
[`STATE_STORE.md`](STATE_STORE.md); protocol code decodes and transports but
does not become those owners.

## Source authority

The selected snapshot's generated `packages/sdk/openapi.json` is primary
declaration authority for HTTP shapes, including unchanged session, prompt,
permission and question envelopes. `packages/docs/openapi.json` is a pointer
containing `../sdk/openapi.json`; unchanged pointer bytes do not establish an
unchanged generated artifact. Handlers, schemas, the event bridge and bus
establish behavior beyond these declarations. Coordinates are relative to its
single
archive root and are verified directly against the tracked archive. OpenCode
uses one long-lived HTTP client and one selected SSE stream; native identifiers
remain backend-private.

| Archive member | Symbol anchors | Contract fact |
|---|---|---|
| `packages/sdk/openapi.json` | `/session`; `/session/{sessionID}/message`; `Model`; `ProviderConfig`; `Config` | Generated HTTP route and payload authority; interleaved responses retain a boolean/object union with an open string field |
| `packages/opencode/src/config/v2-compat.ts` | `lower`; `Object.hasOwn(parsed.value, "permissions")`; `["agents", "agent", "mode"]`; `V2 permissions are not supported by OpenCode V1` | Reject V2 permission keys, including empty values, before configuration lowering; singular V1 permission remains supported |
| `packages/opencode/src/server/routes/instance/httpapi/groups/session.ts` | `SessionPaths.status`; `StatusMap`; `SessionPaths.get`; `SessionPaths.create`; `SessionPaths.remove`; `SessionPaths.prompt`; `SessionPaths.message`; `success: described(SessionV1.WithParts`; `SessionPaths.abort` | Selected status, session lifecycle, foreground, typed exact-message response encoding, and abort route declarations |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts` | `SessionHttpApi.status`; `statusSvc.list()`; `SessionHttpApi.get`; `SessionHttpApi.create`; `shareSvc.create(ctx.payload)`; `SessionHttpApi.remove`; `SessionHttpApi.prompt`; `JSON.stringify(message)`; `SessionHttpApi.message`; `MessageV2.get` | Selected status and lifecycle behavior, creation through SessionShare, raw streamed foreground response, exact message lookup, and boolean removal response |
| `packages/schema/src/v1/session.ts` | `OutputFormatJsonSchema`; prompt `format`; `type: Schema.Literal("json_schema")`; `schema`; `retryCount`; Assistant `structured`; `MessageUpdated`; `PartUpdated`; `PatchPart`; `TextPart`; `TextPartInput`; `id: Schema.optional(PartID)`; `ToolPart`; `ToolStatePending`; `ToolStateRunning`; `ToolStateCompleted`; `ToolStateError`; `Diff`; `Assistant`; `AssistantErrorSchema`; `WithParts` | A foreground prompt may request native JSON-Schema output, the terminal Assistant carries the typed structured value, and typed message/part events carry private ownership evidence; the same schema owns request-part correlation, session-owned file/tool/process lifecycle, response text, completion, error, and token-counter shape |
| `packages/opencode/src/session/prompt.ts` | `StructuredOutput`; `toolChoice: format.type === "json_schema" ? "required" : undefined`; `structured`; deprecated prompt `tools`; `sessions.setPermission`; `PromptInput`; `messageID: Schema.optional(MessageID)`; `id: input.messageID ?? MessageID.ascending()`; `part.id ? PartID.make(part.id) : PartID.ascending()`; `model: Schema.optional(ModelRef)`; `agent: Schema.optional(Schema.String)`; `system: Schema.optional(Schema.String)`; `variant: Schema.optional(Schema.String)`; `currentModel`; `input.model ?? ag.model`; `sessions.setAgentModel`; `Permission.merge(taskAgent.permission, session.permission ?? [])`; `runLoop`; `lastAssistant.parentID === lastUser.id`; `!["tool-calls", "unknown"].includes(lastAssistant.finish)` | Structured output uses an internal required tool and stores its result, while the deprecated tools map mutates persistent session permissions rather than providing portable one-turn selection; foreground fields preserve explicit native model/agent precedence, persisted session model before provider default, server allocation when optional user identity is absent, supplied part-id preservation, session permission precedence, and exact latest-user parent matching; tool-calls and unknown finishes continue the native loop, while native agent and variant values remain private |
| `packages/opencode/src/session/status.ts` | `SessionStatus.list`; `data.get(sessionID) ?? { type: "idle"`; `if (status.type === "idle")`; `data.delete(sessionID)` | The status map contains active state only; a missing entry is native idle for the verified live server instance |
| `packages/opencode/src/server/routes/instance/httpapi/groups/experimental.ts` | `CapabilitiesResponse`; `backgroundSubagents: Schema.Boolean`; `ExperimentalPaths.capabilities`; `ExperimentalPaths.tool`; `ExperimentalPaths.toolIDs`; `ToolListQuery`; `SessionListQuery`; `ExperimentalPaths.session`; `experimental.session.list` | Capability, native built-in/custom tool catalog, and bounded cross-project session inventory routes |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/experimental.ts` | `ExperimentalHttpApi.capabilities`; `flags.experimentalBackgroundSubagents`; `ExperimentalHttpApi.tool`; `registry.tools`; `ExperimentalHttpApi.toolIDs`; `registry.ids`; `ExperimentalHttpApi.session`; `sessions.listGlobal`; `limit + 1`; `x-next-cursor` | Runtime capability value, provider/model-sensitive native tool descriptions and ids, and bounded inventory behavior |
| `packages/opencode/src/session/session.ts` | `Metadata`; `Info`; `metadata: optional(Metadata)`; `GlobalInfo`; `CreateInput`; `createNext`; `metadata: input.metadata`; `parentID: optional(SessionID)`; `share: optional(Share)`; `Session.listGlobal`; `if (!input?.archived)`; `children(sessionID)`; `remove(child.id)`; `events.publish(SessionV1.Event.Deleted`; `events.remove(sessionID)`; `failed to remove session`; `getUsage` | Sessions preserve caller-supplied private metadata through creation, exact reads, and global inventory; they expose share state and native ancestry; root removal attempts descendant cascade but can absorb cleanup failure; counters normalize one model step |
| `packages/opencode/src/share/session.ts` | `SessionShare.create`; `session.create(input)`; `SessionShare.share`; `Session sharing has been discontinued`; `SessionShare.unshare`; `session.setShare({ sessionID, share: undefined })` | Creation delegates directly to Session; sharing fails as discontinued and unsharing only clears the local link |
| `packages/opencode/src/tool/task.ts` | `parentID: ctx.sessionID`; `runInBackground`; `background.start`; `injectBackgroundResult` | Native tasks create descendants and can outlive a foreground response when background mode is enabled |
| `packages/opencode/src/effect/runtime-flags.ts` | `OPENCODE_AUTO_SHARE`; `autoShare`; `OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS`; `experimentalBackgroundSubagents` | Native flags retain auto-sharing and background-subagent settings; Kit spawn continues to disable both, although SessionShare no longer uses auto-sharing |
| `packages/core/src/v1/config/config.ts` | `share: Schema.optional`; `Schema.Literals(["manual", "auto", "disabled"])` | Effective configuration has three native sharing policies |
| `packages/core/src/session/projector.ts` | `messageData`; `SessionV1.Event.MessageUpdated`; `MessageTable`; `SessionV1.Event.Deleted`; `db.delete(SessionTable)` | Message projection stores passive message data for later hydration, while projected deletion removes session inventory before aggregate-event cleanup runs |
| `packages/core/src/event.ts` | `latestSequence`; `row?.seq ?? -1`; `const seq = input?.seq ?? latest + 1`; `function remove(aggregateID: string)`; `delete(EventSequenceTable)`; `delete(EventTable)` | First durable sequence is zero, absent history is minus one internally, and aggregate cleanup removes event state |
| `packages/opencode/src/permission/index.ts` | `findLast`; `export function merge` | The last matching rule wins and merge preserves rule order |
| `packages/opencode/src/session/message-v2.ts` | `info`; `hydrate`; `orderBy(PartTable.message_id, PartTable.id)`; `MessageV2.get`; `eq(MessageTable.id, input.messageID)`; `eq(MessageTable.session_id, input.sessionID)`; `latest`; `isAfter(info, user)`; `info.time.created > other.time.created`; `info.id > other.id` | Database hydration restores persisted message data as a plain object, returned parts have stable `PartID` order, exact lookup scopes both native owners, and the prompt loop selects messages by creation time with identity only as a tie-breaker |
| `packages/opencode/src/session/processor.ts` | `step-finish`; `cleanup` | Each assistant step receives its own counters and terminal timestamp/error state |
| `packages/schema/src/event.ts` | `versionedType`; `` `${type}.${version}` `` | Durable event type names use a dot before the numeric version |
| `packages/schema/src/file-diff.ts` | `Info`; `file`; `patch`; `additions`; `deletions`; `status` | Session diff carries typed optional file identity and bounded change summary fields |
| `packages/opencode/src/tool/shell.ts` | `ShellTool`; `ctx.metadata`; `exit: code`; `output`; `truncated` | Session-owned shell ToolPart metadata can carry terminal exit evidence while raw command output remains private |
| `packages/opencode/src/session/llm/request.ts` | `input.user.system`; `role: "system"` | A supplied prompt system value enters the native model system instructions |
| `packages/opencode/src/server/routes/instance/httpapi/groups/file.ts` | `FilePaths.content`; `FileContent` | File-content route and response schema |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/file.ts` | `FileHttpApi.content`; `text.value.trim()` | Native text response is lossy and missing maps to empty text |
| `packages/opencode/src/server/routes/instance/httpapi/groups/provider.ts` | `ProviderApi`; `provider.list`; `WorkspaceRoutingQuery`; `Provider.ListResult` | Workspace-routed provider/model inventory route and typed response |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/provider.ts` | `ProviderHttpApi.list`; `provider.list()`; `Provider.defaultModelIDs`; `connected` | Inventory merges available and connected providers and returns connected ids plus a provider-relative default map |
| `packages/opencode/src/provider/provider.ts` | `Model`; `capabilities.reasoning`; `variants`; `ListResult`; `defaultModelIDs`; `model.reasoning_options !== undefined`; `ProviderTransform.reasoningVariants(model, parsedModel)`; `mergeDeep(variants, model.variants ?? {})`; `pickBy(merged, (v) => !v.disabled)`; `configProvider?.models?.[modelID]?.variants` | Configured reasoning options take precedence over catalog variants, then configured overrides merge and disabled variants are removed; the returned default map selects one sorted model within each provider rather than one stable agent-wide default |
| `packages/core/src/v1/config/provider.ts` | `ReasoningOption`; `Schema.NullOr(Schema.String)`; `reasoning_options: Schema.optional(`; `budget_tokens` | Optional native model reasoning configuration accepts effort, toggle, and token-budget forms; native effort values may be null although generated HTTP efforts are strings |
| `packages/opencode/src/provider/transform.ts` | `reasoningVariants`; `const options = model.reasoning_options`; `if (options.length === 0) return {}`; `effortVariants`; `budgetVariants` | Empty configured reasoning options yield no generated variants before explicit overrides; native transformation remains server-owned |
| `packages/opencode/src/server/routes/instance/httpapi/groups/instance.ts` | `InstancePaths.agent`; `app.agents`; `Schema.Array(Agent.Info)` | Workspace-routed native agent inventory route; the adapter does not use it for portable discovery |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/instance.ts` | `InstanceHttpApi.agent`; `agent.list()` | The route returns the selected runtime's typed native agent list, not a portable personality domain |
| `packages/opencode/src/agent/agent.ts` | `Info`; `name`; `mode`; `hidden`; `model`; `variant`; `prompt`; `permission`; `Object.entries(cfg.agent ?? {})` | Arbitrary config-owned native agent identity, mode, visibility, defaults, and backend-private behavior fields |
| `packages/opencode/src/tool/registry.ts` | `ToolRegistry.ids`; `ToolRegistry.tools`; `ToolRegistry.named` | Built-in and dynamically registered native tool discovery owned by the server registry |
| `packages/opencode/src/session/tools.ts` | `SessionTools.resolve`; `registry.tools`; `mcp.tools`; `McpCatalog.convertTool` | Session-visible tools merge native registry tools and connected MCP tools through separate owners |
| `packages/opencode/src/server/routes/instance/httpapi/groups/mcp.ts` | `McpPaths.status`; `mcp.status`; `StatusMap` | The selected public MCP route reports server connection status but not a complete MCP tool-definition inventory |
| `packages/schema/src/mcp-event.ts` | `ToolsChanged`; `type: "mcp.tools.changed"`; `server: Schema.String` | Tool-change notification identifies the MCP server but carries no complete replacement tool catalog |
| `packages/schema/src/pty.ts` | `IDSchema`; `Schema.isStartsWith("pty")`; `pty.created`; `pty.updated`; `pty.exited`; `pty.deleted`; `exitCode: NonNegativeInt` | Branded native PTY identity and selected process lifecycle/terminal event shapes |
| `packages/schema/src/server-event.ts` | `Connected`; `type: "server.connected"`; `schema: {}` | Selected connected event uses an additive empty-object payload contract |
| `packages/schema/src/event-manifest.ts` | `ServerDefinitions`; `Definitions`; `Latest` | Complete selected known-event inventory and latest-version classification boundary |
| `packages/opencode/src/server/routes/instance/httpapi/groups/permission.ts` | `PermissionApi`; `permission.list`; `permission.reply`; `PermissionV1.Request`; `ReplyPayload`; `PermissionV1.Reply` | Pending permission inventory, selected typed request, and reply body |
| `packages/schema/src/v1/permission.ts` | `Request`; `Reply`; `permission.asked`; `permission.replied` | Branded request identity, session/tool ownership, permission strings, patterns, metadata, always rules, and once/always/reject outcomes |
| `packages/opencode/src/server/routes/instance/httpapi/groups/question.ts` | `QuestionApi`; `question.list`; `question.reply`; `question.reject`; `Question.Request`; `Question.Answer` | Pending structured-input inventory and selected response routes |
| `packages/schema/src/v1/question.ts` | `Request`; `Info`; `Option`; `Answer`; `question.asked`; `question.replied`; `question.rejected` | Ordered question/option structure, optional multiple/custom flags, zero-permitting arrays, and array-of-string answers |
| `packages/opencode/src/server/routes/instance/httpapi/groups/config.ts` | `const root = "/config"`; `HttpApiEndpoint.get("get", root`; `WorkspaceRoutingQuery` | Workspace-routed effective-configuration route |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/config.ts` | `ConfigHttpApi.get`; `configSvc.get()` | The selected route returns effective configuration |
| `packages/opencode/src/server/routes/instance/httpapi/middleware/workspace-routing.ts` | `defaultDirectory`; `getWorkspaceRouteSessionID`; `session?.directory || defaultDirectory`; `sessionID` | A session route uses the stored session directory before query/header/process defaults, so per-prompt cwd and session cwd migration are not sourced |
| `packages/opencode/src/server/routes/instance/httpapi/groups/global.ts` | `GlobalHealth`; `version: Schema.String`; `GlobalPaths.health`; `GlobalPaths.event` | Health/version and global live-stream route declarations |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/global.ts` | `GlobalHttpApi.health`; `InstallationVersion`; `eventData`; `Stream.tick("10 seconds")`; `Stream.drop(1)`; `server.connected`; `server.heartbeat` | Reported version, raw SSE message framing, connection, and nominal heartbeat behavior |
| `packages/opencode/src/server/routes/instance/httpapi/groups/sync.ts` | `HistoryPayload`; `HistoryEvent`; `SyncPaths.history` | Per-aggregate high-water request and history-row schema |
| `packages/opencode/src/server/routes/instance/httpapi/handlers/sync.ts` | `SyncHttpApi.history`; `EventTable`; `exclude.length`; `not(or(...exclude.map` | History is global to the event store; omitted aggregates contribute full history |
| `packages/opencode/src/event-v2-bridge.ts` | `payload: { id: event.id`; `type: "sync"`; `syncEvent`; `aggregateID` | Event bridge normal and durable-sync payloads before global-bus normalization |
| `packages/opencode/src/bus/global.ts` | `override emit`; `event.payload.syncEvent?.id`; `Identifier.create("evt", "ascending")` | Every object payload receives an id; a sync wrapper reuses its nested event id |
| `packages/core/src/flag/flag.ts` | `OPENCODE_DB: process.env["OPENCODE_DB"]` | Spawn can select a dedicated OpenCode database |
| `packages/core/src/database/database.ts` | `DatabaseMigration.apply`; `Flag.OPENCODE_DB`; `isAbsolute(Flag.OPENCODE_DB)` | The selected path owns the migrated OpenCode database and an absolute path bypasses the shared data-home default |
| `packages/opencode/src/auth/index.ts` | `path.join(Global.Path.data, "auth.json")` | Authentication remains in the normal data home and must not be hidden by database isolation |
| `packages/core/src/event/sql.ts` | `EventTable`; `event_aggregate_seq_idx` | Sequence uniqueness is scoped to aggregate identity |
| `packages/opencode/src/cli/network.ts` | `hasArg("--port")`; `hasArg("--hostname")`; `resolveNetworkOptionsNoConfig` | Explicit command arguments take precedence over configured bind values |
| `packages/core/src/installation/version.ts` | `InstallationVersion`; `"local"` | Health reports the compiled version token or the explicit local-build sentinel |

## Fixed route matrix

The adapter uses the following complete fixed route matrix for the portable
target and exposes no route selector.

| Purpose | Method and path | Selected behavior | Automatic retry |
|---|---|---|---|
| Server compatibility and spawned readiness | `GET /global/health` | Require the version selected by `SPECIFICATIONS.md`; spawned mode also waits for health. | Bounded only for spawned readiness. |
| Foreground-lifecycle compatibility | `GET /experimental/capabilities` | Require `backgroundSubagents == false` before publishing or reopening an Agent. | Never automatically. |
| Verify private session creation | workspace-routed `GET /config` | Require effective `share == "disabled"` immediately before every `POST /session`. | Never automatically. |
| Discover models | workspace-routed `GET /provider` | Decode the complete provider result, retain models only from reported connected providers, and treat the provider-relative default map as private evidence rather than one portable agent-wide default. | Never automatically. |
| Discover native tool ids | workspace-routed `GET /experimental/tool/ids` | Decode the built-in and dynamically registered candidate id set; this route alone proves neither provider/model callability nor the separately merged MCP inventory complete. | Never automatically. |
| Describe native tools | workspace-routed `GET /experimental/tool` with selected provider/model query | Decode typed native tool descriptions for the exact selected provider/model context. | Never automatically. |
| Inspect MCP contribution | workspace-routed `GET /mcp` | Read typed MCP server status only to determine whether separately merged MCP tools could make the exposed native catalog incomplete; the route is not a tool-definition inventory. | Never automatically. |
| Live events | `GET /global/event` | Open the sole live SSE subscription. | Reconnect through recovery, not blind request replay. |
| Verify native conversation | `GET /session/:sessionID` | Verify the bound session during reopen. | Never automatically. |
| Reconcile native execution | `GET /session/status` | After session ownership is verified, read the active-status map; an absent bound id is sourced idle only for this live server instance. | Never automatically. |
| Create native conversation | `POST /session` | Send one unique private creation-correlation value in session metadata and create the session bound to one Kit conversation. After a lost response, exhaust bounded global inventory, require one exact correlation match, and verify it with the exact session read before binding or cleanup. | Never after an ambiguous result. |
| Foreground prompt | `POST /session/:sessionID/message` | Omit the optional `messageID`, supply one private text-part id, let the server allocate the native user identity, and await the response-returning route. Plain prompts omit format; structured prompts send `{ type: "json_schema", schema, retryCount: 0 }`. | Never. |
| Verify plain foreground parent | `GET /session/:sessionID/message/:messageID` | For a plain prompt, read the exact parent claimed by the returned Assistant and require its session, User root shape, and private request-part id before publication. The selected typed route cannot encode a database-hydrated structured User format and is not used as structured-parent proof. | Never automatically. |
| Abort active work | `POST /session/:sessionID/abort` | Request native session cancellation. | Never after an ambiguous result. |
| File capability evidence | `GET /file/content` | The sourced route is lossy and is not called for portable `read_file`. | Not applicable. |
| Reconcile permissions | `GET /permission` | Read pending permission requests. | Never automatically. |
| Resolve permission | `POST /permission/:requestID/reply` | Send one validated permission reply. | Never. |
| Reconcile questions | `GET /question` | Read pending question requests. | Never automatically. |
| Answer question | `POST /question/:requestID/reply` | Send one validated answer set. | Never. |
| Reject question | `POST /question/:requestID/reject` | Send one rejection/cancellation. | Never. |
| Recover durable events | `POST /sync/history` | Query rows after per-aggregate high-water marks. | The identical read-only body may be retried before rows are applied; replay is deduplicated. |
| Verify isolated server inventory | bounded-complete `GET /experimental/session` | Set `archived=true`, omit every other filter and cursor, and request `limit = private total-inventory bound + 1`. A response of at most the private bound with no continuation header is complete. A larger response or continuation header returns `Integration` without adopting or omitting records. | Never automatically. |
| Failed-setup or post-commit root-session cleanup | `DELETE /session/:sessionID` | Record the bounded native subtree before recursive removal. Retain cleanup responsibility until global inventory shows every recorded id absent and bounded sync-history recovery shows no surviving event aggregate for those ids. | Never after an ambiguous result. |

The fixed matrix is complete for this target. Public prompt uses the selected
response-returning message route so its awaited future reaches a terminal
result. Failure of a selected route is that operation's failure and does not
change the matrix.

The adapter's single HTTP client disables redirect following and reqwest
retries. A redirect is the selected route's response, not permission
to send a stateful request to another endpoint. Native transport recovery of an
unstarted pooled request does not imply a second admitted operation. Under
this policy, builder failures and initial connection failures carry private
preacceptance evidence; failures after sending or while reading the response
retain uncertain acceptance. Portable error categories alone never establish
that distinction.

Workspace-routed routes carry the pinned directory and optional native
workspace routing context. Those query fields are derived from private binding
state, never from caller-supplied protocol DTOs. A route returning not found or
unsupported is handled according to its owning operation while the matrix
remains fixed.

Native configuration loading rejects root `permissions` and `permissions` in
`agents`, `agent`, or `mode`, including empty values. This is a configuration
failure, not an invitation to translate or discard policy. Singular V1
`permission` remains supported, and the final matching session rule still wins.
The adapter preserves HTTP rejection status and redacts native config details;
it never retries or repairs rejected configuration.

Session privacy validation is read-only. The adapter does not mutate external
configuration through `PATCH /config`. External attachment therefore requires
exclusive ownership of effective configuration throughout validation and
native session creation.

## Model And Native Catalog Decoding

The provider inventory response is one selected typed object with `all`,
`default`, and `connected` fields. The adapter must validate the complete
selected shape before projection. Only provider ids named by `connected` are
selectable. The native connected set may include stored credentials as well as
initialized providers; membership does not prove credential validity. Within
those providers, the exact provider id plus model id is the private native
selector; a display name alone is not identity. Variant maps are
native option objects. Configured `reasoning_options` replace catalog variant
selection before explicit variant overrides merge and disabled entries are
removed. An empty options array produces no generated variants before those
overrides. When reasoning options are absent, matching API packages retain
existing variants with a computed fallback; other packages compute variants.
Variant maps are decoded privately but never advertised as portable reasoning efforts. Every
portable model therefore reports empty reasoning and personality inventories.

The generated `ProviderConfig` model configuration declares optional
`reasoning_options`: effort requires string `values`, toggle requires only its
type, and token budget permits optional numeric `min` and `max`. The native
configuration source accepts nullable effort strings, unlike the generated HTTP
declaration. This difference does not widen the provider-inventory `Model`
response or establish portable configuration or reasoning selectors.

The model response's `capabilities.interleaved` is a boolean or an object with
exactly one required string `field`. Every string, including `reasoning_text`,
custom names and the empty string, is source-valid. A bare string is not a valid
response. Native provider configuration additionally accepts a bare string and
normalizes it to that object; this does not add a portable configuration input.

The response's `default` object is provider-indexed and is constructed from a
sorted model within each provider. It neither names one default across the
Agent nor proves that later server choice will ignore configuration and recent
state. The adapter therefore does not project it as portable `is_default`.
Provider keys, environment variable names, headers, options, costs, and other
private model metadata are decoded as required by the selected shape but do
not enter public model identity or diagnostics.

The selected server exposes a separate typed native agent inventory, but the
adapter does not call that route for portable discovery. Its arbitrary
config-owned names, prompts, permissions, modes, temperature, step limits,
colors, model defaults, and variants have no sourced portable personality
mapping. The foreground wire accepts optional `agent` and `variant`, but the
adapter never derives either from portable personality or reasoning values.
Every non-absent effective value fails with backend-neutral `Unsupported`
before native mutation; absent values omit both fields and leave native
config/default behavior private.

Native tool discovery has two selected routes. The id route describes the
built-in and dynamically registered registry; the provider/model-sensitive
route returns descriptions and parameter schemas for that registry. Session
tool resolution separately merges tools from connected MCP clients. `GET
/mcp` reports connection status only, while `mcp.tools.changed` reports only a
server name. Neither supplies a complete MCP definition set. Consequently a
connected MCP contribution makes a claimed complete portable catalog
unavailable in this selected version. The catalog owner follows
[`CATALOG.md`](CATALOG.md); protocol code must not fabricate definitions,
scrape config, or return a known-partial inventory as complete.

These reads use the same reusable HTTP client and workspace-routing derivation
as the rest of the matrix. None is automatically retried, and no response
authorizes mutation of provider, tool, MCP, auth, or external configuration.

## Native Prompt Ordering And Retry Behavior

The native loop selects latest User, Assistant, and finished Assistant messages
by `time.created`, breaking equal-time ties by id. Imported ids need not be
monotonic. Loop completion requires the last Assistant's exact
`parentID == lastUser.id`, a finish other than `tool-calls` or `unknown`, and no
remaining non-provider tool calls except cleanup-marked interrupted orphans.
This does not change fresh Kit request-part correlation or make history a
replacement for the foreground HTTP response.

Native tasks default to one subagent level and propagate child Assistant and
tool errors. Provider header and SSE-idle timeouts each default to 300000 ms;
native session retries are capped at five with jittered exponential backoff.
These are native execution facts, not Agent retry or deadline settings. The
adapter preserves its configured deadlines, background-subagent rejection,
exact terminal output, and bounded cancellation and cleanup.

## Process and HTTP ownership

One reusable HTTP client serves the built OpenCode runtime. It owns connection
pooling, base-URL handling, request deadlines, and HTTP execution. The Agent's
event owner exclusively owns the current SSE response, its bounded overlap,
its monotonic one-shot stop signal, and its tracked task join. The adapter does
not create a client per route call, conversation, or reconnect. A partially
started spawned mode cleans up its child and HTTP/SSE resources before
returning the original startup failure.

Finite synchronous HTTP client construction runs on Tokio's blocking pool,
with the original join retained by the existing builder owner across caller
loss. It preserves default proxy discovery and TLS behavior. The worker checks
the original construction deadline before invoking the constructor, so queued
work that starts after expiry does not construct a client. Once started, the
constructor is joined to completion; settlement may exceed that deadline.
The runtime checks the same deadline after joining and rejects a late result
before process startup, HTTP execution, or Agent publication. This bridge
keeps synchronous setup off async workers without granting a new time budget.

Agent construction passes one already-sampled absolute lifecycle deadline
through external health and capability verification and through spawned
readiness and capability verification. The spawned startup deadline may
shorten that budget but cannot extend it. Per-request transport timeouts remain
attempt bounds only. Native inventory, cleanup reconciliation, initial event
synchronization, and reopen reconciliation remain in that construction
episode. Expiry is handled inside the runtime owner so a partially started
child and its tracked output drains are terminated and joined rather than
detached by cancellation of the outer construction future. That failure starts
one lifecycle-bounded cleanup episode whose absolute deadline is passed
unchanged through event, process, child-reap, and output-drain teardown.
Read-only durable preparation may be cancelled at that deadline. Once a setup
transaction is admitted, its exact owner retains commit-or-rollback authority;
a late confirmed commit is recovery state, not authority
to publish the Agent or begin another construction operation.

Known structured non-success envelopes map through the route and owning
operation to caller-recovery categories such as `InvalidInput`, `NotFound`, or
`Busy`. Malformed known success/error envelopes and invalid SSE framing are
`Protocol`; transport execution and unexpected body termination are
`Integration`. Error response bodies may be retained for private diagnostics
but do not become public backend payloads.

## SSE parsing

The stream parser follows the WHATWG event-stream algorithm rather than a
line-oriented JSON shortcut:

- CRLF, lone CR, and lone LF terminate lines;
- a blank line dispatches the accumulated event;
- a line beginning with `:` is an ignored comment; it dispatches no event and
  does not reset the Kit silence deadline;
- a field splits at its first colon and removes at most one leading space from
  the value;
- repeated `data` fields append their values separated by a line feed;
- an `id` containing NUL is ignored;
- `retry` is accepted only when its value contains ASCII digits; and
- EOF discards an incomplete pending event.

Only a complete `data` value is decoded as JSON. SSE field names, retry hints,
raw bytes, and raw event text stay private. The server's selected global
handler emits message events without an SSE `id`; the identifier used for
deduplication is inside the JSON payload, so `Last-Event-ID` is not a recovery
cursor for this adapter.

## Sole live stream

The selected `message.part.delta` schema carries `sessionID`, `messageID`,
`partID`, `field` and `delta`. The decoder retains each field privately instead
of reducing this event to session activity. The selected processor publishes a
typed text-part update before incremental `field: "text"` events; reasoning
has its own part type. `updatePartDelta` publishes transient events without
durable history. The prompt observer validates the full generation and text
part ownership described in [`CONVERSATION.md`](CONVERSATION.md); it never
exposes native ids or raw frames. A later text-end plugin can change complete
text, so these events cannot replace the foreground HTTP terminal result.

`GET /global/event` is the sole live event subscription. Reconnection opens
that same selected stream, preserving one envelope and routing contract.

The event owner has explicit Starting, Live, and Recovering phases. Recovery
also records why it is running: startup, transport reconnect, recoverable
application admission, an aggregate gap, or a structured-parent
synchronization request. Startup and reconnect open a stream and then
synchronize it. Application, gap, and structured-parent recovery retain and
synchronize the already-open stream. They do not open a second SSE response
merely because history is needed. If that current response fails during
synchronization, it is dropped before the sole replacement response is opened;
the replacement's subscribe-buffer-history synchronization must complete
before recovery succeeds.

Connection loss initiates an explicit reconnect-and-history recovery flow.
The stream recovery owner subscribes first, uses bounded memory
to retain live events for the entire synchronization episode: high-water load,
history retrieval, ownership refresh, every awaited durable history apply, and
pending-interaction reconciliation. It drains the overlap after history apply
and again after reconciliation, then returns to live delivery. This overlap is
safe under the contract below. It must not replace live delivery with periodic
`POST /sync/history` polling.

Synthetic `server.connected` and `server.heartbeat` payloads establish stream
liveness. They are connection facts, not portable conversation notifications,
and they may arrive without directory, project, or workspace context.

The selected handler establishes a nominal ten-second heartbeat interval. The
Kit's minimum thirty-second silence deadline is a Kit liveness policy, not an
upstream delivery guarantee. Only a complete successfully parsed SSE event
resets that deadline. Partial bytes do not reset it. Expiry closes the stale
response and starts reconnect-and-history recovery bounded by
`AgentConfig.lifecycle_timeout`; it does not cancel or complete a prompt, does
not emit a portable notification, and does not authorize request replay.
Each startup, reconnect, gap, or structured-parent recovery episode has one
absolute deadline. Create and reopen both inherit the Agent construction
lifecycle deadline; per-request transport timeouts bound individual attempts
without replacing or extending that episode deadline. HTTP attempts, history
reads, overlap capture, and backoff consume the same episode deadline; nested
helpers do not start fresh timeout budgets. The silence deadline is separate
transport-liveness policy. Agent shutdown is the event owner's one-shot
lifecycle cancellation and is not encoded as another history deadline.
Durable event storage performs one transaction attempt. The event owner alone
retries a typed transaction conflict against the live event's application
deadline or the current recovery episode deadline. It normalizes identity and
semantic effect once, and neither classification nor postcommit effects are
replayed by a storage retry. Read-only preparation, HTTP discovery, and backoff
may be cancelled at that deadline. An admitted StateStore mutation is instead
awaited through terminal commit observation and required postcommit publication
before an elapsed deadline, simultaneously observed stream failure, or graceful
shutdown stop is reported. The shutdown deadline remains the final abort and
join authority. Startup readiness expiry returns the still-tracked event owner
to Agent failure cleanup; it never detaches the owner or replaces that final
shutdown authority with an unbounded readiness wait. A recoverable application failure such
as native inventory appearing before its durable binding synchronizes history
on the same response under the still-current application deadline. Exhausting
that deadline or encountering a nonrecoverable application failure terminates
the event owner. Only decoder, transport, EOF, or silence failure drops the
response and enters reconnect with a new recovery episode.
Catalog invalidation is a postcommit effect of an applied
`mcp.tools.changed` event and is published before an already-elapsed episode
deadline is reported. Remembered duplicates, ordinary duplicates, and
gaps publish no process-local generation change. History following a gap may
publish the invalidation only when its durable application is applied.
If a structured-parent command exhausts its caller deadline while the current
SSE response remains healthy, only that command completes with `Timeout`; the
owner retains the response and resumes live delivery. Gap, startup, reconnect,
transport failure, and bounded-overlap invariant failures remain owner-scoped.

## Three distinct event envelopes

The pinned source emits three shapes that require separate typed decoders.
They share semantic event identity only after successful normalization.

### Normal global event

```json
{
  "directory": "...",
  "project": "...",
  "workspace": "...",
  "payload": {
    "id": "evt_...",
    "type": "session.updated",
    "properties": {}
  }
}
```

`project` and `workspace` are optional. The actual handler also emits some
global or synthetic payloads without routing context. `properties` is the
normal-event data field; it must not be decoded as sync-history `data`.

### Live durable sync envelope

For a durable event, the event bridge emits a normal global event and a second
global wrapper whose payload is:

```json
{
  "id": "evt_...",
  "type": "sync",
  "syncEvent": {
    "id": "evt_...",
    "aggregateID": "aggregate_...",
    "seq": 7,
    "type": "session.updated.1",
    "data": {}
  }
}
```

The outer global wrapper carries the same optional directory, project, and
workspace context as a normal event. The outer payload id equals
`syncEvent.id`; a mismatch is `Protocol`. This is a durable
sequencing copy of the semantic event, not a second caller-visible occurrence.

### History row

`POST /sync/history` returns an array of rows shaped as:

```json
{
  "id": "evt_...",
  "aggregate_id": "aggregate_...",
  "seq": 7,
  "type": "session.updated.1",
  "data": {}
}
```

The response field is `aggregate_id`; the live nested field is `aggregateID`.
The adapter normalizes that spelling only after decoding the correct envelope.
It never applies the normal-event schema to a history row or trusts generated
OpenAPI naming over the pinned handler's actual response.

The live payload union is exclusive. A normal event requires `properties` and
must not carry `syncEvent`. A sync payload requires `syncEvent` and must not
carry normal `properties`. Missing the required member, carrying both members,
or labeling one shape as the other is `Protocol`; the decoder never chooses a
branch by whichever optional field happens to deserialize first.

## Typed routing and known-event handling

The global wrapper's directory, project, and workspace plus typed payload
identity form backend-private routing context. Session-bearing event data may
refine that context to one conversation. Missing context on a genuinely global
event is valid; missing context on an event that requires a conversation is an
explicit routing failure, not permission to choose an arbitrary active
conversation.

A Kit-bound root session owns its source-proven descendant tree. Native
descendants remain backend-private and acquire no public Conversation, but
their routed events, interactions, aggregates, and high-water values belong to
that root binding after parent-chain validation. Only a session or aggregate
outside every bound-root closure is foreign.

Known pinned event types decode into typed OpenCode events and receive one
exhaustive semantic classification before portable mapping or runtime side
effects. The outcome is a portable notification fact, portable interaction
fact, internal lifecycle fact, honest unsupported behavior, or honest
not-yet-implemented adapter gap. Every known event has exactly one typed
outcome.

For an active structured foreground prompt, typed `message.updated` and
`message.part.updated` observations are private lifecycle evidence. The
adapter retains the exact session, User role and root-parent shape, message id,
part message owner, part id, and non-synthetic text-part shape. The returned
POST Assistant parent id selects the one admissible User observation; the
private request-part id selects the one admissible part observation. Live and
history forms normalize through the same typed event path, and neither can
provide terminal output or replace the POST response.

For a selected event whose sourced payload schema is an empty object, including
`server.connected`, the typed decoder requires a JSON object but accepts
additive fields. It must not apply `deny_unknown_fields` or freeze the payload
as exactly `{}`. This additive rule applies only to selected empty-object event
schemas, not to arbitrary known payloads or a generic raw-value escape hatch.

Selected session-created or session-updated data retains native share state.
Any share binding on a Kit-owned root or validated descendant contradicts the
disabled-sharing attachment contract and is `InvalidState`; the event does not
publish a handle, portable notification, or high-water advance before that
ownership/privacy check succeeds.

Unknown future event types may retain the complete raw global or history
envelope for private diagnostics. They cannot be converted to invented public
status or success. Raw SSE frames never enter conversation behavior.

Permission and question events enter the same portable interaction lifecycle
as other backends. The pending-list routes reconcile state after reconnect;
they do not create a second public interaction path. A reply or rejection is
validated against the pending typed request and sent at most once.

## Permission And Question Wire Boundaries

The selected permission routes use the compatibility permission schema that
the actual route module imports. A request includes branded `per` identity,
session ownership, permission string, string-pattern array, metadata object,
string-array `always` state, and optional tool origin containing message and
call identities. A reply contains exactly `once`, `always`, or `reject` plus an
optional message. Plain strings and arrays have no selected nonempty constraint;
the adapter enforces their required presence and JSON type without trimming or
inventing content restrictions.

Portable approval Allow maps only to `once`, and Deny maps to `reject`.
Observed native `always` remains decodable private state but is never produced
from portable Allow. The branded identity, session, metadata, always state, and
tool origin remain private.

The selected question request includes branded `que` identity, session
ownership, an ordered `questions` array, and optional tool origin. Each
question contains string `question` and `header`, an ordered options array of
string `label` and `description`, and optional `multiple` and `custom` flags.
The reply contains `answers`, an array with one array of strings per question.

The selected schemas permit an empty questions array, empty source strings,
empty per-question answer groups, and empty complete answers for zero
questions. Portable schema generation must preserve exact outer cardinality:
zero questions yields both minimum and maximum zero and accepts
`{ "answers": [] }`. Wrong types or cardinality are invalid caller input;
source-valid empty content is not a protocol error.

Pending-list snapshots already have these typed request shapes. Reconciliation
inserts them directly into the existing durable interaction owner. It does not
invent a normal event, sync envelope, history row, event id, aggregate
sequence, or high-water advance. Known live events and pending snapshots share
one request identity and cannot publish duplicate portable interactions.

## Sync-history request and response

The history request body is a JSON object from aggregate id to the last
contiguous sequence successfully applied for that aggregate:

```json
{
  "aggregate_a": 12,
  "aggregate_b": 4
}
```

For an aggregate present in the body, the server returns rows whose `seq` is
greater than the supplied value. An aggregate absent from the body contributes
its full available history. An empty object therefore requests full history
for every aggregate in the server's global event store.

The adapter uses this route only under the isolated-database contract in
[`BACKEND.md`](BACKEND.md). Spawn selects a stable private per-agent OpenCode
database and retains that selection as backend attachment state; it does not
replace the normal data home. External attachment asserts exclusive ownership
of its server and OpenCode database. The adapter first subscribes to the global
stream and buffers without applying, then performs the bounded-complete global
session query and issues history. Ownership verification precedes application.
It sends entries only for aggregates with a committed contiguous high-water
and omits every known aggregate without a committed contiguous high-water,
including cleanup-verification aggregates. Native sequence zero is the first
event and the request schema cannot encode the internal minus-one sentinel, so
an empty object is valid whenever none has a committed mark after isolated
inventory ownership validation. History and overlap buffers have
fixed private byte and row bounds.
Buffer saturation returns `Integration` without cursor advancement, event
loss, or an automatic reconnect loop over the same oversized history.

The response is an array of the history-row shape above. There is no returned cursor.
Sequence values are meaningful only within one `aggregate_id`. The
server's returned array order does not establish a global event order, and the
adapter must not create a global cursor, compare sequence values across
aggregates, or derive causality from array adjacency.

History retrieval is explicit recovery after startup/reconnect, a detected
per-aggregate gap, or a documented reconciliation need. It is not the steady
notification transport, a timer-driven poll, or a fallback for failure to
decode `/global/event`.

Gap recovery and active structured-parent reconciliation issue history while
capturing the existing live response. A healthy stream is not replaced for
either reason. Startup and transport reconnection subscribe before history so
their new response closes the history/live race. All retry and backoff inside
one of these episodes remains under the episode's original absolute deadline.

An aggregate or routed session outside every Kit-bound root and its validated
native descendant closure proves that the isolated-database contract was
violated. Recovery returns `Conflict`, advances none of that aggregate, and
does not adopt it. Already committed owned aggregates remain committed, but
the global ownership violation seals the live runtime and stops new admission.
Per-aggregate processing independence applies only while global ownership
remains valid.

Cleanup verification omits every recorded cleanup aggregate from the
high-water request so any surviving history for it is returned in full. The
obligation clears only when that bounded response contains no row for those
ids and the global session inventory also contains none. Session absence alone
is insufficient because the native delete path projects inventory removal
before it attempts aggregate-event cleanup.

## Normal-plus-sync deduplication

Every durable semantic event can be observed as a normal global event, its
live sync envelope, and later a history row. The adapter applies its semantic
effect at most once while still using sync data to advance recovery state.

Known durable events share one semantic identity across those forms. Semantic
application, deduplication, and resulting contiguous high-water advancement
have one crash-atomic durable outcome. Non-durable normal events create no
pairing state. Overlap identity is bounded and retained only long enough to
suppress a captured duplicate; no historical id set grows without bound.

An event with `seq` at or below the aggregate's committed high-water is a
duplicate. An event above the next expected sequence exposes a gap: the
aggregate must not advance past that gap. Retain the event, request history
from the last contiguous mark, and apply recovered rows in per-aggregate
sequence order. Duplicate event ids with conflicting aggregate, sequence,
type, or data are protocol corruption, not benign duplicates, except for the
narrow cross-origin Assistant projection rule below.

The selected publisher schema-encodes an immutable history row before notifying
listeners with the original message object. `updateMessage` passes that same
Assistant object; the event bridge places it in both normal and live-sync
envelopes, and the global handler serializes queued objects later. Selected
processor/prompt code can meanwhile change `info.time.completed`, `info.cost`,
`info.tokens`, `info.finish`, `info.error`, and `info.structured` in place. This
can produce different valid live and history projections of one event id.

Retained overlap evidence distinguishes normal, live-sync, and history origin
from the actual normal/live-sync envelope or history response. Type suffixes,
coordinates, and high-water values do not establish origin. A repeated observation from the same retained
origin requires complete content equality. Between distinct origins, only an
already typed Assistant `message.updated` may differ in those six fields; the
complete remaining data, including unknown additive fields, must still match.
Event type and durable coordinates retain their exact existing checks. User
messages and other event families retain complete content equality.

An actual history row becomes the retained content authority for that identity,
including when its sequence is already committed. Later live observations
cannot replace that history evidence. Promotion neither reapplies the semantic
effect nor advances an already committed sequence. The existing bounded overlap
window remains the evidence scope; this is not an archive of every prior live
projection. Typed validation and postcommit prompt-output observation remain
unchanged, and events never substitute for the foreground HTTP result.

Failure for one aggregate leaves that aggregate's high-water unchanged but
does not roll back or block independently successful commits for other
aggregates. A malformed known frame advances no high-water value; the stream
terminates and reconnects through the same bounded recovery flow.

## Retry and recovery safety

Automatic retry is limited to spawned-server readiness probes and an identical
read-only history request. Event-id and high-water deduplication make repeated
history rows safe.

Session creation, foreground prompt, abort, permission/question resolution,
and session deletion are never blindly retried. Timeout or connection loss
after sending one of those requests is an ambiguous outcome resolved by typed
event, pending-request, durable-state, and native inventory reconciliation.
Reconciliation cannot reconstruct a lost foreground response and therefore
never fabricates prompt success. Recovery repeats only operations admitted by
the fixed matrix and its retry column.

After a successful structured foreground POST, missing live parent evidence
may trigger one bounded read-only `/sync/history` recovery through the same
per-aggregate normalization and deduplication path. It requires the exact
session, User root shape, returned parent id, and private request-part id.
Absence or contradiction is `InvalidState`; recovery never retries the prompt,
uses an event as output, or weakens local structured-schema validation. Plain
prompts retain exact-message parent verification.

Before session creation, the backend durably records one unique private
metadata correlation and an ambiguous-create obligation. Lost-response
recovery exhausts the bounded-complete global inventory, accepts only one exact
correlation match, and verifies that candidate with the exact session read
before binding or cleanup. Zero matches remain recovery-owned; multiple
matches are contradictory state. Creation is never replayed, and title, cwd,
time, order, or active status never substitutes for exact correlation.
