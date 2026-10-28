# Codex Protocol Contract

This file owns selected Codex wire facts; parent specs own root behavior,
[`../API.md`](../API.md) owns public surface, and [`BACKEND.md`](BACKEND.md) owns differences.

Typed wire decode and routing end at the protocol boundary. Runtime effects
belong to the detailed [`CONVERSATION.md`](CONVERSATION.md),
[`NOTIFICATIONS.md`](NOTIFICATIONS.md),
[`SERVER_REQUESTS.md`](SERVER_REQUESTS.md),
[`STATE_STORE.md`](STATE_STORE.md), and [`CATALOG.md`](CATALOG.md) domains.
Those documents may assign behavior around sourced facts but do not select a
different method inventory or transport contract.

## Source authority

The selected snapshot's App Server declarations, request processors, and
transport implementations establish the wire facts below.

The selected protocol facts are:

- App Server uses JSON-RPC.
- Kit-spawned stdio connections carry one JSON value per line.
- TCP and Unix-socket connections carry JSON-RPC values in WebSocket text
  frames.
- The vendored Unix-socket client uses `ws://localhost/rpc` for its WebSocket
  handshake.
- The selected WebSocket clients ignore binary frames.
- JSON-RPC request identifiers are either strings or signed 64-bit integers.
- the Kit initializes one long-lived App Server connection and shares it
  across the Codex runtime; it does not reconnect for each operation or
  conversation.
- initialize-time notification exclusions suppress exact method names for
  only that connection.
- after the last subscriber detaches, an idle native thread is eventually
  unloaded and `thread/closed` is broadcast connection-wide.
- `thread/delete` and `thread/deleted` have the sourced cascade and ordering
  behavior specified below.

The source coordinates below are relative to the selected archive's single
top-level directory. Symbol anchors, rather than extracted-tree paths, are the
stable review coordinates:

| Archive member | Symbol anchors | Contract fact |
|---|---|---|
| `codex-rs/app-server-protocol/src/rpc.rs` | `RequestId`; `JSONRPCMessage` | String/integer request identifiers and directional JSON-RPC message shapes |
| `codex-rs/app-server-protocol/src/protocol/v1.rs` | `InitializeCapabilities`; `opt_out_notification_methods`; `InitializeResponse`; `user_agent` | Initialization declares exact per-connection notification exclusions and reports server identity |
| `codex-rs/app-server-protocol/src/protocol/common.rs` | `ThreadSetName => "thread/name/set"`; `ThreadNameUpdated => "thread/name/updated"`; `ThreadDelete => "thread/delete"`; `ThreadDeleted => "thread/deleted"`; `ThreadUnsubscribe => "thread/unsubscribe"`; `ThreadBackgroundTerminalsClean => "thread/backgroundTerminals/clean"`; `ThreadBackgroundTerminalsList => "thread/backgroundTerminals/list"`; `ExperimentalFeatureList => "experimentalFeature/list"`; `ThreadReadStateChanged => "thread/readState/changed"` | Typed materialization, lifecycle, cleanup, and feature-inspection methods; known read-state notifications remain private |
| `codex-rs/app-server-protocol/src/protocol/v2/permissions.rs` | `SandboxPolicy::DangerFullAccess`; `SandboxPolicy::ReadOnly { network_access }`; `WorkspaceWrite {`; `writable_roots: Vec<AbsolutePathBuf>`; `network_access: bool`; `exclude_tmpdir_env_var: bool`; `exclude_slash_tmp: bool` | Full access has no network field; restricted modes represent their network flag, while WorkspaceWrite can exclude implicit temporary writable roots but neither restricted mode encodes a complete readable-root allowlist |
| `codex-rs/app-server-protocol/src/protocol/v2/thread.rs` | `ThreadStartParams`; `pub cwd: Option<String>`; `pub runtime_workspace_roots: Option<Vec<AbsolutePathBuf>>`; `pub sandbox: Option<SandboxMode>`; `pub config: Option<HashMap<String, JsonValue>>`; `pub developer_instructions: Option<String>`; `pub personality: Option<Personality>`; `pub ephemeral: Option<bool>`; `pub thread_source: Option<ThreadSource>`; `ThreadStartResponse`; `pub sandbox: SandboxPolicy`; `ThreadResumeParams`; `ThreadReadParams`; `ThreadListParams`; `pub excluded_thread_ids: Option<Vec<String>>`; `pub next_cursor: Option<String>`; `ThreadReadStateChangedNotification`; `ThreadReadState`; `ThreadUnreadPosition`; `pub first_unread: Option<ThreadUnreadPosition>`; `pub revision: String`; `pub read_states: Option<HashMap<String, ThreadReadState>>`; `pub read_state: Option<ThreadReadState>` | Thread creation exposes requested roots, selected sandbox mode, per-thread config, instructions, personality, durability, and client-supplied private source correlation; materialized effective sandbox, resume/read shape, and paginated thread inventory belong to distinct response/request owners; read-state metadata is additive on read/list responses and typed on notifications |
| `codex-rs/app-server-protocol/src/protocol/v2/thread_data.rs` | `Feature(String)`; `Thread`; `pub thread_source: Option<ThreadSource>`; `Turn`; `TurnError`; `pub session_id: String`; `pub parent_thread_id: Option<String>` | Arbitrary string source correlation is returned with thread state alongside terminal shape and sourced native ancestry; the same typed terminal failure shape governs live and history paths |
| `codex-rs/app-server-protocol/src/protocol/v2/turn.rs` | `TurnStartParams`; `pub struct TurnStartParams`; `pub cwd: Option<PathBuf>`; `pub model: Option<String>`; `pub effort: Option<ReasoningEffort>`; `pub personality: Option<Personality>`; `pub output_schema: Option<JsonValue>` | Foreground model and effort overrides are sticky for this and subsequent turns; the personality field is deprecated; cwd and structured-output schema are per-turn overrides; no direct developer-instructions or tool-selection field exists |
| `codex-rs/app-server-protocol/src/protocol/v2/item.rs` | `ThreadItem::AgentMessage`; `ThreadItem::CommandExecution`; `From<CoreTurnItem>`; `AgentMessageDeltaNotification`; `thread_id`; `turn_id`; `item_id`; `delta`; `CommandExecutionApprovalKind`; `WriteStdin`; `ToolRequestUserInputParams`; `is_blocking`; `auto_resolution_ms`; `FunctionCallOutput`; `delivery`; `questions` | Completed agent-message text and thread-owned command execution facts; selected `item/agentMessage/delta` carries actual text fragments with exact private thread/turn/item identity, all typed strings; Stdin approvals, explicit blocking policy, and private function output/asynchronous messages |
| `codex-rs/app-server-protocol/schema/typescript/v2/CommandExecutionApprovalDecision.ts` | `acceptWithExecpolicyAmendment`; `execpolicy_amendment`; `applyNetworkPolicyAmendment`; `network_policy_amendment` | Approval decision variant names are camelCase, while the two nested amendment fields are snake_case; malformed or differently spelled nested fields fail protocol validation |
| `codex-rs/app-server-protocol/src/protocol/v2/process.rs` | `ProcessSpawnParams`; `ProcessOutputDeltaNotification`; `ProcessExitedNotification` | Separate caller-process-handle events are connection-scoped and do not identify a Conversation |
| `codex-rs/app-server-protocol/src/protocol/v2/fs.rs` | `FsReadFileParams`; `FsReadFileResponse` | Absolute native input and base64 exact-byte output |
| `codex-rs/app-server-protocol/src/protocol/v2/current_time.rs` | `CurrentTimeReadParams`; `CurrentTimeReadResponse`; `current_time_at` | Thread-scoped request and whole-Unix-seconds response shape |
| `codex-rs/app-server-protocol/src/protocol/v2/model.rs` | `pub struct Model`; `pub id: String`; `pub model: String`; `pub supported_reasoning_efforts: Vec<ReasoningEffortOption>`; `pub default_reasoning_effort: ReasoningEffort`; `pub supports_personality: bool`; `pub is_default: bool`; `ModelListResponse`; `pub next_cursor: Option<String>` | Preset versus selection identity, sourced default marker/effort, and reasoning support belong to model rows; the deprecated personality field is validated but never enables selection |
| `codex-rs/app-server/src/request_processors/turn_processor.rs` | `turn_start`; `TurnInput::UserInput`; `final_output_json_schema` | The selected request processor forwards the per-turn output schema to the native operation |
| `codex-rs/core/src/client_common_tests.rs` | `serializes_text_schema_with_strict_format`; `codex_output_schema`; `/*output_schema_strict*/ true` | The selected server uses a fixed native schema name with strict semantics |
| `codex-rs/app-server/tests/suite/v2/output_schema.rs` | `turn_start_accepts_output_schema_v2`; `turn_start_output_schema_is_per_turn_v2`; `output_schema: Some`; `output_schema: None` | The selected server applies the selected schema independently per turn |
| `codex-rs/models-manager/src/manager.rs` | `default_model_from_available`; `requested_model_is_available` | Native selection and default resolution use the model slug rather than preset id |
| `codex-rs/app-server/src/request_processors/fs_processor.rs` | `pub(crate) async fn read_file`; `/*sandbox*/ None`; `fn map_fs_error`; `io::ErrorKind::InvalidInput`; `internal_error(err.to_string())` | Native read supplies no atomic restricted-context confinement; apart from invalid input, filesystem failures share one untyped internal-error category while preserving the server message |
| `codex-rs/app-server/src/error_code.rs` | `INVALID_REQUEST_ERROR_CODE`; `METHOD_NOT_FOUND_ERROR_CODE`; `INVALID_PARAMS_ERROR_CODE`; `INTERNAL_ERROR_CODE`; `OVERLOADED_ERROR_CODE`; `server_draining_error`; `serverShuttingDown` | Integer JSON-RPC codes support safe request/internal/overload detail without interpreting server prose; shutdown adds private reason data to the existing invalid-request code |
| `codex-rs/app-server-protocol/src/protocol/v2/shared.rs` | `CodexErrorInfo` | Typed terminal failure details retain selected optional HTTP status |
| `codex-rs/app-server-protocol/src/protocol/v2/notification.rs` | `ErrorNotification`; `will_retry` | Retry notifications do not interrupt a turn |
| `codex-rs/app-server-protocol/src/protocol/v2/mcp.rs` | `ListMcpServerStatusParams`; `McpServerStatus`; `ListMcpServerStatusResponse`; `pub next_cursor: Option<String>` | Paged effective MCP inventory is independently inspectable and must be absent when portable policy cannot confine it |
| `codex-rs/protocol/src/config_types.rs` | `pub enum WebSearchMode`; `Disabled`; `Cached`; `Indexed`; `Live` | Web search is a separate selected configuration axis rather than a consequence of shell sandbox policy |
| `codex-rs/app-server/src/current_time.rs` | `send_request_to_connections`; `require_single_current_time_connection`; `External clocks are not interchangeable` | App Server requests time from the one connected client subscribed to the thread |
| `codex-rs/app-server/src/request_processors/initialize_processor.rs` | `get_codex_user_agent`; `InitializeResponse`; `experimental_api_enabled`; `opt_out_notification_methods`; `odd cross-client behavior` | Initialization returns server identity and installs connection-owned notification exclusions, while per-connection capabilities are unsafe authority for shared interactive threads |
| `codex-rs/app-server/src/transport.rs` | `codex_app_server_transport::start_stdio_connection`; `codex_app_server_transport::start_websocket_acceptor`; `should_skip_notification_for_connection`; `opted_out_notification_methods.contains` | Exact method-name exclusions suppress typed notifications for one connection without suppressing requests or responses |
| `codex-rs/app-server/src/bespoke_event_handling.rs` | `receiver.await`; `resolve_server_request_on_thread_listener`; `emit_turn_completed_with_status`; `handle_turn_complete`; `handle_turn_interrupted`; `handle_token_count_event` | A completed response waiter asks the one thread listener to publish resolution only after its callback completes; terminal notification omits items, while status/error and usage arrive through separate typed facts |
| `codex-rs/app-server/src/outgoing_message.rs` | `notify_client_response`; `take_request_callback`; `cancel_requests_for_thread`; `request_id_to_callback.remove`; `send_server_notification_to_connections`; `send_request_to_connections`; `OutgoingEnvelope::Broadcast` | Client response and thread-wide lifecycle cancellation both remove the same pending callback entries; empty notification targets and absent request targets broadcast connection-wide, while explicit request targets remain targeted |
| `codex-rs/app-server/src/request_processors/thread_lifecycle.rs` | `ThreadListenerCommand::ResolveServerRequest`; `resolve_pending_server_request`; `ServerRequestResolvedNotification`; `thread_unload_delay`; `ensure_listener_task_running`; `apply_bespoke_event_handling`; `unload_thread_without_subscribers`; `ThreadClosed`; `thread_read_state::publish`; `thread_read_state::notify` | The later identity-only notification reports request and thread ids without disclosing which completion cause won; one listener applies the ordered thread event stream serially, and the last-subscriber idle timeout unloads a thread and emits `thread/closed`; eligible read-state publication precedes terminal notification |
| `codex-rs/core/src/stream_events_utils.rs` | `finalize_non_tool_response_item`; `handle_output_item_done` | Nonempty final-answer or unphased agent text is finalized as an item before task completion |
| `codex-rs/core/src/tasks/mod.rs` | `on_task_finished`; `EventMsg::TurnComplete` | Task completion emits the terminal turn after its final agent item |
| `codex-rs/app-server/src/lib.rs` | `try_attach_thread_listener`; `thread_created_rx` | Initialized connections attach listeners to newly created native threads |
| `codex-rs/app-server/src/request_processors/thread_processor.rs` | `thread_set_name_response_inner`; `update_thread_metadata`; `send_response`; `ThreadNameUpdated`; `config_snapshot.sandbox_policy()`; `collect_resume_override_mismatches`; `developerInstructions override was provided and ignored while running`; `thread_unsubscribe_response_inner`; `unsubscribe_connection_from_thread`; `thread_background_terminals_clean_inner`; `Op::CleanBackgroundTerminals`; `thread_background_terminals_list_inner`; `list_background_terminals`; `paginated_thread_full_turns`; `history_settings`; `persisted_permission_profile`; `has_permission_override` | Set-name awaits its metadata update and responds before its notification; effective sandbox can differ from request; loaded resume may ignore overrides; detachment, cleanup, and inspection are distinct; `thread/read(includeTurns=true)` still hydrates full history; cold resume restores thread-owned permission state unless explicitly overridden |
| `codex-rs/login/src/auth/default_client.rs` | `get_codex_user_agent`; `env!("CARGO_PKG_VERSION")` | The first slash-delimited user-agent component contains the build package version |
| `codex-rs/app-server/src/models.rs` | `supported_models`; `show_in_picker`; `model_from_preset`; `supports_personality: false` | Default listing excludes hidden presets and always reports retired personality support as false |
| `codex-rs/app-server/src/request_processors/thread_delete.rs` | `pub(crate) async fn thread_delete`; `thread_delete_response`; `send_thread_deleted_notifications` | Descendant-first deletion, response-before-broadcast ordering, and one notification per deleted thread |
| `codex-rs/protocol/src/models.rs` | `MessagePhase`; `PartialAnswer` | Commentary and partial answers are not final answers; only final-answer or absent-phase text can supply portable completion |
| `codex-rs/protocol/src/protocol.rs` | `TokenUsageInfo::new_or_append`; `last_token_usage`; `SessionMeta`; `pub dynamic_tools: Option<Vec<DynamicToolSpec>>` | Usage is partial, while native dynamic tools persist in session metadata |
| `codex-rs/core/src/session/mod.rs` | `conversation_history.get_dynamic_tools`; `dynamic_tools` | Resume restores persisted dynamic tools when no replacement is supplied |
| `codex-rs/core/src/session/handlers.rs` | `clean_background_terminals`; `close_unified_exec_processes`; `Op::CleanBackgroundTerminals` | Accepted cleanup closes the thread's background terminals |
| `codex-rs/features/src/lib.rs` | `Feature::Collab`; `key: "multi_agent"`; `default_enabled: true`; `Feature::MultiAgentV2`; `key: "multi_agent_v2"`; `Feature::SpawnCsv`; `key: "enable_fanout"`; `Stage::Removed` | Active multi-agent controls stay disabled; retired fanout is neither written nor required as effective-state evidence |
| `codex-rs/core/src/tools/handlers/multi_agents/spawn.rs` | `handle_spawn_agent`; `Ok(SpawnAgentResult` | Native spawn can return a child identity while descendant work continues |
| `codex-rs/app-server-protocol/src/protocol/v2/experimental_feature.rs` | `ExperimentalFeatureListParams`; `pub thread_id: Option<String>`; `pub enabled: bool`; `ExperimentalFeatureListResponse` | Paginated per-loaded-thread effective feature state |
| `codex-rs/app-server/src/request_processors/catalog_processor.rs` | `experimental_feature_list_response`; `load_latest_config_with_session_layers`; `config.features.enabled` | Feature inspection reads refreshed effective thread config |
| `codex-rs/app-server-client/src/remote.rs` | `UDS_WEBSOCKET_HANDSHAKE_URL`; `server_version`; `userAgent`; `split_once('/')`; `split_whitespace().next()`; `Message::Text`; `Message::Binary` | Unix handshake URI, parsed user-agent version token, and selected-client binary-frame discard |
| `codex-rs/thread-store/src/local/update_thread_metadata.rs` | `requires_rollout_compatibility_update`; `patch.name.is_some()`; `live_writer::persist_thread` | A live name update takes the rollout-compatibility path and materializes the rollout before success |
| `codex-rs/app-server-transport/src/transport/stdio.rs` | `std::io::stdin().lock().lines()` | One JSON value per stdio line |
| `codex-rs/app-server-transport/src/transport/websocket.rs` | `IncomingWebSocketMessage::Text(text)` | JSON text framing |
| `codex-rs/protocol/src/dynamic_tools.rs` | `DynamicToolSpec` | Native tool declarations remain private |
| `codex-rs/protocol/src/items.rs` | `AgentMessageDelivery`; `AsyncUserInputQuestion` | Async delivery discriminator and exact question shape |
| `codex-rs/core/src/tools/handlers/request_user_input.rs` | `is_blocking: mode == ModeKind::Plan` | Default-mode requests are nonblocking |
| `codex-rs/core/src/tools/approvals.rs` | `ApprovalAction::WriteStdin`; `ExecApprovalKind::WriteStdin` | Stdin uses its own callback with one-shot approval/abort |
| `codex-rs/utils/path-uri/src/api_path_string.rs` | `LegacyAppPathString` | Wire strings preserve foreign path spelling without defining a local execution location |
| `codex-rs/core/src/config/mod.rs` | `approval_policy = \"untrusted\" is no longer supported`; `cfg.approval_policy == Some(AskForApproval::UnlessTrusted)` | Explicit public `untrusted` startup configuration is rejected; internal project trust still selects native approval behavior |
| `codex-rs/app-server/src/request_processors/thread_read_state.rs` | `fn eligible`; `pub(super) async fn publish`; `pub(super) async fn notify`; `manager.subscribed_connection_ids(id)` | Durable local receipts use thread subscribers; feature-source threads are ineligible |
| `codex-rs/app-server/src/config_manager.rs` | `load_latest_config`; `load_non_project_config`; `without_project_context()` | Configuration reads without a cwd exclude project context |
| `codex-rs/app-server-client/src/server_request.rs` | `AppServerReplyHandle`; `server_request_reply_handle`; `compare_exchange(false, true`; `self.request_id.clone()` | The upstream client retains originating-connection, exact-id, single-use replies without reconnect or retry; no wire method is added |

## Selected method matrix

All method declarations below are anchored in
`codex-rs/app-server-protocol/src/protocol/common.rs`.

| Portable/native purpose | Selected method or fact | Symbol anchor |
|---|---|---|
| Create and materialize native conversation | `thread/start` with a unique private `threadSource`, then `thread/name/set` and `thread/read(includeTurns: true)` before portable commit | `ThreadStart`, `ThreadSetName`, `ThreadRead` |
| Reconcile ambiguous native creation | exhaust paginated `thread/list`, match exactly one private `threadSource`, then verify with `thread/read` | `ThreadList`, `ThreadRead` |
| Reopen and verify native conversation | selected `thread/resume` and `thread/read` methods | `ThreadResume`, `ThreadRead` |
| Discover native descendants | paginated `thread/list` with `ancestorThreadId` | `ThreadList` |
| Foreground prompt and optional structured schema | `turn/start` with optional `outputSchema`, terminal `turn/completed` | `TurnStart`, `TurnCompleted` |
| Abort | `turn/interrupt` | `TurnInterrupt` |
| Detach preserved runtime | `thread/unsubscribe` | `ThreadUnsubscribe` |
| Clean and verify background work | `thread/backgroundTerminals/clean`, then exhaust `thread/backgroundTerminals/list` | `ThreadBackgroundTerminalsClean`, `ThreadBackgroundTerminalsList` |
| Verify foreground-only policy | exhaust `experimentalFeature/list` for the loaded thread | `ExperimentalFeatureList` |
| Full-access exact-byte file read | `fs/readFile` | `FsReadFile` |
| Model inventory | paginated `model/list` until `nextCursor` is absent | `ModelList` |
| Dynamic executable tool invocation | `item/tool/call` server request | `DynamicToolCall` |
| Native deletion | `thread/delete`, followed by broadcasts | `ThreadDelete`, `ThreadDeleted` |

The public TCP configuration contains host and port, not an invented auth or
retry surface. Unix-socket support uses positive platform support and returns
an honest unsupported failure elsewhere.

Caller-managed TCP and Unix attachment require exclusive server and thread
ownership for the Agent runtime. The selected server scopes experimental API
per connection while broadcasting native requests, so shared ownership cannot
provide a single authoritative dynamic-tool or interaction responder.

## Connection and framing

The configured connection mode selects one of these private transports:

| Mode | Framing | Ownership |
|---|---|---|
| Kit-spawned App Server | JSON Lines over child stdio | The backend owns child startup, readiness, and termination. |
| TCP | WebSocket text frames | The caller owns the endpoint; the backend owns the connection. |
| Unix socket | WebSocket text frames after the local handshake | The caller owns the socket endpoint; the backend owns the connection. |

Transport startup honors configured startup and request deadlines. A failure
after partial startup cleans up Kit-owned resources before returning the
original failure. Shutdown attempts every independent cleanup responsibility
without awaiting process, socket, or task completion while holding a shared
state guard.

The connection preserves raw JSON-RPC identifiers until a response is sent.
A stable internal rendering may be used as a lookup key, but it never replaces
the original identifier in a response. Malformed JSON, malformed JSON-RPC,
and malformed payloads for known methods are protocol failures. Binary frames
are discarded without a state transition, matching the selected clients.
Unexpected transport closure is an integration failure. Closure or fatal
decode failure completes all pending Kit-originated requests with a failure
rather than leaving waiters parked.

The selected `item/agentMessage/delta` decoder retains its typed text and
thread/turn/item scope. Missing or malformed fields are Protocol. It does not
infer item phase, buffer a synthetic final answer, or classify assistant deltas
as generic state-only events. Completed item and terminal turn processing
retain their independent authoritative roles.

Initialization is part of connection establishment. The selected initialize
payload omits `mcpServerOpenaiFormElicitation`; omission means that extended
OpenAI MCP form elicitation is not enabled. `explicitGatewayOauth` and `extensions` also remain unadvertised; experimental
API negotiation and disabled attestation retain their existing meaning. An
initialization failure tears
down the partially opened connection. The response `userAgent` supplies the
server version token after the first slash and before following whitespace.
Build validates it under the canonical wire-identity rule in
[`../SPECIFICATIONS.md`](../SPECIFICATIONS.md) and publishes no Agent after a
mismatch.

Initialization excludes `thread/closed` from this connection's notifications.
That event reports native in-memory unloading after the last subscriber has
detached; it is broadcast to every connection and does not prove durable
portable closure. Preserved-runtime detach, durable binding reconciliation,
and portable hierarchy invalidation remain backend-owned explicit operations,
so a delayed unload for a foreign or historical thread cannot become lifecycle
authority for the current Agent.

## Directional frame handling

Each valid inbound JSON-RPC frame has exactly one directional meaning:

- a response completes the Kit request with the same raw identifier;
- an App Server request requires a JSON-RPC response from the Kit; or
- an App Server notification has no JSON-RPC response.

Direction is decided before method-specific payload handling. A request must
not be treated as a notification merely because it later produces a portable
interaction. A notification such as `serverRequest/resolved` must not be
treated as a request merely because it affects an interaction lifecycle.

Kit-originated calls use the pinned request/response DTOs privately. Request
timeouts do not imply that a mutating App Server operation was not accepted.
Consequently, the backend never blindly repeats a timed-out mutating request.
Read-only calls may be retried only when the owning behavior explicitly proves
the repetition safe.

Before `thread/start`, the backend durably records a unique private feature
source token and sends it as `threadSource`. A lost response is reconciled by
exhausting the explicitly scoped paginated thread inventory, matching exactly
one token, and verifying that thread through `thread/read`; zero matches remain
recovery-owned and multiple matches are contradictory state. The mutation is
never replayed and no candidate is selected by cwd, time, ordering, title, or
active-thread heuristics. The token is backend-private recovery data, not
public context, configuration, identity, provenance, or capability.

The selected thread-store name update materializes its rollout compatibility
state before success. Before committing a newly created portable conversation,
the adapter additionally requires a full-history read retaining the exact native
thread/cwd binding and creation source token, with an empty typed turn list.
This read verifies the named creation; it is not a flushing or cold-resume API.
Malformed or contradictory readback preserves the original creation failure and
bounded native cleanup. No prompt, legacy-history override, or extra lifecycle
owner is introduced.

## Typed routing scope

After directional decode, every known App Server message is assigned a typed
semantic scope before it reaches backend behavior. Transport broadcast is
connection-global delivery; semantic scope still comes from the decoded
payload. The required scope choices are:

- connection scope for messages with no thread identity;
- thread scope for messages carrying a `threadId` but no turn identity; and
- turn scope for messages carrying both `threadId` and `turnId`.

Additional item or request identifiers refine one of those scopes; they do not
replace it with an untyped string route. Scope extraction validates required
identifiers and preserves them as backend-private typed values. It never
guesses a thread from whichever conversation happens to be active, and it
never broadcasts a thread-scoped event across conversations.

Connection-scoped messages are handled by connection-owned policy. Thread and
turn scopes resolve through the backend's durable native binding and live
route state. A missing, stale, foreign, or ambiguous binding is an explicit
protocol or lifecycle failure according to the owning root behavior; it is
not silently rerouted. A connection-global message must not invent a conversation
merely because its semantic target is absent.

One bound root thread owns its sourced descendant closure privately. Session
and parent-thread facts validate that relationship; native descendants do not
become public Conversations. Their events and interactions route through the
root binding only after that validation. A thread outside every bound closure
remains foreign.

The portable adapter disables both active native multi-agent features for every
managed root thread. Descendant routing remains necessary for defensive
recovery and other sourced native descendants; it does not normalize native
background work into the portable foreground lifecycle.

A typed connection event may update agent- or workspace-owned private state
only when its sourced payload and durable binding provide that aggregate
evidence. Otherwise it remains connection-global. Genuinely future input keeps
its raw scope privately until classification; scope is never inferred by
analogy to a known event.

The private representation may vary while preserving typed scope, owner
checking, and cleanup.

## One classification for known events

The pinned App Server method inventory is decoded to typed backend events and
classified exactly once. The same exhaustive classification owns known server
notifications, request-derived events after acceptance, and backend-generated
facts that enter notification or runtime behavior. Protocol decode, portable
notification conversion, and runtime sequencing must not each maintain a
competing method switch.

Every known event has one explicit outcome:

- a portable notification fact;
- an actionable interaction fact;
- an internal runtime or lifecycle fact, such as terminal turn completion or
  server-request reconciliation;
- a valid but unsupported backend fact; or
- a sourced adapter gap reported as not yet implemented.

Every known method has exactly one explicit typed outcome. Raw payload
preservation is allowed for diagnostics and for genuinely future methods
absent from the pinned inventory. A future method
cannot be converted into a portable success until its semantics and scope are
understood.

`turn/completed` is a typed terminal turn fact whose durable application is
owned by conversation behavior. `serverRequest/resolved` is a typed request
reconciliation fact. Neither is special-cased in transport code. Likewise,
`thread/deleted` is a typed lifecycle fact; creation's `thread/name/updated`
is private thread metadata. Neither is a generic public notification fallback.

The following additions are decoded against their owning `protocol/v2` DTOs.
Their source scope is validated before classification; no connection-level
metadata acquires an invented Workspace or Conversation.

| Method | DTO member | Scope and disposition |
|---|---|---|
| `thread/readState/changed` | `thread.rs` | Thread; validated private read-state metadata, never Kit acknowledgment or completion |
| `thread/reverted` | `thread.rs` | Thread; owned root or verified descendant invalidates with `InvalidState/AttachmentMismatch`; foreign history is ignored and durable recovery/cleanup ownership is retained |
| `thread/queue/changed` | `thread.rs` | Thread; unsupported queued execution |
| `thread/attachment/updated` | `thread_attachment.rs` | Thread; private attachment identity and operation, not managed knowledge |
| `thread/prediction/updated` | `thread_prediction.rs` | Thread/source turn; unsupported prediction, never prompt output |
| `project/changed` | `project.rs` | Connection; private project change |
| `thread/project/updated` | `project.rs` | Thread; private project association |
| `thread/environment/connected`, `thread/environment/disconnected` | `environment.rs` | Thread; private observations, no portable cwd or policy proof |
| `autoApprovalReview/strictReviewRequired` | `notification.rs` | Turn; private review progress, no responder |
| `rawResponse/completed` | `thread.rs` | Turn; private response/usage, not turn completion |
| `mcpServer/event/stream/notification` | `mcp.rs` | Connection/subscription; unsupported event stream |
| `account/gatewayOAuth/changed` | `account.rs` | Connection; private auth state, no exposed URL or native error |
| `modelProvider/authRecoveryStarted`, `modelProvider/authRecoveryCompleted` | `notification.rs` | Turn; private progress, no replay or terminal implication |
| `thread/realtime/item/started`, `thread/realtime/item/transcript/delta`, `thread/realtime/item/completed` | `realtime.rs` | Thread; unsupported realtime behavior, never portable output |

`thread/readState/changed` requires a valid thread identity and an object
`readState` with a string `revision`. The revision is opaque. `firstUnread` may
be omitted or null, or identify `threadStart` or a `turn` with a valid `turnId`.
That unread turn may be historical; it is not the active prompt's identity.
Malformed nested fields are `Protocol`. Valid receipts pass normal thread
ownership validation and remain private even when repeated. They never publish
portable output, acknowledge Kit notifications, resolve interactions, or
complete a prompt. Native publication may precede `turn/completed`; that
terminal fact retains its independent authority. The native publisher excludes
feature-source threads, including Kit creation's private `threadSource`, so
receipt emission is not required as live evidence for Kit-managed threads.

The additive `readState` and `readStates` fields on native read/list responses
do not change Kit history, ownership, or pagination. Inventory traversal still
uses all pages without exclusions; an RPC failure is not an empty inventory.
No read-state update, item lookup, prediction-fork, retained-output, environment,
replay, or realtime operation is added to the selected portable method matrix.

Cold resume can restore thread-owned permissions, including unnamed profiles,
when explicit permission overrides are absent. Kit resume retains its explicit
permission selection, cwd, roots, and workspace config and verifies the returned
effective policy before publishing a handle. A global configuration read without
a cwd now excludes project context; it cannot replace workspace configuration.

Shutdown rejection retains JSON-RPC code `-32600` with private
`data.reason = "serverShuttingDown"`. It maps through the existing safe
invalid-request classification without exposing native prose or automatically
replaying a mutation. The upstream client's connection-bound single-use reply
handle adds no JSON-RPC method or response acknowledgment; existing Kit callback
identity and no-resend rules remain authoritative.

Agent-message `delivery` and `questions` are validated in both live items and
full history. Valid asynchronous semantics are `Unsupported`; malformed fields
are `Protocol`. Function-call output accepts text or typed content arrays and
remains private. History continues to use `thread/read(includeTurns=true)`;
new pagination/resume-cursor operations do not widen the portable API.

## App Server requests to the Kit

The selected pinned server-request inventory is:

```text
item/commandExecution/requestApproval
item/fileChange/requestApproval
item/tool/requestUserInput
mcpServer/elicitation/request
item/permissions/requestApproval
item/tool/call
account/chatgptAuthTokens/refresh
attestation/generate
currentTime/read
```

Known methods require their exact pinned payload shape. Every source-declared
request outside the selected set above is answered once with `Unsupported`
and never enters interaction state. The typed inventory has no generic request
success path.

Command approval, file approval, permission approval, tool user input, and MCP
elicitation adapt to the one portable interaction lifecycle in
[`../NOTIFICATIONS.md`](../NOTIFICATIONS.md). `item/tool/call` invokes the
Codex dynamic-tool boundary and returns its sourced JSON-RPC response; a tool
handler that needs caller input uses the same portable interaction lifecycle.
Wire request identifiers, tool-call payloads, and response DTOs remain private.

Native terminal-input review depends on the process's retained launch permissions.
`SandboxAttempt::is_escalated` requires both an unsandboxed attempt and an ambient
policy that would require sandboxing under automatic selection. The unified-exec
runtime records that result in `TerminalPermissions::for_launch`; a requested
`require_escalated` flag alone does not establish a retained sandbox bypass.
With enabled network and no managed-network requirement, unrestricted filesystem
access does not require a sandbox, while restricted writes do. Subsequent
nonempty stdin can therefore remain ordinary baseline input even after an
explicit escalation request. Retained elevation, additional grants, or policy
drift affect native stdin review; unchanged baseline permissions skip that review
unless strict auto-review applies. These are native approval preconditions, not
a guarantee that every stdin write produces a server request.

ChatGPT auth-token refresh is answered as unsupported. Attestation generation
is answered as not yet implemented. These connection-scoped requests are not
assigned to an arbitrary conversation and do not create public notifications.

`currentTime/read` is thread-scoped. The connected agents-kit process returns
its wall clock at request handling in whole Unix seconds through an immediate
private response; the value is not App Server process time. The exchange
creates no durable or public interaction lifecycle. An invalid thread binding,
pre-epoch clock, or out-of-range value fails explicitly.

A normal actionable server request is answered at most once. The backend
records enough durable state to reject duplicate, late, foreign, malformed,
or wrong-kind resolutions. In the selected version,
`serverRequest/resolved` reports only thread/request identity after the native
callback is consumed or removed, so it cannot prove response acceptance. The
root notification and state specs own the resulting recovery-required
semantics; this protocol owns only the sourced request/response exchange.

## Sourced cascading thread deletion

The pinned contract defines:

```text
request:      thread/delete { threadId }
response:     {}
notification: thread/deleted { threadId }
```

`thread/delete` hard-deletes the selected persisted thread and every spawned
descendant thread. The App Server discovers the spawned subtree, prepares the
threads for deletion, deletes descendants before the selected root, removes
the associated State DB rows, and treats an already-missing rollout file as
already deleted when the remaining sourced state still identifies the thread.
An ephemeral, invalid, or genuinely missing root is rejected according to the
pinned handler.

The success ordering is normative:

1. all required rollout and metadata deletion completes;
2. App Server sends the JSON-RPC success response;
3. App Server emits one `thread/deleted` notification for each deleted thread,
   in the handler's deletion order.

The Kit therefore treats the response as completion of its requested native
cleanup and treats each later connection-global broadcast as an independently
routed typed thread lifecycle fact. It must not wait for the broadcast before
completing the client request, complete the request from the notification, or
apply the portable durable delete a second time when the notification arrives.

Each `thread/deleted` payload is routed by its own `threadId`. Descendant
notifications may refer to bindings other than the root request's binding.
The backend reconciles or invalidates matching private runtime state; an
unmatched notification remains a typed lifecycle observation rather than
being coerced into the requesting conversation. Duplicate or late broadcasts
use idempotent reconciliation.

For preserved-runtime cleanup, the backend exhausts `thread/list` with the
root's `ancestorThreadId`, includes the root itself, and applies inspection and
unsubscribe to that current closure. This is native resource ownership, not a
public descendant inventory.

## Error and retry boundary

`Turn.error` may accompany either failed or interrupted status. Interruption
preserves outer cancellation and an optional normalized cause. Failed turns
require an error; completed turns reject one. New `CodexErrorInfo` values map
`rateLimitExceeded` to `RateLimited`, `flexUnavailable` to `ServiceUnavailable`,
and `misalignmentPolicyViolation`/`tooManyDenials` to `PolicyRejected`. No HTTP
status is invented. Unknown string/object classifications use safe `Other`
normalization without retaining payloads; recognized malformed variants and
scalars/arrays remain `Protocol`. Optional misalignment fields are validated
then discarded, with no steering execution or native text persistence.


Transport and protocol failures map through [`../ERRORS.md`](../ERRORS.md).
Valid App Server behavior that the portable Kit cannot represent is
unsupported; valid behavior whose adapter is unfinished is not yet
implemented. Neither case is disguised as malformed protocol input.

Retries are bounded and operation-aware. Connection readiness and an
explicitly idempotent read may retry within configured deadlines. Prompt
start, abort, interaction response, dynamic tool response, thread mutation,
and thread deletion are not automatically replayed after an ambiguous
failure. Reconnection establishes a new initialized connection and invokes
backend recovery; it is not a transparent replay of in-flight mutations.

## Selected release deltas

The terminal matrix is shared by live notifications and full-history recovery:
completed without error succeeds only with eligible final output; completed with
error and failed without error are `Protocol`; failed with error retains its
normalized `Integration` failure. Interrupted without error is cancellation.
Interrupted with error remains `Cancelled` at `Prompt/Cancellation`, with the
normalized `Integration` error at `Prompt/ModelGeneration` as its cause.
Already-authoritative local timeout, cancellation, or primary failure retains
precedence. In-progress terminal notifications are malformed.

`rateLimitExceeded` maps to `RateLimited`, `flexUnavailable` to
`ServiceUnavailable`, and `misalignmentPolicyViolation` / `tooManyDenials` to
`PolicyRejected`. No HTTP status is invented. Misalignment explanation and
steering metadata remain private and never trigger automatic continuation.

Assistant `delivery: async` or any non-null `questions` is `Unsupported`, even
with `phase: final_answer`. Neither live nor recovered asynchronous output
becomes a Message, answer, or invented Interaction. This foreground restriction
supersedes the earlier mainline passive-async expectation. Ordinary answers
retain completion order.
`functionCallOutput` is typed tool-result data and cannot become assistant output
or another invocation. Changed collaboration statuses/activity and dynamic audio
output decode with their exact selected types; unrepresentable behavior is
`Unsupported`. Malformed metadata, auxiliary identities, or variants are
`Protocol` in both live and recovered history.

New known notifications have these exhaustive outcomes after typed validation:

| Methods | Scope | Outcome |
|---|---|---|
| `modelProvider/authRecoveryStarted`, `modelProvider/authRecoveryCompleted`, `rawResponse/completed`, `autoApprovalReview/strictReviewRequired` | Native thread/turn | Private nonterminal metadata; no public Progress, replay, approval, or prompt completion |
| `thread/environment/connected`, `thread/environment/disconnected`, `thread/attachment/updated`, `thread/project/updated` | Thread | Private metadata; no workspace/root/file mutation |
| `thread/queue/changed` | Thread | Unsupported native queue mutation |
| `thread/reverted` | Thread | Owned root or verified descendant invalidates runtime with `InvalidState/AttachmentMismatch`; foreign history is ignored; durable recovery and cleanup evidence is retained |
| `thread/realtime/item/started`, `thread/realtime/item/completed`, `thread/realtime/item/transcript/delta` | Thread | Unsupported realtime behavior |
| `account/gatewayOAuth/changed`, `project/changed` | Connection | Private metadata, never assigned to an arbitrary Conversation |
| `mcpServer/event/stream/notification` | Connection/subscription | Typed subscriptionId and notification method/required params, then Unsupported; no invented server, thread, or subscription owner |

Exact thread/turn identities are not trimmed. Descendant turns remain scoped to
that native thread and are never compared to the root prompt's turn. Verified
ancestry does not require a full-history read for metadata: ephemeral children
and early turns may have no readable durable history. Nullable projectId also
accepts omission. Signed usage/timestamp fields and only sourced
defaults are retained. Native nested metadata JSON stays private. One upstream
raw response does not define whole-prompt usage.

Fresh durable threads may use paginated native storage. The retained
`thread/read(includeTurns: true)` path still loads full history; resume uses
`excludeTurns: true`. No legacy-storage override or unused paging API is added.
Internal native workers can reject archive/deletion: preserve native rejection
and durable cleanup obligations, without force deletion or ambiguous replay.
