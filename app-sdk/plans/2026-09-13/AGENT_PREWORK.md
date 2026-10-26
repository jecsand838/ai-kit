# Agent integration prerequisite audit and implementation plan

Read-only audit for the 2026-09-13 app-sdk completion plan. No Cargo command, production edit, spec edit, or runtime reproduction was performed. Findings below are source/contract evidence, not new live proof. Files and line anchors refer to the current checkout and must be refreshed with `rg -n` before execution after upstream phases move lines.

## Decision and scope

Agent -> Workspace -> Conversation is already a usable static associated hierarchy. Its portable result DTOs, identities, errors, and notification receiver are constructible by external implementations. Do not introduce Agent normalization, an Extension dispatcher arm, a new KnowledgeBase seam, lower configuration reconstruction, or a new cross-crate runtime.

Two coordinated prerequisites are justified by the user's strengthened target:

1. Replace Agent's required concrete Toolset/ToolHandler composition with `T: ToolsetBackend` and `T::Handler`. This is a real source-level integration blocker, not merely missing coverage. Default handle types may remain ergonomic defaults; they cannot be required.
2. Add a narrow prompt-stream operation to the existing ConversationBackend seam if app agent nodes must produce actual incremental output. Terminal `prompt` and complete-message notifications cannot implement partial generation. Provider already has a publicly constructible, continuation-generic stream; do not add another provider interface.

The existing root and agent documents deliberately allow/require concrete Toolsets and exclude prompt deltas. The user's explicit newer direction authorizes replacing those rules in the specification-amendment phase. Do not implement first and leave contradictory ledgers behind. The source recommendations below are proposals until that phase is applied.

## Evidence and sources

- Normative lower-crate contracts: `agent-sdk/specs/API.md` (Agent/Workspace/Conversation contracts, notification constructor, direct provider/storage types); `agent-sdk/AGENTS.md`; `agent-sdk/specs/RUNTIME.md`, `NOTIFICATIONS.md`, `BUILD_CONFIG.md`, `custom/{AGENT,CONVERSATION,CATALOG,SERVER,WORKSPACE}.md`, `codex/{PROTOCOL,NOTIFICATIONS,CATALOG}.md`, and `opencode/{PROTOCOL,NOTIFICATIONS,CATALOG}.md`.
- Repository observations: concrete-tool ingress and runtime references listed below; terminal-only ConversationBackend; source-classified but discarded native text deltas; public notification construction and existing downstream hierarchy tests.
- Pinned Codex observation: checksum of `agent-sdk/vendor/codex-0.144.1.zip` verified as `51d5acab11f375d3dd85c29c970f1e83010f65ba242de736b55ccaec8f8a6c7f`, matching vendor-lock.toml. Archive member `codex-rust-v0.144.1/codex-rs/app-server/README.md:1399` describes ordered `item/agentMessage/delta` for an item. Lines 80-81 describe the turn lifecycle. Do not conflate these text fragments with durable terminal output or stream raw native ids.
- Pinned OpenCode observation: checksum of `agent-sdk/vendor/opencode-1.17.18.zip` verified as `03f7dd032afaeb8695f7a610fa93be22caeb8bb21783226363cd9b758ff9edf6`. Archive member `anomalyco-opencode-b1fc811/packages/schema/src/v1/session.ts:632` defines `message.part.delta` carrying sessionID, messageID, partID, field and delta. Preserve these native coordinates privately to prove ownership; stream only public text semantics.
- Verified external fact, accessed 2026-09-13: OpenAI's first-party [App Server README](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md) describes streamed text deltas and terminal turn notification; [OpenAI's App Server design article](https://openai.com/index/unlocking-the-codex-harness/) explains incremental item output followed by item and turn completion. The pinned archive, not rolling main, controls selected protocol syntax.
- Verified external fact, accessed 2026-09-13: first-party [OpenCode session schema](https://github.com/anomalyco/opencode/blob/dev/packages/schema/src/v1/session.ts) defines a distinct text-part delta event. Rolling dev corroborates, but does not replace, the selected archive.
- Used supplied agent research, agent change contract, public surface contract, and requested Rust core, async, testing, networking, abstraction, architecture, dependency/platform, and struct/enum rules. No vendor refresh or dependency addition is needed.

## Already ready: do not add work without failing evidence

| Contract | Evidence | Consequence |
|---|---|---|
| Non-Clone, concrete Agent hierarchy | `agent-sdk/src/agent.rs:60`, `workspace.rs` WorkspaceBackend, `conversation.rs:164`; external impls in `tests/agent_backend.rs:216`, `:395`, `:512` | App should consume `A`, `A::Workspace`, and associated Conversation directly. No separate child generics or erased adapters. |
| Agent info and conversation/workspace result construction | `AgentInfo::new`, `WorkspaceInfo::new`, `ConversationInfo::new` (`conversation.rs:52`) and validated typed ids | Do not add implementation identities or public-field escape hatches. |
| Notification construction | `notification.rs:22` (`ConversationNotification::new`), `:65` (kind enum), `:269` (`ConversationNotificationReceiver::from_stream`) | An external Conversation implementation can already emit both messages and actionable notifications. No notification payload prerequisite exists. |
| Notification receiver ownership | `notification.rs:246`, `:279`; `tests/notification_stream.rs:49`, `:138`, `:171` | Constructor is a value-stream adapter, not backend erasure. Existing retryable errors and terminal fusing should remain. |
| Provider-associated model and exact continuation | `custom/adapters/mod.rs:501-507`, `:564`; existing tests `custom_prompt_runtime.rs` | Preserve `P::Model::Continuation` only in the live loop; do not serialize it for App checkpoints or fork history. |
| StateStore and Database implementations | `AgentStorage<S>` plus direct `D` builder composition, public S/D bounds in builder | No new storage factory/registry, public knowledge seam, or Provider/Database/Agent wrapper needed. |
| Revision/cancellation semantics available to App | `specs/RUNTIME.md` exact workspace expected_revision, foreground prompt, Abort and Lifecycle sections | App's compensation, setup timeout, orphan task and repeated notification bugs are App work. Do not alter agent revisions to hide App's fence bug or make abort target a later prompt. |
| Built-in limitations explicit | `specs/opencode/CATALOG.md:12` rejects supplied executable tools; Custom/Codex reject confinement they cannot ensure | Generic ingress does not make unsupported native installation portable. Preserve preacceptance honest errors and cleanup. |

## P1 — trait-only Tool composition and explicit reduction

### Red tests first

Before production edits, add/extend public downstream tests that fail for the intended boundary:

1. In `agent-sdk/tests/public_api.rs` and `tests/agent_backend.rs`, construct Custom from a non-Clone caller Provider, StateStore, Database, Toolset, and a non-Clone associated Handler. Call builder directly with the Toolset implementation; never call `Toolset::from_backend` in this case. Prove inferred supplied child types retain the same `S,D,P,T`. Existing direct-provider tests alone are insufficient.
2. In `tests/custom_prompt_runtime.rs`, replace the forced wrappers at existing lines 801, 1259, 1427, 1806, 1907, 1981 with direct scripted test-local `ToolsetBackend` where applicable. Prove function name routing, distinct associated handler creation per call, completed `is_error=true` values, repeated interactions, terminal continuation replay and tool shutdown exactly once. A caller-owned enum case should combine two distinct Toolsets and Handlers without default handles.
3. In `tests/builder_failure_cleanup.rs:197-202`, pass direct non-Clone test Toolsets and inject a later validation/provider failure. Assert reverse cleanup once, all independent cleanup attempted, primary error preserved, no Arc clone leaks/early drops. Add wrong native composition (OpenCode tool rejection) with cleanup evidence.
4. In `tests/strict_live_suite/custom.rs`, migrate the shell scenario to explicitly caller-built Local tool registration and preserve existing real subprocess output, cancellation, limits, and teardown assertions. Preserve Codex dynamic-tool and OpenCode unsupported cases in their existing live suite.
5. Update source/compile contracts first to reject required concrete Toolset/ToolHandler signatures at the Agent seam, new erasure, hidden `T: Clone`, stale removed APIs/config, and independent Handler generic parameters. Do not assert exact private helper names, match spelling or source file choreography.

### Tight proposed public design

Keep one new parameter `T: ToolsetBackend` on the supplied builder/hierarchy only. `AgentStorage<S>` remains unchanged. Keep `T::Handler` associated; no Handler generic. A homogeneous `Vec<T>` is intentional. Heterogeneous tool inventories are caller-owned exhaustive Toolset/Handler enums or the caller choosing tool-sdk's own supplied Toolset intentionally.

A tight way to avoid type-changing repeated ingress that silently drops an already accepted tool vector is to replace the existing typed-provider constructor pair, rather than add another family:

```rust
// Concept sketch; exact signatures go only in specs/API.md.
pub struct AgentBuilder<S, D, P, T = tool_sdk::Toolset>
where
    S: storage_sdk::StateStoreBackend,
    D: storage_sdk::DatabaseBackend,
    P: provider_sdk::ProviderBackend,
    T: tool_sdk::ToolsetBackend,
{
    // Existing fields remain; tools are owned in one place.
    toolsets: Vec<T>,
    // ...
}

// Default create/reopen remain. Replace create_for_provider/reopen_for_provider
// with create_for_backends::<P2, T2>/reopen_for_backends::<P2, T2>.
// These select only static context. They never construct P2 or T2.
impl<S, D, P, T> AgentBuilder<S, D, P, T>
where
    S: storage_sdk::StateStoreBackend,
    D: storage_sdk::DatabaseBackend,
    P: provider_sdk::ProviderBackend,
    T: tool_sdk::ToolsetBackend,
{
    pub fn with_toolset(mut self, toolset: T) -> Self {
        self.toolsets.push(toolset);
        self
    }
    // with_provider<P2> preserves T; with_knowledge_database<D2> preserves T.
    // build returns Agent<S,D,P,T>.
}
```

The public root supplied defaults are optional conveniences, not required implementation coupling. App still has only `A: AgentBackend` and its app control StateStore generic, regardless of how many parameters the supplied Agent implementation carries. Do not add P/D/T to App.

Private long-lived tool ownership can use `Vec<Arc<T>>` after builder acceptance because handlers and cleanup need shared access and ToolsetBackend does not require Clone. Clone the Arc at task boundaries, never add `T: Clone`. Change `CustomTools::handler` to return `T::Handler`. Methods that step/cancel a handler use `&mut T::Handler` (or an owner-local method generic `H: ToolHandlerBackend` only if shared behavior genuinely requires it). Preserve the existing shared owner and shutdown ordering.

### Explicit deletion: implicit shell construction

Current Agent validation calls `CustomShellTool::toolset` and appends a concrete Local Toolset (`builder.rs:331`, `custom/tools/mod.rs:54-83`). That cannot fit a direct homogeneous caller `T` without erasure or another wrapper. The reductive recommendation is to remove the implicit Agent shell construction/config path and make shell an ordinary tool supplied by the caller, using the public Toolset trait and portable ToolInfo/invocation/result DTOs. This removes Agent's automatic concrete tool construction and task-local shell side channel.

Delete `CustomShellConfig` and its `CustomAgentConfig` field/accessor/constructor argument, the private `CustomShellTool`, `CUSTOM_SHELL_CWD`, and Agent builder/server hooks listed below. Migrate shell examples and tests to caller-owned Local registration or caller-owned direct Toolset. Do not replace this with an Agent Shell/External dispatcher, another public shell runtime trait, a shim, compatibility alias, provider shell configuration, or App's direct Tool dependency.

This is a visible ergonomic reduction: users explicitly supply the shell tool instead of a shell config on Agent. Document it as such. It does not remove the ability to execute shell tools. The prior automatic admitted-prompt-cwd injection is also removed; caller-owned tools must expose/validate their needed workspace/cwd input themselves. Tests/docs must make that change explicit rather than imply unchanged shell-cwd behavior. If preserving automatic cwd injection becomes a nonnegotiable product requirement, stop and amend the portable Tool invocation contract on evidence; do not smuggle context through globals or task locals. That is a separate behavioral decision, not required for App/rurr core completion.

### File-level modification/deletion inventory

| Existing path:line | Change |
|---|---|
| `agent-sdk/src/builder.rs:24`, `:33`, `:43`, `:67`, `:90`, `:102`, `:107`, `:132` | Add T to builder and build owner, type-selecting construction, direct T ingress, preserve T through other builder methods and returned Agent. |
| `agent-sdk/src/builder.rs:331` | Delete automatic shell Toolset construction. |
| `agent-sdk/src/agent.rs:142`, `:150`; `workspace.rs:839`, `:847`; `conversation.rs:203`, `:211` | Supplied handles and three closed dispatcher enums retain T; associated children preserve S,D,P,T. Keep no Extension/dyn ingress. |
| `agent-sdk/src/backends/custom/agent/mod.rs:32`, `:42`, `:46`, `:55`, `:88`, `:105`, `:166`, `:370` | Carry T through composition owner; replace cloned concrete Toolsets with Arc<T>; preserve reverse cleanup and transfer ownership. |
| `agent-sdk/src/backends/custom/tools/mod.rs:26`, `:30`, `:39`, `:209`, `:277`, `:291`, `:575` | Generic owner and catalog input; return T::Handler; one ownership list; no duplicate generic wrapper. |
| `agent-sdk/src/backends/custom/tools/mod.rs:21`, `:51-205` | Delete task-local shell binding and automatic shell implementation after caller tool tests exist. |
| `agent-sdk/src/backends/custom/adapters/mod.rs:37`, `:193`, `:301`, `:330`, `:474`, `:485` | Carry tools as T, step associated handler, remove shell scope, preserve direct P::Model and continuation. |
| `agent-sdk/src/backends/custom/server_requests/mod.rs:20`, `:23`, `:123`, `:902` | Preserve T through tool interaction owner; no handler normalization. |
| `agent-sdk/src/backends/custom/server/mod.rs:89`, `:93`, `:101`, `:116`, `:124`, `:137`, `:148`, `:178`, `:1012` | Propagate T only where the existing owner actually retains generic runtime/tools. Do not add T to passive records. |
| `agent-sdk/src/backends/custom/server/mod.rs:1360-1373` | Delete shell-only cwd validation and callers; ordinary request cwd validation remains in owning context. |
| `agent-sdk/src/backends/custom/server/runtime.rs:15`, `:25`; `custom/conversations/mod.rs:18`; `custom/conversations/runtime.rs:29`, `:119`; `custom/workspace/mod.rs:23`, `:29`, `:37`, `:72`, `:81`, `:95` | Carry T through existing connected runtime types/impls; no new suffix carrier family. |
| `agent-sdk/src/backends/codex/agent/mod.rs:22`, `:30`, `:44`; `codex/agent/runtime.rs:23`, `:36`, `:122`, `:392`, `:417`, `:458`, `:554`, `:566` | Generic native installed-tool owner and typed request routing; retain native provider behavior. |
| `agent-sdk/src/backends/codex/tools/mod.rs:31`, `:35`, `:39`, `:68`, `:73`, `:386`, `:395`, `:402`, `:427`, `:1368`, `:1441` | Generic retained tool registrations, Arc<T> borrowing, associated handler, existing cancellation and cleanup. |
| `agent-sdk/src/backends/codex/workspace/mod.rs:19`, `:28`, `:43`; `codex/conversations/mod.rs:20`; `codex/conversations/runtime.rs:24`, `:28`, `:235` | Carry T where CodexTools is retained. OpenCode carries no unused tools or phantom value; its rejection stays builder-owned. |
| `agent-sdk/src/config/custom.rs:82-183`, `:196`, `:206`, `:239`; `config/mod.rs:6`; `lib.rs:72`; `backends/custom/mod.rs:15` | Delete CustomShellConfig and all exports/config hooks; retain other Custom loop/approval settings. |
| `agent-sdk/src/lib.rs:19-27`; `tests/public_api.rs`, `tests/agent_backend.rs`, `tests/custom_prompt_runtime.rs`, `tests/builder_failure_cleanup.rs`, `tests/strict_live_suite/custom.rs`, `tests/source_contract/public_surface.rs` | Teach/test direct composition, migrate shell construction, remove stale tuple/constructor assertions. |
| root `AGENTS.md`, `ARCHITECTURE.md`; `agent-sdk/AGENTS.md`, `ARCHITECTURE.md`, `README.md`, `specs/API.md:285-354`, `BUILD_CONFIG.md:28`, `custom/{AGENT,CATALOG,SERVER,WORKSPACE,CONVERSATION}.md`, `docs/STRICT_TESTING.md` | Amend contradictory concrete-tool and shell wording; one exact API inventory only; document unchanged native unsupported cases. |

The file-level inventory above is the durable execution reference; the temporary raw search inventory is non-required audit scratch, not an execution dependency.

### Definition of done

New direct non-Clone tool tests pass, exact associated child types compile, failure cleanup tests pass, existing provider/storage/continuation/notification/abort tests pass, supplied built-ins still work, every shell scenario is migrated rather than skipped, no concrete tool requirement remains in Agent composition. Independent subagent review checks (a) public generic boundary, (b) resource ownership/clone and cancellation, (c) deletions and no compatibility paths. Resolve all substantive findings before marking P1 complete.

## P2 — real incremental prompt output at the existing Conversation seam

### Why needed and available lower support

`ConversationBackend::prompt` returns only terminal `PromptResponse` (`conversation.rs:172-176`). `ConversationNotificationKind` has no partial-output kind (`notification.rs:65`); `specs/NOTIFICATIONS.md` explicitly says Message is complete text. Custom uses `model.generate` at `custom/adapters/mod.rs:549`; Codex `item/agentMessage/delta` is decoded into a kind and then discarded from portable output (`codex/protocol/notifications.rs:368`, `codex/notifications/mod.rs:211`); OpenCode `message.part.delta` loses its message/part/delta payload in `into_session` and is classified private (`opencode/protocol/inbound.rs:69`, `:490`, `opencode/notifications/mod.rs:82`). App cannot restore discarded data using public traits.

Provider already supports direct caller streams: `provider-sdk/src/generation.rs:1704` has public `GenerateEventStream<C>::new<S>`, `:1661` ContentDelta and `:1675` Completed(Box<GenerateResponse<C>>); `models.rs:175` returns exactly the Model's Continuation. These facts were confirmed by the sibling provider audit. No new Provider trait, provider identity, or boxed native response is required.

### Proposed minimal public sketch

Use the existing ConversationBackend trait, a single passive event enum, and one opaque output stream in `agent_sdk::prompt`. Do not invent a second behavior trait, subscribe/poll/background runner family, durable token log, or reuse actionable notification settlement for output fragments.

```rust
// Public supporting values belong in prompt, not crate root.
pub enum PromptEvent {
    OutputDelta { text: String },
    Completed(PromptResponse),
}

// Opaque, non-Clone, Send; a public generic value constructor permits an
// external ConversationBackend to provide its own stream without Agent ingress.
pub struct PromptStream { /* private stream ownership + terminal state */ }
impl PromptStream {
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: futures_util::Stream<Item = Result<PromptEvent, SdkError>>
            + Send + 'static;
    pub async fn recv(&mut self) -> Option<Result<PromptEvent, SdkError>>;
}

pub trait ConversationBackend: Send + Sync + 'static {
    // Existing operations stay on this same seam.
    fn prompt_stream(
        &self,
        request: prompt::PromptRequest,
    ) -> impl std::future::Future<Output = Result<prompt::PromptStream, SdkError>>
        + Send + '_;
}
```

Exact names are proposed. Require one run owner/driver shared by `prompt` and `prompt_stream`; ordinary prompt collects the same generation result without maintaining another loop. Keep terminal `prompt` available without assuming every custom Model supports streaming; the owner can have an exhaustive internal delivery mode while transcript advancement, tool execution, cancellation and terminal commit remain one implementation. Do not make every ordinary prompt call a provider stream if that would silently break valid providers supporting only `generate`.

Define semantics in the amendment before implementation: partial text is transient, ordered within the active portable prompt, not replayed after process loss, and not a committed state mutation. `Completed(PromptResponse)` is authoritative after Agent terminal durability and can differ from concatenated partial text (native prompts can contain intermediate assistant messages/tool turns). Reasoning, native ids, commands, tool input/output and protocol frames are not streamed as text. Exactly one terminal item or outer error; premature EOF is Protocol; no items after terminal. Consuming reads fuse termination. Output fragment size, pending bytes and queue capacity are bounded. When a consumer stalls, the protocol reader must remain able to process cancellation and interactions; exhaustion fails observation explicitly without allocating unbounded buffers or silently claiming a complete stream. Document that observation loss/drop does not cancel already accepted prompt work; `abort` retains the same generation-fenced semantics. A stream request can return honest Unsupported before model acceptance when the selected backend cannot provide it.

A fixed public delta cannot reveal which private Codex item phase is final before item completion. Keep the final terminal response authoritative rather than manufacturing phase certainty. For OpenCode, prove current prompt/session/message/part ownership with existing typed owner paths before output publication, reject nontext fields for public output, preserve exactly one global SSE source, and treat reconnect gaps as observation interruption unless recovered fragments can be proved complete. Do not pretend transient delta history is durable.

### Red tests first

- New `agent-sdk/tests/prompt_stream.rs`: external non-Clone ConversationBackend constructs PromptStream; delta ordering; exact terminal response including structured_output/usage; error propagation; EOF-before-terminal; no output after terminal; receiver drop releases only observation, not accepted prompt; overflow bounded; abort and close unblock receiver; second prompt cannot steal preceding generation output; no durable notification duplication.
- Extend existing `tests/custom_prompt_runtime.rs`: supplied direct Model returns actual GenerateEventStream<C>; interleave content delta, function-call completion, tool result, next-model exact continuation and final response. Assert no partial state becomes durable; intermediate text does not overwrite final output; failed/absent stream support never pretends streamed success. Keep ordinary generate-only test provider supported for nonstreaming prompt.
- Extend source-contract protocol coverage and deterministic native event tests through public APIs for Codex item delta and OpenCode exact message/part delta ownership. Malformed typed data produces Protocol; unrelated conversation/generation and reasoning/tool fragments produce no false public output. Do not freeze enum names or private routing files.
- Add real Custom, Codex and OpenCode streaming tests to existing strict-live suite; require actual partial item before terminal from configured capable setup, keep bounds/timeouts, exact terminal result, notification resolution while streaming, abort after partial output, and clean shutdown. Record native mode genuinely unsupported instead of passing with terminal-only output; if the core streaming capability is part of completion, each claimed supporting lane must have real proof.
- App incremental tests consume only `ConversationBackend::prompt_stream`; ordinary delivery uses `prompt`. Terminal-only Agents remain valid for ordinary runs; no native event or provider dependency in App. Include two-agent workflow with partial output on different nodes, node metadata added by App, state handoff only after terminal node commit, and recovery starting from last committed checkpoint.

### Modify/add inventory

- `agent-sdk/specs/API.md:88-96`, `:226-270`, `:378` (prompt module inventory, ConversationBackend operation, receiver construction); `specs/RUNTIME.md` Foreground Prompt and lifecycle; `specs/NOTIFICATIONS.md` clarify durable complete notifications versus transient prompt output; backend `CONVERSATION.md`, `NOTIFICATIONS.md`, `PROTOCOL.md` selected mapping/source rows.
- `agent-sdk/src/prompt.rs:256` extend owning prompt module with PromptEvent and PromptStream; `src/conversation.rs:164`, `:238` and both supplied trait/dispatcher impls add operation; `lib.rs` expose supporting values only through public prompt module.
- `agent-sdk/src/backends/{custom,codex,opencode}/conversations/mod.rs:109`, `:105`, `:155` respectively: factor existing admitted-prompt preparation into each backend's existing owner and add the new trait operation without parallel implementations.
- `agent-sdk/src/backends/custom/adapters/mod.rs:474-604` one shared loop with generation delivery mode; `custom/conversations/runtime.rs:29`, `:119`; `custom/server/runtime.rs:25`; `custom/server/mod.rs:137` attach bounded output observer to existing generation owner; no new prompt-run family.
- `agent-sdk/src/backends/codex/protocol/notifications.rs:257`, `:368`; `codex/notifications/mod.rs:211`; `codex/conversations/runtime.rs:24` and existing agent runtime event routing own typed data and per-generation output attachment. Root dispatcher operation propagates to actual backend.
- `agent-sdk/src/backends/opencode/protocol/inbound.rs:69`, `:490`; `opencode/protocol/schemas.rs:465`, `:1604-1618` (selected `OpenCodePartDeltaEvent` and lossy `into_session` conversion); `opencode/notifications/mod.rs:82`; `opencode/agent/events/mod.rs:1320`; `opencode/conversations/runtime.rs:26`; `opencode/state_store/prompt.rs:69` attach transient output to already owned prompt, not persisted records.
- Add `agent-sdk/tests/prompt_stream.rs`; extend the existing tests above; `scripts/verify_strict_live.sh:37-49` explicitly run the new deterministic target before strict-live; `docs/STRICT_TESTING.md`; focused public doc examples and Rustdoc.

### Definition of done

External direct Agent implementations can implement the new operation entirely using public DTOs and value constructors. Existing terminal prompt behavior is retained through the shared owner. Actual incremental output reaches App through traits; no fake progress or terminal-only imitation is counted. Cancellation, interaction delivery, shutdown, replay and continuation tests pass. Independent reviewers inspect (a) protocol ownership and no reasoning/native payload leakage, (b) bounded queues and accepted-work ownership, (c) API minimality and loop deduplication. All findings resolved before P2 complete.

## Gates and execution constraints

The parent plan owns this coordinated workspace change and must replace stale 'do not validate until reduction assembled' prose with its test-first phase gates. Red behavior tests are required before implementation, followed by the smallest target and no parallel Cargo invocations. Representative focused commands after fixtures are added:

```sh
cargo test -p agent-sdk --test public_api --all-features
cargo test -p agent-sdk --test agent_backend --all-features
cargo test -p agent-sdk --test builder_failure_cleanup --all-features
cargo test -p agent-sdk --test custom_prompt_runtime --all-features
cargo test -p agent-sdk --test notification_stream --all-features
cargo test -p agent-sdk --test prompt_stream --all-features
cargo test -p agent-sdk --test compile_fail --all-features
cargo test -p agent-sdk --test source_contract --all-features
```

Run only feature lanes compatible with the workspace's documented storage conflict split; do not invent all-workspace-all-features. Agent's final exact gate is:

```sh
source agent-sdk/strict-tests.env
bash agent-sdk/scripts/precheck_strict_live.sh
bash agent-sdk/scripts/verify_strict_live.sh
```

`verify_strict_live.sh` already owns lib/source/final_audit/public_api/compile_fail/agent_backend/notification_stream/builder_failure_cleanup/custom_prompt_runtime/opencode_event_ownership, real strict_live, rustdoc/doctest, clippy and formatting. Amend it for new targets without dropping existing ones. The coordinated plan must additionally run the provider, tool and storage integration lanes affected by P1/P2, App two-agent workflow lane and the final workspace release gates. Keep deterministic, compile/source, protocol fixture and strict-live evidence separately labeled. No new credentials, new dependencies, native protocol refresh, public factory, integration crate, observability-sdk, tracking server/UI or CLI belongs in this prework.
