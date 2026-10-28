# Tool prerequisite audit for app-kit completion — 2026-09-13

Scope: direct caller ToolsetBackend→associated ToolHandlerBackend composition, source-neutral execution/interactions, lifecycle, Agent’s concrete Toolset dependence, and same-crate MCP serving portability. Read-only source/API audit; no cargo, production edits, or fresh strict-live execution.

## Decision

**Tool’s two traits and execution DTOs already permit independent non-Clone implementations. The mandatory integration fix is in Agent’s Toolset consumption, not an expanded Tool backend API.** Agent currently requires supplied Toolset/ToolHandler handles; that conflicts with the user’s stronger requirement that lower-crate integration directly preserve caller trait implementations. Existing root/spec text explicitly permits concrete Toolsets, so amend that text first under the user’s authorization.

McpServer currently requires supplied Toolset too. This is a real same-crate reuse limitation, but it does **not** block App workflow completion because App never depends on Tool or McpServer. If the completion scope includes making every existing Tool consumer direct-static, the small generic serving amendment below closes it. Label it supporting Tool follow-through, not a fabricated App prerequisite.

## Source inventory and exact plan anchors

| Owner/file:line | Observation / planned operation |
|---|---|
| `tools-kit/src/toolset.rs:20` | Keep existing ToolsetBackend with associated Handler and three methods. No Clone bound. |
| `tools-kit/src/handler.rs:10` | Keep unique ToolHandlerBackend::step; no Clone or Sync bound on child. |
| `tools-kit/src/execution.rs:13`, `:79`, `:153`, `:168` | ToolInvocation, ToolResult, input/output enums are public source-neutral DTOs; foreign implementations can construct completion and error values without supplied handles. |
| `tools-kit/src/interaction.rs:28`, `:48`, `:128`, `:146` | Foreign handlers can construct fresh IDs, requests, form/text kinds and responses. No interaction factory needed. Private `validate_response` at :62 is an implementation helper, not a constructor restriction; do not publicize it speculatively. |
| `tools-kit/tests/extension_backend.rs:39`, `:44`, `:70` | Existing non-Clone foreign Toolset and associated Handler fixtures exist, but the main test normalizes via Toolset::from_backend. Add a raw generic usage assertion, then Agent integration proving that normalization is unnecessary. |
| `agents-kit/src/builder.rs:24`, `:33`, `:102`, `:123`, `:128`, `:132` | Mandatory: add one root `T: ToolsetBackend` type, preserve it through builder/Agent associated hierarchy and owned composition. Replace concrete per-item Toolset builder ingress with an atomic generic vector ingress. No independent Handler generic. |
| `agents-kit/src/backends/custom/agent/mod.rs:43`, `:52`, `:55`, `:88`, `:105`, `:370` | Mandatory: existing composition owner/state keeps provider plus Toolsets; extend same owner with T, store privately shared Arc<T> values, clone Arcs when preparing, never require T:Clone. |
| `agents-kit/src/backends/custom/tools/mod.rs:40`, `:209`, `:277`, `:291`, `:575` | Mandatory: extend existing catalog/runtime owner with T, store `Vec<Arc<T>>`, return `T::Handler`, retain schema/collision/routing validation. Replace concrete Toolset clones at async lock edges with Arc clones. |
| `agents-kit/src/backends/custom/adapters/mod.rs:299`, `:301`, `:330` | Mandatory: use associated T::Handler for step/cancel. Reuse the existing result/interaction loop, keep is_error as completed model-visible failure and outer ToolError distinct. Bound cancellation under owning Agent deadline. |
| `agents-kit/src/backends/codex/agent/mod.rs:44`; `agents-kit/src/backends/codex/tools/mod.rs:39`, `:395`, `:402`, `:427`, `:784`, `:1368`, `:1441` | Mandatory: migrate native Codex managed-tool registration/catalog/call/shutdown paths too. Fixing Custom alone leaves a built-in coupling path. Native OpenCode continues honest unsupported behavior where its portable tool integration is unavailable. |
| `agents-kit/src/builder.rs:331`; `agents-kit/src/backends/custom/tools/mod.rs:51`, `:54`, `:83` | Generic migration trap: builder implicitly constructs and pushes a Kit Local shell Toolset from CustomShellConfig. `Vec<T>` cannot accept it. Adopt explicit caller-supplied ordinary shell tools and remove implicit lower-product construction/config/redundant shell route; migrate tests/docs. Do not introduce a private Shell/Caller enum to hide continued cross-crate built-in coupling, or normalize all T into Toolset. See reduction decision below. |
| `tools-kit/src/sources/mcp/server/mod.rs:49`, `:58`, `:64`, `:171`, `:172`, `:178`, `:249` | Supporting generic serving amendment: make start generic over T, make existing handler generic over Arc<T>, retain non-generic McpServer owner because its task encapsulates the generic server. Existing list/call code already only needs public trait behavior. |
| `tools-kit/tests/mcp_server_feature.rs:42`, `:340` | Add direct non-Clone custom T serving tests if serving amendment adopted; keep real socket protocol fixture, pagination/interaction/error mapping/shutdown/drop tests. Scripted service is deterministic, not strict-live. |
| `tools-kit/docs/PORTABLE_API.md:30`, `:139`; `tools-kit/AGENTS.md:62` | Clarify direct-static consumption is legal alongside optional Toolset normalization; amend serving signature only if adopted. Exact lines should be re-resolved before edit. |

The Agent audit owner should expand propagation anchors across Agent, Workspace, Conversation, Custom server/workspace/conversation, and Codex types. This report identifies tool-owned prerequisite facts rather than duplicating its full file ledger.

## Chosen minimal composition design

Extend existing owners, preserving homogeneous T (caller-owned exhaustive Toolset/Handler enums provide runtime heterogeneity). Root storage may hold `Vec<Arc<T>>` privately so ready handlers can be constructed without holding a mutex guard across await and without requiring T:Clone. Public builder should accept already-built values in one `with_toolsets<T2: ToolsetBackend>(Vec<T2>)` operation that changes only the root T parameter. Replacing the whole inventory makes type change explicit and avoids a speculative heterogeneous registry or tuple/HList system; document replacement semantics and preserve cleanup authority when replacing an already supplied inventory, or restrict the type-changing operation to the empty/default builder state rather than silently dropping owned runtimes. Prefer a single construction ingress that receives the complete vector up front; do not retain several overlapping append/replace helper families.

Illustrative owner shape (existing owner extended, fields remain private):

```rust
struct CustomTools<T: ToolsetBackend> {
    inner: Arc<CustomToolsInner<T>>,
}

impl<T: ToolsetBackend> CustomTools<T> {
    async fn handler(&self, name: &str) -> Result<T::Handler, KitError> {
        let (source, name) = self.admitted_source(name)?;
        source.tool_handler(name).await.map_err(KitError::from)
    }
}
```

`admitted_source` is a receiver method on this existing owner, returning a private Arc clone and validated ToolName; the sketch is not permission to add a one-field generic adapter or public routing DTO. Source admission and cleanup must be owned by the existing Agent lifecycle, while backend lifecycle obligations remain the public Tool contract.

Optional supporting McpServer change (single constructor, no new backend trait or public server type parameter):

```rust
impl McpServer {
    pub async fn start<T: ToolsetBackend>(
        toolset: Arc<T>,
        config: McpServerConfig,
    ) -> Result<Self, ToolError> { /* existing start body, generic handler */ }
}

struct McpServeHandler<T: ToolsetBackend> {
    toolset: Arc<T>,
    // Existing config fields stay here.
}
```

Caller retains an Arc<T> to explicitly shut down its Toolset after server shutdown; server never calls ToolsetBackend::shutdown. Explicit Clone impl clones the Arc, not T. A public owned `start<T>(T)` also supports non-Clone values, but can consume the caller’s only cleanup owner; shared Arc input makes independent lifecycle responsibility clear. Change all existing examples/tests in one breaking migration, no deprecated alternate constructor. No new dependency is required (std::sync::Arc, rmcp, axum, Tokio already present).

## Tests first, in order

1. **Compile success with no normalization:** add an external non-Clone Toolset and non-Clone Handler; instantiate generic Agent builder directly with it and a caller Provider/StateStore. Must fail at existing concrete `with_toolset` boundary before production migration. Include caller-owned exhaustive tool/handler enums and reject independent child mismatch through ordinary type checking.
2. **Behavior:** same Custom prompt invokes caller handler twice with a function continuation between rounds, handles two distinct interaction requests, retains exact input call correlation, and returns completed is_error without converting it to outer transport failure. No supplied Toolset construction may be required by the fixture. Existing deterministic providers prove boundary behavior only.
3. **Lifecycle:** pending handler cancellation, construction/list failure, Agent build failure, shutdown waiter cancellation, repeated shutdown, and shared-source fan-out retain ownership and bounded cleanup. Count framework shutdown calls per owned source and prove no T:Clone constraint. Direct foreign backend lifecycle semantics remain the implementer’s contract; do not accidentally rely on Toolset’s private decorator sealing/replay.
4. **Built-in regressions:** migrate existing Custom Local/MCP/shell and native Codex dynamic tool fixtures; prove same behavior via `T=Toolset` as one legal caller choice. Multi-source name collision still fails; preexisting configured names absent from inventory still fail. No provider-native tool payloads cross into App.
5. **Generic serving, if adopted:** a custom non-Clone T behind Arc is served without Toolset::from_backend, inventory and actual call work over MCP, foreign associated Handler returns form interaction and model-visible error correctly, server shutdown/drop/cancellation never call T.shutdown, caller can shut T down afterward. Retain eager bind/invalid config/pagination/error/interaction tests.
6. Extend source/compile guards only to reject prohibited cross-crate concrete signatures, normalization, public dyn seams, duplicated contracts and child ingress. Do not freeze private class names or generic parameter spelling.

Each step’s DoD is tests added and observed failing for intended reason before implementation, final focused tests green, documented full owning gates green, and independent subagent code review with all material findings resolved. No claims of built-in MCP proof from foreign fixtures.

## Explicit reductions

- Replace concrete Agent ingress/handler storage; delete obsolete concrete append/normalization paths and old examples instead of keeping shims.
- Reuse current CustomTools, catalog, composition state, handler loop, cancellation owner, Codex inventory and lifecycle owners. No generic runtime framework, trait-object registry, source identity enum, second inventory, or callback framework.
- Retain optional `Toolset::from_backend` and its exactly one private Extension arm in each Tool dispatcher; user’s rule forbids requiring it at cross-crate boundaries, not its existence as an optional supplied Tool API. Built-ins remain concrete variants.
- Remove implicit Agent shell construction if caller-built trait-only composition is chosen. Preserve the *capability* via caller-supplied ordinary Local/foreign shell tool and tested examples; the reduction is the implicit Agent config/construction convenience. Do not silently drop shell behavior without migration proof.
- Do not widen ToolInfo/ToolResult/interaction DTOs; they already cover the necessary generic data. Public IDs are already constructible.
- Do not move App action nodes into tools-kit, or add App→Tool dependencies to avoid an app-native action design. Tool execution and workflow action execution have distinct owners.
- No new crate, dependency, item alias, adapter family, macros, unsafe, or public behavior trait.

## Required validation

Targeted first: `cargo test -p tools-kit --test extension_backend --all-features -- --nocapture`; if serving changed, `cargo test -p tools-kit --test mcp_server_feature --all-features -- --nocapture`. Add new tests to existing targets where honest. Run Agent’s exact targeted tool/composition gates from its AGENTS and changed public compile tests, then `bash tools-kit/scripts/check.sh` and Agent’s complete documented wrapper. Tool wrapper includes formatting, all-target/all-feature clippy, compile/public/source tests, Local/MCP transport/schema/interaction/cancellation/lifecycle/server cases, doctests and docs-site.

Strict-live: `source tools-kit/strict-tests.env && bash tools-kit/scripts/verify_strict_live.sh`. This must use the independently configured real MCP service; do not print env secrets. Run affected Agent strict-live integrations and final five-crate wrapper. If no tool production changes were needed, do not invent broad protocol refactors solely to create pre-work.

## Sources / proof limits

Root/crate AGENTS, SKILLS, both portable APIs and change/research contracts, all user-supplied applicable Rust guidance. Tool source, current tests and scripts inspected only. `Cargo.lock:8678` pins rmcp 1.5.0. Registry rmcp 1.5.0 `src/handler/server.rs:375` exposes a Send+Sync static ServerHandler; `src/transport/streamable_http_server/tower.rs:378` accepts a Send+Sync static service factory. This supports generic Arc-backed McpServeHandler without requiring foreign T:Clone or modifying rmcp.

Verified official documents, 2025-11-25 channel, accessed 2026-09-13: [MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools), [MCP cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation). The protocol separates tool execution errors (`isError`) from protocol errors and supports cancellation signaling. Those facts motivate preserving existing mapping and cancellation semantics; generic Rust ownership is an ai-kit design decision, not a protocol mandate.
