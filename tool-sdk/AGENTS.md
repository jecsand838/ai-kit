# tool-sdk AGENTS

This file adds tool-specific rules to the workspace root
[`AGENTS.md`](../AGENTS.md).

## Scope

`tool-sdk` owns executable Local and MCP toolsets:

```text
Toolset -> ToolHandler
```

The public contracts are `ToolsetBackend` and `ToolHandlerBackend`. The exact
surface is owned by [`docs/PORTABLE_API.md`](docs/PORTABLE_API.md).

Provider-hosted tools, agent loops, durable factories, public conformance
suites, storage adapters, and higher-level mounting policy do not belong here.

## Dispatch And Extension Rules

- `Toolset` and `ToolHandler` are opaque public handles over private exhaustive
  Local/MCP dispatchers with exactly one erased `Extension` arm.
- Local/MCP concrete owners, each dispatcher, and each public handle implement
  the corresponding public RPITIT trait directly.
- `ToolsetBackend::Handler` is constrained by `ToolHandlerBackend`.
- `Toolset::from_backend` is the only public custom ingress. Associated custom
  handlers normalize privately; `ToolHandler::from_backend` must not exist.
- Built-in construction and handler selection create concrete Local/MCP
  variants directly.
- Keep erased adapters private and delete one-field generic extension
  wrappers, duplicate behavioral traits, public dyn routing, aliases, macros,
  and compatibility paths.
- Dispatcher matches remain explicit, exhaustive, and receiver-based.

## Toolset And Handler Ownership

- `ToolsetBackend::list_tools` is fallible async and returns
  `Vec<ToolInfo>`.
- `ToolsetBackend::tool_handler` returns the associated unique Handler.
- `ToolsetBackend::shutdown` is borrowed, shared across clones, exactly once,
  cancellation-safe, and terminal-result replaying.
- Listing and handler construction reject after shutdown sealing. Drop alone
  does not promise async cleanup.
- `ToolHandlerBackend::step` is the sole execution operation.
- A handler returns `Completed(ToolResult)` or `Interaction(...)`.
  Model-visible tool failure is a completed result with `is_error = true`.
  Outer `ToolError` is reserved for invalid state, invalid input, protocol,
  transport, cancellation, timeout, or closure failures.
- `ToolResult` does not repeat call id or tool name; the caller already owns
  invocation correlation.

## Construction And Registration

- Built-ins use `Toolset::local(Vec<ToolRegistration>)` or
  `Toolset::mcp(McpClientConfig)`.
- `ToolRegistration` combines one `ToolInfo` with one private handler owner.
  Do not maintain parallel function/interactive registration families.
- Local duplicate names always fail.
- `ToolInfo` is the complete declaration: name, description, schemas,
  annotations, icons, and interaction capabilities. Inventory is a plain
  `Vec<ToolInfo>`.
- The two legitimate registration `Arc::try_unwrap` copy-on-write paths may
  remain. They transfer registration ownership and are not lifecycle cleanup.
- `McpClientConfig` owns the connection, optional namespace, request timeout,
  and form-validation policy. There is no source id, Toolset config wrapper,
  source enum wrapper, or builder.

## MCP Client And Server Boundaries

- MCP client behavior remains behind the MCP Toolset variant: initialization,
  inventory, native-name routing, call mapping, interaction correlation, and
  cleanup.
- MCP native names stay private. Public `ToolName` is the validated rendered
  identity.
- Empty `tools/list` is valid. Form-enabled calls serialize where the protocol
  adapter lacks reliable per-call elicitation correlation.
- Server behavior is a separate non-Clone `McpServer`, never a Toolset
  sub-lifecycle.
- `McpServer::start<T: ToolsetBackend>(Arc<T>, McpServerConfig)` starts eagerly
  and retains the caller's concrete toolset and associated handler types.
  Neither type requires `Clone` or `Toolset::from_backend` normalization.
  `McpServer::shutdown(self)` consumes the server and joins its tracked owner.
  Cancellation or drop stops serving work while retaining its cleanup owner;
  it must not abort the supervisor that owns accepted cleanup. A shutdown
  timeout is an error and does not transfer or discard unfinished work.
- MCP server shutdown never shuts down the shared Toolset.
- `McpServerConfig` directly owns loopback address, endpoint path, cursor
  prefix, interaction policy, and shutdown timeout.
- Serving never appears on Toolset, client configuration, or agent
  registration hooks. Do not reintroduce restartable server actors or
  endpoint discovery.

## Module And Public-Surface Rules

Supporting API lives in functional public modules:

- `declaration`
- `execution`
- `interaction`
- `local`
- `mcp`
- `schema`

The crate root reexports only the primary handles, backend traits, and
`ToolError`. `ToolName` and all other supporting types stay in their owning
functional modules. Do not reexport `schemars`.

Private source modules own Local/MCP implementation structs, runtime state,
protocol DTOs, native-name mappings, handlers, cancellation guards, and
dispatch variants. Passive DTO fields may be public; runtime and
invariant-bearing owners keep private fields.

## Documentation Ownership

- `README.md`: downstream workflows
- `ARCHITECTURE.md`: private runtime ownership
- `AGENTS.md`: rules and canonical validation
- `docs/PORTABLE_API.md`: exact public hierarchy and signatures
- `docs/STRICT_TESTING.md`: deterministic/strict-live sequencing
- `docs/BEHAVIOR_PROOF_POLICY.md`: honest Local/MCP behavior

Update owning docs and source contracts with public, lifecycle, module, or
validation changes.

## Required Validation

Run these completion gates from the workspace root after the change is
assembled. During implementation, preserve the tests-first sequence: focused
red evidence before production edits, passing controls, and independent review.

```bash
bash tool-sdk/scripts/check.sh
source tool-sdk/strict-tests.env && bash tool-sdk/scripts/verify_strict_live.sh
```

The crate wrapper covers all targets/features, compile contracts,
deterministic Local/MCP behavior, independent MCP server behavior, source
contracts, clippy with warnings denied, rustdoc/doctests, and docs-site.
Strict-live alone proves a real independently configured MCP service.

## Primary mock CI

The direct-trait targets are required by `bash scripts/check_ci.sh` and can be
scoped with `bash scripts/check_mock_contracts.sh --crate tool-sdk`. Follow
[the mock test contract](../docs/mock-testing.md). Keep these credential-free
targets separate from existing integration and strict-live verification; the
existing gates above remain required. Production regressions stay enabled and
are recorded in [the bug ledger](../docs/mock-testing-production-bugs.md).
