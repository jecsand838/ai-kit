# Agent Build And Configuration

This document owns managed runtime construction. Exact signatures and public
types live only in [`API.md`](API.md).

## Construction

New and reopened runtimes are distinct constructors:

```text
AgentBuilder::create(AgentConfig, AgentStorage)
AgentBuilder::reopen(AgentConfig, AgentStorage, AgentId)
AgentBuilder::create_for_backends::<P, T>(AgentConfig, AgentStorage)
AgentBuilder::reopen_for_backends::<P, T>(AgentConfig, AgentStorage, AgentId)
```

The builder always receives storage up front. It has no optional
`with_agent_storage`, `with_agent_id`, or alternate constructor state.
The `for_backends` forms fix the tool type and select the static provider type of native Codex and
OpenCode Agents without accepting or retaining a provider value. The ordinary
forms select the supplied `providers_kit::Provider` default. Custom receives its
required provider value only through `with_provider`; a type selection alone
does not make a Custom build valid.

Provider and tool integrations enter through their owning public contracts:

- Custom requires one direct `P: providers_kit::ProviderBackend`;
- executable sources enter through repeated `T: tools_kit::ToolsetBackend` values;
- managed knowledge uses one optional `D: storage_kit::DatabaseBackend`.

`AgentStorage<S>` is built directly from
`S: storage_kit::StateStoreBackend`. The builder and produced
`Agent<S, D, P, T>` hierarchy preserve `S`, the selected `D`, `P`, and `T` statically,
with supplied Provider and storage-kit handles as defaults. Provider children
remain `P::Model`, its associated `Continuation`, and the Model-associated
Session. Tool children remain `T::Handler`; neither root nor child requires
Clone or supplied-handle normalization. Tool ingress appends to a fixed
homogeneous inventory, and Provider/Database changes preserve that inventory.
Callers represent heterogeneous tools using their own exhaustive enums.
agents-kit accepts no
factory request, declaration payload, registry, credentials, or
provider/tool/storage config. Reopen callers resupply the same logical
integrations; durable state stores only safe compatibility facts.

## AgentConfig

`AgentConfig` owns backend selection, optional display name and instructions,
tracing policy, a nonzero lifecycle timeout, optional nonzero default prompt
timeout, and a nonzero Agent-wide prompt limit. Fields are private.

`BackendConfig` is the closed built-in selector
`Codex(CodexConfig) | OpenCode(OpenCodeConfig) |
Custom(CustomAgentConfig)`. It is construction data, not runtime identity,
capability, provenance, or error labeling.

Codex and OpenCode configuration retain their selected spawn/attach data,
timeouts, version validation, privacy requirements, and backend-host path
semantics. They do not expose protocol clients or lower provider handles.

## CustomAgentConfig

Custom configuration contains exactly:

- required default `ModelSelector`;
- existing `ToolSelection`;
- `CustomLoopLimits`;
- `CustomToolApproval`.

`CustomLoopLimits` owns bounded model-turn, total tool-action, and parallel
tool-action limits. `CustomToolApproval` is an agent-owned allow/deny/durable
interaction policy. Shell execution uses a caller-built ordinary tool. That
tool owns explicit cwd/input validation, process cancellation and cleanup,
timeouts, and output limits. Agent does not construct shells or inject cwd
through task-local state.

Custom configuration contains no credentials, Provider/Model/Session,
Toolset, Database, callback, hook, LoopDriver, route, runtime owner, or task.

## Custom Build

Before publishing an Agent, Custom:

1. validates Provider presence and resolves the configured default model
   through `P::Model`;
2. validates complete Toolset catalogs, unique effective tool names, and each
   function schema against providers-kit's object-schema contract;
3. validates approval and loop limits;
4. prepares private managed knowledge when a Database is supplied;
5. commits durable Agent identity and safe compatibility facts;
6. publishes runtime only after every prepared owner is coherent.

Build failure cleans prepared resources in reverse ownership order within the
lifecycle deadline. It retains the first public failure and records later
cleanup obligations privately.

Codex/OpenCode reject only the smallest genuinely unsupported supplied
integration. They retain backend-native tool ownership and do not accept a
Provider value intended for Custom. Their static provider parameter may still
be selected so a caller-owned Provider/Model/Session enum can be shared with
Custom Agents in one downstream inventory.

## Tool Ownership

Shared build code validates only source-neutral tool information. Each backend
owns native installation, binding, inventory, execution adapters, interaction
mapping, and teardown.

Custom executes Toolsets through `Toolset -> ToolHandler` and translates
normalized provider function calls into handler input. Tool results reenter
the selected Model through providers-kit normalized `FunctionResult`. agents-kit
does not inspect private Toolset source variants or provider wire envelopes.

## Knowledge Ownership

Supplying a Database enables the private managed-knowledge owner described in
[`KNOWLEDGE.md`](KNOWLEDGE.md). Callers ingest sources through the built Agent
or Workspace only after its final durable identity exists. A resupplied
Database may retrieve previously published compatible records.

There is no public KnowledgeBase implementation, trait, factory, conformance
owner, or custom semantic backend.

## Validation

Builder tests cover create/reopen state, missing/extra integration, model and
tool compatibility, cleanup order, redaction, direct static provider/storage
ownership, and the absence of provider/storage/tool/handler `Clone` requirements. Source
contracts reject all deleted optional-state builder methods, provider/storage
erasure and normalization, factories, hosted declarations, and LoopDriver
configuration.
