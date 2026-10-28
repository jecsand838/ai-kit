# ai-kit Composition Authority

## Authority Map

Read these sources in order:

1. Root [`AGENTS.md`](../../../../AGENTS.md) and
   [`app-kit/AGENTS.md`](../../../../app-kit/AGENTS.md) for repository and
   crate constraints.
2. [`app-kit/specs/SPECIFICATIONS.md`](../../../../app-kit/specs/SPECIFICATIONS.md)
   plus the owning app specs for the normative app target.
   [`workflows/RESEARCH.md`](../../../../app-kit/specs/workflows/RESEARCH.md)
   records the approved fixed pin's authority for overlapping core semantics;
   App specs retain exact signatures, translations and explicit exclusions.
3. [`agents-kit/specs/API.md`](../../../../agents-kit/specs/API.md) for the sole
   exact inventory of public agents-kit root types and operations. Cite it; do
   not restate its signatures.
4. Public storage guidance in
   [`storage-kit/README.md`](../../../../storage-kit/README.md).
5. Repository implementation only as a labeled observation, never as authority
   over the selected specifications.

Resolve overlapping semantic conflicts through that approved authority order,
not by following current implementation. A remaining contradiction or unplanned
expansion is an `Unresolved gap` requiring a separate app-kit specification
amendment before production; applying an adopted pinned semantic is not a new
scope decision.

## Lower-Crate Research Map

Start at each crate's public contract, then read only the smallest relevant
implementation owner. Private details are repository observations that app-kit
cannot depend on. Route external provider, agent-backend, storage-backend, or
MCP facts through the owning crate research skill.

| Crate | Normative reading | Current-code evidence |
|---|---|---|
| `agents-kit` | `specs/API.md`, `specs/SPECIFICATIONS.md`, owning behavior specs | `src/lib.rs` and the relevant root-owned implementation |
| `providers-kit` | `AGENTS.md`, `ARCHITECTURE.md`, `docs/PORTABLE_API.md` | `src/lib.rs` and the relevant provider owner |
| `tools-kit` | `AGENTS.md`, `ARCHITECTURE.md`, `docs/PORTABLE_API.md` | `src/lib.rs` and the relevant Local or MCP owner |
| `storage-kit` | `AGENTS.md`, `ARCHITECTURE.md`, `README.md` | `src/lib.rs` and the relevant StateStore or Database owner |

## Contract, Implementation, And Proposal

Keep these layers distinct:

| Layer | Meaning | Permitted claim |
|---|---|---|
| Normative contract | Selected app specs, their adopted fixed-pin core semantics, and lower ledgers | Required design |
| Current implementation | Source and executable evidence in this worktree | Only what inspection or validation proves |
| Proposal | Unadopted change inferred from external research or analysis | Non-normative until the owning specs are amended |

Runtime execution, persistence, concurrency, recovery, and strict-live behavior
require executable evidence; specifications and static contracts do not prove
them.

## Ownership Map

| Owner | Keep here | Reject from app-kit |
|---|---|---|
| `app-kit` | Declarative app configuration, app workspaces, opaque workflow definitions, orchestration, and app control-plane records | Public workflow AST/runtime internals, agent backend logic, provider loops, tool execution, storage backends, or copied lower records |
| `agents-kit` | Agent-to-workspace-to-conversation runtime, prompts, notifications, interactions, lifecycle, tools, knowledge, and agent state | App reconstruction of protocols, private state, or operation signatures |
| `providers-kit` | Provider configuration, credentials, models, hosted declarations, and wire adaptation | App provider payloads, hosted-tool rendering, or credential duplication |
| `tools-kit` | Executable Local and MCP-client toolsets and registered handlers | Provider-hosted execution or app-owned tool envelopes |
| `storage-kit` | Static `StateStoreBackend` control-plane behavior and `DatabaseBackend` data/query behavior used by agent-owned managed knowledge | App persistence adapters, lower key inspection, erasure, or path substitution |
| Caller | Fully built `A: AgentBackend` values and one app-dedicated StateStoreBackend implementation | Hidden app defaults derived from lower values |

Compose through crate-root public contracts and functional public modules
only. Agent and storage implementations are retained through static type
parameters. Do not clone, serialize, debug-format, hash, compare, or inspect
lower values to manufacture compatibility.

Preserve these composition rules:

- `AgentSpec<A>` carries a validated logical key, caller revision, and one
  fully built `A: AgentBackend`. The revision is the sole caller-authored
  compatibility fact.
- Callers create or reopen supplied Agents through `AgentBuilder`; direct
  caller Agents use their own construction path. app-kit never stores or
  reconstructs Agent identity.
- App retains `A` under `Arc<A>` without requiring `A: Clone`, uses
  `A::Workspace` and its associated Conversation directly, and owns eventual
  consuming `A::shutdown`.
- One inventory is homogeneous in `A`. Runtime heterogeneity uses caller-owned
  matching Agent, Workspace, and Conversation enums, never Kit erasure.
- `AppConfig` requires one caller-built app-dedicated `CS: StateStoreBackend`,
  including for process-local storage. App preserves that concrete type; it
  never constructs a StateStore or provides an implicit storage default.
- app-kit has no direct providers-kit or tools-kit dependency and owns no lower
  knowledge, tool, provider, credential, or storage-configuration field.
- Treat `StateStoreBackend` and `AgentBackend` as distinct paths. Learn support
  from public operation results, never provenance.
- Preserve agents-kit's exclusive at-least-once notification path.

## Dossier Checks

For each proposed behavior record its claim, classification, normative state,
exact source, owner, portability, impact, and proof level. Confirm that app
specs define it, the agent API ledger exposes every required operation, and no
step requires private modules or opaque-value inspection. Apply the approved
semantic authority first; stop on a remaining missing or contradictory contract
or unplanned expansion and require an app specification amendment.
