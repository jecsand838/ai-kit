# Supply your own Agent and StateStore

App accepts already-built values through `AgentBackend` and
`StateStoreBackend`. Your implementations keep their concrete types throughout
App → Workspace → Workflow → WorkflowRun. Neither backend needs `Clone`, and
neither needs to use the supplied Agent or StateStore implementation.

This generic construction owner accepts the same values a service would build
at its executable boundary:

```rust
use agent_sdk::AgentBackend;
use app_sdk::{AgentSpec, App, AppConfig, AppError};
use app_sdk::workflow::WorkflowDefinition;
use storage_sdk::StateStoreBackend;

pub struct Deployment;

impl Deployment {
    pub async fn open<CS: StateStoreBackend, A: AgentBackend>(
        state: CS,
        agents: Vec<AgentSpec<A>>,
        definitions: Vec<WorkflowDefinition>,
    ) -> Result<App<CS, A>, AppError> {
        App::open(AppConfig::new(state, agents, definitions)).await
    }
}
```

Construct each `AgentSpec` with a logical key, your configuration revision and
the ready Agent value. A workflow node uses that logical key. App does not use
an Agent's opaque ID to select or compare implementations. The
[compile consumers](../tests/compile_contract.rs) exercise this unconstrained
composition, and the [direct caller fixture](../tests/workspace_support/mod.rs)
implements the complete lower hierarchy without normalizing it into a supplied
handle.

The [mock Agent hierarchy](../tests/mock_agents/mod.rs) provides test-local
struct and exhaustive enum implementations with their exact associated children.
The [real App composition tests](../tests/workflow_mock_composition.rs) use those
values with caller-owned process-local transactions. These are executable testing
examples, not a public mock API or evidence of native backend durability.

## Keep associated children concrete

One App has one Agent type `A`. Two values of that type can have different
configuration. To combine different implementation types, define three matching
exhaustive enums in your application:

| Caller owner | Implements | Associated child |
|---|---|---|
| `ApplicationAgent` | `AgentBackend` | `ApplicationWorkspace` |
| `ApplicationWorkspace` | `WorkspaceBackend` | `ApplicationConversation` |
| `ApplicationConversation` | `ConversationBackend` | None |

Give each enum one variant per implementation family. Each Agent method matches
its own variant, calls the corresponding public backend method, and wraps a
returned Workspace in the matching Workspace variant. Workspace methods do the
same for Conversation. Conversation methods preserve each implementation's
prompt, notification, abort and close behavior. Consuming shutdown matches and
moves the actual Agent value. Write every arm explicitly; the enums do not need
`Clone` or runtime downcasts.

Implement the complete contracts from the
[Agent API ledger](../../agent-sdk/specs/API.md). A partial forwarding example
would omit required lifecycle behavior. The StateStore seam similarly retains
its concrete `CS::Transaction`; return your owned transaction implementation
through `StateTransactionBackend`, including its commit, rollback and Drop
semantics. App does not supply a backend reconstruction factory or an erased
Agent/Storage ingress.

## Construct lower products below Agent

The application supplies App control-plane storage separately from Agent's own
storage. Reopening both products requires both stores and the required Agent
configuration. App checkpoints contain public lower Workspace/Conversation IDs
and application bindings, not credentials or serialized backend handles.

For a Custom Agent, caller-selected Provider, Agent storage, optional Database
and ready Toolsets belong to Agent construction. Provider owns model/session
calls; Toolsets own executable tools; Agent owns conversation behavior and its
prompt loop. Native Codex/OpenCode configuration also stays in agent-sdk. App
neither acquires a Provider nor reconstructs tools or managed knowledge from a
workflow document. See the [Agent contract](../../agent-sdk/specs/API.md) and
[workspace architecture](../../ARCHITECTURE.md) for those ownership boundaries.

App's `with_actions`, `with_conditions`, `with_state_typing`, `with_hooks` and
`with_child_executor` configure separate App behavior seams before open. They do not
replace the lower Agent hierarchy. Their exact methods belong to the
[App public ledger](../specs/SPECIFICATIONS.md).

## Transfer ownership and finish cleanup

After successful open, App owns consuming shutdown of every transferred Agent.
Keep App's async runtime driven and await an explicit shutdown policy. `Drain`
finishes accepted work; `CancelAndDrain` requests cancellation and drains;
`RejectActive` rejects closure while accepted work remains. A timeout ends your
observation, so a later shutdown call can reattach to retained finalization.
The [quickstart](QUICKSTART.md) links a complete executable with cleanup before
operation errors are propagated.

An ordinary failed open after Agent transfer awaits shutdown of the transferred
Agents and preserves the primary failure. This includes ordinary configuration
validation errors returned by open. Dropping a configuration or cancelling an in-progress open
future does not promise asynchronous cleanup of ready lower values. The caller
must retain the configuration needed to reopen durable lower products and
reconcile them; App does not return a replacement Agent inventory in an error.

Dropping an ordinary WorkflowRun observer does not cancel execution. An owning
Execution has its own revocation and accepted-publication rules. Neither kind
of Drop is a substitute for deliberate service shutdown. The
[application ownership contract](../specs/APP.md) specifies the exact transfer,
failed-open and finalization boundaries.
