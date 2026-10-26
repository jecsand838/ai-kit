# Add caller actions and transient inputs

Use an `ActionBackend` when a node must run application-owned behavior. Construct
the action before App open and supply it through `AppConfig::with_actions`.
App keeps the concrete action and its associated stream type; it does not
require `Clone`, a supplied Agent, or an erased action adapter.

An action node selects that binding by logical key and exact revision:

```json
{
  "id": "calculate",
  "step": {
    "kind": "action",
    "action": "calculate_total",
    "revision": "r3",
    "project_result": false
  }
}
```

This is a node fragment for a complete workflow document. Binding validation
at App open checks its declaration, revision, advertised mode, and required
composition. Each invocation checks that the binding supports its requested
mode before constructing the source. A revision identifies stable caller behavior; changing code or
configuration incompatibly requires a new revision. Keep revisions needed by
readable history available when reopening the App.

## Declare the binding once

`ActionDeclaration` records the key/revision, ordered state reads and writes,
required and optional transient input names, fixed bindings, defaults, tags,
source metadata, and execution mode. Return a reference to this stable value
from the action's `declaration()` method. The declaration is metadata and an
input contract; App does not hide undeclared state from the action or prohibit
explicit writes merely because they were not listed.

`start(ActionRequest)` constructs one lazy stream. Defer external work until
the stream is polled. The request contains the full immutable base state,
resolved transient inputs, and an `ActionContext` with the run and its exact
occurrence, node, attempted ordinal, execution mode, and committed base
checkpoint. Child actions also receive the parent run and its exact occurrence;
both parent fields are absent for root runs. Context contains no
lower backend or App runtime handle. Siblings in a wave receive the same base
state even if another sibling finishes first.

An action declaration's mode is `Synchronous`, `Asynchronous`, or `Both`.
The actual invocation context selects one concrete mode. A source can be local
to its driving task; `ActionBackend::Stream` does not require `Send`. Keep local
state in that source rather than erasing it or spawning detached work to meet
an unnecessary bound. See the [public ledger](../specs/SPECIFICATIONS.md) for
the execution owners that support each mode.

## Keep inputs separate from workflow state

Initial workflow state comes from the definition and run admission. Execution
inputs come from `ExecutionOptions.inputs`. Adding an execution input does not
write checkpoint zero or any later checkpoint. One execution carries its
runtime inputs through successive actions.

| Input rule | Result |
|---|---|
| Missing optional input with a default | The default supplies it. |
| Present runtime input, including JSON null | It is present; null is not missing. |
| Required input missing after resolution | Request the missing values through `Inputs` before constructing the action source. |
| Fixed bound input | It may satisfy a required input. |
| Runtime value for a fixed bound name | Reject even when the two values are equal. |
| Default for a required input | Reject the declaration; required inputs cannot be silently defaulted. |
| Extra ordinary runtime input | Preserve it in the action request. |
| Reserved `__*` runtime input name | Reject before action or hook dispatch. |

Input names are data rather than workflow node identities. Do not normalize
punctuation, sort away repeated declaration entries, or remove extra runtime
keys. The ordinary input count, key, nesting, and byte limits still apply.

Child, Map, and Pipeline declarations use `input` to build child workflow state
and optional `inputs` to bind the child's transient action inputs. Both are maps
of expressions evaluated once against the parent's committed state, before the
child control is published. A Map copies the resolved transient map to each
child; a Pipeline retains the original resolved map for each later child.
For example, `"inputs":{"question":{"kind":"state","path":"text"}}`
binds the parent state's `text` value without copying it into child state.

The complete encoded transient map vector has its own 1 MiB limit, checked
incrementally during expansion. The durable child state-input vector has a
separate 1 MiB limit. Exceeding either limit rejects the whole expansion before
any child is registered. Individual input key, count, depth, and value limits
also apply.

Dropping a pending `next()` waiter leaves the original publication and its
resolved maps owned by the execution. If recovery has lost the original
in-memory input owner, it does not reevaluate the original expressions or
reconstruct those maps from checkpoints. A child missing required inputs
exposes an `Inputs` interaction for explicit rebinding. Resolved transient maps never enter runtime
records or history; authored expressions remain part of the stored definition.

## Emit results and explicit state changes

The source yields `Result<ActionEvent, AppError>`. A `Partial` event contains
transient result fields. It neither writes state nor advances the committed
action position. The first `Completed` event supplies a final result map and
ordered `StateChange` operations. App stops polling at that terminal event;
later source items cannot add another commit.

For example, a completed calculation can retain diagnostic output while writing
only its numeric total:

```rust
use app_sdk::workflow::{ActionEvent, StateChange};
use serde_json::{Map, json};

let completed = ActionEvent::Completed {
    result: Map::from_iter([
        ("total".into(), json!(42)),
        ("explanation".into(), json!("Six groups of seven")),
    ]),
    writes: vec![StateChange::Assign {
        values: Map::from_iter([("total".into(), json!(42))]),
    }],
};
```

The result remains distinct from state and is retained in the committed action
outcome. Only the explicit assignment writes `total`; `explanation` is not
automatically copied. State changes are validated in order against one
temporary state and then reduced with sibling contributions at the barrier.
A later write cannot repair an earlier invalid operation. No sibling sees a
partially applied sequence.

With `project_result: true`, App first selects present top-level result fields
from the declaration's ordered `writes` list and constructs one assignment.
Missing result keys are skipped, null is copied, and repeated keys retain their
first position. That assignment precedes all explicit completed writes, so an
explicit later write can replace a valid projected value. Projection counts
toward the same 4,096-operation ceiling even when it selects no fields. Its
values must satisfy state and typing rules; a valid transient result can still
be invalid workflow state.

## Own effects and failure handling

An explicit source error is an action failure. A contained construction or poll
panic is an execution failure, and an App activation deadline is a timeout
failure. EOF without `Completed` ends observation without a completed action;
it does not manufacture a result or state write. Owning execution Drop and
explicit abort discard uncommitted caller-source contributions under their
publication rules. See [errors](ERRORS_AND_TROUBLESHOOTING.md) for the distinction
between an operation error and a retained run failure.

Workflow-state rollback does not undo external I/O performed by your action.
Your source owns its cancellation, any tasks it starts, idempotency keys, and
reconciliation with external services. App cannot infer an unknown external
effect merely from having polled arbitrary caller code. A later explicit
execution can repeat that action, so design external effects accordingly.
App-owned Agent attempts and admitted StateStore mutations have separate,
explicit unknown-effect protocols.

Lifecycle hooks observe actual attempts, outputs and completion at the caller's
chosen content boundary. Source span/attribute events use the same observation
path; they do not create another durable workflow state. Default metadata
tracing does not export the action's inputs or result content.

The deterministic [action tests](../tests/action_contract.rs),
[input and binding tests](../tests/action_bindings.rs), and
[projection tests](../tests/projection_contract.rs) exercise these contracts
through public APIs. They prove App behavior with caller actions; they do not
prove an external service's own transactional or cancellation guarantees.
