# App guides

Start with [the runnable quickstart](QUICKSTART.md), then read the guide for
the behavior you are adding:

- [Workflow authoring and graph inspection](WORKFLOW_LANGUAGE.md)
- [Caller Agent and StateStore integration](TRAIT_INTEGRATION.md)
- [Multi-agent workflows and revision deployment](MULTI_AGENT_WORKFLOWS.md)
- [Caller actions, inputs and results](ACTIONS_AND_INPUTS.md)
- [Step, iterate, halt, resume and synchronous execution](EXECUTION_CONTROLS.md)
- [Partial streams, terminal results and output ownership](STREAMING.md)
- [Input, approvals, independent targets and cancellation](INTERACTIONS_AND_CONTROLS.md)
- [State, typing, serialization and parallel reduction](STATE_AND_PARALLELISM.md)
- [Persistence, retained history and recovery](PERSISTENCE_AND_RECOVERY.md)
- [Errors and troubleshooting](ERRORS_AND_TROUBLESHOOTING.md)
- [Lifecycle, workspaces, shutdown and operations](LIFECYCLE_AND_OPERATIONS.md)
- [Safe tracing, lifecycle hooks and tracking exports](OBSERVABILITY.md)
- [Deterministic checks, real integrations and release proof](STRICT_TESTING.md)
- [Pinned rurr compatibility, translations and exclusions](RURR_PARITY.md)

The public hierarchy is App → Workspace → Workflow → WorkflowRun. App consumes
caller-built Agent and StateStore implementations through their public traits.
Agent owns its associated Workspace and Conversation types; providers, tools
and managed knowledge remain below that boundary. Workflow actions, conditions,
state typing, lifecycle hooks and child execution are App-owned behavior seams.

Executable examples:

- [Native state workflow without an Agent service](../examples/pure_state.rs)
- [Caller action inputs, partial output and explicit writes](../examples/actions_and_inputs.rs)
- [Partial, committed and halted events followed by resume](../examples/controls_and_streaming.rs)
- [Two configured Agents and a structured handoff](../examples/multi_agent.rs)
- [Resolved child tasks, a concrete executor and synchronous Pipeline](../examples/child_composition.rs)
- [Immutable and typed state](../examples/state.rs)
- [Graph composition and export](../examples/graph.rs)
- [Restore, source deletion and App reopen](../examples/history_and_recovery.rs)
- [Long performance consumer: execution, history, cancellation and delayed observers](../examples/performance.rs),
  run explicitly through [the release workload commands](STRICT_TESTING.md#run-explicit-release-workloads).

The JSON corpus provides complete documents. Pass every named child definition
to the same App configuration as its parent. Caller Agents and actions must
already be constructed before opening that App.

| Document | Purpose and required bindings |
|---|---|
| [Linear state](examples/linear_state.json) | Two native state updates. |
| [Conditional loop](examples/expressions_loop.json) | Routing, integer progression and rurr division/remainder. |
| [Input](examples/input.json) | A schema-checked answer with an explicit state target. |
| [Sequential Agents](examples/multi_agent_sequential.json) | Structured `author` output handed to `reviewer`. |
| [Parallel Agents](examples/multi_agent_parallel.json) | `author` and `reviewer` read one base and join independent writes. |
| [Invoice controls](examples/invoice_controls.json) | The shared invoice action, transient quantity inputs and resumed execution. |
| [Child](examples/child.json) | Reduce one [counter child's](examples/child_counter.json) final value. |
| [Map](examples/map.json) | Use that same counter for every item with `max_parallel: 2`; an empty `items` override uses the same graph. |
| [Join](examples/joins.json) | Broadcast native writes and collect them in canonical order. |
| [Retry](examples/retry.json) | A `reviewer` node with one bounded fixed retry policy and deadline. |
| [Sleep](examples/sleep.json) | A timer followed by a state update. |
| [Tasks](examples/tasks.json) | Equal logical task keys with independent occurrences and ordered reduction. |
| [Pipeline](examples/pipeline.json) | Synchronous counter-child composition with state handoff; runnable in the child composition executable. |

Typed state and field/tag codecs use the concrete policy in
[state.rs](../examples/state.rs); they do not require a separate JSON grammar.
The focused [documentation tests](../tests/docs_examples.rs) distinguish
parsing from execution. The
[2026-09-13 execution log](../plans/2026-09-13/EXECUTION_LOG.md) preserves
historical commands, outcomes and proof limits; it is not the current branch's
validation receipt.

The action and controls executables share the same
[invoice action](../examples/invoice/mod.rs). The controls example uses the
[two-step invoice definition](examples/invoice_controls.json); it checks that a
partial leaves total at 0, the first commit and halt retain 42, and resumed
execution uses new transient inputs to commit 49.

[The public ledger](../specs/SPECIFICATIONS.md) owns exact signatures.
[Private architecture](../ARCHITECTURE.md) describes runtime ownership, while
[testing requirements](../specs/workflows/TESTING.md) distinguish deterministic,
real-service, crash and performance evidence.
