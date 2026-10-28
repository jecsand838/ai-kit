# Pinned rurr core compatibility

App adopts the observable core semantics of the vendored rurr snapshot. Where
App and that snapshot disagree, the pinned rurr behavior is the compatibility
target. App uses its own Rust API and transactional records; matching a feature
name without matching its values, errors, ordering or ownership is insufficient.

The immutable source is `app-kit/vendor/rurr-main.zip`, commit
`005bfe3ece631e64b06dac2d42ef84235f727a4f`, with SHA-256
`5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e`.
Its README targets Burr v0.41.0. The
[research ledger](../specs/workflows/RESEARCH.md) records the exact source
families and authority rules. A newer Burr release does not silently change
this baseline.

This page maps compatibility scope to public behavior and test ownership.
“Core” and “translation” below classify required behavior, not a claim that
every case currently passes. Release completion requires test-first evidence,
independent code review, the full integrations/docs/feature gates, and the
prescribed crash/performance and clean-checkout proof. The
[2026-09-13 execution log](../plans/2026-09-13/EXECUTION_LOG.md) preserves
historical results and failures within its recorded source and scope; it does
not establish a current-branch gate result. A source file or guide alone never
closes a row.

## All 31 audit areas

Pinned member paths below are relative to `rurr-main/crates/` unless prefixed
with `docs/` or `goldens/`. Test links identify the owning proof; they do not
mean an unrun case has passed.

| ID | Area and classification | Observable contract and owning proof | Pinned source |
|---|---|---|---|
| A01 | Application composition — core with App extensions | App → Workspace → Workflow → Run, isolated inventories and reopen, direct caller traits and associated children. [Compile contracts](../tests/compile_contract.rs), [workspace behavior](../tests/workspace_contract.rs). | `rurr-core/src/application/mod.rs` |
| A02 | Multiple Agents in one workflow — core requirement | Distinct Agents execute distinct nodes; state handoff, parallel entry, revision recovery and shutdown ownership are exercised. Two configured Agents alone prove nothing. [Real workflows](../tests/workflow_live.rs), [revision/workspace tests](../tests/workspace_contract.rs). | `rurr-core/src/action/api.rs`, `action/function.rs` |
| A03 | Caller actions — core | Ordinary Rust computation/I/O through concrete Action bindings, declarations and lazy sources, with no pretend Agent or App Tool dependency. [Actions](../tests/action_contract.rs), [bindings](../tests/action_bindings.rs), [compile contracts](../tests/compile_contract.rs). | `rurr-core/src/action/{api,function}.rs` |
| A04 | Results separate from writes — core | Preserve exact transient/final results, explicit ordered state changes and terminal native output; no implicit result-to-state insertion. [Action tests](../tests/action_contract.rs), [projection](../tests/projection_contract.rs), [native results](../tests/native_execution_contract.rs). | `rurr-core/src/action/api.rs` |
| A05 | Runtime inputs and binding — core | Required/optional/default/fixed input semantics, present null, extra ordinary keys, reserved-key rejection and fresh binding after loss of transient inputs. [Bindings](../tests/action_bindings.rs), [history](../tests/checkpoint_history.rs). | `rurr-core/src/action/function.rs`, `application/{session,execution_options}.rs` |
| A06 | Graph authoring/composition — syntax translation | Reuse validated graphs, remap node references, preserve routes/metadata and reject collisions, dangling edges and invalid relationships. [Graph contracts](../tests/graph_contract.rs), [schema](../tests/schema_contract.rs). | `rurr-core/src/graph.rs` |
| A07 | Inspection, export and tags — core | Owned graph metadata and deterministic JSON/DOT/Mermaid text; safe label escaping and actual tag-driven controls. Rendering/hosting stays outside App. [Graph tests](../tests/graph_contract.rs), [halt controls](../tests/execution_controls.rs). | `rurr-core/src/graph.rs` |
| A08 | Ordered transitions — core with BSP extension | First matching/default/no-match behavior, short circuit, and outgoing-route errors after successful publication; canonical App broadcast. [Conditions](../tests/condition_contract.rs), [outcome recovery](../tests/outcome_recovery_contract.rs). | `rurr-core/src/graph.rs`, `action/condition.rs` |
| A09 | Expression breadth — core | Arithmetic, indexing/access, slices, unary/boolean/conditional expressions and valid/invalid operands at their actual evaluation boundaries. [Expression parity](../tests/expression_parity.rs). | `rurr-expr/src/{parse,validate}.rs`, `eval/` |
| A10 | Numeric semantics — core | True division, signed remainder/floor division, promotion, exact mixed comparisons, overflow and finite-result rules. [Expression parity](../tests/expression_parity.rs). | `rurr-expr/src/eval/numeric.rs` |
| A11 | Comparisons and invalid operands — core | Missing versus null, supported orderings, equality, membership errors, chained comparison and short circuit. [Expression parity](../tests/expression_parity.rs), [conditions](../tests/condition_contract.rs). | `rurr-expr/src/eval/{compare,numeric}.rs` |
| A12 | State transformations — core with reducers as App extension | Immutable ordered update/append/extend/increment/remove/wipe/merge/subset; separate canonical sibling reduction and atomic failure. [State](../tests/state_contract.rs), [workflow execution](../tests/workflow_execution.rs). | `rurr-core/src/state.rs`, `state/{operations,value_ops}.rs` |
| A13 | Typed state and codecs — core translation | Caller StateTyping, local typed values, process-global field/tag registration and replacement, immutable codec snapshots, durable roundtrip and errors. [Typed state](../tests/typed_state_contract.rs), [serialization](../tests/state_serialization_contract.rs), [typing integration](../tests/typing_integration.rs). | `rurr-core/src/state/typing.rs`, `state/serde_registry.rs`, `state.rs` |
| A14 | Step and iteration — core | One exclusive advancing owner, lazy next/step/finish, exact boundaries/results and native action ordinals independent of control checkpoints. [Execution controls](../tests/execution_controls.rs), [native execution](../tests/native_execution_contract.rs). | `rurr-core/src/application/api.rs` |
| A15 | Halt before/after/tags — core with BSP extension | First-current-session bypass, method-specific precedence, pending-node/no-result versus completed-node/result, retained decisions and complete-wave stopping. [Halt controls](../tests/execution_controls.rs), [BSP](../tests/bsp_execution_contract.rs). | `rurr-core/src/application/{execution_options,halt_controls,session}.rs` |
| A16 | Transactional partial streaming — core | Real lazy partials, terminal publication, EOF/error/Drop semantics, bounded native output and responsive controls. [Caller actions](../tests/action_contract.rs), [native streaming](../tests/native_streaming_contract.rs), [live workflows](../tests/workflow_live.rs). | `rurr-core/src/application/streaming/`, `action/api.rs` |
| A17 | Sync/async/local execution — surface translation | Synchronous controls outside Tokio tasks, asynchronous controls, local non-Send sources/Data, still-driven runtimes and joined worker ownership. [Compile contracts](../tests/compile_contract.rs), [worker behavior](../tests/worker_contract.rs). | `rurr-core/src/action/api.rs`, `docs/compatibility/DIVERGENCES.md` |
| A18 | Checkpoint commit/resume-next — core with stronger App durability | Initial and action checkpoints, exact transactional receipts/fences, correct next work and no replay after uncertain acceptance. [History](../tests/checkpoint_history.rs), [recovery](../tests/outcome_recovery_contract.rs), [workflow faults](../tests/workflow_execution.rs). | `rurr-core/src/application/{step_txn,transaction,state_meta}.rs`, `rurr-persistence/src/traits.rs` |
| A19 | Historical selection/fork/rewind/override — core | Exact sequence/action selection, source immutability, copied clean state, lineage, saved position, override validation and deletion independence. [History](../tests/checkpoint_history.rs), [revisions](../tests/revision_contract.rs). | `rurr-core/src/application/initialize_from_spec.rs`, `rurr-persistence/src/spec.rs` |
| A20 | Retained history/debugging — core | Immutable ordered history with cutoff pagination, delayed readers and results retained until explicit deletion. Tracing is not history. [History](../tests/checkpoint_history.rs), [deletion](../tests/deletion_contract.rs). | `rurr-core/src/tracking/mod.rs`, `rurr-tracking-store/src/tracked_log.rs` |
| A21 | Child workflows/map/reduce — core with App extensions | Empty/bounded/nested children, original-input reduction order, complete descendant accounting, capacity and cancellation/recovery. [Child executor](../tests/child_executor_contract.rs), [workflow execution](../tests/workflow_execution.rs). | `rurr-core/src/parallelism/{types,parallel_map,map_reduce,runtime}.rs` |
| A22 | Child selection/executors/pipelines — core translation | Dynamic declared selection, task identity/reuse, initialization/history policies, custom local executors and synchronous sequential pipelines with explicit handoff. [Child contracts](../tests/child_executor_contract.rs), [schema](../tests/schema_contract.rs). | `rurr-core/src/parallelism/{types,executor,task_action}.rs` |
| A23 | Persistence implementations — trait-owned integration | App history uses direct StateStore/owned transaction traits. Adapter products remain storage-kit or caller responsibilities. [App compile contracts](../tests/compile_contract.rs), [storage contracts](../../storage-kit/tests/state_store_backend.rs), [storage live tests](../../storage-kit/tests/strict_live.rs). | `rurr-persistence/src/lib.rs`, `rurr-{sqlite,postgres,redis,mongodb}/src/lib.rs` |
| A24 | Lifecycle/content hooks — core | Exact sync/async order, pre/item/post failure semantics, attempted versus committed state, child linkage and synchronous-only Drop. [Hook execution](../tests/hook_execution_contract.rs), [lifecycle contracts](../tests/lifecycle_contract.rs). | `rurr-core/src/lifecycle/{adapter,context,guard,default}.rs` |
| A25 | Tracing bridge/telemetry — core metadata; exporter product excluded | Guaranteed safe spans/committed events and host-owned subscriber/export/flush. No automatic telemetry service or exporter dependency. [Observation contract](../specs/OBSERVABILITY.md), [public boundary](../tests/public_surface.rs), [runtime subscriber tests](../tests/observability_contract.rs). Captured runtime-subscriber proof remains distinct from static checks. | `rurr-otel/src/lib.rs`, `rurr-telemetry/src/lib.rs` |
| A26 | Tracking content and artifacts — core models; transport integration excluded | Pinned graph/metadata/step/span/stream/fork/spawn objects and caller JSONL writer. Local/S3 layout/transport stays outside App. [Tracking](../tests/tracking_contract.rs), [checkpoint tracking](../tests/tracking_checkpoint_contract.rs), [writer guide](OBSERVABILITY.md). | `rurr-core/src/tracking/models/`, `tracking/lifecycle_events.rs`, `goldens/schema/tracking.log_entry.schema.json` |
| A27 | Tracking server/UI/annotations/indexing — user-excluded product | No server routes, UI assets, annotation API or service indexer. Future app-server work is separate; core checkpoint snapshots remain required. [Public boundary](../tests/public_surface.rs). | `rurr-tracking-server/` |
| A28 | CLI — user-excluded product | No app-cli product or serve command. Thin executable examples remain library consumers. [Public boundary](../tests/public_surface.rs). | `rurr-cli/src/commands.rs` |
| A29 | Macros/facades/utilities — Rust surface translation | Use explicit structs, enums, methods and static traits. Preserve the underlying binding/local/sync use cases without copying aliases, macros or generated dispatch. [Compile](../tests/compile_contract.rs), [surface](../tests/public_surface.rs), [examples](../tests/docs_examples.rs). | `rurr-macros/src/lib.rs`, `rurr/src/lib.rs`, `rurr-core/src/common/async_utils.rs` |
| A30 | Oracle fixtures/goldens — required proof | Source-derived expectations retain archive/member provenance, numeric types, ordering and error boundaries. Optional installed-oracle execution is labeled separately. [Expression fixtures](../tests/expression_parity.rs), [graph fixtures](../tests/graph_contract.rs), [tracking goldens](../tests/tracking_contract.rs). | `docs/RURR_TEST_MATRIX.md`, `docs/03_test_plan.md`, `goldens/schema/` |
| A31 | Documentation/examples — required deliverable | Complete public guides/schema, executable examples, meaningful rustdoc and accurate proof limits. [Documentation tests](../tests/docs_examples.rs), [App gate](../scripts/check.sh). | `docs/00_overview.md` and the pinned compatibility/operator/testing guides |

## Semantic decisions that must remain visible

`5 / 2` produces `2.5`; `-5 % 2` produces `1`. Rust integer division or remainder
is not a substitute. State preserves insertion order. Missing input/state and
explicit null remain different. Default/bound/runtime input precedence follows
the action contract, and transient inputs are not automatically checkpointed.

Ordinary and streaming halt precedence differ. Both execute their first
current-session action before consulting historical halt position. Partial
output, failed attempts and no-output EOF do not consume a committed action
ordinal. A hook failure after a commit cannot cause that action to replay.

Process-global codecs, local futures, synchronous controls, historical restore,
custom child execution and core tracking objects remain in scope. Their names
can change to fit the tight public Rust surface; their behavior cannot disappear
under a “translation” label.

## App extensions and finite restrictions

App adds explicit workspaces, Agent selection, BSP waves and sibling reducers,
durable cancellation, target-specific interaction settlement, acceptance-receipt
reconciliation and truthful Unknown effects. These stronger guarantees must have
their own tests and cannot change mapped serial behavior.

App has finite graph/state/record/frontier/queue/history/observation bounds where
the pinned implementation may use unbounded collections. The owning contracts
state their units, rejection behavior and retained-effect responsibility.
For example, the 4,096-operation state-change ceiling counts repeated entries
and enabled result projection. The lifecycle observation ceiling is an App
restriction with cooperative synchronous callbacks. Finite bounds are explicit
translations, not measured performance claims; see
[operations](LIFECYCLE_AND_OPERATIONS.md) and [testing](STRICT_TESTING.md).

The core tracking translation intentionally emits no invented child-completion
wire record: the pin's actual checked schema and emission paths contain fork
and spawn-start relationships. ChildFinished still exists as a lifecycle event.
Source/schema evidence takes precedence over an overly broad prose summary.

## Preserve integration ownership

App consumes ready Agent and StateStore values through their public traits and
portable DTOs. Provider APIs belong to providers-kit; Agent implementations own
agent loops and associated workspaces/conversations; tools-kit owns executable
tools; storage-kit owns state stores and database behavior. Managed knowledge
stays below the Agent/Storage boundary. End users can implement those same
traits without using the supplied handles.

Server/UI, CLI, exporter transports and an observability-sdk remain separate
future products. Excluding them does not exclude core graph inspection, hooks,
tracking projection or retained history. Do not introduce a wrapper crate,
default-backend requirement or native payload escape to avoid integration work.

The complete acceptance details are in the
[dated acceptance matrix](../plans/2026-09-13/ACCEPTANCE_MATRIX.md); current
signatures belong only to the [public specification](../specs/SPECIFICATIONS.md).
[Strict testing](STRICT_TESTING.md) explains why compile, deterministic,
source-derived, real-service, crash and performance results must be reported
separately.
