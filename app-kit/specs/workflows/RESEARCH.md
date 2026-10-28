# Pinned core authority and evidence

The 2026-09-13 user goal adopts pinned rurr as the source of truth for all core
observable semantics and authorizes the completion plan's narrow App contract
amendments. This supersedes the former evidence-only classification and former
step/stream/history/fork exclusions. Exact public signatures remain solely in
../SPECIFICATIONS.md; lower Kit ledgers continue to prohibit concrete/default
implementation coupling and native envelope inspection.

## Fixed source

`app-kit/vendor/rurr-main.zip` is immutable, SHA-256
`5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e`.
Its archive comment identifies commit
`005bfe3ece631e64b06dac2d42ef84235f727a4f`; README targets Burr v0.41.0.
Inspect exact members with unzip -p; do not extract/build/modify the archive,
execute vendored automation, add rurr as a dependency or refresh the baseline.
Pinned architecture guardrails/divergences/semantics and executable source/tests
resolve behavior. An apparent prose/source conflict requires source/test/oracle
evidence, never an invented convenient semantic choice.

Key source families, rooted at `rurr-main/`:

| Capability | Source authority |
|---|---|
| Actions/input/result/stream contract | crates/rurr-core/src/action/api.rs; action/condition.rs; application/session.rs |
| State order/operations/typing | crates/rurr-core/src/state.rs; state/typing.rs |
| Process-global field/tag codecs | crates/rurr-core/src/state/serde_registry.rs; docs/compatibility/DIVERGENCES.md §8 |
| Step/iterate/halt and ordinal | crates/rurr-core/src/application/api.rs; session.rs; halt_controls.rs; state_meta.rs; step_txn.rs |
| Streaming transaction/drop/hooks | crates/rurr-core/src/application/streaming/completion.rs; step_core.rs; lifecycle/adapter.rs |
| Resume/fork and saved position | crates/rurr-core/src/application/build/initializer.rs; crates/rurr-persistence/src/traits.rs; spec.rs |
| Local futures/child identity/executor | docs/compatibility/DIVERGENCES.md §§9–10; crates/rurr-core/src/parallelism/ |
| Tracking core content | crates/rurr-core/src/tracking/ and tracking/models/; goldens/schema/tracking.log_entry.schema.json |
| Cross-cutting semantics | docs/compatibility/SEMANTICS.md; docs/RURR_TEST_MATRIX.md; docs/03_test_plan.md |

The full capability/proof matrix is
../../plans/2026-09-13/ACCEPTANCE_MATRIX.md (A01–A31 and C01–C12).
The implementation must add source/golden provenance to each executable App
fixture rather than derive expected results from current App output. Core
scope includes ordinary actions, distinct inputs/results, rich conditions,
typed/serialized state, sync/async/local execution, hooks, child executors/
pipelines, controls/streaming/history/restore/fork and graph inspection/tags.

## Scope translations and explicit exclusions

- App vocabulary and private records remain App-native. Static traits,
  associated children, structs/enums/methods replace rurr macros/facade aliases;
  the required callable/local/synchronous outcomes remain.
- One App execution kernel preserves rurr serial first-match behavior. Explicit
  BSP waves/reducers, durable cancellation, receipt reconciliation and truthful
  Unknown effects are additional App guarantees, not replacements for serial
  stepping or transactional streaming.
- Lower integrations use Provider/Agent/Tool/Storage traits and portable DTOs.
  App constructs none of those products. Knowledge stays below Agent/Storage.
- Server/UI/service indexing/annotation endpoints, CLI and exporter transports
  are excluded by the user. Future app-server/app-cli/observability-sdk work is
  separate. This excludes no core checkpoint/history or lifecycle capability.
- Concrete Local/S3 tracking storage/exports belong to caller/lower adapters;
  App's core graph/metadata/step/child projections remain usable and tested.
- App's transactional StateStore seam translates rurr's sync/async persistence
  implementation classes. It does not expose fictional native sync identity.
  The still-driven caller runtime supplies async I/O for synchronous App controls.

## Corrected semantic anchors

Division 5/2 is 2.5; remainder -5%2 is 1. Missing required reads and invalid
comparisons are errors. State retains insertion order. Sessions may reuse
runtime inputs across successive actions; inputs are not durable state.
Halting validates selectors before effects, but the first current-session action
runs before checking historical halt position. Ordinary run/iteration tests
before-next first; streaming iteration tests after-prior first. WORKFLOW.md
owns the exact next/step/finish mapping, retained halt projection and resume
oracles (`application/session.rs:169–210,335–374,489–514`). Control checkpoints
never inflate committed action ordinal.

Partial stream results do not commit state/ordinal. Drop/error/EOF before commit
admission rolls back the staged boundary; native commit admission retains final
authority after observer loss. Existing network effects are not undone by state
rollback. Local streams/Data stay local; no unsafe Send or eager drain. Global
field/tag handlers are process-global and sampled per operation. Hook completion
error is surfaced while primary cause remains available. Equal child task keys
under the same parent preserve rurr identity reuse across different parent nodes.

## External background and proof limits

The earlier 2026-08-10 audit referenced Apache Burr v0.42.0-incubating application,
persistence and parallelism documentation. That newer release is background,
not this goal's pinned oracle. External documentation is reverified when a new
question depends on it and labeled with version/access date; do not silently
upgrade semantics. No external fact in this ledger substitutes for execution.

Fixture/golden comparison is deterministic parity evidence. It is not runtime
differential execution unless the exact separately installed authorized oracle
was actually invoked, and not strict-live evidence. Compile tests prove public
shape; runtime tests prove behavior; real configured services/processes prove
native integration/recovery. Performance requires measured workloads. Every
area also requires independent review with actionable findings resolved.
