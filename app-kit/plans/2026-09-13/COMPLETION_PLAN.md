# app-kit core completion plan

Prepared 2026-09-13 against workspace commit `9cec19c8eee6ca297e1e5e15de0a5dff3b5e61b9`. This is the implementation contract proposed for the next goal, not a claim that the implementation is complete. Production code and tests were not changed during planning.

## 1. Objective and authority

Deliver a production-ready application workflow core with the observable capabilities of the pinned rurr core, caller-supplied trait-based lower products, complete documentation and executable examples. Complete every acceptance row, required integration prerequisite and regression, with tests written first and independent code review before closing each area.

The user's annotations change the old audit's scope and recommendations:

1. The prior audit remains the defect/evidence baseline; its old suggestions to decide whether to adopt core capabilities are superseded by this completion objective.
2. Pinned rurr controls competing core semantics, including numeric division, modulo, comparison, state changes, halting, streaming and resume behavior. Fix App to match. Do not turn divergences into convenient new exclusions.
3. Server, web UI, CLI, service-specific integration adapters and observability exporters are outside this goal. Future app-server/app-cli/observability-sdk work is separate. App consumes Agent and StateStore traits; Agent consumes Provider, StateStore, optional Database, and Toolset traits. Generic portable DTOs cross those seams. Supplied implementations remain legitimate caller choices, never required integration adapters.
4. Actual two-agent execution, state handoff, recovery and lower-resource shutdown are mandatory deterministic and strict-live acceptance work.
5. This entire plan is one bounded execution goal: amend contracts, complete lower pre-work, fix runtime defects, finish core features and proofs, finish docs, then pass final release evidence. No phase is merely a follow-up recommendation.

Read the prior [audit report](../../audits/2026-09-13/REPORT.md), [runtime review](../../audits/2026-09-13/RUNTIME.md), [compensation review](../../audits/2026-09-13/COMPENSATION.md), [31-area comparison](../../audits/2026-09-13/RURR_PARITY.md), and [documentation review](../../audits/2026-09-13/DOCUMENTATION.md). The new lower-crate reports below are integration audits, not whole-crate certifications:

- [Agent pre-work](AGENT_PREWORK.md): two demonstrated boundary prerequisites and their complete propagation map.
- [Provider pre-work](PROVIDER_PREWORK.md): no demonstrated production prerequisite; preserve exact associated continuation and existing public streams.
- [Tool pre-work](TOOL_PREWORK.md): direct traits/DTOs are ready; Agent's consumption is the mandatory change. Generic MCP serving is supporting work adopted below, not an App blocker.
- [Storage pre-work](STORAGE_PREWORK.md): no new storage operations; strengthen semantic contracts/proof and remove duplicated forwarders/App dispatch.

The complete [acceptance matrix](ACCEPTANCE_MATRIX.md) tracks all 31 audit areas plus 12 correctness requirements. The [plan-review record](PLAN_REVIEW.md) records the three independent reviews and the corrections incorporated before finalization.

The main plan resolves alternatives in those audit notes. It takes precedence over their proposed alternatives, optional wording, or old default-handle examples. Phase 0 updates the owning normative documents before expanding production surfaces.

Use the requested app/agent/provider/tool/storage research skills and Rust core, async, testing, networking, abstractions, architecture, struct/enum organization and dependencies/platforms skills. Use crate change contracts for changes within their owners. Root user instructions override stale skill statements that rurr is merely advisory or that tests must wait until an entire refactor is complete.

### Pinned oracle

`app-kit/vendor/rurr-main.zip` SHA-256 is `5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e`; archive commit is `005bfe3ece631e64b06dac2d42ef84235f727a4f`, targeting Burr 0.41.0. Read archive members using `unzip -p`; do not refresh, extract, patch, run vendor automation or use the extracted tree as authority. Current Apache Burr/graph-engine docs clarify context only; they cannot change this target. Never inspect or modify repository `tmp/**`.

Within rurr, inspect `docs/ARCHITECTURE_GUARDRAILS.md` and `docs/compatibility/{DIVERGENCES,SEMANTICS,API_MAP}.md`, the named source members and existing goldens together. A disagreement between its prose and implementation must become a small oracle fixture with a recorded source decision before App code is changed. Use its explicit guardrails/divergence hierarchy, then its pinned executable tests and actual implementation to resolve ambiguous prose; record the rationale. Do not pick whichever source is easier to implement. Translate Rust names and ownership honestly; a missing operation is not an ergonomic translation.

### Scope and prohibited shortcuts

Core actions, transient inputs/results, expressions, typed state, serialization, execution control, transactional streams, history/restore/fork, child composition/executors, lifecycle callbacks, graph inspection and tracking data remain in scope. Metadata tracing alone does not replace hooks or retained history. A snapshot poll does not replace iteration or streaming. A copied latest state does not replace historical fork.

Keep lower credentials, configuration factories, Provider continuations/native payloads and AgentId out of App records. App may persist only the public lower workspace/conversation identities it needs for recovery. Preserve the nine closed Provider/Storage/Agent dispatchers, associated child contracts and the single optional Toolset erased ingress. No public lower factory, conformance layer, knowledge handle, extra child parameter or App-to-Provider/Tool dependency is authorized.

No new crate, compatibility shim, item alias, production test branch, unsafe code, generated source, dispatch macro, general repository/runtime framework or speculative public capability flag. Necessary new App-only behavior seams must be justified by a concrete rurr row and approved in the exact App ledger in phase 0. New package dependencies are not planned. Any necessary direct use of an already pinned workspace package or feature must be explicitly listed with the affected Cargo graph and reviewed; it cannot arrive as an incidental refactor.

## 2. Objective completion and evidence rules

A phase closes only when all of its acceptance cases pass **and an independent subagent reviews its actual diff**. A reviewer must not be the implementation author. Review public boundaries, observable correctness, task/cancellation/cleanup ownership, reductions and documentation. Resolve all actionable findings; a discovered regression reopens the phase. Final review samples interactions across phases, not just isolated files.

For each vertical slice:

1. Record the requirement ID, owning spec, existing owner to extend, exact planned deletions and smallest test target in `EXECUTION_LOG.md` (new during execution).
2. Add or change public behavior/compile tests **before production changes**. Run the smallest target and retain the meaningful failing assertion/compiler diagnostic. An unavailable new API may produce the first compile-red; after it compiles, the behavioral red is still required before implementing its behavior. If a strengthened proof already passes, record proof closure and make no invented production fix.
3. Implement the smallest coherent change. Refactor only after green tests, preserving the already tested behavior. Never fix a test by weakening its requirement, inserting skips/ignored cases or replacing real-service proof with fixtures.
4. Run the affected focused tests, review the diff with a subagent, resolve findings, and run the owning crate gate after the slice is assembled. Record commands, exit status, counts, revision/diff identity and review disposition.
5. Keep compile, deterministic, protocol-fixture, differential/golden, real-service, crash/process and performance evidence separately labeled. No quantity of unit tests upgrades a claim to strict-live.

All tests stay in the owning crate's `tests/` tree. Reuse its existing support owners with ordinary methods. Use deterministic rendezvous/barriers and controlled time for races; do not depend on a lucky sleep. Test-local foreign backends are allowed in deterministic tests. Strict-live tests must use real configured services/processes/stores and fail when prerequisites are missing. One Cargo command runs at a time across all agents.

The final stop condition is: every in-scope acceptance row has its passing test/evidence pointer; all required pre-work and docs/examples are complete; no unexplained observable rurr divergence remains; every phase and the final diff have clean independent reviews; all documented feature lanes, strict verifiers, workspace wrapper and locked clean-checkout proof pass. Partial gates, unavailable services, unrun crash workloads or unanswered core-parity rows are incomplete, not completion.

### Existing baseline, not fresh validation

The earlier audit recorded 33 deterministic/static tests, one compile-only doctest, App formatting/clippy/rustdoc/hygiene and ten real OpenCode/Ollama live tests passing. Three added temporary probes failed and demonstrated compensation, setup-deadline and panic-orphan defects. Their source is preserved in the audit folder. This planning turn performed read-only integration research; it did not rerun Cargo, services or those probes. The previous green baseline does not prove the new acceptance criteria.

## 3. Reduction and reuse ledger

Check the existing owner before every addition. Put behavior on that owner or on a data-carrying enum representing a real new invariant. An independently reviewed addition must state why extending an existing owner is insufficient. File length alone does not justify a new abstraction.

| Area | Reuse/extend | Explicit deletion or avoided expansion |
|---|---|---|
| App persistence | `AppStore<CS>`, RecordKey, AppMutation, exact-read receipt epilogue | Delete AppState's built-in Ephemeral construction, AppStoreInner and AppStoreTransaction plus forwarding implementations; require caller CS directly. No storage history/fork trait or generic repository. |
| App lower handles | Existing App/Workspace/Workflow/Run generic chain and Arc<A> ownership | Remove supplied lower default generic types/imports from App. Keep only CS and A as lower parameters; no independent Workspace/Conversation or P/D/T parameters. |
| Agent tools | Builder, CustomTools, Codex tool owner, existing prompt/handler loops | Replace concrete Toolset/ToolHandler with T/T::Handler; delete normalization requirements and shell-only construction/task-local routing. No heterogeneous wrapper registry. |
| Agent prompt delivery | Existing accepted-prompt owner, provider continuation loop, notification/abort ownership | Add one transient output channel on the existing seam; no second agent loop, durable token log, native payload bridge or provider stream wrapper family. |
| Storage supplied handles | Existing trait implementations | Delete same-name inherent behavior forwarders; retain constructors and closed built-in dispatch. Fix consumers' imports instead of adding aliases. |
| Workflow definition | CompiledDefinition, Node, Step, ValueExpr, Condition, RetryPolicy, JsonBoundary | Extend the strict typed language. No public mutable AST, arbitrary eval, alternate parser, native action code in JSON or generated schema/parser. |
| Execution | ExecutionEngine, staged activations, route/reducer owners | One transition kernel for run/step/iterate/stream and one control-set representation; delete singleton-control flow and duplicate outcome publication. |
| Runtime task safety | AppTasks and existing run driver/cell | Track typed exits; no generic task framework, detached panic watcher per call, duplicate completion guard or swallowed JoinError. |
| Interaction | Attempt, existing generation/fence and resolution state | Replace OnceLock notification and one-way Applied state with target-keyed state; no parallel notification broker/history engine. |
| History | Existing mutation transaction and record codec | Replace inline current-plus-one-previous as history representation with immutable bounded rows plus head/index. One retention engine; no second store actor or native snapshot escape. |
| Read APIs | Existing WorkflowDefinition/WorkflowRun owners | Add operation-specific methods and passive outputs in `workflow`; avoid Started/Completed/Inputs/Task wrapper families, aliases and duplicate list APIs. |
| Docs/tests | Existing scripts, crate integration targets, documentation ledger | Remove private-name locks, stale allowlist counts, superseded examples and contradictory prose. No shared conformance crate or duplicated signature ledgers. |

The shell change is a deliberate ergonomic reduction: callers supply an ordinary Local or foreign shell tool and its validated cwd/input policy. Agent no longer constructs that tool or injects cwd through task-local state. Preserve actual shell execution/cancellation through migrated tests and examples. This is not permission to delete shell capability or skip its live tests.

MCP serving will use the small generic `McpServer::start<T: ToolsetBackend>(Arc<T>, config)` amendment in the Tool report. This closes an existing concrete-consumer limitation consistently with the user requirement. Keep McpServer non-generic and non-Clone; its existing tracked task owns the generic handler. Server shutdown still never shuts down T. No new server product or protocol capability is added.

## 4. Phase 0 — binding specifications and executable acceptance skeleton

**First changes:** create the acceptance fixtures/compile cases described below, update tests that freeze obsolete APIs, and record expected red status. Amend specs alongside these tests before production implementation. This phase includes short compile-design experiments using existing crate tests for difficult ownership signatures; remove unsuccessful sketches instead of leaving alternate public paths.

| Owning document and current anchor | Required amendment |
|---|---|
| root `AGENTS.md:1`, `ARCHITECTURE.md:1` | Cross-crate direct trait-only construction; explicit minimal App-core seam exceptions; Agent T generic; remove contradictory concrete-tool requirement; preserve lower dispatcher policy. |
| `app-kit/AGENTS.md:7`, `:17`, `:47`, `:90` | Rurr semantic authority; expanded exact ledger; stream execution ownership versus observer drop; simultaneous controls; new epoch; tests-first and review gate. Delete old blanket bans on adopted core features. |
| `app-kit/specs/SPECIFICATIONS.md:1`, `:317`, `:411` | Replace frozen 20-type/22-method allowlist with one minimal exact contract covering adopted core behavior. Tight root remains primary owners; supporting public types remain `workflow`. Remove exclusions of actions/control/streams/history/fork/retention as adopted. |
| `app-kit/specs/workflows/RESEARCH.md:5`, `:46`, `:67`, `:96` | Pinned semantic acceptance matrix, source classifications and test IDs. Delete claims that snapshots replace streaming/history/halts. Record user-excluded products separately. |
| `app-kit/specs/workflows/WORKFLOW.md:1`, `:83`, `:105`, `:146`, `:165` | Full operator/condition/state/reducer grammar and truth tables, rurr arithmetic/errors; complete ready-set barriers; action inputs/results; retry applicability; child/executor policies; failure/panic classification. |
| `app-kit/specs/workflows/INTERACTIONS.md:9` | Multiple targets per activation, delivery and settlement identity, duplicate receipts, cancellation/deadline rules and bounded ownership. |
| `app-kit/specs/workflows/CHECKPOINTS.md:3`, `:23`, `:33`, `:105` | Distinct storage checkpoint/action sequences; control sets; retained history/result policy; source pins and fork/restore; transient stream rollback; delayed observer guarantees. |
| `app-kit/specs/APP.md:36`, `:64`, `:99`, `:109` | Caller CS, domain ownership, limits, admission/backpressure, foreground versus background execution, revision deployment and startup/reopen rules. |
| `app-kit/specs/STATE_STORE.md:25`, `:45` | Direct CS/associated transaction, atomic immutable history/head/receipt mutation, no scan-snapshot claims, retention/prune crash safety. |
| `app-kit/specs/WORKSPACE.md:21`, `:86` | Exact recorded lower revision for replacement/compensation, multi-agent materialization and caller cwd/tool responsibilities. |
| `app-kit/specs/OBSERVABILITY.md:21`, `:43`, `:55` | Guaranteed spans/events, allowed data, durable core history versus transient tracing/hooks, callback failure timing and exporter ownership. |
| `app-kit/specs/workflows/TESTING.md:1` | Every acceptance ID, actual feature lanes, two-agent/crash/stream/retention/performance proofs, docs tests and review evidence. |
| Agent and Storage owning docs | Exact amendments and anchors in the linked pre-work reports; no duplicate API ledger here. |

**Selected version break:** publish the coherent new workflow document as `app-kit.workflow/v2` and private record epoch `app/v6`. Replace the v1 workflow and v5 private-record formats, do not carry dual parsers, aliases or migration adapters. Older epochs fail read-only with the existing explicit unsupported-version category. Tests prove failure causes no writes and no lower calls. Document export/rebuild into a new domain for development data; do not invent an automatic migration. Keep definitions and action revisions pinned for a run; retained historical definitions remain addressable by their version for restore/fork. Global codecs are sampled per serialization operation, not pinned by callable identity; versioned field/tag payloads are the caller codec's responsibility.

**Schema:** hand-maintained `docs/schema/workflow-v2.schema.json`, JSON Schema draft explicitly fixed, local references only, unknown fields rejected, duplicate keys rejected by parser, all size/depth limits enforced before effects. Schema validates syntax; the compiler additionally validates graph semantics. Test both directions for a curated valid/invalid corpus and explicitly document cases requiring semantic validation.

**Phase DoD:** every core capability has an owning spec/test ID and intended public operation, all necessary signature shapes compile in the tests-first branch once implemented, reviewers find no unpriced boundary expansion or hidden semantic exclusion. The subsequent phases implement the selected contract; they do not reopen whether core features are in scope.

## 5. Phase 1 — lower-crate prerequisites and direct composition

Execute the full test-first slices in the linked pre-work reports, with these resolved choices:

1. **Agent tool parameter:** extend existing supplied Agent/Workspace/Conversation and connected runtime owners with `T: ToolsetBackend`; use only `T::Handler` for the child. Replace typed-provider create/reopen constructor pair with typed-backends P,T selection. Keep `.with_toolset(T)` as an append operation for a fixed homogeneous T; no repeated type-changing replacement operation. P/D builder changes preserve T. Use private Arc<T> only where shared ownership is required; T and its Handler remain non-Clone. Migrate Custom and Codex paths and preserve OpenCode's honest unsupported tool-installation behavior.
2. **Delete shell convenience** after the caller-built shell regression is red. Remove `CustomShellConfig`, implicit Local tool creation, shell task-local cwd binding and shell-only validation/hooks. Keep generic request cwd validation and normal caller-supplied tools. All affected tests and docs migrate in the same slice.
3. **Agent incremental output:** add `PromptEvent`, publicly constructible non-Clone `PromptStream` in `agents_kit::prompt`, and `ConversationBackend::prompt_stream`. Preserve ordinary generate-only prompt support. One existing prompt owner drives both delivery modes. Provider's existing stream and exact `P::Model::Continuation` are reused. Bound output queues; a stalled observer must not block protocol interactions/abort. Drop is observation loss, not implicit prompt cancellation; App's execution-stream abort explicitly uses Conversation abort and exact generation fencing. Actual native deltas must come from pinned protocol ownership paths, not terminal-text chunking.
4. **Storage contract proof:** non-Clone CS/Transaction and Database fixtures with populated outputs; serializable read/write and missing-key conflicts, exact revision/delete/recreate rules, read-own-writes, rollback/drop, uncertain commit, ordered exclusive pagination. A list traversal is not a snapshot. Expand real PostgreSQL coverage before trusting these obligations. Repair a built-in only when its new test fails. Remove duplicated inherent behavior forwarders and fix trait imports throughout consumers.
5. **App storage reduction:** require caller CS even for process-local use. Remove lower supplied default generics, AppState, Redb constructor imports and the duplicate store/transaction enums. AppStore retains CS directly and owns only app record semantics. Tests/examples construct supplied storage outside the library.
6. **Tool serving follow-through:** generic Arc<T> input and private handler on the existing McpServer start path; non-Clone direct Toolset is callable over real protocol fixtures, serving lifecycle stays independent. No new Tool trait/DTO expansion.
7. **Provider:** no production changes unless the new consumer tests demonstrate a real defect. Add only missing boundary proofs to existing targets: caller Model stream is constructible, non-Clone child values work, continuation survives function-result replay in the live loop, provider-neutral reopen does not retain continuation. No synthetic streaming or sessions.

Core shape to preserve (illustrative, not another signature ledger):

```rust
struct AppStore<CS: StateStoreBackend> {
    inner: CS,
}

impl<CS: StateStoreBackend> AppStore<CS> {
    fn new(inner: CS) -> Self {
        Self { inner }
    }
    // Existing mutation receiver owns CS::Transaction directly.
}

struct CustomTools<T: ToolsetBackend> {
    sources: Vec<Arc<T>>,
    // Extend the existing routing/catalog/lifecycle owner.
}

impl<T: ToolsetBackend> CustomTools<T> {
    async fn handler(&self, name: ToolName) -> Result<T::Handler, KitError> {
        let source = self.admitted_source(&name)?;
        source.tool_handler(name).await.map_err(KitError::from)
    }
}
```

The source borrow/ToolName details must use the exact existing trait signature; the invariant is concrete associated children, one inventory and no lock held during the lower await. Do not copy this sketch verbatim as an extra wrapper.

**Tests and DoD:** all direct custom values work without normalization, Clone or supplied default handles; all existing supplied backends remain legitimate consumers; failure cleanup calls each transferred source exactly once; no lower trait widening beyond the one Agent stream operation; affected compile/doctest/deterministic/live lanes pass; independent Agent/tool and storage/provider reviews close the phase. Exact files, deletions and targeted commands are in the four linked reports.

## 6. Phase 2 — fix current runtime correctness before feature expansion

### 2A. Workspace compensation fences

**Red:** promote the preserved compensation probe into `tests/workspace_contract.rs`. Agent one applies r1→r2, another actor writes r3, agent two fails, compensation must use recorded r2 and return Conflict while preserving r3. Cover multiple successful legs, partial compensation, receipt loss, stale app revision, cancellation during rollback and reopen of unfinished replacement.

**Modify:** `src/workspace/mod.rs:722` replacement, `:752` latest info use, `:795` compensation, `:817` stored materialization, `:821` refreshed info, `:1211` apply_update; `src/workspace/operations.rs:1` existing operation state. Construct the lower update from the recorded precondition or exact returned revision for that leg. A fresh read may diagnose current truth, but must never authorize overwriting a foreign revision. Commit compensation progress with its existing app fence; preserve the primary failure and attach cleanup/conflict evidence.

**Reduction:** one mutation method taking an explicit validated expected revision; delete the path that silently derives a stronger write fence from latest info. Do not duplicate replacement/compensation DTO families.

### 2B. Whole-operation deadlines and cancellation

**Red:** promote the hung-creation probe; add hangs before/after get-workspace, get/new-conversation, notification subscription, prompt/stream opening, resolve, abort, recovery and workspace materialization. A 25ms activation budget cannot remain Running after setup consumes that budget. Test caller wait timeout separately from accepted-work deadline; abort failure must retain uncertain-effect evidence and bounded cleanup.

**Modify:** `workflow/interaction/mod.rs:43`, `:139`; `workflow/run/mod.rs:1175`, `:2002`, `:2028`, `:2170`, `:2305`; existing App/workspace lower-call owners. Compute one activation deadline at admission, carry remaining budget through setup and dispatch, and preserve a separately documented bounded cleanup grace. Never reset the action budget at prompt start or on retry. Persist the relevant deadline/retry truth for recovery; wall-clock restart translation must be conservative. Include storage receipt reconciliation in operation ownership without dropping an admitted native commit and assuming rollback.

**Reduction:** one deadline/cancellation state on the existing Attempt/operation owner; no timeout wrapper per backend or shared runtime framework. Bound cooperative waits, document that Rust cannot preempt a blocking user implementation in a poll, and enforce the trait contract against blocking callbacks.

### 2C. Panic-safe owned drivers

**Red:** promote the backend panic probe. Inject panic before dispatch, after accepted prompt/effect intent, during result staging, notification resolution, cleanup and finalizer. Assert no orphan Running state, no leaked active capacity, every task joined, preserved primary error, and safe Unknown after potentially accepted effects. Test shutdown after a failed driver and lost waiter.

**Modify:** `app/tasks.rs:27`, `:38`, `:42`, `:77`, `:91`; `workflow/run/mod.rs:560`, `:577`, `:578`; App shutdown `app/mod.rs:633` onward. Existing AppTasks must record typed job ownership and inspect every JoinError; run scheduling must have one guaranteed completion/reconciliation epilogue. The supervisor can classify a panicked job from its persisted intent and exact owner identity without requiring the panicked body to execute cleanup. No `AssertUnwindSafe` catch-and-continue around an unverified partially mutated owner. Distinguish recoverable unwinding from process abort; process death is covered by reopen tests.

**Reduction:** eliminate ignored join results and completion paths that exist only after `.await` returns normally. Reuse the tracked supervisor/finalizer structure rather than spawning a detached watchdog around every lower call.

### 2D. Multiple actionable notifications per activation

**Red:** one prompt emits target N1, waits for answer, then N2; settle both, commit one prompt result. Exercise repeated N1 delivery, duplicate identical resolution, changed response conflict, N1 stale resolution after N2, lost caller receipt after lower resolution, restart between settlement and next target, cancellation with N2 outstanding and bounded flood handling.

**Modify:** `workflow/interaction/mod.rs:19`, `:22`, `:112`; `workflow/run/mod.rs:751`, `:906`, `:927`, `:2305`, `:5009`. Replace OnceLock with an existing-owner state enum whose generation includes the exact public notification target. Resolution evidence is keyed by attempt + target + resolution identity; a settled N1 is not an Applied state for N2. Persist intended/applied evidence before advancing ownership. Retain enough bounded settled-target evidence for exact duplicate acknowledgement without replaying lower effects.

**Reduction:** one target-state machine, one receipt reconciliation path. Delete single-target special cases; do not add a second notification interface to Agent (its current constructors already work).

**Phase tests/gates:** `cargo test -p app-kit --test workspace_contract -- --nocapture --test-threads=1`, then the relevant `workflow_execution` cases or new `runtime_faults` target. Once green, run App's full check and independent workspace/async reviews. Every new target must be added to `app-kit/scripts/check.sh:10` before the phase closes.

## 7. Phase 3 — state, expressions and graph semantics

### 3A. Oracle cases before engine changes

Add `tests/expression_parity.rs`, `tests/state_contract.rs`, `tests/graph_contract.rs` and `tests/parity_oracle.rs` only as distinct behavior domains, with fixture data under `tests/fixtures/rurr/`. Each fixture records archive hash, member, source/golden anchor, input, expected value/type/order/error category, and action/commit trace where relevant. Use the pinned rurr goldens when present. A manually translated fixture is labeled source-derived, not runtime differential.

The ordinary gate must run without importing rurr or executing vendor automation. An optional independent oracle run can be added only when the existing vendor-access rules permit a separately installed pinned executable; this plan does not authorize extracting/building the ZIP or adding rurr as a dependency. Acceptance requires executable App fixtures grounded in exact pinned behavior for every row, with explicit provenance. Never report source-derived fixtures as having executed rurr. If a prose/source conflict cannot be resolved from its pinned tests, retain that row as unresolved until an authorized oracle is available; do not silently approximate.

### 3B. Full state/expression behavior

**Modify existing owners:** `workflow/definition/mod.rs:130` Reducer, `:140` Mutation, `:149` ValueExpr, `:210` Condition, `:396` input/state separation, `:914` mutation validation, `:926` expression validation, `:1077` conditions, `:1114` JSON boundary; `workflow/execution/mod.rs:551`, `:616`, `:692`, `:743`, `:767`, `:782`.

Replace integer-only arithmetic with one private data-carrying numeric owner and methods implementing pinned signed/unsigned/float promotion, exact comparisons, overflow and finite-result errors. Do not route every number through f64 (large integers lose equality/order). Division yields 2.5 for 5/2; modulo yields 1 for -5%2. Cover both denominator signs, zero, MIN/-1, mixed integer/float, values above 2^53 and exponent boundaries. Add floor division and power; preserve rurr result type and error distinctions.

Complete safe expression capabilities: boolean truthiness/short circuit; comparisons and chained comparisons; membership/not-in/contains and invalid-container errors; None/boolean identity; conditional expression; unary numeric/not operators; length; nested attribute/key/index access; negative index and slices, including stride and invalid stride; missing versus null; literal arrays/objects and bounded depth. Extend the typed expression enum; do not evaluate arbitrary Rust/Python/JavaScript or add dynamic method calls. If expression text is supported for rurr authoring parity, one bounded parser compiles into this same enum; do not keep a second evaluator.

State operations cover immutable update, append, extend, increment, delete, wipe, merge and subset, preserving insertion order and typed-state association. Separate per-action operations from sibling reducers. Test existing `collect`, `append_array`, `sum_integer`, `merge_object`, `reject_concurrent` for base-value contribution, two writes from one action, multiple sibling writes, removal/write conflict, empty contributions and deterministic object collision order. Preserve app-native reducer behavior where rurr has no equivalent; label it an extension.

State key insertion order is observable in rurr and must survive serialize/reopen, update-existing, delete/reinsert, subset and graph/action inputs. Evaluate `serde_json`'s already-pinned `preserve_order` feature before writing another map wrapper; explicitly document its workspace feature-unification consequences and ensure no new package is resolved. If that feature is insufficient, use the existing state owner with an ordered field representation rather than duplicating a generic ordered-map library. Keep fingerprint canonicalization separate from order-sensitive state semantics.

### 3C. Strict graph validation and inspection

**Modify:** `definition/mod.rs:32`, `:45`, `:57`, `:283`, `:438`, `:532`, `:548`, `:1003`; `execution/mod.rs:873` routing. Preserve authored transition order; first matching condition wins on rurr serial graphs, default true has its exact validation/selection semantics. Broadcast remains explicit App behavior. Validate empty/duplicate/unknown nodes, unreachable nodes, entrypoints, stop routes, tags/selectors, read/write declarations, required inputs/keys, structural child references, cycles and visit bounds before effects where statically knowable.

Do not forbid all cycles to simplify scheduling: serial loops, repeated tags, repeated joins and bounded recursive/dynamic child admission have specified termination/resource rules. Add one receiver-owned `WorkflowDefinition::compose` operation that imports/reuses definitions with explicit entrypoint, collision/name-remap and connection data, preserving authored transition/tag order and action bindings. Compile/validate the resulting definition through the existing compiler. Test assembling and executing two reusable graph pieces, duplicate names, remapped actions/tags/edges, conflicting definitions and deterministic round trips. There is no public mutable AST, second graph builder family or second parser.

Keep graph inspection read-only: `WorkflowDefinition` exposes a compact passive graph description and deterministic DOT/Mermaid/JSON export through methods, with safe escaping and no Graphviz subprocess/server dependency. Add names/descriptions/tags in the existing Node metadata rather than a separate node-owner family. Inspection must not expose executable lower handles or private runtime state.

**Tests:** duplicate JSON fields, unknown variant/property, every boundary at limit and limit+1, invalid JSON Pointer escaping, null/missing distinctions, all reducer/operator branches, order-preserving parse/serialize, schema/compiler agreement, graph export golden and escaping, malformed condition error versus valid false. Invalid selectors and required input failures produce zero action calls/hooks/state changes. Resolve Sleep's message as a string and document Agent/Input rich message parts independently.

**DoD:** all source-derived semantic fixtures pass with no unexplained rurr numeric/condition/state divergence; ordinary graph behavior passes independently of services; reviewers inspect numeric exactness, resource bounds, schema/evaluator consistency and additions/deletions. Run the new targets and App check.

## 8. Phase 4 — ordinary actions, transient values, typed state and core extension seams

This is an App-owned extension, not an excuse to send every non-agent action through Agent or Tool. Add only the seams justified in the core design appendix; phase 0 names their exact allowed signatures. Concrete caller implementations and exhaustive enums remain first-class. The generic App owner may carry App-owned action/condition/state/hook/executor type parameters in addition to CS/A; these do not add lower Provider/Tool/Database parameters. Defaults, if useful, are App-owned JSON/no-hook/empty-action values with honest semantics, not supplied lower handles or phantom wrappers. Do not merge distinct domains into a god trait just to hide these types.

**Tests first:** new `tests/action_contract.rs` and `tests/state_contract.rs` cover callable terminal and streaming work without an Agent, declared reads/writes, input defaults/binding/required inputs, runtime context, result/state separation, typed validation failures before/after action, field/tag codec round trips, local futures, sync calls and error categories. A state-only workflow needs no live Agent or fake Agent invocation. Caller-owned action enum supports heterogeneous actions without boxing the whole App.

**Minimal ownership sketch (concept, exact signatures in the owning spec):**

```rust
pub trait ActionBackend {
    type Stream; // Concrete lazy stream; local form may be non-Send.
    // Declarations and execution use app-owned portable values.
    // Opening a stream must not eagerly run it or commit state.
}

pub trait StateTyping {
    type Data;
    // Validate candidate transitions, raw state, conversion and round trips.
}

pub trait LifecycleHooks {
    // One typed event vocabulary, with sync and async delivery semantics.
}

enum ActivationState {
    Ready,
    // Extend the existing intent/dispatched/staged/unknown representation.
    // Do not create a parallel generic action lifecycle framework.
}
```

Use the [core design appendix](CORE_DESIGN.md) for concrete draft signatures and the corrected refinements below for synchronous/local ownership and custom conditions/children. ActionBackend, StateTyping, LifecycleHooks, ConditionBackend and ChildExecutor are the five justified App-core behavioral seams; they do not expand any lower hierarchy. Supporting values live under `workflow`, not flattened at the crate root. Reuse existing bounded state changes, attempt identity, deadline and failure categories. Keep fields private on execution owners; passive input/result/event DTOs may have public fields.

**Callable conditions are required too:** pinned `action/condition.rs:323` accepts a caller predicate and `:429` evaluates it independently of action execution. Add one static `ConditionBackend` with declared reads and a synchronous `evaluate(&self, state: &WorkflowState) -> Result<bool, AppError>` method. Bind concrete predicate values by validated logical key through the existing definition binding owner, with caller exhaustive enums for heterogeneity. The private Condition enum gains a logical custom-predicate variant; it does not retain boxed callbacks. Test captured caller context, missing required read, short circuit, predicate panic/error and zero action/sequence advancement during evaluation. A synthetic action returning bool would change action/history semantics and is not equivalent. This additional seam is explicitly priced; no callable predicate registry or global predicate erasure is allowed.

**Actions and input/result contract:** create and validate the action inventory before App ownership transfer; associate logical keys and explicit caller revisions with Node's Action variant. Bind declarations to definitions before effects. Runtime inputs remain distinct from initial/checkpoint state, retain every nonreserved runtime key and are validated according to rurr required/default/bind precedence and session behavior, and never become state merely because an action received them. A terminal action result contains transient output and proposed state changes; validate actual operation values, schemas and typing before commit, without using declaration metadata as a state-access/write allowlist. Reuse/rename the resolved `StateChangeRecord` from `execution/mod.rs:44` as the passive public `StateChange` boundary, containing concrete Values and operation data. The compiler's `StateMutation`/`ValueExpr` remain private syntax and compile into this one resolved vocabulary. Test downstream construction and private-AST rejection. Custom reducers are action behavior followed by the existing canonical state-change reduction, not an arbitrary global mutation callback.

Session inputs may be reused by successive actions in pinned rurr. Test this exact behavior; delete the old blanket first-action-only assertion. On durable recovery, do not persist arbitrary transient secrets to solve missing inputs: represent a pause requiring fresh runtime inputs before unstarted work, with declared durable input binding only when explicitly supplied as state. A partially accepted external effect still becomes Unknown, not a new input request. Fork copies committed state, not ephemeral session input or live action objects.

**Typed state:** match pinned transition validation, raw validation, typed conversion and round-trip checks, preserving the same type association through immutable transformations. JSON default typing avoids unnecessary conversion. Typed Data remains caller-owned and may contain local values in local execution; persistence stores its serialized portable state only. Validation and codec failures cannot partially commit. Schema-only JSON checks do not close this row.

**Serialization:** support pinned process-global field and tagged deserialization semantics, including `__burr_serde__` and deterministic wrapping of handler-local errors. The chosen exception is a single serialization-only global owner with immutable handler snapshots for one encode/decode operation. It may privately erase codec callables to retain arbitrary registered captures; it must not erase Agent/Provider/Storage/Tool backends, expose Any/native payload downcasting or become a runtime service registry. Registering a codec has the same cross-App effect as rurr; a scoped-only injected codec is insufficient. Freeze registration snapshots during an individual serialization/round trip, then observe the current global handler at the next operation. Do not invent a run-pinned codec registry or infer callable identity. Caller codecs encode any version marker in their portable field/tag payload and reject unsupported versions on decode. Match duplicate/replacement behavior to the pin; test replacement between operations on an already-open App and isolated-process concurrency cases.

**Local and synchronous core lanes:** retain real synchronous actions/application controls and caller-polled non-Send async action streams. Do not turn local streams into Send through unsafe, thread smuggling or eager collection. The synchronous facade drives the existing Execution futures on its calling thread through the App's captured Handle::block_on; it creates no worker, command channel or replacement engine. Background execution keeps its existing supervised worker. Both use the captured, still-running runtime for async lower operations; it does not infer runtime independence from A/CS being Send or migrate their reactors to a new runtime. The caller constructs lower values and keeps their original runtimes/services alive through App shutdown. A sync call works outside an async context after App construction; acquisition and blocking methods reject an entered Tokio runtime context before blocking. Document and test this construction recipe with a runtime-bound foreign CS and real supplied Agent/StateStore. No lower factory or reconstruction API is added.

For the actual background/child worker, register cancellation and worker identity with the existing App supervisor before spawn, then store the OS JoinHandle there immediately. SyncExecution owns no worker handle. Bounded command-channel closure revokes admission and causes cooperative finalization/exit. Sync facade Drop delegates synchronous generation revocation to the existing Execution; admitted App work retains its cleanup authority. Actual worker closure and App shutdown wait for registered worker exit; App root Drop's existing supervised finalizer also signals and joins abandoned workers through a tracked blocking-join task on the still-running owner runtime. Never self-join, drop an unjoined handle, run block_on from Drop or claim asynchronous hooks ran because a stream was dropped. Worker panic is classified and joined. Arbitrary noncooperative user code cannot be force-killed safely; its prohibition and resulting shutdown limits must be honest. Keep omitted-close, dropped-close-waiter, pending-command, panic, root-drop and native thread-exit proof for the actual worker. Hypothetical facade-specific worker/queue tests are superseded as inapplicable, never reported passed. Sync proof instead covers caller-thread local sources, still-driven foreign runtimes, sync-only callbacks, blocking-context rejection and admitted-receipt cleanup.

Async inline execution stays caller-polled and may be non-Send. Both lanes use the same transition kernel and commit protocol. Dropping a caller-local source drops it locally; only Send-safe revocation/reconciliation/effect evidence transfers to AppTasks. App cannot continue or asynchronously finalize an arbitrary dropped non-Send future. Caller actions own any tasks they themselves spawn; explicit finish/abort while the source exists performs its async finalization. Test Rc-across-await drop/abort and source destruction, with truthful Unknown for ambiguous I/O. The exact facade and foreground lease must pass phase-0 compile/ownership probes.

**Required narrow amendments:** App's old no-trait/no-registry/no-scheduler blanket is replaced only for these core operations; the serialization registry is the sole new callable erasure exception. Use the already pinned workspace `futures-util` directly in App if needed for the associated Stream contract, with no new resolved package/version and a recorded Cargo graph check. This is an explicit dependency-policy exception for core streams, not permission to add general libraries. Typed-state/codec/hook errors, cleanup errors and engine panic handling remain distinct: rurr hook error precedence is presented correctly while preserving the original execution error as cause; internal cleanup cannot erase primary failure.

**Files:** extend `workflow/mod.rs:32`, `:136` definition/binding operations, `definition/mod.rs:57` Step, `execution/mod.rs:25`, `:52` staged decisions and `run/mod.rs:2028` attempt path. Add `workflow/action/mod.rs`, `workflow/state/mod.rs` only for the new honest owners, exported through `workflow`; extend `app/mod.rs:90`, `:1160` inventory preflight and pins. Do not add a sibling crate, tool adapter or alternate engine. Remove old input-overlay code at `definition/mod.rs:396` and any terminal-only action shortcuts after shared execution tests are green.

**DoD:** native callable/typed/codec/local/sync tests pass, static custom backends compile, all errors leave valid state/effect evidence, one transition driver remains, explicit exception audit is approved by independent architecture and async reviewers. No unfinished local/sync/global-codec row may be relabeled a harmless Rust difference.

## 9. Phase 5 — complete sibling execution, step/iterate/halt and partial streaming

### 5A. One bounded ready set and control set

**Red:** broadcast to Input A, Input B, Sleep C, Agent D, Agent E; all independent ready activations become owned without waiting for A. Resolve B before A and preserve C/D/E. Shuffle completion order and prove identical state, action ordinal order and one barrier. Cover JoinAll/JoinFirst, failed sibling plus successful contribution, cancellation/Unknown loser cleanup, cycles and reopen with several control types.

**Modify:** `execution/mod.rs:164`, `:230`, `:340`, `:397`; `run/mod.rs:55`, `:68`, `:1641`, `:4056`; `workflow/mod.rs:45`, `:59`. Replace singleton `Option<ControlRecord>` with one bounded canonical set on the checkpoint owner. Lifecycle remains a data-carrying enum; don't copy controls into every lifecycle variant. Snapshot exposes a canonical collection of actionable controls, including Unknown, plus overall lifecycle. A target-specific fence lets independent resolutions proceed without invalidating unrelated sibling targets; state/barrier/global mutations still use the whole checkpoint fence.

Stage every ready result against one immutable base, preserve already staged work across controls/recovery, reduce exactly once in canonical ready order, then route after atomic commit. Count active attempts separately from admitted/retained runs. Fair bounded admission must not let one map/starved input monopolize all task slots.

### 5B. Exclusive execution controls

Add one non-Clone execution session owned by WorkflowRun, with one mode/state enum and `step`/`next`/`finish`/`abort` behavior as appropriate. A serial step executes one action; a BSP step executes one canonical ready wave and returns its ordered outcomes. Automatic start, iterator, stream and sync facade claim the same run-generation authority. A second execution handle fails before dispatch; read-only snapshots/history remain usable. Initial construction is lazy. Halting is a resumable boundary, not cancellation or a synthetic Input step.

Add passive execution options with halt-before/after selectors, tags, execution limit and transient inputs; do not add a scheduler tuning family. Validate unknown selectors and inputs before effects. Preserve pinned first-node halt behavior and before-next versus after-prior precedence. Result identifies pending action/no result for before, completed action/result for after. Cover repeated visits, tags, duplicate selector order and exhausted graphs. Keep control checkpoint sequences separate from zero-based rurr action ordinal; retries/control events cannot inflate committed action count.

**Files:** `workflow/mod.rs:147`..`:211`; existing `run/actor.rs` RunCell/registry; `run/mod.rs:319`, `:560`, `:1641`; `execution/mod.rs:164`, `:397`. Add `workflow/execution/session.rs` only after extracting actual shared session behavior; keep `mod.rs` as the directory owner. Remove auto-only scheduling branches and old snapshots-as-iteration claims.

### 5C. Lazy transactional partial output

**Red:** an action yields two distinct values before terminal; committed user state and committed action ordinal/position remain unchanged; terminal commits one action result. Durable intent/control checkpoint sequence and control-only history may advance. Add a notification-plus-partials case proving that distinction across reopen. Before commit admission, error/panic/drop/abort discards staged state. After storage commit admission, finalization retains authority: lost observation cannot promise rollback, and exact receipt/head reconciliation determines committed success or truthful uncertainty. Test drop immediately before admission, after admission before acknowledgement, and a committed transaction with lost reply; none may reexecute the action. A stream that never terminates is bounded by the action deadline. Also test unpolled drop, slow consumer, overflow, duplicate settlement fencing without polling past the first terminal, successful no-output EOF rollback/retry, concurrent execution rejection and cleanup failure.

Use one action stream model for terminal and streaming actions; terminal consumers drain lazily to terminal via the same driver. The execution-owning stream exclusively borrows/leases the execution session. Read-only prompt/event observers are distinct from this owner. Dropping an observer does not cancel accepted work; dropping the execution-owning step stream revokes admission before the commit linearization point, discards uncommitted local staging and prevents later actions. After native commit admission, it loses observation and the existing finalization owner reconciles the receipt. App-owned external work is aborted/reconciled; caller-local source lifetime follows phase 4's explicit limit, and uncertain effects remain Unknown. Drop cannot guarantee asynchronous hook completion. Explicit async finalize/get/abort completes the documented cleanup and joins owned work before returning.

Rurr rollback promises state/sequence rollback, not reversal of an already-issued external side effect. Preserve that distinction in specs/tests. A transport error after dispatch must not become an automatic replay merely because state was rolled back. In BSP, a partial output is tagged with activation/action identity and is never labeled a committed sibling contribution.

Agent nodes use `ConversationBackend::prompt` for ordinary delivery and `prompt_stream` for explicitly requested incremental delivery. Both feed the same App attempt/effect/commit owner. A terminal-only caller Agent must still run normally; a streaming request fails honestly before acceptance with Unsupported if unsupported, with no terminal chunking fallback. Test actual incremental output through caller Agent traits and real supporting native modes. Bound pending items and bytes; do not buffer whole outputs, spawn blocking producers or invent chunks from terminal text. Interaction/abort processing remains live while output observation stalls. Partial deltas are transient; committed action results/history have durable retrieval. Tracing is not a substitute for either.

**DoD:** `execution_controls`, `workflow_streaming`, `action_contract` and sibling regressions pass; local borrow/Send compile contracts and actual partial-before-terminal evidence pass; independent reviewers check stream drop/finalization, generation fencing, barrier atomicity and no parallel engines. Run App and changed Agent gates.

## 10. Phase 6 — retained history, recovery, fork and deployment

Adopt the full [history/control design and 12 test groups](HISTORY_AND_CONTROLS.md). Required implementation decisions:

- Immutable bounded checkpoint rows plus one mutable run head, written with existing AppMutation/CS transaction/receipt handling. Every row identifies initial/control/action/terminal kind, control sequence, attempted/committed action position/ordinal, exact definition revision and sufficient recovery facts. Eliminate `RunRecord.previous` and the permanent second authoritative copy of state.
- History enumeration captures a committed head cutoff, uses prefix/cursor pagination, and validates pruning/generation fences. Exact checkpoint and semantic action-sequence selection are distinct. No native storage snapshots or transactional list widening.
- Completed results and history are retained by default until explicit terminal-run deletion. Delete the 1000ms retention constant and automatic terminal expiry. Release active runtime/capacity independently of retention. Page history lazily; opening performs required epoch preflight one bounded page at a time without materializing all historical rows in memory. Document explicit storage/capacity exhaustion errors, never silently delete user history to satisfy limits.
- One restore/fork request covers source selection, explicit state overrides and resume position. Source stays unchanged. Fork copies a clean selected state/frontier into a new run so its future operation does not depend on source retention; lineage is metadata. A failed-safe boundary can resume at its saved position; a completed boundary resumes after it according to rurr. Explicit missing-selector fallback is separate from strict App checkpoint lookup.
- Never clone lower handles, provider continuation, transient inputs, active notifications or Unknown effect state into a fresh dispatched attempt. A selected Unknown/in-flight checkpoint rejects resume/fork until resolved. A caller may intentionally fork an earlier clean boundary of a currently Unknown run; the new identity makes potential re-execution explicit.
- Pinned run definitions/action revisions remain usable for historical recovery. App reopen supports a caller-supplied inventory containing the retained revisions plus new revisions for new runs. Reject ambiguous same-key/revision content and missing required retained versions before lower dispatch. Global codec replacement follows phase 4; encoded payload version validation belongs to the caller codec. No native Agent identity inspection; lower public workspace/conversation IDs and logical Agent revision keys are the only compatibility evidence.
- Retain single-domain generation fencing and the supported process model. Concurrent App open cannot both acquire mutation authority. Recovery transfers authority through existing durable generation rules; a timeout alone is not a distributed lease or proof the earlier process is dead. No distributed scheduler is added.

**Files:** `run/mod.rs:39`, `:42`, `:55`, `:1175`, `:1447`, `:3272`, `:3753`, `:4056`, `:4620`; `storage/mod.rs:198`, `:342`, `:459`, `:549`, `:1074`; `app/inventory.rs:1`; `app/mod.rs:90` and `:1160`; public `workflow/mod.rs:45`, `:158`, `:165`, `:172`. Put the private persisted checkpoint owner/codec in `workflow/run/checkpoint.rs` if moving it materially removes the run monolith's responsibility; retention methods stay on the existing run/store owners, not a new generic repository.

**Tests first:** history/head/receipt all-or-nothing at each write and lost commit reply; more than one page; action vs control sequence; corrupt row/head/index; definition change/missing revision; result accessible after >1s and shutdown/reopen; terminal capacity reused with retained history; deletion/fork/read races; safe failed/completed/Unknown restore; source deletion after fork; child lineage; source overrides invalid before admission. Add process crash fixtures for durable Redb and real PostgreSQL, with readiness signals at public backend boundaries and bounded child-process teardown. Run fresh process reopen after forced termination, not only App close/open in one process.

**Bounded explicit deletion:** on the existing terminal run owner, atomically seal its head against new read/fork admission and persist deletion progress. Delete bounded history/index pages with existing CS transactions and generation/receipt fences, advancing the cursor atomically with each page. Remove head and final indexes only when all owned rows are gone. Reopen resumes a sealed deletion. Concurrent new reads/forks after the seal return the specified Closed/NotFound/Conflict outcome rather than partial history success. Test more than one page, crash/lost reply at every page, fork versus seal and preservation of unrelated runs. No generic garbage collector, giant transaction or per-observer pin family.

**DoD:** all twelve appendix groups and revision-deployment tests pass; no lost result/duplicate effect/orphan history/capacity leak; immutable history and current state share one mutation authority; independent storage/recovery review resolves every finding. Durable native claims receive real-service/process proof.

## 11. Phase 7 — child composition, retry/structured-output completeness and multi-agent integration

### 7A. Full child and executor behavior

Reuse existing Step::Child/Map, child relation/admission and canonical indexed result reduction (`definition/mod.rs:78`, `:692`, `:755`; `run/mod.rs:3087`..`:3364`; `execution/mod.rs:366`). Extend these owners for rurr task/state source iteration, dynamic child graph selection from a validated revisioned inventory, nested pipelines, map/reduce, per-child logical execution overrides, initializer selection and custom executor behavior. No child receives a lower provider/tool/native store config envelope. Child persistence remains app CS; per-child logical history/initialization policy is app data.

The selected custom execution seam is `ChildExecutor`, separate from ActionRequest because it receives real admitted child execution authority. It receives the entire admitted batch once, may select polling order and concurrency, and returns opaque completions produced only by those child owners. A `ChildBatch<F>` owns unique child futures plus original ordinals and admission context; a `ChildCompletion` has private construction tied to one admitted child. App validates exact batch membership/duplicates/missing completion before reducing in original order. Dropped child owners revoke further admission and preserve App-owned effect reconciliation; a custom executor cannot fabricate success or detach ownership.

Use a generic associated execution future so local child futures remain local. Background execution constructs and polls the concrete future on the existing worker; it imposes no additional Send bound. This is an explicit App-only exception to RPITIT-only return spelling, not a duplicate trait or erased runtime. Trait-associated types are not prohibited item aliases. The compile-design prototype is:

```rust
pub trait ChildExecutor: Send + Sync + 'static {
    type Execution<'a, F>: Future<Output = Result<Vec<ChildCompletion>, AppError>> + 'a
    where
        Self: 'a,
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;

    fn execute<'a, F>(&'a self, batch: ChildBatch<F>) -> Self::Execution<'a, F>
    where
        F: Future<Output = Result<ChildCompletion, AppError>> + 'a;
}
```

ChildBatch exposes ordered unique children to consume/poll, not lower handles or a general spawn API. Its owners enforce deadline/cancellation and completion provenance. Default bounded execution is an existing engine implementation of this seam, not a second engine. The sync facade drives the same executor future outside an async caller context. Before implementation, a foreign executor must really run two children in reversed polling order and with local Rc state, then return original-order reduction through App; tests reject duplicate/missing completions and prove dropped-executor cleanup. Final generic syntax may be refined by these compile tests, but this ownership seam and all outcomes are fixed, not an open scope choice.

Rurr implicit child identity uses parent application + task key, including equal keys across parent actions and within a batch. Stable identity does not deduplicate execution: a fresh occurrence executes from its new inputs with semantic ordinal zero unless an explicit initializer loads saved state; persistence alone does not resume. Only recovery of the same admitted occurrence reattaches without replay. This corrects the earlier conflicting-state reuse interpretation using pinned `tests/contract/parallelism_contract/tracking_and_persistence.rs:525–708`. Preserve independent occurrence ownership, original-index completion membership and immutable App observations; do not reject changed inputs, skip effects, salt public task identities, or serialize equal-key asynchronous occurrences. The owning checkpoint/storage contract must reconcile occurrence-scoped active state and history selection before this cutover; a hash-only change is insufficient. Do not add a hash dependency merely to copy private bytes; if raw child ID text is part of a public fixture, implement its pinned format within the approved dependency policy. Pipeline initializers and missing-state fallback follow the corrected owning WORKFLOW contract.

**TDD cases:** empty/one/many children, nested map, dynamic two-definition choice, sequential dependency pipeline, source/child/reducer failure, bounded parallelism, shuffled completion with canonical output order, parent cancellation propagating through grandchildren, partial admission rollback, crash after admission before activation, child checkpoint initializer/history override, repeated task-key identity, retained parent/child deletion and no leaked task/source permits. Custom executor must not detach work, fake results or bypass app atomic admission.

### 7B. Finish declared but underproved behavior

Add successful structured Agent output and target schema rejection, text+structured mapping, invalid schema before dispatch, wrong output after accepted work without state commit. Exercise Immediate/Fixed/Exponential retry: initial attempt counted, max_attempts, elapsed deadline, zero/jitter-free deterministic schedule, overflow caps, cancellation while sleeping and durable reopen with remaining delay. Safe preacceptance errors may retry under the declared policy; accepted/unknown effects never automatically retry. Caller-authorized proven-safe retries still obey finite limits and target generation. Move policy handling out of workspace-lookup-only paths at `run/mod.rs:2002`, `:2505` into the existing attempt outcome owner, with exact classification tests.

### 7C. Required real multi-agent journey

Replace the empty definition inventory in `tests/workflow_live.rs:185` with a real two-node workflow for the dedicated execution case, retaining unrelated compensation tests separately. Agent A writes a validated structured artifact/key into state; Agent B receives that exact committed value through a prompt/state projection and produces the final validated result. Logical node agent keys must select different Agent values. Assert routing by observable per-agent transcript/outputs and public workspace/conversation identities, never by formatting AgentId.

Run three proof layers:

1. **Direct deterministic:** non-Clone foreign A/CS and associated children, caller exhaustive Agent/Workspace/Conversation enums, two distinct backend families, handoff, overlapping parallel branches, notifications and failure/cleanup counters. App source has no concrete-default dependency.
2. **Lower composition:** supplied Custom Agent constructed from direct foreign P/S/D/T with real associated model/handler behavior in deterministic tests; test populated knowledge retrieval through D below Agent. Preserve provider continuation only during prompt, not in App state. This proves all five crates compose by public traits.
3. **Strict-live:** two real configured Agents execute A→B; at least one supporting lane streams real partial output; a parallel A/B branch proves overlap under bounds. Persist an Input boundary between A and B, shut down/reopen from the same App/Agent domains with newly supplied Agent values, resolve, and prove A was not replayed and B received A's exact committed value. Then run a forced process-loss variant at a defined durable boundary, plus a post-dispatch Unknown case. Drain and CancelActive shutdown modes must close both owned agents once while preserving resources that the caller retained outside ownership. Run same-kind distinct instances and a mixed Custom/OpenCode or Custom/Codex lane where configured; missing required service blocks completion, not a skip.

Assertions should use controlled task outputs/JSON schemas, exact invocation counts and handoff tokens rather than model prose wording. Environment examples name required URLs/models/binaries and domains without secrets. Native backend feature limitations remain honest; choose a capable real configuration for required capabilities.

**DoD:** `child_composition`, retry/structured tests, direct composition tests and real multi-agent cases pass. Independent reviewers cover child ownership, generic composition, retries and the actual live assertions. Empty inventories, two Agents merely opened, or standalone prompts do not satisfy multi-agent workflow acceptance.

## 12. Phase 8 — lifecycle/tracking, completed docs and executable examples

### 8A. Core lifecycle and tracking data

Use `LifecycleHooks` and the existing committed transition owners; do not add an exporter or public conformance API. Implement the full pinned sync/async lifecycle order for application/child registration, execute call, step, stream and span/attribute events. Borrow existing context/result values; classify staged/committed/error states explicitly. Test pre-hook denial prevents action, item failure rolls back, post-hook failure does not replay a committed action, original failure remains available, sync post-hooks run on owning-stream drop, async hooks require explicit finalization. Caller hooks may intentionally receive content; default safe tracing must never include it.

Core tracking must expose usable graph metadata, run lineage, action sequence/position, begin/end/error, stream and child records with stable ordering and rehydration/debugging behavior. Reuse the retained history and typed lifecycle event vocabulary as sources; do not maintain a second authoritative tracking state machine. Provide explicit serialization/export methods for rurr-compatible graph/metadata/log/children records and test the pinned schema/goldens. Host code supplies filesystem/S3 transport or a StateStore writer; those concrete adapters are outside App. App's own control records remain App-native and must not adopt Burr wire records as persistence authority. No tracking HTTP service, UI, CLI, anonymous network telemetry, exporter flush/shutdown owner or Datadog/Grafana dependency is introduced.

Make the seven promised metadata spans/events in `specs/OBSERVABILITY.md:21` an explicit supported contract. Instrument futures by poll, never hold entered span guards across unrelated awaits. Emit committed-truth events only after successful/reconciled commit and document recovery repeats. Keep safe fields closed and redacted. Tests use a small test-local implementation of the existing `tracing::Subscriber` trait; no new subscriber dependency just for assertions.

**Files:** existing `app/mod.rs:90`, `workflow/run/mod.rs:1447` and all commit/attempt/interaction/child publication sites; `specs/OBSERVABILITY.md:1`; new `workflow/observation/mod.rs` only for the actual lifecycle/tracking owners and passive data. Export through `workflow`. `tests/lifecycle_observation.rs` captures hook order, span parentage, redaction, receipt-loss repeats, child context, stream rollback and golden tracking output. Faulting subscribers cannot become commit authority or corrupt primary errors.

### 8B. Completed documentation folder

Create all files below at line 1. Follow the final API, not the old v1 allowlist. These are finished downstream guides, not TODO stubs or copied specification ledgers.

| New file under `app-kit/docs/` | Required completion content |
|---|---|
| `README.md` | Linked guide index, concepts and reading order; supported-core/trait-owned/excluded classification. |
| `QUICKSTART.md` | Runnable state-only App→Workspace→Workflow→Run from actual construction through shutdown, caller CS and Cargo backend features. |
| `TRAIT_INTEGRATION.md` | Non-Clone caller A/CS, associated child enums, lower P/S/D/T composition below Agent, ownership transfer and failure cleanup. |
| `WORKFLOW_LANGUAGE.md` | Complete v2 grammar and fields, every step/expression/condition/reducer, tags/routes/cycles, defaults/errors/limits and valid/invalid examples. |
| `schema/workflow-v2.schema.json` | Human-maintained syntax schema; parser/compiler agreement tests, semantic validation limits explicitly documented. |
| `ACTIONS_AND_INPUTS.md` | Native actions, input bindings/defaults, transient results vs state, sync/local/async modes, side effects and structured error behavior. |
| `STATE_AND_PARALLELISM.md` | Immutable operations, insertion order, StateTyping, global field/tag codecs, reducers and parallel barrier truth tables. |
| `MULTI_AGENT_WORKFLOWS.md` | A→B and parallel A/B, structured handoff, recovery and shutdown; real test link plus caller exhaustive-enum recipe. |
| `EXECUTION_CONTROLS.md` | Run/step/iterate/halt/tag semantics including pinned first-action behavior, unique execution authority and sync call restrictions. |
| `STREAMING.md` | Partial result vs committed state, terminal/finalize, errors/EOF, local streams, bounded backpressure, observer drop vs owning stream drop. |
| `INTERACTIONS_AND_CONTROLS.md` | Correct wait/resolve loop, multiple independent controls, target fences, duplicate/lost receipts, Unknown and proven-safe retry. |
| `PERSISTENCE_AND_RECOVERY.md` | Separate lower/app durability, history/action selection, fork/rewind/override, retained results/deletion, revision deployment and crash runbooks. |
| `LIFECYCLE_AND_OPERATIONS.md` | Workspace compensation/deletion, shutdown escalation/root drop, task/thread ownership, process model, every limit and measured operating envelope. |
| `OBSERVABILITY.md` | Safe spans, hooks/content consent, tracking record export, host subscriber/transport ownership, repeat/loss/flush responsibility. |
| `ERRORS_AND_TROUBLESHOOTING.md` | Operation errors vs run failure/Unknown; stale revisions, unavailable services, deadlines, storage capacity, interrupted streams and recovery limits. |
| `STRICT_TESTING.md` | Exact focused/full/feature/live commands, real service/version/model setup, owned cleanup, no skips, proof labels and source-derived parity limits. |
| `RURR_PARITY.md` | All 31 rows with final implemented/translated/user-excluded classification and exact test links; App extensions labeled separately. |
| `examples/*.json` | Linear state, expressions/loop, two-agent sequential/parallel, Input, structured Agent, action result/input, child/map empty/bounded, joins, retry/sleep, tags, typing, graph composition and pipeline. |

Add thin executable consumers `app-kit/examples/{pure_state,multi_agent,actions_and_inputs,controls_and_streaming,history_and_recovery}.rs`. Their main functions delegate to domain owners. All configuration is caller-supplied; production App never reads environment variables. Examples may read explicit user configuration at the executable boundary. State-only examples must run without a provider service; live examples compile in ordinary gates and execute in their linked real-service suite. No new crate or separate sample runtime framework.

Expand rustdoc on current and new public types/methods (`src/lib.rs:10`, `:148`; `workflow/mod.rs:134`) with complete usage, relevant errors, fences, deadlines, cancellation and Drop semantics. Use executable doctests for pure examples; `no_run` only where a real service is necessary. Do not require one giant duplicate setup block on every DTO: use small focused examples and links to exact full workflows. Every public operation and invariant-bearing type needs an understandable use example.

Update `app-kit/README.md:7`, `:48`, `ARCHITECTURE.md:1`, `AGENTS.md:1`, root `README.md`, `ARCHITECTURE.md`, and `docs/documentation-ledger.toml:54`. The exact signature ledger remains only in `specs/SPECIFICATIONS.md`; downstream docs explain behavior. Read-only audit/plan artifacts remain clearly dated evidence, never current architecture teaching.

### 8C. Tests and documentation review

Add `tests/docs_examples.rs` before filling guides: discover the documented JSON corpus, parse every example, execute deterministic journeys and verify expected outputs; compile every Rust example; verify README/index/local links and schema fixtures; prove every advertised core feature has an acceptance row/test. Use existing compiler fixture machinery from sibling crates for snippet compilation, adapted inside App, without a shared crate/dependency or tests that assert exact private files.

An independent reviewer follows the docs from zero context: construct, author, run, handle multiple controls, stream/finalize, reopen/fork and shut down using public docs only. Another reviewer checks rustdoc/error/ownership promises against actual code. Resolve broken examples, missing information and misleading support claims before closing the phase. Passing `missing-docs` alone is insufficient.

## 13. Phase 9 — integrated gates, crash/performance evidence and final review

### Test target ownership and commands

Prefer existing test targets for closely related cases. The planned new domain targets are `compile_contract`, `action_contract`, `expression_parity`, `state_contract`, `graph_contract`, `execution_controls`, `workflow_streaming`, `checkpoint_history`, `child_composition`, `lifecycle_observation`, `docs_examples`, `parity_oracle`. Runtime defect cases fit existing `workflow_execution` and `workspace_contract`; do not retain a redundant `runtime_faults` target unless its separate ownership proves useful. Compile helper modules live under `tests/support/mod.rs` or the existing support directory; ordinary methods, no generated sources/aliases/macros.

Update `app-kit/scripts/check.sh:10` to include all deterministic targets explicitly; retain all existing checks. Add compile-negative consumers for private field construction, unexpected lower construction/erasure, wrong associated child, accidental Clone constraints, owning execution borrow conflicts. Add positive runtime proof that a real Rc-bearing background source is constructed, polled and dropped on one worker through completion, cancellation and shutdown. The public surface audit must find inherent methods implemented for exported types anywhere under `src/`, not only root declarations. Keep source guards for prohibited constructs, delete private-name assertions at `tests/public_surface.rs:590`.

Run the smallest target during red/green loops, for example:

```bash
cargo test -p app-kit --test expression_parity -- --nocapture --test-threads=1
cargo test -p app-kit --test execution_controls -- --nocapture --test-threads=1
cargo test -p app-kit --test workflow_streaming -- --nocapture --test-threads=1
cargo test -p app-kit --test checkpoint_history -- --nocapture --test-threads=1
cargo test -p app-kit --test child_composition -- --nocapture --test-threads=1
cargo test -p app-kit --test docs_examples -- --nocapture --test-threads=1
cargo check -p app-kit --examples
```

The lower pre-work appendices give their exact target commands. After relevant changes assemble, run:

```bash
bash providers-kit/scripts/check.sh
bash tools-kit/scripts/check.sh
bash storage-kit/scripts/check.sh
bash app-kit/scripts/check.sh
```

Agent's verifier is its full documented completion wrapper; do not invent an `agents-kit/scripts/check.sh`. It requires real setup:

```bash
source agents-kit/strict-tests.env
bash agents-kit/scripts/precheck_strict_live.sh
bash agents-kit/scripts/verify_strict_live.sh
```

Add the Agent `prompt_stream` target to its verifier and retain all existing public/compile/source/backend/notification/cleanup/custom/native tests. Do not turn a provider's honest Unsupported stream into a passing streamed-output test.

### Final workspace pass

Run the documented sequence once against the assembled reviewed candidate; repeat only after changes or unresolved failures justify it. Storage has three supported lanes; do not run storage/workspace all-features as a substitute.

```bash
git diff --check
cargo fmt --all --check
bash app-kit/scripts/check.sh
cargo clippy --workspace --all-targets --all-features --exclude storage-kit --exclude app-kit -- -D warnings
cargo clippy -p storage-kit --all-targets -- -D warnings
cargo clippy -p storage-kit --all-targets --no-default-features -- -D warnings
cargo clippy -p storage-kit --all-targets --no-default-features --features qdrant -- -D warnings
bash scripts/check_docs_site.sh
bash scripts/check_no_type_aliases.sh
bash scripts/check_no_path_wiring.sh
source app-kit/strict-tests.env
bash app-kit/scripts/verify_strict_live.sh
bash scripts/verify_workspace_strict.sh
```

The five-crate wrapper sources each crate's ignored strict env and executes all five real verifiers. Do not print credentials, stage env files, borrow another caller's domain or destroy unrelated service state. Start only documented test-owned services; track/stop their processes after validation. Service startup/precheck failures are blockers to the claimed live acceptance, while independent work continues. Test fixtures and real-service substitutions must never be mixed under a strict-live label.

If necessary to make the clean proof repeatable, narrowly amend wrapper commands to propagate a documented locked Cargo mode; preserve existing default gates. The final candidate's Cargo.lock must be unchanged by checks. App has no Cargo feature family; an explicit direct futures-util/preserve_order adjustment does not justify adding broad app backend features.

### Crash and performance workloads

Before optimization, add bounded workload tests/harnesses and capture an unoptimized correctness baseline. Reuse the existing runtime and std::time measurements; no benchmark crate, fake public clock/conformance interface or new dependency. Put performance execution in a separate explicit harness/ignored-by-default performance target only if its run command is mandatory in this final phase; ordinary behavioral tests must never be ignored. Prefer a named example/performance consumer if that avoids ignored-test ambiguity.

Required scenarios: 1,000-action linear state progression; 32 ready concurrent actions with permuted completion; 4,096-item map under max_parallel=8; 100 repeated runs with active limit=16 in the harness's declared configuration; 10,000 retained history rows with page size=128; fresh-process reopen of those rows; nested child cancellation and delayed observer. Use actual supported limits rather than mutating private constants from tests. If production active limits remain fixed, scale concurrency tests to an exposed admitted-work scenario and explicitly record the tested envelope; do not claim the entire 100,000-run inventory limit is benchmarked.

Correctness targets are exact: no partial barrier visibility, missing/duplicate action commits, unbounded pending output, automatic Unknown replay or unreleased owned resources. After terminal cleanup and after a 100-run repetition, tracked active tasks/children/tool handlers return to baseline; retained rows may grow only as specified. Peak memory must scale with active frontier + bounded page + bounded output, not total history size; compare 1,000 versus 10,000 history-row reopen to detect whole-history materialization. Measure p50/p95/max step and recovery latency, throughput and process peak RSS; report hardware/runtime/backend/dataset/feature lane. Use a 2x p95 regression against the same-host post-correctness baseline as a trigger for investigation, not as a fabricated universal production SLA. Completion requires explaining/resolving algorithmic or ownership regressions; publish the actual measured operating envelope.

For external cooperative waits, add deadline + cleanup-grace assertions tied to declared budgets, not absolute CI scheduling guesses. Crash recovery tests use child-process ready signals and force termination at intent, accepted effect, result staging, history/head/receipt commit and child admission boundaries. Each required durability scenario runs against a real state backend. A finite repeat loop of 100 runs is a soak, not proof of indefinite stability; report its exact scope.

### Locked clean-checkout proof and final review

Create a disposable clean candidate checkout from a snapshot commit in a temporary local Git object store/worktree, without modifying the user's branch or staging unrelated changes. Include all intended new source/tests/docs/config and Cargo.lock; exclude target, secrets, vendor extraction, historical tmp artifacts and unrelated work. Verify the snapshot's source digest matches the reviewed candidate. The goal authorizes this local validation snapshot; it does not require a remote commit/PR/deployment. Run the exact feature lanes/docs/strict wrapper in locked mode with fresh target paths and injected ignored env. Confirm no tracked file or lockfile changes afterward. A reused dirty working directory is not this proof.

Before final release proof, perform the user-required bloat reduction and code cleanup pass across the full five-crate diff. Inventory duplicated behavior, repeated state carriers, wrapper-only methods/types, parallel dispatch or execution paths, oversized domain methods, unnecessary cloning, dead vocabulary and duplicated tests/docs. For each finding, reuse or extend the smallest existing struct/enum owner and its methods; consolidate or delete superseded paths rather than add another layer. Record concrete removals/consolidations and justify any retained complexity by its invariant or distinct contract. Preserve behavioral assertions and required feature coverage; rerun affected tests after reductions. An independent subagent must review the resulting full diff specifically for bloat and organization, and all actionable findings must close. A net line-count reduction alone is not sufficient evidence.

Final review uses two independent subagents: one for execution/effect/stream/history correctness, one for boundaries/reductions/docs/tests. Supply the full candidate diff, acceptance matrix, red/green/review log, reduction findings/dispositions, native service/version evidence, crash/performance results and clean-checkout transcript. Fix findings and rerun affected checks. Mark the goal complete only after all in-scope rows, the full-diff cleanup gate and review findings close.

Deliver `COMPLETION_REPORT.md` with each A/C/pre-work ID, exact test names, commands and results, source revision/digest, reviewer disposition, measured envelope and explicit user exclusions. Include any actual proof limit; never turn unrun work into a completion claim.

## 14. Concrete file map and execution ordering

All existing anchors in this plan refer to the initial checkout; refresh line numbers with `rg -n` before editing after a previous phase moves code. New files begin at line 1. Paths in this document are rooted at `/Users/connorsanders/scratch/ai-kit`; use absolute clickable paths in user-facing reports. The complete lower-crate propagation maps live in their named appendices.

| File/anchor | Operation | Phase |
|---|---|---|
| `app-kit/src/lib.rs:48`, `:54`, `:60`, `:69`, `:92`, `:148` | Modify public generics/config and rustdoc; delete AppState/default lower imports; retain ownership hierarchy. | 0–1, 8 |
| `app-kit/src/app/mod.rs:20`, `:90`, `:633`, `:1114`, `:1160` | Extend finite inventories/revisions/capacity; preserve lower transfer/cleanup; supervised shutdown. | 1–2, 4, 6 |
| `app-kit/src/app/tasks.rs:27`, `:38`, `:77`, `:91` | Replace swallowed task errors with typed owned completion/reconciliation; bound and join. | 2, 5 |
| `app-kit/src/app/inventory.rs:1` | Validate revisioned inventories/current heads lazily and reconstruct active capacity without scanning all history into memory. | 6 |
| `app-kit/src/workspace/mod.rs:433`, `:722`, `:795`, `:1211`; `workspace/operations.rs:1` | Correct recorded compensation fences and whole-operation budgets; multi-agent materialization/reopen. | 2, 7 |
| `app-kit/src/storage/mod.rs:6`, `:31`, `:250`, `:342`, `:459`, `:549`, `:1074` | Delete concrete store construction and dispatch; extend same typed mutation/key/receipt owner for history/fork/delete. | 1, 6 |
| `app-kit/src/workflow/mod.rs:22`, `:32`, `:45`, `:59`, `:107`, `:136`, `:147` | Tight expanded core API, multiple controls, read-only graph/history and execution owners; supporting types stay in workflow. | 0, 4–6, 8 |
| `app-kit/src/workflow/definition/mod.rs:32`, `:45`, `:57`, `:130`, `:149`, `:210`, `:283`, `:396`, `:438`, `:692`, `:773`, `:1114` | v2 language, action bindings/state-input split, rurr expressions, graph/tags/composition, child/retry validation and bounds. | 3–4, 7 |
| `app-kit/src/workflow/execution/mod.rs:164`, `:340`, `:397`, `:551`, `:616`, `:692`, `:767`, `:782`, `:873` | Complete ready set, one barrier/transition kernel, rurr state/numeric/condition methods, routes and typed validation. | 3–5 |
| `app-kit/src/workflow/interaction/mod.rs:19`, `:43`, `:112`, `:139` | Whole-attempt deadline and exact multi-notification ownership; Agent prompt stream consumption. | 2, 5 |
| `app-kit/src/workflow/run/mod.rs:39`, `:42`, `:55`, `:68`, `:560`, `:751`, `:1175`, `:1641`, `:2028`, `:2305`, `:2505`, `:3087`, `:3753`, `:4620`, `:5009` | Remove short expiry/current+previous/single-control assumptions; shared driver, receipts, retry, children, history and recovery. | 2, 5–7 |
| `app-kit/src/workflow/run/actor.rs:1` | Extend existing unique driver/registry state for background/exclusive execution and supervised cleanup; no second registry. | 2, 5–6 |
| New `workflow/action/mod.rs`, `workflow/state/mod.rs`, `workflow/observation/mod.rs` | Only actual action/typing/codec/hook/tracking owners and passive contracts. Avoid a separate public AST. | 4, 8 |
| New private `workflow/execution/session.rs`, `workflow/run/checkpoint.rs` | Move existing connected ownership after deduplication; no facade-only helper files. Sync reuses the same caller-owned Execution and captured runtime handle; it adds no worker or command channel. | 5–6 |
| `app-kit/tests/{public_surface,workspace_contract,workflow_execution,workflow_live}.rs` | Tests first; remove private-name and short-expiry assertions; permanent regression/multi-agent proof. | all |
| New `app-kit/tests/` targets listed in §13, `tests/fixtures/rurr/`, example corpus | Complete observable, compile, source-derived parity and docs acceptance; no production fixtures. | all |
| `app-kit/Cargo.toml:13`, root `Cargo.toml:13`, `:19` | Only explicit existing-package futures-util/preserve_order use if required; review lock/feature unification. No new package/crate. | 0, 3–4 |
| `app-kit/scripts/check.sh:10`, `app-kit/scripts/verify_strict_live.sh:1`, `app-kit/strict-tests.env.example:1` | Register all tests and real multi-agent/stream/crash prerequisites; fail fast without secrets or skips. | all, 9 |
| `scripts/verify_workspace_strict.sh:1`, `scripts/check_docs_site.sh:1`, `docs/documentation-ledger.toml:54` | Keep all existing lanes; include finished guides/examples and locked clean-candidate mechanism. | 8–9 |
| `app-kit/docs/**`, `app-kit/examples/*.rs`, owning specs/root docs | Add/modify full inventory in §§4/12; delete contradictory teaching and duplicated ledgers, no placeholders. | 0, 8 |

The mandatory sequence is 0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9. Inside phase 1, provider/tool/storage proof can run as independent read/edit tasks, but Cargo remains serialized. Within later phases, schema/docs work and independent test families can run in parallel only with explicit file ownership. Workers are not alone in the codebase and must preserve others' edits. Each phase ends with independent review; no final-only review shortcut.

The plan permits implementation-level refinement when compile tests demonstrate a smaller honest design, provided all acceptance outcomes, boundaries and reductions remain satisfied and the owning ledger/docs/tests change together. It does not permit deleting a difficult core row, proliferating optional abstraction families or deferring a required phase to another goal.

## 15. Ready-to-execute goal prompt

OpenAI's [Follow a goal](https://learn.chatgpt.com/use-cases/follow-goals) recommends one durable objective, a verifiable stop condition, initial reference files, validation evidence and checkpoints. The [goal command reference](https://learn.chatgpt.com/docs/developer-commands?surface=cli#set-or-view-a-task-goal-with-goal) limits objectives to 4,000 characters. The prompt below is deliberately short and points to this detailed plan; it is not a goal-file payload.

```text
/goal Complete app-kit end to end according to /Users/connorsanders/scratch/ai-kit/app-kit/plans/2026-09-13/COMPLETION_PLAN.md and its linked acceptance matrix and appendices. Read them first and execute phases 0–9, including required lower-crate pre-work, all pinned-rurr core semantics, runtime fixes, real multi-agent workflows, documentation and release proof. Apply the plan's explicit contract amendments and reductions; preserve direct public-trait integration and tight crate boundaries. Server/UI/CLI and exporter products remain out of scope.

For each area, add/change tests first, record meaningful red evidence, implement and reduce, then obtain independent subagent code review and resolve its findings. Reuse existing owners before adding code. Keep a concise EXECUTION_LOG.md with phase, acceptance IDs, tests, reviews and blockers. Work autonomously through the whole plan; do not stop at partial milestones or weaken tests/scope.

Finish only when every in-scope acceptance row passes, all required integrations, docs/examples, crash/performance workloads, feature lanes, five strict-live verifiers, workspace wrapper and locked clean-checkout proof pass, and final independent reviews have no actionable findings. Record exact results and proof limits in COMPLETION_REPORT.md. Continue independent work if externally blocked; never mark incomplete or unrun work complete.
```
