# app-sdk documentation, specification, and proof audit

Scope: read-only audit of the current checkout on 2026-09-13. No Cargo commands were run by this subtask; validation results must come from the coordinating audit. This report distinguishes written requirements, repository observations, and proposed completion work. All paths are relative to `/Users/connorsanders/scratch/ai-kit` unless stated otherwise. Vendored/external parity verification is assigned to the other audit track; this document assesses the checked-in parity claims rather than treating them as verified external facts.

## Assessment

The crate has substantial implementation-oriented specifications and a meaningful behavior/recovery test suite. It does **not** have a complete downstream documentation set or sufficient evidence to claim its entire feature surface is proven. More fundamentally, the current public specification explicitly disallows several features included in the user's requested full rurr parity. Completing the implementation against the existing allowlist and meeting the newly requested parity objective are different acceptance criteria; the latter requires an explicit specification amendment and behavior matrix first.

The design correctly keeps app composition at ready `A: AgentBackend` and direct app `CS: StateStoreBackend` values, preserves associated child implementations, and provides per-node agent selection in the workflow schema. The missing proof is a real workflow that dispatches different nodes to different configured agents and verifies the resulting state transfer, recovery, and ownership.

## Inventory and existing strengths

Repository observation:

- `app-sdk/docs/` exists but has **zero files**. There is no `app-sdk/examples/` directory.
- `README.md` is 57 lines and contains one abbreviated workflow run function. `ARCHITECTURE.md` is 91 lines and describes implementation ownership. Ten specs exist: `SPECIFICATIONS`, `APP`, `WORKSPACE`, `STATE_STORE`, `OBSERVABILITY`, and five workflow specs including `RESEARCH` and `TESTING`.
- Public source exposes 20 types and 22 inherent methods under the root and `workflow`, matching the declared target counts. Every inspected public item has at least terse rustdoc. The entire source tree has **one Rust example**, the crate-root `no_run` example at `src/lib.rs:10`; individual public methods/types have no executable examples, error guidance, or cancellation sections.
- There are **43 integration tests**: 3 in `tests/public_surface.rs`, 5 in `workspace_contract.rs`, 25 in `workflow_execution.rs`, and 10 in `workflow_live.rs`. Test counts indicate scope, not completeness or a passing result.
- Deterministic tests do cover routing order, integer precision above JavaScript's safe integer range, rejection of concurrent writes, bounded maps, conditional/cyclic joins, malformed definitions, admission/claim races, observer drop, stale fences, storage retry ownership, shutdown escalation, and cleanup.
- Live source uses a real OpenCode process and real Ollama and independent Agent/App Redb domains. It exercises notifications, cancellation during failure, interrupted notification truth, obsolete epochs, materialization recovery, multi-agent replacement compensation/cleanup, child/map crash recovery, and shutdown recovery. Forwarding wrappers gate timing around real calls and return the original results; inspected wrappers do not manufacture provider results (`tests/live_support/mod.rs:382-417`). These observations do not establish successful execution in this audit.
- `scripts/check.sh:8-19` runs formatting, all-target check, three non-live targets, doctests, clippy with warnings denied, warning-denied rustdoc, alias and path checks. `scripts/verify_strict_live.sh:9-22` requires a populated environment, prechecks real services, runs the non-live gate, then executes the live target without a skip path. `scripts/precheck_strict_live.sh:9-183` validates commands, exact lower version, selected model, service health and isolated configuration.
- The workspace docs gate already builds app rustdoc and runs app doctests (`scripts/check_docs_site.sh:30-31,71-72`). The static site includes an app link (`docs/rustdoc-index.html:103`). This is a reference-doc build gate, not a guide-completeness gate.

## Findings

### D1 — Full rurr parity conflicts with the current frozen product scope

Classification: **Normative app rule + Unresolved gap.** Priority: **P0 specification decision before implementation**.

`specs/SPECIFICATIONS.md:411-423` deliberately deletes public graph AST, scheduler/concurrency/timeout/retention tuning, fork/lineage/checkpoint selection, event history/receivers, and public timer/retry/child waits. `specs/workflows/RESEARCH.md:69-75` explicitly says there is no iterate, fork, stream, tracking, or scheduler API and calls those absences intentional. `AGENTS.md:17-26` prohibits an unlisted public item and repeats these restrictions.

The existing research table is not a comprehensive parity acceptance matrix. It maps halt-before/halt-after to interactions/cancel (`RESEARCH.md:56`), and tracking to current snapshots/private checkpoints (`RESEARCH.md:61`), without establishing that these are observably equivalent. A current snapshot does not by itself let a caller pause before an arbitrary action, consume incremental progress, or inspect arbitrary retained history. The rurr audit must classify each capability as equivalent, partial, missing, or intentionally divergent with direct archive evidence. API copying, new backend traits, and lower-crate coupling are not necessary to perform that comparison.

Recommended amendment: put a complete feature/semantic parity matrix under the owning app specs with a pinned rurr member per row, app outcome, deliberate divergence decision, owning implementation, test, and release criterion. Change `SPECIFICATIONS.md`, focused specs and app/root guidance only for approved app-owned additions. Preserve the lower-crate boundaries. Stop describing the old reduced surface as full functional parity.

### D2 — No downstream docs folder, practical authoring reference, or runnable multi-agent example

Classification: **Repository observation + Unresolved gap.** Priority: **P1**.

An empty `docs/` directory and the two small root documents do not satisfy the requested completed docs folder. The README example takes already-built `AppConfig`, `WorkspaceConfig`, and `WorkflowId`, so a new caller cannot derive how to construct the Agent inventory, write a valid definition, enable the selected durable backend, reopen the application, or handle `Waiting`/`Unknown` without searching implementation tests and normative specs (`README.md:7-29`). It calls `wait()` once then `RejectActive`, which only completes naturally when that observation is terminal; actionable snapshots require a caller loop that the guide does not show.

There is no tutorial showing two different agents assigned to nodes, state passed from one agent to the next, or heterogeneous matching Agent/Workspace/Conversation enums. The only crate-root Rust example opens and shuts down an App without creating a workspace/run (`src/lib.rs:10-22`). The README links directly to the exact allowlist and test spec (`README.md:48-57`), exposing implementer material as the only detailed help.

Recommended completion: create the docs tree below, add an end-to-end executable pure-state quickstart, a ready-Agent two-node example, and durable interaction/recovery examples. Link those pages from the README and rustdoc. Keep `SPECIFICATIONS.md` the sole exact signature ledger; guides should teach workflows and link to owning contracts rather than becoming another signature inventory. Update `docs/documentation-ledger.toml:54-68` to include the new guide owners.

### D3 — Public rustdoc is present but too shallow for a production workflow API

Classification: **Repository observation + App-native inference.** Priority: **P1/P2**.

All 22 method docs are essentially one sentence (`src/lib.rs:148-246`, `src/workflow/mod.rs:134-209`). Critical contracts are available only deep in specs: checkpoint fencing, observer cancellation versus work cancellation, App open ownership transfer, timeout semantics, monotonic shutdown escalation, list cursor behavior, unknown-effect retry safety, root-drop revocation, and short retention. `WorkflowDefinition::parse` does not link a full syntax reference or supply a valid definition; `wait` does not explain repeated calls and actionable versus terminal results; `resolve` does not show extracting the exact sequence and checkpoint.

The available `rust-public-api-docs` skill also requires a doctestable example for each public struct and public method, whereas currently only the crate-level example exists. This is a documentation-quality gap even if `-D missing-docs` succeeds: that lint checks presence, not usefulness.

Add examples on public construction/runtime operations and meaningful data docs on output/status types. Include narrow Errors, concurrency/fencing, and cancellation/lifetime explanations where relevant. Use executable doctests for pure parsing/state workflows; use `no_run` only for lower-service examples. Verify guide Rust snippets and JSON examples, not just the sole source example.

### D4 — Multi-agent execution and much of the DSL have no direct behavior evidence

Classification: **Repository observation + Unresolved gap.** Priority: **P1**.

`specs/SPECIFICATIONS.md:364` declares the node's logical `agent` key, and `specs/WORKSPACE.md:21-26` materializes all configured agents. Yet the only two-agent live configuration has `Vec::new()` workflows (`tests/workflow_live.rs:185-201`) and is used for replacement compensation and failed-open cleanup. Real live agent nodes select `opencode`; the deterministic agent fixture focuses on stream failure. There is no test that runs agent A on node one and agent B on node two, routes one result into the second prompt, reopens those identities, and verifies both agents are shut down exactly once.

Search and inspection of all app test sources found no `append_array`, `merge_object`, `max_attempts`, or `structured_schema` fixtures. Runtime reducers `collect`, `sum_integer`, and `reject_concurrent` have examples; the other two reducers do not. The retry control tests prove storage-scheduler retries and caller-approved unknown retries, not the declared workflow `Immediate`/`Fixed`/`Exponential` policies. The structured target parser rejection at `workflow_execution.rs:1561` is not successful structured-output execution. Most declared expression/condition variants likewise have no direct fixture. There is no tracing capture test. These are missing evidence, not a finding that each operation is absent or broken.

Add a contract-to-test matrix and focused public tests for all operators and reducers, invalid operands/overflow, required-key removal, duplicate JSON keys, depth/size/collection limits, route validation, empty-map behavior, join failure precedence, agent structured output, and retry timing/recovery. Add a real two-agent workflow and at least one mixed supplied-backend journey once supported by the chosen environment. Direct custom Agent/StateStore tests establish static composition; service-specific claims need their owning strict-live lane.

### D5 — The authoring schema is described by names more than complete semantics

Classification: **Normative app rule + Unresolved gap.** Priority: **P1 specification extension**.

The schema section gives a useful root/step inventory (`SPECIFICATIONS.md:317-409`) but does not fully document all expression and condition payload fields or edge semantics. For example `length`, `in`, `contains`, `not`, `all`, and `any` are only named at lines 386-391; source DTOs supply their fields (`src/workflow/definition/mod.rs:149-267`). Reducers are named at lines 380-382, while `WORKFLOW.md:105-118` does not define base-value participation, multiple same-node writes, removal interactions, object-key collision behavior, integer ranges/overflow, empty contribution behavior, or exact invalid-type outcomes.

There is a concrete schema ambiguity: `SPECIFICATIONS.md:374` says Sleep has a message, then lines 376-378 state that “Prompts and messages” are arrays of typed parts. Source `Step::Sleep` requires `message: String` (`definition/mod.rs:96-99`), and tests use strings. Explicitly distinguish agent prompt/input messages from sleep messages.

Other missing decisions/documentation: default retry/deadline semantics, whether attempts includes the initial attempt, finite cycle bound behavior, number versus string comparisons, missing path versus explicit null, JSON Pointer escaping, how strings/objects/arrays interpolate, exact map output ordering and duplicate targets, join selection in cycles/multiple branches, and JSON Schema dialect/ref-resolution policy. Some behavior may already be implemented; the spec should bind its intended semantics so implementation/tests can be audited.

Recommended deliverable: one complete human-maintained JSON schema file plus a readable language reference, with valid and invalid examples. This adds an authoring artifact, not a public Rust AST. Define semantics in focused specs and test schema/parser agreement.

### D6 — Production limits and retention policy are not documented enough to operate safely

Classification: **Normative app rule + Repository observation + Unresolved gap.** Priority: **P1**.

`specs/APP.md:109-115` promises finite, **documented** private constants, but docs list categories without values. Current source includes 256 agents, 1,024 workflows, 10,000 workspaces, 100,000 runs, a 1,000 item page limit, 1 MiB definition/runtime bounds, 2 MiB complete definition inventory, 1,024 nodes, 64 nested levels, 4,096 map items, and 65,536 prompt-text bytes (`src/app/mod.rs:20-25`, `workspace/mod.rs:29-32`, `workflow/definition/mod.rs:15-23`, `workflow/execution/mod.rs:13-16`, `workflow/run/mod.rs:33-39`). Admission versus active-work capacity and their units require clear ownership/behavior definitions, not a number dump.

Most consequentially, `TERMINAL_RETENTION_MILLIS` is **1,000 ms** and terminalization calculates `now + 1,000` (`workflow/run/mod.rs:39,4620-4624`). Specs merely require a sampled deletion deadline (`CHECKPOINTS.md:105-117`); README, public rustdoc, and user-facing guides do not tell a caller that terminal runs can disappear after roughly one second once correctness guards permit deletion. A deterministic test deliberately waits for this deletion after reopen (`workspace_contract.rs:165-178`). Current snapshots/private last predecessor therefore do not provide durable audit history.

Define the expected completed-run observation/retention contract, whether consumers must export results, whether retention is configurable under the revised parity scope, and when lookup becomes NotFound. Add limit tables with error outcomes, capacity reconstruction, backpressure/fairness, and boundary tests. Do not declare the 100,000-run bound performance-tested without workload measurements.

### D7 — Observability spec is stronger than the implemented/documented evidence

Classification: **Normative app rule + Repository observation.** Priority: **P1/P2**.

`specs/OBSERVABILITY.md:21-29` lists seven spans (`app.open`, `app.operation`, activate, superstep, attempt, checkpoint, interaction), private closed vocabulary enums at lines 10-19, and poll-scoped Instrument usage at lines 55-56. Current app source has info/warn event calls in app/run owners but no span creation or Instrument use. Tests contain no Subscriber/tracing capture. The underlying implementation audit should confirm the full gap and committed-transition ordering; absence of a public exporter is intentional and correct.

Finish the promised metadata spans/typed classifications or amend the spec if a smaller contract is intentional. Add public-behavior capture tests for correlation, committed ordering, recovery repeat semantics, and exclusion of state/prompts/lower ids/raw errors. Document subscriber setup in the embedding host, keeping exporter dependencies and content capture outside app-sdk.

### D8 — Static proof overstates what it enforces and still freezes private spelling

Classification: **Normative app rule + Repository observation.** Priority: **P2**.

`specs/workflows/TESTING.md:3-4,21-22` rejects private-name/placement phrase tests. `tests/public_surface.rs:590-591` nevertheless asserts the literal private `enum AppStoreInner<CS: StateStoreBackend>` and exact `type Transaction = AppStoreTransaction<CS::Transaction>` spelling. These should be behavior/associated-type composition proofs rather than private naming gates.

The exact-public inventory scanner reads only `src/lib.rs` and `src/workflow/mod.rs` (`public_surface.rs:79-83,111-170`). A future public inherent method implemented on a public type from a private module can escape that method allowlist even though callers can use it. The suite contains positive compile-shaped functions and source privacy checks, but no downstream compile-fail target/probe (`compile_fail`, `rustc`, `trybuild` do not occur in app test source). It does not fully prove rejection of private construction, unexpected exports, accidental traits/auto-trait regressions, or associated-child mismatch solely by counting declarations.

Retain narrow prohibited-construct checks; remove private-name locks; add consumer compilation cases proving the public contracts and rejection cases without introducing prohibited public conformance APIs, macros, or dependencies. Apply surface checking across all implementations of exported types and keep AST/private module organization flexible.

## Specifications that need extension, in priority order

1. **Parity/product acceptance** — amend `SPECIFICATIONS.md` and `workflows/RESEARCH.md` with a complete pinned-rurr matrix and measurable semantics for each accepted capability. Distinguish app-native stronger guarantees from equivalence and missing features.
2. **Workflow language** — extend `WORKFLOW.md` and the schema section with exact payload grammar and operator/reducer/route/join/map semantics, examples, defaults, and error cases. Resolve the Sleep message discrepancy.
3. **Multi-agent ownership and assignment** — `APP.md`, `WORKSPACE.md`, and `WORKFLOW.md` should explicitly describe per-node key selection, same-state transfer, fresh conversation isolation, provider/tool/knowledge ownership, heterogeneous caller composition, and failures when one lower implementation rejects materialization. This should not add provider or tool dependencies.
4. **Operations and durability** — extend `APP.md`, `CHECKPOINTS.md`, and `STATE_STORE.md` with actual bounds, terminal retention, result observation guarantees, durable domain ownership, failure/recovery runbooks, supported generation/host model, and a deployment/revision strategy. The immutable exact agent/workflow inventories have operational consequences when shipping changed workflow definitions (`APP.md:64-65,99-104`; `WORKFLOW.md:73-77`). Specify the supported change path; do not invent a migration or distributed scheduler without an approved product decision.
5. **Execution control** — formalize requested halt/step/stream/history/fork semantics only if the revised parity scope requires them. Separate halting before a side effect from cancellation after dispatch and distinguish read-only history from restart/branch mutation.
6. **Observability** — bind the actual span/event fields and semantics, demonstrate redaction and correlation, and state which events may repeat or be lost after recovery. Keep tracing outside durable authority.
7. **Proof and docs acceptance** — extend `TESTING.md` with feature-by-feature acceptance rows, strict-live backend composition coverage, failure injection boundary evidence, deterministic numeric/DSL coverage, and docs/example checks. State which required invariant each test proves rather than inferring completeness from three journey headings or a passing smoke test.

## Recommended completed docs folder

Keep the exact API inventory in `specs/SPECIFICATIONS.md`. A useful downstream docs tree is:

| Page/artifact | Required content |
|---|---|
| `docs/README.md` | Guide index, concept map, reading order, supported status and verified feature matrix links. |
| `docs/QUICKSTART.md` | Runnable pure-state App → Workspace → Workflow → WorkflowRun example; one ready-Agent example; explicit shutdown; required Cargo backend selection. |
| `docs/WORKFLOW_LANGUAGE.md` | Full JSON grammar, all 8 step kinds, expressions/conditions, prompt parts, routing, reducers, cycles, limits, valid/invalid examples. |
| `docs/schema/workflow-v1.schema.json` | Hand-maintained machine-readable authoring schema aligned with parser acceptance; no public Rust AST or generated code. |
| `docs/MULTI_AGENT_WORKFLOWS.md` | Two agents on different nodes, sequential state handoff, parallel branch assignment, child-map selection; homogeneous generic inventory and caller exhaustive-enum recipe; construction/knowledge/tool ownership. |
| `docs/STATE_AND_PARALLELISM.md` | Immutable base snapshot, canonical reduction, join-all/join-first, child and map ordering, required keys, mutation failures and atomic barrier examples. |
| `docs/INTERACTIONS_AND_CONTROLS.md` | Wait loop, exact input/notification resolutions, stale checkpoints, unknown accept/fail/proven-safe retry, explicit cancellation and safe UI behavior. |
| `docs/PERSISTENCE_AND_RECOVERY.md` | Separate Agent/App stores, caller lower create/reopen, durable App reopen, identities/revisions, crash uncertainty, terminal retention, supported upgrade/deployment procedure. |
| `docs/LIFECYCLE_AND_OPERATIONS.md` | Workspace replacement/compensation/deletion, shutdown modes, observer timeout/drop, root drop, supported process/generation model, limits, capacity/backpressure and cleanup. |
| `docs/OBSERVABILITY.md` | Host subscriber setup, metadata correlation, allowed fields, repeat/loss semantics, and troubleshooting without copying content into logs. |
| `docs/ERRORS_AND_TROUBLESHOOTING.md` | Operation error versus durable run failure/status; InvalidInput/Conflict/Busy/Closed/Timeout/Integration recovery; missing services, stuck Unknown, stale revisions, expired runs. |
| `docs/STRICT_TESTING.md` | Exact non-live/live/workspace commands, owned services/version/model requirements, safe environment setup, result-evidence matrix, blockers, no-skip policy. |
| `docs/RURR_PARITY.md` | Reader-facing supported/partial/missing/different capability matrix with pinned source links and test links, derived from the amended owning spec. |
| `docs/examples/*.json` | Reusable small definitions for linear state, two-agent handoff, conditional loop, input, structured output, child, bounded map, joins, retry/sleep; parse and execute them in public tests. |

Docs acceptance should include: every guide linked from the docs index; README and rustdoc navigation; markdown link checking; every example parsed; non-live examples executed; live examples compiled and linked to successful strict proof; every public operation documented with an example or a direct specific guide section; a truthful feature matrix; no unsupported feature claims; and the workspace documentation ledger updated. A passing missing-docs lint alone is insufficient.

## Suggested implementation sequence

1. Finish the pinned-rurr behavior audit and amend parity/product specifications, preserving the existing loose coupling and static child contracts.
2. Fix concrete implementation defects found by the runtime audit, then add precise public regression tests before changing production.
3. Prove two-agent node assignment, missing DSL/reducer/retry/structured-output cases, unknown recovery, and promised observability; distinguish direct composition proof from real-service proof.
4. Decide and implement the approved retention/history/execution-control scope and write associated lifecycle/recovery specifications.
5. Complete the docs tree and executable examples against the final supported behavior, deepen rustdoc, and add docs-quality checks.
6. Run the exact crate validation ladder sequentially, then required full workspace/strict/clean-checkout proof. Record actual results and unmet prerequisites; never convert static source observations into production or live completion claims.
