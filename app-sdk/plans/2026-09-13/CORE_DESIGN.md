# Bounded app core extension design review — 2026-09-13

This is a proposed specification amendment, not a second public API ledger or implementation. The owning `app-sdk/specs/SPECIFICATIONS.md` must receive final signatures alongside tests-first contract changes and before production implementation. User instructions promote pinned rurr core behavior to the comparison authority; current app/root bans on app traits, streams and history therefore require explicit amendment. Existing crate boundaries still prohibit App→Provider/Tool coupling and lower-product reconstruction.

## Decision

Add independent app-owned static seams for **actions**, **state typing**, **lifecycle events**, **callable conditions**, and **child execution**, plus one narrowly process-global **field/tag serialization owner**. The main plan specifies the final condition and child-executor refinements. Preserve caller behavior types, use associated output types, and let callers compose heterogeneous behavior with exhaustive enums. Explicitly authorize private erased function handlers only inside serialization registration; no public erased backend, `Any`, global general-purpose registry, macro-generated trait twin, callback facade, or `AppExtensions` god trait. This exception preserves the pinned process-global serde behavior without broadening any lower SDK seam.

Reuse the existing workflow/run/definition/state owners and finite configuration inventory. Add an action binding collection parallel to the existing logical-agent bindings; this is a concrete App configuration responsibility, not a standalone extensibility registry product. Supporting declarations/DTOs belong under functional `workflow` modules; root reexports should remain only primary App construction and handles. Add no lower dependency.

This direction preserves core outcomes without copying rurr’s public Rust surface. The phase-0 amendments below explicitly price the required runtime/serialization exceptions. They are required work, not new exclusions. No exception changes lower SDK dispatchers or credential/persistence ownership.

## 1. Ordinary actions and partial results

Proposed narrow seam:

```rust
pub trait ActionBackend: Send + Sync + 'static {
    type Stream: Stream<Item = Result<ActionEvent, AppError>> + 'static;

    fn declaration(&self) -> &ActionDeclaration;
    fn start(&self, request: ActionRequest) -> Self::Stream;
}

pub enum ActionEvent {
    Partial { result: serde_json::Map<String, serde_json::Value> },
    Completed {
        result: serde_json::Map<String, serde_json::Value>,
        writes: Vec<StateChange>,
    },
}
```

ActionDeclaration is a passive validated metadata owner: exact name/binding key, declared reads/writes, required/optional inputs, bound/default input values, tags, source metadata. ActionRequest owns the immutable state snapshot, ephemeral runtime inputs (including undeclared keys, following the pinned input pass-through semantics) and passive app/workspace/run/checkpoint/node/parent context. It exposes no App, Agent, Provider, tool, StateStore, synchronization owner, or private record. Result is separate from writes: intermediate and final result payloads do not enter durable workflow state unless an explicit output/state projection requests that.

The runtime validates declaration metadata, reserved inputs, unknown/duplicate bindings and input precedence before action execution. Authored Action and custom Condition references require the exact caller revision alongside the logical key. This is an App-only deployment extension: no implicit latest/current/order-based selection or unique-key shorthand exists. Distinct revisions coexist; a missing pair rejects before App publication or callbacks, and changing a selected pair under the same definition revision conflicts. Retained definitions/runs preserve their original pairs through reopen while newer definitions select their own explicit revisions. The existing compiled definition and retained revision inventory own these facts, with no public pin DTO, binding wrapper or second registry. Explicit write operations are not restricted to declared write keys; declaration metadata does not hide other state values. Required inputs cannot be defaulted; runtime input cannot override bound input; runtime `__*` keys fail. Lower I/O can be performed by a caller Action that already owns a lower trait implementation—App does not accept or reconstruct lower configuration.

The authored WorkflowDefinition graph has no callable values and therefore leaves unresolved callable source None and declaration-only read/write/input lists empty. Registered receives the same graph DTO resolved against exact selected declarations, never source fabricated from the reference JSON. Omitted node tags inherit declaration tags; explicit arrays replace them, including [], preserving authored duplicates/order as pinned FunctionAction.with_tags does. The effective tags used by halt selection and registration must agree. Composition prefixes nodes/edges without changing callable pairs or overrides. Tests separately prove missing/invalid reference syntax, missing/duplicate pair preflight, reordered inventories, old/new revision coexistence, conflicting rebinding, unresolved tracking failure and resolved source/metadata/tag projection; metadata extraction never constructs an action or evaluates a condition.

One stream operation is sufficient for synchronous, async-terminal and incremental callables. A synchronous callable returns a lazy one-item iterator/stream; async-terminal callable returns a one-item future stream; incremental action returns many partials and one terminal. Run/step/iterate consume that same source through the same execution engine; no parallel unary and streaming implementation loops. In particular, `start` itself must perform no external work before first poll: otherwise an unpolled stream would violate lazy admission. Panic in construction and polling must map to the owning App failure with cleanup.

Use a concrete associated Stream rather than a public boxed erased stream constructor when available. It naturally allows caller-owned streams and avoids a second action-erasure boundary. The associated Stream intentionally has **no Send bound**: rurr’s primary LocalAction supports non-Send futures. Caller execution polls local streams on its owning task; background execution constructs, polls and drops them on the existing supervised worker. Only Send intent and portable results cross that worker boundary. Do not impose Send on the associated stream or manufacture Sendness with unsafe. Both caller and background execution remain required.

Sketch of control ownership, not final names:

```rust
impl<CS, A, X, T, H> WorkflowRun<CS, A, X, T, H> {
    pub async fn execution(&mut self) -> Result<Execution<'_, CS, A, X, T, H>, AppError>;
}

impl<CS, A, X, T, H> Execution<'_, CS, A, X, T, H> {
    pub async fn next(&mut self) -> Result<Option<StepStream<'_, CS, A, X, T, H>>, AppError>;
}
```

The mutable borrow gives one-session exclusivity; existing cloned handles additionally require an atomic durable driver reservation so a different handle cannot advance the same run. Background execution and explicit execution are mutually exclusive driver modes of the same run owner. Snapshot observers remain independent and read committed truth. A public `next_event` async method is often simpler than implementing `Stream` for a durable cursor because each poll may require async persistence/hook finalization. The raw Action source can still implement standard Stream.

Halt advancement follows the exact method mapping in SPECIFICATIONS.md and
WORKFLOW.md: next selects streaming iteration; step/finish/background select
ordinary iteration/run. Extend the existing boundary decision with a small
private lane distinction, not another execution engine. The method committing
the boundary fixes its decision; a later method cannot reinterpret a retained
halt. Session-local committed evidence is independent of durable historical
ordinal. `application/session.rs:169–210,335–374,489–514` and streaming
`step_lifecycle.rs:237–251,441–448` establish the two pinned orders. The existing
RunStatus carries the same halted node/optional result as the event, because
finish and background callers otherwise lose the pinned run-output distinction.
Keep the exact variant in the sole public ledger; add no wrapper or fabricated
pending ActionOutcome. Acceptance A14/A15 owns the full method/resume test table.

Pinned sources: `rurr-main/crates/rurr-core/src/action/api.rs:297-334` separates result/writes; `:454-558` unifies action/stream/update; `docs/compatibility/DIVERGENCES.md` §10 specifies local futures and stream-first dispatch; `docs/compatibility/SEMANTICS.md:104-110` reserves inputs.

## 2. Typed state

Use one static typing seam with an associated concrete Data; do not introduce `Box<dyn Any>` or make every state DTO generic over arbitrary provider-native objects.

```rust
pub trait StateTyping: Send + Sync + 'static {
    type Data;
    fn construct_data(&self, state: &WorkflowState) -> Result<Self::Data, AppError>;
    fn construct_state(&self, data: &Self::Data) -> Result<WorkflowState, AppError>;
    fn validate_pre(&self, state: &WorkflowState, mutation: &StateChange) -> Result<(), AppError>;
    fn validate_post(&self, before: &WorkflowState, after: &WorkflowState,
                     mutation: &StateChange) -> Result<(), AppError>;
    fn validate_state(&self, state: &WorkflowState) -> Result<(), AppError>;
    fn validate_roundtrip(&self, state: &WorkflowState) -> Result<(), AppError>;
}
```

Validation methods can have no-op defaults except roundtrip’s default must invoke construct_data then construct_state. Default raw JSON typing overrides roundtrip with the documented no-op fast path. Primitive mutation order is pre-validation → apply immutable operation → post-transition validation → full-state validation → roundtrip. Merge and subset construct a new snapshot and invoke only full-state validation → roundtrip, following pinned `state.rs:286–315`. Post-validation does not replace either later check. Rurr’s default roundtrip checks successful reconstruction, **not raw equality**; do not impose a new equality requirement silently.

App retains the typing owner and all state-producing operations route through its methods, including append/extend/increment/delete/wipe/merge/subset, child-state sanitization, fork overrides and resume. Decode persisted values, apply overrides and runtime metadata, then perform final typing validation. Whole-state JSON schema alone does not provide this seam.

State must preserve insertion order. Existing serde_json maps may default to sorted keys; check dependency feature unification before choosing representation. An ordered private vector-of-entries with explicit owner methods can preserve behavior without a new dependency, while serialization deliberately emits insertion order. Keep one owner rather than parallel ordered/raw state copies with unsynchronized mutation paths. This is observable parity, not formatting.

Pinned sources: `state/typing.rs:8-63`; `state/typing.rs:70-91` fixed validation order; `docs/compatibility/SEMANTICS.md:13-21`; `state.rs:290-312` ordered merge/subset. Type/codec owners must not be serialized; caller supplies them again on reopen, as with existing lower implementations.

## 3. Field and tagged serialization — required narrow global exception

Rurr explicitly makes field and tag registration process-global, not merely per-App: `state/serde_registry.rs:50-100`, `state.rs:316-365`, `DIVERGENCES.md` §8. Implement that behavior. Scoped codec injection is insufficient and is not part of the recommendation.

One narrowly scoped invariant-bearing owner is sufficient:

```rust
pub struct StateSerialization {
    // Private COW snapshot storage for field and tag handlers only.
}

impl StateSerialization {
    pub fn global() -> &'static Self;
    pub fn register_field<S, D, SE, DE>(
        &self, field: String, serialize: S, deserialize: D,
    ) -> Result<(), AppError>
    where
        S: Fn(&Value) -> Result<Value, SE> + Send + Sync + 'static,
        D: Fn(&Value) -> Result<Value, DE> + Send + Sync + 'static,
        SE: std::fmt::Display,
        DE: std::fmt::Display;

    pub fn register_tag<D, DE>(
        &self, tag: String, deserialize: D,
    ) -> Result<(), AppError>
    where
        D: Fn(&Value) -> Result<Value, DE> + Send + Sync + 'static,
        DE: std::fmt::Display;
}
```

The owner has real registry invariants and private synchronization; it is not a method-only utility namespace. Private function storage may use `Arc<dyn Fn(...)>` with its full concrete signature written directly, no item aliases. This is the single necessary new erasure exception, confined to scalar serialization transformations; it is not a backend extension ingress. No dynamic actions/hooks/typed-state/native payloads enter this registry. The owner removes a codec generic from App instead of proliferating additional generic owners.

At every encode/decode boundary take an immutable COW snapshot, release the registry lock, then invoke handlers. Registration replacement and cross-App visibility match pinned behavior; changes cannot alter an in-progress snapshot midway. Handler errors are deterministically wrapped with field/direction context while retaining the original message. A handled field serializer must return an object. Field decode takes precedence over tagged generic decode. Tagged decode occurs recursively where rurr generic serde does; unknown/malformed tag outcomes must be taken from pinned code, with red tests before implementation. No lock across callback invocation or destruction of replaced captures. Ordinary untagged container traversal is depth-bounded. Tagged handlers receive the entire undecoded object and their output is terminal; do not recursively dispatch that output or reject an identity handler as a cycle. This correction follows the pinned serde.rs implementation.

Resume ordering is decode persisted values → apply caller overrides → apply runtime metadata → final typing validation. Do not confuse runtime values and serialized checkpoint values. Every App observes global registrations, including Apps constructed before registration, at its next serialization boundary; include that cross-App test explicitly. Keep exact JSON/Burr tags where parity requires them without copying lower control-plane record formats.

Tests use unique test-owned field/tag identifiers to avoid process-global interference; no unrequested reset/unregister API solely for test isolation. Add tests for concurrent registration snapshot consistency, same-key replacement, captured-state handlers, ordinary handler error mapping, object-only encoded fields, field-versus-tag precedence, nested tags, missing registrations, and persistence/reopen. Test globals do not become a general production callback framework.

## 4. Lifecycle hooks

Use one static hook owner with two methods and one exhaustive event vocabulary. Sync and async lanes are both necessary for drop semantics; a single async-only callback loses rurr behavior.

```rust
pub trait LifecycleHooks: Send + Sync + 'static {
    fn observe(&self, event: &LifecycleEvent<'_>) -> Result<(), AppError>;
    fn observe_async<'a>(&'a self, event: &'a LifecycleEvent<'a>)
        -> impl Future<Output = Result<(), AppError>> + Send + 'a;
}
```

Both methods may default to success. Normal async execution calls sync then async in fixed order. Drop invokes only the synchronous completion/abort lane, wrapped against panics; explicit finish/abort runs the async lane. Event is a closed data-carrying enum covering app/child registration, execute-call begin/end, step begin/end, stream begin/item/end and action-span/attribute events. Existing phase/context/result data is borrowed in event variants rather than copied into twelve same-root context owner structs. Events expose committed versus staged/partial data explicitly; callback return cannot replace state or issue runtime commands.

A single H implementation can contain ordered concrete hook implementations or be caller-composed. App should not add `Vec<dyn Hook>`, per-event callback registration, a new exporter runtime, or external tracing clients. Built-in safe operational tracing remains metadata-only. Explicit caller hooks may receive requested content/state for custom logging; persistence of that content is caller policy, separate from default tracing.

Hook failures are functional: pre-hook failure prevents action, item-hook failure rolls back staged state, post-hook errors surface rather than being swallowed. `streaming/completion.rs:8-25` makes hook failure the surfaced rurr error even when a primary error already exists. Preserve the primary error as chained/structured evidence while presenting the hook error in rurr-compatible order; this avoids erasing the primary failure while matching observable precedence. Never rerun a committed action because a post-commit hook failed.

Pinned sources: `lifecycle/adapter.rs:52-185` sync phases/drop lane; `:188-194` async lane; `application/streaming/completion.rs:8-25` precedence. External exporters, tracing server/UI and telemetry integrations remain out of scope; core ordered lifecycle events are in scope.

## 5. Stream commit/drop/cancellation rules that must be tests first

Pinned `SEMANTICS.md:154-165` and `DIVERGENCES.md` §§1–4 require lazy, exclusive, transactional streaming.

- Unpolled action streams perform no work. Each poll advances only enough to produce the next partial. No eager collection, hidden background drain or unbounded channel.
- Partial result delivery never changes committed user state or committed action ordinal/position. Durable intent/control checkpoint sequences and control-only history may advance; action-history projection still shows the prior committed action.
- Terminal source result is **staged** until the engine validates writes, typing/codec invariants and commit authority; atomically commit final state/control/sequence, then publish the terminal output. Do not let the raw terminal ActionEvent bypass storage publication.
- Before commit admission, early drop, error, panic or explicit abort discards staged user state. After CS commit admission, native finalization retains authority; observation loss cannot force rollback. Exact receipt/head reconciliation determines the committed result or uncertainty and forbids later-action admission. A source that ends without terminal result rolls back successfully with no output (rurr step_core.rs:157-167,361-365); preserve its prior position for a later step, without synthesizing a failed action or an empty completed result.
- Source outcomes and App-owned effect uncertainty have distinct authority. Any explicit terminal Err(AppError) is Action failure, including Integration; captured construction/poll panic is Execution failure and an App activation deadline is Timeout failure. App also contains caller-source destructor panic before terminal publication, preserving any earlier primary failure. Notification-driven authority reads and their existing retries share the original sampled source deadline; cancelling that read observation never cancels an admitted publication or replaces an accepted control fence. EOF is successful no-output rollback, preserving the prior position for a later explicit step. None automatically retries. This follows `application/streaming/step_core.rs:157–167,245–258,358–365,464–489,505–531`; pre-commit abort/drop rollback follows `:656–678`. App-owned Agent-operation or admitted-CS-publication evidence survives source outcomes when acceptance remains ambiguous. A caller Action's polling or internal I/O alone cannot establish an App-owned Unknown attempt: the trait exposes no acceptance receipt for that work. Caller actions own internal I/O, cancellation, idempotency, external reconciliation and independently spawned tasks. State rollback does not undo their effects, and later explicit execution may repeat them. Explicit async abort cancels/reconciles App-owned effects under the owning deadline; drop can only signal/revoke synchronously. A caller-local non-Send source is dropped locally, not moved to Send AppTasks; only safe App-owned reconciliation/Unknown evidence transfers. Explicit finalization runs documented App/hook cleanup, not asynchronous destruction of arbitrary source internals.
- No async I/O, block_on, block_in_place or new runtime in Drop. `finish/get().await` explicitly drains and finalizes; `abort().await` explicitly runs async hooks/cleanup. Mark owning session/stream must_use.
- Existing WorkflowRun observer-handle drop remains noncancelling. Dropping an exclusive execution stream aborts only its uncommitted step. These are different objects/ownership promises; document both rather than silently reversing run-handle semantics.
- In a parallel ready set, stage sibling writes separately; decide whether the exclusive stream exposes one action or the whole barrier. For serial rurr-mapped graphs, one step equals one commit; for app-native broadcast, partial sibling results cannot leak into the committed barrier before all required siblings settle.

Required red tests: action construction/poll panic; duplicate settlement fencing without terminal-tail polling; successful no-output EOF rollback and later retry; drop after one partial; explicit abort; exact pre/stream-item/post event order; async hooks absent on drop and present on explicit finalize; post-hook failure retains primary evidence; timeout during hook/setup/commit; two execution sessions conflict; snapshot stays unchanged during partials; commit-receipt loss never reexecutes action; recorded App-owned I/O ambiguity survives drop; local Rc-across-await action runs inline and on the existing background worker, with construction/poll/Drop isolation proved in each lane.

## 6. Required phase-0 execution/parallelism amendments

1. **Synchronous execution:** use the main plan phase 4's caller-thread facade refinement. SyncExecution owns the existing Execution and captured runtime handle, blocking those same futures only at synchronous entrypoints. It creates no runtime, thread, channel or second engine. Construct/poll/drop local sources on the calling thread; reject entered Tokio runtime contexts before blocking. Caller-built Agents/StateStores keep their original runtimes alive and driven through shutdown. Drop only delegates synchronous generation revocation; App retains accepted publication/native/child cleanup. Preserve worker supervision, bounded commands, cancellation and joins for the actual background/child worker. Facade-specific worker-spawn/queue/join tests are superseded as inapplicable, not credited as passed. Test real local pending sources, sync-only hooks, mode propagation, panic/receipt ownership, foreign runtime-bound CS and real lower implementations. Noncooperative callbacks cannot be force-killed safely.
2. **Lane discrimination:** rurr rejects async-only actions/persisters from synchronous execution. An App action declaration must state supported execution modes; validate before execution/hook guards. Lower StateStore remains async-only at its approved public seam, so App does not expose or inspect a false native sync/async store identity. All App stores run through the owned async boundary; rurr's separate persistence implementation classes map to one App transactional trait, with identical operation outcomes and no native config leakage. Describe this precise API translation rather than claiming the Rust persistence APIs are identical.
3. **Local action futures:** implement inline and sync caller-local streams, with background streams constructed/polled/dropped on the existing worker. Only Send intents/owners/results cross its boundary; no associated Stream Send bound, generated Send trait twin or dyn action storage is needed.
4. **Custom child execution:** the main plan now selects ChildExecutor with a generic associated Execution future, ChildBatch<F> and opaque ChildCompletion. The complete admitted batch is passed once; callers choose polling order/concurrency, App validates exact completion membership and original-order reduction. Local futures remain on their owning caller or worker thread; only Send control data crosses the worker boundary. This narrow App-only GAT return exception avoids an erased/general runtime. Test real reversed-order child execution, local Rc futures, cancellation/drop, duplicate/missing completions and child initializer/persistence/observation policy. ChildBatch retains the existing ActionMode and exposes mode() with actual Synchronous/Asynchronous evidence, never Both. InlineExecutor returns this same concrete Future: asynchronous polling covers the whole admitted vector, awaits all results including errors, then collects in original input order; synchronous polling is sequential and stops at the first error. This follows pinned parallelism/executor.rs:89–110. Pipeline is synchronous-only, bypasses ChildExecutor, and uses sequential state handoff through the same kernel; asynchronous pipeline rejects before child admission/effects, following :119–136,149–169. No public mode wrapper, second batch future or duplicate trait is introduced. Tests prove mode on genuinely admitted batches, async sibling progress and draining after an early error, sync short circuit and zero custom-executor/child calls for async pipeline rejection. Final spelling is proven by compile tests, but the seam is selected rather than optional.
5. **Child identity collisions:** rurr DIVERGENCES §9 uses sha256(parent_app_id + ':' + task_key), deliberately reusing identity for equal task keys across different parent actions. App's action-scoped structural IDs must change to preserve the same equality/reuse relationships. Red tests use two different parent nodes sharing a task key and verify reuse/conflict/recovery semantics. Keep App IDs opaque and preserve scope boundaries; do not inject a node component to avoid difficult collision reconciliation. Exact bytes require pinned hash behavior only where caller observable IDs are a parity assertion; otherwise equality/aliasing must still match.
6. **Lifecycle failure precedence:** permit the hook error to be the surfaced failure while retaining the original failure as structured/chained evidence. This matches rurr `streaming/completion.rs:8-25` without erasing the primary cause. No best-effort swallowing or post-hook replay of committed actions.
7. **Dependency exception:** permit the existing workspace `futures-util` package as a direct App dependency if associated public Stream bounds/stream combinators require it. No new package/version. Prefer std and existing Tokio/serde machinery everywhere else.

These are concrete required amendments, not outstanding product choices. Phase 0 must include focused compile-design proofs for local/sync stream and child-executor types before freezing their final ledger signatures. A compile failure triggers redesign within this scope rather than quietly deleting a capability.

## 7. Required reductions and sequencing

1. Amend only necessary root/app trait/namespace, streaming/drop and parity rules. Keep lower eleven seams unchanged except separately audited Agent requirements. Authorize only the serialization-function erasure exception, the narrow app behavioral seams proven necessary, and the caller-thread synchronous facade and existing background worker boundaries described above. Delete contradictory current allowlist exclusions; do not retain alternate ledgers.
2. Add compile/behavior tests for foreign non-Clone action/typing/hook owners and captured-state global codec handlers first. No App-owned runtime implementation is required for each seam beyond raw/default concrete behavior with an honest purpose.
3. Extend existing immutable mutation/reduction owner with pre/post/full/roundtrip calls and serialization routing. Remove duplicate raw-state write/validation paths.
4. Extend the existing run state enum with exclusive stepping/streaming driver mode and staged outcome; do not create separate Started/Running/Completed task families or a second workflow engine. Extend the implemented `RunCell`/`RunDriver` with exclusive generation and cancellation; the new `Execution` retains its concrete pinned local source and session inputs. Reuse `ExecutionEngine::advance`, `publish_staged` and `route_committed`, exposing the existing Barrier receipt from `WorkflowRunRuntime::advance_checkpoint` instead of adding a second evaluator. The first vertical slice is prepared run → lazy local Action partial → Stage/Barrier → committed event → deferred Routing. Preserve the measured concrete future-boxing boundaries around recursive driver/control work.
5. Drive action unary completion and streaming through one source. Reuse committed transitions for lifecycle/history observations; do not parallel-write an independent tracking truth model. Share touched-key/order-preserving state-change collection on `StagedActivationRecord` for built-in mutations and caller writes, with one typing application per primitive. Before foreground publication is awaited, transfer only the existing Send mutation data to tracked run-operation ownership so observer drop cannot cancel an admitted commit. Local source drop revokes the exact exclusive generation; reconciliation must settle uncommitted staging before another generation can claim it. EOF/abort preserve resumable position, not permanent run cancellation. Background invokes this kernel on the existing supervised worker; sync invokes it on its caller thread; no source, typed Data or child future crosses the worker command boundary. Verify source finality, local Rc, two-handle exclusion, held commit receipts, method-specific halts and process loss separately before claiming all lanes complete.
6. Review each public addition by downstream use and each owner by its invariant. Reviewer must specifically reject a god extension trait, TypeMap/Any, erased action registry, serialization erasure outside the named narrow owner, two prompt/run engines, mutable AST exposure solely for convenience, and copied rurr machinery.
7. Each area is done only when newly failing behavioral/compile tests now pass, owning gates pass, and independent subagent code review has no unresolved material findings.

## Evidence/proof limits

Archive checksum was reverified: `app-sdk/vendor/rurr-main.zip`, SHA-256 `5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e`; commit `005bfe3ece631e64b06dac2d42ef84235f727a4f`. All archive content read only with unzip -p; no extraction/execution or mutation. Read prior `app-sdk/audits/2026-09-13/RURR_PARITY.md`, app AGENTS/spec allowlist, action, typing, serde, lifecycle, streaming and parallel executor members. This bounded review does not prove performance, runtime conformance or strict-live behavior and does not settle the root plan’s complete file map.

## Corrections integrated into the final main plan

- Public StateChange is a resolved passive operation/Value vocabulary reused from execution::StateChangeRecord. Private StateMutation/ValueExpr authoring syntax is never exported.
- ConditionBackend is additionally required by pinned action/condition.rs:323 and :429 for synchronous custom predicates with declared reads and zero action/sequence advancement. Concrete bindings live with definitions; no global/erased predicate registry.
- The five required App seams are ActionBackend, StateTyping, LifecycleHooks, ConditionBackend and ChildExecutor. The only new callable erasure is the narrowly scoped global serialization owner.
- Workflow/action revisions are pinned; global codec functions are sampled per operation. Serialized field/tag payloads carry any caller-defined codec version.
- Main plan phases 4–7 settle local-source Drop, commit-admission linearization, caller-thread sync ownership, background-worker supervision and child executor details; these replace the earlier ambiguous draft wording.
