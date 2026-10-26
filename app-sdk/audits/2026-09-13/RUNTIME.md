# app-sdk runtime audit — 2026-09-13

Scope: execution, reducers/routing/joins, external attempts and notifications, cancellation, child/map admission/recovery, checkpoint storage/retention, and tracked-task behavior. Read root and app AGENTS, the app public ledger, WORKFLOW/CHECKPOINTS/INTERACTIONS/STATE_STORE/TESTING, and the lower Agent API. Applied app-sdk-research and Rust core/async/testing skills. This sub-audit does not independently claim rurr parity or external framework facts; the parent audit owns those evidence tracks and all Cargo execution. No production or historical `tmp/**` files edited.

## Result

The implementation is substantial, but runtime completion cannot be claimed. Three concrete high-priority correctness defects are visible in implementation: multiple notifications in one logical activation are unsupported, configured node deadlines omit several lower calls, and panics silently orphan the run driver. There is also a material scheduling/specification conflict: ordinary sibling effects execute one at a time and a parked input/timer/attempt prevents every remaining sibling from being considered. The latter is not automatically a bug merely because execution is serial; it is a conflict requiring an explicit product and normative decision.

## Findings

### R1 — P1: second actionable notification within one prompt cannot be durably handled

Classification: **Repository observation** + **Normative app rule**. Source-proven; no executable multi-notification repro run by this sub-audit.

- `/Users/connorsanders/scratch/ai-kit/app-sdk/src/workflow/interaction/mod.rs:19` stores one `OnceLock<Arc<ConversationNotification>>` for the entire live attempt.
- `interaction/mod.rs:112`–129 accepts only the first notification or a redelivery of that exact ID; a later ID returns `Conflict`.
- `/Users/connorsanders/scratch/ai-kit/app-sdk/src/workflow/run/mod.rs:906`–908 marks a successful first notification resolution Applied and returns the attempt to Dispatched, retaining the same live interaction and attempt sequence.
- `run/mod.rs:2305`–2312 installs the next notification in that same owner and immediately returns from `dispatch_agent` if installation fails. The second notification is never durably projected, and the task stops polling the in-flight prompt.
- Independently, `run/mod.rs:5009`–5038 permits one resolution cell to progress Idle→Intended→Applied, then forbids resetting Applied. It has no notification-target-keyed successor cell. Merely replacing the OnceLock would not fully fix this.

Trigger: one Custom/OpenCode/Codex or caller Agent prompt asks for approval/input A, receives a successful response, continues its agent loop, and later asks for approval/input B. Agent behavior legitimately supports multiple sequential tool interactions during a prompt. The app cannot service B and may remain Running or subsequently converge to Unknown depending on pending scheduler causes.

Normative basis: `app-sdk/specs/workflows/INTERACTIONS.md:9`–35 requires exact lower notifications, durable at-least-once ownership, and exact-target settlement. `WORKFLOW.md:130`–136 explicitly calls for a target-keyed reconciliation cell and independent effect evidence. The existing single-notification tests do not exercise this transition.

Fix: retain the logical activation/conversation, but make current notification ownership replaceable only after durable settlement; give each distinct actionable notification an exact durable target/evidence cell and safe public sequence semantics. Preserve prior applied evidence without using it as the current notification's state. Add deterministic two-notification and redelivery-after-settlement tests, plus a real sequential-approval journey.

### R2 — P1: configured activation deadlines do not bound conversation preparation or lower effect settlement

Classification: **Repository observation** + **Normative app rule**. Reproduced during the consolidated audit: the 25 ms deadline probe still observed Running after 300 ms and failed its assertion.

- `app-sdk/src/workflow/interaction/mod.rs:43`–67 directly awaits `AgentBackend::get_workspace`, `WorkspaceBackend::get_conversation`, and `WorkspaceBackend::new_conversation` without a timeout or cancellation/deadline select.
- `app-sdk/src/workflow/run/mod.rs:2028` directly awaits the complete preparation operation. The node deadline was sampled earlier; the prompt-loop deadline select is installed only after preparation finishes (`run/mod.rs:2170` and following).
- `interaction/mod.rs:139`–148 likewise directly awaits `resolve_notification` and `abort`; recovery `run/mod.rs:1175`–1197 includes direct unbounded lower lookup and abort awaits.

Trigger: a legal caller Agent's `new_conversation` future never resolves (or a native transport hangs). A 25ms node deadline leaves the run Running indefinitely, with the sole driver blocked inside preparation. The prepared test observes the exact lower entry and checks status after 300ms. At this boundary timeout must preserve possible conversation-creation uncertainty; it must not synthesize a definitive rejection.

Normative basis: `app-sdk/specs/SPECIFICATIONS.md:349`–350 says a positive `deadline_ms` bounds one activation; root AGENTS requires bounded external waits. INTERACTIONS.md's bounded ownership requires every parked attempt to remain accounted for through settlement.

Fix: carry a sampled durable activation deadline through every lower operation, and bound preparation, resolve, abort, and recovery calls. Distinguish read-only acquisition failure from potentially accepted construction/dispatch. Deadline expiry must publish a durable Timeout or truthful Unknown and preserve runtime ownership where lower work can outlive a dropped future. Add hang probes at each public lower boundary and cancellation during those waits.

### R3 — P1: task panics bypass durable failure/unknown projection and leave the driver marked active

Classification: **Repository observation** + **Normative app rule**. Reproduced during the consolidated audit: the lower panic occurred, then the probe observed Running after 300 ms and failed its durable-outcome assertion.

- `app-sdk/src/app/tasks.rs:38` silently drains all `JoinSet` results, including panic errors.
- `app-sdk/src/app/tasks.rs:40`–44 runs `future.await` without panic capture; the completion notification is also skipped on panic.
- `app-sdk/src/workflow/run/mod.rs:572`–583 calls `drive().await` and only then `complete_drive()`. A panic skips the release/reset path and leaves `RunSchedule` in Driving state. Later causes can only queue behind a nonexistent driver.
- No panic capture or JoinError classification exists anywhere in `app-sdk/src` (read-only `rg` for `catch_unwind|JoinError|is_panic` returned no matches).

Trigger: a caller `WorkspaceBackend::new_conversation` or `ConversationBackend::prompt` panics. The Tokio task dies, while the checkpoint remains Intent/Dispatched and RunStatus remains Running beyond its deadline. The public custom-backend probe panics during new_conversation after signalling entry; after 300ms it requires that the run has projected durable failure or Unknown.

Normative basis: `app-sdk/specs/workflows/WORKFLOW.md:165`–168 explicitly requires panic before an effect to become Execution and panic after possible dispatch to become Unknown. This behavior is not implemented.

Fix: supervise task exit using run/attempt ownership and last committed dispatch evidence. Ensure scheduling authority is released on every exit, classify panic conservatively, commit the appropriate durable outcome, and account for cleanup. A process configured `panic=abort` cannot offer unwind recovery and should be an explicit deployment constraint. Add panic-before-dispatch, panic-after-dispatch, and panic-during-cleanup tests.

### R4 — P2 specification/functional gap: one parked sibling blocks the remaining ready set

Classification: **Repository observation**, **Normative contradiction**, **App-native inference**. Do not present this as “parallelism absent”: map children do run as distinct bounded runs, and serial scheduling alone can preserve deterministic semantics.

- `app-sdk/src/workflow/execution/mod.rs:230`–255 returns at the first canonical Input/Agent activation; equivalent early-return behavior exists for Child/Map/Sleep.
- `app-sdk/src/workflow/run/mod.rs:68`–80 permits only one optional ControlRecord per active run.
- `run/mod.rs:1641`–1680 handles an existing child/input/sleep/retry/notification by waiting/reconciling and returning before examining the remaining frontier.

Impact: broadcasting two input nodes exposes the first only; the second cannot be presented until the first settles. Two sibling agent nodes cannot overlap lower work. A sleeping or interactive branch prevents independent sibling work from reaching its own parked/staged state. JoinFirst cannot act like a concurrent first-success race of ordinary agent siblings. This affects responsiveness and can prevent application-level dependencies where one branch is needed to unblock another.

Supporting normative demand: `WORKFLOW.md:83`–94 requires the complete ready set and every ready sibling staged, parked, failed, or unknown before the barrier; lines99–102 require surviving controls/staged siblings to remain intact. `CHECKPOINTS.md:29` refers to sibling tasks.

Counter-evidence/contradiction: `CHECKPOINTS.md:23`–27 explicitly mandates one optional control with exactly one input/attempt/sleep/child state. The implementation follows this narrower shape. A functional-parity requirement for concurrent/promptly parked siblings must first reconcile the specs; do not simply “fix” it by adding unapproved public AST, worker, event, or control surfaces.

Recommendation: specify ordinary sibling progress/parallelism, canonical winner and loser truth for JoinFirst, readiness vs effect capacity, and how a single public Waiting projection selects among private simultaneous interactions. Decide whether serial effect execution is intentional. Then add a synchronized public Agent test with two distinct logical node assignments and a first branch deliberately blocked while the second must enter.

## Additional spec weaknesses / completion work

1. **Retention usability contract is missing.** Runtime hardcodes `TERMINAL_RETENTION_MILLIS = 1_000` at `app-sdk/src/workflow/run/mod.rs:39`. `CHECKPOINTS.md:112`–117 defines correctness conditions but no minimum observation period or durable result availability SLA; `SPECIFICATIONS.md` deliberately removes public retention controls. As implemented a caller returning a second later may get NotFound for a completed run. This may obey the narrow spec, but requires an explicit production decision, docs, delayed-observer tests, and a usable result-retention story.
2. **Notification multiplicity and settlement replay are underdefined.** INTERACTIONS refers to existing identical/conflicting settlements but does not explicitly define a new notification within the same activation, sequence assignment, replay after successful settlement, or a bounded number of notification cells. `resolve_owned` (`run/mod.rs:751`–773) rejects stale checkpoint or absent current control and keeps no target/result replay object. Specify what callers do after losing their own successful resolve response; a live-but-unknown caller receipt is distinct from loss of the lower notification receipt.
3. **Retry applicability needs an exact matrix.** Every node accepts retry policy, but `retry_delay_millis` is only used in `retry_safe_failure`, currently invoked before prompt preparation when workspace lookup fails (`run/mod.rs:2002`–2017,2505+). State/input/sleep/child/map policies are accepted and never used. Caller `Resolution::Retry` increments attempt count without consulting the declared max_attempts (`run/mod.rs:927`–954). These may be defensible distinct policies, but the schema docs do not say so. Specify which failures/actions each retry policy covers and whether manual proven-safe retry consumes limits.
4. **Exact lower reconciliation contract needs a precise mapping.** App specs speak of authoritative lower truth and dedicated reconciliation actions, while the lower public Agent API offers `info`, `abort`, `notification_stream`, `resolve_notification`, and no completed-prompt response lookup or idempotent caller-selected conversation creation key. Recovery accordingly projects unknown for lost conversation creation/dispatch, and notification recovery depends on redelivery. Document exactly what is provable from each public lower method and what can remain unknown indefinitely; do not invent private lower access to close this gap.
5. **Reducer edge semantics need examples.** `execution/mod.rs:617`+ implements Collect as only this barrier's values; AppendArray and SumInteger include prior state; MergeObject rejects keys that overlap either prior state or sibling contributions. The prose only names reducers. Specify these differences, repeated assignments/removals within a state node, empty-map reduction, numeric width/overflow, and type failures.
6. **Capacity/resource and failure-time contract needs operational bounds.** Specs promise finiteness but expose no documented usable defaults or whether limits apply per App/run/workspace, which errors retry, or whether stored receipts/terminal results consume capacity. Publish a compact operational limits document without adding public tuning surfaces unless separately approved.

## Positive implementation evidence

- Direct generic `A::Workspace` and its associated Conversation remain concrete; no new lower backend erasure is needed by the identified fixes (`workflow/interaction/mod.rs:14`–21).
- Ordered BTree frontier/staged contribution processing, immutable checkpoint state during sibling evaluation, canonical reduction, one barrier mutation, runtime state boundary checks, and checked integer arithmetic are present (`workflow/execution/mod.rs`).
- Map inputs are bounded by count and aggregate encoded bytes; structural child IDs and canonical index order are present; parent control/checkpoint/capacity and child checkpoint zero are staged through one AppMutation before activation (`run/mod.rs:3087`–3364; `storage/mod.rs:978`+).
- Exact mutation receipts, transaction revision checks, classified Applied/Rejected/Unknown, bounded storage calls, and receipt cleanup are implemented (`storage/mod.rs:342`–420).
- Cancellation, stale generation fences, control checkpoints, durable app/v4 validation, recovery inventory validation, separate abort/resolution fields, terminal deletion fencing and scope-guard removal are implemented. These are source observations, not a claim that every race or real service is proven correct.

## Test evidence and blind spots

Read deterministic `workflow_execution.rs` and strict-live `workflow_live.rs` test source. Parent owns actual gate results. Deterministic tests include input/sleep deadlines, first-match routing and large-number comparisons, map bounded payloads, state-only joins, durable admission races, workspace revision changes, transient storage scheduling, and failed receiver acquisition. Strict-live has real notifications, lost receipt/control races, crash/reopen/abort retry, service interruption, and cancellation. This is stronger than a stub crate.

Missing targeted proof for R1–R3: multiple notifications within one activation, hanging lower setup/resolve/abort, and panics. The ordinary parallel test uses empty state-node left/right branches (`workflow_execution.rs:1372` onward) and proves final state, not concurrent agent progress or independently parked siblings. Most retry variants/reducer corner cases are not represented in the visible deterministic journey. New tests should exercise public behavior rather than freeze private names/files.

## Repro artifact

The retained text artifact [runtime-probes.rs.txt](runtime-probes.rs.txt) contains two public API tests, no new dependencies, and bounded cleanup:

- `audit_node_deadline_bounds_lower_conversation_creation`
- `audit_lower_panic_becomes_durable_failure_or_unknown`

Both were temporarily placed in `app-sdk/tests/audit_probe.rs` and executed with `cargo test -p app-sdk --test audit_probe -- --nocapture --test-threads=1`. Both compiled and failed their stated assertions: **0 passed, 2 failed**, 0.85 seconds. Each observed RunStatus::Running after the exact lower call was entered. The temporary Cargo target was removed. These are deterministic probes of app composition, never strict-live proof; the consolidated report records the real-service verifier separately.
