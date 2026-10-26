# Completion-plan review record

This records independent review of the proposed completion plan, not review or certification of implemented production changes. Three subagents first audited the four lower crates, then reviewed the assembled plan across separate responsibilities. No Cargo command or new strict-live test ran during planning.

## Review responsibilities

| Reviewer | Scope |
|---|---|
| agent_integration_prework | Lower Agent readiness, all 31 acceptance areas, user annotations, TDD/review requirements, missing composition operations and local-source ownership. |
| provider_tool_integration_prework | Provider/Tool readiness, core API and dependency scope, normal versus streamed prompts, codec semantics, signature/lifetime consistency. |
| storage_integration_prework | Storage readiness, history/control sequences, commit linearization, sync worker/runtime ownership, bounded deletion and validation evidence. |

Reviewers checked the root-authored main plan and peer contributions. Their own initial research sketches were not treated as independent review of those same sketches. The main plan was revised after cross-review.

## Findings resolved in the final plan

| Finding | Resolution |
|---|---|
| Matrix still required first-action-only inputs and prohibited global codecs | A05 now follows declared session-input reuse; A13 follows pinned process-global field/tag behavior. Initial halt-before behavior is explicitly pinned rather than assumed. |
| Reusable graph assembly had no implementation phase | Phase 3 includes WorkflowDefinition::compose with entrypoint/remap/collision/route-order and execution tests. |
| Public action output risked exposing the private expression AST | One resolved passive StateChange vocabulary reuses execution records; StateMutation/ValueExpr remain private syntax. |
| Every Agent node was routed through prompt_stream | Ordinary delivery uses prompt; explicitly incremental delivery uses prompt_stream, with honest preacceptance Unsupported and no synthetic chunks. |
| Local non-Send source could not transfer into Send AppTasks on Drop | Source is dropped locally; only safe reconciliation/Unknown evidence transfers. Explicit finalization owns documented cleanup; caller-created tasks remain caller-owned. |
| Partial-stream rules prohibited durable control evidence | User state and committed action ordinal stay unchanged, while intent/control sequence and control-only history may advance. |
| Drop unconditionally promised rollback after native commit admission | Commit admission is the linearization boundary. Pre-admission staging can be revoked; admitted finalization retains authority and receipt/head reconciliation resolves observation loss. |
| Sync worker had no owner after facade drop | Existing App supervisor registers cancellation and retains OS join ownership, including abandoned-facade/root-drop finalization; no detached worker or self-join. |
| Send was mistaken for lower-runtime independence | Caller-built lower runtimes stay driven; sync execution uses the captured App runtime and is invoked outside an async task. Runtime-bound foreign and real-lower tests are required. |
| Global codec replacement conflicted with run-pinned codec revisions | Snapshot codecs per operation; caller payload tags encode versions. App pins workflow/action revisions, never callable identity. |
| Custom child executor boundary was undecided | Selected ChildExecutor associated future + admitted ChildBatch + opaque ChildCompletion; local/Send bounds and completion provenance are explicit. |
| Callable predicates were insufficiently specified | Added static ConditionBackend with read declarations, pure synchronous evaluation and no action/sequence advancement. |
| Retained history deletion could require an unbounded transaction | Seal terminal head, delete bounded pages with durable progress/receipts, remove final head/index only after rows, resume after crash. |
| Oracle execution was required while vendor rules prohibited building it | Mandatory App fixtures derive from pinned source/tests/goldens and retain provenance. Runtime differential is separate optional evidence from an authorized installed oracle. |
| Sketch/type/path issues | Correct owned ToolName, shared hook-event lifetime, archive guardrails path, permanent companion links, explicit-deletion vocabulary and sync-call-site wording. |

All material review findings were incorporated into the main plan and acceptance package. Implementation remains obligated to prove every sketch and ownership claim through its phase-specific compile/behavior tests and fresh independent actual-diff reviews. Plan consistency does not establish production readiness.

## Planning artifact checks

- All linked local plan/audit artifacts resolve.
- The acceptance matrix includes A01–A31 and C01–C12.
- Existing-file references in the main concrete file map were checked for existence and valid initial line anchors; new files are explicitly marked.
- No trailing whitespace found; git diff --check passed for tracked changes.
- The inline goal prompt is 1,390 characters including `/goal`, below the documented 4,000-character maximum.
- Changes from this planning turn are confined to app-sdk/plans/2026-09-13. The previous audit artifacts remain under app-sdk/audits/2026-09-13. Production code, existing tests, Cargo files and normative specs remain unchanged.
