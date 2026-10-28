# Choose how to advance a workflow

Use background execution when the App should drive a run. Use a prepared run
and an execution owner when the caller needs to inspect partials, advance one
boundary, or choose where to stop. Both paths use the same state transition,
barrier, checkpoint and recovery rules.

Run `cargo run --locked -p app-kit --example controls_and_streaming` for a
[complete partial → commit → halt → resume example](../examples/controls_and_streaming.rs).

`prepare` admits checkpoint zero without executing its entry node. `start`
admits a run and starts background advancement. The initial state patch belongs
to admission. On an existing run, `ExecutionOptions.state` must be empty;
`inputs` is the separate transient map reused by actions in that execution.
To change retained state when branching, use the explicit restore override in
[history and recovery](PERSISTENCE_AND_RECOVERY.md).

## Hold one execution authority

`run.execution(options)` acquires a non-Clone owner that borrows the run
mutably. Another handle for the same run cannot acquire competing execution.
Input validation, halt-selector validation and admission must succeed before a
new owner can clear a previous transient completion error. A rejected or Busy
request does not advance the workflow.

An acquired execution is lazy: acquiring it does not construct and poll a caller
action source. The advancement methods determine when work is performed:

| Method | Stops at |
|---|---|
| `next` | One partial output or committed/halted boundary event. |
| `step` | One committed boundary after draining its partials, or a parked/halted/exhausted boundary. |
| `finish` | Halt, actionable park or exhaustion after draining the remaining execution. It consumes the owner. |
| `abort` | Finalization of the current uncommitted boundary, with its cleanup rules. It consumes the owner. |

A committed serial boundary contains one action. A committed parallel boundary
contains the complete required ready wave in canonical order. Staged siblings
are not individually published merely because `step` was requested. A pending
Input, timer, Agent notification or child prevents its required barrier from
committing but does not authorize blocking unrelated ready siblings from entering.

Successful publication precedes outgoing-condition evaluation. If the next
route fails, the previous action's committed result and state remain available.
No method reruns that action to reconstruct its output. See
[state and parallelism](STATE_AND_PARALLELISM.md) for the complete barrier rules.

## Distinguish three kinds of position

The checkpoint sequence advances for durable controls as well as action
publication. An action ordinal advances only for a committed named activation.
A halt target identifies the selected stopping condition. These values can
refer to different things.

Partials, notification receipts, retries, failed attempts and no-output EOF do
not consume committed action ordinals. A native State step with several
mutations consumes one; a successful empty Map also consumes one. Children have
their own run-local ordinals while the parent consumes one for its completed
composition node.

For a parallel wave, committed outcomes and ordinals follow canonical
`(node, lineage)` order. Completion timing cannot reorder them. An after-halt
representative can precede the last canonical outcome: the halt names that
representative, while the snapshot's last action ordinal still identifies the
last committed action in the whole wave.

## Apply halts using the pinned ordering

Select exact node IDs or `@tag` in `halt_before` and `halt_after`. Validate all
selectors before effects; tags expand to matching nodes, and duplicates retain
their first occurrence. Selector order does not override canonical sibling
selection.

Both lanes execute the first action of the **current execution session** before
testing halts against prior position. A selector naming the entry node therefore
does not turn a fresh session into a zero-step pause. After reopen or explicit
resume, an old committed action does not count as this session's first commit.
Partial output and no-output EOF also do not enable halt checks.

The method that commits a boundary selects its halt precedence:

| Advancement lane | Order after the current boundary |
|---|---|
| Streaming `next` | Halt after the completed action, then exhaustion, then halt before the next action. |
| Ordinary `step`, `finish`, background `start` | Halt before the next action, then halt after the completed action, then continue or exhaust. |

For A → B with both `halt_after = ["a"]` and `halt_before = ["b"]`, both lanes
commit A once and leave B unexecuted. Streaming `next` reports A and A's exact
result. Ordinary `step`/`finish`/background execution reports B with no result.
This observable difference follows the pinned rurr behavior.

If `next` delivers a partial and `step` then commits the source, ordinary
precedence applies. Once a halt is selected and retained, later method changes
cannot reclassify it. The committed event precedes the halted event, which is
exposed once; further event advancement exhausts that session. A fresh execution
can resume the saved next work without replaying the committed action.

`RunStatus::Halted` and `ExecutionEvent::Halted` carry the same tuple. A before
halt has the pending node and no result. An after halt has the completed node
and its exact result, including a present empty map. App retains that decision
through snapshots, finish and reopen. It does not invent a pending action
outcome or increment an ordinal to represent a before halt.

In a broadcast workflow, a halt applies at a complete wave boundary. Before
halts park the entire next ready wave before any member dispatches. After halts
retain every committed outcome from the current wave. Unknown, unresolved,
failed and suppressed work is not a successful halt candidate. These BSP rules
are App extensions of the serial rurr contract.

`max_steps` counts successfully committed named activations in the current
execution. The limit is checked after the complete wave commits, so a parallel
wave can carry the count past that limit; it is never split to meet the count.
Partials and control checkpoints do not count. The App's finite runtime bounds
still apply when this optional caller limit is absent.

## Resume a caller control

An Input, Agent notification or Unknown is an actionable control, not a selected
halt. Finish may return its snapshot so the caller can release the execution
owner and answer it. An ordinary Input park projects `Paused`; an accepted Agent
notification can still project `Running` while its original lower work remains
owned. Inspect `interactions` to identify the exact target in either case.
Resolving that target records settlement; a prepared run still needs explicit
execution to reach the ordinary barrier. Accepted staged input, state or failure
must survive this reacquisition, and the underlying Agent/action must not replay
to recreate it. Follow the exact target fences in
[interactions and controls](INTERACTIONS_AND_CONTROLS.md).

A successful resolution commits its control settlement. New execution may
briefly return `Busy` while the existing cleanup and hook owner releases. Retry
acquisition without resubmitting the resolution or replaying the action.

Dropping an ordinary run observer does not cancel. Dropping the execution owner
revokes its uncommitted advancement and later admission. Once a native commit
has been admitted, its supervised receipt reconciliation still owns the result.
Drop cannot undo an external effect or promise asynchronous hook completion.
[Streaming](STREAMING.md) explains the exact admission boundary;
explicit asynchronous finalization remains the path for awaited cleanup.

## Use synchronous execution at a synchronous boundary

`SyncExecution::open(&mut run, options)` acquires synchronous execution on the
calling thread. `try_next` returns one partial result or committed boundary;
`step`, `finish` and `abort` use the same execution kernel. It creates no thread, channel or replacement runtime.
Call it outside an entered Tokio runtime context; calls inside one reject before blocking.
Keep the runtime captured by the App and any caller-owned lower runtimes driven
through execution and shutdown. The synchronous facade does not make an async
backend independent of its reactor.

Synchronous execution calls only synchronous lifecycle hooks, even when internal
storage operations are asynchronous. Caller-local sources and typed values stay
on their owning lane. Cooperative callbacks must return; Rust cannot forcibly
stop indefinitely blocking caller code.

The [execution contract](../specs/workflows/WORKFLOW.md) and
[exact public ledger](../specs/SPECIFICATIONS.md) own these rules. The
[halt tests](../tests/execution_controls.rs),
[native execution tests](../tests/native_execution_contract.rs),
[BSP tests](../tests/bsp_execution_contract.rs) and
[worker tests](../tests/worker_contract.rs) cover distinct obligations.
Serial async proof alone does not establish synchronous, parallel or
fresh-process behavior.
