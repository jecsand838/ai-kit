# Errors and troubleshooting

An `AppError` reports an unsuccessful API operation. A `RunStatus` describes
the durable workflow boundary. Check both: successfully observing a run can
return `RunStatus::Failed` or `Cancelled`. Caller attention is reported through
`RunSnapshot.interactions`: an Input can accompany `Paused`, while an Agent
notification or Unknown effect can accompany `Running` or `Cancelling`.

Use the public error variants to decide what to do. Error messages provide
bounded context; they are not a stable protocol to parse. App keeps lower
credentials, provider envelopes and private backend diagnostics out of its
portable records and failures.

| Error | Meaning | Next action |
|---|---|---|
| `InvalidInput` | The request or authored workflow violates the contract. | Correct the data, schema, selector or configuration. Repeating the same request does not repair it. |
| `NotFound` | The requested resource is absent in the exact scope. | Check the App workspace, workflow and run identities. Use an explicit restore fallback only when absence is an intended branch. |
| `Conflict` | A revision, generation, ownership or durable relationship disagrees with the request. | Re-read the public boundary. Resolve the conflict before deciding whether to issue a new request. |
| `Busy` | Another owner, unfinished reconciliation or a finite capacity limit prevents progress. | Let that work settle, release the exclusive execution owner, or cancel it through its public API. |
| `Closed` | The operation's lifecycle or generation no longer admits work. | Finish cleanup and use the current App generation. An old handle cannot regain authority. |
| `Timeout` | An App observation or bounded operation exceeded its deadline. | Check the current durable boundary; timeout alone does not prove the work never happened. |
| `Integration` | A lower public operation failed, or private persisted data is unsupported or inconsistent. | Check the owning backend and its configuration. Preserve the original error and recovery evidence. |

The owning [application contract](../specs/APP.md) specifies exact error and
cleanup behavior. The [public ledger](../specs/SPECIFICATIONS.md) defines the
available variants and methods.

## A run is waiting

Inspect `RunSnapshot.interactions` and submit a `Resolution` for the exact
target. Keep its run handle, control sequence and target generation together.
Use the run checkpoint for cancellation, not interaction resolution.
A second notification from the same prompt is a different target. An answer
that settled an earlier notification must not be applied to the next one.

Dropping a `wait()` future stops observing; accepted workflow work remains
owned by App. A waiting input is not a failed Agent call and is not a reason
to restart the workflow. Read [interaction semantics](../specs/workflows/INTERACTIONS.md)
for duplicate, stale and lost-receipt handling.

## An effect is unknown

`Unknown` means App cannot prove whether an external effect was accepted or
completed. Reopening the App must preserve that uncertainty. Do not assume
that a timeout, connection error, panic or missing observer authorizes replay.
Use the explicit resolution supported by the interaction, backed by evidence
from the caller-owned Agent or external system.

State and checkpoint history remain useful evidence, but cannot prove an
external system did nothing. See [persistence and recovery](PERSISTENCE_AND_RECOVERY.md).

## A revision changed

A workflow, App generation, workspace, lower workspace and checkpoint each
have their own fence. Use the fence expected by the specific operation. A
checkpoint sequence is not an action ordinal or a storage transaction revision.

Workspace compensation uses the revision returned by App's own successful
lower write. It cannot adopt a newer revision just because the configuration
looks identical. If another caller changed the lower workspace, resolve that
conflict before retrying replacement.

Keep the exact workflow and Agent revisions required by retained work when
deploying new configuration. Caller revision strings identify immutable
configuration choices; they are not semantic versions for App to sort. See
the [deployment contract](../specs/APP.md) and [workspace contract](../specs/WORKSPACE.md).

## Shutdown timed out

The duration in `Drain` or `CancelAndDrain` bounds that caller's observation.
The accepted finalizer still owns cleanup. Reattach through the same App and
mode; `Drain` can escalate to `CancelAndDrain`. Keep the Tokio runtime alive
and driven until cleanup joins.

`RejectActive` can return `Busy` while preserving an active App. Root drop is
best-effort revocation and cleanup; it does not provide the durable success
guarantee of an awaited shutdown. An independent cleanup failure does not erase
the primary failure.

## History is available but the workspace is gone

A deleted workspace can retain a history-only lookup scope. Reading a retained
run does not reconstruct workspace configuration or lower products. Snapshot
of the deleted workspace and new start/prepare/restore admissions still require
live coordination. Explicit deletion of retained run history can subsequently
remove that read scope too.

## A real-service test fails

Verify the service and model required by the owning strict-live test, then run
its documented verifier. A deterministic foreign backend cannot diagnose a
real provider, protocol process or persistent database. Keep protocol-fixture,
local deterministic, real-service, process-crash and performance results labeled
separately. The [testing contract](../specs/workflows/TESTING.md) defines the
required evidence; passing a subset does not certify the whole crate.
