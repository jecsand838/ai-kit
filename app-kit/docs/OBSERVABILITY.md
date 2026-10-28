# Observe execution and export tracking records

Choose the observation boundary according to the data you need. App's default
tracing emits safe operational metadata. Lifecycle hooks are an explicit content
boundary. History reads expose retained committed state and results. None of
these replaces the StateStore records that control execution.

## Install tracing in the host

The host installs the tracing subscriber and owns filtering, sampling, export,
flush and shutdown. App installs no subscriber and reads no telemetry environment
variables. It does not construct a Datadog, Grafana or other exporter client.

The [operational contract](../specs/OBSERVABILITY.md) requires spans for admitted
App open/operations, workflow activation, supersteps, attempts, checkpoints and
interactions. An operation span covers an admitted owned task; read accessors do
not each create a span. Each task or worker carries its admitting caller's
subscriber, including Agent attempts. The superstep span covers its barrier
publication. Committed transition events follow proven durable commits. An
uncommitted attempt may report a safe failure category but cannot claim that
state changed. Recovery can repeat or lose transient observations; tracing
does not authorize execution, retry or cancellation.

Safe fields include static categories, App-owned public identities, ordinals,
bounded counts and durations. Prompts, state, results, notifications, lower
identities, paths, credentials, configuration dumps and raw backend errors are
excluded. Use App identities for correlation, not as unbounded metric labels.
Span entry and exit surround individual polls; no entered span guard survives
an await. Synchronous subscriber panics during creation, filtering,
events, entry, exit or destruction cannot change the workflow result. Each
underlying workflow poll still runs once, and its own panic is preserved.
Host installation, exporters and shutdown remain outside this containment.

Interaction events correlate the public target's `control` sequence and
`generation` with its App `run_id` and exact `occurrence`. Keep that token when
correlating repeated logical run identities; it is not a metric label. Their
`category` and `status` describe the committed settlement. In particular, `intended` records App's admitted resolution;
it does not prove that the lower decision completed. `applied` requires the
settlement receipt, while `uncertain` records unresolved outcome evidence.
Notification bodies and lower notification identities remain excluded.
The `app.workflow.delete` event with `transition = completed` follows proof
that the sealed run head and retained history are absent; sealing alone is not
completion. Use public snapshots and history when authoritative state is needed.

The [captured tracing tests](../tests/observability_contract.rs) cover scoped
spans, callback failures, native commit ordering and redaction. Existing
[action input tests](../tests/action_contract.rs) and
[notification receipt tests](../tests/workflow_execution.rs) also correlate
actual public targets with their settlement metadata. Those interaction cases
reuse their original workflow and lower receipt assertions; the native ordering
proof belongs to the dedicated transaction-backed tracing cases.

## Supply a concrete hook

Pass one caller-built `LifecycleHooks` implementation through
`AppConfig::with_hooks`. The two callback methods share an exhaustive borrowed
`LifecycleEvent` vocabulary. Implement a concrete owner or an exhaustive enum
when composing several behaviors; App adds no erased hook registry or callback
command facade.

During asynchronous execution, App calls `observe` and then `observe_async` for
each event. Synchronous execution calls only `observe`. A synchronous callback
failure prevents that event's asynchronous callback. Events borrow their data
for the call; copy only the content your application intends to retain. A hook
cannot replace workflow state through those references.

Registration follows proven durable admission and precedes execution. Step
start carries the actual attempted action context, immutable committed base and
resolved transient inputs. Stream items carry partial results that have not
committed state. Step finish distinguishes a successful outcome, an action
failure, and no outcome. EOF, abort and owning Drop can have no outcome while
still exposing the unchanged committed checkpoint.

Post-step callbacks precede post-stream callbacks and execution completion.
Owning Drop runs only synchronous completion observation with panic containment.
Use explicit asynchronous finish or abort when asynchronous hook cleanup is
required. A successful finish at an actionable native control ends that execution
session while preserving its unfinished activation observation for resolution
and resume. Owning Drop closes that observation synchronously; later settlement
cannot reopen the same guard. Reopen need not reconstruct transient hooks.

## Treat hook failures as functional failures

A pre-hook failure prevents the selected action's dispatch, including native
typing and condition work. A stream-item hook failure rejects uncommitted staged
state. A post-hook failure can happen after the action has committed: the action's
result and state stay committed, and later execution resumes without replaying
that action. The transient execution failure stays observable until a new
execution is successfully acquired; a rejected acquisition cannot erase it.
Reopen reports the durable parked boundary without inventing a retained hook
exception.

Hook failure preserves any primary execution failure as a structured cause.
Do not use hooks for optional remote analytics unless your implementation makes
its desired failure policy explicit. If it queues records, the caller owns the
bounded queue, overflow policy, delivery task, shutdown and final flush. App does
not silently detach an exporter or guarantee its delivery.

One private 30-second ceiling bounds lifecycle observation. Registration and
execution start each have one event budget. Pre-step and stream-item observation
also share the activation's remaining deadline. Post-step, post-stream and the
execution completion in one finalization chain share one cleanup budget; later
callbacks do not restart it. Synchronous callbacks remain cooperative and cannot
be forcibly interrupted. Keep blocking transports out of those callbacks.

## Export the pinned tracking content

`GraphDescription::tracking_record()` exports the graph object.
`LifecycleEvent::tracking_records(observed_at)` exports zero or more pinned
rurr tracking objects. Both are pure, bounded projections. The caller samples
the event time once, chooses a destination, frames JSONL if desired, and owns
transport errors and durability. App's control-plane records do not adopt this
wire format.

| Observation | Export |
|---|---|
| Graph | Application entrypoint, ordered action declarations and transitions. Missing required source metadata is an error. |
| Registration | Application metadata, graph, and applicable fork/spawn relationship. Restore lineage and spawning parent remain distinct. |
| Step start/finish | Begin/end entries with explicit inputs, result or exception, and the appropriate committed state. |
| Span events | Begin/end/attribute records with exact span identity, parent, dependencies and string-valued tags. |
| Stream events | Begin, first-item and end records; later partials remain visible to hooks without another first-item record. |
| Execute boundaries and child completion | No pinned tracking object. These events still have their ordinary hook meaning. |

Tracking can contain inputs, results, user state, authored source and attributes.
It is deliberately more revealing than safe operational tracing. State export
uses the selected process-global field/tag codecs. Step finish injects attempted
`__SEQUENCE_ID` and committed `__PRIOR_STEP` into a temporary state projection
before one serialization pass. Those metadata keys do not mutate user state or
change the outer action ordinal. A failed action can therefore name its own
attempt and the previous successfully committed action accurately.

A StepFinished event with no outcome cannot produce an end entry. The projection
returns an error before any field codec runs. A caller may deliberately omit
that event from its writer, but must not describe it as a successful tracking
record. Other projection failures must not silently become an empty record list.

For example, this caller-owned writer accepts an event and an explicitly sampled
time and writes the resulting objects with JSONL framing. It does no networking
or task management:

```rust
use std::io::Write;
use std::time::SystemTime;
use app_kit::AppError;
use app_kit::workflow::LifecycleEvent;

struct JsonLines<W> {
    destination: W,
}

impl<W: Write> JsonLines<W> {
    fn new(destination: W) -> Self {
        Self { destination }
    }

    fn write(&mut self, event: &LifecycleEvent<'_>, observed_at: SystemTime)
        -> Result<(), AppError>
    {
        let records = event.tracking_records(observed_at)?;
        for record in records {
            serde_json::to_writer(&mut self.destination, &record)
                .map_err(|_| AppError::Integration("tracking write failed".into()))?;
            self.destination.write_all(b"\n")
                .map_err(|_| AppError::Integration("tracking framing failed".into()))?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), AppError> {
        self.destination.flush()
            .map_err(|_| AppError::Integration("tracking flush failed".into()))
    }
}
```

Use this only with a destination appropriate for the calling lane. A synchronous
writer can block, a failure can leave a partially written record, and flushing
does not necessarily make a filesystem durable. A hook owner must choose its
synchronization and error policy; a background writer must have bounded admission
and explicit join/flush ownership. Do not insert an unbounded queue or hold a
synchronous lock across asynchronous I/O.

Timed records use UTC with nine fractional digits and a trailing `Z`. Supported
times run from the Unix epoch through the end of year 9999. An out-of-range time
is an error; an event that emits no timed record does not validate an unused
timestamp. The complete returned JSON list is limited to 1 MiB. Opt-in JSON
content keeps its natural keys instead of treating every attribute or input
name as a workflow identity.

## Reconcile observations with retained history

Use [history and recovery](PERSISTENCE_AND_RECOVERY.md) to read immutable
checkpoints and committed outcomes. A page cursor fixes its history cutoff.
History survives until explicit deletion; partial output, lifecycle delivery
and operational tracing do not acquire durability merely because a run does.

The [tracking projection tests](../tests/tracking_contract.rs),
[checkpoint tracking tests](../tests/tracking_checkpoint_contract.rs) and
[hook execution tests](../tests/hook_execution_contract.rs) prove different
boundaries. Projection tests validate objects and actual history values; they
do not establish that the runtime delivered every event. Runtime callback tests
do not establish an external writer's exactly-once delivery. Report repeated,
lost and unflushed observations under the caller's chosen delivery policy.
