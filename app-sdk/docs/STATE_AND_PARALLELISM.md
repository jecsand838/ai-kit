# State, typing, serialization and reduction

`WorkflowState` owns an insertion-ordered JSON object. Its values are read-only;
`apply` returns a new state and leaves the original available. A state operation
is separate from an action's transient inputs and returned result. In particular,
a native State node returns an empty result map even when it changes state.

Run the complete service-free example from the workspace root:

```sh
cargo run --locked -p app-sdk --example state
```

[state.rs](../examples/state.rs) demonstrates ordered repeated increments,
atomic rejection, local typed data, and field/tag codecs. All imports use
`app_sdk::workflow`; none of these operations requires an App or StateStore.

## Immutable operations

The following small example keeps both versions:

```rust
use app_sdk::AppError;
use app_sdk::workflow::{JsonState, StateChange, WorkflowState};
use serde_json::{Map, json};

fn main() -> Result<(), AppError> {
    let before = WorkflowState::new(Map::from_iter([("count".into(), json!(1))]))?;
    let after = before.apply(
        &StateChange::Increment { values: vec![("count".into(), json!(2))] },
        &JsonState,
    )?;
    assert_eq!(before.values()["count"], 1);
    assert_eq!(after.values()["count"], 3);
    Ok(())
}
```

| Change | Value and order behavior |
|---|---|
| Assign | Replaces existing values in place; new keys append in payload order. |
| Append | Adds one item per ordered entry. A missing key starts an array. Repeated keys append repeatedly. |
| Extend | Adds the supplied array's items, preserving nested arrays. Repeated entries retain their order. |
| Increment | Folds ordered signed integer deltas. Missing keys start at zero; booleans and floating-point values fail. |
| Remove | Ignores missing/repeated names and preserves the other keys' order. |
| Wipe | Keeps selected keys in their original order. Missing/repeated names are harmless. |
| Merge | Replaces overlapping values without moving their keys; new keys append in incoming order. |
| Subset | Selects present keys in requested order. Repeated keys keep their first requested position. |

Removing and later reinserting a key moves it to the end. Keys are exact JSON
strings, including empty strings, Unicode, slashes and escaped control
characters. They do not use the workflow/node identity grammar.

One resolved multi-key change is atomic. If a later entry or validation callback
fails, none of its earlier entries becomes a new state. This does not undo
external side effects performed by caller validation code. In a native State
action, later expressions observe earlier staged mutations, and a later failure
discards that action's entire staged contribution.

Increment deltas must fit `i64`. Existing values and the exact sum may span the
JSON integer range `i64::MIN..=u64::MAX`; overflow fails. This differs from the
`sum_integer` reducer, which accepts only signed `i64` values and checked sums.

## Caller typing

`StateTyping` separates the behavior owner from the data it constructs. The
owner is `Send + Sync + 'static`; its associated `Data` has no required Clone,
Send, Sync or serialization bound. The example uses `Rc<CountData>` and a
non-Clone policy that checks the count's range.

For primitive changes, `apply` runs pre-validation, creates and validates the
candidate JSON, runs post-validation, then validates the whole state and its
roundtrip. Wipe resolves to Remove before those callbacks. Merge and Subset
construct snapshots and run whole-state validation and roundtrip, without the
primitive pre/post callbacks.

The default roundtrip requires `construct_data` and `construct_state` to
succeed. It does not compare reconstructed JSON for equality or replace the
candidate with that reconstruction. Override it when your model requires a
stronger invariant. `WorkflowState::new` checks JSON structure; applying a
change supplies the policy. For App execution, transfer a policy through
`AppConfig::with_state_typing`.

## Explicit codecs

`StateSerialization::global()` is one process-wide registry. Registering an
exact field or tag replaces its previous callbacks for subsequent operations.
One `serialize` or `deserialize` call retains one registry snapshot, including
when a callback registers a replacement. Callbacks run outside the registry
lock; captured replacement values are also dropped outside that lock.

Field serialization operates only on exact top-level names, and its callback
must return a JSON object. Unregistered values pass through unchanged, including
objects carrying a `__burr_serde__` tag. There is no automatic tag encoding.

On deserialization, a top-level field decoder takes precedence over tag
dispatch. Other arrays and objects are walked recursively. An object containing
`__burr_serde__` must give that field a string with a registered decoder. The
decoder receives the complete tagged object. A field or tag decoder's result is
terminal: it is not recursively decoded or dispatched again. Empty registration
names are accepted.

The example registers `labels` as an object containing `version` and `items`,
then roundtrips it. It also decodes an `example-money` tag to a JSON integer.
Choose registry ownership deliberately when several Apps share one process;
registration is not scoped to an App or workflow. Serialization is an explicit
data conversion, not a credential store or a reconstruction mechanism for lower
Agent/Storage handles.

Callback errors become `InvalidInput` with field/tag, direction and Display
context. Treat callback error messages as caller-controlled content. Direct
serialization does not catch callback panics. Failed encoding/decoding leaves
the input state unchanged and does not emit fallback values.

## Reducing staged state

The reducers below are App-native extensions. Their barrier contribution rules
are separate from the pinned immutable state-operation semantics above.

A barrier combines each successful action's final contribution per key. Two
assignments to one key in one action contribute only the last value; two
independent actions contribute separately. Reduction order is canonical by node
and lineage, independent of completion timing. A removal conflicting with a
separate action's write rejects the whole barrier.

| Reducer | Base participation and contributions |
|---|---|
| reject_concurrent | Exactly one contribution replaces the base. Multiple contributions fail even when equal. |
| collect | Replaces the base with an array of contributions, preserving contribution order and nesting. |
| append_array | Starts with the base array and concatenates each array contribution once. Non-arrays fail. |
| sum_integer | Starts with the base signed integer and adds each signed contribution, checking overflow. |
| merge_object | Starts with the base object. Later contributions replace collisions while retaining existing key positions; new keys append. |

For example, a base of 10 with independent contributions 1 and 2 under
`sum_integer` yields 13. One action assigning 1 and then 2 contributes only 2,
yielding 12. An increment is itself a state operation: incrementing that base
to 11 contributes the resulting value 11, not the delta 1. Select the reducer
according to the values your actions actually contribute. Untouched keys retain
their values; dynamically created keys without an explicit reducer use
`reject_concurrent`.

## Children, maps and joins

The [child definition](examples/child.json) supplies `value: 4` to the shared
[counter child](examples/child_counter.json), which increments it to 5. The
child's `reject_concurrent` reducer replaces its value with that result; the
parent's `sum_integer` reduction adds the child's final value to its own sum.
These are separate reduction boundaries. Using `sum_integer` for the counter's
own increment would add its base a second time.

The [map definition](examples/map.json) reuses that child with `items: [1,2,3]`
and a concurrency limit of two. Its children produce 2, 3 and 4, so the parent's
sum is 9. Starting the same definition with an empty `items` state override
retains sum 0. Register the parent and child together; map items are child
inputs, while execution inputs remain a separate transient channel.

### Select and execute child tasks

Run `cargo run --locked -p app-sdk --example child_composition` for the complete
[child composition example](../examples/child_composition.rs). It uses the
[resolved tasks document](examples/tasks.json) and the counter child above.
Two tasks have the same `task_key` but different state inputs, 10 and 20.
They execute independently, producing 11 and 21. Their stable RunId is equal;
each admission has its own public occurrence token, state, controls and history.
The example pages and opens them with `RunSelector::Exact`. Retain the occurrence
alongside any response or history reference; logical identity alone cannot
select one repeated task's target.

A Child or item-based Map selects either a literal `workflow` with optional
`revision`, or `select`, an expression producing `{workflow, revision?}`.
The selected definition must already be configured. Omitted revisions use the
root run's captured defaults; explicit revisions never fall back to a newer one.
Selectors and state/transient input expressions resolve before child admission.

Map's `items` mode maps one selected child over an array. Its alternative
`tasks` mode accepts an ordered array of resolved objects: each supplies
`task_key`, `workflow` and `input`, with optional revision, transient `inputs`,
initializer and history policy. Resolved objects are data; expression-looking
values inside them are not evaluated again. Task mode owns these choices per
task and rejects node-level item/selector/input settings. The whole batch is
validated before effects, including a bad task after a valid prefix.

The example's concrete `ReverseExecutor` implements `ChildExecutor`. It moves
the original unpolled pins out of `ChildBatch` and returns its own concrete
future, with no Send bound on child futures and no erased adapter or detached
task. That future polls the last child to completion before the first and returns
the original opaque completions. App validates exact membership and reduces in
original task order, so reverse completion `[1, 0]` still produces `[11, 21]`.
The executor owns polling policy; it cannot invent or clone completion evidence.
Use the default `InlineExecutor` when no custom policy is needed.

### Drive a Pipeline synchronously

The same executable drives the exact [Pipeline document](examples/pipeline.json).
Its first child receives value 4 and returns 5. The next child's empty state
input takes that handoff and returns 6. The parent result keeps `child_states`
`[{value:5},{value:6}]` and `final_state` `{value:6}`. Its explicit sum reduction
consumes both child contributions, producing 11; final state and reduction are
separate outputs.

Pipeline accepts only synchronous execution and bypasses `ChildExecutor`.
The example creates a caller-owned multithread Tokio runtime for App and storage,
then invokes `sync_execution` outside `Runtime::block_on` while that runtime
continues running. It rejoins App cleanup through the same runtime. Calling
Pipeline on an asynchronous lane rejects before child admission or effects;
switching lanes requires explicit execution ownership, not a second child driver.
See [synchronous controls](EXECUTION_CONTROLS.md#use-synchronous-execution-at-a-synchronous-boundary).

### Choose initialization and history deliberately

An optional initializer copies a clean saved source into the newly selected
child. Its source RunId plus optional exact `occurrence` selects the source;
checkpoint selector and resume position use the
[history rules](PERSISTENCE_AND_RECOVERY.md). Source evidence is validated under
its original revision, while the selected child keeps its own target revision.
Entrypoint uses that target graph; other resume positions require a compatible
saved position there. Initialization never replaces the selected target with
the source definition or settles source uncertainty.

Loaded-state overrides apply only when the source selection exists. An absent
source/checkpoint uses an explicit initializer fallback, otherwise the child's
resolved default state; a later Pipeline child's default is its state handoff.
Corrupt or Unknown evidence is not absence. State inputs/overrides/handoff remain
separate from transient `inputs`, which are never restored from history; see
[actions and inputs](ACTIONS_AND_INPUTS.md).

Child history defaults to `retain`. `delete_after_reduction` authorizes the same
bounded resumable deletion as explicit run deletion, only after the parent
commits the copied result and all accepted effect ownership settles. A completed
child remains retained while another sibling blocks the barrier. Staging,
failure or cancellation alone cannot authorize deletion. The policy belongs to
the exact occurrence; it cannot delete a later reuse of the same task identity.

Custom execution, initialization and history policy share the ordinary count,
byte and deadline bounds. Child depth is at most 64 below a depth-zero root;
nonempty expansion beyond that fails before admitting a partial batch.
The [owning child contract](../specs/workflows/WORKFLOW.md) contains the complete
syntax and authority rules without duplicating trait signatures here.

### Join independent paths

The [join definition](examples/joins.json) deliberately authors `right` before
`left`. Its broadcast siblings contribute independently, and `collect` produces
`["left", "right"]` in canonical node order. A subsequent `join_all` waits for
both paths. No observer sees just one of those writes as committed state.

## Bounds and errors

These finite App restrictions accompany the pinned operation semantics:

| Boundary | Inclusive limit |
|---|---|
| Encoded state JSON, before and after codec conversion | 1,048,576 bytes |
| Each decoded object key, including nested keys | 128 UTF-8 bytes |
| JSON depth, counting the outer state object as depth 1 | 64 |
| Total object entries and array elements across a state value | 262,144 |
| Assign/Append/Extend/Increment outer entries; Remove/Wipe/Subset selectors | 4,096 |

The byte limit counts JSON escaping, punctuation and UTF-8 bytes. Operation
entry limits count repeated names too; Merge uses its input state's JSON bounds.
A newly built state may pass its JSON
checks and still fail a caller typing policy or workflow admission constraint.
Do not use finite-value bounds as a promised throughput or memory envelope.

The owning semantics and source anchors are in
[WORKFLOW](../specs/workflows/WORKFLOW.md); exact public shapes remain in
[SPECIFICATIONS](../specs/SPECIFICATIONS.md).
