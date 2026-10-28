# Workflow definition and execution

`../SPECIFICATIONS.md` owns exact public signatures. The pinned rurr source in
RESEARCH.md determines serial core behavior; BSP and truthful external-effect
recovery are explicit App extensions. The acceptance matrix's A01–A31 and
C01–C12 rows are mandatory proof obligations, not a feature-name checklist.

## Strict versioned language

The parser accepts only `app-kit.workflow/v2`; every object denies unknown and
duplicate fields. It compiles once into private typed expression/step/routing
records. No public mutable AST, arbitrary code evaluator, generated parser,
parallel compatibility engine or alternate publication representation exists.
The hand-maintained schema uses JSON Schema draft 2020-12, local references
only. Semantic graph checks supplement syntax validation. JSON source size,
encoded values, depth, counts, arithmetic and references are checked before
allocating unbounded structures or performing effects.

The root contains `schema`, validated `id`, caller `revision`, initial `state`,
unique `required` keys, `reducers`, `entry`, nonempty unique `nodes`, and
`routes`. Nodes contain `id`, `step`, optional `name`, `description`, ordered
`tags`, `retry`, and `deadline_ms`. Routes contain `mode: first|broadcast`
and ordered `branches`; each has optional `when` and a node/end target.
First-match transitions use authored order, including the default true branch.
No outgoing route, an empty route, or exhaustion of false conditions ends that
path without an invented terminal action. A first route may have no default;
its one optional default may occur anywhere in authored order. A second
default from the same source is rejected. Conditions after a selected default
are not evaluated. This follows pinned graph validation/selection, which does
not require the default to be last. Explicit end targets remain available.
Broadcast is explicit; it does not change first-match graphs into fan-out.

Steps are the closed forms:

- `agent`: logical `agent`, ordered rich `prompt`, optional `structured_schema`,
  `text_target`, `structured_target`, and explicit `stream` boolean (default
  false). Different nodes may use different inventory Agents.
- `action`: logical `action`, required exact caller `revision`, and optional
  `project_result: boolean` (default false).
  The binding's exact revision, reads/writes/inputs/mode/tags are validated
  before dispatch. Result output and proposed writes remain distinct.
  Reads/writes declarations describe the action; they do not hide state or
  authorize an exclusive set of explicit writes. Every valid explicit
  `Completed.writes` change is applied, including keys absent from declaration
  metadata, matching pinned `action/api.rs:548–560`. Existing typing, JSON
  bounds and reducer rules still apply. Undeclared reducer keys use the ordinary
  single-writer/reject-concurrent policy, without requiring a placeholder state
  value. When project_result is true, App selects each present exact top-level
  result key from the selected declaration's ordered writes list. Missing keys
  are skipped; explicit null is copied; duplicate keys keep their first selected
  position. The full result map remains unchanged. App applies one Assign with
  those selected values, even when empty, then every separately supplied
  Completed.writes operation in order. False inserts no projection operation.
  Later explicit operations remain authoritative but cannot bypass an earlier
  projection validation failure; the complete action's state changes remain
  atomic. Projection applies only to Completed, never Partial, EOF or an aborted
  source. It uses the existing state operation/typing/reducer boundaries, so a
  result that is valid under its byte-only bound can still be invalid to copy
  into state. The combined operation count, including an enabled empty
  projection, is checked against App's 4,096-operation bound before any operation
  validation callbacks. This finite bound and projection-before-explicit order
  are App translations. Pinned function.rs259–268,309–333,512–523 selects the same
  present keys for its result-only builders and applies that projection alone;
  the explicit ActionResult path preserves independently supplied writes.
- `state`: ordered `mutations` on one isolated action-local state; sibling actions begin at the same immutable committed barrier state.
- `input`: ordered rich `message`, optional local JSON `schema` and `target`.
- `child`: `workflow`, optional exact `revision`, expression-valued `input`,
  optional `task_key`, initializer/history policy, and ordered `reductions`.
- `map`: validated child selection, array/task/state source, `input_key`,
  optional `index_key`, additional expression-valued `input`, `max_parallel`,
  initializer/history policy, and ordered reductions.
- `pipeline`: ordered child specifications with explicit state handoff and
  reduction; synchronous execution runs sequentially through the same kernel
  and bypasses ChildExecutor. Asynchronous invocation rejects before admitting
  or executing children.
- `join_all` and `join_first`: no arbitrary payload or callback.
- `sleep`: a **string** `message` and positive `delay_ms`; it is not a rich
  message-part array and cannot be caller-resolved.

Agent/Input rich parts are `text` with a nonempty string or `state` with a
validated state path. A reduction contains `source`, `target`, and one of
`reject_concurrent`, `collect`, `append_array`, `sum_integer`, `merge_object`.
`map` rejects reject_concurrent when multiple children can write one target.
Child overrides name logical already-built inventories and App history/
initializer policies; they never contain Provider/Tool/Agent/StateStore config.

### Native node results

Each committed named activation produces one ActionOutcome. Its result is
independent of state writes, including when both intentionally contain the
same value. The following mappings are explicit App-native contracts; rurr
does not define App's Agent, parked Input, State, Join or Sleep node schemas.

Transient result maps contain serializable JSON and retain the aggregate
result/staged-activation byte bounds. They do not inherit workflow-state key,
depth, collection, required-field or reducer grammar: an untargeted structured
Agent result or caller Action result may contain a 129-byte object key, a
65-level JSON value or more state-structure items while remaining within its
byte budget. Explicit state writes still pass the owning state/typing rules.
An Agent node may return its result without any state target. A configured
structured state target still requires its valid structured-output schema.
Result validation is identical on ingress and recovery, with no hidden
conversion into WorkflowState and no extra source-specific result wrapper.

| Step | Committed result map | Partial result map |
|---|---|---|
| action | Exact caller Completed.result | Exact caller Partial.result |
| agent | `output`: exact terminal text; `structured_output` only when present; `usage` only when present, containing exact `input_tokens` and `output_tokens` | With stream enabled, `output_delta`: one exact lower OutputDelta text fragment |
| state | Empty map | None |
| input | `value`: exact accepted resolution value, whether or not a target exists | None; waiting remains an Interaction |
| child / map with state reductions | Empty map | None |
| join_all / join_first | Empty map | None |
| sleep | Empty map | None |

Agent result fields retain the listed insertion order. An absent optional field
is omitted; a present structured null stays present. Only configured text and
structured targets write state. The complete lower PromptResponse is terminal
authority: intermediate deltas may include earlier assistant messages and need
not concatenate to the terminal text. Do not accumulate or fabricate deltas,
stream reasoning/tool/protocol data, or slice terminal text to simulate streaming.
Nonstreaming nodes produce no synthetic progress. Partial delivery never commits
user state, action ordinal or action result history.

Explicit Resolution::Accept on an Unknown Agent effect provides state
contributions, not a PromptResponse. That manually reconciled activation has an
empty result and consumes one ordinal only if its barrier commits. Never infer
output, structured output or usage from the accepted state target names/values,
and never replay the lower prompt merely to reconstruct a result.

State-only Child/Map results above map the empty-result reduction form in pinned
`parallelism/map_reduce.rs:40–44`; they do not collapse all rurr child modes into
that form. Adopted cascade modes retain their distinct result/write behavior:
`parallelism/task_action.rs:127–151` gives None an empty result with child_states
writes, and Fixed/Pipeline result keys child_states and final_state. Completing
that capability and child terminal-result routing remains required child work.
Existing reductions select child state; the later state/result selection extends
that same closed reduction description, never injects transient results into
child state or copies entire histories into a parent result. App's parked Input
is not rurr InputAction, which copies selected runtime input fields into both
result and writes (`action/builtin.rs:56–66`). Caller actions preserve that pinned
capability without changing the parked-input mapping.

The general pinned result/write separation and valid empty terminal maps come
from `action/api.rs:297–334,516–560`. Empty completed results are not source EOF:
`application/streaming/step_core.rs:157–167` rolls a source without its terminal
item back without inventing an outcome.

State mutations are `assign`, `remove`, `append`, `extend`, `increment`,
`wipe`, `merge`, `subset`, with resolved StateChange as their single execution
boundary. Assignment/increment/appending operands are expressions; key sets
for wipe/subset are bounded; duplicate names are accepted. Wipe retains original
state order, while subset retains each present key's first requested position.
Exact mutation payloads are:

- `assign`, `append`, `increment`: `target` and expression `value`;
- `extend`: `target` and expression `values`, which must produce an array;
- `remove`: `target` only;
- `wipe`: ordered `keep` key array (retains original state order);
- `merge`: expression `state`, which must produce an object;
- `subset`: ordered `keys` array (selects present keys in requested order).

A missing append/extend target starts as an empty array; a missing increment
starts at zero. Increment's delta is a signed i64 JSON integer; existing state
may contain any i64/u64 integer and the exact result must remain in that JSON
integer range. Floating-point and boolean increment values fail. Remove is
idempotent; remove then reinsert moves the key to the end. Wipe/subset ignore
missing selected keys. Merge replaces existing values without moving their
positions and appends new keys in incoming order.

Assignment, append, extend and increment may create keys absent from initial
state; remove of an absent key succeeds. State keys and nested object keys are
exact JSON strings, including empty, Unicode and escaped control-containing
keys, subject to the private byte/count bounds. They are not workflow identities.
Read paths are validated syntactically during compilation and resolved against
the actual state when the expression or rich prompt is evaluated, so an earlier
action or mutation may produce the required key.

Each expression observes preceding mutations from the same action. An error
in a later expression or operation discards the entire action contribution;
no sibling observes those temporary values. Dynamically created keys without
an explicit reducer use reject_concurrent in the existing barrier reducer.
Expression failures and state
operation/reduction failures retain their separate portable failure categories.
Multiple writes to one key in one action contribute its final value exactly
once. Sibling contribution order is canonical by node and lineage; independent
remove/write conflicts reject the whole barrier.

Expressions include literal/state,
length, unary positive/negative/not, add/subtract/multiply/divide/remainder,
floor_divide/power, boolean and/or, conditional, attribute/key/index lookup,
and slices with optional start/stop/stride. Conditions include constant,
exists, equality/order comparisons and chains, in/not_in/contains, identity
against null/booleans, not/all/any, and a logical `custom` predicate key.
A bounded text-expression parser, if used, compiles to this same vocabulary;
no arbitrary function invocation, imports, evaluation runtime or object method
execution is accepted. `len` is the sole built-in callable expression.

A state path is one root token followed by optional slash-separated child
reference tokens. Every token, including the root, uses RFC 6901 escaping: `~1`
for slash and `~0` for tilde, decoded in that order exactly once. App omits the
leading document-root slash, so the empty path addresses the empty top-level
key, not the whole state. Decoded tokens preserve exact Unicode/control bytes;
there is no Unicode normalization. Malformed escapes fail definition validation.
Each decoded key retains the 128-byte data-key bound and the entire encoded
path has the existing 65,536-byte text bound. Array tokens require unsigned
ASCII decimal indices without signs or leading zeros (except `0`); object
member names remain exact strings. Missing/nonexistent array members fail
lookup. Graph metadata and required-read validation use the decoded root only,
so short-circuit conditions do not eagerly require a nested member. This is
App's documented syntax translation; string expression indexing/attributes
still follow the pinned semantic corpus. Token and array rules follow
[RFC 6901 §§3–4](https://www.rfc-editor.org/rfc/rfc6901#section-4). Workflow/node/binding keys use the WorkflowId grammar. Dynamic JSON
object keys inside state are data, not new workflow identities. User inputs
with reserved `__*` names fail before action/hooks. All other runtime inputs pass
through, including keys absent from the required/optional declarations, as pinned
`action/function.rs:49–67` and `application/session.rs:704–725` require. Bound
values cannot be overridden, required inputs cannot be defaulted, and optional
defaults apply only where missing. A session reuses its
runtime inputs for successive actions; none are checkpoint-zero state unless
explicitly provided through the separate initial-state/state-write path.

Action and condition declarations reuse App's existing finite workflow JSON
resource bounds. Logical binding keys retain the canonical 128-byte identity
grammar; caller revisions retain the nonempty, trim-stable, control-free
256-byte limit. Each configured action or condition inventory admits at most
4,096 values, counting every retained revision. Every decoded state-read/write or input name is at most
128 UTF-8 bytes; this is a data-key size limit, not the canonical grammar for
workflow/binding identities and not a policy imposed on lower Kits. Each
reads/writes/required-inputs/optional-inputs/tags list and each bound/default
input map admits at most 4,096 entries. Repeated list entries remain observable
and count toward that limit. Each action tag and optional source string admits
at most 65,536 decoded UTF-8 bytes. Each declaration's aggregate JSON-encoded
metadata admits at most 1,048,576 bytes, including escaping and structural
overhead; fitting each individual field does not guarantee aggregate admission.
Bound/default values also retain the existing JSON depth and item limits.
All violations reject during App preflight with InvalidInput before action or
predicate evaluation. These are deliberate finite App resource restrictions
relative to pinned rurr's metadata builders, which do not enforce matching
ceilings; they do not justify filtering inputs, deduplicating declarations,
restricting data-key characters or changing behavior within the bounds.

### Exact expression and child syntax

All expression/condition/step forms are objects with `kind` as their closed tag.
Optional fields absent from the tables are rejected; literal JSON values are
uninterpreted only inside the `literal.value` boundary.

| Expression kind | Additional fields |
|---|---|
| literal | `value: JSON` |
| state | `path: string` |
| length, positive, negative, not | `value: expression` |
| add, subtract, multiply, divide, remainder, floor_divide, power | `left: expression`, `right: expression` |
| and, or | `values: expression array with at least two operands` |
| conditional | `when: condition`, `then: expression`, `else: expression` |
| attribute | `value: expression`, `name: string` |
| index | `value: expression`, `index: expression` |
| slice | `value: expression`, optional `start`, `stop`, `stride` expressions |

| Condition kind | Additional fields |
|---|---|
| constant | `value: boolean` |
| exists | `path: string` |
| equal, not_equal, less_than, less_than_or_equal, greater_than, greater_than_or_equal, in, not_in, contains, is, is_not | `left: expression`, `right: expression` |
| not | `condition: condition` |
| all, any | `conditions: condition array` |
| compare | `values: expression array`, `operators: comparison-kind array` with one fewer operator than values |
| custom | `condition: logical key`, required exact `revision: string`, optional `name: string` display label |

An empty all is true and empty any is false. Expression and/or require at least two operands, matching the pinned AST validator. A chained comparison has at least
two values and evaluates each intermediate operand once. Identity operands
are restricted to the pinned null/boolean vocabulary. Slice omitted bounds
follow pinned semantics; an explicit zero stride errors. Text syntax, if
provided, compiles to these forms and shares their depth/evaluation limits.

Assign/append/increment use `target` and expression `value`; remove uses
`target`; extend uses `target` and array-producing expression `values`; wipe
uses bounded string array `keep`; merge uses object-producing expression `state`;
subset uses bounded string array `keys`. Repeated selection names are idempotent.
A caller action's already resolved
StateChange goes through the same operation owner without reparsing syntax.

A child selector is either a literal `workflow` key with optional `revision`,
or `select: expression` evaluating to an object `{workflow, revision?}`; exactly
one form is present. Its selected pair must already exist in App inventory.
An omitted revision is resolved from the root admission's captured default for
that workflow id, never the inventory default at later execution or reopen.
A root whose reachable definitions contain a dynamic selector or tasks source
captures a default revision for every workflow id in that admission inventory;
a purely static root retains only the existing statically reachable defaults.
Descendants inherit that same finite selection. Dynamic selection does not
require unrelated target state/reduction schemas to agree: validate only the
actual selected ordered batch against its exact definitions before admission.
Explicit selected revisions must exist in the caller-supplied immutable catalog;
they never fall back to a default. A new workflow id absent from the captured
default inventory requires an explicit revision. Static graph validation still
rejects known child cycles. Runtime-selected recursion is supported with an
explicit App operational child-depth limit of 64: a root has depth zero and
children at depths one through 64 are admitted normally. At depth 64, a nonempty selected
child expansion fails with Limit before Park, registration, ChildExecutor or
child effects; no fitting prefix of that expansion is admitted. An empty Map
does not create a deeper occurrence and retains its ordinary behavior. ParentRecord
carries the child's depth, and admission, recovery and native membership proof
check it against the exact parent's depth plus one. Values outside one through
64, arithmetic overflow, and an in-range value inconsistent with the parent
are invalid durable ancestry evidence. Recovery never reruns a selector.
The limit applies to static and dynamic composition. This deliberately bounds
static chains which previously had no explicit child-depth cap; that contraction
is an App resource policy, not a pinned-rurr recursion or identity rule. The
ordinary child-count, encoded-size, deadline and total-capacity limits still
apply independently. Terminating recursion is not rejected merely because the
same workflow appears in its ancestry.
`task_key`, when present, is a string-producing expression used verbatim.
Otherwise the pinned graph/state index key is used: direct Child is `0-0`,
Pipeline declaration i is `{i}-0`, and single-target Map source index i is
`0-{i}` (zero-based canonical decimal). This matches pinned map_common.rs:
175–185 and the public child_context.rs:5–54,58–102 identity fixtures; an action
name or node name never namespaces the key. `input` maps child state keys to expressions. Optional `inputs`
maps transient action-input names to expressions and defaults to an empty map;
explicit null, a bare expression in place of the map and unknown fields reject.
Resolve each value once against the original parent activation state alongside
its selector/state input, before any child effect. These values become that
child occurrence's existing ExecutionOptions.inputs, never state mutations,
results, history, initializer overrides or Pipeline handoff. Bound/default/
required/optional action-input rules remain the existing action contract.
The encoded ordered transient input vector has a separate 1,048,576-byte bound,
including array separators and JSON escaping; the existing encoded durable
state-input vector has its own same-sized bound. Neither is added to the other
for admission. Check each increment while expanding the selected batch, before
Park, registration or child effects; reject the complete oversized batch rather
than admitting a fitting prefix. Individual maps also satisfy the existing
runtime-input key, depth and item restrictions. These finite App resource bounds
do not persist a byte counter or permit filtering caller values.
Resolved durable task metadata, including each selected target, stable key and
initializer policy, also passes an incremental necessary check against the
existing 4,194,304-byte durable-record ceiling before further batch expansion.
This is not a separate lower metadata limit: the complete native record still
passes its unchanged aggregate encoding bound before admission, including all
other checkpoint fields. An oversized expansion rejects the whole batch before
child registration or effects; a fitting prefix is never admitted.

Only the original concrete child execution owner retains transient values.
Recovery never reevaluates the parent source or persists those values to
reconstruct them. Required unstarted action inputs which are unavailable use
the existing Interaction::Inputs / Resolution::Inputs rebinding protocol before
source construction; no accepted or uncertain action is redispatched to obtain
inputs. The same required-name projection and optional/default semantics apply
as for recovered root ExecutionOptions.inputs. Pipeline changes state only and
preserves each task's distinct transient map. Pinned authority is
parallelism/types.rs:105–120,161–164,191–193 and runtime.rs:317–344.

`initializer`,
when present, contains `source: expression` yielding a public RunId and optional
`occurrence: expression` yielding an exact occurrence token. Omission selects
Latest for that source identity; a present malformed or absent exact occurrence
never silently selects another occurrence. It also contains
`checkpoint: {kind: latest}|{kind: sequence, value}|{kind: action, value}`,
`position: after_completed|at_saved|entrypoint`, optional
`overrides: object<string, expression>`, and optional object-producing expression
`fallback`. Overrides use the same key-to-expression map as child input;
an expression object in place of that map is rejected. It uses the same
restore/admission contract and cannot turn an Unknown boundary into a fresh
attempt. Child `history` is `retain` (default) or `delete_after_reduction`;
the latter explicitly authorizes normal sealed deletion only after the parent
commits the copied reduced result and all effect ownership is settled. The
policy belongs to the original admitted child occurrence, not its reusable task
identity. Successful child staging retains the exact child references needed
by that eventual barrier; staging alone never authorizes deletion. The barrier
which commits that staged result publishes deletion authorization atomically
with the parent state and outcome. A failed, discarded or cancelled wave
publishes no such authorization. CHECKPOINTS.md owns its durable deletion fence.
For a child initializer, an absent source or selected checkpoint uses its
explicit fallback when supplied, otherwise the child's resolved default state.
This default is the Pipeline handoff for a later Pipeline child. Loaded-state
overrides do not apply to the missing-state branch. Invalid or Unknown source
evidence is an error, not absence. Root restore retains its separately documented
explicit-fallback policy.

A map uses exactly one source: `items: expression` for an array, or
`tasks: expression` for an ordered array of resolved task objects. Items mode
retains the authored child selector, input expressions, initializer and history
policy; `input_key`, optional `index_key`, additional `input` and
`max_parallel` govern item mapping. A tasks-mode node permits only `kind`,
`tasks`, `max_parallel` and `reductions`. It rejects node-level selectors,
item fields, `input`, `inputs`, `task_key`, `initializer` and `history`.

Each evaluated task is resolved data with required `task_key: string`,
`workflow: string` and `input: object`, plus optional `revision: string`,
`inputs: object`, `history: retain|delete_after_reduction` and `initializer`.
The resolved `inputs` object is already transient data: do not parse its values
as expressions or copy them into the persisted task/state description. The resolved
initializer has `source: RunId string`, optional `occurrence: string`, the same
`checkpoint` and `position` vocabularies as above, optional `overrides: object` and optional
`fallback: object`. These values are used directly; no expression is evaluated
again and no `select` wrapper is accepted inside a resolved task. An omitted
revision uses the root run's captured workflow selection. Explicit `task_key`
is required in tasks mode. The workflow JSON Schema validates the tasks
expression's syntax; its evaluated task data is validated at execution,
including when the expression is a literal. Original source order is retained
independently of custom execution order. A pipeline uses nonempty
`children: array` of untagged child specifications and ordered `reductions`.
Each child object contains the same literal `workflow`/optional `revision` or
`select` alternative, optional `task_key`, `input` map, optional transient
`inputs` map, optional `initializer` and optional `history`. The enclosing pipeline establishes the child shape:
per-child `kind` and `reductions` fields are rejected; only the pipeline owns
its ordered reductions.
Pipeline execution is synchronous-only and bypasses `ChildExecutor`. An
asynchronous invocation returns `AppError::InvalidInput` at the selected node,
before that node's pre-hooks, child admission or effects. It leaves parent
state, outcomes and action ordinal unchanged; a subsequent explicit synchronous
execution may run the node under the ordinary execution-ownership rules.

Resolve every child's selector, task key and input override once against the
original parent activation state. The first child uses its resolved initial
state. Each successor receives the prior committed child state, then applies
its already resolved explicit input overrides; later definition defaults do
not replace handed-off values. That builds the default candidate. An explicit
initializer which loads a checkpoint replaces that candidate, applies only its
initializer overrides and restores its selected position. Do not reapply child
input overrides after a successful load. The selected task graph/revision remains
the target: a historical source is validated against its own retained definition,
materialization and immutable occurrence evidence, while the copied state and
saved position are validated against that already captured target graph. Different
source/target revisions are allowed; copying does not silently switch target
callable or agent pins. Entrypoint uses the target entry; at_saved and
after_completed require the selected saved position/frontier to be valid in the
target, with after_completed routing the saved node through the target's own
transitions rather than copying the source graph's old ready frontier. An unavailable target position fails before that child admission rather
than resetting to entry or admitting an incompatible checkpoint. This uses the
existing restore kernel, not a graph migration API. Pinned initializer loading
and the task graph remain separate in initializer.rs:214–269 and
parallelism/types.rs:191–193. The default input override precedence
is an App-native extension to the pinned sequential state replacement; loaded
initializer precedence follows `parallelism/executor.rs:123–135`,
`parallelism/types.rs:191–193` and `application/build/initializer.rs:214–269`.
A failed child prevents
successor admission and the parent action's barrier commit. Intermediate
retention follows each child's explicit history policy. Nested map/pipeline
work consumes the same depth/capacity budgets.

A successful pipeline result has `child_states` followed by `final_state`:
the complete ordered child states and the exact final child's state. With
absent or empty `reductions`, that final state supplies the parent state
contributions, following pinned Pipeline result/write separation. Contributions
use normal parent typing and reduction validation; parent keys not contributed
remain unchanged. Nonempty explicit `reductions` replace those automatic state
contributions and use the existing ordered child-state reduction rules. This
replacement is an App-native extension: it never double-contributes automatic
and explicit writes, and it does not change `child_states` or `final_state` in
the action result. All parent writes and the single pipeline action outcome
commit together only after every child succeeds.

Retry objects are exactly `{kind: never}`, `{kind: immediate,max_attempts}`,
`{kind: fixed,max_attempts,max_elapsed_ms,delay_ms}` or
`{kind: exponential,max_attempts,max_elapsed_ms,initial_ms,maximum_ms,multiplier}`.
All bounds are positive checked integers, capped by documented private limits;
maximum_ms is at least initial_ms. A non-null retry object is applicable only
on an Agent node, including `never`; other step kinds reject it before effects.
Omitted or null retry is valid on every node and authorizes no additional Agent
attempt. This App-native attempt policy does not change explicit resumption of
an uncommitted failed Action at its original semantic ordinal.
Unknown/inapplicable combinations reject.
The workflow schema/parser corpus must exercise every optional/variant branch,
including semantic constraints JSON Schema cannot express.

## Compilation and composition

An Action or custom Condition reference always names an exact `(key, revision)`
pair. Missing/invalid revision syntax rejects during parsing; a well-formed pair
absent from the supplied inventory rejects with InvalidInput during App binding,
before App publication, hooks or callable effects. Duplicate inventory pairs
also reject with InvalidInput, while distinct revisions of the same key coexist.
Inventory order never chooses an Action or Condition revision. There is no
implicit latest/current selection, unique-key shorthand or unversioned fallback.
This required reference is an App deployment extension, not a rurr revision API.
Custom references may also carry a `name` string for display: omission uses the
logical key, present null/nonstring values reject, and arbitrary empty,
whitespace, Unicode and control-containing strings are preserved within the
existing 65,536-byte label bound (an explicit finite reduction from pinned
unbounded names); the name never selects a binding, and Condition Not/All/Any
compose labels as `~name`, `left & right`, `left | right` without invented
parentheses, following `action/condition.rs:323–333,397–400,467–509`, while
empty All/Any retain App's True/False convention and ValueExpr logical labels
retain expression syntax.
The pair belongs to its authored definition's canonical content and retained
pins; changing it under the same workflow id/revision conflicts before a new
generation is published. Reopen must preserve the exact bindings of older runs
when newer definitions and callable revisions are supplied. No callable value,
function identity, lower configuration or separate public pin DTO is persisted.

Validate uniqueness, references, entrypoints, graph reachability, ordered
transitions/default uniqueness, read-path/write-key syntax, reducers/required keys,
input declarations/default/bind conflicts, tags/selectors, structured schema,
rendered-value bounds where statically knowable, retry applicability and checked
deadline arithmetic before effects. Rendered prompts and dynamic operands are
validated at execution before their associated effect. Invalid conditions are
errors, not false branches. Declared missing reads fail independently of whether
a custom predicate would happen not to inspect the key.
Built-in route conditions likewise validate every required top-level read
before short-circuit evaluation, as pinned rurr Condition does. This checks
root presence, not unchosen nested lookups. App's `exists` predicate is an
optional read. An expression-only conditional uses ordinary short-circuit
evaluation; it does not introduce a separate Condition admission boundary.

Cycles remain supported with finite visit/superstep/depth budgets; the compiler
must not reject all cycles or all recursive child graphs for convenience.
Dynamically selected children must come from the validated revision inventory.
Finite recursion and active child capacity are checked at admission. Reusing
an id/revision with different canonical content conflicts. Runs pin exact
workflow/action/predicate revisions, and new versions do not rewrite admitted
runs. Graph composition imports definitions with explicit prefixes/connections
and reuses the ordinary compiler; name remapping preserves behavior binding,
route order, tag membership, state semantics and revision identity. Read-only
JSON/DOT/Mermaid exports escape caller text and require no external process.
Empty import prefixes retain node names. Nonempty prefixes and the combined
node identities must use the canonical identity grammar, including its
128-byte bound; validate these before expanding nodes or edges. Composition
admits at most 1,024 imports and 4,096 connections, retains the ordinary
1,024-node limit, and bounds the growing encoded document to 1 MiB between
imports. Compatible state declarations merge in import order; conflicting
values/reducers and different content under one id/revision fail. Connections
append to existing branch order and receive ordinary default/reference checks.

Graph exports use a 1 MiB bound on the UTF-8 bytes of the selected document.
JSON includes source/metadata, while DOT/Mermaid contain display labels and
edges. A large source projection must not prevent a small label-only export.
Conversely, a format whose escaping expands its own output beyond the bound
fails even when another format succeeds. Tracking JSON requires actual source
metadata and omits end edges; no synthetic terminal action is introduced.

WorkflowDefinition.graph is an unbound authored projection. For an unresolved
callable node it preserves the authored id/name/description and explicit tags,
but exposes no guessed declaration reads/writes/inputs and leaves source None.
Serialized reference JSON is not the callable's source and cannot make its
tracking export succeed. At run registration the existing compiled-definition
owner resolves that same projection against the exact selected declaration;
Registered borrows the bound GraphDescription, with actual declaration
reads/writes/required inputs/optional inputs/source in their original order.
The graph node id remains the assigned action name, independent of binding key
and optional display name. Missing actual source still makes tracking_record
fail, following pinned `tracking/models/graph.rs:31–53`.

For callable nodes, omitted authored tags inherit the selected declaration's
tag vector. Explicit tags replace it, including an explicit empty array which
clears it; ordered duplicate tags remain observable. This follows pinned
`action/function.rs:505–509,755–762`, not an invented tag union. The compiler
must retain omission versus explicit emptiness. Bound graph projection,
registration and tag-based execution selection use one effective tag list.
Composition prefixes graph-local node references only and preserves exact
callable pairs and tag overrides. Binding/projection never starts an action or
evaluates a predicate, never widens the public graph DTOs, and obeys the existing
metadata and aggregate byte/count bounds. Condition declared reads participate
in required-read validation, not in the outgoing node's own action read list.

Authored tags on every node are arbitrary data strings, including empty,
whitespace, Unicode and control-containing strings; identity syntax does not
apply. A present tags field must be an array of strings, never null. Each array
admits at most 4,096 entries and each string at most 65,536 decoded UTF-8 bytes,
with the existing document byte bound still applying. These finite bounds are
the same deliberate App resource reduction as action declaration metadata.

## Ordered immutable state and numeric semantics

One WorkflowState owner preserves insertion order through construction,
assignment of an existing key, delete/reinsert, subset/merge, serialization and
reopen. Do not sort state solely to simplify fingerprints, and do not maintain
two independently mutable ordered/raw maps. Fingerprint normalization is a
separate private responsibility. Operations return new state, retaining the
App's concrete typing owner throughout all engine paths.

For primitive changes (assign/append/extend/increment/remove/wipe): validate_pre → apply operation to an isolated
candidate → validate_post(before,after,change) → validate_state →
validate_roundtrip. Default roundtrip calls construct_data then construct_state
and checks success, not raw equality; JsonState deliberately skips conversions.
Merge and subset construct snapshots and call only validate_state →
validate_roundtrip, matching pinned `state.rs:220–222,286–315`. New construction,
rebinding, restored/forked snapshots and completed reducer snapshots use this
full-state validation lane; actual primitive overrides retain their operation
callbacks. Schemas alone do not replace typed construction/transition validation.
No failed conversion, codec, required-key or byte/depth check partially commits.

The exact typing value transferred into App configuration validates initial
input state before the first run record is published, including child input
state. Each staged primitive candidate uses that same value. Parallel siblings
validate their isolated candidates against the committed barrier base; the
completed reducer result then runs full-state validation and roundtrip before
publication. This final snapshot validation does not replay primitive pre/post
callbacks. Cancellation settlement uses the same reduction validation and must
not reapply a rejected staged operation. A rejected mutation or reduced
candidate retains the prior committed state and reports a State failure; a
caller validation panic during admitted execution uses the owned task's
Execution failure path. Initial input rejection returns before a run is visible;
a root start/prepare validation panic at that boundary returns a redacted
Integration error while the App remains usable. The narrow synchronous catch
borrows only the uncommitted initial state and typing value; it does not catch
over a partially mutated App or change admitted-driver/child panic semantics.
Constructed Data is consumed
and dropped inside its synchronous validation call, never retained in an App
owner or across an asynchronous wait.

Arithmetic follows rurr numeric promotion and error behavior. In particular
`5 / 2` is `2.5`, and `-5 % 2` is `1`. Division by zero, incompatible types,
nonfinite output, invalid slices and overflow produce the pinned error class.
Negative-denominator modulo/floor division, mixed signed/unsigned/float values,
large integers above 2^53, MIN/-1 and exponent limits require exact oracle cases.
A blanket f64 cast is forbidden because it loses integer equality/order.
Truthiness and short-circuiting, chained comparisons, null versus missing,
container membership/errors, identity restrictions, negative indexing and slice
strides follow pinned source-derived tests rather than intuitive approximations.

Sibling reducers are an App extension distinct from an action's own ordered
state operations. Each reducer has a test truth table for prior state, zero/
one/many writes, conflicting types/removals, required keys and overflow.
Collect gathers canonical contributions; append_array extends the prior array;
sum_integer includes the prior integer; merge_object merges prior then canonical
objects with later canonical keys winning; reject_concurrent rejects independent
multiple writers. Multiple changes from one action remain ordered action
semantics, not falsely classified as concurrent siblings. Barrier failure is
atomic and never publishes a partial reduction.

## Global field and tagged serialization

StateSerialization is one process-global field/tag owner, matching rurr's
`__burr_serde__` and registration/replacement semantics. Every encode/decode
operation snapshots registered handlers, releases locks, then invokes them.
Existing Apps observe later registrations on their next serialization boundary.
Private erased function storage is restricted to field/tag transformations;
there is no Any, native object, general plugin registry or lower backend ingress.

Field serialization returns an object. Field decode takes precedence over
generic tagged decode for each top-level field. Only ordinary untagged objects
and arrays recurse. A tagged object passes whole and undecoded to its registered
handler; neither its children nor the handler's returned value are decoded
again. Field decoder outputs are likewise terminal transformations. An identity
tag handler therefore succeeds without a synthetic cycle error. Tag names are
exact strings, including an explicitly registered empty string. Missing tags
and nonstring dispatch keys fail with the pinned category and type context.
Generic serialization preserves tagged JSON unchanged.

Captured handlers, replacement, concurrent snapshot consistency, container
depth bounds and cross-App visibility are required tests. Callback invocation
and destruction of replaced captures occur outside the registry lock. Each persistence restore performs decode → caller overrides →
runtime metadata → final typing validation. Serialized payloads carry any
caller codec version; callable identity is never persisted or compared. User
handler errors retain required context at the explicit boundary but are not
logged through safe operational tracing.

## One transactional execution kernel

Automatic start, foreground local execution, synchronous execution, stepping,
iteration and streaming share one transition kernel and one mutation protocol.
An exclusive non-Clone execution owner claims the existing run generation;
another handle/open_run cannot advance concurrently. Read-only snapshots and
history may coexist. Construction is lazy; invalid selectors/inputs fail before
any action, effect, state change or hook. Inline execution permits non-Send
streams and typed Data. SyncExecution blocks the same Execution futures on its
calling thread with the captured runtime handle; it creates no thread/channel
and invokes only synchronous hooks. Background streams and typed Data are
constructed, polled and dropped on the existing supervised execution worker.
Asynchronous App-native Child batches, including those reached by a foreground
Execution, also construct, poll and drop the caller executor's local future on
that worker. The parent's caller Action source stays on its polling thread;
this Child handoff does not widen its Send contract. Only Send intents, owners
and results cross the worker channel. Synchronous Child invocation stays on its
calling thread and does not use this asynchronous handoff.

An unavailable action execution mode or runtime override of a bound input is
rejected before source construction and step/stream hooks. Foreground execution
returns InvalidInput; after background run admission, the same rejection is a
durable Execution failure carrying the stable declaration message, with no
attempted action position, state change or outcome. Missing transient inputs
remain a separate actionable rebinding requirement.

`ExecutionOptions.state` is an explicit initial-state patch only for a new run's
admission through `Workflow::start`. On an already admitted run, both execution
entrypoints require it to be empty and return InvalidInput before claiming an
execution generation, invoking hooks or constructing/polling an action source
when it is nonempty. Even a patch equal to the saved state is invalid; neither
silent omission nor an implicit merge/replacement is permitted. Checkpoint,
committed state, prior outcomes and halt position remain unchanged, and a later
empty-state execution can claim that same saved position. Runtime inputs remain
the separate reusable transient map. This is App's explicit reuse rule for its
single options DTO: pinned `application/execution_options.rs:57–60` has only
inputs/halts, `application/session.rs:635–667,746–754` executes from the existing
committed state, and `application/builder.rs:324–330` owns initial state.

A serial step commits one action; an App broadcast step commits one canonical
ready wave and returns ordered outcomes. One immutable base supplies every
ready sibling. Admit each independent ready control under bounded fair capacity,
stage results separately, retain every parked/unknown sibling, and apply reducers
only once the required barrier can settle. An input/sleep/notification cannot
block unrelated siblings from entering. Completion timing never controls
state order, action ordinals, JoinFirst winner selection or routing.

Caller sources in one ready wave are polled fairly on the same execution owner:
an earlier Pending source cannot prevent a later source from entering or
settling. Their attempted contexts reserve potential ordinals in canonical
admission order and remain unchanged across source requests and all Step/Stream
events. A preceding unresolved caller occupies its reservation even if it later
fails or reaches EOF; an already failed native primitive consumes none before
the next activation starts. Successful barrier outcomes still receive dense
canonical ordinals. CHECKPOINTS.md owns the sparse failed-position proof.

A later caller source's terminal error does not abandon already admitted,
earlier pending siblings. Drive those sources under their original deadlines
before choosing the canonical primary failure. An earlier source error takes
precedence; an earlier successful terminal or EOF does not erase the later
actual error or renumber its context. The failed wave commits no sibling state
or successful outcomes. These are App-native BSP extensions; they do not claim
parallel source behavior from the serial pin.

A source terminal error already observed while another source's item hook
publication is pending retains its original failure identity through cancelled
observation or owning execution Drop. Deferring its publication behind that
original receipt cannot erase the known failure. A later hook failure remains
the execution-completion error with the earlier source failure as its cause;
the failed wave still commits no sibling state or successful outcomes.

Expiration of an already constructed earlier source during a later activation's
admitted pre-hooks likewise retains that earlier Timeout failure, even before
any source poll. The original pre-hook receipt still settles; its later failure
is the Hook completion error with the earlier Timeout as its cause.

Successful action publication precedes evaluation of its outgoing conditions.
A serial step returns its committed action result/state/ordinal before resolving
that next transition. Background execution and finish resolve the outgoing route
on their next kernel advance. If that condition is invalid, fails, or panics,
the run fails with the owning evaluation/execution category while retaining the
already committed action state and outcome; it never reruns or rolls back that
action. This ordering follows pinned `application/session.rs:873–909`,
`application/session.rs:746–754`, `application/state_meta.rs:85–91`,
`application/step_txn.rs:710–744` and `graph.rs:368–376`.

App applies the same ordering to its BSP extension: first commit the complete
successful wave and canonical outcomes, then resolve its outgoing routes from
that committed state. A route error preserves the entire committed wave. The
checkpoint's existing data-carrying phase retains the exact selected completed
frontier entries awaiting routing. Recovery routes those entries once through
the existing kernel; it must not re-select JoinFirst winners against the now
updated settled-join facts, rerun actions, or reconstruct results. Route control
publication preserves the action base sequence, state and ordinal/outcomes.
Method-specific halt ordering still determines whether the caller's next
operation needs to evaluate that route; an action commit does not eagerly do so.

Control/checkpoint sequences fence every durable transition. Action ordinals
start at zero and advance only with committed actions, allocated in canonical
ready order for BSP. Intent, notification, retry and cancellation checkpoints
preserve user state/action ordinal. Staged sibling output is never presented
as committed state. JoinFirst cancels losers only with truthful external-effect
accounting; Unknown losers may keep the run nonterminal until reconciled.

Every successfully committed named activation consumes exactly one ordinal:
Action, Agent, State (including empty or multiple mutations), resolved Input,
completed Sleep, Child/Map (including a successful empty map), Pipeline, and an
eligible JoinAll/JoinFirst visit. Each child run owns its own ordinal sequence;
its parent consumes one ordinal for the completed child-composition activation.
Routing and end targets, parked controls, partials, retry/notification/cancel
receipts, failed or no-output EOF attempts, aborted work and suppressed JoinFirst
losers consume no committed ordinal. Allocate consecutive BSP ordinals only
when the complete required wave commits, ordered by (node, lineage), and return
outcomes in that same order. Failed or partially settled waves publish no new
committed action ordinals. This is App's explicit native-node mapping of pinned
`application/state_meta.rs:94–100` and `application/step_txn.rs:95–126`, which
reserve one sequence id per action and restore prior metadata on rollback.

Halting is a resumable execution boundary, not cancellation/input. Selectors
accept exact action ids and `@tag`, validate all selectors before effects,
preserve explicit/tag order and deduplicate first occurrence. Both lanes execute
the first action of the current session before checking halts against historical
position, including after reopen at a nonzero action ordinal. Only a current
session's committed action enables subsequent halt checks; partials and
successful no-output EOF do not. The pinned streaming resume cases are
`tests/contract/application_streaming_contract/resumed_halts.rs:11–115`;
`application/streaming/step_lifecycle.rs:441–448` records that commit evidence.

The public advancement method selects the pinned halt lane. `next` exposes
streaming iteration: after a commit, test halt-after-prior, then exhaustion,
then halt-before-next (`application/session.rs:187–210` and
`application/streaming/step_lifecycle.rs:237–251`). `step` drains one boundary
using ordinary iteration order; `finish` and background start drain using run
order. Those ordinary operations test halt-before-next before halt-after-prior,
then continue or exhaust (`application/session.rs:169–184,335–374,489–514`;
`tests/contract/application_run_contract.rs:428–465,546–583`). Ordinary iteration
and streaming iteration are distinct pinned capabilities even though they share
the same action source and commit kernel.

For a source partially observed with `next` and then drained by `step` or
`finish`, the method that commits the boundary selects the halt order. Once
that boundary's halt decision is retained, later calls expose or drain the same
decision without reclassification or another action dispatch. The next event
after its Committed event exposes Halted once; subsequent event advancement is
exhausted for that session. A new explicit execution resumes saved next work
with fresh current-session commit evidence and does not replay a committed
action merely to recreate its halt output.

Halt-before identifies the pending node and no result; halt-after identifies
the completed node and its exact result. The same retained tuple appears in
ExecutionEvent::Halted and RunStatus::Halted, including snapshots returned by
finish, background wait and reopen. Paused remains ordinary parked-control
status. A snapshot containing only Paused and committed outcomes would lose
rurr's selected run-output identity; the existing data-carrying status enum
preserves it without a new result wrapper. Never insert a pending node into
ActionOutcome or increment its committed ordinal to manufacture that output.
Persist the selected halt decision with the ordinary state-preserving control
transition so delayed observers and reopened runs see the exact same tuple.

Required oracles include A→B with before B plus after A: streaming next returns
Halted(A, Some(A result)); ordinary step/finish/background returns
Halted(B, None). Both commit A exactly once and never dispatch B. A new session
after committed A still executes B once when before B or after historical A is
selected; in A→B→C, current-session after B or before C then stops before C.
Exercise both sync/async facades, partial-to-step/finish mixing, retained-decision
stability, empty terminal result maps, first-node/self-loop behavior, tags and
duplicates, exhausted frontiers, no-output EOF, and exact halt tuple after
checkpoint/reopen. Source-derived fixtures must not claim executed-rurr proof.

For App-native BSP, halt decisions occur only after the entire required current
wave settles and commits. The after candidate is the first matching committed
activation in canonical (node, lineage) order; the before candidate is the first
matching eligible activation in the next complete ready wave in that same order.
`next` checks after, exhaustion, then before; step/finish/background check before,
after, then continuation/exhaustion. Selector/tag order affects validation and
deduplication, not winner selection. Completion timing and executor return order
never choose the representative. Halt-before parks the whole next wave before
any of its members are dispatched. Halt-after retains every committed outcome
of the current wave, including canonical siblings after the representative.
First-current-session bypass applies to the first wave, and retained decisions
remain immutable on method changes or reopen. With one activation per wave this
reduces to the serial pinned rules above; rurr itself does not define BSP halts.

The selected halt-after representative can precede the wave's final canonical
outcome. last_action_ordinal and committed checkpoint position still identify
that final canonical action; the Halted tuple identifies the selected stopping
condition. Failed, Unknown, unresolved or suppressed work is not a committed
halt candidate. A pending required sibling prevents successful wave publication;
halting never fabricates its completion or discards its effect ownership.

## Lazy streaming and finalization

An action source does no external work in start or before first poll. One
source serves synchronous terminal, async terminal and partial actions. Partials
are transient results; user state and action ordinal stay unchanged. Exactly
one terminal event stages final writes/result; validation and durable publication
precede the public committed result. The first terminal event closes and drops
the source without another poll, as pinned `streaming/step_core.rs:401–420`
and `:495–501` require. An unconsumed source tail cannot authorize another
settlement; repeated settlement of the closed activation fails its generation
fence. EOF without a terminal or another authoritative failure in the wave
discards staged output and returns successful
no-output, preserving the prior state and position so a later step can retry
the same action; it does not invent an action failure or success checkpoint.
The original publication receipt first closes the uncommitted wave's retained
Step/Stream guards with no outcome, then discards caller Action and recomputable
State/Join stages from that uncommitted wave. Already accepted Agent, Input,
Sleep and Child results retain their staged evidence for later explicit
collection; this publishes no parent state, action result or ordinal and does
not repeat the child executor or an earlier committed wave. If a sibling
still owns an accepted Agent/Child continuation or native publication, retain
the sampled EOF and service that original owner before closing its guards.
Pure Input/Inputs/Sleep reservations do not require an answer or timer expiry
merely to publish this rollback. A handed-off Agent Unknown remains durable
and actionable; no-output rollback does not claim its effect has settled. Cancelling
only the observing `next` future retains that receipt; resumed observation
returns None before a separate advancement may retry. `finish` after this
observed EOF finalizes the execution without replay. Background EOF publishes
Paused and requires explicit fresh execution to retry. An outward returned
EOF None ends the completed cleanup chain, so caller idle time does not consume
the later execution-finalization budget; uninterrupted finish retains its
shared cleanup deadline.
An explicit terminal Err of any AppError category is an Action failure;
captured construction or polling panic is an Execution failure. App also contains
caller-source destruction panics as Execution failures before terminal publication;
a destruction panic never erases an earlier source, validation or timeout failure.
An App-owned activation deadline is a Timeout failure. Notification-driven
authority reads and their existing read retries share the source's original
sampled deadline. Only read observation is cancelled at that deadline; admitted
publication retains its existing finalization owner and exact revision fence.
These outcomes and pre-commit abort
discard the staged state and preserve the prior committed action ordinal;
none automatically retries the source. The pinned
`application/streaming/step_core.rs:157–167,245–258,358–365` distinguishes terminal
error from successful no-output EOF, and `:464–489,505–531` rolls back captured
construction and polling panics. Owning drop/abort restores the uncommitted
boundary as `:656–678` requires. No terminal-text chunking, eager collection,
hidden drain or unbounded buffering satisfies streaming.

Before storage commit admission, owning-stream drop revokes the execution
generation and discards uncommitted state. After admission, native commit
finalization retains authority; receipt/head reconciliation proves committed
success or uncertainty. Observation loss cannot promise rollback or replay.
External effects are not undone by state rollback. App-owned recorded lower
intent or admitted-publication uncertainty remains until exact evidence or
explicit caller settlement. A caller Action source's first poll, error, panic,
timeout or drop alone cannot establish such App-owned evidence. Its terminal
error is authoritative source failure; EOF is authoritative no-output, not a
claim that caller I/O never happened. Caller actions own their internal I/O,
independently spawned tasks, cancellation and external reconciliation. A later
explicit execution may repeat those caller effects, so idempotency remains the
caller action's responsibility. Caller-local streams are dropped locally; only
Send-safe App-owned effect/reconciliation evidence transfers to App supervision.

Ordinary WorkflowRun observer drop remains noncancelling. Owning Execution
Drop stops later admission, invokes only the synchronous hook lane, signals
owned cleanup and performs no async I/O/block_on/runtime creation. At an
actionable boundary, consuming finish/abort revokes caller-local source work,
then drains documented async hooks and already accepted child/native work
under their original deadlines; closing admits no new group or Pipeline
successor. Normal finish continues advancing sources until an actual boundary.
Timeout ends observation without detaching commit/cleanup authority.
Agent ordinary requests use ConversationBackend::prompt; explicit incremental
requests use prompt_stream. An Unsupported opening error does not
prove whether the lower prompt accepted work. Preserve Unknown without a unary
fallback; interactions and abort remain responsive while output observation stalls.
Once prompt_stream returns an accepted stream, an error item or premature EOF
does not prove a terminal prompt outcome, even if the error variant could have
described pre-admission rejection. Preserve Unknown until exact terminal or
caller settlement evidence is available. Completed supplies terminal output;
existing cancellation and abort settlement authority remains independent.

## Supervised background execution

The worker lane constructs local streams, typed values, child futures and the
executor's associated future on its owning OS thread. Only typed Send intent,
Arc-held Send+Sync behavior owners and portable results cross the boundary.
Poll the same kernel with the App's captured still-driven runtime handle;
construct no replacement runtime and infer no reactor independence from Send.
Caller-supplied lower runtimes stay driven through App shutdown. A LocalSet is
needed only if the kernel truly spawns local tasks; direct polling is preferred.

Register cancellation and generation before thread spawn; the App supervisor
owns the join handle. Bounded command closure/Drop revokes later admission;
explicit finalization, App shutdown and root-drop supervision retain join
responsibility. Join off the runtime worker through a tracked blocking join,
never self-join or discard an unjoined handle. Inline children must not create
recursive blocking worker dependencies that deadlock bounded capacity. All
control/cancellation paths remain serviceable while awaiting child completion.
Sync calls made within an entered Tokio runtime context reject before admission or blocking. Noncooperative
blocking user callbacks violate the contract; Rust cannot force-kill them.

## Attempts, retries, children and failure

One existing attempt owner carries sampled whole-activation deadline,
cancellation, exact public workspace/conversation identities, effect phase,
target generations and independent resolution/abort evidence. Every setup,
subscription, prompt/open-stream, resolve, abort and recovery await shares the
sampled budget; bounded cleanup grace is separate. Retries do not reset it.
Never await lower calls while holding synchronous locks or transactions.

Retry forms are never, immediate, fixed and exponential; max_attempts counts
the first attempt, elapsed ceilings include backoff, and arithmetic is checked.
Schema acceptance requires actual runtime applicability for that node/failure.
Only proven-safe failures repeat; manual Retry consumes the same budget, and
uncertain Agent/action effects never retry automatically. The attempt policy
applies to lower preparation or required-read failures proved to precede
conversation creation. Once a prompt has been dispatched, `Busy`, `InvalidConfig`,
`InvalidInput`, `Unsupported` and `NotYetImplemented` cannot authorize automatic
replay, including when opening a stream returns no handle. Category alone does
not establish pre-admission rejection; accepted-stream failures of every
category remain uncertain. Recovery preserves
backoff/deadline/counter truth rather than sampling a fresh attempt, and checks
the elapsed ceiling again before dispatch after a delayed wake or reopen.

Child admission atomically records identity, checkpoint zero, parent relation,
capacity, exact definition/materialization and parent control before activation.
Only exact Applied receipt activates. Unknown retains capacity and reconciles.
Task identity matches rurr equality/reuse from parent identity plus task_key,
even across different parent actions: do not add node identity to avoid alias
reconciliation. Identity reuse is not execution deduplication. Every newly
admitted parent occurrence executes its child from new inputs unless an explicit
initializer selects saved state. A persister alone does not resume it. Recovery
of the same already admitted occurrence reattaches its exact receipt and child;
it never creates another invocation. Equal keys within one batch still represent
distinct input occurrences and execute independently under the ordinary executor
policy. The existing child head creation token is unique per occurrence, even
within one batch; one shared mutation-receipt token cannot distinguish equal-key
siblings. Sample tokens once before native admission and retain them through
Unknown reconciliation. RunId remains derived only from parent RunId/task_key;
workflow, revision, node, inputs, clock and occurrence token cannot salt it.
Validate completion membership by the admitted occurrence, not by task
identity alone. Deletion does not permanently retire a task key or create cached
success for its next invocation.

Pinned `tests/contract/parallelism_contract/tracking_and_persistence.rs:525–708`
proves changed parent inputs, repeated stable child identities, repeated effects
and fresh semantic sequence zero without an initializer. Registration
deduplication in the same member at287–405 does not suppress those effects.
Equal-key batch behavior follows the unfiltered task vector in
`parallelism/executor.rs:89–109`; it is source-derived, not an executed oracle.
Original input ordinals determine reduction, never completion.
Custom ChildExecutor receives the complete admitted non-pipeline batch once,
controls actual poll order/concurrency, and returns opaque completions that App
validates for missing/duplicate/foreign membership. An ordinary error
returned by the caller executor is a Child failure, independently of the
AppError variant that an input/admission operation would classify differently.
An executor construction or polling panic is an Execution failure. Neither
failure reduces the parent from partial child results; existing descendant
cleanup retains ownership before terminal publication. The batch's mode is the
actual Synchronous or Asynchronous invocation, never Both. The same concrete
ChildBatch Future supplies InlineExecutor's default: asynchronous mode polls
the entire admitted vector and awaits every result, including siblings after an
error, before collecting Results in original input order; synchronous mode
polls sequentially and stops at its first error. Existing child capacity and
deadline/cancellation ownership still bound actual effects. These semantics
follow pinned `parallelism/executor.rs:89–110` and must be proved with child
first-effect ordering, an early error and a gated later child.

Storage read disposition survives portable error redaction. A typed Permanent
failure to load an admitted child's own execution authority cannot become an
automatic transient retry merely because it projects as Integration. The
existing fatal cleanup owner closes the App, retains the redacted failure and
joins accepted child work without re-invoking E or admitting that child's next
effect. Existing durable child/parent rows remain recovery evidence. This
pure-read rule does not cancel an admitted native mutation or replace its exact
receipt/Unknown reconciliation policy; authoritative ownership revocation
retains its existing Closed/fence handling.

A read owned by an admitted child activation, including its initial load and
completion observation, uses that activation's original parent deadline and
the finite pure-read ceiling in [`APP.md`](../APP.md). A queued child group's
fresh parent observation shares that bound across its head, checkpoint and
relationship reads before constructing the original executor. If the original
activation has already expired in the queue, its existing fenced native timeout
publication retains authority without constructing E or opening a new read
budget. A
successful pre-hook retains its original callback result while only its fresh
authority read retries under the earlier of the node and parent deadlines.
Expiry cancels only the read observation; without readable authority the same
fatal cleanup owner closes and joins the accepted work. It does not create a
fresh deadline, repeat a hook, or invent a child outcome. Terminal reads keep
the accepted-drain exception specified in [`APP.md`](../APP.md).

A transient parent stop-observation read failure retains that same executor
future while App retries only the read within the sampled activation deadline
and bounded reconciliation budget. A pending read is also bounded by that
deadline. Permanent unreadable parent authority or exhausted observation closes
the App through its existing fatal cleanup owner, retains a redacted failure,
and joins the executor without re-invocation or invented run publication. The
last durable child/parent records remain recovery evidence for a fresh App
generation. An authoritative generation fence instead revokes the old owner.
When a valid read precedes deadline expiry, its exact storage revision supplies
the existing fenced timeout publication; no unbounded final read is required.

After parent cancellation, the ordered descendant-authority reads needed to
settle its original child reservations share one cleanup observation budget.
Recoverable reads retain that scan; permanent or exhausted unreadable authority
reaches fatal cleanup. Native child cancellation keeps its separate receipt
owner and uses the already validated child revision. If a completed native
mutation or closing callback spends the remaining pass budget, later children
resume in a subsequent cleanup pass rather than starting a read with an expired
budget. A read already begun never receives a renewed deadline.

An Execution retains every admitted child group alongside its caller sources;
Child alone is not an interactive pause. After complete ready-set admission
and a fair polling pass, a real root Input, Inputs or notification may make
foreground `next` return None while those original futures remain Pending.
This is an actionable boundary, not source EOF: later `next` resumes the same
source and executor without re-invocation. Already sampled source errors,
terminals, pending item/publication receipts and returned child completions
must be processed before reporting that boundary. Background execution keeps
polling accepted sources, groups and native continuations; public `wait` may
independently return the actionable snapshot. Pure public pauses may finish an
execution observation, but retained Inputs keep the original active options
generation available for exact rebinding.

Cancelling a `next` waiter retains the original child operation, executor and
receipt. Owning Execution Drop prevents a later parent barrier but preserves
accepted asynchronous child work and native commit authority until its tracked
owner settles; that generation cannot be reacquired before the original child
receipt joins. Completed children remain exact evidence for later collection.
Once the original successful group has completed child lifecycle callbacks,
its parent Stage publication releases those exact terminal children's active
capacity atomically with retaining the staged result. This does not commit
parent state or its action ordinal, and does not remove the staged child
membership or its history-deletion dependency. The original native receipt
survives owning Drop and lost observation without replaying E, callbacks,
effects, or capacity release. Explicit history deletion policy still takes
effect only with the later copied parent barrier. Failed or cancelled groups
continue through their original descendant cleanup path.
Consuming finish/abort at an actionable boundary revokes caller sources and
drains already accepted Sync/Async child continuations without admitting a new
Pipeline suffix. The same existing pending schedule, original deadlines and
storage retry disposition govern this work. When execution already runs on the
shared worker, nested asynchronous groups stay in that invocation rather than
consume another worker admission slot.

Pipelines support synchronous invocation only and bypass ChildExecutor, using
sequential state handoff through the same kernel while preserving initializer,
history and override semantics. Asynchronous pipeline rejects before child
admission/effects; no partial pipeline or custom-executor fallback is admitted.
This follows pinned `parallelism/executor.rs:119–136,149–169`.

Success requires terminal frontier, no open or Unknown effect, drained children,
validated final state and released active capacity. Captured panic becomes
Execution failure. If an App-owned lower operation or storage publication also
has recorded possibly accepted work without authoritative completion, preserve
that independent Unknown evidence; polling a caller Action source alone does
not establish it.
Every task/thread join result is inspected and the driver generation released
by supervision even when the task body unwinds. Post-hook failure has rurr's
surfaced precedence while preserving the primary failure as structured cause;
committed actions never replay because a post-commit hook failed.


### Atomic resolved state payloads

`StateChange` primitive payloads each represent one atomic operation. Assign
uses an ordered map. Append, Extend and Increment retain ordered entries,
including repeated keys; Remove accepts repeated and absent names. Validate
pre once, apply all entries to one isolated candidate, then validate post,
full-state and roundtrip once. Empty payloads still run this validation path.
A failing later entry discards all earlier candidate changes. Increment's
resolved JSON delta must fit signed i64. These are the pinned
`state.rs:202–252` and `state/operations.rs:9–112` semantics.

Wipe resolves its keep selector into Remove keys in current state order before
primitive validation; pre/post callbacks observe that resolved operation,
matching `state.rs:262–283`. Merge/Subset retain their separate full-state and
roundtrip snapshot validation path. The syntax compiler creates one-entry
payloads for singular authored mutations; no public singular compatibility
variants, batch wrapper, or second mutation engine remain.

The existing App collection resource limit applies to each primitive payload or
Wipe/Subset selector: at most 4,096 entries, counting repeated names separately.
Exceeding it returns InvalidInput and leaves the original state unchanged. This
is an explicit finite-resource restriction relative to pinned rurr, whose
operation vectors are unbounded. It reuses the existing collection bound, not a
second independently configurable limit. Candidate JSON still obeys the existing
state-key, depth, byte and nested-collection limits, so a unique-key map or a large
result may hit those earlier even when its payload count is within 4,096. Tests
exercise 4,095/4,096/4,097 entries and retained original state.
