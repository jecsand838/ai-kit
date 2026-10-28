# Workflow authoring and graph inspection

`WorkflowDefinition::parse` validates a versioned JSON workflow before runtime
admission. Graph inspection and composition use the same validated definition;
they do not need an App, an Agent, a StateStore, or an external renderer.

## Author a workflow

A document supplies its schema version, logical `id`, caller-chosen `revision`,
initial `state`, `required` state keys, `reducers`, `entry`, `nodes`, and `routes`.
The entrypoint names a node. Each node has an `id` and a tagged `step`; optional
metadata describes the node without changing its identity. A revision identifies
one exact definition. Change it when changing the graph or its callable bindings.
Every initial state key needs a reducer declaration. A dynamically introduced key
without a reducer uses the ordinary reject-concurrent policy.

Node order is retained in graph descriptions. A route's branch order controls
first-match selection. A `first` route takes the first true branch, including an
unconditional branch wherever it appears; later conditions are not evaluated.
Two unconditional branches on the same first-match route are invalid. A missing
route, an empty route, or no matching branch ends that path. A `broadcast` route
selects every matching branch and creates an explicit parallel wave.

For example, this route continues to `review` only when `needs_review` is true:

```json
{
  "mode": "first",
  "branches": [
    {
      "when": {
        "kind": "equal",
        "left": {"kind": "state", "path": "needs_review"},
        "right": {"kind": "literal", "value": true}
      },
      "target": {"kind": "node", "id": "review"}
    }
  ]
}
```

The compiler checks references and graph relationships before admission. A valid
JSON shape alone does not establish that a graph is executable. Cycles must still
obey the workflow's execution and join rules; do not use a retry policy to encode
a transition that belongs in the graph.

Only Agent nodes accept non-null `retry` objects: `never`, `immediate`, `fixed`
or `exponential`. Every other step kind rejects these objects, including
`never`. Omitted or null retry is valid on every node. For an Agent, omitted,
null or `never` allows the initial attempt but no additional attempt.
The complete [retry example](examples/retry.json) gives an Agent up to three
attempts, counting its first, within one elapsed budget. See
[interaction and retry controls](INTERACTIONS_AND_CONTROLS.md#keep-one-activation-budget)
for safe-failure, manual retry and recovery rules.

## Caller actions and transient values

An action node selects a caller-provided `ActionBackend` by its exact logical key
and revision:

```json
{
  "id": "calculate",
  "step": {
    "kind": "action",
    "action": "calculate_total",
    "revision": "r3",
    "project_result": true
  }
}
```

Construct actions before opening the App and pass them through
`AppConfig::with_actions`. An action declaration describes reads, writes,
transient inputs, supported execution mode, tags, and optional source metadata.
App preserves the caller's concrete action and local stream types. A declaration
does not hide undeclared state from the action or prohibit additional explicit
state writes.

Execution inputs are separate from initial workflow state. An action's partial
result is transient; only `Completed` can propose writes and a committed result.
With `project_result: true`, declared result fields are assigned before explicit
writes. The [actions and inputs guide](ACTIONS_AND_INPUTS.md) owns the input
precedence, projection, validation, operation bounds, and effect-ownership rules.

## Native state steps and expressions

A state step applies its mutations sequentially to one temporary state. Later
expressions in that action see earlier mutations; sibling actions see the same
committed base. The complete wave merges isolated contributions at its barrier.
See [state and reduction](STATE_AND_PARALLELISM.md) for conflict and ordering rules.

| Mutation kind | Payload | Behavior |
| --- | --- | --- |
| `assign` | `target`, expression `value` | Insert or replace one key. |
| `append` | `target`, expression `value` | Append one array element; a missing target starts empty. |
| `extend` | `target`, expression `values` | Append an evaluated array's elements. |
| `increment` | `target`, expression `value` | Add a signed integer delta; a missing target starts at zero. |
| `remove` | `target` | Remove a key; an absent key succeeds. |
| `wipe` | ordered `keep` keys | Keep present selected keys in their original state order. |
| `merge` | expression `state` | Merge an evaluated object, retaining existing key positions. |
| `subset` | ordered `keys` | Select present keys in first-requested order. |

Expressions are tagged objects. `literal` carries `value`; `state` carries
`path`; binary arithmetic carries `left` and `right`; unary arithmetic and
`length` carry `value`. Conditions support comparisons, membership, null/boolean
identity, short-circuit combinations and exact custom predicate bindings.
The [owning grammar](../specs/workflows/WORKFLOW.md) defines the full vocabulary
and error rules. No arbitrary host-language code or import is evaluated.

Arithmetic follows the pinned rurr semantics: `5 / 2` produces `2.5`, and
`-5 % 2` produces `1`. Division, floor division, remainder and exponentiation have
distinct behavior. Do not replace them with Rust integer operators. State
increment is a separate integer operation with its own range checks.

A state path starts at one top-level key and uses slash-separated child tokens.
Escape `/` as `~1` and `~` as `~0` within every token, including the root. The empty
path addresses an empty top-level key. Object keys are exact strings; array
indices are unsigned decimal without leading zeroes except `0`. Reading a missing
state value fails when evaluated; the `exists` condition instead returns false.
An earlier action may create a key that a later one reads.
Short-circuited expressions and conditions do not eagerly evaluate hidden paths.

## Inspect and export a graph

Run the service-free example:

```sh
cargo run --locked -p app-kit --example graph
```

[graph.rs](../examples/graph.rs) parses two one-node counter definitions,
composes them into a two-node chain, inspects their metadata, and exports JSON,
DOT and Mermaid. The executable example contains the complete JSON documents.

`definition.graph()` returns owned metadata: the entrypoint, authored node
order, labels/tags, declared state reads/writes and transient input names.
Edges retain outgoing branch order and are grouped by authored source-node
order. An absent edge destination represents an explicit end. Graph metadata
does not retain executable action or condition bindings.

Choose `GraphFormat::Json` for inspection data, `Dot` for DOT text, or
`Mermaid` for a flowchart. The selected renderer escapes caller labels and
conditions. DOT and Mermaid omit end edges rather than inventing an end action.
Export produces text only; rendering, file storage and transport are caller
responsibilities.

`GraphDescription::tracking_record()` produces the separate pinned application
tracking object. Every node must have actual source metadata. Native nodes have
their canonical authored definition; an unresolved caller binding may lack its
source and make tracking export fail. Inspection can still describe that node.
Tracking is an explicit content export and may contain authored source.

## Compose validated pieces

Each `GraphImport` supplies a validated definition and a prefix. The prefix
changes graph-local node references, including imported routes. It does not
rename state keys, action/condition binding keys or lower inventory identities.
Use already-prefixed names for the composition entrypoint and every
`GraphConnection`.

The example imports `add` twice as `first_add` and `second_add`, then appends
the connection from the first to the second. Both pieces declare the same
initial count and reducer. An incompatible declaration is an error, not an
implicit choice of one import's value.

Connections append default transitions after each source's imported branches.
They do not replace an existing default or force execution past an end branch.
The ordinary compiler validates the assembled graph, including reachability,
references, transitions, conflicting names and state declarations. Reusing a
workflow id/revision with different content is rejected.

Composition returns another `WorkflowDefinition`. Use it wherever you would use
a parsed definition; composition does not create a child run or perform state
handoff by itself.

## Finite authoring bounds

Definition input and exported text each have a 1,048,576-byte limit. A workflow
has at most 1,024 nodes. Composition accepts 1–1,024 imports and up to 4,096
appended connections; the assembled graph still obeys the ordinary definition
limits. Prefixed node names must fit the 128-byte canonical identity limit.
Prefixes can therefore make otherwise valid pieces fail before execution.

Node/workflow identities use lowercase ASCII letters and digits, with `_` and
`-` permitted after the first character. Labels, descriptions, state keys and
tags have their own data/metadata rules; do not sanitize them into identities.
Parsing rejects unknown fields and duplicate JSON keys. Rendering a graph does
not turn an invalid workflow into a valid one.

See [state and reduction](STATE_AND_PARALLELISM.md) for value semantics and the
owning [workflow contract](../specs/workflows/WORKFLOW.md) for authoring behavior.
The [public specification](../specs/SPECIFICATIONS.md) is the sole exact API
ledger.
