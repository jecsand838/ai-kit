# Hand work from one Agent to another

Configure one `AgentSpec` for each logical role, then select those keys in Agent
nodes. An App inventory is homogeneous in its Rust Agent type. Different values
can use different configuration; different Rust implementation families use the
[matching caller enum hierarchy](TRAIT_INTEGRATION.md).

Use a committed state target for information the next node must read. An Agent's
terminal result always retains its text and any structured output or usage, but
untargeted result fields do not become workflow state automatically.

## A structured A → B handoff

This complete workflow asks `author` for an artifact and supplies the committed
artifact to `reviewer`. Both keys must be present in the App configuration.
Each initial state key has an explicit reducer; the structured state target
also has a schema.

Read the complete [sequential handoff definition](examples/multi_agent_sequential.json).
The example keeps the source JSON in one file so the parser acceptance test uses
the exact document linked here.

The [multi_agent executable](../examples/multi_agent.rs) constructs two Agents
against caller-run OpenCode services with separate memory stores, then passes
them to App. Supply two service URLs and an existing workspace directory as its
three arguments. Configure each service's model and credentials before running
it. The example awaits App shutdown; it does not stop the caller's services or
claim process durability from its memory stores.

The route becomes eligible after the author's successful barrier commit. The
reviewer receives the artifact from that committed state. Intermediate output
deltas are transient and cannot serve as this state handoff. Inspect the final
run status: an Agent notification or uncertain lower effect can require caller
resolution rather than yield success. Use the observed control sequence and
target generation for each resolution; the run checkpoint instead fences
cancellation. Uncertainty alone never authorizes replay.

Create the App workspace before admitting the run. App materializes a distinct
lower Workspace for every configured logical Agent key/revision pair and uses
that pair's associated Conversation for its calls. The same public lower ID
appearing under different pairs does not merge their ownership.

## Deploy revisions without changing existing runs

Revisions are caller-chosen opaque strings describing compatible lower
configuration. For a logical key, the last supplied distinct revision selects
new runs. For example, supplying `author/z-old` followed by `author/a-new`
selects `a-new`; lexical order and semantic-version order have no role.
Duplicate key/revision entries are rejected.

A prepared or active run retains its admitted Agent revision bindings, including
an Agent node it has not reached yet. Supply those old revisions alongside the
new ones when reopening. Restoring a saved checkpoint retains its source
bindings. A missing required pair rejects open before the new generation is
claimed or lower effects begin. Reusing the same revision for incompatible
configuration violates the caller contract because App cannot inspect it.

Newly supplied pairs extend existing active workspaces through their creation
journal. Proven older materializations remain intact, preserving their prepared
runs. App shutdown visits every transferred pair once in canonical key/revision
order; selection order for new work remains caller order. See
[persistence and recovery](PERSISTENCE_AND_RECOVERY.md) for reopening the App
store and lower Agent stores together.

## Independent A/B work

A route with `mode: "broadcast"` can select two independent Agent nodes in one ready
wave. Give them separate state targets, or choose an explicit reducer for a
shared target. Both activations read the same base checkpoint. One sibling
cannot read another sibling's speculative write; introduce a later node for
that dependency. Successful contributions reduce in canonical node order at the
barrier. The [state and parallelism guide](STATE_AND_PARALLELISM.md) explains
those state semantics.

The complete [parallel review definition](examples/multi_agent_parallel.json)
uses the same `author` and `reviewer` bindings as the sequential example. Both
read `topic`, write separate `draft` and `risks` targets, and converge at a
`join_all`. The reviewer in this graph evaluates the original topic; reviewing
the author's new draft requires the sequential handoff graph.

A graph containing a ready A/B wave does not establish simultaneous native
prompt execution or a throughput guarantee. The sequential real-service test
below does not prove parallel native Agent dispatch. Capacity, deadlines,
notification handling and lower backend behavior must be verified for the
actual deployment.

## Recovery and real-service evidence

The strict-live test
[`two_real_agents_handoff_after_process_loss_and_drain_once`](../tests/workflow_live.rs)
uses two separately owned OpenCode services. It commits Agent A's structured
artifact, parks at an Input, terminates the test-owned process, reopens both
lower Agents and the App, and then releases the handoff to Agent B. Assertions
check the exact structured value, no repeat of A's prompt, one B prompt,
separate lower conversations and one shutdown for each transferred Agent.

Run the configured suite through `bash app-sdk/scripts/verify_strict_live.sh`
from the workspace root. The script and `strict-tests.env.example` define the
required environment. The test's presence is not evidence that it passed on
your current checkout. Deterministic caller-Agent fixtures prove composition,
revision selection and cleanup contracts, while service behavior and process
persistence require the live run. A config containing two unused Agents proves
neither a handoff nor parallel execution.

Always await App shutdown and keep the runtime driven through its completion.
A timed-out shutdown observation can be resumed; it does not transfer cleanup
to a newly constructed Agent. Persist public workspace/run identities and the
caller configuration needed to reopen exact bindings rather than serializing
runtime handles.
