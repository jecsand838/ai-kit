# Custom Durable State

Custom stores only agent-owned control-plane facts in `AgentStorage`:

- agent/workspace/conversation identities and revisions;
- the safe configuration and catalog compatibility floor;
- bounded provider-neutral caller/final-assistant history;
- prompt generation, cancellation, waiter, terminal, and recovery state;
- durable notifications and resolution attempts;
- private managed-knowledge references and cleanup obligations.

It never stores Provider, Model, Toolset, ToolHandler, Database, credentials,
callbacks, process environment values, native provider envelopes, or
lower-crate configuration. Provider continuation metadata and intermediate
function messages exist only inside the active prompt loop and are never
reconstructed during reopen.

Every mutation uses the public owned StateStore transaction API. Exact
`TransactionConflict` may be retried or reconciled only where the operation
contract permits and only within its existing lifecycle deadline. All other
StateStore errors surface unchanged. Ambiguous commit responses are accepted
only after an exact durable read proves the intended value.

Process-local persistence coordination is split by responsibility. The
mutation owner serializes StateStore changes and retains each spawned task
through an exhaustive completed-or-interrupted exit and finished join. Its
result channel carries only the operation value, never lifecycle truth. The
provisioning owner separately tracks active conversation admissions and the
specific recovery-required state left by lost cleanup. Neither owner shares a
generic task/result carrier with terminal reclamation.

Reopen requires callers to resupply lower-crate handles. Custom validates the
safe compatibility floor and durable state before publishing a live route.
Lost process-local owners are never recreated from speculation: committed work
is reconciled, unaccepted work is released, and ambiguous accepted work stays
sealed until its durable outcome is proven.

Terminal conversation drain is its own durable process, not a generic state
mutation mode. It waits for the notification journal's terminal reclamation,
settles the conversation knowledge-forget obligation, and only then removes
the closed conversation record. One tracked owner retains the task handle and
its exhaustive completion or interruption result. The terminal-drain registry
strongly retains that exact owner through caller loss and removes only the
matching registry entry after its task has joined. Shutdown waits for all such
owners, and reopen completes a durably staged close that lost its process-local
owner. A receiver-held owner retains the configured lifecycle duration, not an
acquisition-time deadline. Its cleanup attempt samples one absolute deadline
at handoff immediately before exact registry admission and reuses it through
notification reclamation, knowledge forget, and closed-record deletion.
Workspace and Agent settlement pass their already sampled deadline into any
admitted drain; expiry aborts and joins remaining tracked tasks instead of
opening a fresh settlement phase.
