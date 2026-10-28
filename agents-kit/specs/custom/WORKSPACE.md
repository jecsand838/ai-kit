# Custom Workspace

Custom Workspace owns one committed workspace revision, its live conversation
routes, source-neutral tool inventory, managed-knowledge scope, and filesystem
confinement policy.

Caller-supplied Toolsets, including ordinary shell tools, execute outside a native sandbox.
When any executable action is present, Custom requires the explicit
full-access/network-enabled workspace policy before publishing the Workspace.
This is a build/runtime compatibility check, not a hidden capability branch.

Workspace update preflight is read-only. Mutation commits the complete
revision atomically, preserves private attachment state, invalidates older
Workspace and Conversation handles, and lets callers reopen by typed id.
Post-commit route cleanup cannot turn a committed update into an error.

Workspace close is consuming and idempotently cascades runtime invalidation to
live conversations while preserving quiescent durable records for reopening.
It settles admitted work and interactions before releasing route claims.
Workspace deletion requires the exact current revision and no live or durable
conversation that would make deletion unsafe.
