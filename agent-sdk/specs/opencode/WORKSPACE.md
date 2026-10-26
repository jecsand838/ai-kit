# OpenCode Workspace Domain

This file defines the OpenCode implementation behind the opaque root
`Workspace`. [`../API.md`](../API.md) owns the public surface. The workspace
domain coordinates durable workspace identity, the current portable revision,
the private OpenCode routing context, the bound native-session closure,
workspace-scoped catalog and knowledge state, live conversation registration,
and lifecycle admission.

A portable workspace is SDK-owned directory and policy context. It is not an
OpenCode experimental-workspace record, a native project, a session directory
discovered at runtime, or an app-owned coordination record.

## Open And Revision Ownership

An opened workspace handle is published only from a committed SDK workspace
record and a revision-valid runtime claim. It contains stable SDK identity and
private collaborators, not a caller-visible persistence record. Reopen loads
the existing record, validates backend attachment and ownership, verifies the
native bindings it already owns, and observes the committed revision. It never
creates a replacement native session for a missing or contradictory binding.

Caller cwd and roots remain exact passive backend-host context. The selected
server's `/path` response supplies its canonical directory; the SDK never
canonicalizes that path using its own filesystem. Workspace construction binds
that native identity before publication under the original lifecycle deadline.
Dropping its caller observer does not cancel an already admitted workspace
creation or discard its committed routing authority.

Conversation creation/reopen, catalog discovery, prompt admission, and Agent or
Workspace reopen check the current requested-cwd mapping against the retained
native identity before publishing new work. A changed alias target with the
same caller context is `InvalidState`; it neither changes the portable revision
nor permits a replacement session. Native session, event, correlation, privacy,
and permission checks remain exact. Cleanup and interrupted-creation recovery
retain their original native directory and correlation even if the caller's
alias has since changed or disappeared. Cleanup authority does not depend on a
fresh alias lookup; new runtime publication still requires current mapping.

Workspace update follows the root preflight and mutation contract:

1. validate the complete request and OpenCode representability without
   changing durable or native state;
2. transiently admit the policy-sensitive preflight read;
3. compare the expected durable revision before dynamic readiness;
4. atomically commit the replacement portable core while preserving valid
   backend-private attachment state;
5. publish deterministic revision invalidation; and
6. perform post-commit cleanup without turning the committed update into an
   error.

Workspace creation, update preflight, and update validate complete defaults
after context validation. Non-absent reasoning or personality defaults return
`Unsupported` before knowledge validation, backend requests, durable creation
or replacement, and revision invalidation. A rejected replacement leaves the
complete prior workspace and its runtime handles unchanged; a supported
replacement retains the existing revision and admission rules.

An unchanged cwd is representable. Changing cwd while any durable conversation
remains is `Unsupported`, because the selected server binds routing to the
stored session directory and exposes no honest migration. With no
conversations, a cwd change may commit and future native sessions use the new
context. No implementation may mutate a live session directory, recreate
conversations behind existing ids, or retain a stale handle that silently uses
the old context.

The complete portable context also persists normalized roots and explicit
sandbox/network policy. OpenCode's selected session rule grants unrestricted
native access, so only the portable full-access, network-enabled policy is
representable. Read-only, workspace-write, network-disabled, or root-level
restricted access returns `Unsupported` during preflight before revision or
native mutation. Unrestricted roots are validated backend-host absolute paths
and remain part of durable context and stale-handle invalidation; they are not
reinterpreted relative to the SDK process, projected as experimental-workspace
records, or claimed to restrict the explicitly full-access native runtime.

The workspace uses the shared hierarchy admission owner. No synchronization
guard or StateStore transaction crosses HTTP, SSE, database, tool, process,
timer, or cancellation awaits. A dropped policy-sensitive read releases its
admission exactly once.

## Workspace-Owned Operations

The workspace domain owns workspace info, conversation creation and durable
inventory, conversation reopen by typed id, native catalog projection,
workspace-scoped knowledge ingestion, and workspace close. Durable SDK records
are inventory authority; native session listing is bounded ownership and
cleanup evidence only.

Tool inventory follows [`CATALOG.md`](CATALOG.md). Workspace knowledge follows
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md); ingestion may select Database,
Agent, or Workspace scope and absence selects Workspace. Neither operation
exposes raw native ids, provider details, MCP transport data, storage
implementation, or protocol routes.

## Conversation Creation

Conversation creation is a staged, recoverable operation:

1. validate the portable request, defaults, knowledge policy, and current
   workspace admission;
2. materialize any conversation-scoped managed knowledge and validate the
   configured catalog compatibility without requiring portable inventory
   completeness or choosing a native default model;
3. verify effective sharing is disabled immediately before native creation;
4. durably record one unique private session-metadata correlation and
   ambiguous-create obligation;
5. create one root native session in the workspace directory with that
   correlation, appending the
   selected unrestricted final permission rule;
6. durably commit the portable conversation record, private session binding,
   catalog observation, knowledge integrity facts, and setup obligations;
7. open the conversation runtime, register its owner-checked route, and only
   then publish the portable handle.

Creation validates effective reasoning and personality after reading the fresh
workspace record. Each field resolves from conversation defaults to workspace
defaults; an omitted conversation field does not clear its inherited value.
Any non-absent value returns `Unsupported` before knowledge work, durable
creation intent, or native session creation. Prompt admission still validates
the fresh effective selections independently.

The native session and all sourced descendants form one private ownership
closure for routing, interactions, recovery, and cleanup. Descendants never
become portable Conversations.

Failure cleanup is phase-aware. A lost creation response is never replayed:
recovery exhausts bounded-complete global inventory, requires exactly one
matching private correlation, and verifies that session before binding or
cleanup. Zero matches remain recovery-owned, while multiple matches are
contradictory state. Before durable commitment, a native session created by
this attempt remains owned by exactly one durable record. A valid session whose
binding transaction fails stays under the `Creating` correlation for reopen.
Normal creation retries only a definitely unpublished binding commit under its
original deadline and unchanged ownership, as specified by `STATE_STORE.md`;
this never repeats native creation or broadens Reopen's single-attempt behavior.
An invalid session changes the exact `Creating` intent into the existing
cleanup obligation atomically before deletion. Exact unchanged truth retains
the correlation and exact applied truth retains cleanup; a still-unobservable
handoff performs no DELETE, revokes local authority, and leaves whichever
atomic durable state committed for reopen. Reopen that confirms this handoff settles knowledge and native
cleanup, finishes the durable obligation, and continues the same construction
under its existing deadline. After durable commitment, the operation records
recoverable setup state rather than deleting or replacing accepted state
invisibly. A completion transaction admitted before the operation deadline
settles to an exact durable outcome; once the portable record, bound native
session, and notification owner are confirmed committed, deadline expiry
cannot turn that accepted conversation into failure or admit native-session
cleanup. The original precommit error remains primary. The correlation never
enters public API, caller configuration, or portable defaults.

## Conversation Reopen

Reopen loads the complete persisted portable defaults once, checks workspace
ownership, validates the selected revision and terminal state, and verifies
the existing native root session, its directory, privacy, final permission
rule, attachment, status, catalog binding, knowledge integrity, pending
interactions, and recovery facts.

The effective prompt configuration is resolved later by the conversation from
request overrides, complete conversation defaults, complete workspace
defaults, and backend/default behavior. Reopen must not reconstruct title,
model, reasoning, personality, knowledge policy, or prompt timeout from native
session data.

If native state is active and agrees with a durable running generation,
recovery may dispatch at most one generation-fenced abort. Ambiguous dispatch
remains recovery-owned. Native idle plus contradictory durable running state is
repaired to a non-success terminal fact and reported as `InvalidState`; a lost
exact foreground response is never reconstructed.

## Close And Delete

Workspace close is a runtime lifecycle operation. It seals admission, invokes
existing generation-fenced prompt and interaction cancellation through the
concrete conversation owners, waits for local descendants and cleanup work to
quiesce, and finalizes runtime closure. It does not delete durable workspace or
conversation records.

Workspace deletion is Agent-owned and uses the StateStore sequencing in
[`STATE_STORE.md`](STATE_STORE.md). After the portable deletion commits, every
recorded native root and descendant remains a private cleanup obligation until
bounded-complete global inventory and bounded sync-history verification both
prove absence. Native cleanup failure cannot reverse a committed portable
delete, and a successful HTTP response alone is not proof of cleanup.

## Required Invariants

- One SDK workspace revision owns one current private routing context.
- Native sessions never replace or define portable workspace inventory.
- Conversation publication follows durable binding and live route ownership.
- Cwd migration never fabricates native support or silently recreates sessions.
- Close invalidates runtime handles; delete removes portable durable state.
- Workspace code coordinates domain owners without becoming a second protocol,
  catalog, knowledge, interaction, or StateStore implementation.
