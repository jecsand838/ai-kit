# agents-kit Architecture

`agents-kit` presents one durable portable hierarchy over Codex, OpenCode, and
Custom:

```text
AgentStorage -> AgentBuilder -> Agent -> Workspace -> Conversation
                                                    -> notification receiver
```

## Public Boundary

| Public trait | Supplied opaque handle | Private closed dispatcher |
|---|---|---|
| `AgentBackend` | `Agent` | `AgentInner` |
| `WorkspaceBackend` | `Workspace` | `WorkspaceInner` |
| `ConversationBackend` | `Conversation` | `ConversationInner` |

`AgentBackend::Workspace` and `WorkspaceBackend::Conversation` are associated
types. Supplied handles preserve `S, D, P, T` through associated children. Caller
implementations stay concrete or use caller-owned exhaustive enums; they are
never normalized into supplied handles.

Each dispatcher contains only explicit Codex, OpenCode, and Custom variants.
There is no Agent erasure, normalization ingress, knowledge backend, trait
object, wrapper, factory, production conformance suite, registry, or duplicate
behavioral contract.

## Public Modules

```text
agents_kit
├── AgentStorage
├── AgentBuilder
├── Agent / AgentBackend
├── Workspace / WorkspaceBackend
├── Conversation / ConversationBackend
├── KitError
├── config
├── identity
├── workspace
├── conversation
├── prompt
├── notification
├── model
├── tool
├── knowledge
└── error
```

The modules expose portable caller data by functional domain. Protocol,
transport, runtime, storage, and backend implementation trees remain private.

## Construction

```text
AgentBuilder::create(config, storage)
AgentBuilder::reopen(config, storage, agent_id)
AgentBuilder::create_for_backends::<P, T>(config, storage)
AgentBuilder::reopen_for_backends::<P, T>(config, storage, agent_id)
Custom only: builder.with_provider(P: ProviderBackend)
Any backend: builder.with_toolset(T)* where T: ToolsetBackend
Any backend: builder.with_knowledge_database(D: DatabaseBackend)?
Any backend: builder.build()
```

`AgentStorage<S>::new(S)` is the sole control-plane ingress, where
`S: StateStoreBackend`. Optional managed knowledge accepts
`D: DatabaseBackend`; `Agent<S, D, P, T>`, `Workspace<S, D, P, T>`, and
`Conversation<S, D, P, T>` preserve those types statically and default to the
supplied lower handles. Tool children stay associated as `T::Handler`; roots
and children do not require Clone. agents-kit never normalizes
Provider, Toolset, or storage implementations or owns their credentials, factories,
declarations, or registries. Reopen requires the caller to resupply the
selected integrations. Caller-owned exhaustive Provider/Model/Session or
Toolset/Handler or storage enums may provide optional runtime selection while
remaining the concrete `P`, `T`, `S`, or `D`; they are not Kit wrapper requirements.

The typed native constructors select `P` only so Codex/OpenCode Agents can
share an upper homogeneous inventory with Custom Agents. They retain no `P`
value and perform no provider operation. `with_provider` remains the sole
builder operation that transfers a concrete `P`, and Custom alone requires it.

## Runtime Ownership

- `Agent` owns runtime sealing, durable identity, Workspace inventory,
  model inventory, agent-scoped ingestion, and consuming shutdown.
- `Workspace` owns one revisioned context, Conversation inventory, complete
  tool inventory, workspace-scoped ingestion, and consuming parent close.
- `Conversation` owns foreground prompt admission, abort, file reads,
  exclusive notification delivery, interaction resolution, and terminal close.
- `AgentStorage<S>` owns backend-blind control-plane persistence over one
  caller-supplied `S: StateStoreBackend`.

One current live handle claim exists per Workspace or Conversation. Drop
releases only that process-local claim. Shutdown/close seal admission and settle
accepted work; parent cascade preserves quiescent durable descendants while
Conversation close is terminal.

## Custom Prompt Loop

Custom owns one private prompt loop built on a direct `P: ProviderBackend`.
Ordinary calls use `generate`; incremental calls consume real `generate_stream`
events in that same loop. One bounded prompt output buffer retains text and a
separate terminal slot. Neither observation loss nor consumer capacity can
block interaction handling, abort, or durable terminalization.

```text
P::model -> P::Model generation
         -> normalized function calls
         -> ToolsetBackend::tool_handler -> ToolHandlerBackend::step
         -> validated function results
         -> next model turn
```

The builder transfers `P` once into the Custom composition, which retains it
behind `Arc<P>` without requiring `P: Clone`. The loop selects `P::Model`
directly; the model's Session remains its associated child rather than another
Agent generic parameter. Its associated `Continuation` likewise remains a
method-local prompt type: returned assistant messages carry it unchanged
through that prompt's tool turns, while durable caller/final-assistant history
contains none. The private owner advances transcript state, applies
model/tool/parallel limits, resolves approvals through durable interactions,
represents shell as a local function tool, detects terminal assistant output,
honors cancellation, and shuts down its Toolsets. providers-kit owns one
selected model call and provider protocol behavior; agents-kit owns the
multi-turn agent loop.

Before handler construction or transcript advancement, Custom validates each
complete response's finish reason against its function calls. Failed,
incomplete, paused, or malformed responses terminate through `KitError` and
cannot become a successful durable turn. The exact classification belongs to
[`specs/custom/CONVERSATION.md`](specs/custom/CONVERSATION.md). Completed tools
must provide actual content or structured content; the loop preserves valid
tool failures and never synthesizes replacement result text.

No `LoopDriver`, mount, hook, recovery-controller, provider shell, or provider
approval carrier crosses this boundary.

## Knowledge

Managed knowledge is private. A caller may supply
`D: DatabaseBackend` directly; agents-kit combines it with `AgentStorage<S>` to
own source manifests, atomic source replacement, retrieval, and verification.
The only public knowledge values are source, scope, policy, and ingestion
request inputs. Owner lineage, storage layout, query budgets, retrieved
snippets, provenance, and formatted context remain private.

Knowledge is injected into prompts by the selected backend after private
retrieval. It is separate from StateStore control-plane state and from
model-visible Toolsets.

## State, Notifications, And Cancellation

State stores only the minimum durable identities, revisioned records,
attachments, active prompt/interaction owner, unacknowledged notifications,
recovery progress, and cleanup obligations.

Notification delivery uses one durable ordered at-least-once stream. The next
receive acknowledges a prior passive item; actionable items require explicit
resolution. Caller loss never acknowledges or silently cancels durable work.
The durable notification journal and process-local delivery runtime are
separate owners: the journal performs only StateStore transitions, while the
delivery runtime owns exclusive leases, wakes, and tracked terminal
reclamation tasks. A receiver's open, terminal-draining, receipt-reconciling,
and ended phases are exhaustive states rather than independent flags.

Accepted prompts remain runtime-owned after caller-future drop. Abort and
parent lifecycle use one generation-fenced cancellation owner. Custom publishes
durable terminalization independently of unary reply acknowledgement, so abort
can complete while the original prompt reply remains unread. Reading or dropping
that reply retains its acknowledgement or waiter-loss path; close and shutdown
still require full quiescence. The captured generation alone defines abort's
target: dispatch checks that generation under the cancellation fence, and its
prompt owner settles interactions before terminalization. An old abort observer
cannot cancel successor interactions. Acquired tool handlers are also cancelled
when interaction publication or waiting fails, retaining the primary error and
any supplementary cleanup failure. The
[Custom contract](specs/custom/CONVERSATION.md) owns these obligations; the
[production bug ledger](../docs/mock-testing-production-bugs.md) retains defect
status and regression evidence. Storage conflicts are preserved as conflicts and
reconciled only inside the owning operation's existing deadline, with captured
durable terminal proof required to reconcile a cancellation-intent conflict.

Custom terminal drain is a dedicated durable-process owner. It serially waits
for notification reclamation, settles conversation knowledge deletion, and
removes the terminal conversation record. The owner retains the spawned task,
terminal result, and join obligation through settlement or shutdown. The
receiver retains only the configured lifecycle duration, so merely holding it
cannot consume a future cleanup budget. A receiver-driven attempt samples one
absolute deadline immediately before registry admission; parent settlement
supplies its already sampled deadline. Expiry aborts and joins remaining
tracked drain and notification-reclamation tasks without minting another
window.
Custom StateStore mutation serialization and conversation provisioning are
separate process owners as well: mutation tasks have one exhaustive
completed/interrupted exit, while provisioning carries only active or
recovery-required admission state.

OpenCode Agent runtime owns one tracked event unit: its handle, monotonic
one-shot stop owner, join handle, and sole `/global/event` response. Explicit
Starting, Live, and Recovering phases distinguish startup/reconnect from
same-stream application, gap, or caller-command history. One absolute
recovery deadline includes attempts and backoff; a response is replaced only
after stream failure and explicit drop. Conversations hold only the
synchronization handle. Its bounded command sender reserves FIFO capacity
under the caller's existing absolute deadline, so saturation applies
backpressure ending in `Timeout` rather than fabricating `Busy`.
OpenCode construction likewise carries one sampled Agent lifecycle deadline
through external verification or spawned readiness and capability checks.
The runtime owner handles expiry after spawn so the child and output drains
remain tracked through termination and join. Failure cleanup samples one
lifecycle-bounded absolute deadline and every nested event, process, and drain
owner consumes that same cleanup deadline.
A synchronization command that reaches its caller deadline on a healthy
response fails locally and returns the owner to live delivery on that response.
The event owner also owns typed StateStore-conflict retries: storage performs
one event transaction attempt, while the owner retains one normalized event
and uses the live-application or recovery episode deadline without replaying
classification or postcommit effects. Every nested operation preserves the
same typed conflict. Recoverable application races use same-response history
under the existing application deadline; fatal or exhausted application work
terminates the owner, and only a failed SSE response authorizes drop and
reconnect.

OpenCode prompt runtime separates policy from persistence. Agent default
timeout and instructions flow through Workspace to Conversation runtime. A
prompt-state owner holds admission permits, reservations, signals, typed native
Active/Idle observation, and Pending/Settling/Settled abort progress; a bound
generation operation carries exact identity and only its prompt, storage, and
notification owners. The bound generation owns the full prompt transaction,
continuation settlement, exact abort publication, and release sequence.
Reservation runtime is exhaustive across live, detached, and recovery
ownership with typed cancellation substates, so requested cancellation and its
owner cannot contradict one another.
The foreground session likewise carries one Reserved/Durable/Released
registration state and one Waiting/Released waiter state instead of independent
optional registration and durability flags.
Prompt state also owns the tracked task set for caller-detached prompt work and
accepted abort persistence. Agent shutdown seals that owner after hierarchy
quiescence, joins it under the existing lifecycle deadline, and aborts and
joins any remainder before notification and event teardown.

## Backend Boundaries

Codex and OpenCode own their selected private protocols, transport clients,
processes, native attachment, event classification, tool installation, and
cleanup. Custom owns provider/model/tool adaptation but no public protocol.

Shared core may own hierarchy admission, durable records, notifications,
managed knowledge, and portable validation. It does not own backend identity,
provider envelopes, native tool catalogs, protocol routes, or a generic
backend framework.

## Validation

Compile tests prove associated child types, trait implementations, privacy,
and absence of child ingress. Deterministic tests prove agent-owned validation,
admission, notification ownership, cancellation, conflict handling, redaction,
and cleanup. Strict-live alone proves selected Codex/OpenCode/provider/tool/
storage/process behavior.

Source contracts enforce stable public/private boundaries and deleted symbols,
not private filenames or helper choreography. Exact public shape lives in
[`specs/API.md`](specs/API.md); exact commands live in
[`AGENTS.md`](AGENTS.md).

## Error ownership

The public `error` module owns opaque `KitError` and its closed classification
and context enums. Detection sites attach safe typed evidence; public operation
boundaries enrich context without reconstructing message-only errors. Lower
Provider, Tool, and Storage failures are normalized into Agent-owned causes.
Only normalized errors appear in the standard source chain. Publication clones
the complete bounded error, and cleanup failures remain supplementary.

Codex alone persists a private version-4 structured failure snapshot because
its foreground outcomes are reconstructed from durable prompt records. It
contains no free-form messages or native diagnostics. Notification record
version 2 and Custom/OpenCode prompt records retain their existing formats.
