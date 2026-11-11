# Custom Notifications

Custom uses the single portable Conversation notification stream. One
exclusive durable receiver observes ordered message, warning, progress,
process, and interaction values. There is no provider-, tool-, shell-, MCP-,
or backend-specific public notification family.

Tool approval and source-neutral ToolHandler interaction requests are the only
Custom actionable notifications. Their private continuation identities remain
inside the request owner. Resolution validates conversation ownership,
notification state, decision kind, and submitted content before changing
durable state.

Prompt final output and prompt-returned errors remain on the prompt return
path. Custom does not fabricate process events merely because a local shell
tool runs; the tool returns bounded structured exit/output data to the model.

Each original live prompt publishes the portable `Preparing response` milestone
after complete local validation and durable Kit execution admission, before
its first Model generation. Its first nonempty function-call batch may publish
`Tool calls requested` only after finish-reason, shape, selection, and total
action-budget validation, before any approval or handler from that batch.
Both use Progress with no detail. Later turns and calls never repeat either
milestone. These historical facts establish neither remote acceptance nor
tool completion, and produce no process or file evidence.

The existing prompt execution directly awaits publication through the same
notification journal as interactions. Each publication retains its identity,
timestamp, payload, and one sampled lifecycle deadline, capped by the original
prompt deadline when present. An admitted transaction remains owned through
observer loss, cancellation, and deadline expiry until its original receipt
settles. Definite conflicts and exact ambiguous-receipt observation use the
journal's existing bounded recovery; neither permits replaying a Model or Tool
effect. Publication failure stops the next effect dispatched by Kit.

Published Progress is immutable and follows ordinary replay, acknowledgment,
interaction-barrier, and terminal-drain rules. There is no post-publication
coalescing or eviction. The two-fact prompt bound does not remove the journal's
lifetime capacity limit: callers must consume retained facts, and a full journal
fails publication before the following generation or handler can run. Reopen
replays retained facts without recreating the original prompt or milestones.

Conversation close, parent cascade, timeout, abort, and runtime loss settle or
cancel pending interactions. Receiver drop releases only receiver ownership;
it does not detach accepted cleanup or erase durable notifications.

When terminal close retains a receiver, that receiver owns drain delivery
until it reaches `None` or is dropped. Either path hands durable reclamation to
the dedicated Custom terminal-drain process, which preserves the order
notification reclamation, knowledge forget, then closed-record deletion. A
lost process-local terminal owner is recovered from the durable closing state
on Agent reopen.

Acquiring or retaining the receiver does not start or consume terminal-cleanup
time. Reaching `None` or dropping the receiver samples one absolute lifecycle
deadline immediately before admitting its cleanup attempt. Every step in that
attempt reuses the sampled deadline; a later explicit retry is a new attempt.
Parent close or shutdown settlement instead supplies its already sampled
lifecycle deadline and never grants the drain a fresh window.
