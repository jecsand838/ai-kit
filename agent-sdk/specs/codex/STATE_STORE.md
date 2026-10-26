# Codex StateStore Domain

This document owns the Codex `state_store/` domain for backend-private
control-plane persistence. [`../STATE.md`](../STATE.md) owns the shared durable
floor and recovery semantics; [`../RUNTIME.md`](../RUNTIME.md) owns public
lifecycle success boundaries.

StateStore is not managed knowledge. Knowledge content and retrieval use the
Agent's private managed-knowledge owner described in
[`KNOWLEDGE_BASE.md`](KNOWLEDGE_BASE.md), backed by the caller-supplied
`D: storage_sdk::DatabaseBackend`.

## Functional Boundary

The Codex StateStore domain owns:

- private key construction and serialized envelopes;
- agent attachment compatibility and cleanup facts;
- full workspace and conversation records with their Codex attachment;
- one bounded native thread-creation correlation and reconciliation
  obligation until exact binding or cleanup is proven;
- prompt generation, effective structured-schema/tool-selection contract,
  native turn acceptance, cancellation intent, terminal evidence, waiter
  disposition, and compaction facts;
- interaction continuation, acceptance, resolution, and cancellation facts;
- ordered notification backlog, acknowledgment, and terminal drain facts;
- native cleanup and reconciliation obligations; and
- transaction-backed mutations and validated record reconstruction.

Shared core remains backend-blind. It may own genuinely portable record data
and StateStore transaction access, but never Codex thread/turn/request ids,
method names, route keys, JSON-RPC payloads, or concrete key shapes. Public
callers see none of those private records or transactions.

The durable state is a bounded current-state recovery model, not a transcript,
execution history, event log, raw protocol archive, or generic repository.
Runtime tracing does not expand it.

## Agent Attachment

One Codex-private attachment is associated with the durable SDK Agent. It
records only facts needed to verify selected server/config compatibility,
exclusive connection ownership, managed registration compatibility, and
pending cleanup. Root agent identity remains backend-neutral.

New build and reopen mutate attachment state through the live storage-sdk
transaction API. If a build creates attachment data but cannot publish the
root Agent, it removes that data or leaves an explicit discoverable recovery
obligation while preserving the primary build error. Orphan attachment state
is never independent inventory authority.

## Workspace And Conversation Records

Workspace state combines the complete portable workspace snapshot and
revision with only the Codex attachment and cleanup facts required for reopen.
Conversation state combines portable ownership/title/defaults/lifecycle and
managed-source references with one native root-thread attachment and current
recovery state.

Portable defaults have one durable owner. Backend attachment data must not
duplicate model, personality, reasoning, knowledge policy, title, workspace context, or
other portable fields in a second authority. It may retain the last applied
native sticky selection and a sourced exact reset value solely as private
compatibility/recovery evidence; those facts never become alternate portable
defaults or caller input. Native descendants remain a private closure of the
one root-thread attachment and never become public conversation records.

Record decoding validates schema/version, typed identity, ownership,
nonzero revision, lifecycle consistency, native id branding, source integrity
references, and mutually exclusive state before constructing a domain value.
Malformed serialized data is not repaired by defaults or lossy conversion.

Storage failures retain the detecting phase before reaching public operation
boundaries: transaction admission, puts, and deletes use `StateWrite`; loads
use `StateRead`; commits use `StateCommit`. Prompt admission, cancellation,
conversation close, and workspace update/delete preserve normalized dependency
details and timeout evidence. Their existing exact-conflict handling and
commit success boundaries remain authoritative; phase enrichment neither
authorizes retries nor replaces a confirmed mutation outcome.

## Prompt And Terminal State

One current prompt owner records the data-carrying lifecycle needed to
distinguish:

- admitted but not natively accepted work;
- accepted work with its private generation and native turn identity;
- cancellation intent and dispatch certainty;
- terminal native observation and portable result classification;
- live waiter delivery versus durable output discard; and
- cleanup or reconciliation still required before later admission.

These are states of one logical prompt record, not independent
`Started`/`Completed`/`Cancelled` carrier families. Native acceptance and
terminal transitions validate generation, thread, and turn ownership.
Exact matching generation, turn, and stored outcome reconcile idempotently
without a second portable result. Contradictory terminal facts remain integrity
failures. After prompt compaction, the runtime may consume an exact historical
terminal only through the bound thread's complete, uniquely client-correlated
native history. This read-only reconciliation neither reconstructs a portable
result nor mutates a newer generation; it does not claim detection of repeated
identical native events after their SDK record is compacted.

Prompt start prepares durable ownership before the non-idempotent native call.
Definite pre-acceptance failure can return to idle. Ambiguous acceptance stays
recovery-owned. Once accepted, normal completion, failure, interruption,
caller-future loss, timeout, close, and abort are terminalized only by the
conversation runtime after authoritative evidence.

Terminal output remains until the live waiter receives it or waiter absence is
durably recorded and output is discarded. Notification and cleanup facts
survive that compaction as required by root state.

Prompt-record encoding version 4 retains the portable failure classification,
operation, phase, typed detail, supplied HTTP status, bounded safe cause and
supplementary failures, and diagnostic-truncation evidence. Its private codec
never serializes the public error owner, free-form messages, native identities,
wire payloads, or lower-crate errors. The record reconstructs the same portable
diagnostic fields for live delivery and authoritative history recovery, including
`Cancelled` with a normalized cause for an error-bearing interruption. This
uses the existing version-4 representation without migration or version bump.
Only the existing terminal categories are admitted; observer timeouts and
unresolved acceptance cannot become durable cancelled or failed terminals.

A terminal observation that cannot match the current durable generation first
settles its read transaction. Native history is read without a StateStore
transaction or context lock held. Identity, full-history completeness, unique
turn/client correlation, terminal status and normalized error are validated
before the current owner is re-read. Only a changed owner or an exact in-flight
clearing owner permits another bounded read-only observation. Storage rollback
failure vetoes reconciliation. Clearing retains its exact client/turn identity
until the existing delete owner settles; the marker alone never proves cleanup
success. A missing record with a non-clearing live owner remains corruption.

One canonical failure snapshot is retained across terminal commit retries.
Exact reconciliation compares the complete snapshot, and duplicate terminal
facts retain their existing generation, turn, and outcome checks. Older,
missing, future, or malformed versions are rejected before decoding the
changed body, without migration, inferred defaults, repair, deletion, or writes.
The private namespace and ownership keys remain unchanged. Other record
versions are unchanged.

## Interactions And Notifications

One interaction owner per actionable notification retains typed kind,
private continuation, pending/resolving state with one private per-attempt
correlation, acceptance certainty, and one terminal outcome. Intent-owned
settlement requires that exact attempt. Secret submitted values are not
retained in cleartext. Known or ambiguous native acceptance never returns to
pending and is never resent blindly.

For selected normal App Server requests, successful local response enqueue is
acceptance-ambiguous. The identity-only `serverRequest/resolved` fact records
that the native callback disappeared but cannot distinguish response
consumption from lifecycle cleanup, so it cannot create an accepted terminal
state. The recovery-owned interaction remains `Conflict` and blocks later
delivery until separate authoritative cancellation/cleanup evidence settles
it.

Notification insertion order is durable before delivery. Passive
acknowledgment, actionable resolution, receiver replay, terminal drain, and
owner deletion follow [`../NOTIFICATIONS.md`](../NOTIFICATIONS.md). The
notification classifier and protocol reader do not write these records
directly; the owning conversation sequences persistence and publication.

## Mutations And Transactions

Every mutation uses storage-sdk's live transaction surface directly:
begin, load, decode, validate the owning transition, put/delete, then commit or
rollback. The Codex domain does not add a batch, CAS, whole-state replacement,
retry wrapper, or generic repository abstraction.

Transactions contain only their own StateStore operations. Hierarchy locks,
route locks, and synchronization guards are released before protocol,
process, timer, tool, Database, notification, or waiter awaits. A
non-idempotent mutation is not blindly retried after ambiguous commit.

The native creation response and matching thread-start notification may race to
record the same creation observation. A definite conflict from either the
transactional read or consuming commit permits one exact obligation readback,
not mutation replay. Success requires the unchanged immutable creation
obligation and the same native thread identity already durably observed. A
missing obligation retains the existing no-match result; an obligation that
remains unobserved, differs, or cannot be read never creates success. Failed readback or
rollback preserves the original typed error and adds the secondary read or
cleanup failure. Non-conflict errors retain their normalized details and phase.
Neither reconciliation path repeats native creation, knowledge preparation, or
materialization. If contradictory ownership requires hierarchy invalidation,
the public publication rejection remains primary, retaining the original
observation error and its subordinate cleanup failure under it.

Workspace update atomically checks ownership/revision/activity, replaces only
portable workspace fields, preserves attachment, increments revision, and
records postcommit invalidation obligations. Safe delete atomically removes
portable inventory and quiescent descendants while recording native cleanup.
Their durable commit is success; later native work cannot reverse it.

## Reopen, Reconciliation, And Cleanup

Reopen loads the complete owning record and performs one bounded reconciliation
attempt. Codex thread reads, exact private thread-creation correlation, terminal
turn facts, request acceptance facts, background-terminal inspection, and
native deletion facts may prove a state transition. Thread existence alone
never proves prompt success, creation ownership, or portable closure.

Active or ambiguous native work remains `Busy`; contradictory well-formed
facts are `InvalidState`; malformed wire input is `Protocol`. A lost
server-request continuation is terminally cancelled rather than reconstructed.
A non-idempotent prompt, interrupt, interaction response, dynamic-tool
response, or deletion is not replayed after ambiguity.

Cleanup obligations remain idempotent durable facts until native thread
deletion, background-terminal cleanup, route detachment, or other owned effect
is proven complete. Conversation close and workspace delete may remove their
terminal portable owners only at the success boundaries specified by root
state and notification documents.

## Required Boundaries

- No public keys, records, envelopes, transactions, native ids, recovery DTOs,
  or backend identity.
- No Codex key construction or native method knowledge in shared core.
- No managed knowledge, chunks, embeddings, or Database configuration in
  control-plane records.
- No item aliases, macro-generated records, generic command bus, actor,
  callback registry, or parallel old/new lifecycle representation.
- No public or reusable backend label, capability model, native error code, or
  persistence diagnostic API. Stored failures use the existing portable error
  vocabulary privately.

## Validation Obligations

Static contracts verify StateStore/Database separation, backend-blind core,
passive record boundaries, and absence of public keyspace data without fixing
private key strings or type names. Strict-live tests with real StateStore
implementations prove durable build/reopen, revision mutation, create/close,
prompt acceptance and terminalization, abort and timeout ambiguity,
caller-future loss, interaction acceptance, ordered notification replay,
process-loss recovery, transaction rollback, cleanup reconciliation, and safe
delete success boundaries.
