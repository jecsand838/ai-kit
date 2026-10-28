---
name: agents-kit-research
description: "Research agents-kit changes against Codex App Server, OpenCode docs, vendored sources, and the authority selected by the active workflow; feed protocol facts into audit and planning artifacts before implementation."
---

# agents-kit Research

## Purpose

Ground `agents-kit` work in authoritative Codex App Server and OpenCode
behavior while preserving workflow-relative authority and crate architecture.

## Trigger Boundary

Use this skill when `agents-kit` work depends on Codex App Server or OpenCode
protocol behavior, transport semantics, capability limits, recovery behavior,
or strict-live assumptions.

## Prerequisites

- Read [`agents-kit/specs/SPECIFICATIONS.md`](../../../agents-kit/specs/SPECIFICATIONS.md).
- Read [`agents-kit/specs/API.md`](../../../agents-kit/specs/API.md) whenever
  research could affect a public operation or root type.
- Read the owning backend/root specs for the researched behavior.
- Read [`agents-kit/AGENTS.md`](../../../agents-kit/AGENTS.md).
- When using the development workflow, read
  [`agents-kit/.codex/development/PROCESS.md`](../../../agents-kit/.codex/development/PROCESS.md)
  and the current audit or plan artifact in full.
- Pair with `agents-kit-change-contract` when the task changes specs, code,
  tests, docs, or validation.

## Workflow

1. Confirm requested behavior against official docs, pinned vendored sources,
   or strict-live proof.
2. Decide whether the behavior belongs on the portable root surface,
   backend-private specs, or providers-kit/tools-kit/storage-kit instead.
3. Follow explicit user direction and the selected workflow's authority.
   Root contracts, `specs/API.md`, and owning behavior specs define the
   required surface; source and tests establish implementation status.
   Record actual conflicts as drift without presuming unfinished reduction
   work or silently selecting a historical workflow.
4. Reject silent degradation when a backend cannot honor a requested portable
   field or lifecycle.
5. Classify honest unmappable behavior as `Unsupported` when the backend
   cannot provide it, or `NotYetImplemented` for a finite adapter gap. Preserve
   opaque `KitError` and its exact closed portable inspection vocabulary from
   `specs/API.md` and `specs/ERRORS.md`, including operation, phase, normalized
   causes, and bounded supplementary cleanup failures. Do not invent public
   backend labels, native prose, reason-code families, unsupported wrappers,
   or any Kit capability object equivalent.
6. For Codex and OpenCode event work, require typed backend event decoding and
   exhaustive classification before portable mapping. Generic known-event
   catch-alls, raw fallback for known events, and silent no-event buckets are
   invalid designs.
7. For OpenCode recovery work, keep the sourced `POST /sync/history` route
   with a JSON body of per-aggregate sequence state. It is recovery support,
   not notification delivery or a conversation cursor.
8. For OpenCode route behavior, distinguish sourced facts from architectural
   inference. Pending permission/question routes feed backend-private request
   reconciliation. Sourced session deletion plus global session/event-history
   verification provide backend-private cleanup; an experimental workspace
   record is not an agent workspace cleanup authority. These routes do not
   define portable delete success or require a particular helper class. Codex
   `thread/delete` and broadcast `thread/deleted` remain sourced protocol
   facts.
9. Treat the selected archive and its lock metadata as protocol evidence only;
   neither establishes target API, private module layout, tests, or fixture
   shapes.
10. Preserve Custom across the complete `Agent -> Workspace -> Conversation`
    hierarchy, distinguishing its contract from observed implementation proof.
    agents-kit owns its multi-turn prompt/tool loop. providers-kit owns
    `Provider -> Model -> Session`, selected-model generation, normalized
    function calls/results, and the Model-associated continuation contract.
    tools-kit owns Toolset/ToolHandler execution. Reopen recreates private
    runtime from bounded provider-neutral committed turns and
    caller-resupplied runtime values;
    `P: ProviderBackend`, `T: ToolsetBackend`, and storage implementations
    remain statically typed; Tool handlers remain `T::Handler` without Clone
    requirements on either `T` or its Handler. Unary and streaming execution
    share one accepted prompt owner. Provider children are `P::Model`, its
    associated `Continuation`, and the Model-associated Session, not
    independent Agent parameters. No Provider,
    Model, Session, Toolset, Database,
    credential, callback, or live continuation is durable.
11. Feed confirmed behavior into explicit structs, private exhaustive enums,
    typed spec data, receiver methods, and the owning public-or-private trait
    contracts. Agent, Workspace, and Conversation traits use associated child
    types. Caller hierarchies remain direct or caller-owned exhaustive enums;
    supplied dispatchers stay closed over Codex, OpenCode, and Custom. Reject
    erasure, generated dispatch/routes, macro protocol abstraction, and shims.
12. After context compaction, resume, or long implementation phases, reload the
    active workflow's complete owning process and artifacts before making
    protocol claims. In the development workflow, reread
    `agents-kit/.codex/development/PROCESS.md`, the complete generated plan,
    `agents-kit/specs/SPECIFICATIONS.md`, the owning backend/root specs, and the
    active reduction authority.
    Uncertain Codex App Server or OpenCode behavior must go back to official
    docs, pinned vendored sources, or strict-live proof; never guess from
    current implementation files or production-parity code.

## Output Rules

- Distinguish sourced facts from implementation inference.
- Name the specs that should receive or already contain the researched fact.
- Call out a providers-kit, tools-kit, or storage-kit prerequisite only when the
  selected authority genuinely requires behavior outside its accepted public
  boundary. Custom owns prompt-loop retention of `P::Model::Continuation`, but
  durable Agent history stays provider-neutral.
- Prefer crate-root portable behavior over backend-private leakage.

## References

- [`agents-kit/specs/SPECIFICATIONS.md`](../../../agents-kit/specs/SPECIFICATIONS.md)
- [`agents-kit/AGENTS.md`](../../../agents-kit/AGENTS.md)
- [references/live-validation.md](references/live-validation.md)
