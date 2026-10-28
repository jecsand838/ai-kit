# agents-kit Development Process

This file is the durable contract for the five-stage `agents-kit` development
workflow. It has an agents-kit-only validation boundary unless an explicitly
approved workspace-wide plan owns coordinated cross-crate work. Generated run
state lives only under
`agents-kit/.codex/development/execution/`, is ignored, and is never committed.
A stage may span multiple turns, but stages run once and in the order below.

## 1. Authority And Guardrails

This workflow selects `agents-kit/specs/**` as target authority. Explicit user
instructions, the nearest `AGENTS.md`, and workspace/crate architectural
guardrails remain higher authority. For validation only, this process and
`agents-kit/AGENTS.md` intentionally narrow the root `AGENTS.md` Shared
Validation Baseline for an ordinary agents-kit-only run. An explicitly approved
workspace-wide task may own coordinated cross-crate phases and its named
gates. Scope is never broadened merely because this crate has local
dependencies.

Record the selected validation scope in the audit, plan and worker handoffs:
`agents-kit-only` by default, or `approved-workspace-coordination` with the
explicit user approval, approved plan, and exact additional paths, packages and
gates. Coordinated work follows each owning crate's contracts and only that
approved scope. Scope selection does not waive stage order, immutable inputs,
worker mutation limits or proof requirements. A document or template's presence
does not select a workflow or authorize a broader run.

Specifications and plans describe behavior and ownership, not exact sketches,
tokens, helper order, or private topology. Implement the smallest sound design;
do not add compatibility paths, migration work, speculative abstractions,
dead code, test tricks, or bloat. Planned public changes are authorized;
unexpected public expansion requires user approval.

The validation lanes are `source-contract`, downstream `compile-contract`,
deterministic `public-contract`, and `strict-live`. Source contracts are
static. Compile contracts use external-crate fixtures to prove exact
signatures, ownership bounds, documented public-module imports, and privacy.
Deterministic public-contract fixtures may implement the retained root traits
to exercise agents-kit-owned validation, direct associated-child hierarchies,
admission, cancellation, lifecycle, and redaction. They do not prove built-in
or external-service behavior. Claims about real Codex/OpenCode and provider,
tool, storage, protocol, transport, recovery, notification, prompt, model,
approval or interaction dependencies require strict-live against those real
configured dependencies. Fixtures, mocks, doubles, fakes, stubs, recordings,
snapshots, scripted services, synthetic outputs and stand-ins are forbidden as
strict-live substitutes.

### agents-kit-Only Validation Boundary

For an ordinary agents-kit-only run, this boundary applies to setup, audit,
planning, prepass, phase work, review corrections, resume, the final real
phase, the zero-phase path, and final acceptance. Its full runtime completion
gate is:

~~~text
bash agents-kit/scripts/verify_strict_live.sh
~~~

Phase checks and focused diagnostics in that ordinary run may run only agents-kit-owned scripts and
Cargo commands that select exactly the `agents-kit` package. Formatting must
use `cargo fmt --package agents-kit --check`. The workflow must never invoke
workspace-wide validation, another package's tests or checks, another
crate's verification script, `scripts/verify_workspace_strict.sh`,
`scripts/check_docs_site.sh`, a Cargo command with `--workspace`, or
`cargo fmt --all`. These commands are forbidden as phase gates, review
follow-ups, failure fallbacks, completion evidence, and optional confidence
checks.

The root-located
`scripts.tests.test_vendor_update.AgentKitProtocolAuthorityTests` class is an
agents-kit-specific archive-authority check, not a workspace or other-crate
gate. No broader Python test module or class is authorized by this workflow.

Running commands from the workspace root provides path and Cargo resolution
context only; it grants no workspace validation scope. Cargo may compile
transitive local dependencies required to build `agents-kit`, and strict-live
may exercise real provider, tool, and storage dependencies through the
agents-kit public surface. Neither permits selecting, testing, checking,
linting, formatting, documenting, or independently gating those packages.
agents-kit-owned source-contract and final-audit targets may read shared or
dependent documentation solely to enforce the agents-kit boundary; they remain
agents-kit checks and must not execute another package's validation.

If an audit, plan, reviewer, command failure, or implementation discovery
requires a change or proof owned by another crate and no approved
workspace-wide plan is active, record the blocker and stop for a separately
authorized workflow. Never infer broader scope. A documentation-only pass runs
documentation/source-contract, formatting, lint/check, and diff-consistency
gates and deliberately skips strict-live because it changes no runtime
behavior. Git snapshot, overlap, staging, and commit inspection are workflow
safety operations rather than validation gates; keep them limited to the
explicit run and phase path sets.

## 2. Immutable Inputs

Only Stage 2 may edit tracked specification files. Clarifications may touch
existing paths only and total at most 199 added-plus-deleted lines from the
recorded baseline revision. Any architectural or behavioral ambiguity requires
user resolution. Before a spec edit, the spec tree and index must contain no
unowned work.

The audit records the baseline revision, approved paths, line accounting, user
decisions, and this canonical finalized-spec fingerprint:

~~~text
git diff --no-ext-diff --binary -- agents-kit/specs | git hash-object --stdin
~~~

Specs freeze when Stage 2 closes. The plan freezes when Stage 3 closes. Stage
4 marker text freezes after review; only whole-phase marker deletion after
phase acceptance is allowed.

## 3. Paths, Names, And Artifacts

All `execution/`, `dispatch/`, and `templates/` shorthand is relative to
`agents-kit/.codex/development/`. Run commands from the workspace root with that
prefix. The workspace-root working directory is path context only, never
workspace test or check authority. Never create those workflow directories at
the workspace root.

One run uses one UTC `YYYYMMDDTHHMMSSZ` timestamp. Verticals use
`vNN-kebab-case`; tasks append `.tNNN`; phase and review counts use two digits.
Only these generated artifact patterns are valid:

~~~text
execution/{timestamp}-audit-findings.md
execution/{timestamp}-plan.md
execution/tmp/{section}-audit/audit-review.md
execution/tmp/planning/verticals.md
execution/tmp/planning/vertical-research-{vertical-id}.md
execution/tmp/prepass/review.md
execution/tmp/phase-reviews/phase-{phase-count}/review-{review-count}.md
~~~

`{section}` is root, core, codex, opencode, or custom. Stage 1 creates these
fixed parents after removing copied examples:

~~~text
execution/tmp/root-audit/
execution/tmp/core-audit/
execution/tmp/codex-audit/
execution/tmp/opencode-audit/
execution/tmp/custom-audit/
execution/tmp/planning/
execution/tmp/prepass/
execution/tmp/phase-reviews/
~~~

## 4. Common Main-Agent And Worker Rules

At every stage start, the main agent reads completely:

- root and crate `AGENTS.md`;
- this `PROCESS.md` and the active human prompt;
- every existing run artifact consumed by the stage;
- each schema for an artifact the stage creates;
- `SPECIFICATIONS.md` and all owning specs for Stages 2 through 5;
- `agents-kit-change-contract`;
- `use-rust-best-practices-core`;
- `use-rust-best-practices-async`;
- `use-rust-best-practices-testing`;
- `rust-code-structuring-best-practices`; and
- `use-rust-best-practices-abstractions`.

The only worker roles are audit-reviewer, vertical-analysis,
vertical-researcher, prepass-reviewer, and phase-reviewer. Each is a bounded
one-shot worker. It reads the common inputs above plus its dispatch's
role-specific inputs, writes only its authorized Markdown artifact, launches
no nested worker, and changes no source, spec, plan, marker, staging, commit, or
remote state.

Every worker inherits the recorded validation scope and its assigned slice.
For an ordinary run, it may record or accept only agents-kit-scoped commands and
evidence. Only an explicitly approved coordinated plan permits its named
cross-crate work or gates. Any need outside that scope is a blocker for a
separately authorized workflow, not permission to recommend or run it here.

After every handoff the main agent requires completed status, verifies the
worker is no longer running, reads and validates the artifact completely, and
records that the worker must not be reused or receive follow-up work. For a
batch, perform this independently as workers finish and verify none from that
batch remains running before reconciliation. If a required role is
unavailable, stop the stage.

The main agent alone owns user questions, reconciliation, edits, validation,
staging, commits, cleanup, and final acceptance.

## 5. Stage State Machine

~~~text
Stage 1: Setup
Stage 2: Audit
Stage 3: Planning
Stage 4: Pre-execution
Stage 5: Goal execution
~~~

Do not combine, skip, or overlap stages.

### Stage 1: Setup

1. Inspect `execution/`. If it contains any prior state, stop and obtain
   explicit discard authorization; an instruction to start setup is not that
   authorization.
2. Recreate it from `templates/execution/`, remove copied `*.example.md`, and
   create every fixed parent in Section 3.
3. Verify only the empty directory skeleton remains, templates are unchanged,
   and no generated path is tracked.

### Stage 2: Audit

Before audit work, record the initial Git state: baseline HEAD plus the exact
initial staged, unstaged, and untracked path sets. Preserve that snapshot for
overlap checks in later stages.

Create `execution/{timestamp}-audit-findings.md`. Compare all non-vendored
`agents-kit` source, tests, config, and manifests to selected specs under the
agents-kit root, core, codex, opencode, and custom sections. Record citations,
observed drift, target behavior, dependencies, uncertainty, and proof
implications in the four declared lanes. Treat a built-in/service claim proved
only by deterministic fixtures or other substitutes as drift; direct foreign
implementations may prove only their Agent-owned public contract. Cross-crate
work is limited to the explicitly approved coordinated plan; otherwise record
the need as a blocker and do not audit or validate that crate. Ask every
architecture/behavior question and apply only approved clarification under
Section 2.

After the full draft exists, dispatch one audit-reviewer per section. Validate
all five artifacts and reconcile confirmed findings. Finish only when user
questions are resolved, workers are no longer running, the audit is
consistent, and the spec fingerprint/path/accounting record is final. A clean
section records zero findings without placeholders.

### Stage 3: Planning

Dispatch one vertical-analysis worker and validate `verticals.md`. Then
dispatch one vertical-researcher per identified vertical, validate every
artifact, and verify the batch is no longer running.

Create the immutable plan with:

- the complete post-update tracked non-vendored `agents-kit` tree and explicit
  removals, plus only additional paths named by an approved coordinated plan;
- unique file-specific test, deletion, extension, or addition tasks;
- exact source/spec citations, one target, one marker host, and host rationale;
- exactly one validation lane for each test;
- a linked test for every addition, extension, and behavior-changing deletion;
- sequential non-overlapping vertical phases ordered
  `test -> deletion -> extension -> addition`; and
- the sole agents-kit runtime completion gate for an ordinary run, plus only
  additional gates named by an approved coordinated plan; the last real phase
  owns completion, never a validation-only phase.

Every tracked file removal gets one task. A whole-file deletion marker lives
in the nearest honest surviving owner within that vertical; never create a
tombstone or marker-only file. Prove audit-to-task, task-to-phase, and
change-to-test coverage. No-drift produces zero tasks/phases and no invented
validation work. A required change or command outside the recorded scope
blocks this plan for a separately authorized workflow; it never becomes an
implicit task, vertical or completion gate. Additional crate work belongs here
only when the approved coordinated plan explicitly assigns it.

### Stage 4: Pre-execution

Place exactly one marker block per plan task in its planned host. New planned
code/config/test files are comment-only skeletons. Marker prefix:

~~~text
AGENTS_KIT_DEV[P01][test][v01-example.t001]
~~~

Every marker has:

~~~text
Target path:
Marker host:
Marker host rationale:
Plan citation:
Spec citation:
Intent:
Acceptance condition:
Non-exact code sketch:
~~~

Verify parity, ids, tags, placement, host survival, skeleton shape, immutable
inputs, and zero implementation. Dispatch one prepass-reviewer. After its
completed handoff, correct marker/scaffolding findings and rerun every check;
the artifact is not rewritten and that reviewer is not redispatched. An
immutable-input defect requires a new owning audit/planning run or user
direction.

The baseline path set is the union of approved spec paths and
marker/scaffolding paths. Reject any initial-diff overlap and any unrelated
staged path. Stage only that explicit set, verify exact staged equality, and
make one reviewed baseline commit when nonempty; never make an empty commit or
push.

### Stage 5: Goal Execution

The goal template's first line is a nonempty, unindented ASCII objective; the
complete template is ASCII and at most 3,900 bytes. For a real plan, implement
the earliest remaining phase, one complete vertical at a time, in exact order:

~~~text
test -> deletion -> extension -> addition
~~~

Write all phase tests first. Before production changes, run the smallest
relevant target and record meaningful red evidence. A missing-API compile
failure proves only the compile contract; once callable, observe the behavioral
red before implementing that behavior. If added missing-proof coverage already
passes, record that fact rather than inventing a failure or production fix.
Assemble the complete vertical before broad validation and independent review.
Run one Cargo command at a time. Do not edit immutable inputs or markers.
Whole-file deletion markers remain in surviving hosts through review.

Run phase validation within the recorded scope, create the numbered phase-review
parent, and dispatch a fresh phase-reviewer for each review cycle. Validate the
handoff, fix findings, revalidate within that same approved scope, and repeat
with a new reviewer until clean. For ordinary runtime work, the last real phase
runs `bash agents-kit/scripts/verify_strict_live.sh` as its only full
marker-compatible completion gate before review. An approved coordinated plan
adds only its named gates. Documentation-only work retains Section 1's non-live
gates and makes no runtime completion claim.

After clean acceptance, delete all phase markers as one operation, rerun phase
validation, stage only explicit phase paths after rejecting initial-diff
overlap, verify the staged list, and create exactly one reviewed commit per
real phase. Never push. The final phase reruns the completion gates selected
above plus marker absence after cleanup and before its commit.

A zero-phase immutable plan dispatches no phase reviewer and creates no phase
artifact or commit; it runs the selected completion gates directly. For an
ordinary runtime run this is only `bash agents-kit/scripts/verify_strict_live.sh`;
documentation-only work uses Section 1's non-live gates. If that direct path
fails, start a new owning audit/plan or obtain user direction rather than
inventing work or invoking workspace/other-crate validation outside the
approved immutable plan.

## 6. Resume And Final Acceptance

After compaction, interruption, or resume, reload every common input, the full
plan, current owning specs, and all earliest-phase markers. Reconcile plan
disposition, worker artifacts, reviewed commits, markers, index, and worktree.
An accepted marker-free phase without its commit remains active: rerun its
post-cleanup gates and finish that commit before advancing.

Completion requires every task implemented, all required tests and selected
completion gates passing, no warning/error, marker, stale skeleton, compatibility
path, test hack, dead code, tracked execution state, or unowned Git overlap, plus
exactly one reviewed baseline commit when required and one reviewed commit per
real phase. Ordinary runtime completion requires the sole agents-kit verifier.
Documentation-only work claims only its non-live proof. Workspace composition
and other-crate gates are required only when explicitly named by the approved
coordinated plan; never infer them from dependencies.
