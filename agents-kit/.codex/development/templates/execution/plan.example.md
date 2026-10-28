# agents-kit Development Plan

## Run

- Timestamp:
- Audit:
- Vertical analysis:
- Vertical research artifacts:
- Approved specification paths:
- Specification diff fingerprint:
- Workflow authority: `agents-kit/specs/**`
- Validation scope: agents-kit-only (default) / approved-workspace-coordination
- Explicit approval and coordinated plan: none / citation
- Authorized additional paths, packages and gates: none / exact list
- Disposition: implementation/no-work
- Status: final immutable

## Decisions And Boundaries

Record only settled architecture, public effect, cross-vertical dependencies,
and explicit exclusions. Additional crate work requires the cited approved
coordinated plan. A change or proof need outside that recorded scope is a
blocker for a separately authorized workflow, never implicitly planned work.

## Post-update Tree

List every non-vendored tracked `agents-kit` path as KEEP, MODIFY, ADD, or
DELETE. Put all deletions in an explicit removal subsection. Add other crate
paths only when named by the approved coordinated plan; otherwise record the
need as a blocker and stop planning.

## Verticals

| Order | Vertical id | Objective | Authority | Findings | Dependencies | Exclusions |
|---|---|---|---|---|---|---|

## Tasks

Each task has exactly one file within recorded scope and one tag: test,
deletion, extension, or addition. Files are agents-kit-only by default; other
crate files require the approved coordinated plan.

| Task | Tag | Target path | Marker host | Host rationale | Source citation | Spec citation | Intent | Validation lane | Linked test | Public effect |
|---|---|---|---|---|---|---|---|---|---|---|

`Validation lane` is exactly `source-contract`, `compile-contract`,
`public-contract`, or `strict-live` for a test task. Foreign implementations
prove only Agent-owned contracts; real built-in/service claims require strict-live.
Every addition, extension and behavior-changing deletion has a `Linked test`.
A whole-file deletion uses a surviving host outside its target.

## Implementation Phases

For each phase record objective, complete task ranges, exclusions, validation,
review path, pre-production red evidence and one-commit boundary. Distinguish
compile-red, reached behavioral red and already-passing missing-proof coverage.
Order tasks exactly:

~~~text
test -> deletion -> extension -> addition
~~~

## Coverage

- Audit finding -> resolving task table
- Change task -> linked test table
- Task -> exactly one phase proof
- Tree path -> task parity proof

## Completion Gates

Ordinary runtime completion uses only
`bash agents-kit/scripts/verify_strict_live.sh`; supporting commands select only
agents-kit. Additional gates require the explicitly approved coordinated plan.
Documentation-only work uses PROCESS non-live gates without runtime claims.
Record marker absence, path-scoped Git safety and no-work behavior. The last
real phase owns completion; never add a validation-only phase or an unapproved
workspace/other-crate gate.
