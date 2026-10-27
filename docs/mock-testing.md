# Direct public-trait tests

The primary deterministic lane is `bash scripts/check_ci.sh`. It validates
caller-owned implementations of the existing public traits and exercises the
real Custom Agent and App with those implementations. It requires no provider
credentials, backend process, socket, or supplied lower-backend construction.
Dependencies still compile, including unconditional native dependencies.

```bash
bash scripts/check_mock_contracts.sh
bash scripts/check_mock_contracts.sh --crate agent-sdk
bash scripts/check_ci.sh
python3 scripts/qualify_mock_runtime.py
```

The scoped command accepts only the five workspace crate names. It is scoped
evidence, not a full-suite receipt. Arbitrary filters and unknown arguments are
errors. Both entrypoints use serial Cargo commands and `--locked`. The mock-only
entrypoint retains isolated crate builds: Storage uses `--no-default-features`
and the other four crates use their default features. Primary CI selects all
five packages together with `--no-default-features`, sharing one dependency
feature graph. All five currently have empty or absent default features;
optional backends stay disabled. CI proves that shared composition, while the
scoped mock commands retain isolated feature evidence. Each behavioral target
gets its own inventory and execution, so one target's failure does not prevent
independent targets from running.

Primary CI runs formatting, no-path/no-alias guards, and Python runner contracts
before expensive compilation; a failure in these cheap gates stops CI. It then
prepares all seven mock targets and App's existing `compile_contract` in one
Cargo build using `--profile test --keep-going`, and runs library/target Clippy
once for the same five packages. It discovers each executable from the current
build's Cargo JSON by exact crate manifest, test source, target kind/name and
test profile, with no fallback to old binaries on disk. Each of the seven mock
binaries runs directly in its owning crate directory; App's `compile_contract`
remains compile-only. Partial build failures remain failures while independently
built mock targets can still run. Missing or ambiguous artifacts cannot pass.
A complete behavioral inventory must be nonempty, execute every listed name, and contain
zero ignored, measured, or filtered tests. Compile-only stages are reported
separately and may contain no runtime tests. Compilation errors, missing
targets, missing summaries, process interruption, or failed log capture cannot
produce PASS evidence. There are no automatic retries or reused pass receipts.

## GitHub Actions

[The CI workflow](../.github/workflows/ci.yml) runs this same entrypoint in one
`ubuntu-24.04` job on pull requests targeting `main`, pushes to `main`, and manual
dispatch. It uses the repository's pinned Rust toolchain with Clippy and rustfmt,
read-only repository permissions, and no provider credentials or service
containers. Select **Deterministic CI** as the required status check in the
repository's branch protection or ruleset after its first GitHub run.

The workflow requires all seven mock targets, including on cache hits.
It does not run strict-live, native backend integration, capacity,
dependency test suites, optional-backend matrices, or the macOS-only restricted
runtime qualifier. App's existing `compile_contract` is compiled but not run.
These exclusions do not change the separate release gates.

Cargo dependencies still compile, including unconditional LanceDB/Lance/Arrow
dependencies. The workflow installs their native build prerequisites and uses
two build workers, no incremental compilation, and no development/test debug
information. Cargo downloads and `target/debug` are cached by Ubuntu version,
architecture, toolchain, explicit build-settings version, lockfile, and workspace
manifests. Timeout/trigger changes do not invalidate compiled dependencies.
Dependency changes can restore an older cache for the same toolchain/settings;
Cargo decides which outputs need rebuilding. Successful runs save missing cache
entries, including PR-local caches for dependency-changing PRs. Exact cache hits
do not upload another copy. `main` runs maintain the shared cache; GitHub isolates
PR-created caches to that PR, so the first `main` run must create its own cache.
Test receipts are never cached. Formatting is also
checked before native package installation and cache transfer to reject cheap
failures early. Local runners default to one compiler worker unless callers set
`CARGO_BUILD_JOBS`; Cargo commands themselves remain serial.

After a failed cache-miss validation, the workflow preserves partial compiled
outputs under a unique key extending the normal cache key. This includes a
controlled validation timeout, but excludes cancelled runs. A later run can
restore that prefix match, let Cargo validate and finish compilation, and execute all required tests
again. Only a successful run saves the normal complete-cache key. Partial caches
contain no reports, never satisfy a check by themselves, and never turn the
original failure into a pass. They prevent a cold build from repeatedly losing
completed work when it exceeds one run's budget.

Superseded runs for the same PR or branch are cancelled. The job has a
20-minute ceiling. GNU `timeout` sends TERM directly to the CI runner after
18 minutes, allowing its existing signal handler to terminate and join the
owned Cargo process group before cache archiving. A ten-second forced-kill
backstop and GitHub's 19-minute step limit bound failed cleanup. This avoids
archiving files while a compiler continues writing after a hard step timeout.
Timeouts fail validation; they do not waive tests. Fresh reports and logs from
`target/mock-contracts/` are uploaded after success or failure when available and
retained for seven days. GitHub step timings, report durations, and the final
disk-usage step provide cold/cached build measurements. Initial hosted runs
exhausted shorter validation budgets during native dependency compilation.
The 20-minute ceiling gives cold compilation more headroom to avoid making a
failed run and retry the normal bootstrap path; cached runs finish as soon as
checks complete.
If an unusually slow cold build still times out, rerun the failed job to reuse
its partial cache. Incomplete runs remain failed and cannot authorize merging.
The timeout is a spending ceiling, not a target duration. Routine runs still
require the complete current-source test inventory, regardless of the cache
restored. Local timings and cached recovery do not establish a single cold
hosted run's duration.

## Test ownership and boundaries

| Crate | Required integration targets | Test-local support |
| --- | --- | --- |
| Provider | `mock_contracts` | `tests/mock_support` |
| Storage | `mock_contracts` | target-local owners |
| Tool | `mock_contracts` | `tests/mock_support` |
| Agent | `mock_contracts`, `custom_mock_composition` | `tests/mock_hierarchy`, `tests/mock_memory`, other `tests/mock_*` owners |
| App | `mock_contracts`, `workflow_mock_composition` | `tests/mock_agents`, `tests/mock_state`, `tests/mock_workflow` |

Mocks are ordinary structs and exhaustive caller-owned enums implementing the
public traits directly, with exact associated child types. They are private to
each crate's tests. There is no shared mock crate, public mock API, extension
ingress, erased backend object, generated dispatcher, or production test branch.
Existing backend, private-erasure, protocol, process, persistence, and strict-live
tests retain their separate targets and registrations.

The StateStore fixtures are process-local transaction implementations. Their
committed ledgers survive reconstruction only while their owning process and
retained state survive. This is same-process reconstruction evidence, not native
durability. Database fixtures compare typed requests and return scripted typed
results; they are not database engines. Mock behavior cannot prove a supplied
backend's wire mapping, actor behavior, persistence, or service compatibility.

## Trait and method coverage

This matrix names behavior and test owners. Exact signatures remain in the
owning public API documentation.

| Trait | Methods/behavior exercised | Primary evidence |
| --- | --- | --- |
| `ProviderBackend` | model selection, discovery pages/info, file upload/list/info/download/delete, batch status/cancel/results/list, health, shutdown | Provider `struct_hierarchy_all_operations_preserve_requests_and_exact_continuations`, enum-family and error-matrix cases |
| `ModelBackend` | selected ID, generation, streaming, embedding, batch submission, session creation; exact opaque continuation association | same hierarchy tests, mismatched-continuation predelegation rejection, demand/drop/terminal tests |
| `SessionBackend` | IDs, send, receive, consuming close | Provider complete operation and failure matrices |
| `DatabaseBackend` | capabilities, catalog, data, query, index; all retained operation variants, opaque cursors, locators, scores, typed failures | Storage `database_struct_and_every_enum_arm_forward_retained_typed_operations`, script-integrity and atomic-operation distinction cases |
| `StateStoreBackend` | load/list/begin; namespace isolation, page-local views, stable observations | Storage hierarchy, listing, and snapshot cases |
| `StateTransactionBackend` | load/put/delete/commit/rollback; revisions/tombstones, atomic visibility, conflict, invalidation, ambiguous finalization | Storage transaction cases, every caller enum arm |
| `ToolsetBackend` | inventory, unique child creation, unknown names, sealed admission, shared shutdown result | Tool inventory, enum, and lifecycle cases |
| `ToolHandlerBackend` | metadata, invocation, interaction continuation, cancellation, terminal reuse | Tool correlation, repeated-interaction and failure cases |
| `AgentBackend` | identity, workspace inventory/create/open, shutdown | Agent `mock_contracts` and shared direct hierarchy fixture |
| `WorkspaceBackend` | identity/revision/state, conversations, updates, cleanup | same direct hierarchy fixture and enum dispatch; real Custom revision cases |
| `ConversationBackend` | identity/state/history, prompt/stream, notifications/resolution, abort, cleanup | direct hierarchy fixture; actual Custom prompt, interaction and reconstruction cases |
| `ActionBackend` | metadata, acceptance, local event stream, terminal/error behavior | App action struct/enum contract cases and real workflow tests |
| `ConditionBackend` | immutable state evaluation and typed failures | App condition struct/enum contract cases |
| `StateTyping` | local data conversion and all typing validations | App typing contract cases and typed real workflow execution |
| `LifecycleHooks` | sync/async methods, borrowed events, typed hook errors | App hook contract cases and sync/async workflow execution |
| `ChildExecutor` | concrete associated execution future, both enum strategies, local borrowed non-Unpin futures | App positive future proof and actual map/child execution |

Two Provider families retain distinct opaque continuation types without adding
Debug, equality, serialization, or Unpin bounds. Mismatched enum continuation
variants fail before delegation. Non-Clone owners and associated children are
used directly. App's child batches and completions come from real execution;
returned child execution futures remain concrete and unerased.

## Fixture integrity and consumer coverage

Typed scripts retain requests and detect wrong arguments, unexpected calls,
exhaustion, and required expectations left unconsumed. Error assertions preserve
the owning category and diagnostic context. Explicit malformed-source scenarios
are separate from conforming streams. Synchronization uses notifications,
polling gates, and bounded waits instead of arbitrary sleeps. Immediate storage
operations do not spawn tasks.

Storage exercises absent/present/tombstoned read conflicts and write skew, while
keeping ordinary store observations outside the transaction read fence. It
checks overflow, multiple staged revisions, rollback/drop without publication,
failure/cancellation invalidation, literal prefix matching, exclusive cursors,
per-page views, and immutable observations. Commit scenarios distinguish failure
before publication from publication followed by reply failure. An admitted
commit belongs to the ledger; losing the waiter does not revoke publication.
The fixture's explicit finalization gate models this ownership without claiming
to reproduce a supplied backend actor.

Actual Custom tests exercise accepted integrations, selected models, unary and
streaming tool loops, continuation identity, interactions, structured output,
policies/limits, notifications, cancellation versus observation, revisions,
same-process reconstruction, direct knowledge requests, and cleanup. Actual App
tests exercise direct Agent hierarchies, workflow values, checkpoints/history,
restore/deletion, validation before effects, child/map isolation and reduction,
incomplete completion rejection, atomic sibling conflicts, action cancellation,
observation ownership, reconstruction, and shutdown. Required regressions cover
captured-generation abort isolation, acquired-handler cleanup with primary-error
preservation, and shutdown reobservation after genuine read-only transaction
conflicts. Their synchronization gates and negative error controls are fixture
contracts, not native storage or provider evidence.

Positive compile proofs live with the behavioral targets. Existing negative
compile contracts are unchanged. App requires new negative cases in owning
public rustdoc; the original mock task froze `src/**`, so no alternate negative harness or
new rustdoc cases are introduced. Missing new negative cases are a source-freeze
limitation, not evidence that those rejection contracts were tested.

## Evidence and runtime qualification

Each invocation writes a fresh `target/mock-contracts/<UTC-time>-<id>/report.json`
and raw logs. Reports include source revision, working-tree state, compiler,
features, exact argument arrays, stage kind, duration, exit status, inventory,
counts, source-content fingerprints before and after execution, and
PASS/FAIL/INCOMPLETE classification. Source changes during a run invalidate its
receipt. Timeout and SIGTERM cleanup settles the owned process group before the
runner proceeds. Working-tree state must be read
alongside the revision: uncommitted test changes are not part of the baseline
commit. Reports never dump credential environment values.

`qualify_mock_runtime.py` first builds dependencies normally. It then launches
the resulting test executables directly under macOS `sandbox-exec` with all
network access denied, an empty inherited environment, and temporary HOME and
TMPDIR directories. Its explicit environment contains only PATH, HOME, TMPDIR,
and LANG. This evidence is separate from network-capable dependency preparation.
Unavailable sandbox support or failed sandbox admission produces incomplete
evidence, never an inferred runtime-independence pass. Primary CI does not force
Cargo offline mode.

## Validation status and history

[The production bug ledger](mock-testing-production-bugs.md) owns known defect
status and reproduction. [PR #22 validation history](pr22-validation.md) retains
the original failures, configuration follow-ups, scoped passes, interruptions,
and source-specific receipts. These dated records are not a current-checkout
pass or a standing release-gate waiver.

## Release and defect policy

The existing crate gates, Storage feature lanes, docs/doctests, all strict-live
verifiers, five-crate wrapper, and separate locked clean-checkout proof remain
required for workspace release completion. The new primary lane does not replace
them. The GitHub workflow provides deterministic CI only; publishing and
deployment remain separate.

Confirmed production defects belong in
[the production bug ledger](mock-testing-production-bugs.md), with enabled
regressions in primary CI. Mock-only work freezes production; separately authorized
fixes retain their regressions and fresh evidence. A red production
regression is not ignored, filtered, weakened, or converted into an expected
failure. Fixture and runner defects belong to the test work; production fixes require
separate authorization when the task freezes production. Passing
mock tests alone do not justify a claim of full workspace completion.
The ledger records resolved defects and any remaining coverage gaps. A
supplemental probe alone does not satisfy the required-CI regression policy;
resolved defects retain their enabled tests in the owning primary target.
