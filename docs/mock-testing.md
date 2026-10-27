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

[The CI workflow](../.github/workflows/ci.yml) runs four `ubuntu-24.04` jobs on
pull requests targeting `main`, pushes to `main`, and manual dispatch. It uses
read-only repository permissions and no provider credentials or service
containers. Configure **Formatting**, **Tests**, **Linting**, and **Security audit**
as required status checks; replace any former **Deterministic CI** requirement.

| Check | Work | Job ceiling |
| --- | --- | --- |
| Formatting | Pinned Rust toolchain, `cargo fmt --all --check` | 5 minutes |
| Tests | `python3 scripts/mock_contract_runner.py ci-tests`: guards, runner contracts, unified compile proofs, all seven public-trait mock targets | 20 minutes |
| Linting | `python3 scripts/mock_contract_runner.py ci-lint`: one Clippy invocation over the five libraries and selected mock targets, with warnings denied | 10 minutes |
| Security audit | Pinned prebuilt `cargo-audit`, committed `Cargo.lock`, current RustSec advisories | 5 minutes |

Formatting gates Tests, and Tests gates Linting so Clippy can reuse compiled
outputs instead of starting a second cold build concurrently. Failed or cancelled
prerequisite jobs prevent their dependent jobs from running. Security audit runs
independently. Separate jobs add runner setup and cache transfer overhead in
exchange for distinct checks. The existing local `bash scripts/check_ci.sh`
entrypoint still runs formatting, guards, runner contracts, build, Clippy, and
all mocks together; it does not perform the security audit. The split runner
modes preserve the same feature selection and complete-inventory requirements.

Tests requires all seven mock targets on every run, including cache hits.
Strict-live, native backend integration, capacity, dependency test suites,
optional-backend matrices, and the macOS-only restricted runtime qualifier stay
outside this workflow. App's existing `compile_contract` is compiled but not run.
These exclusions do not change the separate release gates.

Cargo dependencies still compile, including unconditional LanceDB/Lance/Arrow
dependencies. Tests and Linting install their native build prerequisites and use
two build workers, no incremental compilation, and no development/test debug
information. Both cache Cargo downloads and `target/debug` by Ubuntu version,
architecture, toolchain, explicit build-settings version, lockfile, and workspace
manifests. Separate `-tests` and `-linting` primary keys let each job preserve its
own outputs. A cache miss can restore the other job's current dependency cache,
the previous combined CI cache, or an older dependency cache under the same
toolchain/settings prefix. Cargo decides what needs rebuilding. Exact hits do
not upload another copy. Formatting and Security audit do not transfer build
caches or install native build prerequisites.

`main` maintains the shared cache. GitHub isolates PR-created caches to their PR,
so the first `main` run creates its own cache. Test receipts are never cached.
After a failed cache-miss validation, Tests or Linting preserves partial compiled
outputs under a unique key extending its primary key. Cancelled runs do not save
partial caches. A rerun can restore the partial cache, let Cargo validate it, and
execute its required checks again. Partial caches never satisfy a check or turn
the original failure into a pass; only a successful run saves its normal key.

Superseded runs for the same PR or branch are cancelled. Tests sends TERM to the
runner after 18 minutes with a 19-minute hard step backstop; Linting uses 8 and
9 minutes respectively. GNU `timeout` permits the runner to terminate and join
its Cargo process group before archiving, with a ten-second forced-kill backstop.
Timeouts fail validation and do not waive tests. Job ceilings bound spending;
checks finish as soon as work completes. An unusually slow cold build can reuse
its partial cache on a failed-job rerun, but incomplete runs cannot authorize
merging. Local timings and cached recovery do not establish cold hosted timing.
Fresh reports and logs are uploaded under separate job-specific artifact names
and retained for seven days. Local runners default to one compiler worker unless
callers set `CARGO_BUILD_JOBS`; Cargo commands themselves remain serial.

### Security audit policy

The security job downloads the official cargo-audit 0.22.2 Linux binary and checks
its pinned SHA-256 before executing it. It fetches current RustSec advisories and
scans the entire committed lockfile, including optional and target-specific
packages. It neither compiles nor tests dependencies, updates the lockfile, nor
runs provider requests. Scanner download, checksum, database, and audit failures
fail the job. The audit runs outside the checkout with an explicit lockfile path;
repository-local scanner configuration cannot silently weaken the check.

Known vulnerabilities and unsoundness advisories are blocking. Unmaintained
package warnings remain visible but are not denied. `--no-yanked` omits registry
yank checks, not security advisories. There are no ignored advisory IDs, baseline
exceptions, or `continue-on-error`. Explicit Bash pipefail preserves the scanner's
failure status while `tee` retains the complete diagnostic log as an artifact.
Existing dependency findings require remediation or a separately reviewed policy
decision; adding the job does not approve dependency changes or suppressions.

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
