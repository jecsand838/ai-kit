# Validate an App integration

Run commands from the workspace root, with one Cargo process at a time. Use
the repository's selected Rust toolchain and committed dependency lockfile.
App has no Cargo features. All supplied persistence and knowledge fixtures use
LanceDB, which is always available from Storage without extra features. Pure
caller-defined trait probes remain separate contract evidence. Alternative
backend integration and compatibility belong to storage-sdk's selected lanes;
App does not enable those alternatives through development dependencies.

Keep the host awake throughout validation. App's durable deadlines use wall
time, so suspension can expire them while a platform's monotonic test watchdog
is paused. On macOS, `caffeinate -i bash app-sdk/scripts/verify_strict_live.sh`
prevents idle sleep for that command; it does not prevent lid-close sleep.

## Start with the behavior you changed

The primary direct-trait lane runs without supplied lower backends or services:

```bash
bash scripts/check_mock_contracts.sh --crate app-sdk
```

Its `mock_contracts` target exercises concrete structs and caller-owned exhaustive
enums for all five App workflow traits. `workflow_mock_composition` opens the real
App with caller Agent hierarchies, process-local transactional storage, and
concrete workflow values. It covers associated-child composition, workflow and
effect ownership, checkpoint operations, and same-process reconstruction. These
results do not prove native storage durability or real Agent behavior. The
workspace primary CI entrypoint is `bash scripts/check_ci.sh`; the
[mock testing contract](../../docs/mock-testing.md) owns inventory, fixture and
runtime-independence requirements. The [bug ledger](../../docs/mock-testing-production-bugs.md)
retains unresolved failures; scoped or passing runs do not replace a failed
required invocation. The existing complete App and release gates remain required.

For a workflow-language change, a focused run is:

```bash
cargo test --locked -p app-sdk --test expression_parity --test graph_contract --test schema_contract -- --nocapture --test-threads=1
```

For an Agent execution or recovery change, use the existing public-boundary
tests before running real services:

```bash
cargo test --locked -p app-sdk --test native_execution_contract --test native_streaming_contract --test outcome_recovery_contract --test workspace_contract -- --nocapture --test-threads=1
```

These targets construct caller implementations and real control-plane storage
where their fixture requires it. They can prove routing, fences, deadlines,
receipt handling and ownership. A deterministic Agent implementation cannot
prove that Codex, OpenCode, Ollama or a remote provider behaves correctly.

The [testing specification](../specs/workflows/TESTING.md) maps each behavioral
domain to its required cases. Extend an existing case owner before introducing
another fixture or target. Add or change tests first, record their actual
failure, then implement and obtain an independent review of the diff. A setup
error or missing import is not evidence of the intended behavioral defect.
A new coverage case that passes immediately needs no invented production fix.

## Effectful prompt-error regressions

`workflow_execution` uses its existing direct caller Agent/Workspace/Conversation
hierarchy to record an observable effect before returning unary Busy, InvalidConfig,
InvalidInput, Unsupported, or NotYetImplemented. Those categories do not establish
pre-admission rejection. With automatic retries enabled, each regression requires
one dispatch/effect and caller-owned uncertainty, without committing fabricated
workflow success. Accepted-stream controls require the same safety. A
stream-opening error after an observable effect must also preserve uncertainty
even though no stream handle was returned. Pending notification decisions retain
their original resolution receipts when the lower prompt returns an error.
An explicit caller Retry resolution after authoritative abort remains permitted and consumes
the existing retry budget. Actual workspace lookup failures retain pre-dispatch
backoff tests. No Provider/Tool dependencies or service substitutes are added.

These are ordinary correctness tests. An error category alone does not prove
that a dispatched prompt was rejected before it performed work.

## Run the complete App gate

```bash
git diff --check
bash app-sdk/scripts/check.sh
```

The [App check script](../scripts/check.sh) owns the exact target inventory. It
checks formatting and all targets, runs the deterministic/public-contract
tests and doctests with the explicit workload-scope selection below, executes the
seven short service-free examples, then checks
clippy, rustdoc and the workspace alias/path restrictions. Keep additions in
that script so an ordinary App check cannot silently omit new behavior. The
zero-runtime `compile_contract` target remains compiled by all-target checking
and Clippy. `prepare_contract` has an explicit four-case inventory and outcome
check, including the canonical four-boundary crash case. The full strict verifier
credits that successful execution from its own synchronous `check.sh` invocation;
it does not rerun the crash case or reuse a historical receipt. Standalone scoped
live runs retain the exact crash command.

Fixture validation is reported separately from App behavior: retain the two
`OwnedChild` cleanup cases, receipt-page lifetime probe and real-Lance
conflict-controller test while their machinery remains in use. Test-local
controls do not establish real-Agent behavior; all 18 live cases and the canonical
process-recovery assertions remain required.

The [documentation tests](../tests/docs_examples.rs) check required guide and
executable navigation, ordinary local file links and parsing of the complete
JSON workflow corpus. Behavioral cases execute the documented linear State
workflow, numeric loop, timer, join, child, map and synchronous Pipeline against real in-memory LanceDB
storage and assert their explained results. These checks do not prove Markdown anchor
validity, prose accuracy, example compilation or real-service journeys.
Compile the examples with `cargo check --locked -p app-sdk --examples`, and run the short
service-free executables to verify their assertions; `multi_agent` needs the
configured real services described in its source. The long performance consumer
has a separate explicit release run below.

## Select routine, diagnostic and maximum-capacity evidence

The existing `workflow_execution` and `worker_contract` Cargo targets keep all
their tests; none is ignored or compiled out. The ordinary check runs their
routine cases, including the 64-delivery
`notification_delivery_routine_preserves_receipts_replay_and_cleanup` regression
with receipt replay, public cancellation and joined cleanup. It explicitly
excludes the 256- and 1,024-delivery diagnostics, the maximum-capacity cases
below, and the four diagnostic preflight cases listed next. The selector lists the full and selected inventories, rejects missing cases or
unexpected filtering, and checks executed/ignored/filtered counts.

Diagnostic scope first executes these exact instrumentation checks with the same
verified executable and profile, separately from timed workload samples:

- `storage_observation_cancelled_wait_does_not_remain_active`
- `storage_observation_distinguishes_read_only_and_mutating_commits`
- `storage_transaction_observation_separates_native_calls_and_owner_drop`
- `checkpoint_poll_capture_excludes_suspended_time_and_retains_open_spans`

Each must be present, execute once and pass with no ignored cases or unexpected
filtering. Their receipts are labeled diagnostic preflight, not App behavior or
capacity evidence. Routine scope still uses the instrumentation where a behavioral
test verifies completed rollback for every rejected mutation.

Run the larger scopes separately:

```bash
python3 app-sdk/scripts/check_app_workloads.py diagnostic
python3 app-sdk/scripts/check_app_workloads.py capacity
python3 app-sdk/scripts/check_app_workloads.py diagnostic --profile capacity --case delivery_capacity_growth_measurement
```

`diagnostic` defaults to exactly `delivery_capacity_prefix_measurement` (256),
and may instead select `delivery_capacity_growth_measurement` (1,024). Both use
the existing 2,400-second outer observation budget. Neither proves the maximum
settlement boundary. `capacity` selects each of
the following exact tests serially, preserving its original authored deadlines,
required counts, assertions and supervised outer budget:

| Target | Exact test | Outer budget (seconds) |
| --- | --- | ---: |
| `worker_contract` | `admission_cleanup_does_not_charge_lazy_execution_lifetimes` (1,024 owners) | 9,300 |
| `workflow_execution` | `retained_revision_scan_accepts_short_pages_at_4095_4096_4097` (4,097 heads) | 9,000 |
| `workflow_execution` | `reopen_accepts_valid_short_pages_beyond_the_old_scan_window` (1,450 actions) | 3,600 |
| `workflow_execution` | `late_foreign_prefix_is_observed_before_any_generation_claim` (4,097 heads) | 9,000 |
| `workflow_execution` | `late_invalid_inventory_is_observed_before_any_generation_claim` (1,450 actions) | 4,800 |

Routine scope uses Cargo's `test` profile. Capacity scope uses the workspace
`capacity` profile: release inheritance, optimization level 3, debug information
off, debug assertions and overflow checks on, incremental compilation off, LTO
off, and 16 codegen units. Diagnostic scope permits `--profile test` (default)
or `--profile capacity`. `--case` selects one exact diagnostic or capacity case;
unknown cases and conflicting profile selections fail before execution.

The version-hint experiment permits exactly `LANCE_USE_VERSION_HINT=0` only
with the 256-delivery prefix diagnostic. Receipts identify it as a comparison
of native read/write policy, not default-backend or capacity qualification.
Use fresh child processes because Lance caches this native setting. All other
values, routine scope, the 1,024-delivery growth diagnostic and capacity scope
require the variable to be unset. Candidate qualification also uses the unset
default. The setting remains part of frozen-executable runtime identity;
comparisons must not bypass the existing identity checks.

The runner compiles before timing execution, with Rust 1.94.1, `--locked` and
one build job. It uses the same profile for compilation, inventory and execution,
verifies optimization/assertion/overflow settings from Cargo artifact messages,
and rejects conflicting compiler overrides. Verified binaries execute from the
`app-sdk` directory, matching Cargo's test working directory. Optional OS resource
accounting is probed before workload admission; unavailable metrics are recorded
without replaying the workload. Build failure is incomplete capacity
evidence. Preserve recorded failures and available resource measurements; do not
silently change the profile or delete unrelated files to finish a build.

For the delivery-profile comparison, run 256 deliveries in test/capacity/capacity/test
order using identical fixture inputs. Both optimized body-plus-shutdown samples
must improve against their paired test samples. Report file growth and available
memory measurements as well as time; Storage timings overlap and do not measure
exclusive CPU cost. Diagnostic counters distinguish read-only from mutating
commits, errors and dropped observers, and do not reencode JSON merely to count
bytes inside the measured path.

For a source-change comparison, the diagnostic runner can preserve the original
verified executable and later execute that same baseline after source changes:

```bash
python3 app-sdk/scripts/check_app_workloads.py diagnostic --profile capacity --case delivery_capacity_prefix_measurement --retain-baseline
python3 app-sdk/scripts/check_app_workloads.py diagnostic --profile capacity --case delivery_capacity_prefix_measurement --baseline-receipt /absolute/path/to/original/receipt.json
```

These options are diagnostic-only and mutually exclusive. Reuse requires a
settled matching pass, unchanged executable, compiler, lockfile, fixture and
runtime identities. The receipt distinguishes the executable's original source
from the current checkout; an old-binary pass does not qualify current code.
Candidate samples still compile and validate their own artifacts. Keep the
chosen comparison order, all failures and intervening work in the report.

Transaction timings begin after successful native admission and distinguish
forwarded calls from caller/scheduling gaps and consuming finalization. They
exclude pre-begin work. Checkpoint active-poll timings include synchronous work
inside that span and exclude suspension; they remain wall time, not CPU.
Direct-child user/system CPU deltas cover only verified test execution through
wait/reaping, excluding compilation and inventory. They cover all child threads
and provide no per-owner attribution. An optional resource wrapper or unavailable
metric is reported explicitly; cumulative peak RSS is never subtracted. Do not
add overlapping durations or treat dropped observers as native cancellation.

The script owns and settles only its launched process groups, records exact
inventories, source identity, command outcomes and cleanup in
`target/app-workload-checks`, and fails on interruption, missing/ignored coverage
or uncertain process settlement. Record the actual dataset filesystem and available
space/resources, without inferring a filesystem from a storage mode. A result
qualifies only its recorded profile, machine and filesystem. Process reaping is isolation
cleanup, not proof of graceful App joining.

Routine and diagnostic passes do not qualify any maximum-capacity boundary.
The maximum-capacity cases form a separate qualification task outside the
current Storage feature-split completion scope. Their historical passes and
failures retain their original source/command scope; the previously recorded
4,096-delivery timeout remains failed capacity evidence. A capacity claim requires
its own complete execution. These are controlled App/Lance persistence tests,
not real-Agent strict-live tests.

## Separate inventory capacity from live evidence

The four large inventory cases in `workflow_execution` create real App history
in Lance memory: 1,450 original actions or 4,097 retained run heads. App
reconstruction retains that same memory store. A test-local Storage wrapper
requests deliberately short pages at the 4,095/4,096/4,097 boundaries; negative
cases alter those returned pages to test rejection before mutation. These are
capacity, pagination-contract and negative-contract checks, respectively. None
constructs an Agent or proves strict-live, disk-reopen or process-recovery behavior.

Run each case with exact selection under its existing process supervisor and
budget. Record phase entry, completion, elapsed time and relevant counts for
generation, initial shutdown, enumeration, each reopen, snapshot/history,
deletion, subsequent execution and final settlement. Preserve the last active
phase on failure or outer timeout. The history case currently runs another full
1,450-action workflow after deletion; record that cost separately. Artificially
short-page timings are not representative application benchmarks. Successful
generation alone does not qualify the complete case.

Cleanup reobservation retains the same App and driven runtime. An observation
timeout cannot cancel accepted cleanup. For these fixtures' valid
CancelAndDrain-only owners, a retained Integration result follows completed
joining; preserve it and stop reobserving. This conclusion is specific to that
shutdown path, not a general property of App errors. Unexpected outcomes retain
uncertain ownership under the process supervisor. A later join never erases the
original body failure or initial shutdown timeout.

## Run explicit release workloads

The deterministic cancellation cases separately prove acknowledgement within
the existing abort grace and expiry into public `Unknown`. For expiry, resolve
the exact sequence/generation with `Resolution::Fail` and retain the original
Timeout category/message. A shutdown observation timeout does not release the
original cleanup owner: keep its runtime driven until a later observer joins.

The live process-loss recovery scenario retains the original Agent ID across
explicit caller reconstructions. It joins each failed construction before a new
attempt, admits another only after an exact clean Reopen/StateCommit transaction
conflict, and permits one final construction after the exact existing lifecycle
integrity-repair report. It neither changes Agent Reopen's single-attempt policy
nor retries arbitrary errors. Missing-question diagnostics retain only public
target identities and bounded native session/request/tool status correlations;
they never print native prompt, answer, tool input or tool output bodies.

The [performance consumer](../examples/performance.rs) is mandatory for phase 9
release acceptance. The ordinary App gate compiles it through all-targets but
does not execute its long workloads. Build once, then run the resulting artifact
sequentially; do not include build time in the measurements:

```bash
cargo build --locked -p app-sdk --release --example performance
```

Set `PERF_BIN` to that artifact's absolute path (respect any target-directory
override) and `PERF_ROOT` to a fresh, existing, task-owned parent directory. Seed
commands refuse existing child directories. Each reopen is a separate process
and must follow successful seed cleanup:

```bash
"$PERF_BIN" linear --root "$PERF_ROOT/linear"
"$PERF_BIN" seed-history --root "$PERF_ROOT/history1000" --rows 1000
"$PERF_BIN" seed-history --root "$PERF_ROOT/history10000" --rows 10000
"$PERF_BIN" reopen-history --root "$PERF_ROOT/history1000" --page-size 128 --open-only
"$PERF_BIN" reopen-history --root "$PERF_ROOT/history10000" --page-size 128 --open-only
"$PERF_BIN" reopen-history --root "$PERF_ROOT/history1000" --page-size 128
"$PERF_BIN" reopen-history --root "$PERF_ROOT/history10000" --page-size 128
"$PERF_BIN" ready32 --root "$PERF_ROOT/ready32"
"$PERF_BIN" map4096 --root "$PERF_ROOT/map4096"
"$PERF_BIN" repeated100 --root "$PERF_ROOT/repeated100"
"$PERF_BIN" nested-cancel --root "$PERF_ROOT/nested-cancel"
"$PERF_BIN" delayed-observer --root "$PERF_ROOT/delayed-observer"
```

Run under an external process watchdog of 34 minutes and capture process peak
RSS separately from consumer JSON, using `/usr/bin/time -l` on macOS or the
platform equivalent. Confirm process exit before reopening or deleting its
owned paths. The consumer has a 30-minute workload budget and two-minute cleanup
envelope; the outer watchdog also bounds synchronous native Drop joins.
Record source/artifact digests, toolchain, hardware, OS, feature lane, backend,
fixed cache configuration and warm/cold ordering. Keep open-only and traversal
RSS in separate processes. Both include one bounded identity/state lookup;
`app_open_ns` measures App::open separately. Repeat each size/mode in 20 separate
sequential processes before reporting recovery p95; one invocation is one sample.

`seed-history --rows` selects exactly 1,000 or 10,000 retained checkpoint rows,
including initial, control, action and terminal boundaries. The existing native
State loop commits 332 or 3,332 actions and halts once after its first action;
explicit public execution resumption adds the second control row, giving
`3 * actions + 4` total rows. JSON keeps actual actions and retained rows separate.
The seed and traversal check the halt/resume evidence, exact state and ordinals,
all action commits, total rows, final checkpoint and page/cursor completion.
The separate `linear` workload still checks 1,000 distinct State nodes.
Open-only verifies the saved run's terminal snapshot but does not traverse
history or claim the seed's retained-row count has been rechecked.

The original release plan requires 10,000 retained rows and a 1,000-row memory
comparison. Preserve earlier action-sized datasets and their immutable runners
under their actual larger row counts; do not relabel them as exact row-sized
datasets or claim a matched performance improvement across different sizes.
All commands above are mandatory for the
[required release workloads](../plans/2026-09-13/COMPLETION_PLAN.md#crash-and-performance-workloads).
`ready32` runs 32 distinct completion permutations of 32 simultaneously live
caller sources, verifies no partial barrier visibility, and separates source
release, barrier and whole-wave timings. Whole-wave timings include deliberate
public snapshot probes. `map4096` requires exactly eight live sources at its
first gate, never more than eight, and verifies every retained child's state
and ordinal through pages of 128. Its parent deadline is the declared 30-minute
workload budget; individual leaf nodes keep the 60-second deadline. Child
inventory verification time is reported separately and remains within the
process RSS measurement.

`repeated100` admits runs sequentially and directly polls at most 16 original
executions concurrently, proving the first sixteen are actually live together.
Each run latency starts before its admission; throughput includes the complete
journey. This is a finite 100-run soak of the caller's declared envelope, not a
concurrent-admission contention benchmark or the full production active-run limit.
`nested-cancel` admits two intermediate children and sixteen held leaf sources,
then checks cancellation within its declared 30-second grace, unchanged state,
all terminal children and destruction of every original source. `delayed-observer`
leaves one public wait unpolled through 256 partials, independently confirms the
terminal result, waits two seconds and checks that result and retained history.
It does not accumulate the transient partials.

Source lifetime quantiles count construction through Drop; they are distinct
from commit, step, run and recovery latency. Each reported latency includes its
sample count, and a one-sample workload is not a p95 distribution claim. The
consumer's finite counters measure its own caller sources. App's public joined
shutdown supplies task ownership evidence; private App task counts are not
exposed. No Agent or tool handler is constructed in these workloads, so their
Unknown-effect and resource contracts require the separate integration/fault
suites. Capture source/artifact hashes and actual exit/cleanup results with each
JSON record; an implementation or successful build supplies no workload proof.
Keep each command's peak RSS in its own process, preserve the same backend/cache
settings across comparisons, and investigate any measured p95 regression above
2x the same-host post-correctness baseline. That comparison is an investigation
trigger, not a universal production SLA. The separate crash-at-boundary workload
requirements remain in force.

## Configure real App services

Copy [the tracked environment example](../strict-tests.env.example) to the
ignored `app-sdk/strict-tests.env` and review every value. Start from the
workspace root so its target-backed paths resolve correctly. The example
documents how to launch the reusable OpenCode precheck server after loading
that configuration. Keep the environment file private and out of version
control and logs.

The [precheck](../scripts/precheck_strict_live.sh) requires:

- `curl`, `jq`, `opencode` and `ollama` on the executable path.
- The exact OpenCode server version selected in the
  [Agent specification](../../agent-sdk/specs/SPECIFICATIONS.md), plus a healthy
  reusable OpenCode endpoint with that same version.
- A reachable Ollama API whose base URL ends in `/api`, and the exact installed
  tool-capable model selected by `TEST_OLLAMA_MODEL_OPENCODE_DEFAULT`.
- An OpenCode model configuration with at least 65,536 context, deterministic
  build temperature, and the matching Ollama `/v1` endpoint. Configure the
  actual daemon/model capacity too; a declaration alone does not prove it.
- Sharing, plugins, MCP and background subagents disabled for this suite, and
  only the explicitly selected Ollama provider connected.
- The exact App-owned paths under `target/app-sdk-strict-live/opencode` for the
  reusable precheck server's home, database and XDG directories.

Endpoints must be credential-free HTTP(S) base URLs without surrounding
whitespace, query strings or fragments. The tracked example selects a local
model and ports; changing the model requires changing all matching fields in
the configuration, not just its display name. The precheck queries the running
server's effective configuration as well as validating the supplied document.

Run the complete real-service App verifier:

```bash
bash app-sdk/scripts/verify_strict_live.sh
```

The verifier loads the ignored environment, runs the precheck, then runs the
whole App check script. That invocation executes all four `prepare_contract`
cases, including the canonical four-boundary LanceDB process-loss case exactly
once, before the verifier executes all 18 `workflow_live` cases serially. A
missing or unhealthy prerequisite fails. Do not convert that failure into a skip
or replace the backend with a recording or scripted server.

The [live suite](../tests/workflow_live.rs) and its
[service owners](../tests/live_support/mod.rs) launch real child processes on
unique ports and use unique test-owned durable domains. They must join and
clean up those children even when assertions fail. Stop a reusable precheck
server only if you started and own it. Never kill an unrelated daemon or
remove another application's data to make a gate pass.

The test-local child owner retains its actual operating-system child across
cancelled stop observations. Successful explicit stop requires an exit/reaping
receipt; sending termination alone does not establish cleanup. The deterministic
`worker_contract` target separately exercises ordinary stop and a genuinely
pending stop observer that is dropped before the same owner finishes reaping.
These self-executable process controls are infrastructure evidence, outside the
`workflow_live` real-service inventory.

Gated live observations release their existing forwarding gates before
independent App and service cleanup, including after an assertion, error or
observation timeout. Captured cleanup errors and panics are reported before the
original observation failure is restored, so secondary failures remain visible
without replacing the primary. Explicit process stop retains its ten-second
reaping bound; the synchronous Drop fallback retains its two-second bound.
If the fallback cannot obtain a reap receipt because of an operating-system
error or expiry, it reports that uncertainty and retains the owned paths. That
failing fallback is not a guaranteed join or successful cleanup.

The native service-interruption case preserves the exact Unknown target and
caller Fail outcome. Its original 30-second Drain must either succeed after a
completed lower shutdown success, or return Integration after the actual lower
Agent returned a clean InvalidState / Shutdown / Cleanup repair error. The
forwarding owner retains that typed receipt; missing completion, Timeout, extra
causes or supplementary failures remain errors. Repeated Drain must replay the
same result without another lower shutdown, and public App access must be Closed.
The retained expected failure is not normalized into production success.

The shutdown-crash case first observes a clean lower Agent integrity report:
`InvalidState / Reopen / Lifecycle`, with no narrower detail, cause, HTTP status,
truncated diagnostics or supplementary cleanup failure. It then performs one
explicitly named caller reconstruction before supplying the ready Agent to App.
Both constructions and App opening share the original 30-second observation
budget. This is a specific stopped-service crash trajectory, not a generic
retry policy: the native question is held and the exact service is killed and
reaped before cancellation. App Unknown alone is not interpreted as a lower
terminal or cancellation fact. Unexpected first success is a scenario mismatch,
with the returned Agent retained for explicit shutdown. Other failure categories
remain failures; the original integrity report stays visible. All App Unknown,
refused Retry, exact Accept, cancellation, deletion and retained-history checks
remain blocking. Every parent failure path independently attempts child reaping,
App or untransferred-Agent shutdown and native-service cleanup before restoring
the original failure. Construction and consuming Agent shutdown tasks remain retained across their
observation bounds and are explicitly joined. Unconfirmed cleanup or any
unexpected construction failure without a returned owner retains the owned
paths; task cancellation alone does not prove SDK cleanup.

The recovered-notification authority cases additionally inject one typed failure
or held read through a concrete forwarding App store. Successful rows, native
transactions and notifications still come from real LanceDB and OpenCode. Report
these as real integration with controlled fault injection, separately from
undisturbed strict-live evidence; they prove the selected recovery boundary,
not general service availability.

## Validate changes across crate boundaries

App consumes Agent and StateStore traits. Changes to an associated child,
portable DTO, cancellation rule or shutdown promise must pass each affected
consumer's compile, deterministic and real integration cases. A passing App
test with a caller Agent does not substitute for agent-sdk's native suites.

The workspace completion pass is:

```bash
bash scripts/verify_workspace_strict.sh
```

The [workspace wrapper](../../scripts/verify_workspace_strict.sh) runs format,
alias/path and artifact checks, the non-storage all-feature clippy lane, the
supported storage lanes, documentation, and all five crate strict verifiers.
Each verifier requires its own populated ignored environment and real
prerequisites. The [App operating contract](../AGENTS.md) lists the individually
invocable lanes when diagnosing a failure. Run those lanes without changing
their feature sets or masking a failing verifier.

## Report the proof that actually ran

| Evidence | What it establishes |
|---|---|
| Compile and public-surface contracts | Type composition, visibility, ownership bounds and allowed API shape. |
| Deterministic behavior | The tested App transitions, order, errors and fences under controlled inputs. |
| Pinned-rurr fixtures | The specific source-derived value, type, error or order at the recorded archive member. |
| Real-service test | The exercised built-in backend, installed version, selected model and service journey. |
| Same-process close/reopen | Durable reconstruction across those explicit lifecycle operations. |
| Fresh-process crash test | Recovery after the synchronized loss of the owned process at the tested boundary. |
| Measured workload | The recorded scale, latency, memory and task/thread behavior on that environment. |

For each run, record the command, feature lane, exit status, case count, selected
versions/models, and material limitations. Keep source-derived parity separate
from executing an installed oracle. Keep one forced crash boundary separate
from coverage of every creation, receipt, notification, cancellation and
history-deletion boundary. Neither a configured numerical limit nor a passing
small example establishes a production operating envelope.

Release acceptance also includes the prescribed crash/performance workloads
and a locked clean-checkout pass. Preserve the reviewed source revision and
logs so the result can be reproduced. A partial focused run, an unrun gate or
an externally blocked service remains explicit in the completion report.

## LanceDB process-loss proof

For standalone scoped qualification, execute the canonical case exactly:

```bash
cargo test --locked -p app-sdk --test prepare_contract \
    staged_action_and_atomic_barrier_survive_forced_process_loss \
    -- --exact --nocapture --test-threads=1
```

The full strict wrapper already executes it through `check.sh`; do not add
another invocation there.

The canonical App strict verifier runs
`staged_action_and_atomic_barrier_survive_forced_process_loss` from
`prepare_contract` against real directory-backed LanceDB. Its four boundaries
are a recomputable State stage, an accepted Input answer, before atomic barrier
commit, and after commit before its receipt is observed. Each child and reopen
parent receives the same owned directory. The parent kills and reaps each
child before cleanup. Values, checkpoints, effects and revisions remain exact.
This proves forced App process loss, not machine-power-loss durability.

All persisted live fixtures also use LanceDB. The compensation and unknown
workspace cases coordinate real stale publications: a separately opened Lance
connection commits an unrelated receipt after the lower transaction stages its
writes. The original commit must return its real `TransactionConflict`. The
once/armed test controller never fabricates an outcome or changes Agent/App
production code. Deterministic workspace-deletion timeout coverage explicitly
injects a typed lower-boundary error; it proves App handling, not a Lance native
timeout or admission policy. Native timeout/admission evidence belongs to Storage.
