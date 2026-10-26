# app-sdk AGENTS

This file adds app-specific rules to the workspace root `AGENTS.md`.

## Authority

- `specs/SPECIFICATIONS.md` is the sole exact public allowlist.
- Focused specs own behavior and private correctness.
- `agent-sdk/specs/API.md` and current lower crate-root contracts define the
  only legal composition boundary.
- The user-approved 2026-09-13 completion goal makes the fixed rurr core
  observable semantics authoritative. RESEARCH.md pins exact provenance; newer
  external versions are background, never a silent oracle upgrade.

Only crate root and `app_sdk::workflow` are public. The authoritative ledger
defines the exact allowlist and counts; they are not repeated here.

## Public Boundary

- Add no public item absent from the ledger.
- Keep runtime handles and identities opaque. Public fields exist only on the
  exact passive DTO/output boundaries.
- Keep no public mutable workflow AST, lower native record/handle/config,
  generic repository/conformance owner, compatibility path or public erased
  backend. The sole ledger authorizes five static App behavior seams and the
  exact controls/history/stream/state/graph operations required by pinned rurr.
- StateSerialization alone may privately erase field/tag codec callables for
  process-global registration. No action/hook/condition/typing/child/backend
  value enters that registry. ChildExecutor alone has its App-only GAT future.
- `WorkflowDefinition` parses one strict versioned typed document. JSON is
  syntax, never an arbitrary execution model.
- App-sdk receives one fully built `A: agent_sdk::AgentBackend` implementation
  per `AgentSpec<A>`. No supplied lower handle is a generic default or required implementation. App-sdk has no direct provider-sdk or tool-sdk
  dependency and does not reconstruct or inspect lower configuration.
- AppConfig requires a direct caller-built CS: StateStoreBackend even for
  process-local use. AppState and App-owned Redb construction are deleted.
  App handles preserve CS/A and the concrete App-owned action/condition/typing/
  hook/child-executor parameters; associated lower children stay exact.
  Homogeneous inventories use caller-owned exhaustive enums for heterogeneity,
  never lower erasure or normalization. Only App-owned no-behavior/raw-state
  defaults exist under workflow.
- Retain Agents under `Arc<A>` without requiring `A: Clone`; App owns eventual
  consuming `A::shutdown` after transfer.
- No opaque lower value is cloned, formatted, serialized, hashed, compared, or
  inspected to infer compatibility.

## Correctness

- Deterministic execution is bulk-synchronous: one immutable base checkpoint,
  complete canonical ready set, isolated sibling staging, canonical reduction,
  and one atomic barrier commit.
- Admission atomically creates checkpoint zero. Control-only truth transitions
  create state-preserving checkpoints.
- The current checkpoint is authoritative for state, frontier, status, and
  superstep.
- External effects commit intent before dispatch and terminal-or-unknown
  evidence afterward. Receipt loss is exact-read reconciled; unknown effects
  are never automatically retried.
- Split control uses reservation generations; a stale owner never commits.
- Child/map identities are structural. Exact child relation, control,
  definition, materialization, checkpoint zero, and capacity commit before
  activation.
- Cancellation is explicit and monotonic. Observer/run-handle drop never
  cancels; execution-owning stream drop revokes uncommitted advancement under
  the specified commit-admission rule, preserving accepted-effect uncertainty.
- Workspace replacement is revision-fenced full replacement with complete
  preflight, lower fan-out, compensation, then atomic app publication.
- Immutable checkpoint history and terminal results remain until explicit
  deletion; active capacity releases independently. Deletion seals admission
  and removes bounded resumable pages. Sequences never renumber, and forks
  copy clean state so they do not require a source-retention graph.
- Tasks, channels, queues, retries, graph/state values, and records are
  bounded, owned, cancelled, drained, and joined.
- Never await while holding a synchronous lock. While a StateTransaction is
  live, await only its own transaction operations or a transitively
  transaction-pure receiver method.
- No lower call occurs under storage mutation authority.
- Safe tracing is unconditional and derived from committed typed transitions;
  it contains no secrets, content, protocol payloads, paths, or lower ids.

## Private Structure

Use functional owners:

- `app`: config, agents, lifecycle, admission/revocation, shutdown;
- `workspace`: materialization, revision fencing, compensation, deletion;
- `workflow/definition`: schema, validation, compilation, child graph;
- `workflow/execution`: BSP, routing, reducers, child/map, capacity;
- `workflow/interaction`: lower attempts, notifications, unknown effects;
- `workflow/run`: actor, scheduling, controls, cancellation, recovery,
  retention, snapshots;
- `storage`: keys, codecs, StateTransaction execution, receipts, exact reads.

Delete duplicate state flow before splitting files. Do not add generic
`core`, `common`, `utils`, repository, facade, callback, or helper layers.
Use closed data-carrying enums and receiver methods, not option-soup structs,
same-root lifecycle carriers, loose functions, macros, codegen, aliases, or
speculative traits.

Ready private app records use epoch `app/v7`. Only the cold, one-way
`app/v6` migration specified in `specs/STATE_STORE.md` is authorized, inside
the existing AppStore before generation claim. Read-only preflight precedes
the root's explicit migration state; only that state permits mixed epochs.
Keep one canonical run schema and codec, with legacy decoding confined to
migration. Unsupported epochs and corrupt v7 data fail without fallback,
reset, downgrade, or lower effects. Settle older clients and take a physical
backup before cutover.

## Tests

- Change tests before production.
- Production `src/**` contains no tests or test-only cfg.
- Static tests enforce the exact allowlist, privacy, dependencies, ownership,
  epochs, and prohibited constructs without phrase-locking private topology.
- Compile tests prove a non-Clone caller Agent hierarchy remains exact through
  `A::Workspace` and its associated Conversation without erasure.
- Behavioral proof covers every A01–A31/C01–C12 in-scope row through the
  focused domains in specs/workflows/TESTING.md, including real two-agent
  execution, local/sync streams, custom child executors, history and docs.
- Every area requires observed meaningful red first, passing tests/gates and
  independent subagent code review with actionable findings resolved.
- Strict-live uses real configured services, unique test-owned durable
  domains, and no mock, fake, fixture service, recording, scripted substitute,
  skip, or synthetic success.
- All supplied App test/example persistence and knowledge fixtures use LanceDB.
  Alternative native backend tests belong only to Storage; do not opt them into
  App development dependencies. Pure caller-defined trait probes remain valid.
- App process-loss tests use isolated owned Lance directories. Parents pass the
  exact directory to children, kill/reap before reopen, and remove it only after
  all owners settle. No database service, `psql`, or backend-switch env is needed.
- `strict-tests.env.example` is tracked; `strict-tests.env` is ignored, never
  printed, and never staged.

## Validation

After a source-complete change, run from the workspace root in this order:

```bash
git diff --check
cargo fmt --all --check
bash app-sdk/scripts/check.sh
cargo clippy --locked --workspace --all-targets --all-features --exclude storage-sdk --exclude app-sdk -- -D warnings
bash storage-sdk/scripts/check.sh --all-backends
bash scripts/check_docs_site.sh
bash scripts/check_no_type_aliases.sh
bash scripts/check_no_path_wiring.sh
bash app-sdk/scripts/verify_strict_live.sh
bash scripts/verify_workspace_strict.sh
```

App-sdk has no Cargo features. Storage's supplied Lance `StateStore` is
unconditional and needs no backend feature; its Redb, PostgreSQL and SQLite/Turso
alternatives require explicit Storage features. A custom `CS: StateStoreBackend`
requires no App feature. The workspace validates Storage feature lanes
independently. Run one Cargo command at a time. Missing real
prerequisites fail rather than skip. Never weaken a gate or synthesize success.


## Routine and maximum-capacity execution scopes

The routine App check runs the 64-delivery receipt/replay/cleanup regression and
all other ordinary workflow/worker behavior tests. The same Cargo targets retain the
256- and 1,024-delivery diagnostics and all maximum-capacity cases: 1,024 owners,
two 4,097-head inventory cases and two 1,450-action history
cases. No test is ignored or compiled out. The routine selector explicitly
excludes those named cases and the four measurement-calibration tests
listed in `docs/STRICT_TESTING.md`. Diagnostic scope requires those four tests
as exact, successful preflight checks outside timed samples, using the same
verified executable and profile. They validate instrumentation, not App behavior.
Run the diagnostic or all capacity cases with `python3 app-sdk/scripts/check_app_workloads.py diagnostic` or
`capacity`; `docs/STRICT_TESTING.md` lists exact names and original outer budgets.
The selector must verify exact nonempty inventory and executed/ignored/filtered
counts, preserve command failures and settle invocation-owned processes.
Routine checks use the test profile; dedicated capacity checks use the workspace
`capacity` profile (optimization level 3 with debug assertions and overflow checks).
Diagnostics accept explicit `--profile test` or `--profile capacity`, and diagnostic
and capacity scopes accept one exact `--case`. Compile before measuring execution,
verify effective Cargo artifact settings, reject conflicting overrides, and record
the profile with every receipt. An optimized pass never qualifies an unoptimized
build or a different machine/filesystem.
The native version-hint override is diagnostic-only: accept exact
`LANCE_USE_VERSION_HINT=0` only for the 256-delivery prefix, record its policy,
and retain it in runtime identity. Routine, growth, capacity and candidate
qualification require it unset. Never change it inside SDK production.
Do not change counts, authored deadlines, assertions or historical failures to
obtain a routine pass. Maximum-capacity qualification is a separate task outside
the current Storage feature-split completion claim. Routine and diagnostic
results never substitute for maximum-capacity or strict-live evidence.

## Approved reductions and implementation exceptions

Reuse existing owners before adding code. Remove AppState/store transaction
sums, singleton controls/settlement, current-plus-previous history duplication
and parallel unary/stream drivers rather than preserving compatibility routes.
The existing pinned workspace futures-util package may become a direct App
dependency for Stream contracts. The exact already-locked sha2 0.10.9 package,
without default features, may be used solely for the publicly observable pinned
child identity digest. Remove the old structural hash owner instead of retaining
two identity routes; no other dependency expansion follows.

The shared supervised execution worker constructs local child futures on its
owning worker instead of inventing an impossible generic Send bound. Preserve
its compile/runtime/review obligations in specs/APP.md and
specs/workflows/TESTING.md: bounded workers, no recursive admission deadlock,
driven runtimes, local-value isolation and join-on-drop proof. The worker_contract
and child_executor_contract targets exercise these ownership boundaries; their
existence does not replace passing evidence and independent review for a change.
No extra engine, public ChildTask, erased executor future or detached task is
authorized.

## Primary mock CI

The direct-trait targets are required by `bash scripts/check_ci.sh` and can be
scoped with `bash scripts/check_mock_contracts.sh --crate app-sdk`. Follow
[the mock test contract](../docs/mock-testing.md). Keep these credential-free
targets separate from existing integration and strict-live verification; the
existing gates above remain required. Production regressions stay enabled and
are recorded in [the bug ledger](../docs/mock-testing-production-bugs.md).
