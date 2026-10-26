# Validation Matrix

Use this file for the canonical `ai-kit` validation sequence.

## Active Workflow Scope

Select authority from explicit user direction and the selected workflow before
choosing a gate. The presence of a checked-in workflow does not select it.
When selected, `agent-sdk/.codex/development/PROCESS.md` intentionally narrows
the workspace baseline to its agent-sdk-only validation boundary unless an
explicitly approved workspace-wide plan owns coordinated cross-crate work.
In an ordinary Agent-only run, use only the commands in `agent-sdk/AGENTS.md`;
never run the workspace baseline, `scripts/verify_workspace_strict.sh`, the
workspace docs-site gate, or another crate's validation. If another crate owns
required proof and no approved workspace-wide plan is active, stop for a
separately authorized workflow. Dependency compilation alone never broadens
scope; an approved coordinated plan owns its named cross-crate phases and gates.

The workspace baseline below remains canonical for workspace-scoped work and
for workflows that do not define a narrower validation boundary.

## Primary Deterministic CI

For routine cross-crate contract and consumer validation, run:

```bash
bash scripts/check_ci.sh
```

The focused mock entrypoint is `bash scripts/check_mock_contracts.sh`;
`--crate NAME` accepts only one of the five workspace crate names and reports
scoped evidence. Arbitrary filters and unknown arguments are rejected. The
seven behavioral targets directly implement the 16 public traits with test-local
structs and exhaustive enums and exercise the real Custom Agent/App. They never
construct supplied lower backends, use extension ingress, open sockets, start
backend processes, or require credentials. Existing extension, protocol,
process, persistence, and strict-live tests retain their separate ownership.

Both commands run Cargo serially with `--locked`. Storage mock targets use
`--no-default-features`; other crates use their default features. Primary CI
includes formatting, source guards, runner contracts, positive compile checks,
targeted Clippy, App's compile-only contract, and every behavioral inventory.
Behavioral inventories must be nonempty and execute completely with zero ignored,
measured, or filtered tests; compile-only stages are reported separately.
Independent stages continue after failures and the aggregate remains nonzero.

Use `python3 scripts/qualify_mock_runtime.py` for separate runtime-independence
qualification after dependency preparation. It requires macOS `sandbox-exec`
network denial and a credential-free environment; unavailable restriction support
is incomplete evidence. Dependency compilation may still need network access.
Follow [the mock contract](../../../../docs/mock-testing.md) for exact boundaries,
report fields, fresh log capture, and source identity. Do not retry failures into
pass claims or substitute cached or scoped receipts for complete execution.

Primary CI reduces routine live-service execution; it neither replaces the
release gates below nor proves built-in services, native persistence, or full
workspace completion. Confirmed production defects retain required enabled
regressions and entries in [the bug ledger](../../../../docs/mock-testing-production-bugs.md).
Unresolved failures and incomplete gates remain visible alongside passing evidence.

## Workspace Baseline

Run Cargo commands serially from the workspace root with
`CARGO_INCREMENTAL=0` and `CARGO_BUILD_JOBS=1`. Dependency-resolving commands
use `--locked`; a missing or stale lockfile fails instead of being updated:

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features --exclude storage-sdk --exclude app-sdk -- -D warnings
storage_lanes="$(python3 storage-sdk/scripts/feature_policy.py --all-backends --format lanes)"
while IFS= read -r features; do
    storage_args=(--no-default-features)
    if [[ "${features}" != - ]]; then storage_args+=(--features "${features}"); fi
    cargo clippy --locked -p storage-sdk --all-targets "${storage_args[@]}" -- -D warnings
done <<< "${storage_lanes}"
bash scripts/check_docs_site.sh
```

The Storage loop mirrors the wrapper's supported behavioral lanes; never use
Storage `--all-features`. App remains excluded from the shared Clippy command
because its owning gate supplies its exact checks later. Do not replace
crate-owned test blocks with `cargo test --locked --workspace --all-targets --all-features`.

`bash scripts/verify_workspace_strict.sh` reruns the workspace baseline and
invokes the crate-owned verify scripts in isolated subshells that source
`agent-sdk/strict-tests.env`, `provider-sdk/strict-tests.env`,
`tool-sdk/strict-tests.env`, and
`app-sdk/strict-tests.env`. Tool-sdk fails fast unless a real independently
configured MCP service is supplied.
The repository-managed scripted MCP fixtures remain deterministic transport
evidence and never satisfy that lane. The wrapper is full workspace
strict-live evidence only when every invoked crate-owned gate passes.

## Qualified Coverage And Completion

The workspace wrapper defaults to full coverage. Its explicit
`--skip-long-provider-batches` flag permits exactly these seven exclusions:

- `live::batches::openai_inline_batch_completes`
- `live::batches::openai_provider_file_batch_completes`
- `live::batches::openai_batch_preserves_mixed_row_outcomes`
- `live::batches::openai_batch_result_limit_is_enforced`
- `live::batches::anthropic_batch_preserves_mixed_row_outcomes`
- `live::batches::anthropic_batch_result_limit_is_enforced`
- `live::batches::anthropic_inline_batch_completes`

Report coverage as **QUALIFIED** and list these exclusions. An environment
toggle alone does not authorize partial workspace completion. Cancellation,
missing-resource, unsupported-input, Gemini, and every other case remain
blocking. Run completion still requires the separate locked clean-checkout
proof on unchanged source with fresh build outputs. Partial, interrupted, or
combined historical passes cannot satisfy it. A task-specific acceptance
revision does not change the repository's release gate.

Exactly these two Agent cases report raw model-copy quality separately:

- `strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains`
- `strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build`

Their independently observed exact native answer delivery, correlation, output
fidelity, recovery, streaming, cleanup and valid grade receipts remain blocking.

Exactly these two Provider cases also separate raw model-copy quality:

- `live::resources::openai_interactive_session`
- `live::resources::openai_interactive_cancellation_and_subsequent_progress`

Their required real interactions, exact call/result correlation, output progress,
authoritative terminals, cancellation and cleanup remain blocking. Function
turns require new audio and nonblank transcription after successful result
submission; pre-tool narration cannot satisfy that progress. The ordinary
speech turn retains its own audio, nonblank input/output transcription and
completion checks. Public Session evidence does not independently prove native
parity or semantic result use. The verifier requires two valid case receipts
and all three named boolean grades; missing, malformed or SDK-failed receipts
remain failures.

Report all untouched whole-string copy grades, including visible nonblocking
failures, without inferring model-quality acceptance or reliability. These
four named cases create no other exception; knowledge, pending-interaction
recovery and all other exact-output assertions remain blocking.

Preserve Provider's exact model/operation/case eligibility and complete evidence
requirements in [its strict guide](../../../../provider-sdk/docs/STRICT_TESTING.md).
The narrow Gemini streaming File Search unpaired-declaration exception proves
neither exclusive hosted-family behavior nor code execution. Management absence
after store deletion does not prove retrieval erasure. Only the exact eligible
generated-output deletion rejection may defer to documented provider expiration;
expiration remains unverified until actually observed. Borrowed stores and
retained outputs must remain visible in cleanup reporting; never claim global
resource absence.

## Crate-Owned Command Blocks

After the workspace baseline, run the exact crate-specific command sets from:

- `agent-sdk/AGENTS.md`
- `provider-sdk/AGENTS.md`
- `storage-sdk/AGENTS.md`
- `tool-sdk/AGENTS.md`
- `app-sdk/AGENTS.md`

Then run the crate-owned non-live or strict-live scripts required by the task.

## Strict-Live Notes

- `agent-sdk`: caller must source `agent-sdk/strict-tests.env` before direct
  `strict_live` runs or `verify_strict_live.sh`. The verify script is the
  canonical completion gate. Direct `strict_live` runs are for focused
  iteration after the configured Codex and OpenCode services are already
  running. For loopback endpoints, follow the selected-runtime startup commands
  and isolated configuration in the owning
  [environment example](../../../../agent-sdk/strict-tests.env.example) and
  [strict-testing guide](../../../../agent-sdk/docs/STRICT_TESTING.md) before
  declaring strict-live unavailable. Persistence and knowledge fixtures use
  real embedded Lance. The integrated 29-case inventory retains the Lance
  foreground-conflict composition; PostgreSQL backend proof remains in Storage.
  Preserve configured models, runtime versions, authentication, and the owning
  startup policy.
- `provider-sdk`: caller must source `provider-sdk/strict-tests.env` before
  direct `strict_live` runs or `verify_strict_live.sh`. The verify script is
  the canonical completion gate. Direct `strict_live` runs are for focused
  iteration after provider credentials and local Ollama are available. If a
  configured local Ollama endpoint is unreachable, start `ollama serve` before
  declaring strict-live unavailable; missing models and malformed `/api` URLs
  remain blockers.
- `storage-sdk`: the default verifier requires no alternative credentials or
  services and exercises Lance Database and StateStore. `--backend NAME`,
  `--changed-since BASE`, and `--all-backends` select additional coverage.
  Only selected alternatives require their environment/service prerequisites.
  Full migration qualification includes eight behavioral lanes and every legal
  feature subset. Never use Storage `--all-features`.
- `tool-sdk`: caller must source `tool-sdk/strict-tests.env` before direct
  `strict_live` runs or `tool-sdk/scripts/verify_strict_live.sh`. The verify
  script is the canonical completion gate and fails fast unless its documented
  real stdio MCP program, arguments, environment, timeout, tool name, and tool
  arguments are configured. Repository-authored fixtures remain deterministic
  contract tests and never satisfy this strict-live proof.
- `app-sdk`: caller must source `app-sdk/strict-tests.env` before direct live
  Cargo runs. `app-sdk/scripts/verify_strict_live.sh` validates and sources the
  file itself, runs non-live checks first, then uses separate Agent and app
  Lance StateStores for the real interaction and recovery journeys. The
  four-boundary process-crash proof retains an owned directory across child
  execution, reap, reopen and final owner cleanup.
- Missing or invalid strict-live env values, credentials, model ids, provider
  inventory, required local models, or failed documented startup/provisioning
  attempts are blockers. Missing already-running services are not blockers
  until the crate-owned startup or provisioning path has been attempted.
- Secret env values must follow the owning crate's credential semantics. A
  missing optional credential means absence; a present blank or malformed
  credential must fail typed validation instead of being silently trimmed or
  treated as no auth.

## Cleanup And Documentation

- Successful compile-fail and verification runs must leave crate-owned `wip/`
  directories clean.
- Workspace validation must leave no repository-root executable artifacts.
  Library-crate process fixtures belong under Cargo target directories,
  crate-owned temporary directories, or OS temporary directories; do not add
  generated binary fixture workflows to source, tests, scripts, docs, or
  skills.
- When a task changes behavior, validation guidance, or module ownership, update
  the owning `README.md`, `ARCHITECTURE.md`, `AGENTS.md`, focused guides, and
  source-contract tests in the same change.
