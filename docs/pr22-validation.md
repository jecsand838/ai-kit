# PR #22 validation history

This is dated execution evidence for the public-trait mock implementation and
its cancellation/configuration follow-ups, committed in `1de968b3` and
`d3b763e7`. It is not an active workflow, a prerequisite checklist, or evidence
that a later checkout passed. Source identities and incomplete/failed results
below retain their original scope. Raw reports remain local under `target/` and
are not included in Git.

Use [the mock-testing guide](mock-testing.md) for current commands and
[the production bug ledger](mock-testing-production-bugs.md) for defect status.
At the closing source recorded here, primary CI was red on the App shutdown
conflict, M001 was resolved, and M002 remained unfixed. The linked bug ledger
records subsequent repairs. Earlier missing Agent settings and native
authentication were corrected in the follow-ups below.

## Original mock implementation — 2026-10-25

The production baseline is `ed507711410e4f75aa98f44819ab80fedd8e806e`.
The stable primary run executed 132 Rust cases across all seven targets:
131 passed and the enabled `MOCK-AGENT-001` regression failed. All 18 runner
contracts passed. Primary CI completed 32 stages, with 31 passing and only the
Custom Agent composition stage failing. Formatting, guards, positive compile
contracts, targeted Clippy, and App's existing compile-only contract all passed.

| Invocation | Result | Receipt beneath `target/mock-contracts/` |
| --- | --- | --- |
| Primary CI | FAIL: one confirmed production regression | `20261025T165348Z-817157b1/report.json` |
| Credential-free, network-denied runtime | FAIL: same sole regression; all 132 cases executed | `20261025T165457Z-34708207/report.json` |
| Separate existing release gates | FAIL overall; completed and incomplete coverage described below | `20261025T165743Z-17b9ecba/report.json` |

All three invocations recorded the same before/after source fingerprint:
`f8edd49daeb50bbd58703f6861efd846bf7a9110b92c73432bf3707f5056dcd4`
across 18,054 files. These receipts precede this validation-summary update;
that fingerprint is their historical source identity, not the identity of the
subsequently edited documentation. It includes ignored vendor source files and
must not be equated with a clean snapshot's smaller inventory. Production
sources, manifests, lockfile, dependency features, and tracked vendor sources
were unchanged in that original mock implementation.

The separate release attempts established the following coverage:

- Tool's complete check script and configured MCP strict-live case passed.
- Workspace Clippy for the non-Storage/non-App selection and all eight supported
  Storage Clippy lanes passed. App's library and new targets passed primary
  Clippy. The complete documentation/doctest gate passed for all five crates,
  including both supported alternative Storage documentation lanes.
- Provider strict-live stopped at missing `TEST_GEMINI_MODEL_FILE_SEARCH`.
  Agent strict-live stopped at missing `TEST_CODEX_REASONING_EFFORT`, after both
  13-case process-contract suites passed. App strict-live stopped because
  installed OpenCode 1.19.4 differs from the required 1.19.3. The full workspace
  wrapper passed its compiler/docs gates and stopped at the same Agent setting.
  These are prerequisite failures, not additional confirmed production defects.
- Storage's six individual check lanes (no optional features, Redb, Postgres,
  SurrealDB, SQLite, and Qdrant) passed their test, doctest, Clippy, and documentation
  steps. The first combined lane was still compiling near the runner's
  7,200-second stage limit. The verifier received SIGTERM through its own cleanup
  handler before that limit, reported INCOMPLETE, and settled both owned process
  groups. Its exit 143 is recorded as a failed command in the aggregate report;
  its execution coverage is incomplete. Combined runtime lanes, the remaining
  legal/illegal feature matrix, and Storage strict-live service execution were
  not completed. The separate combined Clippy checks did complete successfully.

Prerequisite details are retained in `release-prerequisites.json`; bounded
interruption and confirmed process settlement are retained in
`release-timeout-ownership.json`, both beneath `target/mock-contracts/`.
No failed case was excluded or converted into an expected failure.

The separate locked clean-checkout compilation passed on the isolated snapshot
before the environment follow-up below. Its record is
`target/mock-contracts/clean-checkout.json`; it identifies the isolated snapshot
commit, its own receipt, and fresh target directory. The exported
`target/mock-contracts/clean-checkout.patch` preserves that snapshot's changes.
This proof checks workspace default features and all Cargo targets; it does not
execute live tests or certify native behavior. Read its compilation status from
its own receipt. The original working checkout is not staged or committed by
snapshot preparation.

### Environment and runtime follow-up

At this follow-up, the local Agent env explicitly exported `TEST_CODEX_REASONING_EFFORT="high"`
for its existing `gpt-5.6-luna` model. The tracked example already exported
`high`; its setup guidance now explains existing-env updates and the absence of
an implicit default. Credentials, model selections, and unrelated env settings
were preserved. Valid process fixtures verify template exports and exact
forwarding into precheck and Cargo. Deliberate missing-setting and incompatible
runtime-version negatives remain enabled.

All 15 precheck contracts and all 14 verifier-process contracts passed; logs are
`config-runtime-precheck.log` and `config-effort-question-grade.log` beneath
`target/mock-contracts/`. The Agent and workspace gates resumed after the effort
correction; both advanced to missing `TEST_CUSTOM_OPENAI_MODEL`. Their receipt is
`20261025T212252Z-d1245553/report.json`. The optional Rust environment-only build
was interrupted after the real precheck established the missing configuration;
its exit 143 is an incomplete compilation, not a test failure or pass. Its owned
process group settled, as recorded in
`config-effort-optional-build-interruption.json`.

The subsequent runtime alignment installed exact Codex `0.163.0` and OpenCode
`1.19.3` packages and corrected the Agent/App local PATH entries and tracked
templates. At that point, default CLI resolution and all five sourced crate environments
reported those versions (`runtime-version-alignment.json`). Older OpenCode
Homebrew binaries remain installed but unlinked; the default commands use the
exact npm packages. The old Codex CLI cask was removed without deleting its
configuration or credentials. Version-specific positive fixtures and verified
protocol comments were refreshed; rejection fixtures remain unchanged.

Actual owned test servers confirmed Codex `0.163.0` and both OpenCode `1.19.3`
identities. Codex's native model inventory advertised `high` for the exact
configured `gpt-5.6-luna` model. App's precheck passed with the selected server;
Agent's precheck at that point still required `TEST_CUSTOM_OPENAI_MODEL`,
`TEST_CUSTOM_ANTHROPIC_MODEL`, and `TEST_OPENCODE_CLOUD_MODEL`. All three smoke-test
servers were terminated and joined (`runtime-native-qualification.json`).
These configuration corrections are separate from `MOCK-AGENT-001`; its enabled
regression and production sources were unchanged during that configuration follow-up.

The affected Rust targets also passed: 334 `opencode_event_ownership` cases and
66 `strict_live_assertions` cases, with none ignored or filtered
(`runtime-fixture-tests.log`). The resumed App gate passed its prerequisite
check and Clippy, then was stopped at the user's request while building its
integration executables. Its result is INCOMPLETE, not a test pass or failure;
`20261025T215222Z-f911fe2b/report.json` records successful owned-server cleanup.
The Cargo and server process groups were both confirmed settled.

The prepared supplemental clean snapshot record is
`target/mock-contracts/clean-checkout-final.json`. Its incremental check was
prepared but not run before the user's stop request. The earlier fresh
compilation passed before these configuration changes; it does not constitute
final-source or live proof. Its prepared snapshot patch is retained separately.

## Cancellation and authentication follow-ups — 2026-10-25

The separately authorized cancellation fix resolved M001 while retaining its
required regression. The bug ledger records its exact supplied diff, red/green
proof, and independent fixture review. Focused validation passed 49 Custom mock
cases, 57 runtime cases, and Agent all-target/all-feature Clippy in
`target/mock-contracts/20261025T220805Z-76e4858e/report.json`. The 22 public API
cases and targeted Clippy passed in `20261025T223105Z-45905da5`; that aggregate
receipt still failed because other stages failed. Agent's 15 source contracts
passed after verified archive preparation in `20261025T223527Z-060e8502`.

The latest primary CI receipt, `20261025T223224Z-332b9212/report.json`, records
**132/133 Rust cases passing and an overall FAIL** at App workflow shutdown;
all 18 runner contracts passed. The separate credential-free, network-denied
receipt `20261025T223300Z-d464579e/report.json` passed all 133 Rust cases. These
receipts are beneath `target/mock-contracts/`. The restricted pass does not
replace the failing primary result. Supplemental probes initially shared build
outputs with the root checkout; those attempts are invalid root-source evidence,
as recorded by `target/mock-agent-001-resolution/artifact-isolation-correction.json`.

The three Agent API model roles were subsequently configured as
`TEST_CUSTOM_OPENAI_MODEL=gpt-6-luna`,
`TEST_CUSTOM_ANTHROPIC_MODEL=claude-sonnet-5-5`, and bare
`TEST_OPENCODE_CLOUD_MODEL=gpt-6-luna`. Configured OpenAI and Anthropic generation
probes succeeded. Native Codex authentication was separately refreshed using the
existing login in the isolated test home; the existing Codex model and explicit
`high` reasoning effort were preserved. No credentials were committed.

The full precheck and both process-contract suites passed. At the user's request,
the full Agent verifier was interrupted during compilation; its receipt is
`target/mock-contracts/20261025T224330Z-2cc654c6/report.json` (INCOMPLETE).
The focused native Codex generation/effort/reset/reopen case then passed:
1 passed, 28 intentionally filtered, in
`target/mock-contracts/20261025T225023Z-69925787/report.json`. Owned services and
process groups settled. Safe configuration/authentication results are recorded
in `target/mock-agent-001-resolution/auth-resolution-result.json` and
`model-access.json` in the same directory.

M002 remains a confirmed pre-existing defect with a supplemental local probe,
not a committed primary regression. App's shutdown conflict remains unclassified.
Full strict-live coverage, remaining Storage runtime/feature-matrix coverage,
the workspace wrapper, and final locked clean-checkout proof were not completed.
The last recorded Provider attempt stopped at `TEST_GEMINI_MODEL_FILE_SEARCH`;
that prerequisite was not revalidated by the Agent follow-up. No later scoped
pass closes those gaps.
