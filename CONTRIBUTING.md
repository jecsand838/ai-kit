# Contributing to ai-kit

Start with an issue describing the problem and the outcome you want. Use the
bug, feature, docs, or vendor-update form, and link that issue in your pull
request. You do not need provider keys or a paid test run to report a bug.

This guide covers contributor setup and the contribution process. The
[root operating contract](AGENTS.md) and each crate's `AGENTS.md` own mandatory
engineering rules and exact validation gates. Read those before changing code;
the [workspace architecture](ARCHITECTURE.md) explains ownership and boundaries.
Provider contributors should also read the
[Provider contribution guide](providers-kit/CONTRIBUTING.md).

## Get started

You need Git, Bash, Python 3.11 or newer,
and Rust installed through rustup. Use the toolchain and components selected by
[rust-toolchain.toml](rust-toolchain.toml), rather than overriding them with a
different Rust release.

```bash
git clone https://github.com/jecsand838/ai-kit.git
cd ai-kit
git switch main
git pull --ff-only origin main
git switch -c describe-your-change
rustup show
python3 --version
```

For an existing checkout, preserve your current work before switching branches.
Run the remaining commands from the repository root.

The repository is public. Contributors without write access should fork it,
push their branch to the fork, and open a pull request against this repository's
`main` branch. Kit versions begin at `0.0.1`; backwards compatibility is not
guaranteed until `1.0.0`. Document breaking changes and migration steps in the
owning guides and release notes.

Native dependencies compile even in the credential-free mock lane. On Ubuntu,
the [CI workflow](.github/workflows/ci.yml) installs:

```bash
sudo apt-get update
sudo apt-get install --yes --no-install-recommends \
  clang libclang-dev cmake protobuf-compiler libprotobuf-dev \
  python3 pkg-config libssl-dev
```

On other development platforms, provide equivalent compiler, libclang, CMake,
Protocol Buffers, pkg-config, and TLS development tooling. Allow disk space and
time for the native dependency build. Node/npm, Ollama, Codex, OpenCode, and
Docker are prerequisites for particular live suites, not for running mock tests;
their owning guides specify which are needed and which versions to install.

## Develop and test a change

Read the owning crate's rules and downstream workflow:

| Crate    | Rules and exact gates                | Usage                             |
|----------|--------------------------------------|-----------------------------------|
| Provider | [AGENTS.md](providers-kit/AGENTS.md) | [README](providers-kit/README.md) |
| Storage  | [AGENTS.md](storage-kit/AGENTS.md)   | [README](storage-kit/README.md)   |
| Tool     | [AGENTS.md](tools-kit/AGENTS.md)     | [README](tools-kit/README.md)     |
| Agent    | [AGENTS.md](agents-kit/AGENTS.md)    | [README](agents-kit/README.md)    |
| App      | [AGENTS.md](app-kit/AGENTS.md)       | [README](app-kit/README.md)       |

Keep changes focused on the linked issue. Before changing production behavior,
add or strengthen a meaningful test and record the required failing result.
Then implement the fix and record the passing result. Test observable public
behavior rather than private helper names or implementation choreography.
Executable Rust tests belong in the owning crate's `tests/` tree.

Run one Cargo command at a time. Start with the primary deterministic lane:

```bash
bash scripts/check_ci.sh
```

For focused iteration, select a crate explicitly, for example:

```bash
bash scripts/check_mock_contracts.sh --crate agents-kit
```

These commands need no provider credentials or running services. They compile
native dependencies and test direct public-trait implementations plus Custom
Agent/App composition. A scoped pass proves only that scope. See the
[mock-testing contract](docs/mock-testing.md) for inventories, logs, feature
selection, and runtime-independence qualification.

Update the owning docs when behavior, public APIs, lifecycle promises, or
validation commands change. Put public examples in rustdoc and documented
public modules. Keep architecture rules and exact signatures in their existing
owners, and leave `tmp/**` historical artifacts untouched. Arrange independent
review of the actual diff; agent-assisted work requires the independent
subagent review specified by `AGENTS.md`.

## Configure local test environments

Each crate has a tracked `strict-tests.env.example`. Copy only the files needed
for the suites you will run, and never overwrite existing local configuration.
For example, this Bash snippet creates a private Provider env file only when
one does not already exist:

```bash
test_crate=providers-kit
if [ ! -e "$test_crate/strict-tests.env" ]; then
  (umask 077; cp "$test_crate/strict-tests.env.example" "$test_crate/strict-tests.env")
fi
```

Edit the copy before sourcing it. Replace blank/example credentials, absolute
paths, resource IDs, and fixture values as directed by its comments. Use your
own authorized credentials and disposable test data. Preserve the exact runtime
versions, model roles, and isolation settings in the owning examples and specs.
After an upstream update, reconcile your local copy with the tracked example.

| Crate    | Env example                                       | Real prerequisites and instructions                                                                                                                                                                                          |
|----------|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Provider | [Example](providers-kit/strict-tests.env.example) | OpenAI, Anthropic, and Gemini credentials; pre-provisioned File Search stores; local Ollama with the exact model tags. [Strict guide](providers-kit/docs/STRICT_TESTING.md).                                                 |
| Storage  | [Example](storage-kit/strict-tests.env.example)   | Default embedded Lance needs no env file, credentials, or service. Selected alternatives need the documented disposable paths/ports and, for service backends, Docker Compose. [Testing guide](storage-kit/docs/testing.md). |
| Tool     | [Example](tools-kit/strict-tests.env.example)     | An independently maintained real MCP server, its launcher, and the exact marker fixture/input/result. Repository mocks do not count. [Strict guide](tools-kit/docs/STRICT_TESTING.md).                                       |
| Agent    | [Example](agents-kit/strict-tests.env.example)    | Cloud credentials, isolated Codex authentication, selected Codex/OpenCode releases and servers, and Ollama models with the required context and GPU residency. [Strict guide](agents-kit/docs/STRICT_TESTING.md).            |
| App      | [Example](app-kit/strict-tests.env.example)       | Selected OpenCode release/server and isolated configuration, real Ollama with the required model/context, and embedded Lance persistence. [Strict guide](app-kit/docs/STRICT_TESTING.md).                                    |

The examples contain setup and service-start instructions; provision those
prerequisites before running the verifiers. Cloud suites can incur charges,
and full Provider batch completion cases can wait up to 24 hours per applicable
job. Missing credentials, models, or services are blockers, not passing skips.

Populated `strict-tests.env` files are ignored by Git. Do not force-add them,
publish authentication files, or paste secrets into issues, PRs, or logs.
Sanitize evidence before sharing it; do not change test outputs or assertions
to make a failed result appear successful.

## Run relevant strict-live checks

The env files share variable names. Run each suite in an isolated shell rather
than sourcing multiple files into your login shell. From the repository root,
after following the relevant setup instructions:

```bash
bash -c 'source providers-kit/strict-tests.env && bash providers-kit/scripts/verify_strict_live.sh'
bash -c 'source tools-kit/strict-tests.env && bash tools-kit/scripts/verify_strict_live.sh'
bash -c 'source agents-kit/strict-tests.env && bash agents-kit/scripts/precheck_strict_live.sh && bash agents-kit/scripts/verify_strict_live.sh'
bash app-kit/scripts/verify_strict_live.sh
bash storage-kit/scripts/verify_strict_live.sh
```

Select the commands relevant to your change; these are separate invocations,
not a replacement for the workspace wrapper. App's verifier loads its own env
file. Its example derives paths from the current directory, so keep the
repository root as your working directory. Storage's default verifies real
embedded Lance Database and StateStore behavior.

For a Storage alternative, configure its env first and select the backend, for
example:

```bash
bash -c 'source storage-kit/strict-tests.env && bash storage-kit/scripts/verify_strict_live.sh --backend postgres'
```

Storage also supports `--changed-since BASE` and `--all-backends`; follow its
owning gate rules to cover every affected alternative after direct-backend or
shared-abstraction changes. Use the explicit supported Storage lanes rather than `--all-features`. Downstream Agent/App persistence fixtures use Lance;
alternative backend proof belongs in Storage.

## Submit a pull request with evidence

Use the [PR template](.github/pull_request_template.md). Link an issue and explain
the resulting behavior and acceptance criteria. `Closes #123` links and closes
an issue when the PR merges into `main`; use an issue reference and explain
remaining work when the PR only addresses part of it. Linked issues and the
acceptance checklist are reviewer-enforced requirements. The template itself
does not block submission or enforce a merge check.

Record exact commands, crate/backend/features, tested revision, results, and
sanitized receipt/log locations. Include failures, exclusions, and blockers.
Certify that all relevant strict-live checks passed locally against real
services using your own authorized keys where required. If a required check
failed or could not run, leave its certification unchecked and describe why.

For changes limited to prose or contribution templates, explain why behavior
tests and strict-live suites are not applicable instead of claiming they ran.
This exception does not waive applicable documentation/link checks. A change
to runtime behavior, test expectations, or executable validation logic is not
prose-only. New documentation should be inventoried in the
[documentation ledger](docs/documentation-ledger.toml). Useful checks include:

```bash
python3 -m unittest scripts.tests.test_documentation_contract
git diff --check
```

The documentation contract checks inventoried active Markdown file links;
template-classified files, YAML form schemas, and anchors also need review.

GitHub's Formatting, Tests, Linting, and Security audit checks do not execute
strict-live suites or the full native/backend matrices. Local `check_ci.sh`
does not run the separate security audit. Neither CI nor a docs-only exception
establishes workspace release qualification.
The [dependency security policy](docs/security-dependencies.md) documents the
explicit temporary advisory exceptions. An audit with accepted findings is
`PASS_WITH_EXCEPTIONS`; keep its raw report and remove each exception ASAP when
compatible upstream fixes become available.

Full workspace qualification uses the exact crate gates and
`bash scripts/verify_workspace_strict.sh`, plus the separate locked
clean-checkout proof. The wrapper does not source Storage's optional env or
replace affected-alternative verification. Follow the
[release qualification guide](docs/release-qualification.md) and root operating
contract. The default is full execution. Explicit skip modes provide qualified
coverage only: `--skip-long-provider-batches` excludes exactly the seven named
Provider cases, while `--skip-strict-live` defers all five live suites. Neither
flag establishes full coverage. Receipt reuse requires an explicitly requested
continuation with complete verified provenance. Report the owning guides'
required raw-copy grades without widening their narrow exceptions.

## Use the repository skills

The [skill catalog](SKILLS.md) is the index. Ask your coding agent to use the
relevant skill by name or point it to the linked `SKILL.md`; read its
prerequisites before starting. Select the narrowest applicable workflow.
Linking a skill here does not activate a workspace campaign.

| Work | Skills |
| --- | --- |
| Everyday Rust work | [Core](.agents/skills/use-rust-best-practices-core/SKILL.md); add [architecture](.agents/skills/use-rust-best-practices-architecture/SKILL.md), [abstractions](.agents/skills/use-rust-best-practices-abstractions/SKILL.md), [async](.agents/skills/use-rust-best-practices-async/SKILL.md), [testing](.agents/skills/use-rust-best-practices-testing/SKILL.md), [networking](.agents/skills/use-rust-best-practices-networking/SKILL.md), or [dependencies/platforms](.agents/skills/use-rust-best-practices-dependencies-platforms/SKILL.md) as needed. |
| Domain structs, enums, and methods | [Rust code structuring](.agents/skills/rust-code-structuring-best-practices/SKILL.md). |
| Research upstream behavior | [Provider](.agents/skills/providers-kit-research/SKILL.md), [Storage](.agents/skills/storage-kit-research/SKILL.md), [Tool](.agents/skills/tools-kit-research/SKILL.md), [Agent](.agents/skills/agents-kit-research/SKILL.md), and [App](.agents/skills/app-kit-research/SKILL.md). Research sourced behavior before changing adapters. |
| Apply crate contracts | [Provider](.agents/skills/providers-kit-change-contract/SKILL.md), [Storage](.agents/skills/storage-kit-change-contract/SKILL.md), [Tool](.agents/skills/tools-kit-change-contract/SKILL.md), and [Agent](.agents/skills/agents-kit-change-contract/SKILL.md). App uses its `AGENTS.md` and specs. Use the [public-surface contract](.agents/skills/ai-kit-public-surface-contract/SKILL.md) for public-boundary changes. |
| Refresh vendors | [vendor-refresh-campaign](.agents/skills/vendor-refresh-campaign/SKILL.md) is the workspace/crate entrypoint. [vendor-release-update](.agents/skills/vendor-release-update/SKILL.md) handles only one updater-generated dossier. Start discovery with `python3 scripts/vendor_update.py check --json`; follow the selected workflow for updates and validation. |
| Refresh agentic documentation | [refresh-workspace-agentic-documentation](.agents/skills/refresh-workspace-agentic-documentation/SKILL.md) reconciles repository guidance, skills, ownership, and the documentation ledger. |
| Refresh public Rust docs | [refresh-workspace-rustdocs](.agents/skills/refresh-workspace-rustdocs/SKILL.md) for a workspace campaign; [rust-public-api-docs](.agents/skills/rust-public-api-docs/SKILL.md) for a focused item or crate-local change. |
| Select and audit validation | [ai-kit-validation-matrix](.agents/skills/ai-kit-validation-matrix/SKILL.md) and [strict-live-audit](.agents/skills/strict-live-audit/SKILL.md). |
