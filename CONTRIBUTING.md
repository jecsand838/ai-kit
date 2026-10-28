# Contributing to ai-kit

Start with an issue describing the problem and the outcome you want. Use the
bug, feature, docs, or vendor-update form, and link that issue in your pull
request. You do not need provider keys or a paid test run to report a bug.

This guide covers contributor setup and the contribution process. The
[root operating contract](AGENTS.md) and each crate's `AGENTS.md` own mandatory
engineering rules and exact validation gates. Read those before changing code;
the [workspace architecture](ARCHITECTURE.md) explains ownership and boundaries.
Provider contributors should also read the
[Provider contribution guide](provider-sdk/CONTRIBUTING.md).

## Get started

You need access to this private repository, Git, Bash, Python 3.11 or newer,
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

| Crate | Rules and exact gates | Usage |
| --- | --- | --- |
| Provider | [AGENTS.md](provider-sdk/AGENTS.md) | [README](provider-sdk/README.md) |
| Storage | [AGENTS.md](storage-sdk/AGENTS.md) | [README](storage-sdk/README.md) |
| Tool | [AGENTS.md](tool-sdk/AGENTS.md) | [README](tool-sdk/README.md) |
| Agent | [AGENTS.md](agent-sdk/AGENTS.md) | [README](agent-sdk/README.md) |
| App | [AGENTS.md](app-sdk/AGENTS.md) | [README](app-sdk/README.md) |

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
bash scripts/check_mock_contracts.sh --crate agent-sdk
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
test_crate=provider-sdk
if [ ! -e "$test_crate/strict-tests.env" ]; then
  (umask 077; cp "$test_crate/strict-tests.env.example" "$test_crate/strict-tests.env")
fi
```

Edit the copy before sourcing it. Replace blank/example credentials, absolute
paths, resource IDs, and fixture values as directed by its comments. Use your
own authorized credentials and disposable test data. Preserve the exact runtime
versions, model roles, and isolation settings in the owning examples and specs.
After an upstream update, reconcile your local copy with the tracked example.

| Crate | Env example | Real prerequisites and instructions |
| --- | --- | --- |
| Provider | [Example](provider-sdk/strict-tests.env.example) | OpenAI, Anthropic, and Gemini credentials; pre-provisioned File Search stores; local Ollama with the exact model tags. [Strict guide](provider-sdk/docs/STRICT_TESTING.md). |
| Storage | [Example](storage-sdk/strict-tests.env.example) | Default embedded Lance needs no env file, credentials, or service. Selected alternatives need the documented disposable paths/ports and, for service backends, Docker Compose. [Testing guide](storage-sdk/docs/testing.md). |
| Tool | [Example](tool-sdk/strict-tests.env.example) | An independently maintained real MCP server, its launcher, and the exact marker fixture/input/result. Repository mocks do not count. [Strict guide](tool-sdk/docs/STRICT_TESTING.md). |
| Agent | [Example](agent-sdk/strict-tests.env.example) | Cloud credentials, isolated Codex authentication, selected Codex/OpenCode releases and servers, and Ollama models with the required context and GPU residency. [Strict guide](agent-sdk/docs/STRICT_TESTING.md). |
| App | [Example](app-sdk/strict-tests.env.example) | Selected OpenCode release/server and isolated configuration, real Ollama with the required model/context, and embedded Lance persistence. [Strict guide](app-sdk/docs/STRICT_TESTING.md). |

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
bash -c 'source provider-sdk/strict-tests.env && bash provider-sdk/scripts/verify_strict_live.sh'
bash -c 'source tool-sdk/strict-tests.env && bash tool-sdk/scripts/verify_strict_live.sh'
bash -c 'source agent-sdk/strict-tests.env && bash agent-sdk/scripts/precheck_strict_live.sh && bash agent-sdk/scripts/verify_strict_live.sh'
bash app-sdk/scripts/verify_strict_live.sh
bash storage-sdk/scripts/verify_strict_live.sh
```

Select the commands relevant to your change; these are separate invocations,
not a replacement for the workspace wrapper. App's verifier loads its own env
file. Its example derives paths from the current directory, so keep the
repository root as your working directory. Storage's default verifies real
embedded Lance Database and StateStore behavior.

For a Storage alternative, configure its env first and select the backend, for
example:

```bash
bash -c 'source storage-sdk/strict-tests.env && bash storage-sdk/scripts/verify_strict_live.sh --backend postgres'
```

Storage also supports `--changed-since BASE` and `--all-backends`; follow its
owning gate rules to cover every affected alternative after direct-backend or
shared-abstraction changes. Never use Storage `--all-features`: SQLite and
Qdrant cannot coexist. Downstream Agent/App persistence fixtures use Lance;
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
| Research upstream behavior | [Provider](.agents/skills/provider-sdk-research/SKILL.md), [Storage](.agents/skills/storage-sdk-research/SKILL.md), [Tool](.agents/skills/tool-sdk-research/SKILL.md), [Agent](.agents/skills/agent-sdk-research/SKILL.md), and [App](.agents/skills/app-sdk-research/SKILL.md). Research sourced behavior before changing adapters. |
| Apply crate contracts | [Provider](.agents/skills/provider-sdk-change-contract/SKILL.md), [Storage](.agents/skills/storage-sdk-change-contract/SKILL.md), [Tool](.agents/skills/tool-sdk-change-contract/SKILL.md), and [Agent](.agents/skills/agent-sdk-change-contract/SKILL.md). App uses its `AGENTS.md` and specs. Use the [public-surface contract](.agents/skills/ai-kit-public-surface-contract/SKILL.md) for public-boundary changes. |
| Refresh vendors | [vendor-refresh-campaign](.agents/skills/vendor-refresh-campaign/SKILL.md) is the workspace/crate entrypoint. [vendor-release-update](.agents/skills/vendor-release-update/SKILL.md) handles only one updater-generated dossier. Start discovery with `python3 scripts/vendor_update.py check --json`; follow the selected workflow for updates and validation. |
| Refresh agentic documentation | [refresh-workspace-agentic-documentation](.agents/skills/refresh-workspace-agentic-documentation/SKILL.md) reconciles repository guidance, skills, ownership, and the documentation ledger. |
| Refresh public Rust docs | [refresh-workspace-rustdocs](.agents/skills/refresh-workspace-rustdocs/SKILL.md) for a workspace campaign; [rust-public-api-docs](.agents/skills/rust-public-api-docs/SKILL.md) for a focused item or crate-local change. |
| Select and audit validation | [ai-kit-validation-matrix](.agents/skills/ai-kit-validation-matrix/SKILL.md) and [strict-live-audit](.agents/skills/strict-live-audit/SKILL.md). |
