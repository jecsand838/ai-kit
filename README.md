# ai-kit

<p align="center">
  <img src="docs/assets/ai-kit-logo.png" alt="ai-kit logo" width="220">
</p>

Modular Rust libraries for model providers, tools, storage, agents, and
recoverable workflows.

Combining these pieces means deciding who owns each resource, how work shuts
down, and what state remains after an interruption. ai-kit provides those
integration and lifecycle boundaries while keeping configuration and composition
in your application. Use a provider or tool crate on its own, or compose agents
and workflows over storage you supply. Install only the crates you need;
`ai-kit` is the workspace that develops them together.

| Crate and usage guide | Use it for | Package | API docs |
| --- | --- | --- | --- |
| [providers-kit](providers-kit/README.md) | Model generation, streaming, embeddings, batches, and sessions across OpenAI, Anthropic, Gemini, and Ollama | [crates.io](https://crates.io/crates/providers-kit) | [rustdoc](https://docs.rs/providers-kit/latest/providers_kit/) |
| [storage-kit](storage-kit/README.md) | Data/search through `Database` and transactional control-plane records through `StateStore` | [crates.io](https://crates.io/crates/storage-kit) | [rustdoc](https://docs.rs/storage-kit/latest/storage_kit/) |
| [tools-kit](tools-kit/README.md) | Executable Local/MCP tools and optional MCP serving | [crates.io](https://crates.io/crates/tools-kit) | [rustdoc](https://docs.rs/tools-kit/latest/tools_kit/) |
| [agents-kit](agents-kit/README.md) | Codex, OpenCode, and provider-backed Custom agents with workspace and conversation lifecycles | [crates.io](https://crates.io/crates/agents-kit) | [rustdoc](https://docs.rs/agents-kit/latest/agents_kit/) |
| [app-kit](app-kit/README.md) | Durable workflows, checkpoints, and recovery over caller-supplied storage and agents | [crates.io](https://crates.io/crates/app-kit) | [rustdoc](https://docs.rs/app-kit/latest/app_kit/) |

## Try a workflow locally

The [history and recovery example](app-kit/examples/history_and_recovery.rs)
runs without an LLM, external service, or credentials. It restores a checkpoint
into a new run and verifies the saved result and its source lineage.

Before running it, install Git, Rust through rustup, and the native build tools:
a C/C++ compiler, libclang, CMake, Protocol Buffers, pkg-config, and TLS
development libraries. The [build prerequisites](CONTRIBUTING.md#get-started)
include Ubuntu installation commands and contributor tooling. The repository
selects Rust **1.99.0** through its [pinned toolchain](rust-toolchain.toml).
Storage and its consumers compile native dependencies even with
`--no-default-features`; allow time and disk space for the first build.

```bash
git clone https://github.com/jecsand838/ai-kit.git
cd ai-kit
cargo run --locked -p app-kit --example history_and_recovery
```

After compilation, the example prints the following (`<run-id>` is generated):

```text
Source count: 2; copied count: 40; completed fork count: 41
Reopened fork <run-id> at count 41; source lineage remains readable
```

Two increments produce `2`. Restoring the checkpoint after the first increment
with an override of `40` and running the remaining increment produces `41`.
The example deletes the source run, then reopens the fork in a new App generation.
This default mode uses memory within one process.

To keep the result across process exit, run these two commands from the same
checkout:

```bash
demo_root="$(mktemp -d)"
cargo run --locked -p app-kit --example history_and_recovery -- create "$demo_root/history"
cargo run --locked -p app-kit --example history_and_recovery -- reopen "$demo_root/history"
```

The first process saves the completed fork in embedded Lance storage and exits
after cleanup. The second reopens that same result at `41` without executing
the workflow again. Follow the [App quickstart](app-kit/docs/QUICKSTART.md) for
the output, stored identities, repeat runs, and cleanup.

Reopening a completed result is distinct from deciding whether interrupted
external work succeeded. An uncertain effect needs an explicit caller decision;
see [persistence and recovery](app-kit/docs/PERSISTENCE_AND_RECOVERY.md#effect-uncertainty).

## Add an individual crate

The published crates require Rust **1.99 or newer**. From your own Cargo project,
add the crate you need, for example:

```bash
cargo add tools-kit@=0.0.2
```

The [Local tool example](https://docs.rs/tools-kit/0.0.2/tools_kit/) registers and
invokes a tool without provider credentials or a service. It imports the backend
traits for their methods and awaits toolset shutdown. The example also uses
Tokio and serde_json; add those dependencies when copying it into your application.

## Choose features and backends

- Provider operations depend on the selected provider and model. Supplied
  Sessions currently support OpenAI only; see the
  [Provider guide](providers-kit/README.md) for support boundaries.
- MCP serving requires tools-kit's optional `mcp-server` feature. The
  [Tool guide](tools-kit/README.md) explains client and server ownership.
- Storage includes LanceDB and embedded Lance in its base build. Additional
  backends are opt-in; use the [Storage feature guide](storage-kit/README.md#features)
  and its [testing guide](storage-kit/docs/testing.md) for supported selections.

## Design and composition

Each crate owns a distinct part of an application. Public backend traits let
callers supply concrete implementations and test-local mocks while preserving
associated child types. Applications receive ready storage and agents; they do
not reconstruct lower-crate credentials or configuration.

See the [workspace architecture](ARCHITECTURE.md) for ownership and public
boundaries, and [direct-trait mock testing](docs/mock-testing.md) for executable
composition examples. Detailed API contracts and workflows live in the linked
crate guides and rustdoc.

## Versioning and compatibility

Backwards compatibility is **not guaranteed until `1.0.0`**. Public APIs,
behavior, and supported backends may change between releases. Pin exact versions
when reproducibility matters, and review the
[release notes](https://github.com/jecsand838/ai-kit/releases) before upgrading.
Repository `main` may contain changes that are not yet published; the package
links above identify available versions and their released API documentation.

All five crates are published at `0.0.2`; see the
[v0.0.2 release](https://github.com/jecsand838/ai-kit/releases/tag/v0.0.2) for its
source revision and recorded qualification scope. The installation example
above pins that release. The repository examples follow `main`, including
changes made after publication. Maintainers publish selected revisions through
the manual [release process](docs/RELEASING.md) after normal qualification;
preparing a release does not publish packages or establish qualification.

## Contributing and testing

Start with [CONTRIBUTING.md](CONTRIBUTING.md) for setup, local test environments,
and issue/PR expectations. Read the [operating contract](AGENTS.md) and the
owning crate's rules before changing code; the [skill catalog](SKILLS.md) lists
useful development workflows.

Run the primary deterministic checks from the workspace root:

```bash
bash scripts/check_ci.sh
```

These checks exercise direct-trait mocks and Custom Agent/App composition
without provider credentials or running services. The
[mock-testing guide](docs/mock-testing.md) describes their scope. Native
integration, strict-live tests, and the separate clean-checkout proof remain
part of [release qualification](docs/release-qualification.md).

For dependency risk and accepted advisories, see the
[dependency security policy](docs/security-dependencies.md). See the repository
[license](LICENSE) and each package's license metadata on crates.io.
