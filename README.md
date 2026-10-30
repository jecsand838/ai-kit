# ai-kit

<p align="center">
  <img src="docs/assets/ai-kit-logo.png" alt="ai-kit logo" width="220">
</p>

Rust libraries for model providers, data and state storage, executable tools,
agent runtimes, and durable workflows. Install the crates you need individually;
`ai-kit` is the workspace that develops them together.

| Crate and usage guide | Use it for | Package | API docs |
| --- | --- | --- | --- |
| [providers-kit](providers-kit/README.md) | Model generation, streaming, embeddings, batches, and sessions across OpenAI, Anthropic, Gemini, and Ollama | [crates.io](https://crates.io/crates/providers-kit) | [rustdoc](https://docs.rs/providers-kit/latest/providers_kit/) |
| [storage-kit](storage-kit/README.md) | Data/search through `Database` and transactional control-plane records through `StateStore` | [crates.io](https://crates.io/crates/storage-kit) | [rustdoc](https://docs.rs/storage-kit/latest/storage_kit/) |
| [tools-kit](tools-kit/README.md) | Executable Local/MCP tools and optional MCP serving | [crates.io](https://crates.io/crates/tools-kit) | [rustdoc](https://docs.rs/tools-kit/latest/tools_kit/) |
| [agents-kit](agents-kit/README.md) | Codex, OpenCode, and provider-backed Custom agents with workspace and conversation lifecycles | [crates.io](https://crates.io/crates/agents-kit) | [rustdoc](https://docs.rs/agents-kit/latest/agents_kit/) |
| [app-kit](app-kit/README.md) | Durable workflows, checkpoints, and recovery over caller-supplied storage and agents | [crates.io](https://crates.io/crates/app-kit) | [rustdoc](https://docs.rs/app-kit/latest/app_kit/) |

## Get started

The crates require Rust **1.99 or newer**. Repository development uses the pinned
[rustup toolchain](rust-toolchain.toml).

From your own Cargo project, add the crate you need. For example, to use the
published `0.0.1` tools package:

```bash
cargo add tools-kit@=0.0.1
```

Start with the [Local tool example](https://docs.rs/tools-kit/latest/tools_kit/),
which registers and invokes a tool without provider credentials or a service.
It imports the backend traits for their methods and awaits toolset shutdown.
The example also uses Tokio and serde_json; add those dependencies when copying
it into your application.

For a complete workflow example, follow the
[App quickstart](app-kit/docs/QUICKSTART.md). From a repository checkout, run:

```bash
cargo run --locked -p app-kit --example history_and_recovery
```

This local example runs a workflow, restores a result, and reopens it in a new
App generation without an LLM or credentials. It uses an in-memory store;
persistence across process exit requires a persistent store.

Storage and its consumers compile native dependencies, including when using
`--no-default-features`. Follow the [build prerequisites](CONTRIBUTING.md#get-started)
before building them.

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

Maintainers publish selected revisions of `main` through the manual
[release process](docs/RELEASING.md).

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
