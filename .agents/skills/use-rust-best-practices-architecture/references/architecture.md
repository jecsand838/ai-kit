# Rust architecture best practices

Use this reference when designing or restructuring package layout, workspace structure, module organization, target placement, or test placement.

## 1. Prefer the standard Cargo target layout

Use the layout Cargo already understands.

```text
my-package/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── main.rs
│   ├── bin/
│   │   ├── migrate.rs
│   │   └── reindex.rs
│   └── users/
│       ├── commands.rs
│       └── query.rs
├── tests/
│   ├── api_smoke.rs
│   └── common/
│       └── mod.rs
├── examples/
│   └── basic_usage.rs
└── benches/
    └── parser.rs
```

Use each area for its intended role:

- `src/lib.rs` — reusable library entry point
- `src/main.rs` — the main binary for the package
- `src/bin/*.rs` — extra binaries that still belong to the same package
- `tests/*.rs` — integration and binary smoke tests
- `examples/*.rs` — runnable examples
- `benches/*.rs` — benchmark targets

## 2. Keep `main.rs` thin

A binary should assemble configuration, dependencies, and process-level concerns. Business logic should live in library code.

### Good: thin binary over reusable library

`src/main.rs`

```rust
use clap::Parser;
use my_app::{config::Config, run};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    config: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::from_path(&cli.config)?;
    run(config).await
}
```

`src/lib.rs`

```rust
pub mod config;
mod http;
mod service;

use config::Config;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let app = service::App::new(config)?;
    http::serve(app).await
}
```

### Bad: fat binary

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parses config, validates business rules, talks to DB,
    // calls APIs, performs retries, and contains all handlers.
    todo!()
}
```

Use a fat `main.rs` only for tiny one-file throwaway programs.

## 3. Follow the repository's module policy first

Repository policy wins. Some repos prefer modern `foo.rs` plus `foo/` layouts;
others intentionally standardize on `mod.rs` for every directory-backed module.
Apply the owning repo's rule before applying generic taste.

When a repo does not already define a module policy, modern `foo.rs` plus
`foo/` layouts are a good default:

Use this structure by default:

```text
src/
├── lib.rs
├── users.rs
└── users/
    ├── commands.rs
    └── query.rs
```

`src/lib.rs`

```rust
pub mod users;
```

`src/users.rs`

```rust
pub mod commands;
pub mod query;

pub use commands::CreateUser;
pub use query::UserSummary;
```

This is often clearer than spreading every nested module through `mod.rs`
entrypoints when the repository has not standardized on `mod.rs`.

### When `mod.rs` is still acceptable

- `tests/common/mod.rs` for shared integration-test helpers
- repos that intentionally standardize on `mod.rs` for directory-backed modules
- rare cases where a directory entrypoint genuinely improves readability

Do not mass-convert a large codebase just for style unless the task already includes that cleanup.

## 4. Keep enum-dispatch contracts provider-agnostic

When a module owns a shared enum-dispatch trait contract, keep that module to
provider-agnostic traits, shared handles, and default helpers. Concrete
backend, provider, model, database, runtime, or source impl blocks belong under
the owning implementation subtree.

Good:

```text
src/
├── backends/
│   ├── manager.rs          # trait AgentBackendManager
│   ├── codex/manager.rs    # impl AgentBackendManager for CodexManager
│   └── custom/manager.rs   # impl AgentBackendManager for CustomAgentManager
└── agent.rs                # enum dispatcher match arms
```

Bad:

```text
src/
└── backends/
    └── manager.rs          # trait plus CodexManager and CustomAgentManager impls
```

This keeps module ownership aligned with the concrete implementation while the
root dispatcher remains portable. The dispatcher module should keep explicit,
exhaustive `match` arms over the closed enum variants; wildcard or catch-all
arms hide missing implementation-family work and belong outside this pattern.
Private dispatcher and config-selection enums are routing mechanics only. Do
not turn them into shared backend/provider/source identity, provenance,
capability, diagnostic-label, or error-label helpers; those facts belong in the
owning implementation subtree or in a documented public data/config type with a
narrow role.

## 5. Organize by domain and behavior

Prefer:

```text
src/
├── billing.rs
├── billing/
│   ├── commands.rs
│   ├── invoice.rs
│   └── repository.rs
└── users.rs
```

Avoid:

```text
src/
├── controllers/
├── managers/
├── helpers/
├── types/
└── utils/
```

A behavioral/domain layout usually produces clearer ownership boundaries and less cross-module coupling.

## 6. Use `src/bin/` before adding a new package

### Use `src/bin/` when

- the extra binary shares the same core dependencies and domain logic
- it is an operational helper such as `migrate`, `seed`, `backfill`, or `reindex`
- you do not need a separate release cadence or deployment unit

Example:

```text
src/
├── lib.rs
├── main.rs
└── bin/
    ├── migrate.rs
    └── backfill.rs
```

### Add a new package when

- the new binary needs a different dependency stack or runtime model
- it is deployed independently
- it should compile without pulling in the main app’s dependency graph
- it represents a separate public or internal API boundary
- it needs its own versioning or release cadence

## 7. Use workspaces for real package boundaries

A healthy workspace often looks like this:

```text
my-workspace/
├── Cargo.toml
├── crates/
│   ├── api/
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs,main.rs}
│   ├── domain/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── infra/
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── worker/
│       ├── Cargo.toml
│       └── src/main.rs
└── tests/
    └── e2e/
        ├── Cargo.toml
        └── src/lib.rs
```

### Workspace root example

```toml
[workspace]
members = ["crates/*", "tests/e2e"]
default-members = ["crates/api", "crates/domain", "crates/worker"]
resolver = "3"

[workspace.package]
edition = "2024"
license = "MIT OR Apache-2.0"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
thiserror = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
tracing = "0.1"

[workspace.lints.rust]
unsafe_op_in_unsafe_fn = "deny"

[workspace.lints.clippy]
dbg_macro = "deny"
todo = "deny"
unwrap_used = "deny"
```

### Member example

```toml
[package]
name = "domain"
version = "0.1.0"
edition.workspace = true

[lints]
workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true
```

### Why `default-members` matters

Use `default-members` when the workspace contains expensive or optional crates, such as:

- end-to-end test harnesses
- migration helpers
- experimental binaries
- benchmarking-only crates

This keeps common root commands fast and predictable.

## 8. Decide between extending a crate and creating a new one

### Extend an existing crate when

- the new code belongs to the same domain
- it shares the same runtime assumptions
- it needs the same dependency stack
- it would mostly re-export types from the old crate anyway
- splitting would create artificial boundaries and extra boilerplate

### Create a new crate when at least one of these is true

- **dependency boundary**: heavy or optional dependencies should not leak into other packages
- **ownership boundary**: the code has a clearly distinct lifecycle or responsibility
- **deployment boundary**: it runs as a different process or artifact
- **reuse boundary**: multiple packages consume it independently
- **release boundary**: it has meaningfully different change cadence or versioning pressure

If none of those are true, split cautiously. Many Rust workspaces become worse when every concept gets its own crate.

## 9. Place tests at the narrowest useful level

### Put tests here

- **unit tests**: next to the code in the same file
- **integration tests**: `tests/*.rs` inside the package being tested
- **binary smoke tests**: `tests/*.rs`, using `CARGO_BIN_EXE_<name>`
- **doctests**: on public APIs in library docs
- **workspace e2e**: dedicated `tests/e2e` crate or top-level app package

### Example placement

```text
crates/api/
├── src/
│   ├── lib.rs
│   ├── handlers.rs
│   └── service.rs
└── tests/
    ├── http_contract.rs
    └── cli_smoke.rs

tests/e2e/
├── Cargo.toml
└── src/lib.rs
```

Do not put full-system live-dependency e2e tests inside a tiny domain crate unless that crate truly is the system boundary.

## 10. Design for the borrow checker instead of fighting it

Move ownership at natural boundaries instead of cloning everything “just in case”.

### Bad: clone-heavy flow

```rust
#[derive(Clone)]
pub struct User {
    pub id: String,
    pub email: String,
}

pub fn user_emails(users: Vec<User>) -> Vec<String> {
    users.iter().map(|user| user.email.clone()).collect()
}
```

This needlessly takes ownership of the whole vector.

### Better: borrow the collection and return borrowed views when possible

```rust
pub struct User {
    pub id: String,
    pub email: String,
}

pub fn user_emails<'a>(users: &'a [User]) -> Vec<&'a str> {
    users.iter().map(|user| user.email.as_str()).collect()
}
```

### Better when ownership is actually needed

```rust
pub fn owned_user_emails(users: &[User]) -> Vec<String> {
    users.iter().map(|user| user.email.clone()).collect()
}
```

The function now makes the ownership cost explicit and keeps the API flexible.

## 11. Move ownership once at boundary seams

### Good pattern

- parse and validate borrowed inputs first
- convert into owned domain types once
- persist/send/spawn using owned values
- borrow from owned domain types during internal processing

```rust
#[derive(Debug)]
pub struct CreateOrder<'a> {
    pub customer_id: &'a str,
    pub sku: &'a str,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub customer_id: String,
    pub sku: String,
}

impl Order {
    pub fn from_request(req: CreateOrder<'_>) -> Self {
        Self {
            customer_id: req.customer_id.to_owned(),
            sku: req.sku.to_owned(),
        }
    }
}

pub(crate) async fn create_order(
    repo: &impl OrderRepository,
    req: CreateOrder<'_>,
) -> Result<Order, OrderError> {
    let order = Order::from_request(req);
    repo.insert(&order).await?;
    Ok(order)
}

pub(crate) trait OrderRepository {
    async fn insert(&self, order: &Order) -> Result<(), OrderError>;
}

#[derive(Debug, thiserror::Error)]
pub enum OrderError {
    #[error("repository failure")]
    Repository,
}
```

## 12. Architectural dos and don'ts

### Do

- keep binaries thin
- keep modules domain-oriented
- keep shared enum-dispatch contract modules provider-agnostic
- use `src/bin/` for small operational helpers
- use workspaces for real package boundaries
- place tests at the narrowest useful level
- make ownership transitions intentional

### Don't

- create “utils” dumping grounds
- centralize concrete enum-dispatch impls in shared contract modules
- split crates for speculative abstraction
- keep all behavior in `main.rs`
- change a repository-wide module policy casually
- duplicate domain types across crates without a strong reason
- clone data early to “make borrow checker errors go away”

## Further reading

- Cargo package layout: <https://doc.rust-lang.org/cargo/guide/project-layout.html>
- Cargo workspaces: <https://doc.rust-lang.org/cargo/reference/workspaces.html>
- Cargo targets: <https://doc.rust-lang.org/cargo/reference/cargo-targets.html>
- Rust module reference: <https://doc.rust-lang.org/reference/items/modules.html>
- Rust book on packages, crates, and modules: <https://doc.rust-lang.org/book/ch07-00-managing-growing-projects-with-packages-crates-and-modules.html>


## 13. Additional merged examples

### `tests/common/mod.rs` is still a useful exception

`tests/common/mod.rs` remains a good pattern for integration-test helpers
because it prevents `tests/common.rs` from being compiled as a standalone
integration test crate. In repositories that intentionally standardize on
`mod.rs`, follow that repo policy consistently instead of mixing styles
arbitrarily.

```text
tests/
├── common/
│   └── mod.rs
└── api_roundtrip.rs
```

```rust
// tests/api_roundtrip.rs
mod common;

#[tokio::test]
async fn healthcheck_is_ok() {
    let app = common::spawn_app().await;
    let response = app.client.get(app.url("/health")).send().await.unwrap();
    assert!(response.status().is_success());
}
```

### Bad: duplicated logic across binaries

```rust
// src/bin/import_users.rs
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = std::env::var("DATABASE_URL")?;
    let repo = users::PgRepo::connect(&config).await?;
    users::import(repo).await
}
```

```rust
// src/bin/sync_users.rs
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = std::env::var("DATABASE_URL")?;
    let repo = users::PgRepo::connect(&config).await?;
    users::sync(repo).await
}
```

This duplicates bootstrap logic and pushes real behavior into executable targets.

### Better: shared library, thin binaries

```rust
// src/lib.rs
pub mod users;

pub async fn repo_from_env() -> anyhow::Result<users::PgRepo> {
    let database_url = std::env::var("DATABASE_URL")?;
    users::PgRepo::connect(&database_url).await
}
```

```rust
// src/bin/import_users.rs
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let repo = my_app::repo_from_env().await?;
    my_app::users::import(repo).await
}
```

```rust
// src/bin/sync_users.rs
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let repo = my_app::repo_from_env().await?;
    my_app::users::sync(repo).await
}
```

## Additional source links

- Rust book: defining modules: <https://doc.rust-lang.org/book/ch07-02-defining-modules-to-control-scope-and-privacy.html>
- Rust book: separating modules into files: <https://doc.rust-lang.org/book/ch07-05-separating-modules-into-different-files.html>
- Rust book: test organization: <https://doc.rust-lang.org/book/ch11-03-test-organization.html>
- Cargo workspaces: <https://doc.rust-lang.org/cargo/reference/workspaces.html>
- rust-analyzer style notes: <https://rust-analyzer.github.io/book/contributing/style.html>
- Rust API Guidelines checklist: <https://rust-lang.github.io/api-guidelines/checklist.html>
- Rust design patterns: <https://rust-unofficial.github.io/patterns/>
