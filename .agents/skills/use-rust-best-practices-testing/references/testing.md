# Rust testing best practices

Use this reference when choosing test levels, placing tests, or wiring live dependencies.

For `ai-kit`, root `AGENTS.md` overrides generic placement examples below:
production `src/**` contains no `#[cfg(test)]` modules, so crate `/tests` trees
own unit-level owner semantics as well as integration coverage. Ai-kit also
separates static source and downstream compile contracts, primary direct
public-trait mocks and real consumer composition, other deterministic/process
integration, and real-service strict-live. Real dependencies are mandatory for
the built-in or infrastructure behavior claimed; deterministic framework
contracts use caller-owned implementations.

## 1. Use the smallest effective test level

### Unit tests

Use unit tests for:

- pure functions
- small state machines
- validation logic
- branch coverage
- fake or mock interactions for dependencies you own

In repositories that permit file-local tests, put them in the same file as the
code they test. In ai-kit, place these cases under the owning crate's `/tests`
tree.

### Integration tests

Use integration tests for:

- public API behavior of a package
- real database / cache / message broker dependencies
- request/response or repository contracts
- compiled binaries or process-level smoke tests

Put them in `tests/*.rs` inside the package being tested.

### Doctests

Use doctests for:

- public APIs that benefit from executable examples
- library usage snippets
- compile-fail or no-run documentation examples

Put them in doc comments on the public item.

### Binary smoke tests

Use them for:

- CLI help output
- config parsing
- basic process startup and error messages

Put them in `tests/*.rs` and invoke the compiled binary through `CARGO_BIN_EXE_<name>`.

### End-to-end tests

Use e2e tests for:

- full-system flows across multiple crates and services
- external contract validation
- live dependencies and actual process boundaries

In ai-kit, use existing app boundaries; do not add a new e2e or conformance
crate. Other repositories may use a dedicated workspace crate when their
policy permits it.

## 2. Unit tests belong next to the code

### Generic-repository example: file-local unit test with a fake

Do not copy this placement into ai-kit production `src/**`; move the equivalent
case to the crate `/tests` tree.

```rust
pub trait Clock {
    fn unix_seconds(&self) -> u64;
}

pub struct SessionService<C> {
    clock: C,
}

impl<C: Clock> SessionService<C> {
    pub fn new(clock: C) -> Self {
        Self { clock }
    }

    pub fn issue_expiry(&self, ttl_seconds: u64) -> u64 {
        self.clock.unix_seconds() + ttl_seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct FakeClock {
        now: u64,
    }

    impl Clock for FakeClock {
        fn unix_seconds(&self) -> u64 {
            self.now
        }
    }

    #[test]
    fn computes_expiry_from_fake_clock() {
        let service = SessionService::new(FakeClock { now: 1_700_000_000 });
        assert_eq!(service.issue_expiry(300), 1_700_000_300);
    }
}
```

### Bad

- reaching the network
- talking to the real database
- sleeping for arbitrary durations
- requiring Docker or external credentials

## 3. Built-in/infrastructure integration claims use public APIs and real dependencies

Ai-kit's primary deterministic lane uses crate-local structs and exhaustive
enums implementing all 16 approved public behavioral traits directly. It tests
their public semantics and the real Custom Agent/App with caller-owned Provider,
Toolset, StateStore, Database, and Agent hierarchies. Keep exact associated
children and continuations, consuming non-Clone owners, and App's five concrete
workflow seams, including local, borrowed, non-Unpin child futures without
boxing or erasing the returned execution future.

Use [the mock contract](../../../../docs/mock-testing.md) for fixture integrity,
complete inventories, process-local transaction semantics, and primary CI/runtime
qualification commands. These targets prohibit supplied lower-backend construction,
extension ingress, sockets, backend processes, and credentials. Same-process
reconstruction does not prove durability. Existing Tool extension routing and
associated-child normalization tests remain a separate deterministic lane. No
mock result proves a supplied built-in, wire protocol, persistence engine, or
external service.

### Prefer `testcontainers-modules` when available

When a maintained module exists, it is usually easier than building a raw image request yourself.

```rust
use sqlx::PgPool;
use testcontainers_modules::{postgres, testcontainers::runners::SyncRunner};

#[test]
fn repository_can_connect_to_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let container = postgres::Postgres::default().start()?;
    let host_port = container.get_host_port_ipv4(5432)?;

    let database_url = format!(
        "postgres://postgres:postgres@127.0.0.1:{host_port}/postgres"
    );

    tokio::runtime::Runtime::new()?.block_on(async {
        let pool = PgPool::connect(&database_url).await?;
        let row: (i64,) = sqlx::query_as("SELECT 1").fetch_one(&pool).await?;
        assert_eq!(row.0, 1);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
```

### `GenericImage` fallback remains valid

Use it when no maintained community module exists or when you need a very specific custom image.

```rust
use std::error::Error;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};

#[tokio::test]
async fn redis_round_trip() -> Result<(), Box<dyn Error>> {
    let container = GenericImage::new("redis", "7.2.4")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(6379).await?;
    let client = redis::Client::open(format!("redis://127.0.0.1:{host_port}/"))?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let _: () = redis::cmd("SET")
        .arg("greeting")
        .arg("hello")
        .query_async(&mut conn)
        .await?;

    let value: String = redis::cmd("GET")
        .arg("greeting")
        .query_async(&mut conn)
        .await?;

    assert_eq!(value, "hello");
    Ok(())
}
```

## 4. Use Docker Compose or dev containers for multi-service integration and e2e

When a test needs several services together, a compose file can be clearer than hand-wiring several raw containers.

```rust
use testcontainers::compose::DockerCompose;

#[tokio::test]
async fn stack_healthcheck_passes() -> Result<(), Box<dyn std::error::Error>> {
    let mut compose = DockerCompose::with_local_client(&["tests/docker-compose.yml"]);
    compose.up().await?;

    let web_port = compose.get_host_port_ipv4("web", 8080).await?;
    let response = reqwest::get(format!("http://127.0.0.1:{web_port}/health"))
        .await?
        .text()
        .await?;

    assert_eq!(response, "OK");
    Ok(())
}
```

Use real test API keys through environment variables only when the repository already supports that workflow and the test boundary genuinely requires it.

## 5. Add binary smoke tests for CLI behavior

Cargo exposes compiled binaries to integration tests via `CARGO_BIN_EXE_<name>`.

```rust
use std::process::Command;

#[test]
fn help_output_mentions_config_flag() {
    let output = Command::new(env!("CARGO_BIN_EXE_my-app"))
        .arg("--help")
        .output()
        .expect("binary should run");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    assert!(stdout.contains("--config"));
}
```

These tests are ideal for:

- `--help`
- missing required arguments
- config-file parsing
- startup banner or version output

They are not a replacement for full e2e tests.

## 6. Use doctests for public APIs

### Simple doctest

```rust
/// Parses a user id.
///
/// ```
/// use my_lib::parse_user_id;
/// assert_eq!(parse_user_id("42").unwrap(), 42);
/// ```
pub fn parse_user_id(input: &str) -> Result<u64, ParseUserIdError> {
    input.parse().map_err(ParseUserIdError::from)
}

#[derive(Debug, thiserror::Error)]
#[error("invalid user id")]
pub struct ParseUserIdError(#[from] std::num::ParseIntError);
```

### `compile_fail`

```rust
/// ```compile_fail
/// use my_lib::parse_user_id;
/// let _ = parse_user_id(42);
/// ```
```

### `no_run`

Use when the example should compile but should not execute in docs, such as examples that call external services.

```rust
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let body = reqwest::get("https://example.com").await?.text().await?;
/// println!("{body}");
/// # Ok(())
/// # }
/// ```
```

### `should_panic`

Use sparingly, mostly for deliberate panic examples.

```rust
/// ```should_panic
/// assert_eq!(2 + 2, 5);
/// ```
```

## 7. Turn on rustdoc link hygiene for libraries

At the crate root of a reusable library:

```rust
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(rustdoc::private_intra_doc_links)]
```

This catches documentation links that silently rot.

## 8. Generic-repository example: a dedicated workspace crate for e2e

This example is for repositories outside ai-kit that permit a dedicated e2e
crate. Ai-kit keeps full-system tests at existing app boundaries.

Example layout:

```text
tests/e2e/
├── Cargo.toml
└── src/
    ├── lib.rs
    └── signup_flow.rs
```

`tests/e2e/Cargo.toml`

```toml
[package]
name = "workspace-e2e"
version = "0.1.0"
edition.workspace = true

[lints]
workspace = true

[dependencies]
reqwest = { version = "0.12", features = ["json"] }
tokio.workspace = true
```

This keeps top-level system tests from polluting individual domain crates.

## 9. Make async tests deterministic

### Prefer readiness signals over arbitrary sleeps

Bad:

```rust
#[tokio::test]
async fn bad() {
    tokio::spawn(async move {
        start_server().await;
    });

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let response = reqwest::get("http://127.0.0.1:3000/health").await.unwrap();
    assert!(response.status().is_success());
}
```

Better:

```rust
#[tokio::test]
async fn waits_for_readiness_signal() {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        start_server(ready_tx).await;
    });

    ready_rx.await.expect("server should signal readiness");
    let response = reqwest::get("http://127.0.0.1:3000/health")
        .await
        .expect("request should succeed");
    assert!(response.status().is_success());
}

async fn start_server(ready_tx: tokio::sync::oneshot::Sender<()>) {
    let _ = ready_tx.send(());
}
```

### Add explicit timeouts for test awaits

Wrap network or readiness waits in `tokio::time::timeout(...)` when a stuck test would otherwise hang forever.

## 10. Guard composition without freezing private topology

Compile tests prove each exported handle implements its public trait, parent
associated types preserve the exact child contract, and non-Clone caller
hierarchies work directly. Behavioral tests prove supplied built-in dispatch.

At the open Tool seam, narrow source guards may require the sole root ingress
and reject public erasure or a bare trait-object handle. For the nine closed
Provider, Storage, and Agent dispatchers, narrow guards reject erased adapters,
`Extension` arms, normalization constructors, backend trait objects,
macro-generated dispatch, `async_trait`, compatibility shims, and stale
restoration TODO markers.

Do not freeze private enum or helper names, exact files, variant payloads, impl
placement, or match-arm spelling. Those details are implementation
choreography, not public behavior.

## 11. Testing dos and don'ts

### Do

- test behavior at the narrowest useful level
- keep unit-level tests fast and, in ai-kit, under crate `/tests`
- use real dependencies for built-in/infrastructure integration and e2e claims
- combine public compile/behavior proof with narrow negative source guards
- add doctests for public API examples
- add binary smoke tests for CLI behavior
- keep e2e tests at the actual system boundary

### Don't

- use mocks or scripted substitutes to claim built-in/infrastructure behavior
- replace enum-dispatch source-contract tests with macro-generated checks
- use networked resources in unit tests
- place whole-system tests inside unrelated domain crates
- rely on arbitrary sleeps for async coordination
- duplicate the same scenario across every test layer

## Further reading

- Cargo test: <https://doc.rust-lang.org/cargo/commands/cargo-test.html>
- Cargo environment variables: <https://doc.rust-lang.org/cargo/reference/environment-variables.html>
- Rustdoc doctests: <https://doc.rust-lang.org/rustdoc/write-documentation/documentation-tests.html>
- Rustdoc lints: <https://doc.rust-lang.org/rustdoc/lints.html>
- Testcontainers for Rust: <https://rust.testcontainers.org/>


## 12. Additional merged examples

### Unit tests: trait + fake, no live dependencies

```rust
pub trait EmailSender {
    fn send_welcome(&self, email: &str) -> Result<(), String>;
}

pub struct SignupService<S> {
    sender: S,
}

impl<S: EmailSender> SignupService<S> {
    pub fn new(sender: S) -> Self {
        Self { sender }
    }

    pub fn signup(&self, email: &str) -> Result<(), String> {
        if !email.contains('@') {
            return Err("invalid email".into());
        }
        self.sender.send_welcome(email)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeSender {
        sent: Arc<Mutex<Vec<String>>>,
    }

    impl EmailSender for FakeSender {
        fn send_welcome(&self, email: &str) -> Result<(), String> {
            self.sent.lock().unwrap().push(email.to_owned());
            Ok(())
        }
    }

    #[test]
    fn signup_sends_welcome_email() {
        let fake = FakeSender::default();
        let sent = fake.sent.clone();
        let service = SignupService::new(fake);

        service.signup("user@example.com").unwrap();

        assert_eq!(sent.lock().unwrap().as_slice(), ["user@example.com"]);
    }
}
```

### Integration tests can use live test API keys

```rust
#[tokio::test]
async fn invoices_api_healthcheck() -> anyhow::Result<()> {
    let base_url = std::env::var("TEST_API_BASE_URL")?;
    let api_key = std::env::var("TEST_API_KEY")?;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{base_url}/health"))
        .bearer_auth(api_key)
        .send()
        .await?;

    assert!(response.status().is_success());
    Ok(())
}
```

Use this pattern for real crate-level integrations when the repository intentionally supports live test credentials in CI, dev containers, or secure local workflows.

### Timer-heavy async tests can start with time paused

This requires Tokio's `test-util` feature.

```rust
#[tokio::test(start_paused = true)]
async fn retry_waits_before_next_attempt() {
    use std::time::{Duration, Instant};

    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(start.elapsed() >= Duration::from_secs(5));
}
```

## Additional source links

- Rust book: test organization: <https://doc.rust-lang.org/book/ch11-03-test-organization.html>
- testcontainers crate docs: <https://docs.rs/testcontainers/latest/testcontainers/>
- testcontainers quickstart: <https://rust.testcontainers.org/quickstart/testcontainers/>
- testcontainers networking: <https://rust.testcontainers.org/features/networking/>
- testcontainers modules: <https://docs.rs/testcontainers-modules>
- Dev Containers spec: <https://containers.dev/>
- GitHub Codespaces project setup: <https://docs.github.com/en/codespaces/setting-up-your-project-for-codespaces>
- Tokio testing guide: <https://tokio.rs/tokio/topics/testing>
- `tokio::test` docs: <https://docs.rs/tokio/latest/tokio/attr.test.html>
