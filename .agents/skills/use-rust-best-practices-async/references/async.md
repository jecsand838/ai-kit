# Rust async, Tokio, and futures best practices

Use this reference when designing async APIs, task orchestration, shutdown logic, or concurrency limits.

## 1. Use async where it creates real value

Use async when:

- waiting on network I/O
- waiting on disk or database I/O through async drivers
- multiplexing many concurrent operations
- implementing long-lived services

Do not make everything async by default. For CPU-only local work, plain sync code is often simpler.

## 2. Use native async trait methods for private contracts

For private or `pub(crate)` traits, use native `async fn` in traits when the
repository MSRV supports it and the design does not need dynamic dispatch.

```rust
pub(crate) trait JobRunner {
    async fn run(&self, job: Job) -> Result<JobOutput, JobError>;
}
```

Native async trait methods return opaque futures and are not dyn-compatible.
That is acceptable for closed enum-dispatch contracts that delegate through an
enum and explicit `match` arms. If the design needs `Box<dyn JobRunner>`, the
problem is no longer the closed enum-dispatch pattern and needs a separate API
decision. Do not add `async_trait` just to preserve an old shape.

## 3. Choose the right concurrency shape

### Direct `.await`

Use when the work is sequential or when spawning would add needless overhead.

```rust
pub async fn refresh_cache(client: &ApiClient, cache: &Cache) -> Result<(), Error> {
    let latest = client.fetch_latest().await?;
    cache.store(latest)?;
    Ok(())
}
```

### `tokio::try_join!`

Use for a small fixed set of sibling futures where failure should short-circuit.

```rust
pub async fn load_dashboard(client: &ApiClient) -> Result<Dashboard, Error> {
    let (profile, orders, alerts) = tokio::try_join!(
        client.fetch_profile(),
        client.fetch_orders(),
        client.fetch_alerts(),
    )?;

    Ok(Dashboard { profile, orders, alerts })
}
```

### `tokio::select!`

Use when you need “first event wins” behavior, such as cancellation vs work.

```rust
use tokio::select;
use tokio_util::sync::CancellationToken;

pub async fn run_until_cancelled(token: CancellationToken) {
    select! {
        _ = token.cancelled() => {}
        _ = do_work_forever() => {}
    }
}

async fn do_work_forever() {}
```

### `tokio::task::JoinSet`

Use for a dynamic set of spawned tasks that all return the same type.

```rust
use tokio::task::JoinSet;

pub async fn square_all(values: Vec<u64>) -> Vec<u64> {
    let mut set = JoinSet::new();

    for value in values {
        set.spawn(async move { value * value });
    }

    let mut out = Vec::new();
    while let Some(result) = set.join_next().await {
        out.push(result.expect("task should not panic"));
    }

    out
}
```

### Bounded stream fan-out with `buffer_unordered`

Use for many similar async operations over a collection, but keep concurrency bounded.

```rust
use futures::{stream, StreamExt, TryStreamExt};

pub async fn fetch_all(
    client: ApiClient,
    ids: Vec<u64>,
) -> Result<Vec<Record>, Error> {
    stream::iter(ids)
        .map(|id| {
            let client = client.clone();
            async move { client.fetch_record(id).await }
        })
        .buffer_unordered(16)
        .try_collect()
        .await
}

#[derive(Clone)]
pub struct ApiClient;

#[derive(Debug)]
pub struct Record;

#[derive(Debug, thiserror::Error)]
#[error("api failure")]
pub struct Error;

impl ApiClient {
    pub async fn fetch_record(&self, _id: u64) -> Result<Record, Error> {
        Ok(Record)
    }
}
```

## 4. Do not `spawn` just to immediately await

### Bad

```rust
let handle = tokio::spawn(async move { fetch_user(id).await });
let user = handle.await??;
```

This adds scheduling overhead and weakens cancellation semantics.

### Better

```rust
let user = fetch_user(id).await?;
```

Spawn only when the work must run independently, outlive the current future, or be managed as part of a task set.

## 5. Own data at task boundaries

A spawned task must usually own what it uses.

### Bad

```rust
pub async fn bad<'a>(name: &'a str) {
    tokio::spawn(async move {
        println!("{name}");
    });
}
```

Borrowed data tied to the caller often does not live long enough for the spawned task.

### Better

```rust
pub async fn good(name: &str) {
    let name = name.to_owned();
    tokio::spawn(async move {
        println!("{name}");
    });
}
```

Convert to owned data exactly at the spawn boundary, not everywhere upstream.

## 6. Choose the right synchronization primitive

### Prefer `std::sync::Mutex` for short plain-data critical sections

```rust
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct Cache {
    inner: Arc<Mutex<HashMap<String, String>>>,
}

impl Cache {
    pub fn insert(&self, key: String, value: String) {
        let mut guard = self.inner.lock().expect("mutex poisoned");
        guard.insert(key, value);
    }
}
```

### Use `tokio::sync::Mutex` only when the guard must cross `.await`

```rust
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SharedConnection {
    inner: Arc<Mutex<tokio::net::TcpStream>>,
}
```

Even then, prefer a dedicated owner task plus message passing when the protected resource is long-lived I/O.

### Bad: hold a lock across `.await` without a strong reason

```rust
async fn bad(state: std::sync::Arc<tokio::sync::Mutex<Vec<String>>>) {
    let mut guard = state.lock().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    guard.push("done".to_owned());
}
```

### Better: compute outside the lock and keep the lock scope tiny

```rust
async fn better(state: std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    let value = do_async_work().await;

    let mut guard = state.lock().expect("mutex poisoned");
    guard.push(value);
}

async fn do_async_work() -> String {
    "done".to_owned()
}
```

## 7. Design the sync/async boundary before using blocking bridges

Blocking inside async code prevents the runtime thread from driving other
futures. A non-async method called by async code is still running inside the
async context, including any blocking work hidden in helpers or destructors.

Use this decision ladder before proposing `spawn_blocking`, `block_in_place`,
`block_on`, or semaphore throttling.

### 1. Use an async-native API when it exists

Prefer async HTTP clients, async database drivers, `tokio::fs`, async timers,
and stream-based APIs when they provide the same behavior. Do not adapt a
blocking client just because it was the first dependency found.

### 2. Make small CPU work cooperative

If local CPU work is small and naturally chunked, keep it in the async task and
yield periodically or process it through bounded async fan-out. Avoid this for
unbounded, expensive, or latency-sensitive CPU work.

```rust
pub async fn process_chunks(chunks: Vec<Chunk>) -> Vec<Output> {
    let mut outputs = Vec::with_capacity(chunks.len());

    for (index, chunk) in chunks.into_iter().enumerate() {
        outputs.push(process_one(chunk));
        if index % 128 == 0 {
            tokio::task::yield_now().await;
        }
    }

    outputs
}

pub struct Chunk;
pub struct Output;

fn process_one(_chunk: Chunk) -> Output {
    Output
}
```

### 3. Use a dedicated sync worker for long-lived synchronous resources

For synchronous engines with durable state, single-writer rules,
non-cancellable side effects, or long-lived connection/client ownership, use a
private owner thread or worker runtime. Expose async methods through bounded
`tokio::sync::mpsc` command queues and typed reply channels. In the sync worker
context, receive commands with `blocking_recv`.

This shape is better than `spawn_blocking` plus `Semaphore` for embedded
databases, blocking client libraries, single-writer state stores, and other
persistent synchronous resources. The queue provides backpressure, the worker
owns resource lifecycle, and the command enum keeps behavior method-oriented.

Use `assets/templates/sync_worker_bridge.rs` as the starting point.

Important timeout rule: once a non-cancellable side-effectful operation starts,
return its definitive result. Time out enqueueing work, and optionally time out
waiting for side-effect-free reads, but do not report a write timeout while the
write may still commit in the background.

### 4. Use `spawn_blocking` only for short finite blocking work

`spawn_blocking` is acceptable when the work is unavoidable, bounded, and
eventually finishes on its own.

Good uses:

- one-shot legacy parser
- finite compression or decompression step
- password hashing or similar finite CPU-bound operation
- unavoidable blocking filesystem call while migrating to an async-native path

Bad uses:

- long-running worker loops
- persistent embedded database or sync-engine adapters
- unbounded queues of CPU-heavy jobs
- hiding repeated blocking calls instead of choosing an async-native library

```rust
pub async fn parse_legacy_file(path: std::path::PathBuf) -> Result<Config, Error> {
    tokio::task::spawn_blocking(move || parse_sync(path))
        .await
        .expect("blocking task should not panic")
}

#[derive(Debug)]
pub struct Config;

#[derive(Debug, thiserror::Error)]
#[error("parse failure")]
pub struct Error;

fn parse_sync(_path: std::path::PathBuf) -> Result<Config, Error> {
    Ok(Config)
}
```

Tokio's blocking pool has a large upper limit because it also supports blocking
I/O. If many CPU-heavy computations are unavoidable, use a dedicated CPU
executor or process-level architecture instead of filling Tokio's blocking
pool.

### 5. Use `block_in_place` only in narrow multi-thread-runtime cases

`block_in_place` tells Tokio that the current worker thread is about to block,
but code inside it cannot be cancelled. It is unavailable on the
current-thread runtime and suspends other work in the same task, such as sibling
branches inside `join!`. Use it only for a small, local, well-contained bridge
where those tradeoffs are acceptable.

### 6. Use `block_on` only at sync entry points

Use `#[tokio::main]`, a process-owned runtime, or a deliberate sync wrapper at
the outer boundary. Do not call `block_on` from async code, do not create nested
runtimes, and do not repeatedly construct runtimes for individual operations.

## 8. Design graceful shutdown explicitly

A solid default is:

- `CancellationToken` to signal shutdown
- `TaskTracker` to wait for tasks to finish
- bounded cleanup logic inside each worker

```rust
use tokio::select;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub async fn run_workers() {
    let token = CancellationToken::new();
    let tracker = TaskTracker::new();

    for worker_id in 0..4 {
        let token = token.clone();
        tracker.spawn(async move {
            loop {
                select! {
                    _ = token.cancelled() => break,
                    _ = do_one_job(worker_id) => {}
                }
            }
        });
    }

    wait_for_shutdown_signal().await;
    token.cancel();
    tracker.close();
    tracker.wait().await;
}

async fn do_one_job(_worker_id: usize) {}
async fn wait_for_shutdown_signal() {}
```

For dynamic spawned tasks, `JoinSet` also provides a useful shutdown story.

## 9. Use `LocalSet` and `spawn_local` for `!Send` futures

Some async code uses `Rc`, `RefCell`, or other `!Send` state. Do not force it into `tokio::spawn`.

```rust
use std::rc::Rc;
use tokio::task::LocalSet;

pub async fn run_local_tasks() {
    let local = LocalSet::new();

    local
        .run_until(async {
            let state = Rc::new("hello".to_owned());
            let state2 = state.clone();

            tokio::task::spawn_local(async move {
                println!("{state2}");
            })
            .await
            .expect("local task should finish");
        })
        .await;
}
```

Use this only when `!Send` is genuinely required.

## 10. Bridge sync and async at one clear boundary

### Good

- `#[tokio::main]` in a binary
- one owned runtime in a dedicated host
- async APIs exposed as async all the way down

### Bad

- creating nested runtimes
- calling blocking sync wrappers inside already-async code
- repeatedly constructing runtimes for individual calls

```rust
fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    Ok(())
}
```

## 11. Use timeouts and cancellation consciously

Add timeouts around network or external waits so operations fail clearly instead of hanging forever.

```rust
use tokio::time::{timeout, Duration};

pub async fn timed_fetch(client: &ApiClient) -> Result<Record, TimedFetchError> {
    timeout(Duration::from_secs(5), client.fetch_record(42))
        .await
        .map_err(|_| TimedFetchError::Timeout)?
        .map_err(TimedFetchError::Api)
}

#[derive(Debug, thiserror::Error)]
pub enum TimedFetchError {
    #[error("request timed out")]
    Timeout,
    #[error("api failure: {0}")]
    Api(#[from] Error),
}
```

## 12. Async dos and don'ts

### Do

- choose concurrency shape deliberately
- prefer async-native crates such as async HTTP clients, async database drivers,
  and `tokio::fs` where behavior is equivalent
- use dedicated owner workers for synchronous engines with long-lived state,
  single-writer rules, or non-cancellable side effects
- use bounded channels for backpressure and typed command enums for behavior
  ownership
- apply timeouts before enqueueing work, or while waiting for
  side-effect-free reads
- use native async trait methods for private closed contracts
- bound fan-out
- own values at spawn boundaries
- use cancellation tokens and task tracking
- keep lock scopes tiny
- prefer message passing for long-lived I/O owners

### Don't

- spawn just to await immediately
- put long-running worker loops inside `spawn_blocking`
- combine `spawn_blocking` plus `Semaphore` as the first draft of a persistent
  sync-engine adapter
- return a timeout after a non-cancellable write may have started
- call `block_on` from async code or create nested runtimes
- use `block_in_place` where current-thread runtimes or cancellation matter
- add `async_trait` to closed private enum-dispatch contracts
- hold guards across `.await` casually
- use `Arc<Mutex<_>>` as the default answer to every borrow issue
- bury blocking work inside helpers or destructors called from async code
- leave shutdown behavior unspecified

## Further reading

- OneUptime async blocking guide: <https://oneuptime.com/blog/post/2026-01-07-rust-async-without-blocking/view>
- Tokio task blocking and yielding: <https://docs.rs/tokio/latest/tokio/task/index.html#blocking-and-yielding>
- Tokio `JoinSet`: <https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html>
- Tokio `spawn_blocking`: <https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html>
- Tokio `block_in_place`: <https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html>
- Tokio `mpsc::Receiver::blocking_recv`: <https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Receiver.html#method.blocking_recv>
- Tokio mutex guidance: <https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html>
- Tokio-util `CancellationToken`: <https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html>
- Tokio-util `TaskTracker`: <https://docs.rs/tokio-util/latest/tokio_util/task/task_tracker/struct.TaskTracker.html>
- futures `StreamExt`: <https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html>


## 13. Additional merged examples

### Dynamic fan-out with `FuturesUnordered`

Use this when the set of futures is large or discovered incrementally.

```rust
use futures::{stream::FuturesUnordered, StreamExt};

pub async fn fetch_many(urls: Vec<String>) -> Vec<anyhow::Result<String>> {
    let client = reqwest::Client::new();

    let mut in_flight = urls
        .into_iter()
        .map(|url| {
            let client = client.clone();
            async move {
                let response = client.get(url).send().await?;
                let text = response.error_for_status()?.text().await?;
                Ok::<_, anyhow::Error>(text)
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut results = Vec::new();
    while let Some(result) = in_flight.next().await {
        results.push(result);
    }
    results
}
```

Prefer this shape when you need incremental completion handling rather than one giant `join_all`.

### Give one task ownership of async resources

```rust
use tokio::sync::mpsc;

pub fn start_reporter(client: reqwest::Client) -> mpsc::Sender<String> {
    let (tx, mut rx) = mpsc::channel(256);

    tokio::spawn(async move {
        while let Some(body) = rx.recv().await {
            if let Err(error) = client
                .post("https://example.com/report")
                .body(body)
                .send()
                .await
            {
                tracing::warn!(?error, "report failed");
            }
        }
    });

    tx
}
```

This is often better than hiding a network client behind `Arc<tokio::sync::Mutex<_>>`.

### Prefer one runtime boundary

```rust
pub fn run_job() -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { do_work().await })
}
```

Avoid nested runtimes or helper functions that create a fresh runtime on every call when a reusable boundary is available.

## Additional source links

- Async Book: <https://rust-lang.github.io/async-book/>
- `Future` in std: <https://doc.rust-lang.org/std/future/trait.Future.html>
- futures crate docs: <https://docs.rs/futures/latest/futures/>
- `FuturesUnordered`: <https://docs.rs/futures/latest/futures/stream/struct.FuturesUnordered.html>
- Tokio tutorial: <https://tokio.rs/tokio/tutorial>
- Tokio shared-state guidance: <https://tokio.rs/tokio/tutorial/shared-state>
- Tokio spawning guide: <https://tokio.rs/tokio/tutorial/spawning>
- Tokio testing guide: <https://tokio.rs/tokio/topics/testing>
- Tokio graceful shutdown: <https://tokio.rs/tokio/topics/shutdown>
- Tokio `join!`: <https://docs.rs/tokio/latest/tokio/macro.join.html>
- Tokio `try_join!`: <https://docs.rs/tokio/latest/tokio/macro.try_join.html>
- Tokio `select!` tutorial: <https://tokio.rs/tokio/tutorial/select>
- Rust blog on async fn in traits: <https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/>
- Rust Reference trait dyn compatibility: <https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility>
