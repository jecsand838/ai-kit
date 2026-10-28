# Redb StateStore Research

Use this file when `storage-kit-research` selects Redb StateStore behavior,
including its opt-in in-memory or persisted paths, transactions, savepoints and
durability. Generic embedded, in-memory or no-`sqlite` research does not select
Redb; use [state-stores.md](state-stores.md) to route the actual implementation.

## Boundary

- Redb research is for `storage_kit::StateStore` implementation work only.
- Redb is not a `storage_kit::Database` backend and is not model-visible
  storage.
- Redb should inform embedded transactional control-plane state: conversations,
  active executions, execution checkpoints, revisions, approvals, and recovery
  records.
- Redb must not be used to reintroduce a separate `InMemory` StateStore
  implementation family. Its opt-in in-memory path remains Redb-backed; Lance is the
  default StateStore.
- SQLite/Turso remains a separate StateStore backend for users who already
  rely on SQLite. Redb is not a SQLite replacement.

## Required Local Sources

Start with the pinned vendor metadata:

- `storage-kit/vendor/vendor-lock.toml`
- `storage-kit/vendor/vendors.toml`
- `storage-kit/vendor/redb-*.zip`

If an extracted `storage-kit/vendor/redb-*` directory exists, inspect it
directly. If it does not exist, inspect the zip with read-only commands such as
`unzip -p`; do not require tracking extracted vendor directories.

Priority local Redb files:

- `README.md`
- `Cargo.toml`
- `CHANGELOG.md`
- `docs/design.md`
- `src/db.rs`
- `src/transactions.rs`
- `src/backends.rs`
- `src/tree_store/page_store/backends.rs`
- `src/error.rs`
- `tests/basic_tests.rs`
- `tests/integration_tests.rs`
- `tests/multithreading_tests.rs`
- `tests/backward_compatibility.rs`

Treat the vendored source as the local code truth for the version pinned by
`vendor-lock.toml`.

## Required Official Sources

Use the active Redb docs and repository as authoritative behavior sources:

- [redb docs.rs crate root](https://docs.rs/redb/latest/redb/)
- [redb::backends](https://docs.rs/redb/latest/redb/backends/index.html)
- [InMemoryBackend](https://docs.rs/redb/latest/redb/backends/struct.InMemoryBackend.html)
- [FileBackend](https://docs.rs/redb/latest/redb/backends/struct.FileBackend.html)
- [Builder](https://docs.rs/redb/latest/redb/struct.Builder.html)
- [Database](https://docs.rs/redb/latest/redb/struct.Database.html)
- [ReadableDatabase](https://docs.rs/redb/latest/redb/trait.ReadableDatabase.html)
- [WriteTransaction](https://docs.rs/redb/latest/redb/struct.WriteTransaction.html)
- [ReadTransaction](https://docs.rs/redb/latest/redb/struct.ReadTransaction.html)
- [Savepoint](https://docs.rs/redb/latest/redb/struct.Savepoint.html)
- [Durability](https://docs.rs/redb/latest/redb/enum.Durability.html)
- [StorageBackend](https://docs.rs/redb/latest/redb/trait.StorageBackend.html)
- [Redb GitHub repository](https://github.com/cberner/redb)
- [Redb stable releases](https://github.com/cberner/redb/releases)
- [redb.org](https://www.redb.org/) as a directory to docs and repo only

Use stable release tags only for dependency updates; ignore prerelease tags
unless the user explicitly asks for prerelease research.

## Behavior To Verify

For in-memory StateStore research:

- Verify Redb's `InMemoryBackend` API and `Default` behavior.
- Verify `Builder::create_with_backend(InMemoryBackend::new())` as the
  opt-in embedded in-memory construction path.
- Treat Redb's synchronous database API as requiring an owned private
  worker-thread async boundary before implementing async `StateStore` receiver
  methods. The target boundary is bounded command queues into OS threads with
  typed replies and deterministic shutdown. Do not plan `spawn_blocking`,
  `block_on`, `block_in_place`, `tokio::sync::Semaphore`, runtime-task worker
  loops, or direct Redb calls on Tokio worker threads.

For persisted StateStore research:

- Verify persisted file construction through `Builder::create`,
  `Builder::open`, and `Builder::create_file`.
- Verify `FileBackend` behavior and platform file-lock notes before claiming
  multi-process read-only or writer behavior.
- Verify file-format and upgrade notes from `README.md`, `CHANGELOG.md`,
  `docs/design.md`, and official releases before changing persistence
  compatibility claims.

For transactionality:

- Verify ACID, MVCC, and serializable isolation claims against official docs
  and vendored `docs/design.md`.
- Verify single writer and multiple concurrent readers constraints before
  designing concurrency, locking, or async-boundary behavior.
- Verify `WriteTransaction::commit` and `WriteTransaction::abort` behavior
  before mapping `StateTransaction` reads and mutations.
- Treat `StateStore::begin_transaction(&self)` as the portable Redb write
  entry point. The public owned `StateTransaction` stays opaque while the Redb
  worker owns the underlying `WriteTransaction`; all savepoint behavior stays
  private to the Redb backend. The method returns after bounded logical-session
  queue admission, not native `begin_write`. Redb's single writer admits queued
  sessions FIFO; a second public handle may be retained while its first
  operation waits until transaction one terminates.
- Redb live transaction recovery uses one private initial ephemeral savepoint
  created before the worker opens state tables. If `put` or `delete` fails,
  restore to that initial checkpoint where possible, mark the transaction
  terminal, reject commit, and require rollback or drop.
  Timed-out or canceled non-final transaction commands must close the command
  session so commit cannot follow a caller-observed failure.
- Verify `ReadTransaction` snapshot behavior before mapping `load` and `list`.
- Verify persistent savepoint and ephemeral savepoint behavior through
  `persistent_savepoint`, `ephemeral_savepoint`, `restore_savepoint`,
  `get_persistent_savepoint`, and `delete_persistent_savepoint`.
- Verify durability behavior through `Durability::Immediate`,
  `Durability::None`, `set_durability`, `set_two_phase_commit`, and
  quick-repair notes before exposing or hard-coding a durability policy.
  The StateStore implementation leaves durability at the safe default/Immediate
  policy, must not use `Durability::None`, and must keep
  persistent savepoints private unless a portable checkpoint API is designed
  first.

For dependency and platform research:

- Verify Redb's pinned version from `vendor-lock.toml` before citing behavior.
- Verify Redb 4.3.0 upstream metadata: Rust 1.90, edition 2024, upstream
  default `std`, and optional `logging`, `cache_metrics`, `chrono_v0_4`, and
  `uuid` features. Upstream also declares `experimental-api-5`,
  `experimental_cursor`, and `experimental-multiprocess`; these are metadata,
  not enabled Storage features. Keep Storage's exact `=4.3.0` dependency with
  `default-features = false` and no experimental features. The campaign uses
  Rust 1.99.0 separately from Redb's native minimum.
- Verify workspace toolchain and MSRV compatibility before adding or updating
  the Redb dependency.
- Verify the explicit `redb` opt-in and its supported feature combinations;
  Redb embedded StateStore work must not require the `sqlite` feature.

## Output Requirements

Redb StateStore research output must include:

- selected scope: `state stores` or `both`
- local vendored Redb version and exact files inspected
- official Redb docs.rs and GitHub pages consulted
- sourced facts
- implementation inference
- async-boundary recommendation for Redb's synchronous API
- validation impact for source contracts, dependency guards, and local
  integration tests
- unresolved risks that still need local proof
