# Storage prerequisite audit for the app-sdk completion plan

Scope: **both** StateStore/StateTransaction control-plane persistence and Database managed knowledge. Read-only audit on 2026-09-13. No Cargo command, service, or runtime probe was run; conclusions below distinguish source observations from proposed acceptance work.

## Verdict

The public type surface is already sufficient for trait-based app persistence and lower Agent knowledge. **Do not add StateStore history, snapshot, fork, batch, CAS, watch, repository, factory, or shutdown traits.** Checkpoint/history/fork semantics belong to app-sdk records and transactions, not to a native storage escape hatch.

There is nevertheless required pre-work: specify and prove the semantic obligations that caller StateStores must satisfy; strengthen the shallow downstream and PostgreSQL tests; remove prohibited redundant inherent forwarders; then remove app's built-in ephemeral construction and concrete default types. No new behavioral seam, crate, dependency, feature, wrapper, erasure, or registry is justified by this audit.

## Classified findings and exact touchpoints

1. **Normative lower-crate contract / repository observation — static composition is ready.** `storage-sdk/src/state_handle.rs:12` requires only `Send + Sync + 'static` for `StateStoreBackend`, not Clone. Line 14 returns an associated `Transaction`; line 35 requires only `Send + 'static` for that unique child. Lines 55 and 60 consume the child on commit/rollback. `storage-sdk/src/database_handle.rs:16` similarly accepts direct non-Clone Database implementations. Supplied handles and private dispatchers remain closed over built-ins. No erasure ingress is present.

2. **Repository observation — returned DTOs are constructible externally.** `storage-sdk/src/state/record.rs:112` publicly constructs positive StateRevision values; lines 163, 180 and 281 expose passive StateRecord, StateRecordSnapshot and StateListResponse fields. Request namespace, prefix, limit and exclusive cursor are public at lines 211, 222, 228, 239 and readable at 250-274. Namespace/key bounds are 256/1024 bytes and page bounds are 1-1000 (`record.rs:7-10`). Database `QueryResponse` and `ScoredRecord` are public DTOs at `storage-sdk/src/core/query/mod.rs:135,146`; PageToken has public construction at 31. CatalogResponse and CollectionInfo are publicly constructible at `storage-sdk/src/core/catalog/mod.rs:16,176`. No DTO-construction change is needed.

3. **Repository observation / required contract pre-work — transaction semantics are underdocumented for custom implementations.** `storage-sdk/src/state_handle.rs:11-62` has only short method summaries. `storage-sdk/ARCHITECTURE.md:99-163` repeats the signatures but does not fully specify the portable isolation/read-set, missing-key fence, revision, pagination, cancellation and ambiguous-commit obligations relied on by App and Agent. `storage-sdk/docs/PORTABLE_API.md` does not exist. Add one authoritative portable contract there, move the signature inventory out of private-architecture prose, link it from README and trait rustdoc, and expand the trait rustdoc with the critical behavior. Do not add another behavior trait or public conformance owner.

   The contract must say: committed transactions are serializable across their read/write key sets, including missing-key observations; failed/conflicting transactions do not partly apply; reads see own writes/deletes; a put/delete advances each key's revision according to a documented rule (decide and prove exact +1 for each staged operation, consistent with current adapters, rather than leaving it as merely increasing); delete/recreate cannot reset revision and admit ABA; rollback/drop never commit; admitted finalization retains native authority after a waiting future is dropped; success means all mutations are committed; transport/time-out/uncertain commit failure does not prove rollback. Native backend admission/rollback machinery stays private. Clarify invalid request, exact conflict and ambiguous failure mapping.

   List must guarantee namespace/prefix filtering, canonical ascending key order, exclusive advancing key cursor, bounded page length, no tombstones and honest end-of-list. A page sequence **is not a transactional historical snapshot**. App must fence its inventory/head and revalidate if mutations can race. The transaction trait deliberately has no list operation: App's known keys and append-only immutable history records remove the need to broaden it.

4. **Repository observation — downstream proof is too weak.** `storage-sdk/tests/state_store_backend.rs:10` and `storage-sdk/tests/database_backend.rs:15` derive Clone on the main recording implementations. The state test returns None/empty pages (`state_store_backend.rs:26-39`); its conflict test manufactures one commit error (`:213`). The database test queries an empty response (`database_backend.rs:52-54`). These prove dispatch shape, not a non-Clone caller hierarchy with populated DTOs, revisions, continuation pages, atomic writes or lower Agent knowledge operation results.

5. **Repository observation — PostgreSQL integration proof is much lighter than the accepted contract.** `storage-sdk/tests/strict_live.rs:292-344` only connects, writes one record, loads it, then deletes it. It does not prove the two-client serializable read/write and absent-key conflicts, rollback/drop, read-your-writes, pagination, exact revision progression, lock timeout, terminal caller loss or multi-key atomicity required by storage's research/architecture. Expand this target before considering the app persistence prerequisites green. Redb/SQLite local coverage exists in `storage-sdk/tests/state_store_local.rs:15,79,168,218,275,298,331,398`, but exact revision assertions currently only check `second > first` at line 124; add precise sequences, absence-fence and atomic history/head/receipt cases.

6. **Repository observation / required reduction — duplicated inherent behavioral contracts survive.** Delete the same-name inherent forwarding methods at `storage-sdk/src/database_handle.rs:82-106`, `storage-sdk/src/state_handle.rs:100-119`, and `storage-sdk/src/state_handle.rs:235-261`. Keep construction (`connect`) and trait implementations. Update downstream tests/examples/imports to bring the owning traits into scope and call receiver methods. The root instructions explicitly forbid same-name inherent forwarding contracts. Do not replace them with aliases, blanket extension traits, macros, UFCS dispatch paths or new wrappers. Source guards may ban the prohibited shape but must not freeze private file names.

7. **Repository observation / app-owned integration pre-work — App currently constructs a default implementation.** `app-sdk/src/storage/mod.rs:6-12` imports Redb construction and concrete StateStore/StateTransaction; lines 31-110 define two dispatch enums and duplicate all state/transaction behavior; lines 250-256 connect in-memory Redb for Ephemeral. App public generics default to supplied Agent/StateStore at `app-sdk/src/lib.rs:49,54,61,69,92` and `app-sdk/src/workflow/mod.rs:23,28`. Remove that concrete coupling. Amend `app-sdk/specs/STATE_STORE.md:25-30` and `app-sdk/specs/APP.md:36` and related exact API inventory first. Require caller-built CS in all cases, then store CS directly in AppStore. Process-local/durable persistence comes from the caller's chosen store; if an App policy distinction has no actual behavior beyond construction, eliminate AppState rather than retaining a wrapper-only `AppState(CS)`. If an app retention/reopen policy remains necessary, keep the policy separate from storage implementation selection and specify its invariant. Constructors in executable examples may instantiate built-ins outside the library.

8. **Repository observation — tests freeze the wrong internals.** `app-sdk/tests/public_surface.rs:480` explicitly expects concrete default generic strings; line 590 requires `enum AppStoreInner<CS: StateStoreBackend>`. Replace these topology assertions with positive external compilation for direct non-Clone CS + A and negative checks against forbidden construction/erasure, in the test-first change. Do not preserve AppStoreInner merely to satisfy stale source spelling.

9. **Normative lower-crate contract / repository observation — Database remains below Agent.** `agent-sdk/src/agent_storage.rs:241,297` is generic in DatabaseBackend; Agent builders accept a generic knowledge database at `agent-sdk/src/builder.rs:108`. Storage's catalog/data/query/index DTO surface is adequate for that composition. App must not receive Database, KnowledgeBase, credentials or lower configuration to implement workflow actions/history. Integration tests in agent-sdk should exercise a non-Clone caller Database through the actual managed-knowledge path. The Agent audit owns any inability to compose that path; there is no storage API gap established here.

## Tests-first implementation sequence

Every slice ends only when its focused tests pass and an independent subagent reviews public boundaries, async ownership and the exact changed diff. A reviewer finding reopens the slice. Record failing test output before production changes; when a new test already passes, record it as missing-proof closure and do not invent an implementation change.

### S1: Portable obligations and downstream implementations

- First replace Clone recording fixtures in `storage-sdk/tests/state_store_backend.rs:10` and `database_backend.rs:15` with non-Clone owners. Keep fixture logs in shared observation state; the backend object itself must not implement Clone. Use ordinary test-local structs/methods, no public conformance framework.
- Add a genuine test-local revisioned state implementation to the owning existing test tree, with its own non-Clone associated transaction, read set, staged mutation map and data-carrying transaction state. Exercise positive revisions, nonempty page output, multiple pages, get-after-put, tombstone/recreate, rollback, stale read conflict and concurrent absent-key insertion. This proves the public seam, not any built-in service. Prefer adapting existing App fixture owners rather than duplicating a full state engine into multiple crates; no cross-crate fixture crate is permitted.
- Add externally compiled calls for `StateRevision::new`, StateRecordSnapshot and StateListResponse construction, including nonempty continuation, and a Database QueryResponse with nonempty hits, explicit score meaning and a caller-generated PageToken. Exercise Database logical locator request fields and unsupported errors; never infer Database success from capability presence alone.
- Establish exact semantic cases for serializable known/absent read fences in real Redb/SQLite and PostgreSQL suites before updating the production adapters or normative docs.
- Add `storage-sdk/docs/PORTABLE_API.md` (new line 1), expand `state_handle.rs:11-62` and `database_handle.rs:15-42` rustdoc, link README, and replace the competing signature ledger in `ARCHITECTURE.md:99-163` with ownership prose plus link.

### S2: Delete redundant forwarding contracts

- First update compile/public behavior tests and doctests to import DatabaseBackend, StateStoreBackend and StateTransactionBackend wherever they call trait operations; add a narrow source check rejecting inherent methods that reproduce the approved trait contract. Existing tests that merely use `.method` must still compile with the trait in scope.
- Delete only the three forwarding blocks listed in finding 6; retain trait implementations, closed private enums, built-in constructor and drop/cleanup ownership.
- Compile every dependent Agent/App integration target and doctest. Fix imports in consumers, not by restoring forwarders or widening surfaces.

### S3: Validate built-in transactional guarantees

- Add to `storage-sdk/tests/state_store_local.rs` cases for two keys committed atomically with a receipt; failed staging rolls all keys back; absent-key fences serialize concurrent creation; revision values are exact across multiple puts/delete/recreate, including writes within one transaction; list traverses more than one bounded page in key order, and namespace/prefix limits remain exact; cancelled queued admission and dropped admitted transactions leave no accidental commit.
- Add to `storage-sdk/tests/strict_live.rs` PostgreSQL cases for the same portable guarantees plus two independent connections with a deterministic barrier around conflicting serializable reads, a missing-key insert race, deliberate row lock contention mapped to TimedOut, and rollback/drop/finalization after waiting caller loss. Use real configured PostgreSQL and bounded test control. Never wrap native admitted commit and treat timeout cancellation as rollback; application observation deadlines and storage finalization authority are separate.
- Only if tests identify defects, modify the owning backend: PostgreSQL `state_stores/postgres/live_transaction.rs:470,504,533` (read/write/atomic revision statements), `store.rs:170,292` (error/list behavior); SQLite `live_transaction.rs:514,547,556,587` and `store.rs:142`; Redb `transaction_session.rs:390,399` and its existing worker/actor ownership. Do not preemptively consolidate distinct native actors.
- Add an App-level uncertain-commit test using an existing custom store that commits then loses its reply; reload the receipt and classify success exactly once, without replaying lower work. This tests `app-sdk/src/storage/mod.rs:342-419`. Historical writes must enter this same epilogue.

### S4: Remove App's default-store construction and implement history through existing traits

- First update App public compile contracts and behavior to require the supplied CS and accept non-Clone A/CS with caller-owned associated children. A source guard rejects concrete built-in storage construction in production App. Tests with real built-ins construct them at the test boundary.
- Change App configuration and owners to `AppConfig<CS, A>` / `App<CS, A>` and `AppStore<CS> { inner: CS }`; delete AppStoreInner and AppStoreTransaction and their implementations entirely. AppStore initialization becomes a domain constructor and cannot fail because it opens a lower built-in backend.
- Reuse AppStore's record codec, RecordKey, closed AppMutation and one receipt/commit epilogue for history. Conceptually extend the existing RecordKey with a typed checkpoint key carrying workspace/workflow/run/sequence. Store immutable checkpoint rows separately from the mutable run head. In one existing associated transaction: fence the app generation and run head; prove the sequence key absent; write the immutable checkpoint; update the run head/control; write the mutation receipt; commit. Do not introduce a generic repository, mutation builder, history backend trait, storage snapshots or separate app persistence actor.
- Historical enumeration uses StateListRequest prefix + cursor and captures an upper sequence from the revision-fenced run head; load exact keys to inspect/fork. Fork reads the selected immutable checkpoint and transactionally creates its new run/head under normal admission guards. Retention deletes only App-owned keys after App's fork/active/observer reachability rules authorize removal; it does not invoke native backend snapshots. New history should not be retained inline in one 4 MiB run record (`app storage/mod.rs:25`).
- Reconcile no snapshot across separate `list` calls honestly: immutable sequence rows plus head cutoff avoid fabricating a storage snapshot; partial pruning/concurrent history views need explicit App contract and bounded revalidation. Persisted native StateRevision is a fence; checkpoint sequence is App data and must not be inferred from native revisions.

Conceptual sketch, intentionally not an extra public API ledger:

```rust
struct AppStore<CS: StateStoreBackend> {
    inner: CS,
}

impl<CS: StateStoreBackend> AppStore<CS> {
    fn new(inner: CS) -> Self {
        Self { inner }
    }

    // Existing commit receiver owns one CS::Transaction, receipt and outcome.
    // Extend existing RecordKey/AppMutation with checkpoint cases.
    // No second StateStoreBackend implementation or transaction wrapper.
}
```

## Validation commands and completion evidence

Run focused targets during each assembled tests-first vertical; the user's explicit test-first instruction supersedes local blanket prose deferring validation to an entire workspace batch. Keep the supported dependency feature split. Do not run storage `--all-features`: default sqlite plus qdrant remain incompatible in the pinned graph.

Canonical full lane script: `bash storage-sdk/scripts/check.sh` (`scripts/check.sh:8-45`). It runs all-target no-run, `public_api`, `database_backend`, `state_store_backend`, `state_store_local`, `source_contract`, doctests, clippy `-D warnings`, and rustdoc `-D warnings` sequentially under (1) defaults, (2) `--no-default-features`, (3) `--no-default-features --features qdrant`. Expanded commands are `storage-sdk/docs/testing.md:31-55`.

Focused examples:

```bash
cargo test -p storage-sdk --test state_store_backend
cargo test -p storage-sdk --test database_backend
cargo test -p storage-sdk --test state_store_local
cargo test -p storage-sdk --test public_api
cargo test -p storage-sdk --test source_contract
cargo test -p storage-sdk --doc
bash storage-sdk/scripts/check.sh
source storage-sdk/strict-tests.env
bash storage-sdk/scripts/verify_strict_live.sh
```

Actual strict verifier runs the combined real Database/PostgreSQL target with `cargo test -p storage-sdk --test strict_live --no-default-features --features qdrant -- --nocapture --test-threads=1` (`verify_strict_live.sh:92`) and then the real Redb/SQLite local suite with defaults (`:95`). Its test volume resets are scoped to the declared storage compose project. Missing services/env fail; no skip/mock substitutes.

All modified importing crates must then pass their documented checks, strict verifiers and the final five-crate strict wrapper/locked-clean-checkout proof. Record exact commit, feature lane, passed/failed counts, service/version context and remaining unresolved evidence. No strict-live run occurred in this audit.

## Evidence and proof limits

- Normative local sources: root AGENTS; storage AGENTS, README, ARCHITECTURE and docs/testing; requested storage research plus state-store/redb/postgres references; storage change-contract; App STATE_STORE and the exact source/test paths above.
- Pinned Turso v0.6.1: verified `storage-sdk/vendor/turso-0.6.1.zip` SHA-256 `0f3f339f7cda39e136d457e0f3e8b3fbeae18d986864be1e43c4b83a7d147791`, matching vendor-lock. Read `turso-0.6.1/bindings/rust/src/transaction.rs:9,154,217,261,274`: transaction modes, consuming commit, dangling-transaction drop recovery and transaction construction. Official Rust reference consulted: https://docs.turso.tech/sdk/rust/reference (accessed 2026-09-13).
- Pinned Redb v4.1.0: verified `storage-sdk/vendor/redb-4.1.0.zip` SHA-256 `43d6a566fe35213fbaa4461b7b099f55e5a3c9101af9626ed09ed74e8e691dbc`, matching vendor-lock. Read `redb-4.1.0/src/db.rs:1037` (blocking single native writer), `src/transactions.rs:1460,1601,2084` (commit/abort/drop). Official latest WriteTransaction/Database docs consulted at https://docs.rs/redb/latest/redb/struct.WriteTransaction.html and https://docs.rs/redb/latest/redb/struct.Database.html; latest is 4.2.0, so **do not apply newer transaction-poisoning guarantees to the pinned 4.1.0 implementation**. No dependency upgrade is proposed.
- Verified external fact: PostgreSQL SERIALIZABLE admits only serializable committed outcomes and can reject with serialization failure. https://www.postgresql.org/docs/current/transaction-iso.html (PostgreSQL 18 docs, accessed 2026-09-13). This supports tests of the existing adapter, not a claim that tests passed.
- Verified external fact: tokio-postgres Transaction consumes self on commit/rollback and rolls back on drop. https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.Transaction.html (0.7.18 current docs accessed 2026-09-13; local dependency remains pinned). It does not by itself prove this SDK's detached-waiter semantics; the SDK owns actor and connection-driver lifetime.
- No new LanceDB/SurrealDB/Qdrant backend behavior is proposed or asserted. Database conclusions here are portable DTO/trait availability and Agent ownership observations, not fresh service capability validation.
