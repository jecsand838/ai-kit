# storage-sdk Testing

For each approved change, add or strengthen focused tests first and record
meaningful red evidence before production edits. An already-passing new test
closes a proof gap; it does not justify an unrelated implementation change.
Run Cargo commands serially and finish each area with green focused tests and
independent review of the actual diff before the full feature and live gates.

## Primary Direct-Trait Lane

From the workspace root, run:

```bash
bash scripts/check_mock_contracts.sh --crate storage-sdk
bash scripts/check_ci.sh
```

The scoped command runs `mock_contracts` with `--no-default-features`;
`check_ci.sh` owns the complete workspace primary lane. Test-local structs and
exhaustive enums implement Database, StateStore, and StateTransaction traits
directly. StateStore fixtures publish process-local transactional state;
reconstruction over retained state is same-process evidence, not durability.
Database fixtures compare typed requests and return scripted responses without
implementing a database engine.

The target constructs no supplied backend, opens no sockets or backend processes,
and requires no credentials. Unconditional Lance dependencies still compile.
Scoped results are not full-suite evidence. Follow the
[shared mock contract](../../docs/mock-testing.md) for inventory and runtime
qualification. Existing native integration tests, feature lanes and strict-live
gates below remain required; never use Storage `--all-features`.

## Proof Lanes

1. Source contracts protect the public module allowlist, privacy, three closed
   built-in dispatcher enums, absence of erasure/identity symbols, and
   backend-detail isolation.
2. Downstream tests implement `DatabaseBackend` and `StateStoreBackend`
   directly with an associated custom transaction, while public API tests
   prove the built-in handles implement the same traits.
3. Direct-trait mocks prove caller transaction semantics, typed request/result
   fidelity, fixture integrity, and process-local consumer composition. They
   do not reproduce supplied native actors or establish persistence.
4. Deterministic adapter and local integration tests cover validation, native
   actor ownership, bounded queues, conflict mapping, rollback-on-drop,
   cancellation, terminal authority, redaction, and deterministic joining.
   Local integration exercises Redb, SQLite/Turso, and embedded Lance StateStore.
5. Strict-live verification runs the selected real engines/services, including
   embedded Lance, supported Database modes, and selected alternative StateStores.

Ordinary test helpers replace the deleted production conformance suites. They
remain private to `/tests`, use no macro framework, and never claim built-in
behavior from a fake implementation.

The SQLite cases in `state_store_local` exercise repeated point reads and
exhausted multi-row lists, exact revision increments through delete/recreate,
and rollback without publishing staged revisions. Separate memory/file cases
poll a nonterminal operation once before dropping its waiter, require mutation
and commit rejection, and join rollback cleanup. Other memory/file cases admit
commit before losing its waiter and reconcile exact atomic receipts, then
commit another transaction. These use two-second operation timeouts and
five-second outer bounds. File cases drop every store owner and reopen twice;
memory cases distinguish shared-owner visibility from fresh empty stores.
Clean reopen proves ordinary persistence, not crash-before-checkpoint durability.
The separate commit/rollback, dropped-transaction reopen, and real slow-trigger
1 ms timeout regressions remain required alongside both strict memory/file cases.

## Feature Matrix

The historical feature-split evidence belongs in the
[feature-split qualification ledger](feature-split-qualification.md).

Lance is always available; default and no-default builds contain the same
implementations. The alternatives are opt-in `redb`, `postgres`, `surrealdb`,
`sqlite`, and `qdrant`. Never combine `sqlite` with `qdrant` or run Storage
`--all-features`.

```bash
bash storage-sdk/scripts/check.sh
bash storage-sdk/scripts/check.sh --backend postgres
bash storage-sdk/scripts/check.sh --all-backends
python3 -m unittest discover -s storage-sdk/scripts/tests -v
```

Each selected lane compiles every target, then runs applicable deterministic
contracts, real embedded tests, doctests, Clippy and rustdoc. Disabled native
targets are not credited as executed tests. The full matrix runs Lance-only,
five singleton alternatives, and the two maximal compatible unions:
`redb,postgres,surrealdb,sqlite` and `redb,postgres,surrealdb,qdrant`. It separately
compiles all 24 legal subsets and verifies rejection of the eight illegal
SQLite/Qdrant unions. Cargo commands are serialized and use `--locked`.

`database_backend` includes real root and namespaced memory collections, scalar
indexes, clone retention, and independent-connection isolation.
`lancedb_session_ownership` separately exercises native directory session
release. Its shared-session case is an explicit pinned-engine counterexample;
the separate-session case must release both owners before any diagnostic
cleanup. Both targets execute in every supported check lane and the default
strict-live verifier. Native ownership evidence does not replace public
catalog/data/index and shared-directory coexistence tests.

### Embedded Lance StateStore

The three Lance targets execute real local engines, with no Storage
`--all-features` lane. Native feasibility probes are explicitly separate from
supplied-handle evidence. `lancedb_state_store` exercises the public handles and
owns ordinary child-process tests for independent writers, acknowledged commit
followed by abrupt termination, and fresh reopen. It retains transaction-drop,
unpolled-future, owner-lifetime, revision, corruption, and workload coverage.
Deep JSON checks run in supervised children so a codec regression fails its
test without aborting the parent suite. Assert commit acknowledgment, fresh
reads/listing, unrelated progress, reopen where applicable, and joined cleanup.
The implementation must not pass by adding a nesting limit or increasing a
worker stack. Cancelled read/list replies must retain private value owners until
synchronous public conversion, and malformed or replaced values must also be
disposed without recursive traversal.

Run `bash storage-sdk/scripts/verify_lancedb_state_store.sh NEW_EVIDENCE_DIRECTORY`
for the three ordinary embedded test targets in the no-default-features lane,
run serially. The verifier requires macOS or Linux, Python 3 for supervision,
Rust 1.99.0, and native engine build prerequisites. Select the compiler with
`RUSTUP_TOOLCHAIN=1.99.0`. Use the existing low-memory build settings when needed:
`CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
CARGO_PROFILE_TEST_DEBUG=0`. No credentials or external database service are
needed.

Each target receives its own complete log and command record. `summary.json`
contains the compiler, platform, temporary-dataset filesystem observation,
lockfile SHA-256, before/after source manifests and digests, command arguments,
exit codes, elapsed times, final harness counts, and actual errors. Source
identity covers root Cargo/toolchain configuration, workspace member manifests,
and Storage source, tests, and scripts, including untracked files. The evidence
directory itself is excluded. This receipt identifies this scoped execution;
a release candidate also needs the separate clean-checkout and consumer gates.
Unavailable filesystem metadata is explicitly marked unavailable, so that
receipt cannot establish a particular filesystem's coverage.

`PASS` means all three targets completed with nonempty passing suites and no
ignored or filtered cases, on unchanged source inputs. `FAIL` preserves a
failed command's exit code and log and stops before the next target.
`INCOMPLETE` exits 2 for missing prerequisites, the wrong compiler, interruption,
missing/empty harness counts, incomplete case selection, or changed source.
A run never overwrites an existing evidence directory. The verifier owns every
test command's process group, including interruption during process creation:
it acquires the child handle before unwinding and settles the group before
writing the terminal receipt. Cleanup errors remain alongside an earlier
interruption, launch error, or test failure. These
are execution results, not a hardcoded aggregate product verdict.

The finite commit-observation workload accepts either success or an actual
observation timeout. It requires at least one committed operation to settle and
pass independent atomic receipt/value verification after final-owner joining;
all attempts timing out during setup is a failure. Actual commit-timeout
occurrence is reported separately and may use a source-bound receipt with
unchanged production/dependency inputs. Cancellation, runtime loss and bootstrap
timeouts do not substitute for that evidence. Faster execution alone must not
fail this correctness test or manufacture a timeout claim.

The optional
`scripts/lancedb-linux/Dockerfile` provides ordinary Linux build dependencies
with Rust 1.99.0; it requires no additional container capabilities or relaxed
security settings. Keep test datasets and build output on the native Linux
filesystem when recording Linux filesystem evidence.
If the ordinary runtime is inaccessible, record Linux execution as unavailable;
do not relax container security to obtain that lane.

The user explicitly withdrew the privileged fault campaign. GDB/MI, ptrace,
syscall interception/modification, and equivalent workarounds are outside this
embedded StateStore's active validation scope. Its controller, campaign, native fixture,
and dedicated fault-child target have been removed. Earlier receipts remain
historical evidence only. Ordinary tests do not establish cancellation at a
selected native I/O boundary or every combined publication/cleanup failure.
Do not infer those results from a passing local test or a historical harness
control. Accepted design reasoning, native probe results, supplied-adapter
execution, and platform coverage remain separately identified in release evidence.

Use supported application-level native ObjectStore hooks for engine outcome
probes: distinguish publication followed by an error, an exact-version winner
with another transaction UUID, and unavailable readback after publication.
These probes cannot establish the supplied wrapper's combined-error branches.
For public-handle cancellation use observable ordinary activity and owned
processes; sleeping for an assumed internal publication boundary is not proof
of admission. Preserve primary errors when cleanup also fails, and keep
unexercised branches explicit in the contract matrix. Missing performance-only
RSS or process-I/O measurements are unavailable metrics, never fabricated values
or reasons to skip a behavioral assertion.

The create-only preparation probes use real native writes, an intentional
repeated transaction UUID, physical winner-byte readback and subsequent native
progress. Their test-local role selection does not exercise the supplied
wrapper's exact private-table classifier, barrier ordering or private UUID
generation. Supplied-adapter actor, maintenance and process-recovery results
require separate receipts. Injected barrier errors are not actual OS `fsync`
failures, and process termination is not hardware power-loss testing.

Maintenance directory-sync batching reuses these probes and existing supplied
StateStore maintenance, reference-retention, obstruction, process-recovery and
lifecycle cases. The private 64-descriptor bound and phase/barrier choreography
require independent source review; test-local native probes must not duplicate
the batching algorithm or claim direct private-adapter fault injection. Pair
baseline/candidate complete costs, including subsequent reads and joined cleanup,
using identical existing instrumentation. The four existing App diagnostic
calibrators must pass on both frozen executables outside timed samples. Faster
prefixes do not replace the unchanged capacity screening and full-workload gate.

The embedded maintenance recovery cases include recent native orphans newer
than the oldest retained manifest, future-dated native files during full GC,
and unsupported metadata in retained history.
They exercise cold public connection, preserve committed history and exact
record revisions, and require subsequent progress. Requalify the reference
collector and pinned filename rules when changing native dependencies or table
format. The current directory-backed orphan-only regression also preserves fresh,
aged and nested unknown payload names, an unsupported index payload and a
temporary-manifest sentinel while removing recognized native orphans. The same
reclamation-only listing boundary serves full cleanup; that shared-path source
review is distinct from the exact regression coverage. Files matching supported
native names remain eligible when unreferenced; empty caller directories are not
promised. See the [Lance 13 integration evidence](../../docs/pr20-pr21-integration.md#evidence-and-execution-boundaries).
The [earlier orphan reclamation source proof](lancedb-state-store-prototype.md#orphan-reclamation-correction-and-source-proof)
is historical evidence for its recorded implementation.

The [evidence ledger](lancedb-state-store-prototype.md) records commands,
red/green evidence, platform and feature-lane coverage, consumer and packaged-build
results, measured capacity, and limitations. Historical results apply only to
their recorded candidates.
Run both platform lanes, all existing feature lanes, focused real Lance
Database regressions, real Agent/App persistence paths, measured capacity cases,
and a locked clean-checkout proof for each release candidate. Package
and downstream smoke checks must use the packaged candidate, retaining its
locked dependency versions and excluding repository-only vendor archives.
Process-crash, synchronization-order, and actual machine/power-loss evidence
remain separately labeled. This focused verifier does not replace or relax
the broader strict-live service verifier.

### Reproducing an ordinary Linux candidate

The Dockerfile installs Rust 1.99.0 plus its matching rustfmt/Clippy components
and ordinary native build tools. The base Rust image uses a minimal profile;
its compiler alone is insufficient for `check.sh`. Use the normal container
security defaults. Keep Cargo invocations serial across host and container.

Freeze source edits before export. The candidate must include current tracked
**and untracked** implementation/tests; an archive of `HEAD` alone omits part
of this work. One reproducible export from the workspace root is:

```bash
LANCE_EVIDENCE="$(mktemp -d "${TMPDIR:-/tmp}/lance-release.XXXXXXXX")"
git ls-files --cached --others --exclude-standard -z -- . \
  ':(exclude)tmp/**' > "$LANCE_EVIDENCE/inventory.nul"
while IFS= read -r -d '' candidate_path; do
  if [[ -f "$candidate_path" || -L "$candidate_path" ]]; then
    printf '%s\0' "$candidate_path"
  fi
done < "$LANCE_EVIDENCE/inventory.nul" > "$LANCE_EVIDENCE/source-files.nul"
tar -czf "$LANCE_EVIDENCE/source.tar.gz" --null \
  -T "$LANCE_EVIDENCE/source-files.nul"
git rev-parse HEAD > "$LANCE_EVIDENCE/git-head.txt"
(cd "$LANCE_EVIDENCE" && shasum -a 256 source.tar.gz > source.sha256)
```

This exports working-tree file contents, omits deleted files and immutable
historical `tmp/**`, and preserves the included path list plus archive hash.
Keep the source frozen through export and verify the archive hash before
extraction. The release receipt may additionally record per-file hashes and
verify the extracted inventory, as the disposable execution fixture does.
The scoped verifier records its actual before/after compiled source inputs.
Use a fresh owned volume and refuse a previous or partial source extraction:

```bash
LANCE_VOLUME="ai-kit-${LANCE_EVIDENCE##*/}"
docker build --tag ai-kit-lance-release:1.99.0 \
  --file storage-sdk/scripts/lancedb-linux/Dockerfile storage-sdk/scripts/lancedb-linux
docker volume create "$LANCE_VOLUME"
docker run --name "$LANCE_VOLUME" \
  --mount "type=volume,source=$LANCE_VOLUME,target=/workspace" \
  --mount "type=bind,source=$LANCE_EVIDENCE,target=/evidence" \
  ai-kit-lance-release:1.99.0 bash -euo pipefail -c '
    test ! -e /workspace/source && test ! -L /workspace/source
    cd /evidence
    sha256sum --check source.sha256
    mkdir /workspace/source /workspace/test-tmp
    tar -xzf /evidence/source.tar.gz -C /workspace/source
    cd /workspace/source
    export TMPDIR=/workspace/test-tmp CARGO_TARGET_DIR=/workspace/target
    export RUST_TEST_THREADS=1
    export TEST_LANCEDB_EMBEDDED_PATH=/workspace/test-tmp/lancedb-live
    rustc -vV > /evidence/linux-compiler.txt
    uname -a > /evidence/linux-platform.txt
    stat -f -c "%T" "$TMPDIR" > /evidence/linux-filesystem.txt
    cat /proc/self/mountinfo > /evidence/linux-mountinfo.txt
    df -P "$TMPDIR" > /evidence/linux-capacity.txt
    bash storage-sdk/scripts/verify_lancedb_state_store.sh /evidence/embedded
    bash storage-sdk/scripts/check.sh > /evidence/storage-lanes.log 2>&1
    cargo test --locked -p storage-sdk --no-default-features --test strict_live \
      lancedb_ -- --nocapture --test-threads=1 > /evidence/lance-database.log 2>&1
    sed -n "/^test result: /p" /evidence/lance-database.log | tail -n 1 | \
      grep -Eq "^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out;"
    cargo test --locked -p storage-sdk --no-default-features \
      --features serde_json/preserve_order --test lancedb_state_store \
      -- --nocapture --test-threads=1 > /evidence/preserve-order.log 2>&1
    sed -n "/^test result: /p" /evidence/preserve-order.log | tail -n 1 | \
      grep -Eq "^test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;"
  '
```

The `lancedb_` filter includes both the shared Database cases and the three
top-level real embedded Lance cases. It needs no unrelated service credentials.
The final harness summary must contain at least one pass and zero failures,
ignored or measured tests; intentional filtering is allowed only for this
Database selection. The `preserve_order` lane runs the complete StateStore
target, including disposal/cache cases without `json` in their names, and also
requires zero filtered tests. A zero-match Cargo success is never evidence.
The checks use the last summary so an earlier child pass cannot mask an empty
or failed parent suite. Record the mount covering the actual dataset directory; Docker's image storage
driver is not that evidence. This lane tests embedded behavior and Storage's
selected feature lanes; it does not run the remote-service strict suite. Record
Agent/App consumer and capacity runs separately. Retain the owned container,
volume and logs until results are collected, then remove only those named
resources. Cleanup failures must not replace an earlier command failure.

Run the affected, fully embedded consumer selections separately inside that
same ordinary Linux environment, with the same captured source, compiler,
absolute target and dataset paths. These are reproduction commands, not
executed Linux receipts:

```bash
cargo test --locked -p agent-sdk --test custom_prompt_runtime \
  --test codex_prompt_stream lance_state_ -- --nocapture --test-threads=1
cargo test --locked -p app-sdk --test deletion_contract --test prepare_contract \
  --test workspace_contract lance_state_ -- --nocapture --test-threads=1
```

The current selection contains four Custom and two Codex Agent fixture cases,
and four App deletion cases, one App recovery case covering four process-loss
boundaries, and two App workspace cases. These targets use ordinary local
fixtures and embedded stores; they require no native CLI, model, remote service
or extracted vendor source. For each selected target, retain a nonempty final
parent summary with zero failed/ignored/measured cases, allowing the intentional
`lance_state_` filtering. Count the recovery case once, not once per child or
boundary. Empty selections or a passing child summary do not qualify a target.
These selections supplement the full consumer gates; they do not replace them.

The exported source omits `.git`. Run the no-path/no-alias guards in the real
current Git checkout and retain their receipts separately. Those scripts use
`git ls-files`; an archive invocation inspecting zero files is not a pass.
A fresh export/build target establishes the clean-candidate build inputs,
including untracked files, without claiming they were committed to Git.

Agent's separate source-contract gates read pinned native protocol source. A
clean checkout/export contains its tracked ZIP archives and `vendor-lock.toml`, while
`**/vendor/*/` extraction directories are ignored. Before running those Agent
source-contract gates, materialize the **currently locked** Codex/OpenCode
archives with the existing extraction owner and verify their complete file contents. This is a
local prerequisite, not a vendor update, download or new version selection.
Run from the source root; existing extraction directories are verified rather
than overwritten:

```bash
python3 - <<'PY'
from pathlib import Path, PurePosixPath
import hashlib
import tomllib
import zipfile
from scripts.vendor_update import VendorUpdater

root = Path.cwd()
vendor = root / "agent-sdk/vendor"
locked = tomllib.loads((vendor / "vendor-lock.toml").read_text())["vendors"]
updater = VendorUpdater(root)
for name in ("codex", "opencode"):
    selected = locked[name]
    archive = vendor / selected["archive"]
    destination = vendor / selected["extracted_dir"]
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == selected["sha256"]
    if not destination.exists():
        updater._extract_archive_to_path(archive, destination)
    with zipfile.ZipFile(archive) as source:
        entries = [item for item in source.infolist() if not item.is_dir()
                   and not updater._is_archive_metadata_path(PurePosixPath(item.filename))]
        assert len({PurePosixPath(item.filename).parts[0] for item in entries}) == 1
        expected = {"/".join(PurePosixPath(item.filename).parts[1:]): item for item in entries}
        actual = {str(path.relative_to(destination)) for path in destination.rglob("*") if path.is_file()}
        assert actual == set(expected)
        for relative, item in expected.items():
            assert (destination / relative).read_bytes() == source.read(item)
        print(name, selected["tag"], len(expected), "files verified")
PY
```

Do not run the vendor update workflow to satisfy this prerequisite. Keep the
selected tags, lock hashes and archived bytes unchanged, and retain any initial
missing-extraction test failure separately from the subsequent full-gate result.

### Packaging and downstream composition

Use the complete final manifest/lockfile and retain the exact package listing,
archive and logs. `vendor/**` is repository research material and must be absent
from the package. Resolve the shared target directory to an absolute path;
an external consumer manifest otherwise creates its own large target cache.

```bash
export RUSTUP_TOOLCHAIN=1.99.0 CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(pwd -P)/target}"
CARGO_TARGET_DIR="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$CARGO_TARGET_DIR")"
export CARGO_TARGET_DIR
cargo package --locked -p storage-sdk --no-default-features --allow-dirty --list \
  > "$LANCE_EVIDENCE/package-list.txt"
cargo package --locked -p storage-sdk --no-default-features --allow-dirty \
  > "$LANCE_EVIDENCE/package-verification.log" 2>&1
mkdir "$LANCE_EVIDENCE/package"
tar -xf "$CARGO_TARGET_DIR/package/storage-sdk-0.1.0.crate" \
  -C "$LANCE_EVIDENCE/package"
```

Plain `cargo package` performs packaged-build verification; `--no-verify` does
not qualify it. Then run a standalone consumer with its own `[workspace]`, a
path dependency on the **extracted package**, `default-features = false`, and
an existing-version-only lock seed. Use `cargo run --locked --manifest-path
<consumer>/Cargo.toml -- <new-owned-data-directory>`; retain the consumer source
and lockfile with its receipt. Any stale seed must be reconciled explicitly
against existing locked identities, never by permitting version upgrades.

Package and downstream passes qualify their recorded locked dependency graph.
They do not establish fresh unlocked resolution or every version permitted by
the manifest. Preserve Cargo's actual yanked-version warnings in the receipt:
existing lockfiles can retain yanked versions, while new resolution excludes
them under [Cargo's documented policy](https://doc.rust-lang.org/cargo/commands/cargo-yank.html).
The [historical prototype ledger](lancedb-state-store-prototype.md) records four
warnings for its original candidate; it is not evidence for the current graph.
A dependency upgrade requires its own graph review and validation; never suppress
warnings or claim historical package receipts validate a changed graph.

The approved macOS/Qdrant browser constraint and its limits are owned by
[the Storage dependency policy](../AGENTS.md#backend-and-secret-ownership).
Validate the published manifest and independently resolved extracted consumer,
not only the workspace lock: shared-lock selection can affect other lanes, and
another dependency can still enable the conflicting `objc2` features. An
isolated compiler control does not qualify the candidate graph or native
behavior; retain the actual target, features, lock and result with each receipt.

The downstream smoke must exercise public memory/directory constructors,
clones, two independent connections, winning/losing atomic receipts observed
through one transaction, tombstone revision continuity after reopen, and
same-directory Database knowledge/catalog reservation. It must also compile
and exercise a concrete caller `StateStoreBackend` with its associated
`StateTransactionBackend`, without a supplied-handle normalization factory.
Use supported `DataRecord::without_id()` for Lance knowledge rows. This small
consumer supplements the full contract tests; it is not a replacement for
strict-live or platform gates.

## Strict-Live

```bash
# No service credentials or Docker required:
bash storage-sdk/scripts/verify_strict_live.sh
# Add coverage selected from committed and working-tree changes:
bash storage-sdk/scripts/verify_strict_live.sh --changed-since BASE
# Source only the selected alternative prerequisites:
bash storage-sdk/scripts/verify_strict_live.sh --backend postgres
bash storage-sdk/scripts/verify_strict_live.sh --all-backends
```

Lance always runs, including the supplied StateStore, native/durability targets,
and real Database strict cases. If `TEST_LANCEDB_EMBEDDED_PATH` is absent, the
verifier owns a fresh temporary path. Backend changes add their owner; shared
StateStore/Database changes add that product's implementations. Shared dependency
changes compare both base and candidate closures. Unrelated workspace and
documentation-only changes add no remote coverage. Invalid selection fails.

Selected prerequisites are checked before deterministic gates and service
mutation. SurrealDB/Qdrant/PostgreSQL use a fresh invocation-owned Compose
project; caller projects are untouched. Only selected services start, and exit
cleanup removes their volumes. Port defaults remain 8000, 6333 and 15432; choose
unused ports and matching IPv4-loopback endpoints for concurrent runs. Endpoint
mismatches fail before service mutation. No reset of a caller's
project is performed. Startup failure still triggers owned cleanup; cleanup
failure cannot erase the primary failure or produce success.

Readiness uses 60 attempts, each with 2-second connect and 10-second transfer
limits, then 1-second retry delay. Owned service readiness processes have a
15-second outer deadline; Compose startup and cleanup each have a 120-second
execution deadline. Every owned command has a 30-second graceful group-cleanup
bound before termination is required and reported as failure. These are per-attempt bounds, not a one-minute
whole-run deadline. Test-local command doubles and real loopback stall tests
prove orchestration only. Real selected engines prove backend behavior.

SurrealDB readiness uses `/ready`. After readiness, the owned service version
commands and Compose image identities are recorded before live execution.
Every feature-selected live lane receives a new owned child directory below
each selected embedded root. Caller paths and data are never used as disposable
lane directories; allocated children are removed only after owned work settles.

The verifier records selection reasons and separate inventories/logs. Every live
invocation must execute its complete nonempty feature-selected test inventory;
missing, ignored, filtered or failed cases prevent success. Selecting SQLite and
Qdrant schedules separate legal invocations. Alternative local StateStores use
real embedded tests rather than fictional remote services.

Downstream actual persistence uses Lance. Agent/App backend-specific storage
prerequisites are removed; their native runtime/provider prerequisites remain.
Feature-split qualification runs Storage's full backend scope and only changed
downstream live targets, not unrelated Provider/Tool or workspace live gates.

The PostgreSQL collation case requires the pinned PostgreSQL 18 service with
ICU support and a test role permitted to create databases. It creates one
UUID-named database from `template0` with the `en-US` ICU locale, confirms the
database's locale provider, and runs the portable pagination case through public
StateStore calls. The test removes that database after success, assertion
failure, or its bounded observation timeout. Setup failure is not a substitute
for the ordering regression result. No additional environment keys are needed.
These prerequisites follow PostgreSQL's [database creation contract](https://www.postgresql.org/docs/18/sql-createdatabase.html),
[locale catalog](https://www.postgresql.org/docs/18/catalog-pg-database.html),
and [database cleanup contract](https://www.postgresql.org/docs/18/sql-dropdatabase.html).

The PostgreSQL cleanup-receipt pair uses a transparent loopback relay to the
configured real service. A real SERIALIZABLE locking-read conflict is followed
by the server's actual rollback completion. One control forwards the receipt;
the other cuts the same connection before forwarding it. Both preserve the
original operation conflict and check the winner and absent losing write.
The latter proves lost cleanup acknowledgment, not failed native rollback or
surviving mutations. Consuming public rollback must retain that uncertainty.
Each case joins the relay connections and observer, confirms its native backend
sessions ended, and removes only its UUID namespace before asserting outcomes.
The public error enum has no secondary-error field; these controls observe the
nonterminal primary and subsequent rollback result separately. They do not
claim additional cleanup diagnostics for already-consumed begin/commit failures.

Strict-live uses no mocks, recordings, scripted servers, or synthetic
substitutes. Every wait is bounded. Test-owned state is reset only within
declared temporary paths or crate-owned Docker volumes, and every task/thread
is settled before a case returns.

### Complete strict target commands

Against already-running configured services, run both commands serially:

```bash
cargo test --locked -p storage-sdk --test strict_live -- --nocapture --test-threads=1
cargo test --locked -p storage-sdk --test strict_live --no-default-features --features qdrant -- --nocapture --test-threads=1
```

Their combined inventory includes Lance memory/filesystem; Surreal memory,
RocksDB, SurrealKV, HTTP and WebSocket; Qdrant Edge/REST; Redb memory/file;
SQLite/Turso memory/file; and PostgreSQL. The SQLite/Qdrant dependency conflict
requires separate lanes; it never permits dropping a supported mode.

### Retained Database operation cases

The strict target's private database_cases module calls only DatabaseBackend
operations on the shipped Database. It exercises real Lance memory/filesystem, Surreal memory,
RocksDB, SurrealKV, HTTP and WebSocket modes, plus Qdrant Edge and REST in the
separate Qdrant feature lane. Embedded cases own fresh temporary directories;
remote cases use the configured service and UUID-named collections. Each
operation case has its own connection and collection, a bounded observation,
and cleanup after success, assertion failure, or timeout. A grouped test records
each failure and continues its other cases, so one failing operation cannot
hide unrelated evidence.
The case runner heap-pins each concrete operation future inside its existing
timeout to reduce the outer runner's debug-build stack pressure. This resolved
the observed LanceDB test-harness stack overflow on the selected build. Inner
polling frames and native parser calls still use the same runtime thread; the
result does not establish remaining stack headroom or a general production
stack guarantee. Observation, panic propagation, cleanup, and runtime ownership
remain unchanged.

The catalog_data_schema tests cover collection create/reuse/overwrite,
open/list/drop, append/scan/count/update/delete, logical-ID lookup, default
ID-based merge, atomic partition replacement and schema add/alter/drop. The
query_index tests cover all five QueryRequest variants, scalar/full-text/
vector index lifecycle, invalid index requests, and nonempty/empty data overwrite. Supported
operations assert actual returned or persisted contents. Optional counts,
readiness and scores are not manufactured or required when absent. Unsupported
operations assert UnsupportedCapability and compare public records, catalog
and supported index snapshots to prove no observable mutation. The supplied
Qdrant Edge adapter's index inventory explicitly returns UnsupportedCapability;
the tests retain that checked absence, not a fabricated empty inventory.

Partition replacement includes both successful replacement/clearing and a
separate cross-partition collision case: a globally unique payload identity
already owned outside the selected partition must be rejected without changing
either partition, including a NULL value in the outside partition field. Schema evolution checks persisted values after column removal;
logical-ID lookup checks actual matching records and omission of missing IDs.
Unfiltered update has a separate fixture, so a failure cannot hide filtered
update or deletion evidence. Nearest-query offset likewise has its own fixture
to keep pagination failures separate from basic search and parameter validation.

The individually filterable Qdrant regressions run both real Edge and REST:

- `qdrant_lookup_missing_ids_and_projections` checks exact identities, full
  payloads, both named vectors, payload-only and selected-vector projections,
  interspersed missing IDs, empty input and all-missing input.
- `qdrant_named_vectors_and_search_by_id_source_exclusion` checks distinct
  rankings through default and nondefault vector names, projected vector
  selection, and exact SearchById results when the caller filter admits the
  source, excludes it, is absent, or matches nothing.
- `qdrant_nearest_offset_exact_cardinality` requires three ordered results for
  four candidates with offset one, plus zero/beyond-population offsets and a
  selective filter with enough candidates. The grouped Qdrant query cases use
  this same stronger oracle.
- `qdrant_uuid_pagination_with_initial_offset` traverses shuffled UUID records
  through both Scan and Scroll, reusing tokens unchanged while retaining the
  initial numeric offset. Exact pages prove no duplication or gaps and no
  continuation on short or exactly full final pages.
- `qdrant_filtered_payload_and_index_lifecycle` checks equality, range and
  composed predicates through count, scan, nearest, update and delete, with
  complete unaffected data/vector comparisons and immediate visibility.
  REST retains its full-text payload index across mutations, then drops it and
  rejects readiness. Edge explicitly rejects create/list/drop and missing-index
  readiness without changing data or query results.
- `qdrant_retained_vector_representations` checks F32/F64/I16/I32/I64 inputs,
  normalized native f32 readback, and rejected lossy writes/queries.

`qdrant_edge_mutations_persist_after_reopen` is Edge-specific. It performs
insert/update/delete, drops all owners, reopens and checks catalog, count,
lookup, scan, projections, default/nondefault named-vector queries and rejected
wrong-dimension writes to both named fields, with unchanged records. A second
mutation and reopen prove continued usability. This is
normal close/reopen evidence, not crash durability or a receipt for successful
native Drop flushing. Each case retains bounded observation and owned cleanup.

For focused vendor iteration against an already-provisioned service, source the
configured environment and run serially with `CARGO_INCREMENTAL=0` and
`CARGO_BUILD_JOBS=1`:

```bash
source storage-sdk/strict-tests.env
cargo test --locked -p storage-sdk --no-default-features --features qdrant --test strict_live qdrant -- --nocapture --test-threads=1
cargo test --locked -p storage-sdk --no-default-features --features qdrant --test qdrant_rest_completion -- --nocapture
cargo test --locked -p storage-sdk --no-default-features --features qdrant --test public_api --test database_backend --test source_contract --test strict_env_contract -- --nocapture
cargo test --locked -p storage-sdk --test public_api --test source_contract -- --nocapture
cargo test --locked -p storage-sdk --no-default-features --test public_api --test source_contract -- --nocapture
```

These focused checks do not replace any completion gate or the complete
unfiltered strict-live lanes. SQLite and Qdrant remain separate feature lanes.

| Operation | Lance | Surreal modes | Qdrant REST | Qdrant Edge |
| --- | --- | --- | --- | --- |
| Logical-ID lookup / search by ID | Unsupported | Supported | Supported | Supported |
| Default ID merge | Unsupported | Supported | Supported | Supported |
| Atomic partition replacement | Supported | Supported | Unsupported | Unsupported |
| Schema evolution | Supported | Supported with explicit restrictions | Unsupported | Unsupported |
| Nearest / scroll | Supported | Supported | Supported | Supported |
| Text query | Supported | Supported | Unsupported | Unsupported |
| Random sample | Unsupported | Supported | Supported | Unsupported |
| Scalar index creation | Supported | Supported | Unsupported | Unsupported |
| Full-text index | Supported | Supported | Supported | Unsupported |
| Vector index creation | Supported | Supported | Unsupported | Unsupported |
| Index inventory | Supported | Supported | Supported | Unsupported |
| Index drop | Supported | Supported | Supported | Unsupported |

Lance and Surreal reject present computed-column read_columns options,
including empty lists, without changing rows, schema, count, or index inventory.
Surreal also rejects nullable-only alteration; mixed supported/unsupported
alteration must leave earlier fields untouched.
Lance rejects a cast combined with actually tightening nullability without
applying an earlier rename, then proves rename/cast, separate nullability
change, and drop through supported requests. A separate case preserves support
for casts on already non-nullable columns, including complete rows/schema and
unrelated index inventory. Concurrent creation/reuse remains exercised at
root and child namespace/database scopes.
Surreal and Qdrant reject non-ID merge keys and custom merge actions. Qdrant's
full-text payload index does not imply a portable Text query, and its vector
index is configured by collection creation. Scalar index creation does not
guess a Qdrant native payload field type. Edge's rejected index creation is
followed by a missing-index wait failure and explicit unsupported drop. No
capability bit skips a case.

The Lance SQL regressions check row-dependent eager backfill immediately after
completion in memory and filesystem modes, and persisted values and schema
after dropping every database owner and reconnecting. Rejected-option
preservation is also checked across filesystem reopening. Quoted String
fields with spaces and double quotes are filtered through scan,
count, update, and delete using literal values containing apostrophes, quotes,
backticks, backslashes, and SQL-looking text. These cases compare complete
records, preserve control rows and schema, and require exact native mutation
counts. A separate partition case checks one inserted, one updated, and one
deleted identity, preserved unrelated records/schema/indexes, and empty
replacement clearing only the selected apostrophe-containing partition value.

The Lance deleted-row scan regressions create rows `0..8`, append `8..16`,
`16..24`, and `24..32` separately, then delete
`{0,3,7,8,10,15,16,17,18,19,20,21,22,23,24,28,31}` through public operations.
They require exactly the 15 survivors
`{1,2,4,5,6,9,11,12,13,14,25,26,27,29,30}`, including complete vectors and
payloads, exact public count, and exact supplied mutation counts. An unlimited,
unfiltered scan is independently checked against that set before its observed
sequence becomes the same-state offset oracle. Limit-four scans cover offsets
`0,4,5,9,10,14,15,40`; token traversals use page sizes four and five from initial
offsets zero and two, retaining that numeric offset alongside unchanged tokens.
Each traversal has a 16-request bound and checks exact page cardinalities,
complete suffix contents, and duplicate-free markers. A full final page may
lead to one empty terminal page. Named `marker`/`number` projection excludes all
other payload and vectors; a filtered traversal supplies a separate control.
The independently selectable `lancedb_scan_deleted_rows_memory` and
`lancedb_scan_deleted_rows_filesystem_reopen` cases share these assertions.
The latter drops every database owner before reopening and establishes a fresh
reference sequence against the same independent survivor set. This proves
ordinary persistence, not crash durability, cursor survival across mutations,
insertion order, or identical ordering across reopen. No compaction, index,
native fragment inspection, or storage-version override shapes the fixture.

Individually filterable regression cases also cover caller __distance payload
preservation in Surreal vector results, caller __score preservation in text and
unscored queries, overwrite removing unmatched old rows,
Lance and Qdrant rejecting unsupported overwrite seeds before destroying the
original collection, and full-text creation/query in a fresh Surreal memory
database.
The Qdrant cases independently use an unnamed dense vector and a string logical
ID, which the supplied adapter explicitly rejects, plus inconsistent dimensions;
all preserve the original full-text index in REST mode. Surreal rejected catalog
overwrite cases check reserved payload IDs and invalid schema names/types.
Qdrant REST also tests its pinned 65536-dimensional upper boundary and verifies
that a rejected larger replacement preserves prior data and indexes. Empty
Append/Merge identity controls preserve existing data/schema/indexes and reject
missing collections. Lance JSON controls cover inferred and explicitly declared
JSON columns, including distinct scalar strings, JSON null and absent Arrow cells.
Independent scalar-only append, update and equality controls distinguish JSON field
encoding from ordinary String columns without native schema access.
Fresh full-text cases use no native analyzer setup or privileged workaround.
A compile or setup failure is not a regression result.

For an explicitly scoped regression pass, run serial commands such as:

    cargo test --locked -p storage-sdk --no-default-features --features surrealdb --test strict_live database_cases::surrealdb_vector_payload_score_collision -- --exact --nocapture --test-threads=1
    cargo test --locked -p storage-sdk --no-default-features --features surrealdb --test strict_live database_cases::surrealdb_overwrite_replaces_collection_contents -- --exact --nocapture --test-threads=1
    cargo test --locked -p storage-sdk --no-default-features --test strict_live database_cases::lancedb_unsupported_overwrite_preserves_collection -- --exact --nocapture --test-threads=1
    cargo test --locked -p storage-sdk --no-default-features --features surrealdb --test strict_live database_cases::surrealdb_fresh_fulltext_index_and_query -- --exact --nocapture --test-threads=1
    cargo test --locked -p storage-sdk --no-default-features --features surrealdb --test strict_live database_cases::surrealdb_memory_catalog_data_schema -- --exact --nocapture --test-threads=1
    cargo test --locked -p storage-sdk --no-default-features --features qdrant --test strict_live database_cases::qdrant_overwrite_replaces_collection_contents -- --exact --nocapture --test-threads=1

After sourcing the configured environment, remote cases can be selected using
the database_cases::surrealdb_remote_http_ or
database_cases::surrealdb_remote_websocket_ prefix, or
database_cases::qdrant_remote_rest_ in the separate Qdrant lane. These targeted
commands are focused evidence, not replacements for the full release gates.
Public-operation oracles follow the portable DTO contract and native
[Lance vector search](https://docs.lancedb.com/search/vector-search/),
[Surreal vector distance](https://surrealdb.com/docs/reference/query-language/functions/database-functions/vector),
and [Qdrant query](https://qdrant.tech/documentation/concepts/search/) semantics;
the locked native versions remain unchanged.

### Native wire and mutation ownership regressions

`surreal_partition_deadline` uses the pinned native public FlatBuffers codec to
exercise exact conflict classification and the original operation deadline for
partition replacement and collection removal. Collection cases cover retry
exhaustion including zero retries, context selection, query waits, and backoff
inside one deadline. Controls preserve mixed primary errors and reject
not-executed-only responses, conflict-shaped untyped messages, indeterminate
commits, and accepted requests whose transport response is lost.
`qdrant_rest_completion` exercises data and index mutations and replacement wire requests:
applied-completion requests, rejection of queued acknowledgements, completed-result
controls, upsert-before-prune ordering, no pruning after an unconfirmed upsert,
and the original portable operation budget spanning preflight and mutation.
Index cases also reject absent, malformed and unknown completion statuses,
preserve bodyless deletion, and require a single mutation without replay.
Readiness cases enforce the caller's deadline through stalled headers and bodies,
retain a shorter CommonConfig deadline, and cover ready, missing and invalid-input
outcomes. It runs 27 cases only in the Qdrant feature lane. These loopback fixtures are
normalization/deadline proofs outside strict-live, not real-service evidence.
Their threads are bounded and joined even when an assertion fails.

The Lance strict cases supervise separate test processes to verify observer
loss, last-owner drop, and bounded lock admission without requiring that a
pending first poll proves mutation admission. A reopened public snapshot may
show either the complete original or complete replacement state, but resuming
the old caller runtime must not cause a later mutation after final-owner drop.
Memory mode, independent database paths, and native failure followed by reuse
provide separate controls.

The `predicate_contract` Lance cases separately require exact Unsupported for
Json range/pattern Scan, Update and Delete, preserving records, schema and a
real scalar index. Ordinary String cases require exact matching records. Json
equality, scalar Append/Update and mixed-value roundtrips remain positive tests;
unsupported range/pattern behavior does not stand in for those supported paths.

## Regression Ownership

Keep focused cases for:

- non-Clone downstream `DatabaseBackend` retained operations with populated
  catalog/query DTOs, explicit score semantics and caller continuation tokens;
- non-Clone downstream `StateStoreBackend -> associated Transaction`, populated
  records/pages, actual present/absent read conflicts and atomic rollback;
- the [portable contract](PORTABLE_API.md): exact per-operation revisions,
  tombstone/recreate behavior, literal case-sensitive prefixes, UTF-8 key order,
  exact bounded pages and cursors (including no cursor on a final full page),
  exclusive advancing cursors and non-snapshot multi-page traversal;
- real PostgreSQL absent-read write skew, stale present-key snapshots, native
  row-lock timeout rollback, admitted commit after waiter loss, and canonical
  pagination under an explicitly non-binary database collation;
- Redb FIFO admission, queued drop, admitted rollback, and thread joins;
- PostgreSQL/SQLite actor finalization after caller loss;
- exact conflict and non-conflict error propagation;
- feature-disabled type absence;
- removed realtime, factory, conformance, builder, tool-adapter,
  `CollectionHandle`, identity, erasure, and backend-ingress symbols.

Tests should assert observable contracts rather than private filenames, helper
names, actor type names, or match-arm syntax.

### SurrealDB release regressions

The canonical `strict_live` target also exercises populated `sq_` collection
create/open/list/scan/drop behavior, exact filtered indexed-vector top-k and
search-by-ID (including seed exclusion and empty predicates), full-text term
removal after update and partition replacement/clearing, and concurrent first
use of the same collection name in three namespace/database scopes. Every case
runs memory, RocksDB, SurrealKV, HTTP, and WebSocket, aggregates mode failures,
and cleans all owned collections after failure or timeout. Scoped first use
shares one Database through public clones and settles all three concurrent
cold-scope creates. Successful responses must report the exact locator and
record count. Only the recorded single native `TransactionConflict` error
permits recovery: the caller must first observe the exact missing-table error
for its owned collection, then issue one serial strict `Create`. Every other
error and any retry failure fail the case. This proves isolation with explicit
caller recovery after observed absence, without promising conflict-free
creation or SDK write replay. Repeated cached reads, native IDs, update/drop
isolation, and the unchanged default-context sentinel remain required.
Full-text mutations preserve
unrelated records and the existing index inventory without rebuilding indexes.
Existing focused Surreal score-payload, full-text setup, overwrite, and
rejected-overwrite regressions also run all five modes. The repeated full-text
collection-removal case runs 16 create/index/query/immediate-drop cycles per
mode, alternating `ignore_missing`. It proves the collection is absent, requires
an error for a second strict drop, and requires completion for an absent-table
drop with `ignore_missing: true`. It adds no sleeps, readiness workaround, or
caller retry around removal; usable full-text indexes can still have native
compaction transactions in flight.
Nearest-query validation also runs all five modes: empty projection precedes
missing indexed `ef` or incompatible distance/`ef`, while unsupported
`fast_search` still returns `VECTOR_SEARCH_PARAMS` before projection rejection.
Vector fixtures use shuffled physical IDs so identity order cannot substitute
for distance ranking; the seed itself matches the caller predicate and must
still be excluded before indexed top-k selection.

SurrealDB 3.3.0's versioned
[debug-test configuration](https://github.com/surrealdb/surrealdb/blob/v3.3.0/.cargo/config.toml)
documents an 8 MiB stack for deep native async query execution in unoptimised
builds. The shared Surreal database cases own and join a thread with that stack and run
their current-thread Tokio runtime there. This scopes the native debug-test
requirement to the fixtures, without a process-wide environment override or
changed verifier limits. Assertion failures propagate after each case's cleanup
and the owning thread is joined before the test returns.

The deterministic `surreal_partition_deadline` peer retains its `/health`
route because the native Rust SDK's HTTP connection probes that endpoint. Its
synthetic version response tracks 3.3.0. Its fourteen deadline/conflict cases and
the verifier process fixtures are protocol/orchestration evidence only; real native
behavior requires the strict-live engines and service. Focused vendor runs do
not replace any complete feature lane or blocking completion gate above.

### Lance 13 adapter reduction proof

Private wrapping and shared durability ownership changes use the existing
`lancedb_durability_feasibility` and `lancedb_state_store` owners, followed by
the default `verify_strict_live.sh` gate. Native injected-error probes establish
native feasibility; they do not establish supplied-adapter fault handling.
The public cases retain memory isolation, large-value cold reopen, ordinary
filesystem obstruction, retained-transaction ownership, admitted-commit
cancellation and unknown-file protection. Private unsupported-write rejection,
exact barrier ordering and combined cleanup-error precedence also require
actual-diff source review; no production fault hook is exposed for those checks.

Keep local downstream persistence/recovery proof separate from real-model
qualification. App's existing `check.sh` owns the four prepare cases and runs
the canonical crash case once; do not duplicate that case in a preliminary
selection. Preserve original receipt identities and platform scope when a later
private adapter changes. Scoped macOS proof is not Linux or full-workspace
qualification.
