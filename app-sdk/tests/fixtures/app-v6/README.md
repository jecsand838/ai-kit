# Cold App v6 migration fixtures

These four complete Lance directory backups were created by the frozen App v6
implementation through public App operations. They are immutable migration inputs,
not fabricated records or Agent strict-live evidence.

- `notification-history`: 96 delivered notifications, exact returned resolution
  receipts, 295 checkpoints, restore and explicit deletion of the restored run.
- `unknown-history`: one actual caller-Agent outcome failure, its historical
  `Unknown` checkpoint, explicit public failure resolution and joined cleanup.
- `sealed-deletion`: 130 actual actions, a committed 256-key deletion page and a
  controlled failure before the next page; 1,180 original history/index rows
  remain under the durable deletion cursor.
- `current-recovery`: historical v1 execution, a current v2 Pending Input with its
  original finite deadline, and a current retry-safe Unknown. The initial App
  joined; the reopened App was deliberately terminated after a durable witness.
  Its owned process group was reaped before the cold copy. This fixture captures inputs for
  process-loss recovery, not graceful shutdown of that current owner. Migration
  first checks both cold heads and checkpoints against the pre-loss witness.

Each `provenance.json` identifies the v6 source, Rust compiler, capacity profile,
executable and source-bound execution receipt. Its per-file SHA-256 inventory
covers the original `expected.json` and every byte in `state/`. The migration
fixture owner verifies those hashes and copies the complete cold store into a
fresh test-owned directory. All App and StateStore owners settle before that
copy is removed.

`expected.json` contains synthetic test data and public lower workspace and
conversation metadata. Persisted `/tmp` or `/private/tmp` roots are backend path
values authored by the original caller; tests do not rewrite them or create
external directories. The controlled caller Agent reconstructs those exact public
identities. Separate disk/process-recovery and real-Agent suites retain their
own evidence and prerequisites.

Do not regenerate these inputs using the current v7 implementation. An intentional
fixture replacement requires a source-bound v6 executable, actual public workload
execution, recorded ownership settlement and a fresh complete checksum inventory.
For a process-loss fixture, record process reaping separately from SDK joining;
never describe forced termination as graceful shutdown.
The original generation commands used the exact golden tests named in each
provenance file with `AI_KIT_APP_GOLDEN_OUTPUT` set to a fresh owned output directory.
