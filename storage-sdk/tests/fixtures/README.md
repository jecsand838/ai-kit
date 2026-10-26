# Native persistence fixture

`redb-4.1.0-state.redb` is a closed native Redb 4.1.0 database generated on
2026-10-05 through storage-sdk's public StateStore API. Tests copy it to an
owned temporary directory before opening or mutating it. Never regenerate it
with the currently selected dependency; it proves compatibility with 4.1.0.

SHA256: `dc1519690dcdec1c6e9f127525e18fc8afac67f591a70dbec25593b204b30a07`.

## Provenance

Generation used Rust 1.94.1 and the existing no-default Storage build whose
StateStore/Redb implementation matches commit `2ea2e582` (later source changes
were documentation-only). This is artifact provenance, not a fresh-checkout
validation claim. Its Cargo dep-info resolves Redb 4.1.0.

- Storage rlib SHA256: `fe317de9be3871240896a729e871b1655aab48f147f7a16c591acddc65cb31ba`
- Redb rlib SHA256: `7999fcbf234ce667eeb37dc9dd54247f30cd325cc9201d2818dafd4312e4c72c`

The retained generator and command evidence are in the ignored
`target/storage-vendor-campaign` directory. The generator was compiled with
`rustc +1.94.1 --edition=2024`, dependency search path `target/debug/deps`, and
explicit Storage `b1dbd6ca2b27ecc7`, Tokio `71b09749155a4211`, and serde_json
`730808f20d068bdd` rlibs. It ran once with this previously nonexistent fixture
path, consumed all transactions, and dropped the sole StateStore before exit.

## Seed recipe

All keys use namespace `vendor-upgrade`. One committed transaction performs:

- `records/a`: put 1, put 2, delete, put `{"text":"preserved","n":42}`; revision 4.
- `records/b`: put `{"n":2}`; revision 1.
- `records/deleted`: put 1, delete; tombstone revision 2.
- `records/absent-delete`: delete absent; tombstone revision 1.

A separate put to `rolled-back` is rolled back. A transaction putting `dropped`
is dropped uncommitted. Neither key is present. The permanent local test checks
exact values/revisions, ordered prefix pagination, absent uncommitted writes,
and continued tombstone revisions after reopening the old file.
