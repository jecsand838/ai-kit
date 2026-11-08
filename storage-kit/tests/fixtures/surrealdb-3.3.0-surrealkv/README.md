# Closed SurrealDB 3.3.0 SurrealKV fixture

`native/store` is a minimal, closed 3.3.0 store. Never open the original with
4.0.0 or regenerate it during normal validation. `manifest.json` and its SHA-256
bind every native file; `generation/receipt.json` records image identity, native
version, commands, exact catalog/records, graceful shutdown and container removal.

The fixed namespace is `vendor_migration`, database `surreal_330`, schema-full
table `records`, with string `label` and integer `sequence`. `records:a` is
`alpha`/1 and `records:b` is `beta`/2. `expected.json` preserves the complete
normalized record, database and table catalog responses. `seed.surql` and
`verify.surql` are the exact generation and verification queries.

The generator uses the immutable image digest recorded in the manifest and
checks version 3.3.0. To reproduce in a separate checkout where this fixture
does not exist, run:

```sh
python3 storage-kit/tests/surreal_legacy_store/fixture.py generate
```

It refuses to overwrite an existing fixture, creates the data through the real
server, stops and removes the server, then verifies a disposable cold copy.
Native IDs, timestamps and storage bytes may vary between reproductions;
the committed manifest identifies this particular fixture.

The public regression in `strict_live` copies the original before attempting
connection. A 4.0.0 refusal must explicitly identify the experimental native
3.x upgrade being refused by default. It then verifies the same attempted copy
through the pinned 3.3.0 reader and rechecks the sealed original hashes. Ordinary
3.3.0 success is the intentional version-boundary red, not a Kit defect.
The script retains per-attempt receipts and copies at the printed evidence path.
The original CI failures and failed fixture preparation attempts remain under
`target/security-2026-0454-0452/leaf-20261107T204006Z`.
