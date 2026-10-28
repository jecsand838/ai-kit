# Database Capability Matrix

`CapabilitySet` describes only optional behavior in the retained portable
Database inventory, and the operation call remains authoritative. The live
`Database` handle and `DatabaseBackend` contract expose no backend identity or
provenance; construction mode remains explicit only in configuration.

The portable inventory is:

- catalog: create/open/list/drop collection and schema alteration;
- data: insert, scan, update, delete, atomic partition
  replacement, and count;
- query: nearest, text, and scroll;
- index: create, list, drop, and wait for readiness.

| Optional behavior | LanceDB | SurrealDB |
|---|---:|---:|
| Dense vector | yes | yes |
| Full text query | yes | yes |
| Filtering | yes | yes |
| Schema alteration | yes | yes |
| Wait for index | yes | yes |
| Atomic partition replacement | yes | yes |

The table is a guide, not an authorization registry. Requests still validate
their exact fields and backend mode. An adapter returns
`UnsupportedCapability` or `NotYetImplemented` rather than dropping fields,
substituting a nearby algorithm, inventing counts/scores/ids, or simulating
success.

Schema alteration support does not guarantee every request option. Supplied
Lance and Surreal reject any present SQL `read_columns` option, including an
empty list, with `UnsupportedCapability` before mutation. Lance eagerly
backfills SQL expression values before returning completion; see the
[portable API contract](PORTABLE_API.md#database).
Lance also rejects batches combining a cast with making a currently nullable
column required; callers can issue those alterations separately. Casts on
already non-nullable columns remain supported.

Excluded native capabilities—realtime, namespace administration, aliases,
hybrid and multivector search, facets, recommendations, discovery, grouped
queries, version restore, consistency controls, import/export, snapshots,
prewarm, optimizer controls, native statistics/rebuild, and sparse-only
operations—are not portable surface flags. Concrete backends may use them
privately, but callers do not receive a supposedly portable wrapper.

`StateStore` is outside this matrix. It exposes one fixed revisioned
transaction contract and no identity or capability projection. Redb's FIFO
single-native-writer admission is an implementation detail, not a public
capability difference.

The explicitly selected embedded Lance StateStore uses that same fixed
contract; it adds no capability projection or trait method. Its accepted
transaction design, native-version fencing, measured capacity and actual
platform receipts live in the [evidence ledger](lancedb-state-store-prototype.md).
Lance is always compiled for both products, and Cargo's default feature set is
empty. Alternatives require explicit `redb`, `postgres`, `surrealdb`, and `sqlite` features. Redb, PostgreSQL and
SurrealDB configuration values remain available without their native features;
SQLite retains conditional public availability. Cold-copy recovery
preserves the entire directory; it introduces no public backup, revision-import
or cross-product transaction capability.
Private decoded-image caching and lease-protected reclamation are implementation
details. Tombstones persist, and retained transactions can defer reclamation;
there is no portable retention or maintenance control.
