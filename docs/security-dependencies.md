# Workspace dependency security

The workspace source-build remediation keeps LanceDB 0.40.0, Lance 13.0.0 and
SurrealDB SDK/Core 3.3.0. Qdrant Edge is 0.9.0, the Arrow 58 family is 58.4.1,
and the optional macOS browser constraint is 1.2.5. Existing backend features
and public SDK signatures are preserved. The committed lockfile records the
reviewed compatible security updates; release commands use `--locked`.

## Approved source patches

Exactly seven original upstream packages are checked in under
[`third-party/security/`](../third-party/security/). Their directory names,
normalized package identities, original manifests and licenses are retained.
Only normalized manifests change, apart from restoring omitted upstream license
files. The readable diffs are in that directory's `patches/` subdirectory.

| Original registry package | Narrow change |
| --- | --- |
| arrow-ipc 58.4.1 | Require zstd 0.14.3, retaining features |
| lance-encoding 13.0.0 | Require zstd 0.14.3, retaining features |
| include-flate-compress 0.3.4 | Require zstd 0.14.3, retaining features |
| object_store 0.13.2 | Require quick-xml 0.41.0, retaining features |
| surrealdb-core 3.3.0 | Remove unsupported WASM JWT dependency; retain native AWS-LC |
| geo 0.32.0 | Change i_overlay range from 4.0.x to 4.5.x; retain features |
| surrealdb-expr 3.3.0 | Remove its independent WASM JWT dependency; retain native AWS-LC |

Geo 0.32 is retained for SurrealDB and revision; Qdrant independently uses
registry Geo 0.33.1. The compatible overlay range lets Cargo resolve both without
rewriting Geo’s Rust source. Both SurrealDB parents must remove the
unsupported WASM crypto backend to remove the vulnerable RSA dependency. This
remediation promises supported native builds only, with no WASM compatibility
claim. Registry ObjectStore 0.14.2 remains alongside patched 0.13.2.
Compression resolves through zstd 0.14.3 → zstd-safe 8.2.0 → zstd-sys 2.1.2.
No vulnerable package version is relabeled or advisory suppressed.

`manifest.json` records each original crates.io archive URL and SHA-256, license
paths, reviewed diff digest, and complete file inventory with hashes and
executable bits. Archive checksums were verified before extraction. Original
`Cargo.toml.orig` remains available for review, along with `.cargo_vcs_info.json`
where included upstream.
The checker pins the approved archive identities, closes the entire source
inventory, validates both Cargo patch/exclusion tables, and rejects symlinks,
unknown packages, missing/extra files, changed content and changed modes.
Review changes to source inventories and diffs together; hashes detect drift,
not malicious changes to both the source and its reviewed manifest.

Three published archives omit their full license texts. The checked-in copies
restore those files from their exact published source commits:

- Lance: [LICENSE at 0923349](https://github.com/lance-format/lance/blob/0923349baa19fd9af776b7d4626dc758dc610c80/LICENSE).
- include-flate: [LICENSE at 4118d8e](https://github.com/SOF3/include-flate/blob/4118d8e5d30b81c678f889b492945c39c019f846/LICENSE).
- Geo: [LICENSE-MIT](https://github.com/georust/geo/blob/240f203818c978a87f3fe3376aa5e4155caf6634/LICENSE-MIT) and [LICENSE-APACHE](https://github.com/georust/geo/blob/240f203818c978a87f3fe3376aa5e4155caf6634/LICENSE-APACHE).

Only verified upstream roots may retain upstream aliases, macros, unsafe code,
source generation conventions and tests. SDK code and unrelated paths remain
subject to the existing architecture rules. No additional patches are permitted
by this exception. Remove an override when an adequately tested upstream release
makes it unnecessary, updating the checker, lockfile, inventory and regression
evidence together.

## Audit and cache coverage

```sh
python3 scripts/security_dependencies.py verify --root "$PWD"
python3 scripts/security_dependencies.py audit --root "$PWD" \
  --scanner /absolute/path/to/cargo-audit \
  --database /absolute/path/to/advisory-db \
  --output /absolute/path/to/audit-evidence
```

The CI scanner is cargo-audit 0.22.2, downloaded with a pinned checksum. The
first scan fetches current RustSec data and audits the real committed lockfile.
Because cargo-audit skips path dependencies, a second temporary lockfile lists
all seven parents under their original registry names, versions and checksums.
That scan uses the same database without another fetch. Either failure blocks
Security audit. Scans run outside repository-local scanner configuration,
with `--deny unsound --no-yanked`; vulnerabilities and unsoundness fail, while
maintenance warnings remain visible and nonblocking. There are no ignored IDs.
Both scan logs, supplemental lockfile and aggregate report are retained.

Every source file, original manifest, license, patch diff and inventory enters
mock and release source fingerprints. Both Tests and Linting cache keys hash
the complete security directory. Verified sources are required before build
caches are restored. Existing CI ceilings remain 5/20/10/5 minutes for
Formatting/Tests/Linting/Security audit. Dependency compatibility tests, native
backend tests, strict-live services and capacity suites stay outside CI.

## Local regression evidence

`python3 scripts/check_security_compatibility.py` explicitly runs the small
dependency compatibility harness outside the workspace. It verifies frozen
pre-update Zstd, include-flate and compressed Arrow IPC fixtures, tests new
round trips and malformed data, exercises ObjectStore's actual S3 XML parser
against a local HTTP fixture, and checks Geo area, buffer, boundary and hole
behavior. It records the isolated resolved lockfile, verifies the intended
dependency versions, then tests with `--locked`. The legacy writer and its
lockfile are fixture provenance only; their old dependencies are not added to
the workspace or executed by CI. Run this command serially with other Cargo
commands.

The real scanner's fail-closed behavior can also be checked explicitly:

```sh
python3 -m scripts.tests.test_security_dependencies --parent-advisory-proof \
  --scanner /absolute/path/to/cargo-audit --output /new/evidence/directory
```

That opt-in test uses an isolated synthetic advisory database: its ordinary
lockfile scan must succeed and its patched-parent scan must fail. It never
changes the production audit policy or advisory database. Normal Python test
discovery uses deterministic fault fixtures and does not invoke the scanner.
The original failing audit, authentic-import comparison, Qdrant offset red/green
regression and local compatibility reports are retained under
[`security-evidence/`](security-evidence/); dated evidence is scoped to its
recorded inputs and never grants a standing validation waiver.

## Downstream projects

Root `[patch.crates-io]` settings do **not** propagate through a published SDK
crate. This is a workspace source-build remediation; crates.io consumers do not
automatically inherit it. A downstream project requiring the same remediation
must copy the reviewed `third-party/security/` tree (including manifests,
licenses and provenance) from the chosen ai-kit revision, then add these entries
to its own workspace root Cargo.toml, with paths adjusted to that copy:

```toml
[patch.crates-io]
arrow-ipc = { path = "third-party/security/arrow-ipc-58.4.1" }
lance-encoding = { path = "third-party/security/lance-encoding-13.0.0" }
include-flate-compress = { path = "third-party/security/include-flate-compress-0.3.4" }
object_store_013 = { package = "object_store", path = "third-party/security/object_store-0.13.2" }
surrealdb-core = { path = "third-party/security/surrealdb-core-3.3.0" }
geo_032 = { package = "geo", path = "third-party/security/geo-0.32.0" }
surrealdb-expr = { path = "third-party/security/surrealdb-expr-3.3.0" }
```

Exclude those directories from downstream workspace membership. Resolve the
same reviewed dependency versions into the downstream lockfile, audit registry
and patched parents, and run that project's native compatibility tests. The
ai-kit checker deliberately enforces this repository's exact workspace and
patch tables; adapt that policy explicitly for a different root. A downstream
feature graph can differ, especially Objective-C features on macOS, so this
configuration alone is not proof that an arbitrary consumer graph builds.
