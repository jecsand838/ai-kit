# Workspace dependency security

All Kit dependencies now resolve from crates.io without local source patches.
LanceDB 0.40.0, Lance 13.0.0 and SurrealDB client/Core 3.3.0 remain selected.
Qdrant is temporarily unavailable: its Geo dependency conflicts with the
published SurrealDB dependency graph. Restoring it requires compatible upstream
releases and fresh backend validation. Release commands use the committed
lockfile with `--locked`.

## Temporary accepted advisories

The maintainer has explicitly accepted the findings below to unblock the initial
0.0.1 release preparation. **We will fix these as soon as possible** by adopting
compatible upstream fixes and removing each exception. These exceptions are not
a permanent security policy or evidence that the affected code is safe.

The maintainer assesses exposure in the intended local Lance use as negligible
and in the supported SurrealDB use as low. This is a scoped risk acceptance,
not a reachability audit or a guarantee for arbitrary inputs, feature graphs,
platforms or downstream applications. The lockfile includes optional and
target-specific dependencies, so a finding does not by itself prove execution in
a particular native build. Conversely, a passing build does not prove an
advisory unreachable.

The current registry lockfile was scanned using cargo-audit 0.22.2 and RustSec
commit `ee1a49a2769a24a596afc1c20cb33748d2769586`. It contains seven vulnerability
findings and ten unsoundness findings covered by **17 exact exceptions**:

| Locked package | Accepted advisory IDs | Upstream removal condition |
| --- | --- | --- |
| `zstd 0.13.3` | [RUSTSEC-2026-0391](https://rustsec.org/advisories/RUSTSEC-2026-0391.html) | Lance/Arrow permit a compatible graph with `zstd >=0.14.3`. |
| `zstd-safe 7.3.0` | [RUSTSEC-2026-0395](https://rustsec.org/advisories/RUSTSEC-2026-0395.html), [0396](https://rustsec.org/advisories/RUSTSEC-2026-0396.html), [0397](https://rustsec.org/advisories/RUSTSEC-2026-0397.html), [0398](https://rustsec.org/advisories/RUSTSEC-2026-0398.html), [0399](https://rustsec.org/advisories/RUSTSEC-2026-0399.html), [0400](https://rustsec.org/advisories/RUSTSEC-2026-0400.html), [0401](https://rustsec.org/advisories/RUSTSEC-2026-0401.html), [0402](https://rustsec.org/advisories/RUSTSEC-2026-0402.html), [0403](https://rustsec.org/advisories/RUSTSEC-2026-0403.html), [0404](https://rustsec.org/advisories/RUSTSEC-2026-0404.html), [0405](https://rustsec.org/advisories/RUSTSEC-2026-0405.html), [0406](https://rustsec.org/advisories/RUSTSEC-2026-0406.html) | Lance/Arrow permit the fixed Zstd stack; confirm every ID is absent in a fresh raw scan. |
| `quick-xml 0.39.4` | [RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194.html), [0195](https://rustsec.org/advisories/RUSTSEC-2026-0195.html) | SurrealDB/ObjectStore permit `quick-xml >=0.41.0`. |
| `rsa 0.9.10` | [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | SurrealDB's target-specific JWT chain removes the affected RSA dependency or adopts a RustSec-confirmed fix. No patched version was listed at review. |
| `crypto-bigint 0.5.5` | [RUSTSEC-2026-0387](https://rustsec.org/advisories/RUSTSEC-2026-0387.html) | The SurrealDB JWT/RSA chain removes or upgrades the affected dependency, confirmed by a fresh raw scan. |

RSA's exception is explicitly a **2023 advisory**, in addition to the 2026
findings. The already-fixed `zstd-sys 2.1.2+zstd.1.5.7` remains locked; its older
advisory is not ignored. Unrelated compatible fixes remain in the lockfile.

The executable policy is [`scripts/security_exceptions.toml`](../scripts/security_exceptions.toml).
It enumerates full `RUSTSEC-YYYY-NNNN` IDs; cargo-audit does not accept
`--ignore RUSTSEC-2026` as a wildcard. No exception covers future advisories.
Review this list and the dependency paths on every dependency update and before
each release. Remove stale exceptions immediately when an upstream fix lands,
then run the raw audit and affected compatibility, deterministic and strict-live
gates. A new or broadened exception needs explicit maintainer review.

## Remediated build dependency

The lockfile selects `libz-sys 1.1.31`, which fixes
[RUSTSEC-2026-0418](https://rustsec.org/advisories/RUSTSEC-2026-0418.html).
The affected `1.1.28` arrived through Storage's optional SurrealDB/RocksDB
dependency chain. The [upstream fix](https://github.com/rust-lang/libz-sys/pull/280)
removes compiler-command logging that can expose build environment values.
The update changes only that package's version and checksum; no dependency
patch, new direct dependency, feature change, or advisory exception is needed.

PR #28's original failed audit remains historical evidence: its raw report
contained eight vulnerability and ten unsoundness findings, and its qualified
report retained only this new advisory. A fresh audit of the fixed graph passed
the same two-scan gate with the existing 17 exceptions: this advisory was absent
from the raw report, and the qualified scan had no blocking findings. The result
is `PASS_WITH_EXCEPTIONS`; all raw accepted findings remain visible.

The recorded graph uses `cc 1.2.59`, below the advisory's `cc >=1.6.0` trigger,
and the supported RocksDB path enables static zlib, which skips the affected
probe unless overridden. These facts do not establish the safety of arbitrary
historical environments or caches; no secret exposure was confirmed during
this investigation. Updating a dependency does not delete existing build output.
Downstream applications with their own lockfiles must update their resolved
`libz-sys` separately.

For this targeted update, the maintainer explicitly accepts the existing
strict-live qualification without rerunning live suites. Original receipts,
failed attempts, accepted historical passes, and the separately accepted Gemini
cleanup exception retain their recorded source and execution identities. Fresh
dependency-audit and deterministic results remain distinct from that reused
live evidence; this decision adds no advisory exception.

## Audit evidence and CI

```sh
python3 scripts/security_dependencies.py verify --root "$PWD"
python3 scripts/security_dependencies.py audit --root "$PWD" \
  --scanner /absolute/path/to/cargo-audit \
  --database /absolute/path/to/advisory-db \
  --output /absolute/path/to/new-audit-evidence
```

CI downloads cargo-audit 0.22.2 using a pinned checksum. The verifier rejects
Cargo patches/replacements and foreign path or Git lockfile entries. Only the
five workspace packages may be local; the four approved workspace dependency
paths remain allowed. There are no vendored source exceptions to architecture
or source guards.

The audit runs twice against the real lockfile. The first run fetches current
RustSec data and records every finding without ignores. The second uses the
same database with `--no-fetch` and repeated `--ignore FULL-ID` arguments from
the reviewed policy. Both run with `--deny unsound --no-yanked` in an isolated
directory containing an empty `.cargo/audit.toml`, preventing repository or
user audit filters from changing the gate.

Nonexcepted vulnerabilities and unsoundness findings fail the gate. Scanner
errors, malformed results and inconsistent input/database identities cannot
produce a pass. Maintenance and notice warnings remain visible and nonblocking;
yanked-package checking is not part of this gate. Evidence includes `raw.json`,
`raw.log`, `qualified.json`, `qualified.log` and `report.json`, with commands,
exit results, input identities and accepted findings. A successful scan with
accepted findings is labeled **`PASS_WITH_EXCEPTIONS`**, never a clean audit.
Cargo-audit omits ignored findings from its qualified JSON, which is why the raw
report must be retained alongside it.

The exception policy participates in mock and release input fingerprints.
Changing it invalidates prior receipts. Existing CI job ceilings remain
5/20/10/5 minutes for Formatting/Tests/Linting/Security audit. Deterministic CI,
audit qualification and real strict-live validation establish different facts;
none substitutes for the others or for the complete release qualification.

## Downstream projects and historical evidence

Published Kits require no downstream Cargo patch configuration. The temporary
audit ignores also do not propagate to consumers: each downstream maintainer
must assess its own resolved graph and accept or remediate the findings under
its own policy. Registry-only packaging is not a security clearance or a
promise that all downstream feature combinations build.

The former source patch implementation, provenance inventory and compatibility
harness have been removed. Dated files under [`security-evidence/`](security-evidence/)
remain historical records of their exact earlier inputs, including original
failures. They are not evidence for the current unpatched graph and grant no
standing waiver.
