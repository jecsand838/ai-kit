# Embedded LanceDB StateStore capacity measurements

Execution evidence: **COMPLETE macOS PAIRED MATRIX AND TWO LINUX CANDIDATE REPETITIONS**.

These measurements describe a bounded capacity envelope for embedded local-directory stores on the recorded hosts. The macOS matrix compares a baseline and candidate; the separate Linux section repeats the candidate only. Process-local memory mode is outside this measurement matrix. These results set no customer SLO and do not establish production-profile performance, device write amplification, power-loss durability, or unlimited growth.

## Provenance

- Baseline source digest: `f800046ff61586f6b3d9d9d8d860d128240544482d70bdd2e4d7e4ea2c9a855c`; harness SHA-256 `85ae8cd624c97965d061e1273009ed36cd7a0dc473ab47fc8f6dbbfa701d7701` (frozen source reread at parser execution; aggregate digest verified against recorded run identity).
- Candidate source digest: `a6dc335f2137b37a4ce692761d805801270bcbd18aef13ca2f7b3bb60772be0a`; harness SHA-256 `85ae8cd624c97965d061e1273009ed36cd7a0dc473ab47fc8f6dbbfa701d7701` (recorded file manifest; aggregate digest verified).
- Compiler: `rustc 1.94.1 (e408947bf 2026-03-25)`.
- Profile: `test (unoptimized; debug info disabled)`; settings `{"CARGO_BUILD_JOBS": "1", "CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0", "RUSTUP_TOOLCHAIN": "1.94.1"}`.
- Host receipt: `ProductName:		macOS; ProductVersion:		26.7; BuildVersion:		25G229`; architecture `arm64`; dataset root `/var/folders/8j/lp9htlzn26542tjhq7b1g7pw0000gn/T`.
- Filesystem receipt: `/dev/disk3s1 on /System/Volumes/Data (apfs, local, journaled, nobrowse, protect, root data)`; recorded volume usage `99%`. Available space and filesystem cache/thermal state are uncontrolled measurement conditions.
- Command: `cargo test --locked -p storage-sdk --no-default-features --test lancedb_state_performance -- --nocapture --test-threads=1`.
- Baseline retains the earlier cloud dependency feature graph; candidate includes the approved dependency-feature reduction. This measures the full candidate, not an isolated causal experiment for one optimization.

| Run | Result | Duration seconds | Log SHA-256 |
| --- | --- | ---: | --- |
| baseline-final-1 | 15 passed / 0 failed | 382.50 | `44041584b2466dc917952359412d9fabe18ca2e9fb89b91500194506177163b4` |
| baseline-final-2 | 15 passed / 0 failed | 383.90 | `1d7e1615b50cb43ee19dd35f048e4edd6b4ad62c8aa97abc918ae61974f96bd4` |
| candidate-final-1 | 15 passed / 0 failed | 204.86 | `4cf4fc5c98f9a3ca3f9af4e370bf54a8a008de32bc0ea2e47bdfde621d2c7959` |
| candidate-final-2 | 15 passed / 0 failed | 215.91 | `4b2a2e7b866c07afca541c7f482f514088951d0faea996ab585aa100dcd7916f` |

## Measured findings and regressions

Both repetitions passed all 15 scenarios on each source, with no missing metric samples. Mean complete-target time decreased from 383.20s to 210.38s (45.1%); this includes setup and verification and is not a universal throughput claim. Values below summarize means of the two per-run quantiles, without confidence intervals.

Warm point-load p50 spans 0.55–1.74ms across the eight candidate profiles, 81.5–99.4% below baseline. 72/72 list cells improved in p50; their full observed candidate/baseline p50 ratio range is 0.005–0.224×. Reusing validated decoded images benefits these repeated-read cases; the measurements do not quantify RSS savings.

Fresh-connection cold-open/load p50 regressed in 7 of eight profiles, by 4.3–61.8%. The retained-versions profile improved by 57.2%. Startup now validates retained references for safe maintenance, so repeated reads and repeated connections have different cost profiles; the paired full-candidate comparison does not isolate one cause of each change.

Candidate single-key p50 spans 98.5–213.0ms; multi-key p50 spans 106.6–774.3ms. Writes did not improve uniformly: less-compressible single-key p50 increased 9.1%, large-payload multi-key p50 increased 6.6%, and all four fixed-16-MiB p50 cells increased 7.0–10.9%. Single-key p95 also increased 31.4% for cardinality, 26.1% for retained versions, and 37.2% for less-compressible payloads. These are observed regressions, not waived measurements.

Eight shared-handle disjoint writers improved reconciled publication rate from 1.83/s to 9.61/s. Independent disjoint writers decreased from 4.58/s to 4.31/s. Same-root knowledge/state activity decreased from 7.33/s to 6.16/s, with substantial candidate repeat variation shown below. Every eight-writer scenario had 112 conflicts out of 128 attempts per run; cheaper rejection does not remove table-wide serialization or supply application retries.

The tested envelope covers 10,000 live 1-KiB records; 1,000 live records with 10,000 tombstones; 128 live 256-KiB records; 16-MiB authored transitions split across 1–1,024 rows; eight writers; and 32 simultaneous snapshots of one version. Remaining versions (repeat1 / repeat2) were retained_versions: 312 / 312 baseline → 60 / 60 candidate; cardinality: 92 / 92 baseline → 29 / 29 candidate; tombstones: 96 / 96 baseline → 33 / 33 candidate. The large-payload profile retains about 521MiB across 53 / 53 versions, illustrating retained-history cost. These finite operating points establish neither a maximum capacity nor a fixed SLO.

## Protocol and interpretation

Each profile uses five unmeasured point loads, five begin/rollback warmups before measured begin/load/rollback calls, five list warmups per cell, and five warmups for each mutation class. The 10 cold observations include a fresh SDK connection, one load, and final-owner drop, with no warmup; the operating-system filesystem cache is not cleared. There are 100 warm loads, 100 begin/load/rollback operations, nine list cells with 10 measured calls each, 30 single-key transitions and 10 multi-key transitions. Ten unmeasured mutation warmups retain their receipt checks. Whole contention/retained-image/fixed-byte scenarios have no discarded warmup.

Values separated by `/` are repeat 1 / repeat 2. Latencies below are milliseconds. C/B ratios divide the mean of the two candidate per-run quantiles by the mean of the two baseline per-run quantiles; below 1 is lower observed latency. They are descriptive ratios, not pooled quantiles or confidence intervals. P95 uses nearest-rank per run, so for 10 samples it is the maximum. P99 is emitted only for at least 100 samples. Full p95/p99/max, serial throughput, error counts and optional metrics were retained in `capacity-comparison.json` beside the original logs. Those temporary macOS artifacts are now unavailable; the [completion ledger](lancedb-state-store-prototype.md#production-completion--2026-10-09) discloses that loss. The recorded tables and hashes below remain historical observations, not recreated raw receipts.

A mutation observation includes begin, load and put for each workload row, two additional receipt/companion puts, and commit. “Single-key” means one workload row plus those two evidence rows; “multi-key” means min(profile rows,129) workload rows plus the same two. Fixed-byte cases likewise include receipt overhead beyond their stated payload-string total. Every mutation outcome is reconciled after all StateStore writer owners join. Checks cover all seeded payloads and sequences, receipt/companion presence and revisions, no writes from conflicts, and subsequent progress. Same-root knowledge additionally checks 129 retained records. Conflicts are conservative table-wide serialization decisions; latency metrics include failed observations. No automatic retries are measured.

## Capacity profiles

All payload sizes refer to the string field, excluding JSON/Arrow overhead. Tombstones adds 10,000 permanent deleted keys. Retained-versions performs 256 additional writes; actual remaining versions are measured, not assumed. Less-compressible repeats one pseudo-random 16 KiB string across rows, retaining favorable row-to-row compression.

### small: 100 live rows × 1,024 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 7.90 / 6.39 | 12.39 / 9.99 | 1.566× | 1.978× |
| warm_load | 100 / 100; 100 / 100 | 2.80 / 3.15 | 0.57 / 0.53 | 0.185× | 0.175× |
| begin_load_rollback | 100 / 100; 100 / 100 | 2.83 / 2.93 | 0.47 / 0.44 | 0.159× | 0.157× |
| single_key_mutation | 30 / 30; 30 / 30 | 97.97 / 94.23 | 100.72 / 96.28 | 1.025× | 0.988× |
| multi_key_mutation | 10 / 10; 10 / 10 | 256.70 / 109.62 | 105.60 / 107.70 | 0.582× | 0.529× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 2.73 / 3.56 | 0.56 / 0.56 | 0.179× | 0.176× |
| list limit=1, first=50 | 10 / 10; 10 / 10 | 2.76 / 2.80 | 0.57 / 0.52 | 0.195× | 0.176× |
| list limit=1, first=98 | 10 / 10; 10 / 10 | 3.23 / 2.71 | 0.66 / 0.54 | 0.202× | 0.201× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 2.73 / 2.74 | 0.65 / 0.58 | 0.224× | 0.218× |
| list limit=100, first=50 | 10 / 10; 10 / 10 | 2.83 / 3.06 | 0.61 / 0.56 | 0.199× | 0.227× |
| list limit=100, first=98 | 10 / 10; 10 / 10 | 2.81 / 2.76 | 0.63 / 0.54 | 0.210× | 0.199× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 2.74 / 3.24 | 0.63 / 0.58 | 0.202× | 0.206× |
| list limit=1000, first=50 | 10 / 10; 10 / 10 | 2.80 / 2.85 | 0.62 / 0.58 | 0.214× | 0.302× |
| list limit=1000, first=98 | 10 / 10; 10 / 10 | 2.76 / 2.85 | 0.69 / 0.51 | 0.214× | 0.246× |

### medium: 1,000 live rows × 1,024 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 25.95 / 25.14 | 37.76 / 35.87 | 1.441× | 1.336× |
| warm_load | 100 / 100; 100 / 100 | 12.42 / 11.62 | 0.77 / 0.67 | 0.060× | 0.073× |
| begin_load_rollback | 100 / 100; 100 / 100 | 12.09 / 11.69 | 0.59 / 0.58 | 0.049× | 0.060× |
| single_key_mutation | 30 / 30; 30 / 30 | 108.30 / 110.55 | 112.94 / 109.91 | 1.018× | 1.033× |
| multi_key_mutation | 10 / 10; 10 / 10 | 129.34 / 135.18 | 130.59 / 131.92 | 0.992× | 0.961× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 12.25 / 12.23 | 0.88 / 1.09 | 0.080× | 0.093× |
| list limit=1, first=500 | 10 / 10; 10 / 10 | 12.61 / 11.86 | 0.88 / 0.92 | 0.073× | 0.078× |
| list limit=1, first=998 | 10 / 10; 10 / 10 | 11.99 / 12.33 | 0.77 / 0.67 | 0.059× | 0.072× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 13.26 / 12.09 | 1.35 / 1.11 | 0.097× | 0.117× |
| list limit=100, first=500 | 10 / 10; 10 / 10 | 12.55 / 11.74 | 1.27 / 0.89 | 0.089× | 0.096× |
| list limit=100, first=998 | 10 / 10; 10 / 10 | 12.33 / 11.81 | 0.92 / 0.72 | 0.068× | 0.076× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 12.05 / 11.57 | 1.78 / 1.33 | 0.132× | 0.137× |
| list limit=1000, first=500 | 10 / 10; 10 / 10 | 12.35 / 11.65 | 1.20 / 0.94 | 0.089× | 0.108× |
| list limit=1000, first=998 | 10 / 10; 10 / 10 | 12.15 / 11.83 | 0.94 / 0.65 | 0.066× | 0.082× |

### cardinality: 10,000 live rows × 1,024 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 225.73 / 219.55 | 283.17 / 284.39 | 1.275× | 1.248× |
| warm_load | 100 / 100; 100 / 100 | 108.96 / 107.25 | 1.78 / 1.67 | 0.016× | 0.018× |
| begin_load_rollback | 100 / 100; 100 / 100 | 104.58 / 107.56 | 1.54 / 1.58 | 0.015× | 0.016× |
| single_key_mutation | 30 / 30; 30 / 30 | 220.29 / 228.81 | 180.58 / 231.67 | 0.918× | 1.314× |
| multi_key_mutation | 10 / 10; 10 / 10 | 251.91 / 252.60 | 197.79 / 192.29 | 0.773× | 0.640× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 120.35 / 120.26 | 3.58 / 3.38 | 0.029× | 0.028× |
| list limit=1, first=5000 | 10 / 10; 10 / 10 | 114.46 / 120.53 | 2.84 / 2.77 | 0.024× | 0.024× |
| list limit=1, first=9998 | 10 / 10; 10 / 10 | 107.53 / 108.32 | 2.01 / 1.89 | 0.018× | 0.018× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 111.84 / 114.60 | 5.64 / 5.36 | 0.049× | 0.049× |
| list limit=100, first=5000 | 10 / 10; 10 / 10 | 108.91 / 109.44 | 3.82 / 3.92 | 0.035× | 0.037× |
| list limit=100, first=9998 | 10 / 10; 10 / 10 | 111.20 / 105.31 | 2.02 / 2.11 | 0.019× | 0.021× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 111.24 / 109.25 | 6.84 / 7.18 | 0.064× | 0.064× |
| list limit=1000, first=5000 | 10 / 10; 10 / 10 | 106.61 / 111.89 | 4.10 / 4.35 | 0.039× | 0.041× |
| list limit=1000, first=9998 | 10 / 10; 10 / 10 | 102.88 / 109.45 | 1.85 / 1.88 | 0.018× | 0.016× |

### medium_payload: 1,000 live rows × 16,384 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 113.57 / 113.38 | 129.00 / 125.49 | 1.121× | 1.160× |
| warm_load | 100 / 100; 100 / 100 | 56.87 / 55.09 | 0.65 / 0.68 | 0.012× | 0.015× |
| begin_load_rollback | 100 / 100; 100 / 100 | 57.22 / 54.99 | 0.59 / 0.57 | 0.010× | 0.013× |
| single_key_mutation | 30 / 30; 30 / 30 | 150.02 / 150.37 | 158.44 / 154.50 | 1.042× | 1.057× |
| multi_key_mutation | 10 / 10; 10 / 10 | 218.66 / 211.73 | 229.25 / 213.37 | 1.028× | 1.030× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 57.05 / 55.31 | 0.89 / 0.89 | 0.016× | 0.018× |
| list limit=1, first=500 | 10 / 10; 10 / 10 | 57.39 / 54.92 | 1.00 / 0.80 | 0.016× | 0.018× |
| list limit=1, first=998 | 10 / 10; 10 / 10 | 57.03 / 54.69 | 0.84 / 0.71 | 0.014× | 0.016× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 57.66 / 56.36 | 1.21 / 1.16 | 0.021× | 0.024× |
| list limit=100, first=500 | 10 / 10; 10 / 10 | 56.88 / 54.85 | 0.97 / 0.89 | 0.017× | 0.018× |
| list limit=100, first=998 | 10 / 10; 10 / 10 | 56.74 / 55.03 | 0.76 / 0.73 | 0.013× | 0.015× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 57.48 / 56.41 | 1.31 / 1.29 | 0.023× | 0.026× |
| list limit=1000, first=500 | 10 / 10; 10 / 10 | 56.20 / 54.77 | 1.03 / 1.01 | 0.018× | 0.021× |
| list limit=1000, first=998 | 10 / 10; 10 / 10 | 56.72 / 54.82 | 0.65 / 0.70 | 0.012× | 0.014× |

### large_payload: 128 live rows × 262,144 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 205.59 / 201.63 | 195.46 / 229.39 | 1.043× | 1.062× |
| warm_load | 100 / 100; 100 / 100 | 102.70 / 96.69 | 0.59 / 0.58 | 0.006× | 0.008× |
| begin_load_rollback | 100 / 100; 100 / 100 | 107.09 / 97.91 | 0.44 / 0.47 | 0.004× | 0.006× |
| single_key_mutation | 30 / 30; 30 / 30 | 198.13 / 203.08 | 199.74 / 216.73 | 1.038× | 1.028× |
| multi_key_mutation | 10 / 10; 10 / 10 | 730.33 / 722.12 | 756.30 / 792.30 | 1.066× | 1.004× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 105.81 / 97.16 | 0.55 / 0.61 | 0.006× | 0.007× |
| list limit=1, first=64 | 10 / 10; 10 / 10 | 106.52 / 97.47 | 0.68 / 0.58 | 0.006× | 0.007× |
| list limit=1, first=126 | 10 / 10; 10 / 10 | 106.35 / 96.31 | 0.60 / 0.57 | 0.006× | 0.007× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 106.29 / 99.18 | 0.64 / 0.62 | 0.006× | 0.008× |
| list limit=100, first=64 | 10 / 10; 10 / 10 | 107.84 / 98.32 | 0.62 / 0.60 | 0.006× | 0.006× |
| list limit=100, first=126 | 10 / 10; 10 / 10 | 106.96 / 97.96 | 0.57 / 0.55 | 0.005× | 0.006× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 103.06 / 97.12 | 0.59 / 0.65 | 0.006× | 0.007× |
| list limit=1000, first=64 | 10 / 10; 10 / 10 | 103.22 / 96.98 | 0.58 / 0.61 | 0.006× | 0.008× |
| list limit=1000, first=126 | 10 / 10; 10 / 10 | 103.67 / 97.04 | 0.55 / 0.55 | 0.005× | 0.007× |

### tombstones: 1,000 live rows × 1,024 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 127.06 / 127.24 | 217.98 / 193.49 | 1.618× | 1.672× |
| warm_load | 100 / 100; 100 / 100 | 62.93 / 75.17 | 1.81 / 1.67 | 0.025× | 0.030× |
| begin_load_rollback | 100 / 100; 100 / 100 | 62.93 / 76.28 | 1.61 / 1.61 | 0.023× | 0.026× |
| single_key_mutation | 30 / 30; 30 / 30 | 188.53 / 194.42 | 125.95 / 125.72 | 0.657× | 0.965× |
| multi_key_mutation | 10 / 10; 10 / 10 | 208.18 / 210.24 | 137.61 / 141.19 | 0.666× | 0.673× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 65.86 / 77.43 | 1.92 / 2.22 | 0.029× | 0.032× |
| list limit=1, first=500 | 10 / 10; 10 / 10 | 64.27 / 77.34 | 1.83 / 1.85 | 0.026× | 0.026× |
| list limit=1, first=998 | 10 / 10; 10 / 10 | 62.49 / 77.48 | 1.74 / 1.78 | 0.025× | 0.027× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 65.05 / 76.90 | 2.18 / 2.15 | 0.031× | 0.033× |
| list limit=100, first=500 | 10 / 10; 10 / 10 | 62.84 / 77.00 | 1.97 / 2.01 | 0.028× | 0.033× |
| list limit=100, first=998 | 10 / 10; 10 / 10 | 64.51 / 76.15 | 1.72 / 1.75 | 0.025× | 0.025× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 62.65 / 76.21 | 2.46 / 2.27 | 0.034× | 0.040× |
| list limit=1000, first=500 | 10 / 10; 10 / 10 | 63.42 / 77.01 | 2.05 / 1.99 | 0.029× | 0.028× |
| list limit=1000, first=998 | 10 / 10; 10 / 10 | 64.44 / 77.38 | 1.77 / 1.74 | 0.025× | 0.017× |

### retained_versions: 1,000 live rows × 1,024 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 112.44 / 111.37 | 51.33 / 44.46 | 0.428× | 0.511× |
| warm_load | 100 / 100; 100 / 100 | 54.95 / 55.00 | 0.74 / 0.70 | 0.013× | 0.014× |
| begin_load_rollback | 100 / 100; 100 / 100 | 56.09 / 61.88 | 0.62 / 0.60 | 0.010× | 0.011× |
| single_key_mutation | 30 / 30; 30 / 30 | 107.73 / 109.25 | 103.76 / 104.07 | 0.958× | 1.261× |
| multi_key_mutation | 10 / 10; 10 / 10 | 135.68 / 129.75 | 134.08 / 128.97 | 0.991× | 0.952× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 55.43 / 59.65 | 0.92 / 0.94 | 0.016× | 0.019× |
| list limit=1, first=500 | 10 / 10; 10 / 10 | 54.81 / 59.39 | 0.83 / 0.81 | 0.014× | 0.014× |
| list limit=1, first=998 | 10 / 10; 10 / 10 | 56.54 / 57.11 | 0.75 / 0.73 | 0.013× | 0.014× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 55.76 / 57.73 | 1.16 / 1.36 | 0.022× | 0.021× |
| list limit=100, first=500 | 10 / 10; 10 / 10 | 55.64 / 55.84 | 0.96 / 0.94 | 0.017× | 0.017× |
| list limit=100, first=998 | 10 / 10; 10 / 10 | 56.65 / 58.29 | 0.78 / 0.74 | 0.013× | 0.013× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 54.67 / 58.34 | 1.25 / 1.24 | 0.022× | 0.020× |
| list limit=1000, first=500 | 10 / 10; 10 / 10 | 54.93 / 56.76 | 1.00 / 0.98 | 0.018× | 0.017× |
| list limit=1000, first=998 | 10 / 10; 10 / 10 | 54.77 / 58.42 | 0.73 / 0.73 | 0.013× | 0.013× |

### less_compressible: 1,000 live rows × 16,384 payload bytes

| Operation | Samples B; C (1 / 2) | B p50 ms (1 / 2) | C p50 ms (1 / 2) | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| cold_open_load | 10 / 10; 10 / 10 | 200.29 / 202.50 | 205.37 / 223.23 | 1.064× | 1.071× |
| warm_load | 100 / 100; 100 / 100 | 101.11 / 100.27 | 0.66 / 0.69 | 0.007× | 0.008× |
| begin_load_rollback | 100 / 100; 100 / 100 | 99.82 / 99.70 | 0.54 / 0.63 | 0.006× | 0.007× |
| single_key_mutation | 30 / 30; 30 / 30 | 195.04 / 195.29 | 218.51 / 207.48 | 1.091× | 1.372× |
| multi_key_mutation | 10 / 10; 10 / 10 | 263.57 / 260.81 | 274.52 / 274.70 | 1.047× | 0.969× |
| list limit=1, first=0 | 10 / 10; 10 / 10 | 101.62 / 102.18 | 0.81 / 1.08 | 0.009× | 0.011× |
| list limit=1, first=500 | 10 / 10; 10 / 10 | 100.37 / 99.65 | 0.78 / 0.98 | 0.009× | 0.010× |
| list limit=1, first=998 | 10 / 10; 10 / 10 | 104.44 / 99.76 | 0.70 / 0.76 | 0.007× | 0.007× |
| list limit=100, first=0 | 10 / 10; 10 / 10 | 99.25 / 99.32 | 1.05 / 1.28 | 0.012× | 0.014× |
| list limit=100, first=500 | 10 / 10; 10 / 10 | 99.48 / 99.69 | 0.86 / 1.05 | 0.010× | 0.011× |
| list limit=100, first=998 | 10 / 10; 10 / 10 | 100.12 / 99.20 | 0.64 / 0.73 | 0.007× | 0.009× |
| list limit=1000, first=0 | 10 / 10; 10 / 10 | 99.40 / 99.68 | 1.17 / 1.53 | 0.014× | 0.014× |
| list limit=1000, first=500 | 10 / 10; 10 / 10 | 99.97 / 100.72 | 0.90 / 1.17 | 0.010× | 0.012× |
| list limit=1000, first=998 | 10 / 10; 10 / 10 | 99.44 / 99.65 | 0.64 / 0.69 | 0.007× | 0.007× |

## Retained storage

Whole-fixture bytes/files include retained native history and, where present, knowledge tables. These are retained sizes, not device writes. Verification writes one final progress record, and connection-triggered maintenance may also run before final inventory.

| Profile | B initial→final MiB (1 / 2) | C initial→final MiB (1 / 2) | B final files | C final files | B versions | C versions |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| small | 0.11→2.07 / 0.11→2.07 | 0.11→2.07 / 0.11→2.07 | 279 / 279 | 280 / 280 | 53 / 53 | 53 / 53 |
| medium | 1.05→3.50 / 1.05→3.50 | 1.05→3.50 / 1.05→3.50 | 307 / 307 | 308 / 308 | 56 / 56 | 56 / 56 |
| cardinality | 10.58→13.22 / 10.58→13.22 | 10.58→11.90 / 10.58→11.90 | 451 / 451 | 160 / 160 | 92 / 92 | 29 / 29 |
| medium_payload | 15.70→47.01 / 15.70→47.01 | 15.70→47.01 / 15.70→47.01 | 307 / 307 | 308 / 308 | 56 / 56 | 56 / 56 |
| large_payload | 32.01→521.25 / 32.01→521.25 | 32.01→521.25 / 32.01→521.25 | 279 / 279 | 280 / 280 | 53 / 53 | 53 / 53 |
| tombstones | 1.63→4.28 / 1.63→4.28 | 1.63→2.95 / 1.63→2.95 | 467 / 467 | 184 / 184 | 96 / 96 | 33 / 33 |
| retained_versions | 4.47→6.97 / 4.47→6.97 | 1.08→3.53 / 1.08→3.53 | 1,426 / 1,426 | 332 / 332 | 312 / 312 | 60 / 60 |
| less_compressible | 16.03→47.99 / 16.03→47.99 | 16.03→47.99 / 16.03→47.99 | 307 / 307 | 308 / 308 | 56 / 56 | 56 / 56 |

## Contention and knowledge ingestion

Each scenario authors 128 attempts. Throughput counts reconciled publications and includes StateStore final-owner joins in the elapsed denominator; independent verification is outside that denominator. Disjoint writers still share table-wide conflicts.

| Scenario | B publications/s | C publications/s | C/B rate | B conflicts/timeouts | C conflicts/timeouts | B final MiB/files | C final MiB/files |
| --- | ---: | ---: | ---: | --- | --- | --- | --- |
| one_writer | 7.16 / 6.83 | 7.24 / 6.85 | 1.008× | 0/0 / 0/0 | 0/0 / 0/0 | 2.64/664 / 2.64/664 | 1.09/39 / 1.09/39 |
| eight_shared_disjoint | 1.79 / 1.86 | 9.76 / 9.46 | 5.265× | 112/0 / 112/0 | 112/0 / 112/0 | 1.63/440 / 1.63/440 | 1.16/105 / 1.16/105 |
| eight_independent_disjoint | 4.48 / 4.69 | 4.37 / 4.25 | 0.941× | 112/0 / 112/0 | 112/0 / 112/0 | 1.63/440 / 1.63/440 | 1.16/105 / 1.16/105 |
| eight_independent_hot | 5.55 / 4.59 | 5.98 / 5.66 | 1.148× | 112/0 / 112/0 | 112/0 / 112/0 | 1.55/320 / 1.55/320 | 1.15/90 / 1.15/90 |
| same_root_knowledge | 7.49 / 7.17 | 7.10 / 5.21 | 0.840× | 0/0 / 0/0 | 0/0 / 0/0 | 3.50/1056 / 3.50/1056 | 1.95/431 / 1.95/431 |

## Retained transactions and fixed authored bytes

Retained-image cases keep 1, 8 or 32 snapshots of the same 1,000-row, 4 KiB image alive while opening more transactions. They measure retention cardinality, not arbitrary pin duration or distinct-version memory. Long-lived snapshots can defer reclamation indefinitely; the separate maintenance suites establish that behavior.

| Retained transactions | Samples B; C (1 / 2) | B begin p50 ms | C begin p50 ms | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 1 / 1; 1 / 1 | 22.10 / 21.45 | 22.19 / 23.24 | 1.043× | 1.043× |
| 8 | 8 / 8; 8 / 8 | 20.59 / 20.62 | 0.52 / 0.42 | 0.023× | 0.030× |
| 32 | 32 / 32; 32 / 32 | 20.42 / 20.78 | 0.43 / 0.39 | 0.020× | 0.023× |

Fixed-byte cases author 16 MiB of payload strings per transition across 1/16/128/1,024 rows, three measured transitions each. They expose byte-volume versus row-count costs; their sample count cannot support tail claims.

| Authored rows | Samples B; C (1 / 2) | B p50 ms | C p50 ms | C/B p50 | C/B p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 3 / 3; 3 / 3 | 367.20 / 362.72 | 382.65 / 398.63 | 1.070× | 1.084× |
| 16 | 3 / 3; 3 / 3 | 367.65 / 367.28 | 377.32 / 408.99 | 1.070× | 1.080× |
| 128 | 3 / 3; 3 / 3 | 374.45 / 375.49 | 384.37 / 423.54 | 1.077× | 1.066× |
| 1024 | 3 / 3; 3 / 3 | 380.97 / 397.33 | 408.81 / 454.41 | 1.109× | 1.111× |

## Separate App consumer observations

Source: `/private/tmp/lancedb-production-20261008/app-candidate-focused-3.log`, SHA-256 `628c800fc1839fbf387b4a46446cbbb9f9d9b60e101f9287e45e6dd809d5128e`. These are separate consumer correctness cases, not repeated capacity-harness measurements or latency ratios. The elapsed interval includes workflow start and terminal observation for 130 steps, before retained-history deletion. The test then independently checks bounded deletion pages, forks, and lost-receipt behavior.

| Consumer case | Workflow start + terminal observation seconds | Supervision limit seconds | Terminal observation completed |
| --- | ---: | ---: | --- |
| lance_state_directory_deletion_preserves_bounded_pages_and_forks | 69.794 | 120 | True |
| lance_state_directory_lost_deletion_receipt_is_not_replayed | 75.676 | 120 | True |
| lance_state_memory_deletion_preserves_bounded_pages_and_forks | 24.655 | 120 | True |
| lance_state_memory_lost_deletion_receipt_is_not_replayed | 24.623 | 120 | True |

| Focused consumer target | Passed | Failed | Ignored | Filtered |
| --- | ---: | ---: | ---: | ---: |
| deletion_contract.rs | 4 | 0 | 0 | 20 |
| prepare_contract.rs | 1 | 0 | 0 | 4 |
| workspace_contract.rs | 2 | 0 | 0 | 17 |

The larger Lance test supervision deadline addresses the observed workload cost; it is not a production latency guarantee or a relaxed correctness assertion. The filtered cases remain outside this focused consumer receipt.

## Limits of this envelope

Optional self-process memory counters are unavailable on the recorded macOS runs; no external process inspection was used. Consequently these receipts do not quantify peak memory, shared-image RSS savings, or multipart-buffer peaks. The original JSON recorded that unavailability. The macOS matrix supplied no disk-I/O byte counter, fsync count, native write-amplification measurement, independent random-per-row corpus, or production release-profile run. The separate Linux section adds only the specifically labeled process counters.

Complete snapshots are still scanned/decoded on a changed identity, list still visits the decoded image, and retained distinct versions keep distinct decoded images. Lazy adapter batches contain at most 128 rows, with no per-record byte bound or promise that Lance's internal query plan buffers only one batch. Multipart buffers and permanent tombstones remain relevant capacity costs. Use the measured cells as tested operating points, then repeat representative caller workloads; these finite scenarios do not identify a universal maximum.

To repeat these measurements, freeze a complete source candidate and record its file manifest/digest, lockfile, compiler, build environment, platform and dataset filesystem as described in the [testing guide](testing.md#reproducing-an-ordinary-linux-candidate). Use a new receipt directory and run the following command twice with distinct labels, serially and with the same build profile. Preserve each complete log and actual command exit status, then recheck the source manifest. For a paired comparison, repeat on the other frozen source with the same harness and environment; two candidate runs alone do not establish a baseline comparison.

```bash
set -euo pipefail
export RUSTUP_TOOLCHAIN=1.94.1 CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
LANCE_EVIDENCE=$(mktemp -d "${TMPDIR:-/tmp}/lance-capacity.XXXXXX")
for repeat in 1 2; do
  label="candidate-${repeat}"
  if AI_KIT_LANCE_PERF_LABEL="$label" cargo test --locked \
    -p storage-sdk --no-default-features --test lancedb_state_performance \
    -- --nocapture --test-threads=1 > "$LANCE_EVIDENCE/$label.log" 2>&1; then
    printf '0\n' > "$LANCE_EVIDENCE/$label.exit"
  else
    lance_command_status=$?
    printf '%s\n' "$lance_command_status" > "$LANCE_EVIDENCE/$label.exit"
    exit "$lance_command_status"
  fi
done
```

Each accepted run must have a final parent summary of 15 passed and zero failed, ignored, measured or filtered tests, plus exactly 15 valid `LANCE_STATE_CAPACITY` JSON records with the selected label and the expected eight profiles, five contention scenarios, retained-image scenario and fixed-byte scenario. Preserve all raw records, sample counts, errors and optional-metric availability. Use the per-run quantiles and two-run ratio calculation documented under **Protocol and interpretation**; do not pool quantiles, invent missing cells, or report a complete matrix when a source, run or required measurement is absent.

## Linux candidate repetitions — 2026-10-09

Two complete candidate repetitions passed **15/15** cases each in **139.80 / 139.23 seconds**. These are additional Linux operating points, not a Linux baseline comparison or a cross-platform performance ratio. All required samples and semantic assertions were present; the analysis rejected missing or inconsistent receipts.

Source: `3edeee0f81e407d80627a801e6a380b8a73a8decd8a1dc32227681ea3c2d0239` (976 tracked and untracked candidate files); lockfile: `7ef9b5f1511c54bf743df27c8af6e4084c84287e321b9f13ae3e54f38ecddc34`. The harness hash matches the macOS matrix. The native compiler was Rust 1.94.1, `aarch64-unknown-linux-gnu`, with the same unoptimized, debug-disabled, one-job settings. Datasets and builds used a Linux-owned volume: `/workspace/test-tmp` beneath `/dev/vda1` mounted as **ext4** at `/workspace`; the raw `stat -f` type string was `ext2/ext3`. The ordinary Docker container used default security settings and no added capabilities.

The retained `linux-capacity-analysis.json` has SHA-256 `d767cdda059f19eb9aa88bbaf6dac70d3f5cf507334ac590243926bf214250ed`. Its terminal receipts bind both complete logs, exact commands, environment and source before/after. Fresh evidence is retained in the checkout's ignored `target/lancedb-production-evidence-20261009` directory; preserve it before cleaning that directory. Earlier macOS temporary raw artifacts were lost as disclosed in the [completion ledger](lancedb-state-store-prototype.md#production-completion--2026-10-09); their prose observations are not regenerated execution receipts.

Values below are repeat 1 / repeat 2. Latencies are per-run p50 milliseconds under the protocol above.

| Profile | Cold open/load | Warm load | Begin/load/rollback | Single-key mutation | Multi-key mutation | Retained versions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 12.63 / 12.33 | 0.74 / 0.79 | 0.63 / 0.71 | 46.65 / 43.85 | 66.54 / 69.96 | 53 / 53 |
| medium | 41.47 / 37.54 | 0.80 / 0.79 | 0.67 / 0.67 | 55.60 / 57.09 | 92.42 / 86.55 | 56 / 56 |
| cardinality | 319.99 / 333.55 | 1.51 / 1.46 | 1.61 / 1.37 | 155.72 / 160.35 | 162.84 / 160.53 | 29 / 29 |
| medium_payload | 157.54 / 156.44 | 0.79 / 0.80 | 0.67 / 0.67 | 124.31 / 116.28 | 214.99 / 214.72 | 56 / 56 |
| large_payload | 290.69 / 275.16 | 0.75 / 0.82 | 0.63 / 0.71 | 185.98 / 179.73 | 1153.29 / 1136.92 | 53 / 53 |
| tombstones | 225.41 / 229.02 | 1.50 / 1.58 | 1.39 / 1.47 | 69.60 / 74.01 | 94.36 / 88.63 | 33 / 33 |
| retained_versions | 50.28 / 48.39 | 0.87 / 0.89 | 0.77 / 0.76 | 50.39 / 58.52 | 79.20 / 79.77 | 60 / 60 |
| less_compressible | 264.47 / 274.85 | 0.78 / 0.85 | 0.67 / 0.72 | 177.51 / 176.22 | 276.68 / 270.10 | 56 / 56 |

The 144 list cells across both runs have p50 values from 0.74 to 7.11 ms. The large-payload fixture retains 521.25 / 521.25 MiB in 280 files and 53 versions. The retained-version profile ends with 60 versions and 3.53 / 3.53 MiB. These costs include native history; bounded admission does not imply a byte bound.

| Contention scenario | Reconciled publications/second | Conflicts out of 128 attempts | Timeouts |
| --- | ---: | ---: | ---: |
| one_writer | 15.90 / 15.57 | 0 / 0 | 0 / 0 |
| eight_shared_disjoint | 19.09 / 20.04 | 112 / 112 | 0 / 0 |
| eight_independent_disjoint | 20.30 / 19.97 | 112 / 112 | 0 / 0 |
| eight_independent_hot | 21.14 / 21.15 | 112 / 112 | 0 / 0 |
| same_root_knowledge | 15.98 / 16.10 | 0 / 0 | 0 / 0 |

All eight-writer runs reconciled 16 publications and 112 complete conflicts, with subsequent progress. No application retry loop was measured. Same-root knowledge retained its independent data assertions.

| Retained snapshots of one image | Begin p50 ms |
| ---: | ---: |
| 1 | 26.77 / 26.93 |
| 8 | 0.62 / 0.64 |
| 32 | 0.61 / 0.63 |

| Rows per fixed 16-MiB transition | Mutation p50 ms |
| ---: | ---: |
| 1 | 549.02 / 564.12 |
| 16 | 565.73 / 556.35 |
| 128 | 567.45 / 557.88 |
| 1024 | 590.23 / 586.55 |

Ordinary `/proc/self/status` counters were available. The highest sampled whole-process RSS was 594,788 / 699,240 KiB; the largest observed process-lifetime high-water mark was 645,760 / 699,240 KiB. These include native caches and prior scenarios, and do not measure isolated adapter peaks, per-operation allocation, or image-sharing savings. Fixed-byte cases still have only three samples per cell; they support no tail-latency claim.

### Separate Linux shared-directory workload counters

The supplied-adapter suite separately verified 32 state transactions, 64 knowledge rows inserted in four batches, and exact recreation revisions for 58 tombstoned keys against the same directory. Its state-transaction median/p95 was 65.13/92.03 ms, with 15.06 transactions/second over the concurrent workload. The subsequent four contention rounds had four commits and four conflicts. These are functional workload observations, separate from the 15-case capacity target.

| Completed state transactions | Authored JSON bytes | Retained file-length growth | Kernel process write-byte delta | Cancelled write-byte delta |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 2,698,478 | 2,732,136 | 2,879,488 | 4,096 |
| 16 | 5,396,980 | 5,476,433 | 5,787,648 | 4,096 |
| 24 | 8,095,490 | 8,227,625 | 8,695,808 | 4,096 |
| 32 | 10,794,000 | 10,985,707 | 11,603,968 | 4,096 |

The final process-write/JSON-byte ratio was 1.075; net retained file-length growth/JSON-byte ratio was 1.018. `/proc/self/io` covers the entire integration process and waited-for children, including knowledge, metadata, logging and other writes. Kernel `write_bytes` counts page-dirtying activity, not durable device writes. Cancelled writeback is reported separately, not subtracted. The denominator includes serialized state and knowledge values and excludes keys, tombstone requests, baseline data and native metadata. File lengths do not measure allocated filesystem blocks. Native rewrite counts and device write amplification remain unmeasured. Neither these counters nor the ordinary process-loss tests simulate machine power loss.
