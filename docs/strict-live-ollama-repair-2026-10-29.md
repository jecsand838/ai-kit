# Ollama runtime repair and affected-case qualification — 2026-10-29

**Both previously failing Ollama cases now pass: ten unary generation trials
and ten function-continuation trials, with every exact assertion preserved.**
This is a passing qualification of the two repaired cases under the maintainer's
instruction to rerun only failures. Unchanged passing results, including the
seven historical long-batch passes, remain accepted under the maintainer's reuse
rule. Their historical provenance stays separate from this fresh evidence.

The repair refreshed the official artifact under the same exact model name,
`ministral-3:8b-instruct-2512-q4_K_M`, and restored effective Tekken tokenization.
No Rust source, dependencies, test assertions, prompts, sampling settings,
credentials, or model pin changed. No output correction, fallback, source patch,
or additional retry was introduced. Authorization to change the model pin was
not needed.

## Original failures and investigation

The [previous Provider report](strict-live-provider-followup-2026-10-29.md)
retains the canonical result of 128 passes and two failures, plus seven accepted
historical batch passes. Its failed prompt candidate, earlier failed
confirmation, and later unchanged diagnostic passes remain intact. The two
canonical failures were:

- `live::generation::ollama_generation`: 38 output bytes instead of the
  required 39-byte token.
- `live::generation::ollama_function_continuation`: a different 39-byte token.

Prior native captures established faithful Kit output preservation. Request
replay, tool JSON, correlation, context size, and sampling investigations did
not demonstrate a Kit adapter defect. A new artifact inspection found that the
installed converted model retained `tokenizer.ggml.pre=default` while lacking
every legacy marker that activates Ollama's Mistral tokenizer correction.
The original loader log and pinned dispatch path support effective DEFAULT
loading; that older log does not directly print an effective tokenizer enum.

The pinned upstream sources explain the configuration defect:

- [Conversion](https://github.com/ollama/ollama/blob/v0.40.12/compatmigrate/mistral_pixtral.go)
  copies tokenizer metadata while splitting vision tensors and converting RoPE
  keys. The resulting local artifact no longer matched the correction detector.
- [Compatibility handling](https://github.com/ollama/ollama/blob/v0.40.12/llama/compat/llama-ollama-compat.cpp)
  changes `default` to `tekken` only when the legacy Mistral markers are present.
- [Pulling](https://github.com/ollama/ollama/blob/v0.40.12/server/images.go)
  and [manifest storage](https://github.com/ollama/ollama/blob/v0.40.12/manifest/manifest.go)
  replace the selected manifest with the official registry artifact. Automatic
  compatibility migration is disabled in the pinned
  [scheduling path](https://github.com/ollama/ollama/blob/v0.40.12/server/routes.go).
- [Runner selection](https://github.com/ollama/ollama/blob/v0.40.12/llm/server.go)
  sends GGUF models to llama-server even when the legacy manifest has a `ggml`
  label.

These source facts and observed loader behavior establish a corrected runtime
configuration. The subsequent bounded passes support repair acceptance; they
do not prove that every historical copying failure had that single cause or
establish statistical model reliability.

## Preserved and repaired identities

Fourteen manifest/artifact files were preserved with APFS clones before the
pull; every blob's contents matched its SHA-256 name. The backup covers about
12.04 GB of logical data without changing any source artifact. Pulling can
prune unreferenced blobs, so this preservation preceded the mutation.

| Identity | Before | After |
| --- | --- | --- |
| Ollama server | `0.40.12` | `0.40.12` |
| Exact selected model | `ministral-3:8b-instruct-2512-q4_K_M` | Unchanged |
| Selected manifest SHA-256 | `100cf353f4fb9447e81485bc81f54ef3ddcc54bbb38d6bd3601d5ff5a02c5250` | `1922accd5827ebe6829e536369195db25eaf664528dc66206d646ea3bb386b71` |
| Model blob SHA-256 | `3b6c20d21e9437966b7c979f24e7ce4f1feea6ac71002a08edc47d1daa5d983c` | `3ea32f882c990baebcfe6ab5cda2e3167d54ab16d7937c0b9aa3db11aba01574` |
| Template and stored sampling parameters | Recorded | Identical |
| Tokenizer loading | Converted artifact bypassed the legacy correction | Native loader explicitly reported correction and `tekken` |

The official same-ID pull succeeded. A load-only request returned
`done_reason: load` with empty response text; it generated no test answer.
The new process loaded the official model blob, the previous converted runner
was absent, and no tokenizer override was supplied. Raw `/api/show` metadata
still says `default`; the native initialization log proves the effective
correction. The first log reader stopped on a non-UTF-8 byte. That diagnostic
failure is retained; ASCII inspection subsequently used replacement decoding
while hashing and preserving the untouched raw bytes.

## Fixed confirmation

Source: clean `cfe5d0cf834664077a5d8fc4ee88aecd5db200f6` on
`codex/prepare-initial-release`. Lock SHA-256:
`5f28a1a14d4ab6a4e5400d843dac2edb4aba4be4bdf6f5a61c840590a418f7d2`.

The unchanged strict-live binary was prepared with:

```bash
cargo test --locked -p providers-kit --test strict_live --all-features \
  --no-run --message-format=json
```

Build settings were `CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, and the existing
`target/docs-site-build` target directory. The binary SHA-256 was
`e268e90373798a4b594f9da377cf373b379bc7b51cb05a644618d43720a5d139`.
This is identical to the prior Provider campaign's final binary hash.
Its complete inventory remained 137 cases. Each invocation selected one exact
case with `--exact --nocapture --test-threads=1`: one passed, zero ignored,
136 filtered. No broad verifier or previously passing live case was rerun.

Before inference, a fixed plan declared ten alternating unary/continuation
pairs. The reviewed private driver invoked the compiled test executable and
retained native HTTP entity bodies through the existing byte-preserving relay.
Complete failures would remain blocking and the fixed sample would continue;
incomplete evidence would stop it. No rerun or sample extension was allowed.

| Round | Unary generation | Function continuation | Both raw exact-token grades | Both final native/Kit text comparisons |
| --- | --- | --- | --- | --- |
| 1 | PASS | PASS | true | Equal |
| 2 | PASS | PASS | true | Equal |
| 3 | PASS | PASS | true | Equal |
| 4 | PASS | PASS | true | Equal |
| 5 | PASS | PASS | true | Equal |
| 6 | PASS | PASS | true | Equal |
| 7 | PASS | PASS | true | Equal |
| 8 | PASS | PASS | true | Equal |
| 9 | PASS | PASS | true | Equal |
| 10 | PASS | PASS | true | Equal |

Result: **20 passed, zero failed, zero incomplete; driver exit 0**. All twenty
fresh tokens were exactly 39 bytes and distinct from each other and the 46
known historical expected tokens. The earlier canonical unary expected token
was not exported, so freshness against that unknown value cannot be checked.
Every final native response stopped successfully. All ten continuations
validated the actual tool call, exact arguments, complete assistant replay,
JSON result, correlation where present, and exact final token.

Thirty native HTTP requests completed with full entity-body capture and
unchanged forwarding. Source, binary, environment, helper, repair-proof and
runtime bindings remained unchanged throughout the sample. Command and relay
processes were reaped; owned process groups were absent, with no cleanup
signals or errors. Ollama remains the externally owned local service; the
model backup is deliberately retained for evidence.

## Receipts, review, and scope

Private evidence is retained under
`target/strict-live-runs/2026-10-29-ollama-repair/`. It includes every trial's
command result, raw response, Kit observation, expected token, exact grade,
cleanup record, and content hashes. Credentials and populated env files are
not committed. Native captures prove entity-body preservation, not packet bytes
or independent observation of client consumption.

| Receipt | SHA-256 |
| --- | --- |
| `same-id-refresh/repair-proof.json` | `195938035c72117ba4161fb5db161f26e4596580fb254a87ef6ba58c3ab2d000` |
| `provider-build.json` | `0b26b7c7b0ad2ef2f2dca30c5f27111f5af3cc3d8c3b26e6e30e5a0455c79497` |
| `fixed-plan.json` | `061cd22ee74ed56741c64d7a88a090e21aa606d54202cdfa9e0ceb4d963244be` |
| `fixed-summary.json` | `1c87f404c24bafd78dd169eaf18a48ab60c9f4114b3efcf188b8728ecae2686b` |
| `post-run-audit.json` | `07a432b3fdc3befe47d48f8cf684da4f3c2c7c6595b0f14e43ba28666f240ad2` |

Twelve offline harness checks passed, including replay of fifteen historical
captures with native failures. One earlier offline test incorrectly expected
a failure among a historical set that had all passed; its failed receipt is
retained, and the fixture now additionally checks the recorded failed candidate
captures. Independent review before live execution resolved a missing
repair-proof binding and accepted the final driver. The driver distinguishes
completed diagnostics from passing assertions.

An independent post-run audit passed: all 311 bound file hashes matched, all
thirty HTTP responses were 200, and every captured native output matched its
normalized observation. All twenty test processes and thirty relay-owned curl
processes settled without errors or signals; relay handlers, children, and
listeners were gone. The audit replayed the preserved bodies through the
unchanged auditors without making new service calls.

The subsequent tracked changes document this local repair and classify this
new report. All 13 documentation-contract tests, repository path/type-alias
guards, and whitespace checks passed. Independent review of the actual diff
found no remaining issues. All five preceding reports retain their original
contents. The [accepted Gemini retention exception](strict-live-delivery-2026-10-29.md)
remains separate: its generated output is still undeleted and expiration is
unverified. That exception does not waive any Ollama assertion.

This affected-case qualification closes the two Ollama blockers using fresh
passing evidence. It does not rewrite the earlier failed canonical command,
represent historical long batches as newly executed, or claim a fresh full
workspace release-wrapper execution.
