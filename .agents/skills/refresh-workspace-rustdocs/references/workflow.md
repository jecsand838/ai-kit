# Workspace Rustdoc Refresh Workflow

Use this file for the exact `ai-kit` workspace rustdoc refresh sequence.

## Inputs

1. Treat current `HEAD` as the implementation source of truth.
2. Use the official rustdoc writing guide as external guidance when crate-root
   docs, examples, or rustdoc sections need confirmation:
   - `https://doc.rust-lang.org/stable/rustdoc/how-to-write-documentation.html`
3. Use public-source files, focused source-contract tests, and
   `scripts/check_docs_site.sh` as the local authority for current
   expectations.

## Inventory

1. Inventory every rustdoc-visible public item for the crates in scope.
2. Mark which items need crate-root or module `//!` docs, runnable examples,
   `# Errors`, `# Panics`, or `# Safety`.
3. Identify the items that may justify `no_run` because they require live
   services, credentials, or subprocesses.

## Execution

1. Refresh crate-root and public-module `//!` docs first.
2. Refresh public item `///` docs after the crate and module framing is
   correct.
3. Prefer runnable examples for pure or local APIs and reserve `no_run` for
   real live-service or subprocess entrypoints.
4. Update focused source-contract tests and the shared docs gate together when
   rustdoc expectations change.

## Validation

Run these commands serially from the workspace root after the refresh, with
`CARGO_INCREMENTAL=0` and `CARGO_BUILD_JOBS=1`. Preserve the separate Storage
feature lanes and native `--locked` behavior:

```bash
cargo test --locked -p agent-sdk --test source_contract --all-features -- --nocapture
cargo test --locked -p provider-sdk --test source_contract --all-features -- --nocapture
cargo test --locked -p storage-sdk --test source_contract -- --nocapture
cargo test --locked -p storage-sdk --test source_contract --no-default-features -- --nocapture
cargo test --locked -p storage-sdk --test source_contract --no-default-features --features qdrant -- --nocapture
cargo test --locked -p tool-sdk --test source_contract --all-features -- --nocapture
cargo test --locked -p app-sdk --test public_surface --test docs_examples -- --nocapture --test-threads=1
bash scripts/check_docs_site.sh
```

The docs-site gate requires Python 3.11+ and runs
`python3 -m unittest scripts.tests.test_documentation_contract` before Cargo.
Those checks reconcile the documentation ledger, skill catalog, and local
file links; they do not replace Rust source contracts, doctests, or App's
existing public-surface and guide/example checks.

## Rules

- Never suppress rustdoc warnings, doc-test failures, or contract failures.
- Never leave compile-only async helper shells on pure local examples when the
  example can actually run.
- If the task reduces to markdown-only docs, redirect to
  `refresh-workspace-agentic-documentation`.
