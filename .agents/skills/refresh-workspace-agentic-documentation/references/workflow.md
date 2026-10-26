# Workspace Documentation Refresh Workflow

Use this file for the exact `ai-kit` workspace documentation refresh sequence.

## Inputs

1. Select governing authority before inspecting current repository state:
   explicit user direction first, followed by the active workflow's selected
   specs and instructions.
2. Inspect current `HEAD` and diff the current branch against `main` only after
   authority selection. Treat them as implementation-status evidence, not as
   competing authority when the active workflow is spec-led.
3. Record every conflict between selected authority and current source, tests,
   implementation-status docs, or rustdoc as drift for later implementation
   work.
4. Use the official `AGENTS.md`, `ARCHITECTURE.md`, and Codex AGENTS docs as
   external guidance when documentation structure or instruction layering is
   part of the refresh:
   - `https://agents.md/`
   - `https://architecture.md/`
   - `https://developers.openai.com/codex/guides/agents-md`

## Inventory

1. Inventory every in-scope repo-owned markdown file and skill/agent metadata
   file, including skill `agents/openai.yaml` default prompts and invocation
   policies. Review their consistency with the owning skill; preserve policies
   unless the user requests a change.
2. Reconcile the inventory with `docs/documentation-ledger.toml`. Record frozen
   requirements incorporated by current owners through explicit
   `adopted_requirements` entries with their source, adopting authority, and
   bounded scope. Ordinary links do not incorporate every referenced document.
3. Preserve one canonical explanatory owner for repeated facts. Applicable
   instruction documents must still state mandatory rules, prohibitions,
   exceptions, validation requirements, and authority precedence explicitly.
   Consolidation may remove repeated explanation, never weaken an obligation;
   map every moved or shortened requirement to its resulting explicit location.
4. Separate authority surfaces from implementation-status surfaces so an
   authority-only reconciliation cannot erase or silently absorb remaining
   implementation work.
5. Keep adopted requirements inside historical documents distinct from those
   documents' dated status, draft signatures, phase instructions, and receipts.
   Adoption neither activates the old workflow nor makes old results current
   validation. Current owning specs retain exact signatures and explicit
   refinements. Do not rewrite dated evidence to imply a current pass.

## Execution

1. Refresh root `README.md`, `ARCHITECTURE.md`, `AGENTS.md`, and `SKILLS.md`
   when the branch changes workspace-wide behavior or skill inventory.
2. Refresh crate docs, focused guides, and skill/agent guidance and metadata
   when their owning code, scripts, tests, or validation flows change.
3. Update docs-contract tests in the same change whenever skill inventory,
   workflow boundaries, or required workspace artifacts change.
4. Keep historical execution evidence clearly marked as historical. Preserve
   explicitly adopted frozen requirements within their recorded scope without
   promoting the entire historical document to active guidance.
5. When the active workflow explicitly authorizes authority-only
   preimplementation reconciliation, edit only its declared authority surfaces
   and ledger entries. Leave source, tests, rustdoc, and implementation-status
   docs unchanged, and carry their recorded drift into the later implementation
   workflow.

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
```

Then run the shared docs gate:

```bash
bash scripts/check_docs_site.sh
```

The docs-site gate requires Python 3.11+ and runs
`python3 -m unittest scripts.tests.test_documentation_contract` before Cargo.
Those checks reconcile the documentation ledger, skill/agent metadata and
adopted-requirement inventory, skill catalog, enabled agent skill paths and
supported local file links. A ledger-declared local-evidence exemption permits
missing ignored artifacts only for its exact source and contained target prefix;
it checks documentation portability, not receipt validity, and never makes a
historical result current. Ordinary links must still resolve. These checks do
not interpret YAML policies or prose authority, and do not replace Rust source
contracts, doctests, or App's existing public-surface and guide/example checks.

## Rules

- Never suppress warnings or doc-test failures.
- Keep `SKILLS.md` compact and keep the detailed workflow in the owning skill
  docs and references.
- For spec-led work, distinguish accepted contracts from current evidence.
  Record actual implementation gaps without assuming a historical Custom or
  other implementation delta remains open. Preserve completed fixes and
  failed or incomplete historical evidence; later passes do not erase it.
- If the task reduces to public rustdoc only, redirect to
  `refresh-workspace-rustdocs` or `rust-public-api-docs`.
