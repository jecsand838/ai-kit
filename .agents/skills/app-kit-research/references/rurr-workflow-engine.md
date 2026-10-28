# Fixed rurr Workflow-Engine Evidence

## Authority Boundary

The approved fixed rurr pin governs overlapping core observable semantics, as
recorded in
[`workflows/RESEARCH.md`](../../../../app-kit/specs/workflows/RESEARCH.md).
[`app-kit/specs/SPECIFICATIONS.md`](../../../../app-kit/specs/SPECIFICATIONS.md)
owns exact public signatures; owning specs define App translations and explicit
exclusions. Newer external versions are background, never a silent oracle
upgrade. Do not add a rurr dependency, extract its API into app-kit, copy its
persistence records, or reuse its public names.

## Snapshot Identity And Safe Access

Use only this fixed repository artifact:

- Archive: `app-kit/vendor/rurr-main.zip`
- Upstream commit: `005bfe3ece631e64b06dac2d42ef84235f727a4f`
- SHA-256: `5b5f20eb1d233467c600dc9dd249feaaa9046c5a303ae4e937040be5b6ecf25e`

Verify the checksum before reading. If it differs, stop using the snapshot.
Read exact members without extracting:

```bash
shasum -a 256 app-kit/vendor/rurr-main.zip
unzip -Z1 app-kit/vendor/rurr-main.zip
unzip -p app-kit/vendor/rurr-main.zip rurr-main/ARCHITECTURE.md
```

Do not create an unpacked tree, refresh the snapshot, fetch a replacement, run
vendor automation, or modify the archive.

## Architectural Path Map

| Concern | Archive members | Inspect for |
|---|---|---|
| Workspace | `README.md`, `ARCHITECTURE.md` | Crate roles and runtime boundaries |
| Runtime | `crates/rurr-core/ARCHITECTURE.md` | Assembly, sessions, steps, children |
| Graph | `crates/rurr-core/src/graph.rs`, `action/condition.rs` | Validation, entry, transitions, conditions |
| Validation | `crates/rurr-core/src/validation.rs`, matching contract tests | Reachability, integrity, failure cases |
| Expressions | `crates/rurr-expr/src/parse.rs`, `validate.rs`, `value.rs`, `eval/` | Parsing, validation, typed evaluation |
| Assembly | `application/builder.rs`, `application/build/` | Build validation versus execution |
| Execution | `application/api.rs`, `application/session.rs` | Progression and terminal behavior |
| Controls | `application/execution_options.rs`, `halt_controls.rs` | Halt validation and boundaries |
| Commit | `application/step_txn.rs`, `transaction.rs`, `state_meta.rs` | Prepared state, ordering, rollback |
| Resume | `application/initialize_from_spec.rs`, `build/initializer.rs` | Initial and restored state |
| Streaming | `application/streaming/` | Progress, completion, rollback coupling |
| Children | `parallelism/` | Fan-out, child context, join, cancellation |
| State | `state.rs`, `state/` | Typed updates and reserved runtime state |
| Persistence | `crates/rurr-persistence/src/lib.rs`, `layout.rs`, `spec.rs`, `traits.rs` | Checkpoint and backend separation |
| Guardrails | `docs/ARCHITECTURE_GUARDRAILS.md`, `docs/compatibility/SEMANTICS.md`, `DIVERGENCES.md` | Intended semantics and divergence |

Archive paths are relative to `rurr-main/`. Tests corroborate a snapshot claim;
they never prove app-kit behavior.

## Evidence Extraction

For each relevant concern, cite the exact member, paraphrase its behavior, map
it to the owning app spec, classify alignment, and record the validation,
execution, state, checkpoint, failure, concurrency, or lifecycle question it
raises. Preserve the adopted core controls, streaming, history, child execution
and persistence outcomes. Server/UI, CLI and exporter products remain excluded;
rurr macros and native API/record shapes do not become App implementation
permissions. A genuinely unplanned capability requires the owning amendment.

The snapshot proves only what its checked-in files state or implement at the
fixed commit. It proves no app compatibility, correctness, performance, or
runtime behavior.
