# Continuing release qualification

The canonical release owners are the five crate verifiers and
[`verify_workspace_strict.sh`](../scripts/verify_workspace_strict.sh). A normal
invocation still executes their full required inventories. The workspace wrapper
covers default Lance strict-live behavior; the supported local Storage matrix
remains an additional requirement. App's routine workload selector retains its
documented capacity exclusions. Neither primary mocks nor earlier release
receipts replace native service, persistence, or final clean-checkout proof.

When a release has already produced passing evidence, explicitly continue with
an audited evidence bundle:

```bash
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 \
  bash scripts/verify_workspace_strict.sh --resume /absolute/path/to/bundle.json
```

The continuation first validates its runner contracts, then admits Storage's
`check.sh --all-backends` and executes the actual
workspace wrapper, sequentially. In continuation mode the wrapper runs Provider
last, so its long batch waits follow the other independent crate owners. The owning scripts still select every
command, feature lane, native grade reporter, and prerequisite. Independent
owners continue after a failure; the aggregate remains unsuccessful. Continuation
never enables the seven optional Provider long-batch exclusions. Full Provider
execution retains the test-authored batch deadlines, including 24-hour jobs.

## An explicitly limited local pass

When the user explicitly defers all strict-live execution for a pass, append
`--skip-strict-live` to the continuation command. The local Storage matrix,
compiler, ordinary integration, documentation, and structural gates still run
or use eligible unchanged evidence. The five native/service suites are omitted:
Agent, Provider, Storage and Tool `strict_live`, and App `workflow_live`.
Local native persistence and process-contract tests retain their ordinary roles.

This mode does not load strict-live environment files, launch external native
servers, run their prechecks, or call provider APIs. Existing contract tests
that validate those prechecks still execute. The receipt and wrapper explicitly
report `QUALIFIED_NONLIVE`, list all five omissions, and cannot establish full
workspace release qualification. Normal invocation continues to require all
five verifiers. The separate final-commit clean-checkout proof remains required
for the local pass too.

## Eligible evidence

[`release_evidence.py`](../scripts/release_evidence.py) records each command as
`EXECUTED` or `REUSED`; the latter always references an original executed or
reviewed imported receipt. Reuse requires all of the following:

- A successful, complete original command with retained, hash-verified raw logs.
- The same effective command, compiler, features, build configuration and relevant
  source inputs, including fixtures, dependencies, lockfile and toolchain.
- Complete test names and counts, with no unexplained ignored or filtered cases.
- Verified runtime configuration identity when reusing live evidence. Current
  nonempty environment variables cannot establish historical identity.
- Settled command ownership and unchanged inputs during the original execution.

For the workspace doctest commands, a build-output directory change alone does
not change validation identity: the audited examples do not observe that path.
Receipts retain the original arguments and name this equivalence explicitly.
Features, test filters, thread settings, compiler flags and ordinary integration
commands still require exact matching. Rustdoc publication builds remain fresh.

A source-scope reduction needs a separate review explaining each excluded input.
For example, another independent Cargo integration target is not automatically a
runtime input to the selected target; a source-contract test reading that target
or its scripts has a wider scope. A changed relevant input invalidates the owning
receipt. Missing provenance is incomplete evidence, even when the old log says
`ok`. Failed or interrupted parent runs retain that status; only independently
proven, completed constituent commands may be imported. An import records its
original invocation and any reviewed derivation rather than claiming that a
newly split command ran historically.

Unknown stages execute normally. A declared reuse entry that fails verification
fails closed; it does not silently rerun or choose another result. Review the
reason, retain the failed evidence, and create a new explicit continuation bundle
when execution is necessary. No automatic retry turns a failure into a pass.

An explicit `storage.owner.local_matrix` binding may reuse the complete local
Storage owner. Its reviewed imported certificate must prove all eight supported
lanes, every nested gate, all 24 legal feature combinations, and all eight
incompatible combinations with their retained compiler diagnostics. The driver
checks every nested receipt against current inputs and records the owner as
`REUSED` with its original evidence references. An absent binding executes the
owner normally; an invalid or incomplete binding remains incomplete while the
independent workspace wrapper continues. Runner bootstrap always executes first.
Root documentation publication still runs freshly, including default-Lance
restoration; owner reuse does not substitute historical pages for current output.

Fresh prerequisite checks remain required. App's four `prepare_contract` cases
must run together in the current check invocation, even if an older receipt
passed. Rustdoc publication builds execute to establish current artifact state,
including restoring default Lance pages after alternate Storage builds. Doctest
evidence can be reused separately when its inputs are verified unchanged.

## Receipts and process ownership

Each continuation writes a private directory beneath `target/release-qualification/`
with the bundle, command logs, immutable stage receipts, source fingerprints,
compiler identity, timings, inventories and aggregate status. Evidence is local
and ignored by Git. Do not put credentials, local environment files, generated
artifacts, or an environment dump into a bundle or commit. Runtime identities
use only explicitly selected nonsecret settings.

Only one coordinator may own release execution. Run Cargo sequentially; coordinate
with other active validation before starting. The process owner settles its
children before advancing. Storage retains its dataset/service ownership rules;
uncertain child settlement prevents destructive cleanup. Do not stop preexisting
unowned native servers. Use the documented isolated homes and pinned native
versions, preserving credentials and exact model selections.

The wrapper continuation receipt and the final clean-checkout proof are separate.
After reviewed changes are committed, prepare a genuinely clean checkout at that
commit using the documented vendor preparation, then run the locked workspace
check with a fresh target directory. Keep the source commit, lockfile identity,
command, exit status and cleanup evidence. Never infer this result from a dirty
workspace or an earlier commit.

Report newly executed passes, reused passes, failures and incomplete work
separately. The dated [PR #22 receipts](pr22-validation.md) retain historical
results; [the production bug ledger](mock-testing-production-bugs.md) owns defect
status. A passing precheck or primary mock suite alone is not workspace release
qualification.
