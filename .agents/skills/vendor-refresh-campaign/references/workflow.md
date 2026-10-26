# Campaign Workflow

Use this file for the exact `ai-kit` top-level vendor campaign sequence.

## Preflight

1. Run `git status --porcelain --untracked-files=no` and fail if tracked files
   are dirty.
2. Run `git branch --show-current` and fail if it returns an empty branch name.
3. Treat that branch as the landing branch for the full campaign.
4. For each in-scope crate whose vendor manifest runs a strict-live verify
   completion gate, confirm the crate-local `strict-tests.env` file exists and
   is populated before the first updater run. Today that means
   `agent-sdk/strict-tests.env` and `storage-sdk/strict-tests.env`.
5. For `agent-sdk` Codex vendor refreshes, confirm the Codex app-server used by
   strict-live matches the target vendored Codex release. When the installed
   `codex --version` matches the target tag, use that installed binary. When it
   does not, install the matching stable package through the user's existing
   package manager or download the official full `codex-package-*` release
   asset, verify the release-published SHA256, extract it under
   `target/codex-releases/<tag>/`, and invoke that package's `bin/codex` by
   path. Never validate a vendor update against a different Codex version.

## Discovery

1. Run `python3 scripts/vendor_update.py check --json`.
2. Filter to `status == "update-available"` within the requested scope.
3. Preserve the JSON order exactly: crates stay grouped and vendors stay in
   manifest order.
4. If nothing is outdated, report a no-op and stop without creating commits.

## Execution

For each queued vendor, run:

```bash
python3 scripts/vendor_update.py update --crate <crate> --vendor <vendor> --git-mode current-branch
```

If the user explicitly allows pre-existing failures while prohibiting broad
backlog implementation, use:

```bash
python3 scripts/vendor_update.py update --crate <crate> --vendor <vendor> --git-mode current-branch --completion-policy no-regression
```

No-regression mode runs configured preflight gates strictly, captures every
completion gate before and after adaptation, persists raw logs and normalized
diagnostics in the ignored dossier, and blocks pass-to-fail transitions or new
failure signatures. Existing failures are not implementation work unless the
vendor change directly owns them.

Rules:

- Stay on the starting branch for the whole campaign.
- Accept one local commit per successful vendor update.
- Stop on the first failed vendor update.
- The default `strict` completion policy is unchanged. Use `no-regression`
  only when the user's scope explicitly requires it.
- Let updater-managed completion gates source the crate-local strict-live env
  files they require; do not rely on ambient shell state.
- Leave prepared artifacts and implementation edits intact if a vendor run
  fails after dossier preparation. The updater restores the exact original
  lock on implementation, target preflight, gate, timeout, or interruption
  failure before commit; both source archives and extracts remain available.
  If interruption occurs after the local commit lands, preserve the committed
  target and report its HEAD. An unexpected changed HEAD is preserved for
  inspection rather than overwritten by rollback.
- Command interruption or timeout remains the primary failure when owned-process
  cleanup also fails. Report descendant termination as unverified; an exited
  command leader does not prove its descendants stopped. Resolve owned-process
  cleanup before resuming the dossier. Lock or archive restoration failures also
  remain supplementary to an existing failure or interruption; restoration
  attempts continue for the remaining owned archives. A failed restoration
  after otherwise successful adaptation still fails the invocation.
- The updater publishes its candidate lock before implementation, then runs
  target preflight after specifications have been adapted and before completion
  gates. No-regression still runs its original strict preflight and baseline
  gates before planning; target preflight never substitutes for that baseline.
- Resume a failed prepared update with its existing target and saved plan:

  ```bash
  python3 scripts/vendor_update.py resume --dossier target/vendor-updates/<crate>/<vendor>/<tag>
  ```

  Resume validates the recorded workspace, branch, HEAD, manifest, dossier
  metadata, both archive hashes, and exact original/candidate lock bytes.
  It rejects unrelated dirty paths and symlink escapes. Its permitted change
  scope is the owning crate plus the manifest's external local focus paths;
  other vendor artifacts remain outside that scope. Unchanged pre-existing
  untracked files are preserved and excluded from staging. Resume never
  resolves a newer release or regenerates the saved plan. Ordinary `update`
  still requires a clean tracked worktree.
- Archive cleanup and scoped staging occur only after all selected gates pass.
  The updater commits only campaign-owned paths, never a workspace-wide add.

## Final Validation

### One prerequisite commit outside a frozen vendor scope

When a failed strict leaf is blocked by an already-authorized campaign
prerequisite outside its recorded vendor scope, the parent may land that
prerequisite separately on the same campaign branch. It must be exactly one
commit whose sole parent is the leaf's recorded starting HEAD. Review the
prerequisite diff first and commit only its exact files; retain the failed
vendor implementation edits, both archives, extracts, and original dossier.
Wait for the failed updater and its owned processes to finish before doing so.
This exception concerns preparation after a prerequisite, not vendor acceptance
or permission to implement unrelated backlog.

Ordinary `resume` still rejects every changed HEAD. Prepare a separate recovery
dossier using the full prerequisite commit ID and every committed file path:

```bash
python3 scripts/vendor_update.py import-prerequisite \
  --dossier <failed-single-vendor-dossier> \
  --destination target/vendor-updates/<crate>/<vendor>/<tag>-prerequisite \
  --commit <full-prerequisite-HEAD> \
  --path <exact-committed-file> \
  --path <another-exact-committed-file>
```

`--path` is repeatable and must match the commit's changed files exactly,
including both sides of a rename. Directories, omitted files, duplicate paths,
and extra paths do not provide an outside-scope waiver. The commit must not
touch `Cargo.lock`, any recorded vendor scope (including other external
paths), any vendor artifact directory, or `tmp/**`. Unrelated dirty
paths remain errors. The command accepts only an original version-1 strict,
current-branch single-vendor dossier, never an imported or coordinated dossier,
a merge, unrelated history, or an already-landed vendor update.

Preparation requires the updater-restored original lock and a new ignored
destination under this workspace's `target/vendor-updates/`, separate from
the original dossier, without symlink redirection. It preserves the original
dossier and records its complete file inventory and hashes. The new dossier
copies the frozen plan, metadata, release evidence, diff summaries, and exact
lock snapshots; `source-resume.json` retains the original recovery bytes.
Its own `resume.json` records the prerequisite HEAD and immutable source
lineage, without changing the original scope, target, plan, gates, or locks.
Preparation performs no discovery, model call, implementation, gate, lock
publication, vendor cleanup, or commit.

After independent review, continue with:

```bash
python3 scripts/vendor_update.py resume --dossier <new-prerequisite-dossier>
```

Resume revalidates the exact prerequisite ancestry and paths, original source
inventory and hashes, copied inputs, branch, HEAD, manifest, archives, locks,
and original dirty-path boundary before implementation and again before
staging. Recovery metadata also remains byte-for-byte unchanged during the
run. Keep the original dossier read-only, including historical evidence;
write new evidence only in the new dossier. Do not resume the original leaf,
manually change recovery inputs, or rerun ordinary `update` over retained
edits. Strict target preflight and all completion gates still precede vendor
cleanup, scoped staging, and the updater's one vendor commit. Failures retain
both dossiers and implementation edits and restore the original lock.

### Explicitly authorized coordinated Agent pair

When the human explicitly authorizes preparing both Agent targets before the
whole-crate gate, a failed strict Codex dossier may be imported into a separate
coordinated dossier. This is an exception to intermediate sequencing and the
one-commit-per-vendor rule, not to strict acceptance. Ordinary updates and
single-vendor resume retain all existing requirements.
Prerequisite-import dossiers cannot be chained into a coordinated pair.

Save a decision-complete OpenCode plan, then prepare without running any model,
implementation, service, acceptance gate, lock publication, or commit:

```bash
python3 scripts/vendor_update.py coordinate --dossier <existing-codex-dossier> --vendor opencode --target-tag <approved-stable-tag> --plan <saved-opencode-plan>
```

Preparation validates the retained Codex recovery inputs, checks that both
approved tags are still latest stable, downloads the official OpenCode source
archive, records its hash, and copies frozen inputs into
`target/vendor-updates/agent-sdk/coordinated/<codex-tag>_<opencode-tag>/`.
The original leaf's recovery metadata, plans, and evidence stay unchanged.
Preparation permits only the Agent scope, manifest-owned external paths, and
the exact coordination updater/test/workflow files. Unrelated dirty paths,
changed branch/HEAD, altered archives, and unrecognized locks remain errors.
Existing target dossiers are never overwritten.

The pair requires identical nonempty preflight and completion commands for
both vendors. Its two explicit execution phases are:

```bash
python3 scripts/vendor_update.py resume --dossier <coordinated-dossier> --implementation-only
# Independently review the actual combined diff and resolve findings.
python3 scripts/vendor_update.py resume --dossier <coordinated-dossier> --validation-only
```

Each phase validates the frozen plans, imported input hashes, branch, HEAD,
manifest, both vendors' current and target archives, and change scope. Current
and target artifact paths may coincide when a vendor is already current.
Resume never resolves a newer release. The implementation phase installs the combined candidate lock and
adapts Codex then OpenCode; focused checks belong to those implementations.
Copied dossier directories and implementation output must remain within their
coordinated owner, without symlink redirection. The updater revalidates frozen
inputs and scope after each implementation, before dispatching the next vendor,
and preserves the exact recovery metadata bytes throughout the phase.
It restores the exact original lock on return, including success, and creates
no commit. The parent owns independent review and evidence-directed focused
live checks; there is no intermediate whole-crate acceptance against old
OpenCode. Do not manually edit either dossier's locks or recovery metadata.

The implementation prompt identifies the validated original import directory
for read-only historical references omitted from the copied dossier. Those
references do not supersede the coordinated recovery contract or this workflow;
historical results and service-state assumptions do not establish current
acceptance or runtime state. Never write into or resume an imported original
leaf. Keep original and copied frozen inputs unchanged; write new evidence only
in the corresponding coordinated leaf and implementation edits only within the
recorded scope for that vendor.

After review, validation installs the combined candidate and runs target
preflight followed by the complete common manifest gates once. It does not
rerun implementation. Both mandatory Question receipts and all other strict
assertions remain required. Any failure, timeout, or interruption before commit
restores the exact original lock and retains all archives, extracts, plans,
and edits. Only successful strict gates permit superseded-artifact cleanup,
scoped staging, and one local commit for the pair. Cleanup removes a current
archive or extracted directory only when its path differs from the corresponding
target path; selected artifacts with unchanged paths are preserved. There is no
push. Inspect an unexpected changed HEAD without automatic rollback, as in
ordinary resume.

The final validation block below still applies. Combined historical successes,
model substitution, deadline increases, output normalization, skipped cases,
and retry-until-pass cannot establish campaign completion.

After the queue completes, always rerun the exact workspace baseline in the
[validation matrix](../../ai-kit-validation-matrix/references/validation.md):
formatting, non-Storage/non-App workspace Clippy, every Storage lane selected
by `feature_policy.py --all-backends`, and the docs-site gate. Preserve serial
Cargo execution and native `--locked`; App retains its separate owning gate,
and Storage `--all-features` remains prohibited. Do not replace these final
gates with the updater's earlier receipts or treat no-regression completion
as permission to omit a gate.

Then rerun the exact crate validation blocks for every crate in scope from the
owning `AGENTS.md` files.

## Parallelism

- Default to a single parent agent.
- Only use subagents when the user explicitly asks.
- Keep subagents read-only and limit them to release-note triage, upstream diff
  review, or gate-failure diagnosis.
