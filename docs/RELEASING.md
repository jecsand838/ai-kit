# Manual releases from main

Releases are deliberate maintainer actions. Merging a PR, pushing a commit or
creating a tag does not invoke the release workflow. Merge the reviewed release
preparation PR into `main`, then run **Manual release** in
[GitHub Actions](https://github.com/jecsand838/ai-kit/actions/workflows/release.yml).
The workflow is available in the normal Actions chooser after it reaches the
default branch.

All five crates remain available at **0.0.2**; the
[v0.0.2 release](https://github.com/jecsand838/ai-kit/releases/tag/v0.0.2) records
its source revision and qualification declaration. The five manifests and
manual workflow default prepare **0.0.4**. The maintainer yanked all five
**0.0.3** versions and deleted their GitHub release/tag after hosted documentation
was not verified. Preserve the successful package/validation evidence and the
missing-docs observations; neither establishes a docs.rs fault.
After the preparation PR merges, bind the reviewed qualification declaration
to the selected `main` revision and dispatch `0.0.4` with
**initial_release_override disabled**. Preparation alone does not publish.

The breaking change in this candidate is the Provider Session API. Existing
Realtime callers must select a profile and split the Session to submit commands
and receive events. Follow the [Session migration guide](../providers-kit/README.md#breaking-session-migration).
The scoped Session results on [PR #45](https://github.com/jecsand838/ai-kit/pull/45)
apply to the revisions recorded there. The release requires its own qualification.

### v0.0.4 authorized qualification carry-forward

The maintainer explicitly authorized carrying v0.0.3 validation and qualification
forward for this release. Report **QUALIFIED coverage**, with historical runs
attributed to their original sources; do not describe them as newly executed
v0.0.4 tests or forge canonical `--resume` receipts across changed manifests.
The reviewed delta must be confined to version/internal dependency metadata,
three wire client-version identifiers, rustdoc logos, release tooling and its
tests/documentation. External dependencies and features stay unchanged; library
behavior changes only in those three wire-version identifiers. A broader change
requires a new qualification assessment.

| Carried evidence | Original source and scope |
| --- | --- |
| Local workspace, primary CI and Storage alternatives | `3803a58`: six Storage lanes, 16 feature combinations, 294 native case executions; the local wrapper explicitly skipped all five strict-live suites |
| Reviewed native coverage | Provider's 145 named cases across `29c44a5`/`1c537ae`, plus ten final Session/Realtime cases at `c844e60`; Agent 29, App 19 and Tool 1 at `1c537ae` |
| Refreshed rustdoc and clean build | `fa7f798`/identical-tree merge `cd3ae32`: 51 source/public/example cases, 903 doctests, 15 documentation contracts; fresh locked workspace/all-targets check at `cd3ae32` |
| Final v0.0.3 release validation | `953edd6`, [run 41960912812](https://github.com/jecsand838/ai-kit/actions/runs/41960912812): 35 primary stages, eight compile proofs, 147 behavioral tests, seven rustdoc builds, 903 doctests and five verified package archives |

[PR #49](https://github.com/jecsand838/ai-kit/pull/49) retains the evidence chain.
Recheck the retained artifact hashes and independently review the final delta.
Run the three existing wire-version regressions, logo checks, affected tooling
tests, current prerequisite/prepare proofs, fresh doc output and an exact-final-
commit clean-checkout proof. Normal CI/security and publication package checks
remain required. Record fresh results separately in the preparation PR.

Preserve the original native failures, interruptions, collector errors and
[failed ENOSPC release](https://github.com/jecsand838/ai-kit/actions/runs/41944447644).
The seven formerly deferred Provider batch cases have accepted historical passes;
they are not new v0.0.4 executions. Retain the false OpenCode idle-waiter answer-copy
grade under its documented exception, routine App capacity/calibration exclusions,
17 advisory exceptions, four retained Gemini outputs with unverified expiry and
four retained App fault datasets. Carry-forward does not resolve these limitations.

## Historical one-time initial 0.0.1 release

The repository owner explicitly authorized a qualification override for
`0.0.1` to establish the five crates.io names promptly, followed by fixes in
`0.0.2`. This exception applies only to the initial `0.0.1` release; it is not
a standing waiver or a claim of full workspace release qualification.

The **initial_release_override** option requires version **0.0.1** without a `v`
on **main**; only that exception permits leaving the qualification revision
and URL blank. It cannot be used for `0.0.2` or later releases.

The override retains formatting, deterministic Python/tooling contracts,
documentation inventory/link checks, source guards and the real security audit.
It defers strict-live suites, slow integration/capacity tests, Rust mock execution,
rustdoc/doctests and Cargo package build verification. Native Rust compile
checks recorded on the PR remain separate evidence; they do not convert the
release into full qualification. The workflow rejects this override for every
version other than `0.0.1` and records exclusions in the run and release notes.

The current dependency audit contains explicitly accepted vulnerabilities and
unsoundness findings. Its result is `PASS_WITH_EXCEPTIONS`, not a clean audit.
See the [dependency security policy](security-dependencies.md) for the exact
IDs and the commitment to remove them ASAP when compatible upstream fixes
become available. New, unaccepted blocking advisories still fail release.

## Who may release and what to configure

The repository owner may dispatch the workflow. To designate additional release
maintainers, set the repository Actions **variable** `RELEASE_MAINTAINERS` to a
JSON array of exact GitHub usernames, for example `["maintainer-login"]`.
These users also need GitHub permission to dispatch repository workflows.
Both the original actor and a rerun's triggering actor must be the owner or in
this explicit list. Ordinary write access alone does not pass the release gate.

Set the repository Actions **secret** `CARGO_REGISTRY_TOKEN` to a crates.io token
permitted to create and publish all five packages. A crates.io account with
verified email is required. The token is exposed only to the publication step.
Never put it in a repository variable, source file, issue, PR or chat. The first
publication establishes each crate; a later migration to crates.io Trusted
Publishing can replace the long-lived secret after the crates exist.

`GITHUB_TOKEN` supplies the finalization job's `contents: write` permission for
the GitHub release. Validation and crate publication have read-only repository permissions. The
workflow accepts only manual dispatch from this repository's `main`, checks out
the immutable selected commit in all three jobs and serializes release attempts
without cancelling an in-progress upload. It does not use pull-request code or
run automatically when a GitHub release is created.

## Preparing subsequent releases

Prepare and review a new PR that changes the five package versions, internal
dependency constraints, lockfile and owning documentation together. Cargo
versions omit the `v`; GitHub tags use `vVERSION`. The input must match all five
committed manifests. The workflow does not write versions or push a preparation
commit on the maintainer's behalf.

For a normal release, complete the current
[qualification contract](release-qualification.md), retain the reviewed receipts
for the exact main revision and record a stable HTTPS evidence URL. After the
release preparation PR is merged, select `main`, enter the version from all five
committed manifests without a `v`, leave **initial_release_override** disabled,
and supply that exact 40-character SHA as `qualification_revision` plus its
`qualification_url`.

Those fields are a **maintainer declaration**. The workflow checks the selected
SHA and URL syntax; it does not independently inspect or certify the linked
receipts. Normal runs additionally execute `bash scripts/check_ci.sh`,
`bash scripts/check_docs_site.sh` and Cargo package verification. Strict-live
and full native qualification remain the maintainer's recorded local evidence;
GitHub CI does not replace them. Storage uses explicit supported feature lanes,
never `--all-features`.

The disposable Ubuntu release runners remove unused .NET and Android SDKs
before building. Validation shares `target/docs-site-build` between primary CI
and rustdoc/doctests to avoid duplicating dependency outputs. Publication keeps
its separate `target/release-build` archive directory. Both jobs report disk
usage, including after failure. SDK cleanup leaves build outputs and evidence
untouched.

## Publication, documentation and retries

The release helper packages and publishes in dependency order:

1. `providers-kit`
2. `storage-kit`
3. `tools-kit`
4. `agents-kit`
5. `app-kit`

It uses the repository-pinned Rust toolchain and native Cargo `--locked`.
Package verification occurs before upload on normal runs and is explicitly
waived only for the initial override. Uploads are not atomic: an earlier crate
can be published even if a later crate fails. Published versions are immutable;
there is no automatic yank, rollback or version bump.

For each package the helper compares its archive SHA-256 with the crates.io
version checksum. An existing version is reused only when its bytes match the
freshly packaged archive. Different bytes or a yanked version stop the run.
A Cargo error after an upload remains recorded; registry confirmation can
establish that the exact upload succeeded without sending it again. If the
registry cannot confirm publication, the run fails and retains its receipt.

After a partial failure, inspect the logs and publication artifact, then rerun
the same workflow attempt at the same source revision. Do not dispatch a changed
commit at the same version to bypass a checksum mismatch. A fresh attempt at
a newer `main` may legitimately produce different package bytes and will fail
rather than silently adopting another release. Inspect the cause before deciding
whether a new version is required.

After all five versions are confirmed, publication saves a `PACKAGES_PUBLISHED`
receipt and the verified archives. A separate finalization job downloads that
exact artifact ID from the successful publication job. It verifies the original
repository, run, source, version, qualification declaration, archive hashes and
current non-yanked registry checksums before checking hosted documentation.

Finalization waits at most 20 minutes for all five exact-version docs.rs status
responses and crate-root HTML pages. Each page must identify the expected crate
and version and render the reviewed logo URL; the fetched PNG must match the
repository asset. Redirects, wrong versions, missing pages and invalid logos do
not pass. HTTP 404 or `doc_status: false` means not ready, not proof of a failed
build or upstream fault. Per-attempt observations and diagnostics are retained.

Only after all five hosted docsites pass does finalization create the GitHub
release and `vVERSION` tag at the selected SHA. An existing tag must resolve to
that exact commit. An existing draft or conflicting release requires maintainer
review. The release lists crates.io links, versioned rustdoc links, source SHA,
qualification scope and the unresolved advisory policy. Validation and
publication artifacts preserve raw audit results, package checksums, command
outcomes and uploaded archives.

**docs.rs** independently observes published crates and builds their documentation.
No generated HTML is uploaded to docs.rs by this workflow. Versioned
URLs use the form `https://docs.rs/CRATE/VERSION`, such as
`https://docs.rs/agents-kit/0.0.2`. A successful upload alone does not prove the
asynchronous documentation build succeeded. If finalization fails, preserve its
artifact and use **Re-run failed jobs** on the same workflow run: only docs/release
finalization repeats, without Cargo, credentials for crate uploads, or rerunning
qualification. This path depends on retaining the successful publication artifact
(30 days). Missing or conflicting evidence fails closed. There is no automatic
yank, rebuild request, upstream issue, republish or version bump.
Successful checks establish availability at their recorded time, not permanence.
This workflow does not deploy the
combined workspace `target/doc` site to GitHub Pages.

Crate logos use the same public, commit-pinned HTTPS image URL so nested pages
and docs.rs do not depend on the combined site's asset directory. When changing
the logo, verify that URL serves the repository image before publishing.

Backwards compatibility is not guaranteed until `1.0.0`. Pin versions and
review release notes when upgrading; the initial override does not weaken the
qualification requirements for subsequent releases.
