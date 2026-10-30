# Manual releases from main

Releases are deliberate maintainer actions. Merging a PR, pushing a commit or
creating a tag does not invoke the release workflow. Merge the reviewed release
preparation PR into `main`, then run **Manual release** in
[GitHub Actions](https://github.com/jecsand838/ai-kit/actions/workflows/release.yml).
The workflow is available in the normal Actions chooser after it reaches the
default branch.

The current preparation targets **0.0.2** across all five crates. Preparing or
merging this change does not publish packages or establish release qualification.
After merge, complete normal qualification for the exact selected `main`
revision before manually dispatching version **0.0.2** with
**initial_release_override disabled** and the required qualification evidence.

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

`GITHUB_TOKEN` supplies the publication job's `contents: write` permission for
the GitHub release. Validation has read-only repository permissions. The
workflow accepts only manual dispatch from this repository's `main`, checks out
the immutable selected commit in both jobs and serializes release attempts
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
`0.0.2` preparation PR is merged, select `main`, enter **0.0.2** without a `v`,
leave **initial_release_override** disabled, and supply that exact 40-character
SHA as `qualification_revision` plus its
`qualification_url`.

Those fields are a **maintainer declaration**. The workflow checks the selected
SHA and URL syntax; it does not independently inspect or certify the linked
receipts. Normal runs additionally execute `bash scripts/check_ci.sh`,
`bash scripts/check_docs_site.sh` and Cargo package verification. Strict-live
and full native qualification remain the maintainer's recorded local evidence;
GitHub CI does not replace them. Storage uses explicit supported feature lanes,
never `--all-features`.

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

Only after all five versions are confirmed does the helper create the GitHub
release and `vVERSION` tag at the selected SHA. An existing tag must resolve to
that exact commit. An existing draft or conflicting release requires maintainer
review. The release lists crates.io links, versioned rustdoc links, source SHA,
qualification scope and the unresolved advisory policy. Validation and
publication artifacts preserve raw audit results, package checksums, command
outcomes and uploaded archives.

crates.io uploads automatically queue **docs.rs** builds from each published
package. No generated HTML is uploaded to docs.rs by this workflow. Versioned
URLs for the prepared release will have the form
`https://docs.rs/agents-kit/0.0.2`. Confirm each docs.rs build after publication;
a successful upload or GitHub release does not prove an
asynchronous documentation build succeeded. This workflow does not deploy the
combined workspace `target/doc` site to GitHub Pages.

Backwards compatibility is not guaranteed until `1.0.0`. Pin versions and
review release notes when upgrading; the initial override does not weaken the
qualification requirements for subsequent releases.
