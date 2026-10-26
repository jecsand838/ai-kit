# Dossier Contract

The updater prepares a dossier under:

`target/vendor-updates/<crate>/<vendor>/<tag>/`

## Required Inputs

- `dossier.md`
  Primary human-readable summary of the update. Start here.
- `metadata.json`
  Machine-readable context for the crate, vendor, old and new archive names,
  focus paths, and completion gates.
- `resume.json`, `original-vendor-lock.toml`, and `candidate-vendor-lock.toml`
  Updater-owned recovery evidence: starting branch and HEAD, exact lock
  snapshots, input/archive hashes, permitted change scope, and any recorded
  no-regression baseline. Do not edit these files. The candidate lock is
  installed before implementation and target validation; failure restores
  the original bytes without discarding implementation edits before commit.
  An already-landed local commit remains authoritative after interruption.
- `release.json`
  Release metadata returned from upstream, including tag, publish time, release
  notes, and source archive URL.
- `upstream_diff_stat.txt`
  Diffstat between the previously committed vendor archive and the new archive.
- `upstream_changed_files.txt`
  Changed upstream file paths for the release-to-release diff.
- `completion-gates/report.json` when the updater selects no-regression
  completion. This records baseline and post results, normalized diagnostic
  identities, comparisons, and regressions. Adjacent `*.stdout.log` and
  `*.stderr.log` files retain raw gate output.
- For a prerequisite import, `source-resume.json` preserves the original
  recovery bytes and the new `resume.json` records the prerequisite commit and
  original dossier file inventory/hashes. The copied frozen inputs retain the
  same target, plan, scope, gates, and locks. The original dossier is read-only,
  including historical evidence; new output belongs in the imported dossier.
  The campaign's prerequisite preparation contract owns this transition.

## Reading Order

1. Read `dossier.md` for the end-to-end summary.
2. Use `metadata.json` to anchor exact local paths, archive names, extracted
   review directories, and validation commands.
3. Use `release.json` and the upstream diff files to decide whether the change
   is protocol, transport, config, docs, or test-surface drift.
4. Treat `upstream_focus_paths` and `local_focus_paths` as the primary review
   scope, not an exhaustive limit.
5. Treat `plan_prompt_suffix` as vendor-specific nuance layered on top of the
   shared workflow.
6. In no-regression mode, read `completion-gates/report.json` before treating
   any failing gate as work. Unchanged baseline failures stay outside the
   vendor plan unless the sourced release drift directly owns them.

## Output Expectations

- Planning mode writes a decision-complete `plan.md`.
- Execution mode uses `plan.md` plus the dossier to update code, tests, and
  docs.
- If a gate fails, diagnose the failure against the dossier context, fix the
  crate state, and leave the worktree ready for the updater to rerun the gates.
