import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock


class CoordinatedVendorTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "vendor_update", Path(__file__).resolve().parents[1] / "vendor_update.py")
        self.module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.module
        spec.loader.exec_module(self.module)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.vendor = self.root / "agent-sdk/vendor"
        self.vendor.mkdir(parents=True)
        (self.root / ".gitignore").write_text("target/\nagent-sdk/vendor/*/\n")
        self.updater = self.module.VendorUpdater(self.root, stdout=io.StringIO())
        manifest = "version = 1\n"
        locks = {}
        for name in ("codex", "opencode"):
            manifest += f'''\n[vendors.{name}]
repo = "acme/{name}"
source_kind = "github-release-source"
channel = "stable"
release_tag_prefix = "v"
archive_prefix = "{name}"
upstream_focus_paths = ["README.md"]
local_focus_paths = ["agent-sdk/src"]
plan_prompt_suffix = "Adapt selected release."
preflight_gates = [["preflight"]]
completion_gates = [["complete"]]
'''
            archive = self.archive(self.vendor / f"{name}-1.0.0.zip", "old")
            locks[name] = self.module.VendorLock("v1.0.0", archive.name, f"{name}-1.0.0",
                                                "2026-01-01", self.updater._sha256(archive))
        (self.vendor / "vendors.toml").write_text(manifest)
        self.updater._write_vendor_lock(self.vendor / "vendor-lock.toml", locks)
        self.original = b"# preserve bytes\r\n" + (self.vendor / "vendor-lock.toml").read_bytes()
        (self.vendor / "vendor-lock.toml").write_bytes(self.original)
        self.git("init", "-b", "update-vendors")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.test")
        self.git("add", ".")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "-m", "base")
        self.source = self.root / "target/vendor-updates/agent-sdk/codex/v2.0.0"
        self.source.mkdir(parents=True)
        current = self.updater._load_crate_state("agent-sdk")
        target = self.archive(self.vendor / "codex-2.0.0.zip", "new codex")
        metadata = {"current": vars(locks["codex"]), "target": {
            "tag": "v2.0.0", "archive": target.name, "extracted_dir": "codex-2.0.0",
            "published_at": "2026-02-01", "sha256": self.updater._sha256(target)}}
        (self.source / "metadata.json").write_text(json.dumps(metadata))
        (self.source / "dossier.md").write_text("# Retained Codex evidence\n")
        (self.source / "plan.md").write_text("# Retained Codex plan\n")
        self.updater._prepare_resume_state(current, current.configs["codex"], self.source,
            locks["codex"], self.module.ReleaseInfo("v2.0.0", "2026-02-01", False),
            {"publishedAt": "2026-02-01"}, self.updater._sha256(target), "update-vendors",
            "current-branch", "strict", {}, ())
        self.source_bytes = {p.name: p.read_bytes() for p in self.source.iterdir()}
        self.plan = self.root / "target/opencode-plan.md"
        self.plan.write_text("# Adapt OpenCode; keep final acceptance strict\n")
        self.download = self.archive(self.root / "target/download.zip", "new opencode")

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, check=True,
                              capture_output=True, text=True).stdout.strip()

    def archive(self, path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("repo/README.md", text)
        return path

    def prepare(self, opencode_tag="v2.0.0"):
        unchanged = opencode_tag == "v1.0.0"
        published_at = "2026-01-01" if unchanged else "2026-02-01"
        releases = {
            "codex": self.module.ReleaseInfo("v2.0.0", "2026-02-01", False),
            "opencode": self.module.ReleaseInfo(opencode_tag, published_at, False),
        }
        download = self.vendor / "opencode-1.0.0.zip" if unchanged else self.download
        with mock.patch.object(self.updater, "_resolve_latest_release", side_effect=
                lambda config: releases[config.name]), \
             mock.patch.object(self.updater, "_fetch_release_details", return_value=
                {"publishedAt": published_at, "tagName": opencode_tag}), \
             mock.patch.object(self.updater, "_download_release_archive", return_value=download):
            return self.updater.coordinate(self.source, "opencode", opencode_tag, self.plan)

    def test_preparation_preserves_leaf_and_original_lock_and_freezes_both_targets(self):
        dossier = self.prepare()
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
        self.assertEqual({p.name: p.read_bytes() for p in self.source.iterdir()}, self.source_bytes)
        state = json.loads((dossier / "resume.json").read_text())
        self.assertEqual(list(state["vendors"]), ["codex", "opencode"])
        self.assertEqual(state["completion_policy"], "strict")
        self.assertEqual((dossier / "codex/plan.md").read_bytes(), self.source_bytes["plan.md"])

    def test_coordination_rejects_prerequisite_import_before_discovery(self):
        for name in ("release.json", "upstream_diff_stat.txt", "upstream_changed_files.txt"):
            (self.source / name).write_text("{}\n")
        prerequisite = self.root / "app-sdk/src/lib.rs"
        prerequisite.parent.mkdir(parents=True)
        prerequisite.write_text("//! Corrected rustdoc link.\n")
        self.git("add", "--", "app-sdk/src/lib.rs")
        self.git("commit", "-m", "fix prerequisite docs")
        destination = self.source.with_name("v2.0.0-prerequisite")
        self.updater.import_prerequisite(
            self.source, destination, self.git("rev-parse", "HEAD"), ["app-sdk/src/lib.rs"])
        with mock.patch.object(self.updater, "_resolve_latest_release", side_effect=AssertionError("discovery")), \
             mock.patch.object(self.updater, "_run_codex_implementation", side_effect=AssertionError("implementation")):
            with self.assertRaisesRegex(self.module.VendorUpdateError, "original strict"):
                self.updater.coordinate(destination, "opencode", "v2.0.0", self.plan)
        self.assertFalse((self.root / "target/vendor-updates/agent-sdk/coordinated").exists())
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)

    def test_all_implementations_precede_common_gates_and_failure_restores_every_archive(self):
        dossier = self.prepare()
        events = []
        def implement(crate, config, *args, **kwargs):
            locks = self.updater._load_crate_state("agent-sdk").locks
            self.assertEqual([lock.tag for lock in locks.values()], ["v2.0.0", "v2.0.0"])
            events.append(config.name)
            (self.root / "agent-sdk/retained.txt").write_text("keep edit")
        def fail(*args):
            self.assertEqual([lock.tag for lock in self.updater._load_crate_state("agent-sdk").locks.values()],
                             ["v2.0.0", "v2.0.0"])
            events.append("complete")
            raise self.module.VendorUpdateError("gate failed")
        head = self.git("rev-parse", "HEAD")
        with mock.patch.object(self.updater, "_resolve_latest_release", side_effect=AssertionError("resolved")), \
             mock.patch.object(self.updater, "_run_codex_implementation", side_effect=implement), \
             mock.patch.object(self.updater, "_run_preflight_gates", side_effect=lambda _: events.append("preflight")), \
             mock.patch.object(self.updater, "_run_completion_gates", side_effect=fail):
            self.updater.resume(dossier, implementation_only=True)
            self.assertEqual(events, ["codex", "opencode"])
            self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
            with self.assertRaisesRegex(self.module.VendorUpdateError, "gate failed"):
                self.updater.resume(dossier, validation_only=True)
        self.assertEqual(events, ["codex", "opencode", "preflight", "complete"])
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual(self.git("diff", "--cached", "--name-only"), "")
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
        for name in ("codex", "opencode"):
            for version in ("1.0.0", "2.0.0"):
                self.assertTrue((self.vendor / f"{name}-{version}.zip").is_file())
                self.assertTrue((self.vendor / f"{name}-{version}").is_dir())
        self.assertEqual((self.root / "agent-sdk/retained.txt").read_text(), "keep edit")

    def test_resume_rejects_changed_head_branch_archive_lock_plan_and_unrelated_dirt(self):
        dossier = self.prepare()
        for path, content in [
            (self.vendor / "vendor-lock.toml", b"unrecognized"),
            (self.vendor / "opencode-2.0.0.zip", b"tampered"),
            (dossier / "codex/plan.md", b"altered plan"),
            (self.root / "unrelated.txt", b"unrelated"),
        ]:
            with self.subTest(path=path):
                original = path.read_bytes() if path.exists() else None
                path.write_bytes(content)
                with self.assertRaises(self.module.VendorUpdateError):
                    self.updater.resume(dossier, validation_only=True)
                if original is None:
                    path.unlink()
                else:
                    path.write_bytes(original)
        for method in ("_current_head", "_current_branch_name"):
            with mock.patch.object(self.updater, method, return_value="changed"):
                with self.assertRaises(self.module.VendorUpdateError):
                    self.updater.resume(dossier, validation_only=True)

    def test_interruption_preserves_original_lock(self):
        dossier = self.prepare()
        with mock.patch.object(self.updater, "_run_codex_implementation", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.updater.resume(dossier, implementation_only=True)
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)

    def test_interruption_survives_restore_failures_and_all_archives_are_attempted(self):
        dossier = self.prepare()
        write_bytes = Path.write_bytes
        materialize = self.updater._materialize_extract_if_missing
        attempts = []
        interrupted = KeyboardInterrupt("implementation interrupted")
        stderr = io.StringIO()

        def deny_restore(path, data):
            if path == self.vendor / "vendor-lock.toml" and data == self.original:
                raise OSError("restore denied")
            return write_bytes(path, data)

        def restore_archive(archive, destination):
            if (self.vendor / "vendor-lock.toml").read_bytes() != self.original:
                attempts.append(archive.name)
                if archive.name == "codex-1.0.0.zip":
                    raise self.module.VendorUpdateError("archive restore denied")
            return materialize(archive, destination)

        with mock.patch.object(Path, "write_bytes", deny_restore), \
             mock.patch.object(self.updater, "_materialize_extract_if_missing", side_effect=restore_archive), \
             mock.patch.object(self.updater, "_run_codex_implementation", side_effect=interrupted), \
             mock.patch.object(self.updater, "stderr", stderr):
            with self.assertRaises(KeyboardInterrupt) as failure:
                self.updater.resume(dossier, implementation_only=True)
        self.assertIs(failure.exception, interrupted)
        self.assertEqual(attempts, ["codex-1.0.0.zip", "opencode-1.0.0.zip"])
        self.assertIn("restore denied", stderr.getvalue())
        self.assertIn("archive restore denied", stderr.getvalue())
        self.assertEqual(self.git("diff", "--cached", "--name-only"), "")
        for name in ("codex", "opencode"):
            for version in ("1.0.0", "2.0.0"):
                self.assertTrue((self.vendor / f"{name}-{version}.zip").is_file())

    def test_successful_adaptation_with_failed_lock_restore_is_not_success(self):
        dossier = self.prepare()
        write_bytes = Path.write_bytes

        def deny_restore(path, data):
            if path == self.vendor / "vendor-lock.toml" and data == self.original:
                raise OSError("restore denied")
            return write_bytes(path, data)

        with mock.patch.object(Path, "write_bytes", deny_restore), \
             mock.patch.object(self.updater, "_run_codex_implementation"):
            # An unrelated handled caller error must not make this operation's
            # restoration failure supplementary to a successful adaptation.
            try:
                raise ValueError("caller handled this")
            except ValueError:
                with self.assertRaisesRegex(self.module.VendorUpdateError, "restore denied"):
                    self.updater.resume(dossier, implementation_only=True)
        self.assertEqual(self.git("diff", "--cached", "--name-only"), "")

    def test_copied_leaf_and_output_symlinks_cannot_write_the_original_dossier(self):
        dossier = self.prepare()
        copied = dossier / "codex"
        saved = dossier / "saved-codex"
        original_output = self.source / "implementation.md"
        original_output.write_text("original evidence")
        for redirected in ("directory", "output"):
            with self.subTest(redirected=redirected):
                if redirected == "directory":
                    copied.rename(saved)
                    copied.symlink_to(self.source, target_is_directory=True)
                else:
                    (copied / "implementation.md").symlink_to(original_output)
                def implement(crate, config, leaf, *args, **kwargs):
                    (leaf / "implementation.md").write_text("overwritten evidence")
                try:
                    with mock.patch.object(self.updater, "_run_codex_implementation", side_effect=implement) as implementation:
                        with self.assertRaises(self.module.VendorUpdateError):
                            self.updater.resume(dossier, implementation_only=True)
                        implementation.assert_not_called()
                    self.assertEqual(original_output.read_text(), "original evidence")
                    self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
                finally:
                    if redirected == "directory":
                        copied.unlink()
                        saved.rename(copied)
                    else:
                        (copied / "implementation.md").unlink()
                    original_output.write_text("original evidence")

    def test_frozen_inputs_are_rechecked_before_the_next_vendor_runs(self):
        dossier = self.prepare()
        head = self.git("rev-parse", "HEAD")
        for frozen in (dossier / "opencode/plan.md", dossier / "resume.json"):
            with self.subTest(frozen=frozen):
                original = frozen.read_bytes()
                calls = []
                def implement(crate, config, *args, **kwargs):
                    calls.append(config.name)
                    if config.name == "codex":
                        frozen.write_bytes(original + b"\n")
                try:
                    with mock.patch.object(self.updater, "_run_codex_implementation", side_effect=implement):
                        with self.assertRaises(self.module.VendorUpdateError):
                            self.updater.resume(dossier, implementation_only=True)
                    self.assertEqual(calls, ["codex"])
                    self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
                    self.assertEqual(self.git("rev-parse", "HEAD"), head)
                finally:
                    frozen.write_bytes(original)

    def test_dispatched_prompt_resolves_historical_references_read_only(self):
        (self.source / "dossier.md").write_text("Read `implementation-checkpoint.md` before continuing.\n")
        checkpoint = self.source / "implementation-checkpoint.md"
        checkpoint.write_text("Historical evidence; preserve this original leaf.\n")
        dossier = self.prepare()
        state = json.loads((dossier / "resume.json").read_text())
        commands = []
        run = self.updater._run
        def capture(command, **kwargs):
            if command[:2] == ["codex", "exec"]:
                commands.append(command)
                return subprocess.CompletedProcess(command, 0, "", "")
            return run(command, **kwargs)
        with mock.patch.object(self.updater, "_run", side_effect=capture):
            self.updater.resume(dossier, implementation_only=True)
        self.assertEqual(len(commands), 2)
        for name, command in zip(("codex", "opencode"), commands):
            prompt = command[-1]
            self.assertIn(str(Path(state["imports"][name]["path"])), prompt)
            self.assertIn(str(dossier / "resume.json"), prompt)
            self.assertIn(str(self.root / ".agents/skills/vendor-refresh-campaign/references/workflow.md"), prompt)
            self.assertIn("relative historical references", prompt)
            self.assertIn("read-only", prompt)
            self.assertIn("Do not write to the original", prompt)
            self.assertIn("frozen inputs", prompt)
            self.assertIn("New evidence", prompt)
            self.assertIn("supersedes", prompt)
            self.assertIn("parent must review both", prompt)
            self.assertEqual(command[command.index("--output-last-message") + 1],
                             str(dossier / name / "implementation.md"))
        self.assertFalse((dossier / "codex/implementation-checkpoint.md").exists())
        self.assertEqual(checkpoint.read_text(), "Historical evidence; preserve this original leaf.\n")
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)

    def test_import_rejects_unrelated_edits_and_target_advance(self):
        (self.root / "unrelated.txt").write_text("not campaign owned")
        with self.assertRaises(self.module.VendorUpdateError):
            self.prepare()
        (self.root / "unrelated.txt").unlink()
        with mock.patch.object(self.updater, "_resolve_latest_release", return_value=
                self.module.ReleaseInfo("v3.0.0", "2026-03-01", False)):
            with self.assertRaisesRegex(self.module.VendorUpdateError, "advanced|target"):
                self.updater.coordinate(self.source, "opencode", "v2.0.0", self.plan)

    def test_success_commits_both_archives_only_after_strict_validation(self):
        dossier = self.prepare()
        events = []
        with mock.patch.object(self.updater, "_run_codex_implementation", side_effect=AssertionError("implementation during validation")), \
             mock.patch.object(self.updater, "_run_preflight_gates", side_effect=lambda _: events.append("preflight")), \
             mock.patch.object(self.updater, "_run_completion_gates", side_effect=lambda _: events.append("complete")):
            self.updater.resume(dossier, validation_only=True)
        self.assertEqual(events, ["preflight", "complete"])
        self.assertEqual(self.git("status", "--porcelain"), "")
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(),
                         (dossier / "candidate-vendor-lock.toml").read_bytes())
        for name in ("codex", "opencode"):
            self.assertFalse((self.vendor / f"{name}-1.0.0.zip").exists())
            self.assertTrue((self.vendor / f"{name}-2.0.0.zip").exists())

    def test_success_preserves_unchanged_target_archive_and_extract(self):
        original_lock = self.updater._load_crate_state("agent-sdk").locks["opencode"]
        dossier = self.prepare(opencode_tag="v1.0.0")
        archive = self.vendor / original_lock.archive
        readme = self.vendor / original_lock.extracted_dir / "README.md"
        archive_bytes = archive.read_bytes()
        readme_bytes = readme.read_bytes()
        events = []
        with mock.patch.object(self.updater, "_run_codex_implementation", side_effect=AssertionError("implementation during validation")), \
             mock.patch.object(self.updater, "_run_preflight_gates", side_effect=lambda _: events.append("preflight")), \
             mock.patch.object(self.updater, "_run_completion_gates", side_effect=lambda _: events.append("complete")):
            self.updater.resume(dossier, validation_only=True)
        self.assertEqual(events, ["preflight", "complete"])
        self.assertTrue(archive.is_file(), "the selected unchanged archive must survive cleanup")
        self.assertTrue(readme.is_file(), "the selected unchanged extraction must survive cleanup")
        self.assertEqual(archive.read_bytes(), archive_bytes)
        self.assertEqual(readme.read_bytes(), readme_bytes)
        self.assertEqual(self.updater._load_crate_state("agent-sdk").locks["opencode"], original_lock)
        self.assertEqual(self.git("ls-files", "--error-unmatch", "agent-sdk/vendor/opencode-1.0.0.zip"),
                         "agent-sdk/vendor/opencode-1.0.0.zip")
        self.assertEqual(self.git("show", "HEAD:agent-sdk/vendor/vendor-lock.toml"),
                         (dossier / "candidate-vendor-lock.toml").read_text().strip())
        self.assertFalse((self.vendor / "codex-1.0.0.zip").exists())
        self.assertFalse((self.vendor / "codex-1.0.0").exists())
        self.assertTrue((self.vendor / "codex-2.0.0.zip").is_file())
        self.assertTrue((self.vendor / "codex-2.0.0/README.md").is_file())
        self.assertEqual(self.git("status", "--porcelain"), "")

    def test_commit_failure_preserves_unchanged_target_and_restores_recovery(self):
        dossier = self.prepare(opencode_tag="v1.0.0")
        archive = self.vendor / "opencode-1.0.0.zip"
        readme = self.vendor / "opencode-1.0.0/README.md"
        archive_bytes = archive.read_bytes()
        readme_bytes = readme.read_bytes()
        head = self.git("rev-parse", "HEAD")
        at_commit = []
        run = self.updater._run
        def fail_commit(command, **kwargs):
            if command[:2] == ["git", "commit"]:
                at_commit.append((archive.is_file(), readme.is_file()))
                raise self.module.VendorUpdateError("injected commit failure")
            return run(command, **kwargs)
        with mock.patch.object(self.updater, "_run", side_effect=fail_commit), \
             mock.patch.object(self.updater, "_run_codex_implementation", side_effect=AssertionError("implementation during validation")), \
             mock.patch.object(self.updater, "_run_preflight_gates"), \
             mock.patch.object(self.updater, "_run_completion_gates"):
            with self.assertRaisesRegex(self.module.VendorUpdateError, "injected commit failure"):
                self.updater.resume(dossier, validation_only=True)
        self.assertEqual(at_commit, [(True, True)], "unchanged targets must remain present before rollback")
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
        self.assertEqual(archive.read_bytes(), archive_bytes)
        self.assertEqual(readme.read_bytes(), readme_bytes)
        for name in ("codex-1.0.0", "codex-2.0.0", "opencode-1.0.0"):
            self.assertTrue((self.vendor / f"{name}.zip").is_file())
            self.assertTrue((self.vendor / name / "README.md").is_file())
        self.assertEqual(set(self.git("diff", "--cached", "--name-only").splitlines()), {
            "agent-sdk/vendor/codex-1.0.0.zip",
            "agent-sdk/vendor/codex-2.0.0.zip",
            "agent-sdk/vendor/vendor-lock.toml",
        })

    def test_default_resume_cannot_commit_coordinated_work_without_explicit_phase(self):
        dossier = self.prepare()
        with self.assertRaisesRegex(self.module.VendorUpdateError, "exactly one"):
            self.updater.resume(dossier)

    def test_preflight_failure_and_symlink_escape_preserve_original_inputs(self):
        dossier = self.prepare()
        with mock.patch.object(self.updater, "_run_preflight_gates", side_effect=self.module.VendorUpdateError("preflight")), \
             mock.patch.object(self.updater, "_run_completion_gates", side_effect=AssertionError("ran gates")):
            with self.assertRaisesRegex(self.module.VendorUpdateError, "preflight"):
                self.updater.resume(dossier, validation_only=True)
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
        (self.root / "agent-sdk/escape").symlink_to(self.root / "target", target_is_directory=True)
        with self.assertRaises(self.module.VendorUpdateError):
            self.updater.resume(dossier, validation_only=True)

    def test_combined_candidate_can_resume_but_original_leaf_cannot_accept_it(self):
        dossier = self.prepare()
        candidate = (dossier / "candidate-vendor-lock.toml").read_bytes()
        (self.vendor / "vendor-lock.toml").write_bytes(candidate)
        with mock.patch.object(self.updater, "_run_codex_implementation") as implementation:
            self.updater.resume(dossier, implementation_only=True)
        self.assertEqual(implementation.call_count, 2)
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
        (self.vendor / "vendor-lock.toml").write_bytes(candidate)
        with self.assertRaises(self.module.VendorUpdateError):
            self.updater.resume(self.source)
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), candidate)

    def test_recorded_input_and_all_archive_tampering_is_rejected(self):
        dossier = self.prepare()
        for path in [self.source / "original-vendor-lock.toml", self.source / "plan.md",
                     self.vendor / "vendors.toml", dossier / "original-vendor-lock.toml",
                     *self.vendor.glob("*.zip")]:
            with self.subTest(path=path):
                old = path.read_bytes()
                path.write_bytes(old + b"\n# altered\n")
                with self.assertRaises(self.module.VendorUpdateError):
                    self.updater.resume(dossier, validation_only=True)
                path.write_bytes(old)

    def test_timeout_has_no_commit_and_no_archive_cleanup(self):
        dossier = self.prepare()
        head = self.git("rev-parse", "HEAD")
        with mock.patch.object(self.updater, "_run_codex_implementation", side_effect=
                               self.module.VendorUpdateError("implementation timed out")):
            with self.assertRaisesRegex(self.module.VendorUpdateError, "timed out"):
                self.updater.resume(dossier, implementation_only=True)
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual((self.vendor / "vendor-lock.toml").read_bytes(), self.original)
        self.assertEqual(len(list(self.vendor.glob("*.zip"))), 4)


class VendorUpdateProcessCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        spec = importlib.util.spec_from_file_location(
            "vendor_update_cleanup", Path(__file__).resolve().parents[1] / "vendor_update.py")
        self.module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.module
        spec.loader.exec_module(self.module)
        self.workspace = Path.cwd()
        self.stderr = io.StringIO()
        self.updater = self.module.VendorUpdater(
            self.workspace, env={"CLEANUP_REGRESSION": "1"},
            stdout=io.StringIO(), stderr=self.stderr,
        )

    def test_interruption_remains_primary_when_final_group_kill_is_denied(self) -> None:
        primary = KeyboardInterrupt("explicit interruption")
        denied = PermissionError(1, "Operation not permitted")
        # The leader has exited, but a descendant may still own the group.
        process = mock.Mock(pid=1234567, returncode=0)
        process.communicate.side_effect = [primary, ("", "")]
        with mock.patch.object(self.module.subprocess, "Popen", return_value=process), \
                mock.patch.object(self.module.os, "killpg", side_effect=[None, denied]) as killpg:
            with self.assertRaises(KeyboardInterrupt) as caught:
                self.updater._run(["mock-gate"], cwd=self.workspace, description="mock gate")
        self.assertIs(caught.exception, primary)
        self.assertIs(caught.exception.__cause__, denied)
        self.assertTrue(any("termination is unverified" in note
                            for note in caught.exception.__notes__))
        self.assertEqual(killpg.call_args_list, [
            mock.call(process.pid, self.module.signal.SIGTERM),
            mock.call(process.pid, self.module.signal.SIGKILL),
        ])

    def test_timeout_and_cleanup_denial_both_remain_visible_at_cli_boundary(self) -> None:
        primary = self.module.subprocess.TimeoutExpired(["mock-gate"], 0.25)
        denied = PermissionError(1, "Operation not permitted")
        process = mock.Mock(pid=1234567, returncode=0)
        process.communicate.side_effect = [primary, ("", "")]
        with mock.patch.object(self.module.subprocess, "Popen", return_value=process), \
                mock.patch.object(self.module.os, "killpg", side_effect=[None, denied]):
            with self.assertRaises(self.module.VendorUpdateError) as caught:
                self.updater._run(["mock-gate"], cwd=self.workspace, description="mock gate",
                                  timeout_seconds=0.25)
        self.assertIs(caught.exception.__cause__, denied)
        self.assertIs(denied.__context__, primary)
        with mock.patch.object(self.module.VendorUpdater, "check", side_effect=caught.exception), \
                mock.patch.object(self.module.signal, "signal"):
            code = self.module.main(["check"], workspace_root=self.workspace,
                                    env={"CLEANUP_REGRESSION": "1"},
                                    stdout=io.StringIO(), stderr=self.stderr)
        self.assertEqual(code, 1)
        diagnostic = self.stderr.getvalue()
        self.assertIn("mock gate timed out after 0.25s", diagnostic)
        self.assertIn("PermissionError", diagnostic)
        self.assertIn("termination is unverified", diagnostic)

    def test_exited_leader_does_not_skip_descendant_group_cleanup(self) -> None:
        primary = KeyboardInterrupt("explicit interruption")
        process = mock.Mock(pid=1234567, returncode=0)
        process.communicate.side_effect = [primary, ("", ""), ("", "")]
        with mock.patch.object(self.module.subprocess, "Popen", return_value=process), \
                mock.patch.object(self.module.os, "killpg") as killpg:
            with self.assertRaises(KeyboardInterrupt) as caught:
                self.updater._run(["mock-gate"], cwd=self.workspace, description="mock gate")
        self.assertIs(caught.exception, primary)
        self.assertIsNone(caught.exception.__cause__)
        self.assertEqual(killpg.call_args_list, [
            mock.call(process.pid, self.module.signal.SIGTERM),
            mock.call(process.pid, self.module.signal.SIGKILL),
        ])
        self.assertEqual(process.communicate.call_count, 3)

    def test_absent_process_group_preserves_original_timeout_and_reaps_leader(self) -> None:
        primary = self.module.subprocess.TimeoutExpired(["mock-gate"], 0.25)
        process = mock.Mock(pid=1234567, returncode=0)
        process.communicate.side_effect = [primary, ("", ""), ("", "")]
        with mock.patch.object(self.module.subprocess, "Popen", return_value=process), \
                mock.patch.object(self.module.os, "killpg", side_effect=ProcessLookupError) as killpg:
            with self.assertRaises(self.module.VendorUpdateError) as caught:
                self.updater._run(["mock-gate"], cwd=self.workspace, description="mock gate",
                                  timeout_seconds=0.25)
        self.assertIs(caught.exception.__cause__, primary)
        self.assertEqual(str(caught.exception), "mock gate timed out after 0.25s")
        self.assertEqual(killpg.call_count, 2)
        self.assertEqual(process.communicate.call_count, 3)


if __name__ == "__main__":
    unittest.main()
