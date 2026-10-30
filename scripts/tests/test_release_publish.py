"""Release admission and immutable publication behavior; no real uploads or Cargo."""

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.release_publish import Release, ReleaseError


class ReleaseTests(unittest.TestCase):
    SHA = "a" * 40

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / "Cargo.toml").write_text("[workspace]\nmembers = " + json.dumps(list(Release.PACKAGES)) + "\n")
        for name in Release.PACKAGES:
            folder = self.root / name
            folder.mkdir()
            (folder / "Cargo.toml").write_text(f'[package]\nname = "{name}"\nversion = "0.0.1"\n')
        self.env = {"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "workflow_dispatch",
                    "GITHUB_REF": "refs/heads/main", "GITHUB_REPOSITORY": Release.REPOSITORY,
                    "GITHUB_SHA": self.SHA, "GITHUB_ACTOR": "jecsand838", "GITHUB_TRIGGERING_ACTOR": "jecsand838",
                    "RELEASE_VERSION": "0.0.1", "INITIAL_RELEASE_OVERRIDE": "true",
                    "GH_TOKEN": "fixture-not-a-token", "CARGO_REGISTRY_TOKEN": "fixture-not-a-token"}
        self.release = Release(self.root, self.env)
        self.git = patch.object(self.release, "git", side_effect=lambda *args: self.SHA if args[0] == "rev-parse" else "")
        self.git.start()
        self.addCleanup(self.git.stop)

    def change(self, **values):
        self.env.update(values)
        self.release.env.update(values)
        self.release.version = self.env["RELEASE_VERSION"]
        self.release.override = self.env["INITIAL_RELEASE_OVERRIDE"] == "true"

    def test_initial_override_is_visible_and_limited_to_initial_version(self):
        self.assertEqual(self.release.admit()["qualification"], "INITIAL_0_0_1_OVERRIDE")
        self.assertIn("strict-live", self.release.report["deferred"])
        self.change(RELEASE_VERSION="0.0.2")
        with self.assertRaisesRegex(ReleaseError, "only for 0.0.1"):
            self.release.admit()

    def test_nonmanual_foreign_or_nonmain_runs_are_rejected(self):
        for key, value in (("GITHUB_ACTIONS", "false"), ("GITHUB_EVENT_NAME", "push"),
                           ("GITHUB_REF", "refs/heads/feature"), ("GITHUB_REPOSITORY", "fork/ai-kit")):
            with self.subTest(key=key), patch.dict(self.release.env, {key: value}):
                with self.assertRaisesRegex(ReleaseError, "manual dispatch"):
                    self.release.admit()

    def test_both_original_actor_and_rerun_actor_need_authority(self):
        for key in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR"):
            with self.subTest(key=key), patch.dict(self.release.env, {key: "collaborator"}):
                with self.assertRaisesRegex(ReleaseError, key):
                    self.release.admit()
        self.change(RELEASE_MAINTAINERS='["Maintainer"]', GITHUB_ACTOR="maintainer", GITHUB_TRIGGERING_ACTOR="MAINTAINER")
        self.release.admit()

    def test_invalid_maintainer_variable_cannot_broaden_access(self):
        for value in ('"everyone"', '{"owner":true}', '[""]', '["bad user"]', '[1]', 'oops'):
            with self.subTest(value=value), patch.dict(self.release.env, {"RELEASE_MAINTAINERS": value}):
                with self.assertRaisesRegex(ReleaseError, "JSON username array"):
                    self.release.admit()

    def test_normal_release_requires_exact_revision_and_https_evidence(self):
        self.change(INITIAL_RELEASE_OVERRIDE="false")
        with self.assertRaisesRegex(ReleaseError, "exact main revision"):
            self.release.admit()
        self.change(QUALIFICATION_REVISION=self.SHA, QUALIFICATION_URL="http://example.invalid/evidence")
        with self.assertRaisesRegex(ReleaseError, "HTTPS"):
            self.release.admit()
        self.change(QUALIFICATION_URL="https://example.invalid/evidence")
        self.assertEqual(self.release.admit()["qualification"], "MAINTAINER_DECLARED")

    def test_normal_followup_verifies_packages_and_retains_qualification(self):
        for name in Release.PACKAGES:
            manifest = self.root / name / "Cargo.toml"
            manifest.write_text(manifest.read_text().replace('0.0.1', '0.0.2'))
        self.change(RELEASE_VERSION="0.0.2", INITIAL_RELEASE_OVERRIDE="false",
                    QUALIFICATION_REVISION=self.SHA, QUALIFICATION_URL="https://example.invalid/v002-evidence")
        self.assertEqual(self.release.admit()["qualification"], "MAINTAINER_DECLARED")
        with patch.object(self.release, "command", side_effect=self.archive_command) as command, patch.object(
                self.release, "registry", side_effect=[None, {"checksum": self.checksum()}]):
            self.release.publish_package("providers-kit")
        self.assertEqual(command.call_args_list[0].args[0],
                         ["cargo", "package", "--locked", "-p", "providers-kit"])
        self.assertEqual(command.call_args_list[1].args[0],
                         ["cargo", "publish", "--locked", "--no-verify", "-p", "providers-kit"])
        notes = self.release.notes()
        self.assertIn(self.SHA, notes)
        self.assertIn(self.env["QUALIFICATION_URL"], notes)
        self.assertNotIn("initial 0.0.1 qualification override", notes)
        self.assertNotIn("were deferred", notes)
        for name in Release.PACKAGES:
            self.assertIn(f"https://crates.io/crates/{name}/0.0.2", notes)
            self.assertIn(f"https://docs.rs/{name}/0.0.2", notes)

    def test_wrong_head_or_dirty_checkout_fails_before_publication(self):
        with patch.object(self.release, "git", return_value="b" * 40):
            with self.assertRaisesRegex(ReleaseError, "exactly match"):
                self.release.admit()
        with patch.object(self.release, "git", side_effect=[self.SHA, " M Cargo.lock"]):
            with self.assertRaisesRegex(ReleaseError, "clean"):
                self.release.admit()

    def test_all_five_manifest_versions_must_match_input(self):
        manifest = self.root / "app-kit/Cargo.toml"
        manifest.write_text(manifest.read_text().replace('0.0.1', '0.0.2'))
        with self.assertRaisesRegex(ReleaseError, "app-kit"):
            self.release.admit()

    def test_version_arguments_cannot_be_shell_fragments_or_v_tags(self):
        for version in ("v0.0.1", "0.0.1; echo bad", "01.0.1", "0.0.1-rc.1", ""):
            with self.subTest(version=version):
                self.release.version = version
                with self.assertRaisesRegex(ReleaseError, "numeric Cargo version"):
                    self.release.admit()

    def test_existing_tag_must_resolve_to_exact_revision(self):
        with patch.object(self.release, "github", return_value={"object": {"type": "commit", "sha": "b" * 40}}):
            with self.assertRaisesRegex(ReleaseError, "another revision"):
                self.release.tag_identity()
        with patch.object(self.release, "github", side_effect=[
            {"object": {"type": "tag", "sha": "c" * 40}},
            {"object": {"type": "commit", "sha": self.SHA}},
        ]):
            self.assertEqual(self.release.tag_identity(), self.SHA)

    def test_registry_identity_rejects_wrong_or_yanked_versions(self):
        base = {"crate": "providers-kit", "num": "0.0.1", "checksum": "a" * 64, "yanked": False}
        for change in ({"crate": "impostor"}, {"num": "0.0.2"}, {"yanked": True}, {"checksum": "short"}):
            with self.subTest(change=change), patch.object(self.release, "json_request", return_value={"version": dict(base, **change)}):
                with self.assertRaisesRegex(ReleaseError, "invalid or yanked"):
                    self.release.registry("providers-kit")

    def archive_command(self, arguments, check=True):
        if arguments[1] == "package":
            name = arguments[arguments.index("-p") + 1]
            archive = self.root / "target/release-build/package" / f"{name}-{self.release.version}.crate"
            archive.parent.mkdir(parents=True, exist_ok=True)
            archive.write_bytes(b"deterministic fixture archive")
        return subprocess.CompletedProcess(arguments, 0)

    @staticmethod
    def checksum():
        return hashlib.sha256(b"deterministic fixture archive").hexdigest()

    def test_partial_retry_reuses_only_identical_archive_without_upload(self):
        with patch.object(self.release, "command", side_effect=self.archive_command) as command, patch.object(
                self.release, "registry", return_value={"checksum": self.checksum()}):
            self.release.publish_package("providers-kit")
        self.assertEqual([call.args[0][1] for call in command.call_args_list], ["package"])
        self.assertEqual(self.release.report["packages"][0]["status"], "ALREADY_PUBLISHED_IDENTICAL")

    def test_partial_retry_rejects_conflicting_published_bytes(self):
        with patch.object(self.release, "command", side_effect=self.archive_command) as command, patch.object(
                self.release, "registry", return_value={"checksum": "b" * 64}):
            with self.assertRaisesRegex(ReleaseError, "different bytes"):
                self.release.publish_package("providers-kit")
        self.assertEqual(command.call_count, 1)

    def test_successful_upload_requires_registry_checksum_confirmation(self):
        with patch.object(self.release, "command", side_effect=self.archive_command) as command, patch.object(
                self.release, "registry", side_effect=[None, {"checksum": self.checksum()}]):
            self.release.publish_package("providers-kit")
        self.assertEqual([call.args[0][1] for call in command.call_args_list], ["package", "publish"])
        self.assertEqual(self.release.report["packages"][0]["status"], "PUBLISHED")

    def test_ambiguous_upload_error_is_retained_without_reupload(self):
        def execute(arguments, check=True):
            result = self.archive_command(arguments, check)
            result.returncode = 1 if arguments[1] == "publish" else 0
            return result
        with patch.object(self.release, "command", side_effect=execute) as command, patch.object(
                self.release, "registry", side_effect=[None, {"checksum": self.checksum()}]):
            self.release.publish_package("providers-kit")
        self.assertEqual(command.call_count, 2)
        self.assertEqual(self.release.report["packages"][0]["publish_exit"], 1)
        self.assertEqual(self.release.report["packages"][0]["status"], "PUBLISHED_AFTER_CARGO_ERROR")

    def test_unconfirmed_upload_fails_without_creating_github_release(self):
        with patch.object(self.release, "command", side_effect=self.archive_command), patch.object(
                self.release, "registry", return_value=None), patch("scripts.release_publish.time.sleep"):
            with self.assertRaisesRegex(ReleaseError, "not confirmed"):
                self.release.publish_package("providers-kit")

    def test_github_release_is_created_only_after_every_package_in_dependency_order(self):
        observed = []
        def github(suffix, **kwargs):
            if kwargs.get("method") == "POST":
                self.assertEqual(observed, list(Release.PACKAGES))
                self.assertEqual(kwargs["payload"]["target_commitish"], self.SHA)
                return {"html_url": "https://example.invalid/release"}
            return None
        with patch.object(self.release, "github", side_effect=github), patch.object(
                self.release, "publish_package", side_effect=observed.append):
            self.release.publish()
        self.assertEqual(self.release.report["status"], "PUBLISHED")

    def test_failure_retains_receipt_and_never_creates_release(self):
        with patch.object(self.release, "github", return_value=None) as github, patch.object(
                self.release, "publish_package", side_effect=ReleaseError("fixture failure")):
            with self.assertRaisesRegex(ReleaseError, "fixture failure"):
                self.release.publish()
        self.assertTrue(all(call.kwargs.get("method") != "POST" for call in github.call_args_list))
        self.assertEqual(json.loads((self.root / "target/release/report.json").read_text())["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
