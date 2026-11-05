"""Release admission and immutable publication behavior; no real uploads or Cargo."""

import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.error

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
                    "GITHUB_RUN_ID": "1234", "GITHUB_RUN_ATTEMPT": "1",
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

    def test_validation_reuses_the_docs_build_directory_for_both_gates(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/release.yml").read_text()
        validation = workflow.split("  validate:\n", 1)[1].split("  publish:\n", 1)[0]
        docs = (root / "scripts/check_docs_site.sh").read_text()
        target = re.search(r'DOC_BUILD_TARGET_DIR="\$\{WORKSPACE_DIR\}/([^\"]+)"', docs)[1]
        self.assertIn("CARGO_TARGET_DIR: ${{ github.workspace }}/" + target,
                      validation.split("    steps:\n", 1)[0])
        self.assertIn("bash scripts/check_ci.sh\n          bash scripts/check_docs_site.sh", validation)

    def test_publication_keeps_its_archive_directory_despite_inherited_target(self):
        self.release.env["CARGO_TARGET_DIR"] = str(self.root / "target/docs-site-build")
        with patch("scripts.release_publish.Runner.owned_process", return_value=subprocess.CompletedProcess([], 0)) as run:
            self.release.command(["cargo", "package", "--locked", "-p", "providers-kit"])
        self.assertEqual(run.call_args.kwargs["env"]["CARGO_TARGET_DIR"],
                         str(self.release.root / "target/release-build"))

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

    def test_publication_stops_after_every_package_without_creating_github_release(self):
        observed = []
        def github(suffix, **kwargs):
            self.assertNotEqual(kwargs.get("method"), "POST")
            return None
        with patch.object(self.release, "github", side_effect=github), patch.object(
                self.release, "publish_package", side_effect=observed.append):
            self.release.publish()
        self.assertEqual(observed, list(Release.PACKAGES))
        self.assertEqual(self.release.report["status"], "PACKAGES_PUBLISHED")

    def test_failure_retains_receipt_and_never_creates_release(self):
        with patch.object(self.release, "github", return_value=None) as github, patch.object(
                self.release, "publish_package", side_effect=ReleaseError("fixture failure")):
            with self.assertRaisesRegex(ReleaseError, "fixture failure"):
                self.release.publish()
        self.assertTrue(all(call.kwargs.get("method") != "POST" for call in github.call_args_list))
        self.assertEqual(json.loads((self.root / "target/release/report.json").read_text())["status"], "FAIL")

    def publication_receipt(self):
        self.release.admit()
        report = dict(self.release.report, status="PACKAGES_PUBLISHED", repository=Release.REPOSITORY,
                      run_id="1234", run_attempt="1", packages=[])
        folder = self.root / "downloaded/release"
        folder.mkdir(parents=True, exist_ok=True)
        for name in Release.PACKAGES:
            archive = folder.parent / "release-build/package" / f"{name}-{self.release.version}.crate"
            archive.parent.mkdir(parents=True, exist_ok=True)
            archive.write_bytes(b"deterministic fixture archive")
            report["packages"].append({"name": name, "version": self.release.version, "status": "PUBLISHED",
                                       "sha256": self.checksum(),
                                       "archive": f"target/release-build/package/{archive.name}"})
        path = folder / "report.json"
        path.write_text(json.dumps(report))
        self.logo = b"\x89PNG\r\n\x1a\nfixture logo"
        logo_path = self.root / "docs/assets/ai-kit-logo.png"
        logo_path.parent.mkdir(parents=True, exist_ok=True)
        logo_path.write_bytes(self.logo)
        self.logo_url = f"https://raw.githubusercontent.com/{Release.REPOSITORY}/{self.SHA}/docs/assets/ai-kit-logo.png"
        for name in Release.PACKAGES:
            source = self.root / name / "src/lib.rs"
            source.parent.mkdir()
            source.write_text(f'#![doc(html_logo_url = "{self.logo_url}")]\n')
        self.release.env.pop("CARGO_REGISTRY_TOKEN", None)
        return path

    def docs_response(self, url, **kwargs):
        self.assertTrue(kwargs["exact"])
        self.assertLessEqual(kwargs["timeout"], 30)
        if url == self.logo_url:
            body, content_type = self.logo, "image/png"
        elif url.endswith("status.json"):
            body, content_type = json.dumps({"version": self.release.version, "doc_status": True}).encode(), "application/json"
        else:
            name = url.split("/")[3]
            crate = name.replace("-", "_")
            body = (f'<html><meta name="rustdoc-vars" data-current-crate="{crate}">'
                    f'<div class="sidebar-crate"><img src="{self.logo_url}" alt="logo">'
                    f'<span class="version">{self.release.version}</span></div>'
                    f'<h1>Crate <span>{crate}</span></h1></html>').encode()
            content_type = "text/html; charset=utf-8"
        return {"status": 200, "url": url, "headers": {"content-type": content_type}, "body": body}

    def finalize_fixture(self, path, response=None):
        observed = []
        def github(suffix, **kwargs):
            if kwargs.get("method") == "POST":
                observed.append(kwargs["payload"])
                self.assertEqual(len(self.release.report["docs"]["packages"]), 5)
                self.assertTrue(all(row["status"] == "READY" for row in self.release.report["docs"]["packages"]))
                return {"html_url": "https://example.invalid/release"}
            return None
        with patch.object(self.release, "request", side_effect=response or self.docs_response), patch.object(
                self.release, "registry", return_value={"checksum": self.checksum()}), patch.object(
                self.release, "github", side_effect=github), patch.object(self.release, "command") as command:
            self.release.finalize(path)
        command.assert_not_called()
        return observed

    def test_finalization_verifies_docs_and_archive_without_cargo_or_upload_token(self):
        path = self.publication_receipt()
        original = path.read_bytes()
        self.release.env["GITHUB_RUN_ATTEMPT"] = "2"
        observed = self.finalize_fixture(path)
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]["target_commitish"], self.SHA)
        self.assertEqual(self.release.report["status"], "PUBLISHED")
        self.assertEqual(path.read_bytes(), original)
        self.assertTrue(list((self.release.output / "docs").glob("*")))

    def test_finalization_rechecks_release_binding_after_docs_wait(self):
        path = self.publication_receipt()
        for transition in ("missing-tag", "draft"):
            with self.subTest(transition=transition):
                state = {"docs_ready": False}
                def github(suffix, **kwargs):
                    self.assertNotEqual(kwargs.get("method"), "POST")
                    if suffix.startswith("git/ref/tags/"):
                        if state["docs_ready"] and transition == "missing-tag":
                            return None
                        return {"object": {"type": "commit", "sha": self.SHA}}
                    self.assertEqual(suffix, "releases/tags/v" + self.release.version)
                    return {"html_url": "https://example.invalid/existing-release",
                            "draft": state["docs_ready"] and transition == "draft"}
                with patch.object(self.release, "registry", return_value={"checksum": self.checksum()}), patch.object(
                        self.release, "github", side_effect=github), patch.object(
                        self.release, "wait_for_docs", side_effect=lambda: state.update(docs_ready=True)):
                    with self.assertRaisesRegex(ReleaseError, "draft or unbound"):
                        self.release.finalize(path)
                self.assertTrue(state["docs_ready"])
                self.assertEqual(self.release.report["status"], "FAIL")

    def test_finalization_rejects_untrusted_or_incomplete_receipt(self):
        path = self.publication_receipt()
        original = json.loads(path.read_text())
        for change in ({"revision": "b" * 40}, {"version": "0.0.2"}, {"run_id": "999"},
                       {"repository": "other/repo"}, {"run_attempt": "2"}, {"status": "FAIL"},
                       {"packages": original["packages"][:-1]}, {"packages": original["packages"][:1] * 5},
                       {"qualification": "invented"}, {"qualification_url": "https://other.invalid"}):
            with self.subTest(change=change):
                path.write_text(json.dumps(dict(original, **change)))
                with patch.object(self.release, "github") as github, patch.object(self.release, "command") as command:
                    with self.assertRaises(ReleaseError):
                        self.release.finalize(path)
                self.assertFalse(any(call.kwargs.get("method") == "POST" for call in github.call_args_list))
                command.assert_not_called()
        path.write_text("[]")
        with self.assertRaises(ReleaseError):
            self.release.finalize(path)

    def test_finalization_preserves_receipt_even_if_input_is_the_output_path(self):
        path = self.publication_receipt()
        self.release.output.mkdir(parents=True)
        same_path = self.release.output / "report.json"
        same_path.write_bytes(path.read_bytes())
        original = same_path.read_bytes()
        with self.assertRaisesRegex(ReleaseError, "immutable"):
            self.release.finalize(same_path)
        self.assertEqual(same_path.read_bytes(), original)

    def test_finalization_rejects_tampered_package_fields(self):
        path = self.publication_receipt()
        original = json.loads(path.read_text())
        for change in ({"version": "0.0.2"}, {"status": "INCOMPLETE"}, {"sha256": "a" * 64},
                       {"sha256": []}, {"archive": "target/../foreign.crate"}):
            with self.subTest(change=change):
                receipt = json.loads(json.dumps(original))
                receipt["packages"][0].update(change)
                path.write_text(json.dumps(receipt))
                with self.assertRaises(ReleaseError):
                    self.release.finalize(path)

    def test_exact_http_transport_retains_redirect_without_following_it(self):
        url = "https://docs.rs/crate/app-kit/0.0.1/status.json"
        response = urllib.error.HTTPError(url, 302, "Found", {"Location": "/latest"}, io.BytesIO(b"redirect"))
        with patch("scripts.release_publish.urllib.request.build_opener") as opener:
            opener.return_value.open.side_effect = response
            result = self.release.request(url, exact=True, timeout=7)
        self.assertEqual(result["status"], 302)
        self.assertEqual(result["headers"]["location"], "/latest")
        opener.return_value.open.assert_called_once()
        self.assertEqual(opener.return_value.open.call_args.kwargs["timeout"], 7)

    def test_exact_http_transport_bounds_the_whole_response_read(self):
        with patch("scripts.release_publish.urllib.request.build_opener") as opener:
            response = opener.return_value.open.return_value
            response.read.side_effect = lambda size: time.sleep(0.1) or b"{}"
            with self.assertRaisesRegex(ReleaseError, "request failed"):
                self.release.request("https://docs.rs/example/0.0.1/index.html", exact=True, timeout=0.01)

    def test_malformed_publication_json_is_a_retained_release_error(self):
        path = self.publication_receipt()
        path.write_text("{broken")
        with self.assertRaises(ReleaseError):
            self.release.finalize(path)
        self.assertEqual(self.release.report["status"], "FAIL")

    def test_finalization_rejects_modified_archive_and_missing_registry_version(self):
        path = self.publication_receipt()
        archive = path.parent.parent / "release-build/package/providers-kit-0.0.1.crate"
        archive.write_bytes(b"changed")
        with self.assertRaisesRegex(ReleaseError, "archive"):
            self.release.finalize(path)
        archive.write_bytes(b"deterministic fixture archive")
        with patch.object(self.release, "registry", return_value=None), patch.object(self.release, "github") as github:
            with self.assertRaisesRegex(ReleaseError, "registry"):
                self.release.finalize(path)
        self.assertFalse(any(call.kwargs.get("method") == "POST" for call in github.call_args_list))

    def test_docs_polling_recovers_from_not_ready_without_creating_release_early(self):
        path = self.publication_receipt()
        counts = {}
        def response(url, **kwargs):
            value = self.docs_response(url, **kwargs)
            if url.endswith("status.json"):
                counts[url] = counts.get(url, 0) + 1
                if counts[url] == 1:
                    value["body"] = json.dumps({"version": self.release.version, "doc_status": False}).encode()
            return value
        with patch("scripts.release_publish.time.sleep"):
            self.assertEqual(len(self.finalize_fixture(path, response)), 1)
        self.assertTrue(all(count == 2 for count in counts.values()))

    def test_hosted_media_type_tokens_allow_case_and_whitespace(self):
        path = self.publication_receipt()
        def response(url, **kwargs):
            value = self.docs_response(url, **kwargs)
            media_type = value["headers"]["content-type"].split(";")[0].upper()
            value["headers"]["content-type"] = " " + media_type + " ; charset=utf-8"
            return value
        with patch("scripts.release_publish.time.sleep", side_effect=AssertionError("ready content must not wait")):
            self.assertEqual(len(self.finalize_fixture(path, response)), 1)

    def test_docs_fail_closed_for_bad_status_identity_content_and_logo(self):
        path = self.publication_receipt()
        cases = ("404", "false", "wrong-version", "malformed", "redirect", "latest", "root-name",
                 "root-version", "root-error", "root-mime", "logo-url", "logo-bytes", "logo-mime", "http-error")
        for case in cases:
            with self.subTest(case=case):
                def response(url, **kwargs):
                    value = self.docs_response(url, **kwargs)
                    if url.endswith("status.json") and "app-kit/" in url:
                        if case == "404": value["status"] = 404
                        if case == "false": value["body"] = b'{"version":"0.0.1","doc_status":false}'
                        if case == "wrong-version": value["body"] = b'{"version":"0.0.2","doc_status":true}'
                        if case == "malformed": value["body"] = b'not json'
                        if case == "redirect": value["status"] = 302
                        if case == "latest": value["url"] = url.replace("0.0.1", "latest")
                        if case == "http-error": raise ReleaseError("fixture HTTP 503")
                    if url.endswith("index.html") and "/app-kit/" in url:
                        if case == "root-name": value["body"] = value["body"].replace(b'app_kit', b'other_kit')
                        if case == "root-version": value["body"] = value["body"].replace(b'0.0.1', b'0.0.2')
                        if case == "root-error": value["body"] = b'<html>Documentation not found</html>'
                        if case == "root-mime": value["headers"]["content-type"] = "text/plain"
                        if case == "logo-url": value["body"] = value["body"].replace(self.logo_url.encode(), b'../logo.png')
                    if url == self.logo_url:
                        if case == "logo-bytes": value["body"] = self.logo + b'changed'
                        if case == "logo-mime": value["headers"]["content-type"] = "text/html"
                    return value
                clock = [0]
                with patch.object(self.release, "DOCS_TIMEOUT", 30), patch("scripts.release_publish.time.monotonic", side_effect=lambda: clock[0]), patch(
                        "scripts.release_publish.time.sleep", side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)):
                    with self.assertRaises(ReleaseError):
                        self.finalize_fixture(path, response)
                self.assertEqual(clock[0], 30)
                self.assertEqual(self.release.report["status"], "FAIL")
                self.assertNotIn("build failed", self.release.report["error"].lower())


if __name__ == "__main__":
    unittest.main()
