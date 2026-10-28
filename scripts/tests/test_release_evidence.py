"""Strict continuation receipts, using process fixtures rather than Cargo."""

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from scripts.release_evidence import (
    BUILD_NAMES,
    Command,
    EvidenceError,
    JsonFile,
    Outcome,
    OwnedProcess,
    RuntimeIdentity,
    SourceIdentity,
    Stage,
)


def rust_output(names, *, filtered=0, ignored=0):
    return (f"running {len(names)} tests\n"
            + "".join(f"test {name} ... ok\n" for name in names)
            + f"test result: ok. {len(names)} passed; 0 failed; {ignored} ignored; 0 measured; {filtered} filtered out; finished in 0.00s\n")


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / "target" / "release"
        self.directory.mkdir(parents=True)
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["agents-kit", "providers-kit"]\n[workspace.dependencies]\nproviders-kit = {path = "providers-kit"}\n')
        (self.root / "Cargo.lock").write_text("version = 4\n")
        for crate in ("agents-kit", "providers-kit", "app-kit"):
            (self.root / crate / "src").mkdir(parents=True)
            (self.root / crate / "tests").mkdir()
            (self.root / crate / "src" / "lib.rs").write_text("pub struct Owner;\n")
            (self.root / crate / "tests" / "behavior.rs").write_text("// integration input\n")
            dependency = '\n[dependencies]\nproviders-kit = {workspace = true}\n' if crate == "agents-kit" else ""
            (self.root / crate / "Cargo.toml").write_text(f'[package]\nname = "{crate}"\nversion = "0.0.1"\n{dependency}')
        (self.root / "scripts").mkdir()
        (self.root / "scripts/security_exceptions.toml").write_text("advisories = []\n")
        (self.root / "scripts" / "gate.sh").write_text("exit 0\n")
        self.command = ["cargo", "test", "--locked", "-p", "agents-kit", "--test", "behavior", "--", "--nocapture"]
        self.compiler = "rustc fixture identity"
        self.manifest = {"schema": 1, "root": str(self.root), "compiler": self.compiler, "stages": {}}
        self.save_manifest()
        self.calls = []
        self.output = rust_output(["alpha", "beta"])
        self.exit_code = 0
        self.env = mock.patch.dict(os.environ)
        self.env.start()
        self.addCleanup(self.env.stop)
        for name in ("AI_KIT_RELEASE_STAGE_FRESH", "AI_KIT_RELEASE_PARENT_RECEIPT"):
            os.environ.pop(name, None)

    def save_manifest(self):
        (self.directory / "manifest.json").write_text(json.dumps(self.manifest))

    def execute(self, arguments, root, output, timeout, stream=True, stdout_output=None):
        self.calls.append(arguments)
        if "--list" in arguments:
            output.write(b"alpha: test\nbeta: test\n")
            return 0
        output.write(self.output.encode())
        if stdout_output is not None:
            stdout_output.write(self.output.encode())
        return self.exit_code

    def stage(self, name="agent.test.behavior", command=None, execute=None):
        with contextlib.chdir(self.root):
            return Stage(self.directory, name, command or self.command,
                         compiler=self.compiler, execute=execute or self.execute)

    def run_stage(self, stage=None):
        with contextlib.chdir(self.root), contextlib.redirect_stderr(io.StringIO()):
            with io.TextIOWrapper(io.BytesIO()) as output, contextlib.redirect_stdout(output):
                return (stage or self.stage()).run()

    def latest(self, name="agent.test.behavior"):
        reference = JsonFile.read(self.directory / "index.json")["stages"][name]
        return JsonFile.read(JsonFile.verified(reference)), reference

    def import_reference(self, prior, reference):
        other = self.root / "target" / "continued"
        other.mkdir()
        self.directory = other
        self.manifest["stages"] = {"agent.test.behavior": {
            "command": self.command,
            "reuse": {"receipt": reference["path"], "sha256": reference["sha256"]},
        }}
        self.save_manifest()

    def test_unknown_stage_executes_and_records_complete_inventory(self):
        self.assertEqual(self.run_stage(), 0)
        receipt, _ = self.latest()
        self.assertEqual(receipt["execution"], "EXECUTED")
        self.assertEqual(receipt["inventory"], ["alpha", "beta"])
        self.assertEqual(receipt["counts"]["passed"], 2)
        self.assertEqual(receipt["source_before"], receipt["source_after"])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(receipt["build_environment"], RuntimeIdentity.capture(Command.build_names(self.command)))

    def test_identical_current_run_stage_reuses_original_evidence(self):
        self.assertEqual(self.run_stage(), 0)
        first, reference = self.latest()
        self.assertEqual(self.run_stage(), 0)
        reused, _ = self.latest()
        self.assertEqual(reused["execution"], "REUSED")
        self.assertEqual(reused["reused_from"], reference)
        self.assertEqual(reused["log"]["sha256"], first["log"]["sha256"])
        self.assertEqual(self.run_stage(), 0)
        self.assertEqual(len(self.calls), 2)

    def test_identical_command_reuses_verified_execution_across_stage_names(self):
        self.assertEqual(self.run_stage(), 0)
        _, original = self.latest()
        self.assertEqual(self.run_stage(self.stage("workspace.same-agent-target")), 0)
        receipt, _ = self.latest("workspace.same-agent-target")
        self.assertEqual(receipt["execution"], "REUSED")
        self.assertEqual(receipt["reused_from"], original)
        self.assertEqual(len(self.calls), 2)

    def test_doctest_output_directory_alias_reuses_same_verified_execution(self):
        plain = ["cargo", "test", "--locked", "-p", "agents-kit", "--doc", "--", "--nocapture"]
        isolated = ["env", "CARGO_TARGET_DIR=target/docs", *plain]
        self.assertEqual(self.run_stage(self.stage("docs.agent.doctest", isolated)), 0)
        self.assertEqual(self.run_stage(self.stage("agent.doctest", plain)), 0)
        receipt, _ = self.latest("agent.doctest")
        self.assertEqual(receipt["execution"], "REUSED")
        self.assertEqual(receipt["command"], plain)
        self.assertIn("workspace-doctest-build-output-directory", receipt["command_identity_rules"])
        original = JsonFile.read(JsonFile.verified(receipt["reused_from"]))
        self.assertEqual(original["command"], isolated)
        self.assertEqual(len(self.calls), 2)

    def test_failed_command_cannot_retry_under_another_stage_name(self):
        self.exit_code = 1
        self.assertEqual(self.run_stage(), 1)
        self.exit_code = 0
        with self.assertRaisesRegex(EvidenceError, "another stage identity"):
            self.run_stage(self.stage("workspace.same-agent-target"))
        self.assertEqual(len(self.calls), 2)

    def test_fresh_required_command_ignores_successful_cross_name_execution(self):
        command = [sys.executable, "-c", "pass"]
        self.assertEqual(self.run_stage(self.stage("ordinary.command", command)), 0)
        self.assertEqual(self.run_stage(self.stage("agent.precheck", command)), 0)
        self.assertEqual(self.latest("agent.precheck")[0]["execution"], "EXECUTED")
        self.assertEqual(len(self.calls), 2)

    def test_prior_receipt_reuses_only_verified_raw_evidence(self):
        self.assertEqual(self.run_stage(), 0)
        prior, reference = self.latest()
        self.import_reference(prior, reference)
        self.assertEqual(self.run_stage(), 0)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.latest()[0]["execution"], "REUSED")

    def test_tampered_raw_log_is_incomplete_and_never_executed(self):
        self.assertEqual(self.run_stage(), 0)
        prior, reference = self.latest()
        Path(prior["log"]["path"]).write_text("tampered\n")
        self.import_reference(prior, reference)
        self.assertEqual(self.run_stage(), 1)
        self.assertEqual(self.latest()[0]["status"], "INCOMPLETE")
        self.assertEqual(len(self.calls), 2)

    def test_missing_raw_log_is_rejected(self):
        self.assertEqual(self.run_stage(), 0)
        prior, reference = self.latest()
        Path(prior["log"]["path"]).unlink()
        self.import_reference(prior, reference)
        self.assertEqual(self.run_stage(), 1)
        self.assertEqual(len(self.calls), 2)

    def test_source_change_rejects_reuse_without_running(self):
        self.assertEqual(self.run_stage(), 0)
        (self.root / "providers-kit" / "src" / "lib.rs").write_text("pub struct Changed;\n")
        with self.assertRaisesRegex(EvidenceError, "source changed within"):
            self.run_stage()
        self.assertEqual(len(self.calls), 2)

    def test_command_and_features_changes_reject_declared_stage(self):
        self.manifest["stages"]["agent.test.behavior"] = {"command": self.command}
        self.save_manifest()
        for command in (self.command + ["--exact"], self.command[:2] + ["--all-features"] + self.command[2:]):
            with self.assertRaisesRegex(EvidenceError, "command differs"):
                self.stage(command=command)

    def test_changed_compiler_and_cwd_are_rejected(self):
        self.manifest["compiler"] = "different compiler"
        self.save_manifest()
        with self.assertRaisesRegex(EvidenceError, "compiler identity"):
            self.stage()
        self.manifest["compiler"] = self.compiler
        self.manifest["stages"]["agent.test.behavior"] = {"command": self.command, "cwd": "agents-kit"}
        self.save_manifest()
        with self.assertRaisesRegex(EvidenceError, "workspace root"):
            self.stage()

    def test_failed_current_stage_cannot_be_retried(self):
        self.exit_code = 1
        self.assertEqual(self.run_stage(), 1)
        self.assertEqual(self.latest()[0]["status"], "FAIL")
        self.exit_code = 0
        with self.assertRaisesRegex(EvidenceError, "cannot be retried"):
            self.run_stage()
        self.assertEqual(len(self.calls), 2)

    def test_incomplete_current_stage_cannot_be_retried(self):
        self.output = "compiler started but no completed test output\n"
        self.assertEqual(self.run_stage(), 1)
        self.assertEqual(self.latest()[0]["status"], "INCOMPLETE")
        with self.assertRaisesRegex(EvidenceError, "cannot be retried"):
            self.run_stage()

    def test_wrong_counts_even_with_valid_artifact_hash_are_rejected(self):
        self.assertEqual(self.run_stage(), 0)
        prior, reference = self.latest()
        prior["counts"]["passed"] = 300
        JsonFile.write(reference["path"], prior)
        reference["sha256"] = JsonFile.digest(reference["path"])
        self.import_reference(prior, reference)
        self.assertEqual(self.run_stage(), 1)
        self.assertIn("counts disagree", self.latest()[0]["error"])

    def test_expected_inventory_is_checked_before_behavior(self):
        self.manifest["stages"]["agent.test.behavior"] = {"command": self.command, "inventory": ["missing"]}
        self.save_manifest()
        self.assertEqual(self.run_stage(), 1)
        self.assertEqual(len(self.calls), 1)

    def test_empty_inventory_and_inventory_compiler_failure_stop_execution(self):
        def empty(arguments, root, output, timeout, stream=True, stdout_output=None):
            self.calls.append(arguments)
            return 0
        self.assertEqual(self.run_stage(self.stage(execute=empty)), 1)
        self.assertEqual(len(self.calls), 1)

    def test_inventory_compiler_failure_is_incomplete(self):
        def compiler_failure(arguments, root, output, timeout, stream=True, stdout_output=None):
            output.write(b"error: could not compile\n")
            self.calls.append(arguments)
            return 101
        self.assertEqual(self.run_stage(self.stage(execute=compiler_failure)), 1)
        self.assertEqual(self.latest()[0]["status"], "INCOMPLETE")
        self.assertEqual(len(self.calls), 1)

    def test_log_capture_failure_prevents_command_admission(self):
        stage = self.stage(command=[sys.executable, "-c", "pass"])
        original = Path.open
        def fail_log(path, *args, **kwargs):
            if path.suffix == ".log" and args and args[0] == "xb":
                raise OSError("fixture capture failure")
            return original(path, *args, **kwargs)
        with mock.patch.object(Path, "open", fail_log):
            self.assertEqual(self.run_stage(stage), 1)
        self.assertFalse(self.calls)
        self.assertEqual(self.latest()[0]["status"], "INCOMPLETE")

    def test_changed_source_during_execution_never_passes(self):
        def changed(arguments, root, output, timeout, stream=True, stdout_output=None):
            code = self.execute(arguments, root, output, timeout, stream, stdout_output)
            if "--list" not in arguments:
                (root / "Cargo.lock").write_text("version = 3\n")
            return code
        self.assertEqual(self.run_stage(self.stage(execute=changed)), 1)
        self.assertIn("source changed", self.latest()[0]["error"])

    def test_source_change_before_lock_admission_is_rejected(self):
        stage = self.stage()
        (self.root / "Cargo.lock").write_text("version = 3\n")
        with self.assertRaisesRegex(EvidenceError, "awaiting stage admission"):
            self.run_stage(stage)
        self.assertFalse(self.calls)

    def test_future_target_path_observation_cannot_inherit_doctest_equivalence(self):
        command = ["cargo", "test", "-p", "agents-kit", "--doc"]
        SourceIdentity.capture(self.root, command)
        (self.root / "agents-kit" / "src" / "lib.rs").write_text('/// let output = std::env::var("CARGO_TARGET_DIR");\npub struct Owner;\n')
        with self.assertRaisesRegex(EvidenceError, "renewed source review"):
            SourceIdentity.capture(self.root, command)

    def test_fresh_required_stage_executes_each_time(self):
        command = [sys.executable, "-c", "pass"]
        for name in ("agent.precheck", "app.prepare.execution", "release.clean-checkout"):
            self.assertEqual(self.run_stage(self.stage(name, command)), 0)
            self.assertEqual(self.run_stage(self.stage(name, command)), 0)
            self.assertEqual(self.latest(name)[0]["execution"], "EXECUTED")
        self.assertEqual(len(self.calls), 6)

    def test_live_reuse_requires_runtime_binding(self):
        name = "agent.live.strict_live"
        self.assertEqual(self.run_stage(self.stage(name)), 0)
        self.assertEqual(self.run_stage(self.stage(name)), 1)
        self.assertIn("runtime configuration", self.latest(name)[0]["error"])

    def test_runtime_selection_change_rejected_without_reading_credentials(self):
        os.environ["TEST_CODEX_MODEL"] = "fixture-model"
        self.manifest["stages"]["agent.test.behavior"] = {"command": self.command, "runtime": RuntimeIdentity.capture(["TEST_CODEX_MODEL"])}
        self.save_manifest()
        os.environ["TEST_CODEX_MODEL"] = "another-model"
        with self.assertRaisesRegex(EvidenceError, "runtime configuration differs"):
            self.stage()

    def test_import_requires_audited_origin_and_unchanged_artifacts(self):
        self.assertEqual(self.run_stage(), 0)
        prior, _ = self.latest()
        prior["execution"] = "IMPORTED"
        with self.assertRaisesRegex(EvidenceError, "reviewed provenance"):
            self.stage().matching(prior)
        artifact = self.root / "target" / "old-report.json"
        artifact.write_text('{"original": true}\n')
        reference = {"path": str(artifact), "sha256": JsonFile.digest(artifact)}
        prior["origin"] = {"description": "fixture audit", "reviewer": "independent reviewer",
                           "artifact": reference}
        with self.assertRaisesRegex(EvidenceError, "historical cleanup"):
            self.stage().matching(prior)
        prior["origin"]["cleanup_evidence"] = {"settled": True, "basis": "historical_owner_completion",
                                                "reason": "Reviewed original owner joined its applicable work.", "artifacts": [reference]}
        self.stage().matching(prior)
        artifact.write_text("tampered")
        with self.assertRaisesRegex(EvidenceError, "hash differs"):
            self.stage().matching(prior)

    def test_behavioral_scope_tracks_dependencies_but_not_gate_script_churn(self):
        before = SourceIdentity.capture(self.root, self.command)
        (self.root / "scripts" / "gate.sh").write_text("exit 1\n")
        self.assertEqual(before, SourceIdentity.capture(self.root, self.command))
        (self.root / "providers-kit" / "src" / "lib.rs").write_text("pub struct DependencyChanged;\n")
        self.assertNotEqual(before, SourceIdentity.capture(self.root, self.command))

    def test_security_exception_policy_is_required_in_all_source_scopes(self):
        relative = "scripts/security_exceptions.toml"
        source = self.root / relative
        for command in (self.command, ["bash", "scripts/gate.sh"]):
            with self.subTest(command=command):
                source.write_text("advisories = []\n")
                before = SourceIdentity.capture(self.root, command)
                self.assertIn(relative, before["files"])
                source.write_text('advisories = ["RUSTSEC-2026-0001"]\n')
                self.assertNotEqual(before, SourceIdentity.capture(self.root, command))

    def test_invalid_security_exception_policy_rejects_release_receipts(self):
        (self.root / "scripts/security_exceptions.toml").write_text('advisories = ["RUSTSEC-2026"]\n')
        with self.assertRaisesRegex(EvidenceError, "security exception policy"):
            SourceIdentity.capture(self.root, self.command)

    def test_scope_review_cannot_exclude_the_security_exception_policy(self):
        relative = "scripts/security_exceptions.toml"
        paths = SourceIdentity.paths(self.root, self.command)
        selected = [name for name in paths if name != relative]
        audit_path = self.root / "target" / "scope.json"
        audit_path.write_text(json.dumps({
            "schema": 1, "status": "APPROVED", "reviewer": "independent fixture",
            "reason": "bounded source audit", "command": Command.canonical(self.command),
            "paths": selected, "excluded_paths": {relative: "cannot omit security policy inputs"},
        }))
        override = {"paths": selected, "audit": {"path": str(audit_path), "sha256": JsonFile.digest(audit_path)}}
        with self.assertRaisesRegex(EvidenceError, "root build authority"):
            SourceIdentity.capture(self.root, self.command, scope_override=override)

    def test_contract_scope_includes_scripts_and_documentation(self):
        command = self.command.copy()
        command[command.index("behavior")] = "source_contract"
        before = SourceIdentity.capture(self.root, command)
        (self.root / "scripts" / "gate.sh").write_text("exit 1\n")
        self.assertNotEqual(before, SourceIdentity.capture(self.root, command))

    def test_contract_scope_includes_other_crates_script_and_template_inputs(self):
        other = self.root / "storage-kit"
        (other / "scripts").mkdir(parents=True)
        (other / "scripts" / "check.sh").write_text("exit 0\n")
        (other / "strict-tests.env.example").write_text("export TEST_NONSECRET_MODEL=fixture\n")
        command = ["cargo", "test", "-p", "providers-kit", "--test", "strict_env_contract"]
        paths = SourceIdentity.paths(self.root, command)
        self.assertIn("storage-kit/scripts/check.sh", paths)
        self.assertIn("storage-kit/strict-tests.env.example", paths)

    def test_vendor_scope_uses_archives_and_never_walks_ignored_extractions(self):
        vendor = self.root / "agents-kit" / "vendor"
        vendor.mkdir()
        (vendor / "pinned.zip").write_bytes(b"archive")
        (vendor / "ignored-checkout").symlink_to(self.root / "outside", target_is_directory=True)
        listing = subprocess.CompletedProcess([], 0, b"agents-kit/vendor/pinned.zip\0", b"")
        with mock.patch("scripts.release_evidence.subprocess.run", return_value=listing) as run:
            paths = SourceIdentity.paths(self.root, self.command)
        self.assertIn("agents-kit/vendor/pinned.zip", paths)
        self.assertNotIn("agents-kit/vendor/ignored-checkout", paths)
        self.assertIn("--exclude-standard", run.call_args.args[0])

    def test_unknown_local_cargo_dependency_is_not_silently_omitted(self):
        manifest = self.root / "agents-kit" / "Cargo.toml"
        manifest.write_text(manifest.read_text() + 'new-local = {path = "vendor/local"}\n')
        with self.assertRaisesRegex(EvidenceError, "local Cargo dependency"):
            SourceIdentity.paths(self.root, self.command)

    def test_unknown_stage_fields_and_duplicate_json_fields_fail_closed(self):
        self.manifest["stages"]["agent.test.behavior"] = {"command": self.command, "silently_skip": True}
        self.save_manifest()
        with self.assertRaisesRegex(EvidenceError, "unknown fields"):
            self.stage()
        (self.directory / "manifest.json").write_text('{"schema":1,"schema":2}')
        with self.assertRaisesRegex(EvidenceError, "duplicate field"):
            self.stage()

    def test_scope_override_requires_exclusion_review_and_root_authority(self):
        paths = SourceIdentity.paths(self.root, self.command)
        excluded = "agents-kit/tests/behavior.rs"
        selected = [name for name in paths if name != excluded]
        audit_path = self.root / "target" / "scope.json"
        audit = {"schema": 1, "status": "APPROVED", "reviewer": "independent fixture", "reason": "bounded source audit",
                 "command": Command.canonical(self.command), "paths": selected, "excluded_paths": {excluded: "unrelated target"}}
        audit_path.write_text(json.dumps(audit))
        override = {"paths": selected, "audit": {"path": str(audit_path), "sha256": JsonFile.digest(audit_path)}}
        self.assertNotIn(excluded, SourceIdentity.capture(self.root, self.command, scope_override=override)["files"])
        audit["paths"].remove("Cargo.lock")
        audit["excluded_paths"]["Cargo.lock"] = "not an allowed exclusion"
        audit_path.write_text(json.dumps(audit))
        override["audit"]["sha256"] = JsonFile.digest(audit_path)
        with self.assertRaisesRegex(EvidenceError, "root build authority"):
            SourceIdentity.capture(self.root, self.command, scope_override=override)


    def test_cargo_receipt_binds_stdout_separately_from_diagnostics(self):
        def split(arguments, root, output, timeout, stream=True, stdout_output=None):
            if "--list" in arguments:
                return self.execute(arguments, root, output, timeout, stream)
            output.write(b"test corrupted {diagnostic} output\n")
            stdout_output.write(self.output.encode())
            return 0
        self.assertEqual(self.run_stage(self.stage(execute=split)), 0)
        receipt, reference = self.latest()
        self.assertEqual(receipt["counts"]["passed"], 2)
        self.assertNotEqual(receipt["log"]["sha256"], receipt["stdout_log"]["sha256"])
        self.stage().matching(receipt)
        Path(receipt["stdout_log"]["path"]).write_text("tampered")
        with self.assertRaises(EvidenceError):
            self.stage().matching(receipt)

    def test_cargo_stdout_cannot_hide_failed_exit_or_bad_parent_outcomes(self):
        for code, output in ((1, rust_output(["alpha", "beta"])),
                             (0, rust_output(["alpha", "beta"], filtered=1)),
                             (0, rust_output(["alpha", "beta"], ignored=1)),
                             (0, rust_output(["alpha", "beta"]).replace("test alpha ... ok", "test alpha ... FAILED"))):
            with self.subTest(code=code, output=output):
                self.exit_code, self.output = code, output
                stage = self.stage(name="case" + str(len(self.calls)),
                                   command=self.command + ["--test-threads=" + str(len(self.calls) + 1)])
                self.assertEqual(self.run_stage(stage), 1)


class CommandAndOutcomeTests(unittest.TestCase):
    def test_cargo_option_order_normalizes_but_features_do_not(self):
        left = ["cargo", "test", "-p", "agents-kit", "--features", "b,a", "--locked", "--test", "contract"]
        right = ["cargo", "test", "--test", "contract", "--locked", "--features=a,b", "--package=agents-kit"]
        self.assertEqual(Command.canonical(left), Command.canonical(right))
        self.assertNotEqual(Command.canonical(left), Command.canonical(right + ["--no-default-features"]))

    def test_env_wrapped_cargo_keeps_semantics_and_effective_build_identity(self):
        command = ["env", "RUSTDOCFLAGS=-D warnings", "CARGO_TARGET_DIR=target/docs", "cargo", "test", "--doc", "-p", "agents-kit"]
        self.assertEqual(Command.kind(command), "tests")
        self.assertEqual(Command.values(command, "-p"), ["agents-kit"])
        self.assertIn("--list", Command.listing(command))
        self.assertEqual(Command.environment(command)["CARGO_BUILD_JOBS"], "1")
        self.assertNotEqual(RuntimeIdentity.capture(BUILD_NAMES, Command.environment(command)), RuntimeIdentity.capture(BUILD_NAMES))

    def test_only_workspace_doctest_build_output_directory_is_nonsemantic(self):
        plain = ["cargo", "test", "--locked", "-p", "agents-kit", "--doc", "--", "--nocapture"]
        for alternate in (["env", "CARGO_TARGET_DIR=target/a", *plain],
                          plain[:2] + ["--target-dir", "target/b"] + plain[2:],
                          plain[:2] + ["--target-dir=target/c"] + plain[2:]):
            self.assertEqual(Command.canonical(plain), Command.canonical(alternate))
        for operation in (["cargo", "doc", "-p", "agents-kit"],
                          ["cargo", "test", "-p", "agents-kit", "--test", "behavior"]):
            self.assertNotEqual(Command.canonical(operation), Command.canonical(operation + ["--target-dir", "target/a"]))
            self.assertNotEqual(Command.canonical(operation), Command.canonical(["env", "CARGO_TARGET_DIR=target/a", *operation]))

    def test_doctest_directory_equivalence_preserves_features_filters_and_threads(self):
        plain = ["cargo", "test", "--locked", "-p", "agents-kit", "--doc", "--", "--nocapture"]
        variants = [plain[:2] + ["--no-default-features"] + plain[2:],
                    plain + ["--test-threads=1"], plain + ["--skip", "case"],
                    ["env", "CARGO_TARGET_DIR=target/a", "RUSTDOCFLAGS=-D warnings", *plain]]
        for alternate in variants:
            self.assertNotEqual(Command.canonical(plain), Command.canonical(alternate))

    def test_foreign_or_manifest_selected_doctests_keep_exact_output_directory(self):
        for command in (["cargo", "test", "--doc", "-p", "foreign-crate"],
                        ["cargo", "test", "--doc", "-pforeign-crate"],
                        ["cargo", "test", "--doc", "--manifest-path", "agents-kit/Cargo.toml"],
                        ["cargo", "test", "--doc", "--manifest-path=agents-kit/Cargo.toml"]):
            self.assertFalse(Command.workspace_doctest(command))
            self.assertNotEqual(Command.canonical(command), Command.canonical(command + ["--target-dir", "target/a"]))
            self.assertNotEqual(Command.canonical(command), Command.canonical(["env", "CARGO_TARGET_DIR=target/a", *command]))

    def test_build_identity_omits_only_settings_irrelevant_to_command(self):
        self.assertNotIn("RUSTDOCFLAGS", Command.build_names(["cargo", "test", "--test", "behavior"]))
        self.assertIn("RUSTDOCFLAGS", Command.build_names(["cargo", "test", "--doc"]))
        self.assertIn("RUSTDOCFLAGS", Command.build_names(["cargo", "test", "-p", "agents-kit"]))
        self.assertEqual(Command.build_names(["cargo", "fmt", "--check"]), ())

    def test_unknown_or_secret_env_prefix_is_rejected(self):
        for assignment in ("API_KEY=fixture", "CARGO_BUILD_JOBS=4", "UNKNOWN=fixture"):
            with self.assertRaises(EvidenceError):
                Command.kind(["env", assignment, "cargo", "test"])
        with self.assertRaises(EvidenceError):
            RuntimeIdentity.capture(["TEST_OPENAI_API_KEY"])

    def test_rustdoc_artifact_stages_are_fresh_but_doctests_may_reuse(self):
        self.assertTrue(Command.fresh("docs.storage.rustdoc.lance.restore", ["env", "RUSTDOCFLAGS=-D warnings", "cargo", "doc"]))
        self.assertFalse(Command.fresh("docs.agent.doctest", ["cargo", "test", "--doc"]))

    def test_nested_child_summary_does_not_double_count_parent(self):
        output = ("running 2 tests\ntest parent ... \nrunning 1 test\n"
                  "test child ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 89 filtered out\n"
                  "ok\ntest second ... ok\n"
                  "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        counts = Outcome.validate("tests", output, ["parent", "second"])
        self.assertEqual(counts["passed"], 2)
        self.assertEqual(counts["filtered"], 0)

    def test_parallel_parent_rows_survive_named_single_case_child_output(self):
        names = ["owner", "first_sibling", "second_sibling", "last"]
        output = ("running 4 tests\nrunning 1 test\ntest owner ... \n"
                  "test first_sibling ... ok\nok\n"
                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out\n"
                  "running 1 test\ntest owner ... diagnostics\n"
                  "test second_sibling ... ok\nok\n"
                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out\n"
                  "test owner ... ok\ntest last ... ok\n"
                  "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        counts = Outcome.validate("tests", output, names, supervised_parent="integration")
        self.assertEqual(counts, {"passed": 4, "failed": 0, "ignored": 0, "measured": 0, "filtered": 0})
        self.assertCountEqual(Outcome.rows(output), [(name, "ok") for name in names])

    def test_interleaved_parent_rows_keep_complete_inventory_and_failure_checks(self):
        names = ["owner", "sibling"]
        output = ("running 2 tests\nrunning 1 test\ntest owner ... \n"
                  "test sibling ... ok\nok\n"
                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out\n"
                  "test owner ... ok\n"
                  "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        for malformed in (
                output.replace("test sibling ... ok", "test sibling ... FAILED"),
                output.replace("test sibling ... ok", "test sibling ... ignored"),
                output.replace("test sibling ... ok\n", ""),
                output.replace("test sibling ... ok\n", "test sibling ... ok\ntest sibling ... ok\n"),
                output.replace("test sibling ... ok", "test unexpected ... ok"),
                output.replace("2 passed; 0 failed", "1 passed; 1 failed"),
                output.replace("2 passed; 0 failed; 0 ignored", "1 passed; 0 failed; 1 ignored"),
                output.replace("0 filtered out", "1 filtered out"),
                output.replace("test owner ... ok\n", "")):
            with self.subTest(output=malformed), self.assertRaises(EvidenceError):
                Outcome.validate("tests", malformed, names, supervised_parent="integration")

    def test_multi_case_child_cannot_reassign_an_ambiguous_pending_sibling(self):
        output = ("running 2 tests\nrunning 2 tests\ntest owner ... \n"
                  "test sibling ... ok\nok\n"
                  "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
                  "test owner ... ok\n"
                  "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        with self.assertRaises(EvidenceError):
            Outcome.validate("tests", output, ["owner", "sibling"], supervised_parent="integration")

    def test_sequential_doctest_harnesses_all_count_even_with_same_name(self):
        output = rust_output(["src/lib.rs - Owner (line 1)"]) * 2
        counts = Outcome.validate("tests", output, ["src/lib.rs - Owner (line 1)"] * 2)
        self.assertEqual(counts["passed"], 2)

    def test_app_prepare_requires_complete_parent_despite_deliberate_child_crashes(self):
        names = ["first", "second", "reopen", "crash_boundaries"]
        output = ("running 4 tests\ntest first ... ok\ntest second ... ok\n"
                  "test reopen ... \nrunning 1 test\ntest reopen ... ok\n"
                  "test crash_boundaries ... \nrunning 1 test\ntest crash_boundaries ... \n"
                  "running 1 test\ntest crash_boundaries ... \n"
                  "running 1 test\ntest crash_boundaries ... \n"
                  "running 1 test\ntest crash_boundaries ... ok\n"
                  "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        self.assertEqual(Outcome.validate("tests", output, names, supervised_parent=True)["passed"], 4)
        with self.assertRaises(EvidenceError):
            Outcome.validate("tests", output, names)
        for malformed in (output.replace("test first ... ok\n", ""),
                          output.replace("4 passed", "3 passed"),
                          output.replace("test crash_boundaries ... ok\n", "")):
            with self.assertRaises(EvidenceError):
                Outcome.validate("tests", malformed, names, supervised_parent=True)
        self.assertTrue(Command.supervised_parent(["cargo", "test", "-p", "app-kit", "--test", "prepare_contract"]))
        self.assertEqual(Command.supervised_parent(["cargo", "test", "-p", "providers-kit", "--test", "prepare_contract"]), "integration")
        self.assertFalse(Command.supervised_parent(["cargo", "test", "-p", "app-kit", "--doc"]))

    def test_lance_parent_accounts_completed_and_deliberately_lost_children(self):
        names = ["child_entry", "lease", "crash"]
        output = ("running 3 tests\ntest child_entry ... ok\ntest lease ... \n"
                  "running 1 test\ntest child_entry ... ok\n"
                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out\nok\n"
                  "test crash ... \nrunning 1 test\ntest child_entry ... ok\n"
                  "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        self.assertEqual(Outcome.validate("tests", output, names, supervised_parent="storage-lance")["passed"], 3)
        with self.assertRaises(EvidenceError):
            Outcome.validate("tests", output.replace("test lease ... \n", ""), names, supervised_parent="storage-lance")

    def test_single_thread_nocapture_rows_and_parallel_complete_rows(self):
        output = ("running 2 tests\ntest alpha ... diagnostic text\nmore diagnostics\nok\n"
                  "test beta ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n")
        self.assertEqual(Outcome.validate("tests", output, ["alpha", "beta"])["passed"], 2)

    def test_failed_ignored_filtered_empty_and_missing_outcomes_are_rejected(self):
        samples = [rust_output(["a"], filtered=1), rust_output(["a"], ignored=1), rust_output([]),
                   rust_output(["a"]).replace("test a ... ok\n", ""),
                   rust_output(["a"]).replace("result: ok", "result: FAILED"),
                   rust_output(["a"]).replace("running 1 tests\n", "")]
        for sample in samples:
            with self.subTest(sample=sample), self.assertRaises(EvidenceError):
                Outcome.validate("tests", sample, ["a"])

    def test_bounded_filtered_inventory_requires_explicit_expected_count(self):
        self.assertEqual(Outcome.validate("tests", rust_output(["a"], filtered=7), ["a"], expected_filtered=7)["filtered"], 7)

    def test_compile_only_and_library_empty_inventory_remain_explicit(self):
        self.assertTrue(Command.allow_empty(["cargo", "test", "-p", "app-kit", "--test", "compile_contract"]))
        self.assertTrue(Command.allow_empty(["cargo", "test", "--lib"]))
        self.assertFalse(Command.allow_empty(["cargo", "test", "--test", "mock_contracts"]))
        self.assertEqual(Outcome.validate("tests", rust_output([]), [], allow_empty=True)["passed"], 0)

    def test_python_receipts_reject_skips_and_missing_summary(self):
        valid = "....\nRan 4 tests in 0.01s\n\nOK\n"
        self.assertEqual(Outcome.validate("python-tests", valid)["passed"], 4)
        for invalid in (valid.replace("OK", "OK (skipped=1)"), "OK\n", valid.replace("4 tests", "0 tests")):
            with self.assertRaises(EvidenceError):
                Outcome.validate("python-tests", invalid)

    def test_rustdoc_execution_modes_preserve_exact_listed_names(self):
        names = ["sdk/src/lib.rs - Owner (line 12)", "sdk/src/lib.rs - Owner (line 24)"]
        output = rust_output([names[0] + " - compile"]) + rust_output([names[1] + " - compile fail"])
        self.assertEqual(Outcome.validate("tests", output, names)["passed"], 2)
        self.assertEqual([name for name, _ in Outcome.rows(output)], names)

    def test_rustdoc_mode_mapping_cannot_hide_missing_or_different_cases(self):
        name = "sdk/src/lib.rs - Owner (line 12)"
        output = rust_output([name + " - compile fail"])
        for inventory in ([], [name.replace("12", "13")], [name, name]):
            with self.subTest(inventory=inventory), self.assertRaises(EvidenceError):
                Outcome.validate("tests", output, inventory)
        failed = output.replace("result: ok", "result: FAILED")
        with self.assertRaises(EvidenceError):
            Outcome.validate("tests", failed, [name])

    def test_execution_mode_labels_are_not_removed_from_ordinary_test_names(self):
        for name in ("ordinary - compile", "ordinary - compile fail", "Owner (line 12) - compile",
                     "sdk/src/lib.rs - Owner (line zero) - compile", "sdk/src/lib.rs - Owner (line 0) - compile"):
            self.assertEqual(Outcome.test_name(name), name)

    def test_provider_timeout_covers_authored_multi_day_batch_budgets(self):
        self.assertGreaterEqual(Command.timeout("provider.live.strict_live", ["cargo", "test"]), 10 * 24 * 3600)


class ProcessOwnershipTests(unittest.TestCase):
    def test_separate_stdout_survives_stderr_between_name_and_terminal_and_keeps_eof(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = ("import os; "
                      "os.write(1,b'running 1 test\\ntest case ... '); "
                      "os.write(2,b'{diagnostic without newline'); "
                      "os.write(1,b'ok\\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\\n'); "
                      "os.write(2,b' EOF}')")
            with (root / "combined").open("wb") as output, (root / "stdout").open("wb") as stdout:
                self.assertEqual(OwnedProcess.run([sys.executable, "-c", script], root, output, 5, stream=False, stdout_output=stdout), 0)
            observed = (root / "stdout").read_text()
            self.assertEqual(Outcome.validate("tests", observed, ["case"])["passed"], 1)
            combined = (root / "combined").read_bytes()
            self.assertIn(b"{diagnostic without newline", combined)
            self.assertIn(b" EOF}", combined)
            self.assertEqual(len(combined), len(observed.encode()) + len(b"{diagnostic without newline EOF}"))
            self.assertNotIn("diagnostic", observed)

    def test_short_stdout_capture_write_fails_and_settles_owned_process(self):
        class PartialCapture:
            def write(self, chunk):
                return len(chunk) - 1
            def flush(self):
                pass
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (root / "output").open("wb") as output:
                with self.assertRaisesRegex(EvidenceError, "stdout capture"):
                    OwnedProcess.run([sys.executable, "-c", "print('output', flush=True)"], root, output, 5,
                                     stream=False, stdout_output=PartialCapture())

    def test_timeout_with_both_capture_pipes_retains_partial_stdout_and_settles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = "import os,signal; os.write(1,b'partial stdout'); os.write(2,b'partial stderr'); signal.pause()"
            with (root / "combined").open("wb") as output, (root / "stdout").open("wb") as stdout:
                with self.assertRaises(subprocess.TimeoutExpired):
                    OwnedProcess.run([sys.executable, "-c", script], root, output, 0.5,
                                     cleanup_timeout=1, stream=False, stdout_output=stdout)
            self.assertEqual((root / "stdout").read_bytes(), b"partial stdout")
            self.assertIn(b"partial stderr", (root / "combined").read_bytes())


    def test_timeout_joins_child_and_grandchild_before_acknowledgement(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            parent = root / "parent.json"
            parent.write_text("{}")
            pids = root / "pids.json"
            command = ("import json,os,signal,subprocess,sys,time\n"
                       "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
                       "def stop(number, frame):\n"
                       "    child.kill()\n"
                       "    child.wait(timeout=2)\n"
                       "    sys.exit(0)\n"
                       "signal.signal(signal.SIGTERM, stop)\n"
                       f"open({str(pids)!r}, 'w').write(json.dumps([os.getpid(), child.pid]))\n"
                       "time.sleep(30)\n")
            with mock.patch.dict(os.environ, {"AI_KIT_RELEASE_PARENT_RECEIPT": str(parent)}):
                with (root / "output.log").open("wb") as output, self.assertRaises(subprocess.TimeoutExpired):
                    OwnedProcess.run([sys.executable, "-c", command], root, output, 0.5, cleanup_timeout=1, stream=False)
            for pid in json.loads(pids.read_text()):
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
            self.assertFalse(list(root.glob("parent.json.release-*")))

    def test_exit_and_launch_failure_restore_handlers_and_remove_markers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            parent = root / "parent.json"
            parent.write_text("{}")
            before = signal.getsignal(signal.SIGTERM)
            with mock.patch.dict(os.environ, {"AI_KIT_RELEASE_PARENT_RECEIPT": str(parent)}):
                with (root / "output.log").open("wb") as output:
                    self.assertEqual(OwnedProcess.run([sys.executable, "-c", "print('completed')"], root, output, 5, stream=False), 0)
                with (root / "failed.log").open("wb") as output, self.assertRaises(FileNotFoundError):
                    OwnedProcess.run([str(root / "missing")], root, output, 5, stream=False)
            self.assertFalse(list(root.glob("parent.json.release-*")))
            self.assertEqual(signal.getsignal(signal.SIGTERM), before)

    def test_timeout_joins_owned_process_and_acknowledges_parent(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            parent = root / "parent.json"
            parent.write_text("{}")
            pid = root / "pid"
            command = [sys.executable, "-c", f"import os,time; open({str(pid)!r}, 'w').write(str(os.getpid())); time.sleep(30)"]
            with mock.patch.dict(os.environ, {"AI_KIT_RELEASE_PARENT_RECEIPT": str(parent)}):
                with (root / "output.log").open("wb") as output, self.assertRaises(subprocess.TimeoutExpired):
                    OwnedProcess.run(command, root, output, 0.2, cleanup_timeout=0.2, stream=False)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid.read_text()), 0)
            self.assertFalse(list(root.glob("parent.json.release-*")))

    def test_uncertain_settlement_retains_parent_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            parent = root / "parent.json"
            parent.write_text("{}")
            with mock.patch.dict(os.environ, {"AI_KIT_RELEASE_PARENT_RECEIPT": str(parent)}), mock.patch.object(OwnedProcess, "settle", return_value=False):
                with (root / "output.log").open("wb") as output, self.assertRaisesRegex(EvidenceError, "settlement"):
                    OwnedProcess.run([sys.executable, "-c", "pass"], root, output, 5, stream=False)
            markers = list(root.glob("parent.json.release-*"))
            self.assertEqual(len(markers), 1)
            self.assertIn("pgid", JsonFile.read(markers[0]))

    def test_signal_cleanup_settles_child_before_parent_acknowledgement(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            parent = root / "parent.json"
            parent.write_text("{}")
            child_pid = root / "child.pid"
            script = root / "owner.py"
            repository = Path(__file__).resolve().parents[2]
            child = f"import os,time; open({str(child_pid)!r}, 'w').write(str(os.getpid())); time.sleep(30)"
            script.write_text(
                "import sys\nfrom pathlib import Path\n"
                f"sys.path.insert(0, {str(repository)!r})\n"
                "from scripts.release_evidence import OwnedProcess\n"
                f"with open({str(root / 'output.log')!r}, 'wb') as output:\n"
                f"    OwnedProcess.run([sys.executable, '-c', {child!r}], Path({str(root)!r}), output, 30, cleanup_timeout=0.5, stream=False)\n")
            process = subprocess.Popen([sys.executable, str(script)], env=dict(os.environ, AI_KIT_RELEASE_PARENT_RECEIPT=str(parent)),
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            try:
                deadline = time.monotonic() + 5
                while not child_pid.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(child_pid.exists())
                self.assertEqual(len(list(root.glob("parent.json.release-*"))), 1)
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=5)
                self.assertNotEqual(process.returncode, 0)
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(child_pid.read_text()), 0)
                self.assertFalse(list(root.glob("parent.json.release-*")))
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
