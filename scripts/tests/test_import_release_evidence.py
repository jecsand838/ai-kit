"""Legacy import provenance checks; fixture output is never release evidence."""

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.import_release_evidence import LegacyImport
from scripts.release_evidence import Command, EvidenceError, JsonFile, SourceIdentity


class LegacyImportTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name).resolve()
        self.evidence = self.root / "target/evidence"
        self.evidence.mkdir(parents=True)
        sources = {"Cargo.toml": '[workspace]\nmembers = ["provider-sdk"]\n',
                   "Cargo.lock": "# locked fixture\n", "rust-toolchain.toml": '[toolchain]\nchannel = "stable"\n',
                   "provider-sdk/Cargo.toml": '[package]\nname = "provider-sdk"\nversion = "0.1.0"\n',
                   "provider-sdk/src/lib.rs": "// original fixture source\n",
                   "provider-sdk/tests/first.rs": "// first integration fixture\n",
                   "provider-sdk/tests/second.rs": "// independent integration fixture\n"}
        for name, contents in sources.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        self.git("init", "-q")
        self.git("add", "--", *sources)
        self.git("-c", "user.name=Evidence Fixture", "-c", "user.email=fixture@example.invalid",
                 "-c", "core.hooksPath=/dev/null", "commit", "-qm", "retained fixture snapshot")
        self.revision = self.git("rev-parse", "HEAD").strip()
        self.command = ["cargo", "test", "--locked", "-p", "provider-sdk", "--test", "first", "--", "--nocapture"]
        self.log = self.evidence / "original.log"
        self.log.write_text("     Running tests/first.rs (target/debug/deps/first-fixture)\n\nrunning 1 test\n"
                            "test confirms_contract ... ok\n\n"
                            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n")
        self.report_path = self.evidence / "report.json"
        self.report = {"status": "PASS", "source_before": {"sha256": "a" * 64, "files": len(sources)},
                       "source_after": {"sha256": "a" * 64, "files": len(sources)},
                       "compiler": "fixture compiler", "build_environment": {"CARGO_INCREMENTAL": "0", "CARGO_BUILD_JOBS": "1"},
                       "stages": [{"name": "original", "status": "PASS", "returncode": 0,
                                   "command": self.command, "log": str(self.log)}]}
        self.write(self.report_path, self.report)
        segment = {"byte_start": 0, "byte_end": self.log.stat().st_size, "sha256": JsonFile.digest(self.log)}
        self.legacy = {"report": self.ref(self.report_path), "stage_name": "original",
                       "original_command": self.command, "log": self.ref(self.log), "segment": segment}
        paths = SourceIdentity.paths(self.root, self.command)
        self.approval = {"schema": 1, "status": "APPROVED", "reviewer": "independent-fixture-reviewer",
                         "reason": "Fixture snapshot and original source witness agree.",
                         "command": Command.canonical(self.command), "paths": paths, "excluded_paths": {},
                         "source_binding": {"repository": str(self.root), "revision": self.revision,
                                            "report": self.legacy["report"], "source_before": self.report["source_before"],
                                            "source_after": self.report["source_after"]},
                         "legacy_binding": {key: self.legacy[key] for key in ("report", "stage_name", "log", "segment")},
                         "witnesses": [self.ref(self.report_path)],
                         "build_environment": {name: LegacyImport.setting(self.report["build_environment"].get(name)) for name in Command.build_names(self.command)},
                         "build_environment_evidence": {name: {"artifact": self.ref(self.report_path),
                                                               "reason": "Controlled fixture environment recorded by this test."} for name in Command.build_names(self.command)},
                         "cleanup_evidence": {"settled": True, "basis": "historical_owner_completion",
                                              "reason": "No child was created by this controlled fixture.",
                                              "artifacts": [self.ref(self.report_path)]},
                         "inventory": ["confirms_contract"],
                         "counts": {"passed": 1, "failed": 0, "ignored": 0, "measured": 0, "filtered": 0}}
        self.approval_path = self.evidence / "approval.json"
        self.spec_path = self.evidence / "spec.json"
        self.spec = {"schema": 1, "root": str(self.root), "stage_id": "provider.test.first",
                     "command": self.command, "source": {"repository": str(self.root), "revision": self.revision},
                     "legacy": self.legacy}
        self.persist()

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.root), *arguments], text=True, stderr=subprocess.PIPE)

    def ref(self, path):
        return {"path": str(path), "sha256": JsonFile.digest(path)}

    def write(self, path, value):
        path.write_text(json.dumps(value, indent=2) + "\n")

    def persist(self):
        self.write(self.approval_path, self.approval)
        self.spec["approval"] = self.ref(self.approval_path)
        self.write(self.spec_path, self.spec)

    def refresh_report_references(self):
        reference = self.ref(self.report_path)
        self.legacy["report"] = reference
        self.approval["source_binding"]["report"] = reference
        self.approval["legacy_binding"]["report"] = reference
        self.approval["witnesses"] = [reference]
        self.approval["cleanup_evidence"]["artifacts"] = [reference]
        for witness in self.approval["build_environment_evidence"].values():
            witness["artifact"] = reference

    def execute(self):
        return LegacyImport(self.spec_path, compiler="fixture compiler").run(self.evidence / "import.json")

    def test_import_keeps_original_identity_and_exact_raw_segment(self):
        result = self.execute()
        imported = JsonFile.read(JsonFile.verified(result))
        self.assertEqual(imported["execution"], "IMPORTED")
        self.assertEqual(imported["origin"]["original_command"], self.command)
        self.assertEqual(imported["counts"]["passed"], 1)
        self.assertEqual(JsonFile.verified(imported["log"]).read_bytes(), self.log.read_bytes())
        self.assertEqual(imported["source_before"], imported["source_after"])
        self.assertEqual(imported["scope_override"]["audit"], self.ref(self.approval_path))

    def test_changed_source_cannot_be_imported(self):
        (self.root / "provider-sdk/src/lib.rs").write_text("// changed production\n")
        with self.assertRaisesRegex(EvidenceError, "snapshot differs"):
            self.execute()

    def test_unrelated_target_can_be_excluded_only_by_exact_review(self):
        path = "provider-sdk/tests/second.rs"
        self.approval["paths"].remove(path)
        self.approval["excluded_paths"][path] = "Separate integration target; selected first.rs does not include it."
        self.persist()
        (self.root / path).write_text("// unrelated target changed\n")
        self.execute()

    def test_missing_exclusion_reason_fails(self):
        self.approval["paths"].remove("provider-sdk/tests/second.rs")
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "explain every omitted"):
            self.execute()

    def test_failed_stage_is_never_relabeled_pass(self):
        self.report["stages"][0].update(status="FAIL", returncode=1)
        self.write(self.report_path, self.report)
        self.spec["legacy"]["report"] = self.ref(self.report_path)
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "failed or incomplete"):
            self.execute()

    def test_log_hash_tampering_fails(self):
        self.log.write_text(self.log.read_text().replace("confirms_contract", "other_contract"))
        with self.assertRaisesRegex(EvidenceError, "hash differs"):
            self.execute()

    def test_segment_hash_tampering_fails(self):
        self.spec["legacy"]["segment"]["sha256"] = "b" * 64
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "segment hash differs"):
            self.execute()

    def test_missing_historical_setting_witness_fails(self):
        del self.approval["build_environment_evidence"]["RUSTFLAGS"]
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "every build setting"):
            self.execute()

    def test_cleanup_is_not_inferred_from_a_passing_exit(self):
        del self.approval["cleanup_evidence"]
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "historical cleanup"):
            self.execute()

    def test_review_cannot_override_explicit_unsettled_cleanup(self):
        self.report["stages"][0]["cleanup_settled"] = False
        self.write(self.report_path, self.report)
        self.refresh_report_references()
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "explicitly records unsettled"):
            self.execute()

    def test_recorded_setting_cannot_be_contradicted(self):
        self.approval["build_environment"]["CARGO_BUILD_JOBS"] = LegacyImport.setting("4")
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "contradicts"):
            self.execute()

    def test_compiler_mismatch_fails(self):
        with self.assertRaisesRegex(EvidenceError, "compiler differs"):
            LegacyImport(self.spec_path, compiler="other compiler").run(self.evidence / "import.json")

    def test_incomplete_inventory_fails(self):
        self.approval["inventory"] = []
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "inventory"):
            self.execute()

    def test_wrong_source_binding_fails(self):
        self.approval["source_binding"]["revision"] = "a" * 40
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "retained snapshot"):
            self.execute()

    def test_changed_executable_mode_is_a_source_change(self):
        (self.root / "provider-sdk/tests/first.rs").chmod(0o755)
        with self.assertRaisesRegex(EvidenceError, "snapshot differs"):
            self.execute()

    def test_claimed_subcommand_without_owning_script_proof_fails(self):
        self.report["stages"][0].update(status="FAIL", returncode=143,
                                       command=["bash", "provider-sdk/scripts/check.sh"])
        self.write(self.report_path, self.report)
        self.legacy["original_command"] = self.report["stages"][0]["command"]
        self.refresh_report_references()
        self.approval["completed_subcommand"] = True
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "exact source mapping"):
            self.execute()

    def test_log_destination_cannot_overwrite_its_own_raw_segment(self):
        with self.assertRaisesRegex(EvidenceError, "end in .json"):
            LegacyImport(self.spec_path, compiler="fixture compiler").run(self.evidence / "import.log")

    def test_exact_command_cannot_import_only_its_nested_child(self):
        child = ("running 1 test\ntest child_only ... ok\n"
                 "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n")
        outer = ("Running tests/first.rs (target/debug/deps/first-fixture)\nrunning 1 test\n"
                 "test confirms_contract ... \n" + child + "ok\n"
                 "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n")
        self.log.write_text(outer)
        self.legacy["log"] = self.ref(self.log)
        start = outer.index(child)
        self.legacy["segment"] = {"byte_start": start, "byte_end": start + len(child),
                                   "sha256": hashlib.sha256(child.encode()).hexdigest()}
        self.approval["legacy_binding"] = {key: self.legacy[key] for key in ("report", "stage_name", "log", "segment")}
        self.approval["inventory"] = ["child_only"]
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "inventory"):
            self.execute()

    def test_multi_target_projection_keeps_feature_and_libtest_arguments(self):
        original = self.command[:]
        original[original.index("--"):original.index("--")] = ["--test", "second"]
        self.report["stages"][0]["command"] = original
        self.write(self.report_path, self.report)
        self.legacy["original_command"] = original
        self.refresh_report_references()
        self.persist()
        result = self.execute()
        self.assertEqual(JsonFile.read(result["path"])["origin"]["original_command"], original)

    def test_feature_option_difference_requires_its_independent_equivalence_proof(self):
        self.spec["command"] = self.command[:]
        self.spec["command"].insert(2, "--all-features")
        self.approval["command"] = Command.canonical(self.spec["command"])
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "exact source mapping"):
            self.execute()
        self.approval["command_equivalence"] = {
            "original": Command.canonical(self.command), "current": self.approval["command"],
            "reason": "Fixture package has no features; reviewed manifest proves identical feature selection.",
            "witnesses": [self.ref(self.root / "provider-sdk/Cargo.toml")]}
        self.persist()
        self.execute()

    def test_equivalence_cannot_exclude_tests(self):
        original = self.command[:]
        self.spec["command"] = original + ["--skip", "confirms_contract"]
        self.approval["command"] = Command.canonical(self.spec["command"])
        self.approval["command_equivalence"] = {
            "original": Command.canonical(original), "current": self.approval["command"],
            "reason": "An invalid purported equivalence must still fail.",
            "witnesses": [self.ref(self.report_path)]}
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "cannot change target, selection"):
            self.execute()

    def test_live_import_cannot_assume_current_credentials_or_runtime(self):
        self.spec["stage_id"] = "provider.live.first"
        self.persist()
        with self.assertRaisesRegex(EvidenceError, "historical runtime"):
            self.execute()

    def test_import_never_overwrites_prior_evidence(self):
        self.execute()
        with self.assertRaisesRegex(EvidenceError, "already exists"):
            self.execute()


if __name__ == "__main__":
    unittest.main()
