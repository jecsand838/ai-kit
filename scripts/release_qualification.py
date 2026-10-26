#!/usr/bin/env python3
"""Resume the canonical release gates using reviewed, unchanged-source receipts.

The bundle is a release_evidence manifest. Missing entries execute normally;
failed, stale, or incomplete evidence cannot authorize a skipped command.
"""

import argparse
import datetime
import fcntl
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

from release_evidence import Command, Compiler, EvidenceError, JsonFile, OwnedProcess, SourceIdentity, Stage


class ReleaseQualification:
    def __init__(self, root, bundle, skip_strict_live=False):
        self.root = Path(root).resolve()
        self.skip_strict_live = skip_strict_live
        self.bundle_path = Path(bundle).resolve()
        self.bundle = JsonFile.read(self.bundle_path)
        if self.bundle.get("schema") != 1 or Path(self.bundle.get("root", "")).resolve() != self.root:
            raise EvidenceError("bundle schema or workspace differs")
        self.compiler = Compiler.read(self.root)
        if self.bundle.get("compiler") != self.compiler:
            raise EvidenceError("bundle compiler differs from the active toolchain")
        if not isinstance(self.bundle.get("stages"), dict):
            raise EvidenceError("bundle requires a stage mapping")
        self.branch = self.git("branch", "--show-current")
        if self.bundle.get("branch") != self.branch or not self.branch:
            raise EvidenceError("bundle branch differs from the active branch")
        self.revision = self.git("rev-parse", "HEAD")
        self.base = self.root / "target" / "release-qualification"
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.directory = self.base / (stamp + "-" + uuid.uuid4().hex[:8])

    def git(self, *arguments):
        return subprocess.run(["git", *arguments], cwd=self.root, check=True,
                              capture_output=True, text=True, timeout=30).stdout.strip()

    @staticmethod
    def commands(skip_strict_live=False):
        # The shell owners retain their inventories, feature lanes, native grade
        # reporters and fresh prechecks. This supplements the wrapper's default
        # Lance scope with the mandatory supported local Storage matrix.
        return [
            ("release-runner-contracts", ["bash", "scripts/release_stage.sh", "--fresh", "workspace.contract.release_runner",
                "python3", "-m", "unittest", "scripts.tests.test_release_stage_hooks",
                "scripts.tests.test_release_evidence", "scripts.tests.test_release_qualification",
                "scripts.tests.test_import_release_evidence"]),
            ("storage-local-matrix", ["bash", "storage-sdk/scripts/check.sh", "--all-backends"]),
            ("workspace-wrapper", ["bash", "scripts/verify_workspace_strict.sh", *(["--skip-strict-live"] if skip_strict_live else [])]),
        ]

    @staticmethod
    def storage_inventory():
        """Closed inventory of the existing local owner, including negative gates."""
        backends = ("redb", "postgres", "surrealdb", "sqlite", "qdrant")
        lanes = ["-", *backends, "redb,postgres,surrealdb,sqlite", "redb,postgres,surrealdb,qdrant"]
        combinations = [parts for count in range(6) for parts in itertools.combinations(backends, count)]
        legal = [",".join(parts) or "-" for parts in combinations if not {"sqlite", "qdrant"} <= set(parts)]
        negative = [",".join(parts) for parts in combinations if {"sqlite", "qdrant"} <= set(parts)]
        stages = {
            "storage.contract.feature_policy": ["python3", "-m", "unittest", "discover", "-s", "storage-sdk/scripts/tests", "-v"],
            "storage.format": ["cargo", "fmt", "--all", "--check"],
        }
        for features in lanes:
            lane = "lance" if features == "-" else features.replace(",", "-")
            args = ["--no-default-features", *(["--features", features] if features != "-" else [])]
            targets = ["feature_contract", "public_api", "database_backend", "state_store_backend", "mock_contracts",
                       "lancedb_native_feasibility", "lancedb_durability_feasibility", "lancedb_session_ownership",
                       "lancedb_state_store", "strict_env_contract", "source_contract"]
            selected = features.split(",")
            if "redb" in selected or "sqlite" in selected:
                targets.append("state_store_local")
            if "surrealdb" in selected:
                targets.append("surreal_partition_deadline")
            if "qdrant" in selected:
                targets.append("qdrant_rest_completion")
            stages["storage.compile." + lane] = ["cargo", "test", "--locked", "-p", "storage-sdk", "--all-targets", "--no-run", *args]
            for target in targets:
                stages["storage.test." + lane + "." + target] = ["cargo", "test", "--locked", "-p", "storage-sdk", "--test", target, *args, "--", "--nocapture", "--test-threads=1"]
            stages["storage.doctest." + lane] = ["cargo", "test", "--locked", "-p", "storage-sdk", "--doc", *args, "--", "--nocapture"]
            stages["storage.clippy." + lane] = ["cargo", "clippy", "--locked", "-p", "storage-sdk", "--all-targets", *args, "--", "-D", "warnings"]
            stages["storage.rustdoc." + lane] = ["env", "RUSTDOCFLAGS=-D warnings", "cargo", "doc", "--locked", "-p", "storage-sdk", "--no-deps", *args]
        for features in legal:
            lane = "lance" if features == "-" else features.replace(",", "-")
            args = ["--no-default-features", *(["--features", features] if features != "-" else [])]
            stages["storage.check." + lane] = ["cargo", "check", "--locked", "-p", "storage-sdk", "--all-targets", *args]
        return lanes, legal, negative, stages

    def storage_reuse(self, command):
        ident = "storage.owner.local_matrix"
        if ident not in self.bundle["stages"]:
            return None
        if command != ["bash", "storage-sdk/scripts/check.sh", "--all-backends"]:
            raise EvidenceError("Storage owner reuse requires the exact complete local matrix")
        definition = self.bundle["stages"][ident]
        if (not isinstance(definition, dict) or definition.get("command") != command
                or not isinstance(definition.get("reuse"), dict)):
            raise EvidenceError("declared Storage owner reuse lacks its receipt binding")
        binding = definition["reuse"]
        if set(binding) != {"receipt", "sha256"} or not isinstance(binding["receipt"], str):
            raise EvidenceError("malformed Storage owner receipt reference")
        reference = {"path": binding["receipt"], "sha256": binding["sha256"]}
        prior = JsonFile.read(JsonFile.verified(reference))
        if prior.get("execution") != "IMPORTED":
            raise EvidenceError("Storage owner requires an independently reviewed imported certificate")
        Stage(self.directory, ident, command, compiler=self.compiler).matching(prior)
        proof = prior.get("owner_proof")
        fields = {"schema", "original_run_report", "original_owner_name", "lanes", "legal", "negative", "nested_stages"}
        if (not isinstance(proof, dict) or set(proof) != fields
                or type(proof.get("schema")) is not int or proof["schema"] != 1):
            raise EvidenceError("Storage owner completion proof is missing or malformed")
        report = JsonFile.read(JsonFile.verified(proof["original_run_report"]))
        if (not isinstance(report.get("source_before"), dict) or report["source_before"] != report.get("source_after")
                or not isinstance(report.get("revision"), str) or not report["revision"]
                or report["revision"] != report.get("revision_after")
                or not isinstance(report.get("working_tree_before"), str)
                or report["working_tree_before"] != report.get("working_tree_after")):
            raise EvidenceError("original Storage owner report has incomplete or changed source provenance")
        entries = report.get("commands")
        if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
            raise EvidenceError("original owner report command inventory is malformed")
        owners = [entry for entry in entries if entry.get("name") == "storage-local-matrix"]
        if (proof["original_owner_name"] != "storage-local-matrix" or len(owners) != 1
                or owners[0].get("command") != command or owners[0].get("status") != "PASS"
                or owners[0].get("returncode") != 0 or owners[0].get("cleanup_settled") is not True
                or owners[0].get("log") != prior.get("log")):
            raise EvidenceError("original Storage owner did not complete successfully")
        lanes, legal, negative, expected = self.storage_inventory()
        if proof["lanes"] != lanes or proof["legal"] != legal:
            raise EvidenceError("Storage owner feature inventory is incomplete")
        nested = proof["nested_stages"]
        if not isinstance(nested, dict) or set(nested) != set(expected):
            raise EvidenceError("Storage owner nested stage inventory is incomplete")
        for stage_id, arguments in expected.items():
            item = nested[stage_id]
            if (not isinstance(item, dict) or set(item) != {"definition", "receipt"}
                    or not isinstance(item["definition"], dict)
                    or not isinstance(item["definition"].get("command"), list)
                    or any(not isinstance(part, str) for part in item["definition"]["command"])
                    or item["definition"] != self.bundle["stages"].get(stage_id)
                    or Command.canonical(item["definition"].get("command", [])) != Command.canonical(arguments)):
                raise EvidenceError("Storage owner nested stage binding differs: " + stage_id)
            receipt = JsonFile.read(JsonFile.verified(item["receipt"]))
            Stage(self.directory, stage_id, arguments, compiler=self.compiler).matching(receipt)
        checks = proof["negative"]
        if not isinstance(checks, list) or len(checks) != len(negative):
            raise EvidenceError("Storage owner requires all eight negative feature checks")
        owner_log = JsonFile.verified(prior["log"]).read_text(errors="replace")
        grades = []
        for match in re.finditer("STORAGE_NEGATIVE_FEATURE_GRADE=", owner_log):
            grade, _ = json.JSONDecoder(object_pairs_hook=JsonFile.unique_pairs).raw_decode(owner_log[match.end():])
            grades.append(grade)
        if len(grades) != len(negative):
            raise EvidenceError("original Storage owner log lacks its eight negative outcomes")
        observed = set()
        diagnostic = re.compile(r"send_guard.*deadlock_detection|deadlock_detection.*send_guard|sqlite.*qdrant.*(incompatible|cannot)")
        for item in checks:
            if not isinstance(item, dict) or set(item) != {"receipt", "log"}:
                raise EvidenceError("malformed negative feature evidence")
            receipt = JsonFile.read(JsonFile.verified(item["receipt"]))
            features = receipt.get("features")
            expected_command = ["cargo", "check", "--locked", "-p", "storage-sdk", "--lib", "--no-default-features", "--features", features]
            if (features not in negative or features in observed or receipt.get("schema") != 1
                    or receipt.get("status") != "PASS" or type(receipt.get("returncode")) is not int
                    or receipt["returncode"] == 0 or receipt.get("command") != expected_command
                    or receipt.get("log") != item["log"] or receipt not in grades):
                raise EvidenceError("negative feature check is missing, duplicated or unsuccessful")
            text = JsonFile.verified(item["log"]).read_text(errors="replace")
            reported = receipt.get("expected_diagnostic")
            if not isinstance(reported, str) or not diagnostic.search(reported) or reported not in text:
                raise EvidenceError("negative feature check lacks its required compiler diagnostic")
            observed.add(features)
        return {"execution": "REUSED", "reused_from": reference,
                "original_run_report": proof["original_run_report"], "log": prior["log"],
                "completion": {"schema": 1, "owner_proof": reference, "lanes": len(lanes),
                               "legal_feature_sets": len(legal), "negative_feature_checks": len(negative),
                               "nested_stages": len(expected)}}

    def run(self):
        self.base.mkdir(parents=True, exist_ok=True)
        with (self.base / "coordinator.lock").open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise EvidenceError("another release coordinator owns Cargo execution") from error
            return self._locked()

    def _locked(self):
        self.directory.mkdir(mode=0o700)
        manifest = dict(self.bundle)
        manifest.update(root=str(self.root), compiler=self.compiler)
        JsonFile.write(self.directory / "manifest.json", manifest)
        report = {
            "schema": 1, "status": "INCOMPLETE", "branch": self.branch,
            "revision": self.revision, "working_tree_before": self.git("status", "--porcelain=v1", "--untracked-files=normal"),
            "compiler": self.compiler,
            "source_before": SourceIdentity.capture(self.root, ["bash", "scripts/verify_workspace_strict.sh"]),
            "bundle": {"path": str(self.bundle_path), "sha256": JsonFile.digest(self.bundle_path)},
            "commands": [], "clean_checkout": "REQUIRED_SEPARATELY_AT_FINAL_COMMIT",
            "scope": "QUALIFIED_NONLIVE" if self.skip_strict_live else "FULL_CANONICAL_GATES",
            "excluded_live_targets": ["agent-sdk/strict_live", "provider-sdk/strict_live",
                "storage-sdk/strict_live", "tool-sdk/strict_live", "app-sdk/workflow_live"] if self.skip_strict_live else [],
        }
        JsonFile.write(self.directory / "report.json", report)
        saved = {name: os.environ.get(name) for name in ("AI_KIT_RELEASE_RECEIPT_RUN", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS")}
        os.environ.update(AI_KIT_RELEASE_RECEIPT_RUN=str(self.directory), CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1")
        print(f"Release evidence: {self.directory}", flush=True)
        try:
            for name, command in self.commands(self.skip_strict_live):
                start = time.monotonic()
                log = self.directory / (name + ".log")
                entry = {"name": name, "command": command, "status": "INCOMPLETE"}
                print(f"Running canonical owner: {name}", flush=True)
                try:
                    reused = self.storage_reuse(command) if name == "storage-local-matrix" else None
                    if reused is None:
                        with log.open("xb") as output:
                            code = OwnedProcess.run(command, self.root, output, 14 * 24 * 3600, cleanup_timeout=30, stream=False)
                        entry["execution"] = "EXECUTED"
                    else:
                        entry.update(reused)
                        code = 0
                    entry.update(returncode=code, status="PASS" if code == 0 else "FAIL", cleanup_settled=True)
                except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError, InterruptedError, KeyboardInterrupt) as error:
                    entry.update(error=str(error))
                    if isinstance(error, (InterruptedError, KeyboardInterrupt)):
                        raise
                finally:
                    entry["duration_seconds"] = round(time.monotonic() - start, 3)
                    if log.is_file():
                        entry["log"] = {"path": str(log), "sha256": JsonFile.digest(log)}
                    report["commands"].append(entry)
                    JsonFile.write(self.directory / "report.json", report)
                print(f"{entry['status']}: {name}", flush=True)
                if name == "release-runner-contracts" and entry["status"] != "PASS":
                    break
            report["working_tree_after"] = self.git("status", "--porcelain=v1", "--untracked-files=normal")
            report["revision_after"] = self.git("rev-parse", "HEAD")
            report["source_after"] = SourceIdentity.capture(self.root, ["bash", "scripts/verify_workspace_strict.sh"])
            states = [item["status"] for item in report["commands"]]
            report["status"] = "INCOMPLETE" if "INCOMPLETE" in states else "FAIL" if "FAIL" in states else "PASS"
            if (report["working_tree_after"] != report["working_tree_before"] or report["revision_after"] != self.revision or report["source_after"] != report["source_before"]):
                report.update(status="INCOMPLETE", error="tracked source or commit changed during release execution")
            # A successful wrapper continuation is not the separate final-commit
            # clean-checkout proof. Keep both visible in the final receipt.
            report["qualification"] = ("QUALIFIED_NONLIVE; STRICT_LIVE_DEFERRED_BY_USER; FINAL_CLEAN_CHECKOUT_PROOF_REQUIRED"
                                       if self.skip_strict_live else "CANONICAL_GATES_ONLY; FINAL_CLEAN_CHECKOUT_PROOF_REQUIRED")
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            JsonFile.write(self.directory / "report.json", report)
        print(f"{report['status']}: {self.directory / 'report.json'}", flush=True)
        return 0 if report["status"] == "PASS" else 1

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        parser.add_argument("--resume", required=True, metavar="BUNDLE")
        parser.add_argument("--skip-strict-live", action="store_true",
                            help="qualify local gates only; explicitly defer all five strict-live suites")
        options = parser.parse_args()
        os.umask(0o077)
        try:
            return cls(Path(__file__).resolve().parent.parent, options.resume, options.skip_strict_live).run()
        except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
            print(f"Release qualification incomplete: {error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(ReleaseQualification.main())
