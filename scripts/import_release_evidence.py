#!/usr/bin/env python3
"""Import reviewed legacy evidence without inventing an execution or identity.

An import spec names a hash-bound legacy report, one stage and a bounded raw log
segment. Its separate approval binds that evidence to a retained Git snapshot,
the exact current command, a reviewed input scope and historical build settings.
The importer performs no Cargo, service or authentication operations.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

try:
    from .release_evidence import Command, Compiler, EvidenceError, JsonFile, Outcome, SourceIdentity
except ImportError:
    from release_evidence import Command, Compiler, EvidenceError, JsonFile, Outcome, SourceIdentity


class GitSnapshot:
    trees = {}
    blob_hashes = {}

    @staticmethod
    def identity(repository, revision, paths):
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise EvidenceError("source revision must be a full immutable Git commit")
        repository = Path(repository).resolve()
        actual = subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", revision + "^{commit}"],
            text=True, timeout=30).strip()
        if actual != revision:
            raise EvidenceError("source revision did not resolve exactly")
        key = (str(repository), revision)
        if key not in GitSnapshot.trees:
            tree = subprocess.check_output(
                ["git", "-C", str(repository), "ls-tree", "-rz", "--full-tree", revision], timeout=60)
            objects = {}
            for record in tree.split(b"\0"):
                if record:
                    header, name = record.split(b"\t", 1)
                    mode, kind, oid = header.decode().split()
                    objects[name.decode()] = (mode, kind, oid)
            GitSnapshot.trees[key] = objects
        objects = GitSnapshot.trees[key]
        selected = []
        for name in sorted(paths):
            if name not in objects or objects[name][0] not in ("100644", "100755") or objects[name][1] != "blob":
                raise EvidenceError(f"source snapshot lacks a regular tracked input: {name}")
            selected.append((name, *objects[name]))
        missing = sorted({oid for _, _, _, oid in selected if oid not in GitSnapshot.blob_hashes})
        request = "".join(oid + "\n" for oid in missing).encode()
        data = subprocess.check_output(["git", "-C", str(repository), "cat-file", "--batch"],
                                       input=request, timeout=120)
        offset, files = 0, {}
        for oid in missing:
            ending = data.index(b"\n", offset)
            header = data[offset:ending].decode().split()
            if len(header) != 3 or header[:2] != [oid, "blob"]:
                raise EvidenceError("Git blob transfer differs from its source identity")
            length = int(header[2])
            contents = data[ending + 1:ending + 1 + length]
            if len(contents) != length or data[ending + 1 + length:ending + 2 + length] != b"\n":
                raise EvidenceError("Git blob transfer is incomplete")
            offset = ending + length + 2
            GitSnapshot.blob_hashes[oid] = hashlib.sha256(contents).hexdigest()
        if offset != len(data):
            raise EvidenceError("Git blob transfer has unexpected trailing data")
        for name, mode, _, oid in selected:
            files[name] = {"sha256": GitSnapshot.blob_hashes[oid], "mode": 0o111 if mode == "100755" else 0}
        encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        return {"sha256": hashlib.sha256(encoded).hexdigest(), "files": files}


class LegacyImport:
    def __init__(self, spec, compiler=None):
        self.spec_path = Path(spec).resolve()
        self.spec = JsonFile.read(self.spec_path)
        if self.spec.get("schema") != 1:
            raise EvidenceError("unsupported import spec schema")
        self.root = Path(self.spec["root"]).resolve()
        self.command = self.spec["command"]
        self.canonical = Command.canonical(self.command)
        self.stage_id = self.spec["stage_id"]
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", self.stage_id):
            raise EvidenceError("invalid import stage identity")
        self.approval_path = JsonFile.verified(self.spec["approval"])
        self.approval = JsonFile.read(self.approval_path)
        if (self.approval.get("schema") != 1 or self.approval.get("status") != "APPROVED"
                or not self.approval.get("reviewer") or not self.approval.get("reason")
                or self.approval.get("command") != self.canonical):
            raise EvidenceError("import requires an independent approval for this exact command")
        self.compiler = compiler if compiler is not None else Compiler.read(self.root)

    @staticmethod
    def setting(value):
        return hashlib.sha256(("absent" if value is None else "present\0" + value).encode()).hexdigest()

    def source(self, report):
        source = self.spec["source"]
        binding = self.approval.get("source_binding", {})
        expected = {"repository": str(Path(source["repository"]).resolve()),
                    "revision": source["revision"], "report": self.spec["legacy"]["report"],
                    "source_before": report["source_before"], "source_after": report["source_after"]}
        if binding != expected:
            raise EvidenceError("source approval does not bind the report to its retained snapshot")
        witnesses = self.approval.get("witnesses")
        if not isinstance(witnesses, list) or not witnesses:
            raise EvidenceError("source identity bridge requires retained evidence witnesses")
        for witness in witnesses:
            JsonFile.verified(witness)
        self.scope = {"audit": self.spec["approval"], "paths": self.approval.get("paths")}
        current = SourceIdentity.capture(self.root, self.command, scope_override=self.scope)
        old = GitSnapshot.identity(source["repository"], source["revision"], self.scope["paths"])
        if old != current:
            changed = sorted(name for name in set(old["files"]) | set(current["files"])
                             if old["files"].get(name) != current["files"].get(name))
            raise EvidenceError("source snapshot differs from current inputs: " + ", ".join(changed[:8]))
        return current

    def build_environment(self, report):
        names = Command.build_names(self.command)
        settings = self.approval.get("build_environment", {})
        evidence = self.approval.get("build_environment_evidence", {})
        if set(settings) != set(names) or set(evidence) != set(names):
            raise EvidenceError("historical build identity requires every build setting and witness")
        for name in names:
            if not isinstance(settings[name], str) or not re.fullmatch(r"[0-9a-f]{64}", settings[name]):
                raise EvidenceError("historical build setting must be a SHA256 identity")
            witness = evidence[name]
            if not isinstance(witness, dict) or not witness.get("reason"):
                raise EvidenceError("historical build setting lacks a reviewed explanation")
            JsonFile.verified(witness.get("artifact"))
        for name, value in report.get("build_environment", {}).items():
            if name in names and settings[name] != self.setting(value):
                raise EvidenceError("historical build identity contradicts its report")
        return settings

    def cleanup(self, report, stage):
        if report.get("cleanup_settled") is False or stage.get("cleanup_settled") is False:
            raise EvidenceError("legacy evidence explicitly records unsettled cleanup")
        proof = self.approval.get("cleanup_evidence", {})
        if (proof.get("settled") is not True or not proof.get("reason")
                or proof.get("basis") not in ("historical_owner_completion", "observed_process_group_settlement")
                or not isinstance(proof.get("artifacts"), list) or not proof["artifacts"]):
            raise EvidenceError("import requires reviewed historical cleanup settlement evidence")
        for artifact in proof["artifacts"]:
            JsonFile.verified(artifact)

    def command_binding(self, stage, block, outer_pass):
        original = self.spec["legacy"].get("original_command")
        if original != stage.get("command"):
            raise EvidenceError("original command differs from the legacy stage")
        if outer_pass and Command.canonical(original) == self.canonical:
            return
        command, _ = Command.unwrap(self.command)
        original_command, _ = Command.unwrap(original)
        targets = Command.values(command, "--test")
        selected = targets[0] if len(targets) == 1 else "lib" if "--lib" in command else None
        if selected is not None:
            header = (r"\s*Running unittests src/lib\.rs \(" if selected == "lib"
                      else r"\s*Running tests/" + re.escape(selected) + r"\.rs \(")
            if not re.match(header, block):
                raise EvidenceError("target projection lacks its exact Running header")
        if outer_pass and len(original_command) > 1 and original_command[:2] == command[:2] == ["cargo", "test"]:
            originals = Command.values(original_command, "--test")
            if selected in originals or selected == "lib" and "--lib" in original_command:
                projected, index = [], 0
                while index < len(original):
                    value = original[index]
                    if value == "--":
                        break
                    if value == "--test":
                        index += 2
                        continue
                    if value == "--lib":
                        index += 1
                        continue
                    projected.append(value)
                    index += 1
                projected.extend(["--lib"] if selected == "lib" else ["--test", selected])
                projected.extend(original[index:])
                if Command.canonical(projected) == self.canonical or self.equivalent(projected):
                    return
        mapping = self.approval.get("command_source", {})
        if (mapping.get("command") != self.canonical or not mapping.get("reason")
                or not selected and "--doc" not in command):
            raise EvidenceError("legacy shell subcommand requires a reviewed exact source mapping")
        # A completed subcommand inside an interrupted owner needs both raw test
        # completion and its immutable owning script, never the owner's failure
        # converted to a pass.
        identity = GitSnapshot.identity(mapping["repository"], mapping["revision"], [mapping["path"]])
        if identity["files"][mapping["path"]]["sha256"] != mapping.get("sha256"):
            raise EvidenceError("legacy command source differs from its reviewed script")
        if not outer_pass and self.approval.get("completed_subcommand") is not True:
            raise EvidenceError("failed legacy parent cannot supply a passing stage")

    def equivalent(self, projected):
        """Permit only explicit reviewed feature/logging/thread option changes.

        The selected target, operation, profiles, test selection and all other
        arguments must remain identical. The review owns the semantic proof;
        this method never infers equivalent features from a spelling alone.
        """
        audit = self.approval.get("command_equivalence")
        if audit is None:
            return False
        if (audit.get("original") != Command.canonical(projected)
                or audit.get("current") != self.canonical or not audit.get("reason")
                or not isinstance(audit.get("witnesses"), list) or not audit["witnesses"]):
            raise EvidenceError("option equivalence lacks an exact reviewed command binding")
        for witness in audit["witnesses"]:
            JsonFile.verified(witness)
        def invariant(arguments):
            arguments = Command.canonical(arguments)
            result, index, libtest = [], 0, False
            while index < len(arguments):
                item = arguments[index]
                if item == "--":
                    libtest = True
                if not libtest and item in ("--all-features", "--no-default-features"):
                    index += 1
                    continue
                if not libtest and item == "--features":
                    index += 2
                    continue
                if libtest and (item == "--nocapture" or item.startswith("--test-threads=")):
                    index += 1
                    continue
                if libtest and item == "--test-threads":
                    index += 2
                    continue
                result.append(item)
                index += 1
            return result
        if invariant(projected) != invariant(self.command):
            raise EvidenceError("reviewed option equivalence cannot change target, selection or other behavior")
        return True

    def run(self, output):
        legacy = self.spec["legacy"]
        report_path = JsonFile.verified(legacy["report"])
        report = JsonFile.read(report_path)
        if not report.get("source_before") or report.get("source_before") != report.get("source_after"):
            raise EvidenceError("legacy source changed or lacks before/after identity")
        if report.get("compiler", "").strip() != self.compiler.strip():
            raise EvidenceError("legacy compiler differs from current compiler")
        stages = [stage for stage in report.get("stages", []) if stage.get("name") == legacy["stage_name"]]
        if len(stages) != 1:
            raise EvidenceError("legacy stage is missing or ambiguous")
        stage = stages[0]
        outer_pass = stage.get("status") == "PASS" and stage.get("returncode") == 0
        if not outer_pass and self.approval.get("completed_subcommand") is not True:
            raise EvidenceError("failed or incomplete legacy stage is not importable")
        log = JsonFile.verified(legacy["log"])
        recorded_log = Path(stage.get("log", ""))
        if (self.root / recorded_log).resolve() != log.resolve():
            raise EvidenceError("raw log does not belong to the legacy stage")
        segment = legacy["segment"]
        raw = log.read_bytes()
        start, end = segment.get("byte_start"), segment.get("byte_end")
        if not isinstance(start, int) or not isinstance(end, int) or not 0 <= start < end <= len(raw):
            raise EvidenceError("legacy log segment bounds are invalid")
        block = raw[start:end]
        if hashlib.sha256(block).hexdigest() != segment.get("sha256"):
            raise EvidenceError("legacy log segment hash differs")
        text = block.decode("utf-8")
        expected_binding = {"report": legacy["report"], "stage_name": legacy["stage_name"],
                            "log": legacy["log"], "segment": segment}
        if self.approval.get("legacy_binding") != expected_binding:
            raise EvidenceError("approval does not bind this exact legacy segment")
        self.command_binding(stage, text, outer_pass)
        source = self.source(report)
        environment = self.build_environment(report)
        self.cleanup(report, stage)
        if Command.live(self.stage_id, self.command):
            raise EvidenceError("legacy live import requires a separate historical runtime identity protocol")
        kind = Command.kind(self.command)
        if kind == "command" and not outer_pass:
            raise EvidenceError("failed parent needs independently parsed test completion")
        inventory = self.approval.get("inventory")
        filtered = self.approval.get("expected_filtered", 0)
        if filtered != 0:
            raise EvidenceError("ordinary imported inventories cannot contain filtered cases")
        counts = Outcome.validate(kind, text, inventory, filtered, Command.allow_empty(self.command),
                                  supervised_parent=Command.supervised_parent(self.command))
        if counts != self.approval.get("counts"):
            raise EvidenceError("legacy raw results differ from reviewed counts")
        if outer_pass and Command.canonical(stage["command"]) == self.canonical:
            # An exact whole-command receipt cannot select only a nested child
            # or the first of several rustdoc groups. Compare the complete raw
            # original invocation against the same declared result.
            if kind == "command" and (start != 0 or end != len(raw)):
                raise EvidenceError("whole command import requires its complete original log")
            full_counts = Outcome.validate(kind, raw.decode("utf-8"), inventory, filtered, Command.allow_empty(self.command),
                                           supervised_parent=Command.supervised_parent(self.command))
            if full_counts != counts:
                raise EvidenceError("bounded results omit part of the original command")
        output = Path(output).resolve()
        if output.suffix != ".json":
            raise EvidenceError("import receipt destination must end in .json")
        if output.exists() or output.with_suffix(".log").exists():
            raise EvidenceError("import destination already exists")
        if SourceIdentity.capture(self.root, self.command, scope_override=self.scope) != source:
            raise EvidenceError("source changed while preparing the import")
        output.parent.mkdir(parents=True, exist_ok=True)
        segment_path = output.with_suffix(".log")
        segment_path.write_bytes(block)
        artifact = {"path": str(self.spec_path), "sha256": JsonFile.digest(self.spec_path)}
        additional = [self.spec["approval"], legacy["report"], legacy["log"], *self.approval["witnesses"]]
        additional.extend(witness["artifact"] for witness in self.approval["build_environment_evidence"].values())
        additional.extend(self.approval.get("command_equivalence", {}).get("witnesses", []))
        additional.extend(self.approval["cleanup_evidence"]["artifacts"])
        receipt = {"schema": 1, "stage_id": self.stage_id, "command": self.command,
                   "canonical_command": self.canonical, "cwd": ".", "compiler": self.compiler.strip(),
                   "kind": kind, "source_before": source, "source_after": source, "runtime": {},
                   "build_environment": environment, "status": "PASS", "execution": "IMPORTED",
                   "returncode": 0, "cleanup_settled": True, "inventory": inventory, "counts": counts,
                   "cleanup_basis": self.approval["cleanup_evidence"]["basis"],
                   "scope_override": self.scope,
                   "log": {"path": str(segment_path), "sha256": JsonFile.digest(segment_path)},
                   "origin": {"artifact": artifact, "additional_artifacts": additional,
                              "reviewer": self.approval["reviewer"], "description": self.approval["reason"],
                              "cleanup_evidence": self.approval["cleanup_evidence"],
                              "outer_status": stage.get("status"), "outer_returncode": stage.get("returncode"),
                              "original_command": stage.get("command"), "legacy_segment": segment,
                              "source_repository": self.spec["source"]["repository"],
                              "source_revision": self.spec["source"]["revision"]}}
        JsonFile.write(output, receipt)
        return {"path": str(output), "sha256": JsonFile.digest(output)}

    @classmethod
    def main(cls, arguments=None):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        parser.add_argument("--spec", required=True)
        parser.add_argument("--output", required=True)
        options = parser.parse_args(arguments)
        try:
            print(json.dumps(cls(options.spec).run(options.output), sort_keys=True))
            return 0
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
            print(f"INCOMPLETE legacy import: {error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(LegacyImport.main())
