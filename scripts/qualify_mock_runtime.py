#!/usr/bin/env python3
"""Prepare dependencies normally, then execute each mock binary without network or credentials.

This qualification currently uses macOS sandbox-exec; unsupported hosts fail
explicitly. It is separate from primary CI and does not silently enable offline Cargo.
"""

import json
import os
from pathlib import Path
import sys
import tempfile

from mock_contract_runner import Runner, TARGETS


class Qualification:
    @staticmethod
    def main():
        if len(sys.argv) != 1:
            print("usage: python3 scripts/qualify_mock_runtime.py", file=sys.stderr)
            return 2
        root = Path(__file__).resolve().parent.parent
        runner = Runner(root, tuple(TARGETS), "runtime-qualification")
        runner.metadata()
        sandbox = Path("/usr/bin/sandbox-exec")
        if not sandbox.is_file():
            runner.report["error"] = "network restriction unavailable: requires macOS sandbox-exec"
            runner.save()
            return 1
        profile = "(version 1) (allow default) (deny network*)"
        runner.report["network_policy"] = profile
        executables = []
        for crate, targets in TARGETS.items():
            for target in targets:
                stage, output = runner.command(crate + "-" + target + "-build", runner.cargo("test", crate, "--test", target, "--no-run", "--message-format=json"), "dependency-preparation")
                candidates = []
                for line in output.splitlines():
                    try:
                        artifact = json.loads(line)
                    except ValueError:
                        continue
                    if (artifact.get("reason") == "compiler-artifact"
                            and artifact.get("target", {}).get("name") == target
                            and artifact.get("profile", {}).get("test")
                            and artifact.get("executable")):
                        candidates.append(artifact["executable"])
                if stage["status"] != "PASS":
                    continue
                if len(candidates) != 1:
                    stage.update(status="INCOMPLETE", error="required test executable was not uniquely identified")
                    continue
                executables.append((crate, target, candidates[0]))
        with tempfile.TemporaryDirectory(prefix="ai-kit-mock-runtime-") as temporary:
            runner.environment = {"PATH": "/usr/bin:/bin", "HOME": temporary, "TMPDIR": temporary, "LANG": "en_US.UTF-8"}
            runner.report["runtime_environment_keys"] = sorted(runner.environment)
            runner.report["credential_environment"] = "empty; temporary HOME; no inherited environment"
            for crate, target, executable in executables:
                command = [str(sandbox), "-p", profile, executable]
                listing, output = runner.command(crate + "-" + target + "-restricted-inventory", command + ["--list", "--format", "terse"], "restricted-inventory")
                inventory = runner.inventory(output)
                listing["inventory"] = inventory
                if listing["status"] != "PASS" or not inventory or len(set(inventory)) != len(inventory):
                    listing.update(status="INCOMPLETE", error="restricted runtime inventory unavailable")
                    continue
                stage, output = runner.command(crate + "-" + target + "-restricted", command + ["--test-threads=1"], "restricted-behavioral")
                runner.validate_execution(stage, output, inventory)
                runner.save()
        runner.report["source_after"] = runner.fingerprint()
        if runner.report["source_after"] != runner.report["source_before"]:
            runner.report["stages"].append({"name": "source-stability", "status": "INCOMPLETE", "error": "source changed during qualification"})
        states = [stage["status"] for stage in runner.report["stages"]]
        runner.report["status"] = "FAIL" if "FAIL" in states else "INCOMPLETE" if "INCOMPLETE" in states else "PASS"
        runner.save()
        print(f"{runner.report['status']}: credential-free restricted runtime evidence: {runner.directory}")
        return 0 if runner.report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(Qualification.main())
