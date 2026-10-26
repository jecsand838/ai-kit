#!/usr/bin/env python3
"""Serial, fail-closed primary CI. Live and native integration gates are separate."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid


TARGETS = {
    "provider-sdk": ("mock_contracts",),
    "storage-sdk": ("mock_contracts",),
    "tool-sdk": ("mock_contracts",),
    "agent-sdk": ("mock_contracts", "custom_mock_composition"),
    "app-sdk": ("mock_contracts", "workflow_mock_composition"),
}
SUMMARY = re.compile(
    r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; "
    r"(\d+) measured; (\d+) filtered out;", re.M
)


class Runner:
    def __init__(self, root, selected, mode, execute=None):
        self.root = Path(root)
        self.selected = selected
        self.mode = mode
        self.execute = execute or self.owned_process
        self.environment = dict(os.environ, CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1")
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.directory = self.root / "target/mock-contracts" / (stamp + "-" + uuid.uuid4().hex[:8])
        self.directory.mkdir(parents=True)
        self.report = {
            "mode": mode,
            "scope": "workspace" if len(selected) == len(TARGETS) else "scoped",
            "crates": list(selected),
            "started_utc": stamp,
            "status": "INCOMPLETE",
            "stages": [],
        }
        self.save()

    @staticmethod
    def owned_process(command, **options):
        timeout = options.pop("timeout")
        options.pop("check", None)
        # The whole process group is owned: Cargo may have rustc or libtest
        # descendants when it times out or the operator interrupts the run.
        process = None
        pending = []
        def interrupt(signum, _frame):
            if process is None:
                pending.append(signum)
            else:
                raise KeyboardInterrupt(f"received signal {signum}")
        previous_term = signal.signal(signal.SIGTERM, interrupt)
        previous_int = signal.signal(signal.SIGINT, interrupt)
        try:
            # Defer exceptions until the child handle is assigned. Unlike a
            # blocked signal mask, this does not alter signals inherited by exec.
            process = subprocess.Popen(command, start_new_session=True, **options)
            if pending:
                raise KeyboardInterrupt(f"received signal {pending[0]}")
            return subprocess.CompletedProcess(command, process.wait(timeout=timeout))
        except BaseException:
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
            raise
        finally:
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)

    @staticmethod
    def features(crate):
        return ["--no-default-features"] if crate == "storage-sdk" else []

    @classmethod
    def cargo(cls, operation, crate, *args):
        return ["cargo", operation, "--locked", "-p", crate, *cls.features(crate), *args]

    def save(self):
        destination = self.directory / "report.json"
        temporary = self.directory / "report.json.tmp"
        temporary.write_text(json.dumps(self.report, indent=2) + "\n")
        temporary.replace(destination)

    def fingerprint(self):
        files = set()
        for folder in (*TARGETS, "scripts", "docs", ".cargo"):
            directory = self.root / folder
            if directory.is_dir():
                files.update(path for path in directory.rglob("*") if path.is_file()
                             and path.suffix in (".rs", ".py", ".sh", ".md", ".toml"))
        files.update(path for path in self.root.iterdir() if path.is_file()
                     and (path.suffix in (".md", ".toml") or path.name == "Cargo.lock"))
        digest = hashlib.sha256()
        for path in sorted(files):
            digest.update(str(path.relative_to(self.root)).encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
        return {"sha256": digest.hexdigest(), "files": len(files)}

    def command(self, name, command, kind="gate"):
        stage = {"name": name, "kind": kind, "command": command, "status": "INCOMPLETE"}
        self.report["stages"].append(stage)
        self.save()
        log = self.directory / (str(len(self.report["stages"])) + "-" + name + ".log")
        stage["log"] = str(log.relative_to(self.root))
        start = time.monotonic()
        output = ""
        print("==> " + " ".join(command), flush=True)
        try:
            # Open capture before execution; a missing log must never count as a pass.
            with log.open("w") as capture:
                result = self.execute(command, cwd=self.root, env=self.environment,
                                      stdout=capture, stderr=subprocess.STDOUT,
                                      timeout=7200, check=False)
            stage["returncode"] = result.returncode
            output = log.read_text(errors="replace")
            stage["status"] = "PASS" if result.returncode == 0 else "FAIL"
        except (OSError, subprocess.TimeoutExpired) as error:
            stage["error"] = str(error)
        finally:
            stage["duration_seconds"] = round(time.monotonic() - start, 3)
            self.save()
        return stage, output

    def metadata(self):
        self.report["source_before"] = self.fingerprint()
        for name, command in (
            ("revision", ["git", "rev-parse", "HEAD"]),
            ("working-tree", ["git", "status", "--porcelain=v1"]),
            ("compiler", ["rustc", "--version", "--verbose"]),
        ):
            stage, output = self.command(name, command, "provenance")
            self.report[name] = output.strip() if stage["status"] == "PASS" else None
        self.report["features"] = {crate: self.features(crate) or ["default"] for crate in self.selected}
        self.report["build_environment"] = {key: self.environment[key] for key in ("CARGO_INCREMENTAL", "CARGO_BUILD_JOBS")}
        self.save()

    @staticmethod
    def inventory(output):
        return [line.removesuffix(": test") for line in output.splitlines() if line.endswith(": test")]

    @staticmethod
    def validate_execution(stage, output, inventory):
        summaries = SUMMARY.findall(output)
        if len(summaries) != 1:
            stage.update(status="INCOMPLETE", error="missing or ambiguous final test summary")
            return
        outcome, *counts = summaries[0]
        passed, failed, ignored, measured, filtered = map(int, counts)
        stage["counts"] = dict(passed=passed, failed=failed, ignored=ignored, measured=measured, filtered=filtered)
        # Single-threaded terse harness output records every executed test by name.
        rows = re.findall(r"^test (\S+)(?: - should panic)? \.\.\. (ok|FAILED|ignored)(?: .*)?$", output, re.M)
        executed = [name for name, _ in rows]
        actual_counts = [sum(outcome == status for _, outcome in rows) for status in ("ok", "FAILED", "ignored")]
        if (ignored or measured or filtered or passed + failed != len(inventory)
                or sorted(executed) != sorted(inventory)
                or actual_counts != [passed, failed, ignored]):
            stage.update(status="FAIL", error="execution did not cover the complete nonignored inventory")
        elif failed or outcome != "ok" or stage.get("returncode") != 0:
            stage["status"] = "FAIL"
        else:
            stage["status"] = "PASS"

    def behavioral(self, crate, target):
        label = crate + "-" + target
        if not (self.root / crate / "tests" / (target + ".rs")).is_file():
            self.report["stages"].append({"name": label, "kind": "behavioral", "status": "INCOMPLETE", "error": "required target source is missing"})
            self.save()
            return
        base = self.cargo("test", crate, "--test", target)
        listing, output = self.command(label + "-inventory", base + ["--", "--list", "--format", "terse"], "inventory")
        inventory = self.inventory(output)
        listing["inventory"] = inventory
        if listing["status"] != "PASS":
            self.save()
            return
        if not inventory or len(set(inventory)) != len(inventory):
            listing.update(status="FAIL", error="empty or duplicate behavioral inventory")
            self.save()
            return
        stage, output = self.command(label, base + ["--", "--test-threads=1"], "behavioral")
        self.validate_execution(stage, output, inventory)
        self.save()

    def mocks(self):
        for crate in self.selected:
            for target in TARGETS[crate]:
                self.behavioral(crate, target)

    def ci(self):
        self.command("format", ["cargo", "fmt", "--all", "--check"])
        self.command("no-path-wiring", ["bash", "scripts/check_no_path_wiring.sh"])
        self.command("no-type-aliases", ["bash", "scripts/check_no_type_aliases.sh"])
        self.command("runner-contracts", [sys.executable, "-m", "unittest", "discover", "-s", "scripts/tests", "-p", "test_mock_contract_runner.py", "-v"])
        for crate, targets in TARGETS.items():
            arguments = [argument for target in targets for argument in ("--test", target)]
            self.command(crate + "-positive-compile", self.cargo("test", crate, *arguments, "--no-run"), "compile-only")
            self.command(crate + "-clippy", self.cargo("clippy", crate, "--lib", *arguments, "--", "-D", "warnings"))
        self.command("app-existing-compile-contract", self.cargo("test", "app-sdk", "--test", "compile_contract", "--no-run"), "compile-only")
        self.mocks()

    def run(self):
        self.metadata()
        self.ci() if self.mode == "ci" else self.mocks()
        self.report["source_after"] = self.fingerprint()
        if self.report["source_after"] != self.report["source_before"]:
            self.report["stages"].append({"name": "source-stability", "status": "INCOMPLETE", "error": "source changed during validation; results are not one coherent source snapshot"})
        states = [stage["status"] for stage in self.report["stages"]]
        self.report["status"] = "FAIL" if "FAIL" in states else "INCOMPLETE" if "INCOMPLETE" in states else "PASS"
        self.save()
        print(f"{self.report['status']}: {self.report['scope']} {self.mode}; evidence: {self.directory}", flush=True)
        return 0 if self.report["status"] == "PASS" else 1

    @classmethod
    def main(cls, arguments=None):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        subparsers = parser.add_subparsers(dest="mode", required=True)
        mocks = subparsers.add_parser("mocks", allow_abbrev=False)
        mocks.add_argument("--crate", choices=tuple(TARGETS))
        subparsers.add_parser("ci", allow_abbrev=False)
        args = parser.parse_args(arguments)
        selected = (args.crate,) if args.mode == "mocks" and args.crate else tuple(TARGETS)
        try:
            return cls(Path(__file__).resolve().parent.parent, selected, args.mode).run()
        except (OSError, KeyboardInterrupt) as error:
            print(f"INCOMPLETE: evidence capture or execution interrupted: {error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(Runner.main())
