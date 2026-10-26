"""Runner faults use command recording, never a stand-in backend service."""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import select
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("mock_contract_runner", Path(__file__).resolve().parents[1] / "mock_contract_runner.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class Commands:
    def __init__(self, fault=None):
        self.calls = []
        self.fault = fault

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        output, code = "", 0
        if command[:2] == ["git", "rev-parse"]:
            output = "0123456789\n"
        elif command[0] == "rustc":
            output = "rustc fixture\n"
        elif command[:2] == ["cargo", "test"] and "--no-run" not in command:
            target = command[command.index("--test") + 1]
            fault = self.fault if target == "mock_contracts" else None
            if fault == "compiler":
                output, code = "error: unresolved import\n", 101
            elif "--list" in command:
                output = "" if fault == "empty" else "case_a: test\ncase_b: test\n"
            elif fault == "failure":
                output, code = "test case_a ... ok\ntest case_b ... FAILED\ntest result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out;\n", 101
            elif fault == "ignored":
                output = "test case_a ... ok\ntest case_b ... ignored\ntest result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out;\n"
            elif fault == "filtered":
                output = "test case_a ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out;\n"
            elif fault == "wrong-name":
                output = "test unrelated ... ok\ntest case_b ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n"
            elif fault == "crash":
                output, code = "test case_a ... ", -9
            else:
                output = "test case_a ... ok\ntest case_b ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n"
        kwargs["stdout"].write(output)
        return subprocess.CompletedProcess(command, code)


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for crate, targets in RUNNER.TARGETS.items():
            directory = self.root / crate / "tests"
            directory.mkdir(parents=True)
            for target in targets:
                (directory / (target + ".rs")).write_text("// fixture target marker\n")

    def run_fixture(self, fault=None, selected=None, mode="mocks"):
        commands = Commands(fault)
        runner = RUNNER.Runner(self.root, selected or tuple(RUNNER.TARGETS), mode, commands)
        with contextlib.redirect_stdout(io.StringIO()):
            result = runner.run()
        report = json.loads((runner.directory / "report.json").read_text())
        return result, report, commands, runner

    def test_all_targets_are_separate_locked_complete_and_serial(self):
        result, report, commands, _ = self.run_fixture()
        self.assertEqual(result, 0)
        self.assertEqual(report["status"], "PASS")
        executed = [row for row in report["stages"] if row["kind"] == "behavioral"]
        self.assertEqual(len(executed), 7)
        self.assertTrue(all(row["counts"]["passed"] == 2 for row in executed))
        for command in commands.calls:
            if command[0] != "cargo":
                continue
            self.assertIn("--locked", command)
            self.assertEqual(command.count("--test"), 1)
            self.assertEqual("--no-default-features" in command, "storage-sdk" in command)
            self.assertNotIn("--all-features", command)

    def test_scoped_report_does_not_claim_workspace_pass(self):
        result, report, commands, _ = self.run_fixture(selected=("tool-sdk",))
        self.assertEqual(result, 0)
        self.assertEqual(report["scope"], "scoped")
        self.assertEqual(report["crates"], ["tool-sdk"])
        self.assertFalse(any("app-sdk" in call for call in commands.calls))

    def test_compiler_failure_preserves_independent_target_execution(self):
        result, report, commands, _ = self.run_fixture("compiler", ("agent-sdk",))
        self.assertEqual(result, 1)
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(any("custom_mock_composition" in call and "--list" not in call for call in commands.calls))

    def test_behavioral_faults_cannot_be_pass_evidence(self):
        for fault in ("failure", "ignored", "filtered", "empty", "wrong-name", "crash"):
            with self.subTest(fault=fault):
                result, report, _, _ = self.run_fixture(fault, ("tool-sdk",))
                self.assertEqual(result, 1)
                self.assertNotEqual(report["status"], "PASS")

    def test_per_case_outcomes_must_agree_with_summary(self):
        for label in ("FAILED", "ignored"):
            stage = {"status": "PASS", "returncode": 0}
            output = f"test case_a ... {label}\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n"
            RUNNER.Runner.validate_execution(stage, output, ["case_a"])
            self.assertEqual(stage["status"], "FAIL")

    def test_ambiguous_summaries_are_incomplete(self):
        stage = {"status": "PASS", "returncode": 0}
        output = "test case_a ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n"
        RUNNER.Runner.validate_execution(stage, output + output, ["case_a"])
        self.assertEqual(stage["status"], "INCOMPLETE")

    def test_expected_panic_self_tests_keep_the_exact_inventory_name(self):
        stage = {"status": "PASS", "returncode": 0}
        output = "test case_a - should panic ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n"
        RUNNER.Runner.validate_execution(stage, output, ["case_a"])
        self.assertEqual(stage["status"], "PASS")

    def test_source_mutation_invalidates_the_receipt(self):
        runner = RUNNER.Runner(self.root, ("tool-sdk",), "mocks", Commands())
        source = self.root / "tool-sdk/tests/mock_contracts.rs"
        with patch.object(runner, "mocks", side_effect=lambda: source.write_text("// changed\n")), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.run(), 1)
        self.assertEqual(runner.report["status"], "INCOMPLETE")

    def test_provenance_failure_cannot_report_a_pass(self):
        commands = Commands()
        def unavailable(command, **kwargs):
            if command[0] in ("git", "rustc"):
                return subprocess.CompletedProcess(command, 1)
            return commands(command, **kwargs)
        runner = RUNNER.Runner(self.root, ("tool-sdk",), "mocks", unavailable)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.run(), 1)
        self.assertEqual(runner.report["status"], "FAIL")

    def test_missing_target_is_incomplete_and_other_targets_run(self):
        (self.root / "app-sdk/tests/mock_contracts.rs").unlink()
        result, report, commands, _ = self.run_fixture(selected=("app-sdk",))
        self.assertEqual(result, 1)
        self.assertEqual(report["status"], "INCOMPLETE")
        self.assertTrue(any("workflow_mock_composition" in call for call in commands.calls))

    def test_log_open_failure_never_executes_or_passes(self):
        commands = Commands()
        runner = RUNNER.Runner(self.root, ("tool-sdk",), "mocks", commands)
        (runner.directory / "1-capture.log").mkdir()
        with contextlib.redirect_stdout(io.StringIO()):
            stage, _ = runner.command("capture", ["cargo", "test"])
        self.assertEqual(stage["status"], "INCOMPLETE")
        self.assertEqual(commands.calls, [])

    def test_timeout_and_launch_failure_are_incomplete(self):
        for error in (subprocess.TimeoutExpired("cargo", 7200), FileNotFoundError("cargo")):
            runner = RUNNER.Runner(self.root, ("tool-sdk",), "mocks")
            with patch.object(runner, "execute", side_effect=error), contextlib.redirect_stdout(io.StringIO()):
                stage, _ = runner.command("launch", ["cargo", "test"])
            self.assertEqual(stage["status"], "INCOMPLETE")

    def test_timeout_settles_grandchildren_before_returning(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        child = "import signal; print('ready', flush=True); signal.pause()"
        parent = "import subprocess, sys, signal; subprocess.Popen([sys.executable, '-c', sys.argv[1]]); signal.pause()"
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                RUNNER.Runner.owned_process([sys.executable, "-c", parent, child], stdout=write_fd,
                                           stderr=subprocess.DEVNULL, timeout=1, check=False)
        finally:
            os.close(write_fd)
        chunks = []
        while True:
            ready, _, _ = select.select([read_fd], [], [], 2)
            self.assertTrue(ready, "a surviving descendant still owns the output pipe")
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
        self.assertIn(b"ready", b"".join(chunks), "grandchild must start before the timeout")

    def test_sigterm_settles_the_owned_group_before_runner_exits(self):
        child = "import signal; print('ready', flush=True); signal.pause()"
        parent = "import subprocess, sys, signal, os; print(os.getpgrp(), flush=True); subprocess.Popen([sys.executable, '-c', sys.argv[1]]); signal.pause()"
        harness = "import sys; sys.path.insert(0, sys.argv[1]); from mock_contract_runner import Runner; Runner.owned_process([sys.executable, '-c', sys.argv[2], sys.argv[3]], timeout=30, check=False)"
        process = subprocess.Popen([sys.executable, "-c", harness, str(Path(__file__).resolve().parents[1]), parent, child], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        group = None
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            group = int(process.stdout.readline().strip())
            self.assertEqual(process.stdout.readline().strip(), "ready")
            process.terminate()
            process.communicate(timeout=5)
            self.assertNotEqual(process.returncode, 0)
        finally:
            if group is not None:
                try:
                    os.killpg(group, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)

    def test_child_does_not_inherit_newly_blocked_signals(self):
        with tempfile.TemporaryFile(mode="w+") as output:
            child = "import signal; print(sorted(int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, set())))"
            result = RUNNER.Runner.owned_process([sys.executable, "-c", child], stdout=output, stderr=subprocess.STDOUT, timeout=5, check=False)
            self.assertEqual(result.returncode, 0)
            output.seek(0)
            inherited = json.loads(output.read())
            expected = sorted(int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, set()))
            self.assertEqual(inherited, expected)

    def test_ci_runs_all_required_stages_despite_failure(self):
        result, report, commands, _ = self.run_fixture("failure", mode="ci")
        self.assertEqual(result, 1)
        self.assertTrue(any("workflow_mock_composition" in call and "--list" not in call for call in commands.calls))
        compilation = [row for row in report["stages"] if row["kind"] == "compile-only"]
        self.assertEqual(len(compilation), 6)
        self.assertTrue(all(row["status"] == "PASS" for row in compilation))
        self.assertTrue(any("compile_contract" in call and "--no-run" in call for call in commands.calls))

    def test_fresh_runs_never_reuse_pass_receipts(self):
        _, _, _, first = self.run_fixture(selected=("tool-sdk",))
        result, _, commands, second = self.run_fixture("failure", ("tool-sdk",))
        self.assertNotEqual(first.directory, second.directory)
        self.assertEqual(result, 1)
        self.assertEqual(len(commands.calls), 5)

    def test_invalid_cli_arguments_fail_before_execution(self):
        for args in (("mocks", "--crate", "unknown"), ("mocks", "case_filter"),
                     ("mocks", "--ignored"), ("mocks", "--crate"),
                     ("ci", "--crate", "tool-sdk"), ("mocks", "--cr", "tool-sdk")):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as failure:
                    RUNNER.Runner.main(args)
                self.assertEqual(failure.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
