"""Ordinary verifier process contracts; command fixtures are not live evidence."""
import json
import os
from pathlib import Path
import re
import select
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class QuestionGateFixture:
    CASES = (
        "strict_live_suite::delivery_and_events::opencode_question_delivery_replays_resolves_once_and_drains",
        "strict_live_suite::prompt_and_io::opencode_native_idle_terminal_waiter_reopens_in_one_build",
    )
    PREFIX = "AGENT_SDK_QUESTION_GRADE="

    def __init__(self):
        self.crate = Path(__file__).resolve().parents[1]
        self.root = Path(tempfile.mkdtemp(prefix="agent-question-grade-")).resolve()
        self.child = None
        self.cleanup_errors = []
        self.stdout = self.stderr = ""
        self.exit_code = None
        self.calls = []
        self.config_observations = []
        self.logs = []
        self.log_modes = []
        self.log_directory_modes = []
        self.observation_error = None

    def receipts(self, exact=True):
        spec = (self.crate / "specs/SPECIFICATIONS.md").read_text()
        version = re.search(r"^- OpenCode Server: `v([^`]+)`$", spec, re.M).group(1)
        return [{"version": 2, "case": case, "sdk_contract": True,
                 "model_yes_description_exact_copy": True, "model_no_description_exact_copy": True,
                 "model_exact_copy": exact, "runtime_version": version,
                 "provider_id": "fixture-provider", "model_id": "fixture-model",
                 "configured_model": "fixture-model"} for case in self.CASES]

    def lines(self, receipts):
        return [self.PREFIX + json.dumps(receipt) for receipt in receipts]

    def executable(self, name, source):
        path = self.root / "bin" / name
        path.write_text(source)
        path.chmod(0o700)

    def observe(self, lines, cargo_exit=0, fail_log=False, configured_model="fixture-model", report_case=None):
        try:
            scripts = self.root / "agent-sdk/scripts"
            scripts.mkdir(parents=True)
            (self.root / "scripts").mkdir()
            shutil.copyfile(self.crate.parent / "scripts/release_stage.sh", self.root / "scripts/release_stage.sh")
            (self.root / "agent-sdk/specs").mkdir()
            (self.root / "agent-sdk/tests").mkdir()
            (self.root / "bin").mkdir()
            for name in ("verify_strict_live.sh", "report_question_grades.py"):
                source = self.crate / "scripts" / name
                if source.exists():
                    shutil.copyfile(source, scripts / name)
            if report_case is not None:
                verifier = scripts / "verify_strict_live.sh"
                source = verifier.read_text()
                command = 'python3 agent-sdk/scripts/report_question_grades.py "${STRICT_LOG}"'
                if source.count(command) != 1:
                    raise ValueError("Question reporter command changed")
                # Exercise the real reporter's selected-case CLI without native work.
                verifier.write_text(source.replace(command, command + " --case " + shlex.quote(report_case), 1))
            shutil.copyfile(self.crate / "specs/SPECIFICATIONS.md", self.root / "agent-sdk/specs/SPECIFICATIONS.md")
            precheck = '''import json,os
with open(os.environ['FIXTURE_CONFIG_OBSERVATIONS'], 'a') as log:
 log.write(json.dumps({'process': 'precheck', 'selection': {key: os.environ.get(key) for key in ('TEST_CODEX_MODEL_DEFAULT', 'TEST_CODEX_REASONING_EFFORT')}}) + '\\n')
'''
            (scripts / "precheck_strict_live.sh").write_text(
                f"#!/bin/bash\nexec {shlex.quote(sys.executable)} -c {shlex.quote(precheck)}\n")
            self.executable("python3", f"#!{sys.executable}\nimport os,sys\nif sys.argv[1].endswith('report_question_grades.py'):\n os.execv({sys.executable!r},[{sys.executable!r}]+sys.argv[1:])\n")
            self.executable("cargo", f"#!{sys.executable}\n" + '''import json,os,sys
with open(os.environ['FIXTURE_CALLS'], 'a') as log:
 log.write(json.dumps(sys.argv[1:]) + '\\n')
with open(os.environ['FIXTURE_CONFIG_OBSERVATIONS'], 'a') as log:
 log.write(json.dumps({'process': 'cargo', 'selection': {key: os.environ.get(key) for key in ('TEST_CODEX_MODEL_DEFAULT', 'TEST_CODEX_REASONING_EFFORT')}}) + '\\n')
args = sys.argv[1:]
if '--test' in args and args[args.index('--test') + 1] == 'strict_live':
 for line in json.loads(os.environ['FIXTURE_LINES']): print(line, flush=True)
 raise SystemExit(int(os.environ['FIXTURE_CARGO_EXIT']))
''')
            if fail_log:
                # The real tee receives an unwritable destination (a directory),
                # while consuming stdin normally. This tests actual log failure.
                real_tee = shutil.which("tee")
                if not real_tee:
                    raise RuntimeError("real tee is required")
                self.executable("tee", f"#!{sys.executable}\nimport os,sys\nfrom pathlib import Path\np=Path(sys.argv[1])\np.unlink()\np.mkdir()\nos.execv({real_tee!r},[{real_tee!r}]+sys.argv[1:])\n")
            env = {"PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"],
                   "LC_ALL": "C", "FIXTURE_CALLS": str(self.root / "calls.jsonl"),
                   "FIXTURE_CONFIG_OBSERVATIONS": str(self.root / "config-observations.jsonl"),
                   "FIXTURE_LINES": json.dumps(lines), "FIXTURE_CARGO_EXIT": str(cargo_exit),
                   "TEST_CODEX_MODEL_DEFAULT": "gpt-6-luna",
                   "TEST_CODEX_REASONING_EFFORT": "high"}
            if configured_model is not None:
                env["TEST_OLLAMA_MODEL_OPENCODE_SHELL"] = configured_model
            self.child = subprocess.Popen(["bash", str(scripts / "verify_strict_live.sh")],
                                          cwd=self.root, env=env, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE, text=True, start_new_session=True)
            self.stdout, self.stderr = self.child.communicate(timeout=15)
            self.exit_code = self.child.returncode
            self.calls = [json.loads(line) for line in (self.root / "calls.jsonl").read_text().splitlines()]
            self.config_observations = [json.loads(line) for line in (self.root / "config-observations.jsonl").read_text().splitlines()]
            for path in sorted((self.root / "target").glob("**/strict-live.log")):
                if path.is_file():
                    self.logs.append(path.read_text())
                    self.log_modes.append(path.stat().st_mode & 0o777)
                    self.log_directory_modes.append(path.parent.stat().st_mode & 0o777)
        except BaseException as error:
            self.observation_error = error
        finally:
            self.cleanup()
        self.restore_observation()
        selected = [call for call in self.calls if '--test' in call and call[call.index('--test') + 1] == 'strict_live']
        if len(selected) != 1:
            raise AssertionError(f"expected one strict-live invocation, got {selected!r}")
        expected = ['test', '--locked', '-p', 'agent-sdk', '--test', 'strict_live', '--all-features', '--', '--nocapture', '--test-threads=1']
        if selected[0] != expected:
            raise AssertionError(f"changed live selection: {selected[0]!r}")

    def group_exists(self):
        try:
            os.killpg(self.child.pid, 0)
        except ProcessLookupError:
            return False
        return True

    def cleanup(self):
        settled = self.child is None
        if self.child is not None:
            try:
                settled = self.child.poll() is not None and not self.group_exists()
                for stop in (signal.SIGTERM, signal.SIGKILL):
                    if settled:
                        break
                    deadline = time.monotonic() + 3
                    try:
                        os.killpg(self.child.pid, stop)
                    except ProcessLookupError:
                        pass
                    try:
                        self.child.communicate(timeout=max(0.01, deadline - time.monotonic()))
                    except subprocess.TimeoutExpired:
                        pass
                    # A joined direct child alone does not establish descendant
                    # completion. Observe disappearance of this exact owned group.
                    while self.group_exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    settled = self.child.poll() is not None and not self.group_exists()
            except BaseException as error:
                self.cleanup_errors.append(repr(error))
        if not settled:
            self.cleanup_errors.append(f"fixture ownership unsettled; retained {self.root}")
            return
        try:
            shutil.rmtree(self.root)
        except OSError as error:
            self.cleanup_errors.append(repr(error))

    def restore_observation(self):
        for error in self.cleanup_errors[:8]:
            print("Question gate fixture cleanup: " + error[:1024], file=sys.stderr)
        if self.observation_error is not None:
            raise self.observation_error
        if self.cleanup_errors:
            raise AssertionError(self.cleanup_errors)


class QuestionGradeContract(unittest.TestCase):
    def test_explicit_codex_selection_reaches_precheck_and_cargo(self):
        fixture = QuestionGateFixture()
        fixture.observe(fixture.lines(fixture.receipts()))
        self.assertEqual(fixture.exit_code, 0, fixture.stderr)
        self.assertEqual(
            [row["process"] for row in fixture.config_observations],
            ["precheck"] + ["cargo"] * len(fixture.calls))
        self.assertGreater(len(fixture.calls), 0)
        for row in fixture.config_observations:
            self.assertEqual(row["selection"], {
                "TEST_CODEX_MODEL_DEFAULT": "gpt-6-luna",
                "TEST_CODEX_REASONING_EFFORT": "high",
            })

    def test_explicit_selected_case_receipt_is_scoped_and_nonblocking(self):
        fixture = QuestionGateFixture()
        receipt = fixture.receipts(exact=False)[0]
        receipt["model_yes_description_exact_copy"] = False
        fixture.observe(fixture.lines([receipt]), report_case=fixture.CASES[0])
        self.assertEqual(fixture.exit_code, 0, fixture.stderr)
        self.assertIn("Question SDK receipts: PASS (1/1)", fixture.stdout)
        self.assertIn("selected case only; full two-case gate not implied", fixture.stdout)
        self.assertIn("FAILED (1/1); nonblocking", fixture.stdout)
        self.assertNotIn("PASS (2/2)", fixture.stdout)
        self.assertNotIn(fixture.CASES[1], fixture.stdout)

    def test_selected_case_rejects_missing_foreign_duplicate_legacy_and_failed_receipts(self):
        for variant in ("missing", "foreign", "duplicate", "legacy", "sdk_failed", "unknown_selection", "both"):
            with self.subTest(variant=variant):
                fixture = QuestionGateFixture()
                receipts = fixture.receipts()[:1]
                selected = fixture.CASES[0]
                if variant == "missing": receipts = []
                elif variant == "foreign": receipts = fixture.receipts()[1:]
                elif variant == "duplicate": receipts *= 2
                elif variant == "legacy": receipts[0]["version"] = 1
                elif variant == "sdk_failed": receipts[0]["sdk_contract"] = False
                elif variant == "unknown_selection": selected = "not-an-approved-case"
                elif variant == "both": receipts = fixture.receipts()
                fixture.observe(fixture.lines(receipts), report_case=selected)
                self.assertEqual(fixture.exit_code, 1, fixture.stderr)
                self.assertNotIn("Question SDK receipts: PASS", fixture.stdout)

    def test_entered_descendant_settles_before_fixture_files_are_removed(self):
        fixture = QuestionGateFixture()
        # The parent retains and joins its entered descendant. It ignores TERM
        # itself so the direct-child exit proves the descendant was reaped.
        descendant = "import signal; signal.signal(signal.SIGTERM, signal.SIG_DFL); print('ENTERED', flush=True); signal.pause()"
        parent = "import signal,subprocess,sys; signal.signal(signal.SIGTERM, signal.SIG_IGN); child=subprocess.Popen([sys.executable,'-c',sys.argv[1]]); child.wait()"
        entered = False
        try:
            fixture.child = subprocess.Popen([sys.executable, "-c", parent, descendant],
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             text=True, start_new_session=True)
            ready, _, _ = select.select([fixture.child.stdout], [], [], 3)
            if ready:
                entered = fixture.child.stdout.readline() == "ENTERED\n"
            if not entered:
                raise AssertionError("descendant did not enter its gate")
            fixture.observation_error = RuntimeError("observer lost after descendant entry")
        finally:
            fixture.cleanup()
        self.assertTrue(entered)
        self.assertEqual(fixture.cleanup_errors, [])
        self.assertEqual(fixture.child.returncode, 0)
        self.assertFalse(fixture.group_exists())
        self.assertFalse(fixture.root.exists())
        with self.assertRaisesRegex(RuntimeError, "observer lost after descendant entry"):
            fixture.restore_observation()

    def test_false_copy_is_visible_nonblocking_with_complete_sdk_receipts(self):
        for grades in ((False, False), (True, False), (False, True)):
            with self.subTest(grades=grades):
                fixture = QuestionGateFixture()
                receipts = fixture.receipts()
                for receipt, grade in zip(receipts, grades):
                    receipt["model_exact_copy"] = grade
                lines = fixture.lines(receipts)
                fixture.observe(lines)
                self.assertEqual(fixture.exit_code, 0, fixture.stderr)
                failures = sum(not grade for grade in grades)
                self.assertIn(f"Model raw-copy quality: FAILED ({failures}/2); nonblocking", fixture.stdout)
                self.assertIn("Question SDK receipts: PASS (2/2)", fixture.stdout)
                self.assertEqual(fixture.logs, ["\n".join(lines) + "\n"])
                self.assertEqual(fixture.log_modes, [0o600])
                self.assertEqual(fixture.log_directory_modes, [0o700])
                self.assertTrue(any(call[:1] == ["clippy"] for call in fixture.calls))
                self.assertTrue(any("strict_live_assertions" in call for call in fixture.calls))

    def test_description_grades_are_independent_visible_and_nonblocking(self):
        for grades in ((False, False), (True, False), (False, True), (True, True)):
            with self.subTest(grades=grades):
                fixture = QuestionGateFixture()
                receipts = fixture.receipts()
                for receipt in receipts:
                    receipt["model_yes_description_exact_copy"] = grades[0]
                    receipt["model_no_description_exact_copy"] = grades[1]
                fixture.observe(fixture.lines(receipts))
                self.assertEqual(fixture.exit_code, 0, fixture.stderr)
                self.assertIn("Question SDK receipts: PASS (2/2)", fixture.stdout)
                for case in fixture.CASES:
                    for label, grade in zip(("Yes", "No"), grades):
                        outcome = "EXACT" if grade else "FAILED (nonblocking)"
                        self.assertIn(f"Model description-copy quality [{label}]: {outcome}: {case}", fixture.stdout)
                self.assertEqual(fixture.logs, ["\n".join(fixture.lines(receipts)) + "\n"])

    def test_description_grades_missing_or_wrong_types_fail(self):
        for field in ("model_yes_description_exact_copy", "model_no_description_exact_copy"):
            for value in ("missing", None, 0, 1, "false", [], {}):
                with self.subTest(field=field, value=value):
                    fixture = QuestionGateFixture()
                    receipts = fixture.receipts()
                    if value == "missing":
                        del receipts[0][field]
                    else:
                        receipts[0][field] = value
                    fixture.observe(fixture.lines(receipts))
                    self.assertEqual(fixture.exit_code, 1)
                    self.assertIn("Question SDK receipt report: FAILED", fixture.stderr)
                    self.assertNotIn("Question SDK receipts: PASS", fixture.stdout)

    def test_true_copy_is_reported_without_general_quality_acceptance(self):
        fixture = QuestionGateFixture()
        fixture.observe(fixture.lines(fixture.receipts()))
        self.assertEqual(fixture.exit_code, 0, fixture.stderr)
        self.assertIn("Model raw-copy quality: observed exact copies 2/2; no general quality acceptance", fixture.stdout)

    def test_missing_receipt_fails_even_with_successful_live_command(self):
        for count in (0, 1):
            with self.subTest(count=count):
                fixture = QuestionGateFixture()
                fixture.observe(fixture.lines(fixture.receipts()[:count]))
                self.assertNotEqual(fixture.exit_code, 0, fixture.stdout)
                self.assertNotIn(["clippy", "--locked"], [call[:2] for call in fixture.calls])

    def test_malformed_unknown_duplicate_and_invalid_receipts_fail(self):
        rows = ("malformed", "deeply_nested", "unknown", "duplicate", "sdk_false", "quality_string", "version_bool",
                "version_float", "version_unknown", "version_legacy", "extra_field", "missing_field", "duplicate_key",
                "wrong_runtime", "wrong_configured_model", "empty_provider", "empty_model", "unanchored")
        for row in rows:
            with self.subTest(row=row):
                fixture = QuestionGateFixture()
                receipts = fixture.receipts()
                first = receipts[0]
                if row == "unknown": first["case"] = "not-an-approved-case"
                elif row == "sdk_false": first["sdk_contract"] = False
                elif row == "quality_string": first["model_exact_copy"] = "false"
                elif row == "version_bool": first["version"] = True
                elif row == "version_float": first["version"] = 2.0
                elif row == "version_unknown": first["version"] = 3
                elif row == "version_legacy":
                    first["version"] = 1
                    del first["model_yes_description_exact_copy"]
                    del first["model_no_description_exact_copy"]
                elif row == "extra_field": first["extra"] = True
                elif row == "missing_field": del first["runtime_version"]
                elif row == "wrong_runtime": first["runtime_version"] = "wrong"
                elif row == "wrong_configured_model": first["configured_model"] = "other-model"
                elif row == "empty_provider": first["provider_id"] = " "
                elif row == "empty_model": first["model_id"] = ""
                lines = fixture.lines(receipts)
                if row == "malformed": lines[0] = fixture.PREFIX + "{"
                elif row == "deeply_nested":
                    lines[0] = fixture.PREFIX + "[" * 1500 + json.dumps("private-receipt-marker") + "]" * 1500
                    self.assertLess(len(lines[0]), 4096)
                elif row == "duplicate": lines.append(lines[0])
                elif row == "duplicate_key": lines[0] = lines[0][:-1] + ', "sdk_contract": true}'
                elif row == "unanchored": lines[0] = "test prefix " + lines[0]
                fixture.observe(lines)
                self.assertNotEqual(fixture.exit_code, 0, fixture.stdout)
                self.assertIn("Question SDK receipt report: FAILED", fixture.stderr)
                if row == "deeply_nested":
                    # JSON recursion protection varies by Python version. A parsed
                    # array must still fail the receipt's required object shape.
                    self.assertIn(fixture.stderr.splitlines(), [
                        ["Question SDK receipt report: FAILED (RecursionError)"],
                        ["Question SDK receipt report: FAILED (ValueError)"],
                    ])
                    self.assertEqual(fixture.exit_code, 1)
                    self.assertNotIn("Question SDK receipts: PASS", fixture.stdout)
                    self.assertNotIn("Traceback", fixture.stderr)
                    self.assertNotIn("private-receipt-marker", fixture.stderr)

    def test_missing_model_configuration_cannot_validate_receipts(self):
        fixture = QuestionGateFixture()
        fixture.observe(fixture.lines(fixture.receipts()), configured_model=None)
        self.assertNotEqual(fixture.exit_code, 0, fixture.stdout)

    def test_cargo_failure_remains_primary_with_valid_or_missing_receipts(self):
        for present in (True, False):
            with self.subTest(present=present):
                fixture = QuestionGateFixture()
                fixture.observe(fixture.lines(fixture.receipts()) if present else [], cargo_exit=101)
                self.assertEqual(fixture.exit_code, 101, fixture.stderr)
                self.assertIn("Question SDK", fixture.stdout + fixture.stderr)
                self.assertFalse(any(call[:1] == ["clippy"] for call in fixture.calls))

    def test_real_tee_failure_blocks_even_with_valid_receipts(self):
        fixture = QuestionGateFixture()
        fixture.observe(fixture.lines(fixture.receipts()), fail_log=True)
        self.assertNotEqual(fixture.exit_code, 0, fixture.stdout)
        self.assertIn("strict-live log capture failed", fixture.stderr)
        self.assertIn("Question SDK receipt report: FAILED", fixture.stderr)

    def test_cargo_failure_remains_primary_when_real_tee_and_report_also_fail(self):
        fixture = QuestionGateFixture()
        fixture.observe(fixture.lines(fixture.receipts()), cargo_exit=101, fail_log=True)
        self.assertEqual(fixture.exit_code, 101, fixture.stderr)
        self.assertIn("strict-live log capture failed", fixture.stderr)
        self.assertIn("Question SDK receipt report: FAILED", fixture.stderr)


if __name__ == "__main__":
    unittest.main()
