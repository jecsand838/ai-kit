#!/usr/bin/env python3
"""Ordinary orchestration tests; no App, Cargo, or service execution is claimed."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "check_app_workloads.py"
SPEC = importlib.util.spec_from_file_location("app_workloads", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
AppWorkloads = MODULE.AppWorkloads


class AppWorkloadTests(unittest.TestCase):
    CALIBRATORS = (
        "storage_observation_cancelled_wait_does_not_remain_active",
        "storage_observation_distinguishes_read_only_and_mutating_commits",
        "storage_transaction_observation_separates_native_calls_and_owner_drop",
        "checkpoint_poll_capture_excludes_suspended_time_and_retains_open_spans",
    )

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.owner_count = 0
        self.binary = self.root / "workflow_execution"
        self.binary.write_text("test artifact\n")

    def owner(self, scope="diagnostic", profile=None, case=None):
        self.owner_count += 1
        root = self.root / str(self.owner_count)
        if profile is None and case is None:
            return AppWorkloads(root, scope)
        return AppWorkloads(root, scope, profile=profile, case=case)

    def artifact(self, profile="capacity", **changes):
        settings = dict(opt_level="3" if profile == "capacity" else "0",
                        debug_assertions=True, overflow_checks=True, test=True,
                        debuginfo=0)
        settings.update(changes)
        return json.dumps(dict(reason="compiler-artifact", package_id="path+file:///candidate/app-sdk#0.1.0",
                               target=dict(name="workflow_execution", kind=["test"]),
                               executable=str(self.binary), profile=settings))

    def build_output(self, profile="capacity", **changes):
        rows = []
        for name in AppWorkloads.RUNTIME:
            artifact = json.loads(self.artifact(profile))
            artifact.update(target=dict(name=name, kind=["lib"]), executable=None)
            artifact["profile"]["test"] = False
            rows.append(json.dumps(artifact))
        rows.append(self.artifact(profile, **changes))
        return "\n".join(rows)

    def test_fixed_profiles_and_diagnostic_default(self):
        self.assertEqual(self.owner("routine").profile, "test")
        self.assertEqual(self.owner("capacity").profile, "capacity")
        owner = self.owner()
        self.assertEqual((owner.profile, owner.case), ("test", AppWorkloads.DIAGNOSTIC))

    def test_version_hint_default_is_unset_in_every_scope(self):
        for scope, case in (("routine", None), ("capacity", None),
                            ("diagnostic", AppWorkloads.DIAGNOSTIC),
                            ("diagnostic", AppWorkloads.GROWTH)):
            with self.subTest(scope=scope, case=case), patch.dict(os.environ, {}, clear=True):
                owner = self.owner(scope, case=case)
                owner.preflight()
                self.assertEqual(owner.receipt["version_hint"], {
                    "mode": "native-default", "value": None,
                    "qualification": "ordinary workload policy; no hint override",
                })
                self.assertNotIn("LANCE_USE_VERSION_HINT", os.environ)

    def test_version_hint_zero_is_only_an_explicit_prefix_diagnostic(self):
        for profile in ("test", "capacity"):
            with self.subTest(profile=profile), patch.dict(os.environ, {"LANCE_USE_VERSION_HINT": "0"}, clear=True):
                owner = self.owner(profile=profile)
                before = dict(os.environ)
                owner.preflight()
                self.assertEqual(owner.receipt["version_hint"], {
                    "mode": "explicit-diagnostic-disable", "value": "0",
                    "qualification": "net-policy diagnostic only; not default, growth, or capacity qualification",
                })
                self.assertEqual(dict(os.environ), before)

    def test_version_hint_rejects_other_values_and_nonprefix_scopes(self):
        selections = (("routine", None), ("capacity", None),
                      ("diagnostic", AppWorkloads.DIAGNOSTIC),
                      ("diagnostic", AppWorkloads.GROWTH))
        for value in ("", "1", "false", "off", " 0", "0 ", "00", "0"):
            for scope, case in selections:
                if value == "0" and scope == "diagnostic" and case == AppWorkloads.DIAGNOSTIC:
                    continue
                with self.subTest(value=value, scope=scope, case=case), patch.dict(os.environ, {"LANCE_USE_VERSION_HINT": value}, clear=True):
                    owner = self.owner(scope, case=case)
                    with self.assertRaisesRegex(ValueError, "LANCE_USE_VERSION_HINT"):
                        owner.preflight()
                    self.assertEqual(os.environ["LANCE_USE_VERSION_HINT"], value)

    def test_version_hint_rejection_precedes_any_execution(self):
        owner = self.owner("routine")
        with patch.dict(os.environ, {"LANCE_USE_VERSION_HINT": "0"}, clear=True), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "command") as command, patch.object(owner, "environment") as environment, patch("signal.signal"):
            self.assertEqual(owner.run(), 1)
        command.assert_not_called()
        environment.assert_not_called()
        self.assertEqual(owner.receipt["status"], "INCOMPLETE")
        self.assertIn("LANCE_USE_VERSION_HINT", owner.receipt["error"]["message"])

    def test_invalid_selections_fail_before_creating_evidence(self):
        for scope, profile, case in (("routine", "capacity", None),
                                     ("capacity", "test", None),
                                     ("routine", None, AppWorkloads.DIAGNOSTIC),
                                     ("diagnostic", None, "unknown"),
                                     ("diagnostic", None, AppWorkloads.CAPACITY[0][1])):
            with self.subTest(scope=scope, profile=profile, case=case):
                with self.assertRaises(ValueError):
                    self.owner(scope, profile, case)
        self.assertFalse((self.root / "target").exists())

    def test_no_run_build_returns_verified_artifact_before_timing(self):
        owner = self.owner(profile="capacity")
        with patch.object(owner, "command", return_value=self.build_output()) as command:
            result = owner.build("workflow_execution")
        self.assertEqual(result, [str(self.binary)])
        args = command.call_args.args[1]
        for flag in ("--no-run", "--locked", "--message-format=json"):
            self.assertIn(flag, args)
        self.assertEqual(args[args.index("--profile") + 1], "capacity")
        self.assertEqual(args[args.index("--jobs") + 1], "1")
        self.assertEqual(owner.receipt["artifacts"]["workflow_execution"]["profile"]["opt_level"], "3")

    def test_artifact_profile_mismatches_fail_closed(self):
        for changes in (dict(opt_level="0"), dict(debug_assertions=False),
                        dict(overflow_checks=False), dict(test=False)):
            with self.subTest(changes=changes):
                owner = self.owner(profile="capacity")
                with patch.object(owner, "command", return_value=self.build_output(**changes)):
                    with self.assertRaises(ValueError):
                        owner.build("workflow_execution")

    def test_empty_duplicate_or_wrong_target_artifact_fails(self):
        wrong = json.loads(self.artifact())
        wrong["target"]["name"] = "worker_contract"
        for output in ("", self.artifact() + "\n" + self.artifact(), json.dumps(wrong)):
            owner = self.owner(profile="capacity")
            with patch.object(owner, "command", return_value=output):
                with self.assertRaises(ValueError):
                    owner.build("workflow_execution")

    def test_conflicting_overrides_fail_but_low_memory_debug_is_allowed(self):
        owner = self.owner(profile="capacity")
        for overrides in ({"RUSTFLAGS": "-C opt-level=0"},
                          {"CARGO_BUILD_RUSTFLAGS": "-C opt-level=0"},
                          {"CARGO_INCREMENTAL": "1"},
                          {"RUSTFLAGS": "-C incremental=/tmp/unqualified"},
                          {"CARGO_BUILD_RUSTC_WRAPPER": "/tmp/unqualified"},
                          {"RUSTFLAGS": "-Cdebug-assertions=no"},
                          {"RUSTFLAGS": "--codegen=opt-level=0"},
                          {"RUSTFLAGS": "--codegen overflow-checks=off"},
                          {"CARGO_PROFILE_CAPACITY_CODEGEN_UNITS": "256"},
                          {"CARGO_ENCODED_RUSTFLAGS": "-C\x1foverflow-checks=off"},
                          {"CARGO_PROFILE_CAPACITY_DEBUG_ASSERTIONS": "false"},
                          {"CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS": "-Copt-level=1"}):
            with self.subTest(overrides=overrides), patch.dict(os.environ, overrides, clear=True):
                with self.assertRaises(ValueError):
                    owner.preflight()
        with patch.dict(os.environ, {"CARGO_BUILD_JOBS": "1", "CARGO_PROFILE_CAPACITY_DEBUG": "0",
                                     "RUSTFLAGS": "-C debuginfo=0"}, clear=True):
            owner.preflight()

    def test_exact_capacity_and_growth_selection(self):
        for scope, profile, case in (("capacity", None, AppWorkloads.CAPACITY[1][1]),
                                     ("diagnostic", "capacity", "delivery_capacity_growth_measurement")):
            owner = self.owner(scope, profile, case)
            commands = []
            full = {name for target, name, _ in AppWorkloads.CAPACITY if target == "workflow_execution"}
            full.update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, "delivery_capacity_growth_measurement", "ordinary_case"))
            full.update(self.CALIBRATORS)
            def execute(label, arguments, budget):
                commands.append((label, arguments, budget))
                if label == "compiler":
                    return "release: 1.99.0\n"
                if "build" in label:
                    return self.build_output()
                if "inventory" in label:
                    selected = {arguments[1]} if "--exact" in arguments else full
                    return "\n".join(name + ": test" for name in sorted(selected))
                return f"test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; {len(full) - 1} filtered out\n"
            with patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "environment"), patch.object(owner, "preflight"), patch("signal.signal"):
                self.assertEqual(owner.run(), 0)
            labels = [label for label, _, _ in commands]
            self.assertLess(labels.index("workflow_execution-build"), labels.index("workflow_execution-full-inventory"))
            execution = [row for row in commands if row[0].endswith("-execution")]
            self.assertEqual(len(execution), 1)
            self.assertIn(str(self.binary), execution[0][1])
            self.assertIn(case, execution[0][1])
            calibrations = [row for row in commands if row[0].startswith("diagnostic-preflight-") and row[0].endswith("-check")]
            self.assertEqual(len(calibrations), 4 if scope == "diagnostic" else 0)

    def test_config_profile_and_package_overrides_are_rejected(self):
        for config in ("[profile.capacity.package.lance]\nopt-level = 0\n",
                       "[profile.release]\noverflow-checks = false\n",
                       "[build]\nrustflags = [\"--codegen=debug-assertions=no\"]\n",
                       "[build]\nincremental = true\n",
                       "[build]\nrustc-wrapper = \"/tmp/unqualified\"\n"):
            owner = self.owner(profile="capacity")
            folder = owner.root / ".cargo"
            folder.mkdir()
            (folder / "config.toml").write_text(config)
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(ValueError):
                    owner.preflight()
        owner = self.owner(profile="capacity")
        (owner.root / "Cargo.toml").write_text("[profile.capacity.package.lance]\nopt-level=0\n")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "per-package"):
                owner.preflight()

    def test_ignored_execution_never_receives_pass(self):
        owner = self.owner(case=AppWorkloads.DIAGNOSTIC)
        full = {name for target, name, _ in AppWorkloads.CAPACITY if target == "workflow_execution"}
        full.update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH))
        full.update(self.CALIBRATORS)
        def execute(label, arguments, budget):
            if label == "compiler":
                return "release: 1.99.0\n"
            if "inventory" in label:
                selected = {arguments[1]} if "--exact" in arguments else full
                return "\n".join(name + ": test" for name in selected)
            if label.startswith("diagnostic-preflight-"):
                return f"test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; {len(full) - 1} filtered out\n"
            return f"test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; {len(full) - 1} filtered out\n"
        with patch.object(owner, "build", return_value=[str(self.binary)]), patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "environment"), patch.object(owner, "preflight"), patch("signal.signal"):
            self.assertEqual(owner.run(), 1)
        self.assertNotEqual(owner.receipt["status"], "PASS")
        self.assertIn("exact nonempty inventory", owner.receipt["error"]["message"])

    def test_runtime_profile_override_is_rejected(self):
        owner = self.owner(profile="capacity")
        records = [json.loads(line) for line in self.build_output().splitlines()]
        records[3]["profile"]["overflow_checks"] = False
        with patch.object(owner, "command", return_value="\n".join(json.dumps(row) for row in records)):
            with self.assertRaisesRegex(ValueError, "runtime artifact"):
                owner.build("workflow_execution")

    def test_routine_excludes_both_diagnostics_and_all_maxima(self):
        owner = self.owner("routine")
        full = {target: {case for source, case, _ in AppWorkloads.CAPACITY if source == target}
                for target in ("workflow_execution", "worker_contract")}
        full["workflow_execution"].update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH))
        full["workflow_execution"].update(self.CALIBRATORS)
        full["worker_contract"].add("ordinary_worker")
        commands = []
        def execute(label, arguments, budget):
            commands.append((label, arguments, budget))
            if label == "compiler":
                return "release: 1.99.0\n"
            target = next(name for name in full if name in arguments)
            skipped = {arguments[index + 1] for index, value in enumerate(arguments) if value == "--skip"}
            selected = {name for name in full[target] if not any(part in name for part in skipped)}
            if "inventory" in label:
                return "\n".join(name + ": test" for name in selected)
            return f"test result: ok. {len(selected)} passed; 0 failed; 0 ignored; 0 measured; {len(full[target] - selected)} filtered out\n"
        with patch.object(owner, "build", side_effect=lambda target: [target]), patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "environment"), patch.object(owner, "preflight"), patch("signal.signal"):
            self.assertEqual(owner.run(), 0, owner.receipt.get("error"))
        self.assertEqual([row["selected"] for row in owner.receipt["selections"]], [[AppWorkloads.ROUTINE], ["ordinary_worker"]])
        self.assertEqual(len([row for row in commands if row[0].endswith("execution")]), 2)

    def test_resource_accounting_wraps_only_the_execution_child(self):
        owner = self.owner()
        executable = [sys.executable, "-c", "print('resource child')"]
        command = owner.execution_command(executable)
        output = owner.command("resource-execution", command, 5)
        self.assertIn("resource child", output)
        self.assertEqual(owner.receipt["commands"][-1]["exit"], 0)
        if command != executable:
            self.assertIn("maximum resident set size", owner.receipt["commands"][-1]["maximum_resident_set_size_raw"].lower())
            self.assertNotIn("unavailable", owner.receipt["execution_resources"])

    def test_diagnostic_calibration_passes_before_measured_execution(self):
        for profile in ("test", "capacity"):
            for case in (AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH):
                with self.subTest(profile=profile, case=case):
                    owner = self.owner(profile=profile, case=case)
                    full = {name for target, name, _ in AppWorkloads.CAPACITY if target == "workflow_execution"}
                    full.update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH, *self.CALIBRATORS))
                    commands = []
                    def execute(label, arguments, budget):
                        commands.append((label, arguments, budget))
                        if label == "compiler":
                            return "release: 1.99.0\n"
                        if "inventory" in label:
                            selected = {arguments[1]} if "--exact" in arguments else full
                            return "\n".join(name + ": test" for name in selected)
                        return f"test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; {len(full) - 1} filtered out\n"
                    with patch.object(owner, "build", return_value=[str(self.binary)]), patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "environment"), patch.object(owner, "preflight"), patch.object(owner, "execution_command", side_effect=lambda args: args) as measured, patch("signal.signal"):
                        self.assertEqual(owner.run(), 0)
                    calibrations = [row for row in commands if row[0].startswith("diagnostic-preflight-") and not row[0].endswith("inventory")]
                    self.assertEqual([row[1][1] for row in calibrations], list(self.CALIBRATORS))
                    for row in calibrations:
                        self.assertIn("--exact", row[1])
                        self.assertIsNone(row[2], "calibrator supervision retains its former routine policy")
                    workload = next(row for row in commands if row[0] == case + "-execution")
                    self.assertTrue(all(commands.index(row) < commands.index(workload) for row in calibrations))
                    self.assertEqual(workload[2], 2400)
                    self.assertEqual(measured.call_count, 1, "calibration stays outside measured workload commands")
                    self.assertEqual(measured.call_args.args[0][1], case)
                    self.assertEqual([row["purpose"] for row in owner.receipt["selections"]], ["diagnostic-preflight"] * 4 + ["workload"])

    def test_missing_calibration_fails_before_any_workload(self):
        for missing in self.CALIBRATORS:
            with self.subTest(missing=missing):
                owner = self.owner()
                full = {name for target, name, _ in AppWorkloads.CAPACITY if target == "workflow_execution"}
                full.update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH, *self.CALIBRATORS))
                full.remove(missing)
                commands = []
                def execute(label, arguments, _budget):
                    commands.append(label)
                    if label == "compiler":
                        return "release: 1.99.0\n"
                    if "inventory" in label:
                        selected = {arguments[1]} if "--exact" in arguments else full
                        return "\n".join(name + ": test" for name in selected)
                    return f"test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; {len(full) - 1} filtered out\n"
                with patch.object(owner, "build", return_value=[str(self.binary)]), patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "environment"), patch.object(owner, "preflight"), patch("signal.signal"):
                    self.assertEqual(owner.run(), 1)
                self.assertNotEqual(owner.receipt["status"], "PASS")
                self.assertFalse(any(label.endswith("-execution") for label in commands))

    def test_invalid_calibration_never_admits_measured_workload(self):
        for failure in ("exit", "empty", "ignored", "filtered", "wrong-selection"):
            with self.subTest(failure=failure):
                owner = self.owner()
                full = {name for target, name, _ in AppWorkloads.CAPACITY if target == "workflow_execution"}
                full.update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH, *self.CALIBRATORS))
                commands = []
                def execute(label, arguments, _budget):
                    commands.append(label)
                    if label == "compiler":
                        return "release: 1.99.0\n"
                    calibration = label.startswith("diagnostic-preflight-")
                    if "inventory" in label:
                        selected = {arguments[1]} if "--exact" in arguments else full
                        if calibration and failure == "wrong-selection":
                            selected = {AppWorkloads.DIAGNOSTIC}
                        return "\n".join(name + ": test" for name in selected)
                    if calibration:
                        if failure == "exit":
                            raise RuntimeError("calibration failed: exit=7")
                        if failure == "empty":
                            return ""
                        if failure == "ignored":
                            return f"test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; {len(full) - 1} filtered out\n"
                        if failure == "filtered":
                            return "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
                    return f"test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; {len(full) - 1} filtered out\n"
                with patch.object(owner, "build", return_value=[str(self.binary)]), patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value={"source": "same"}), patch.object(owner, "environment"), patch.object(owner, "preflight"), patch("signal.signal"):
                    self.assertEqual(owner.run(), 1)
                self.assertNotEqual(owner.receipt["status"], "PASS")
                self.assertFalse(any(label.endswith("-execution") for label in commands))
                if failure == "exit":
                    self.assertIn("exit=7", owner.receipt["error"]["message"])

    def test_resource_probe_failure_leaves_original_execution_unchanged(self):
        owner = self.owner()
        execution = [str(self.binary), "--exact", AppWorkloads.DIAGNOSTIC]
        failed = MODULE.subprocess.CompletedProcess(["time", "true"], 1, "", "restricted metric")
        with patch.object(MODULE.subprocess, "run", return_value=failed) as probe:
            self.assertEqual(owner.execution_command(execution), execution)
            self.assertEqual(owner.execution_command(execution), execution)
        self.assertEqual(probe.call_count, 1)
        self.assertNotIn(str(self.binary), probe.call_args.args[0])
        self.assertEqual(owner.receipt["execution_resources"]["probe_exit"], 1)

    def test_interruption_reaps_disposable_child(self):
        owner = self.owner()
        sleeping = MODULE.time.sleep
        def interrupt(seconds):
            owner.interrupted = 15
            sleeping(min(seconds, 0.01))
        with patch.object(MODULE.time, "sleep", side_effect=interrupt):
            with self.assertRaises(InterruptedError):
                owner.command("disposable-interrupt", [sys.executable, "-c", "import time; time.sleep(30)"], 5)
        self.assertIsNone(owner.child)
        self.assertEqual(owner.receipt["commands"][-1]["error"]["category"], "InterruptedError")
        self.assertFalse(owner.receipt["process_cleanup_errors"])

    def test_failed_command_keeps_primary_exit_when_cleanup_also_fails(self):
        owner = self.owner()
        def cleanup():
            process, owner.child = owner.child, None
            process.wait(timeout=5)
            owner.receipt["process_cleanup_errors"].append("injected cleanup failure")
            raise OSError("secondary cleanup failure")
        with patch.object(owner, "settle", side_effect=cleanup):
            with self.assertRaisesRegex(RuntimeError, "exit=7"):
                owner.command("disposable-failure", [sys.executable, "-c", "raise SystemExit(7)"], 5)
        self.assertEqual(owner.receipt["commands"][-1]["exit"], 7)
        self.assertEqual(owner.receipt["commands"][-1]["cleanup_error"], "OSError")

    def test_timeout_reaps_owned_child_and_preserves_timeout(self):
        owner = self.owner()
        with self.assertRaises(TimeoutError):
            owner.command("disposable-timeout", [sys.executable, "-c", "import time; time.sleep(30)"], 0.01)
        self.assertIsNone(owner.child)
        item = owner.receipt["commands"][-1]
        self.assertTrue(item["timed_out"])
        self.assertEqual(item["error"]["category"], "TimeoutError")
        self.assertFalse(owner.receipt["process_cleanup_errors"])

    def test_cpu_accounting_uses_direct_execution_and_excludes_compile(self):
        owner = self.owner()
        (owner.root / "app-sdk").mkdir()
        owner.receipt["artifacts"]["workflow_execution"] = {"executable": sys.executable}
        usage = [SimpleNamespace(ru_utime=100.0, ru_stime=20.0, ru_maxrss=999999),
                 SimpleNamespace(ru_utime=100.25, ru_stime=20.125, ru_maxrss=999999)]
        with patch("resource.getrusage", side_effect=usage) as measure:
            owner.command("compiler", [sys.executable, "-c", "print('compile substitute')"], 5)
            owner.command("prefix-selected-inventory", [sys.executable, "-c", "print('inventory substitute')"], 5)
            owner.command("prefix-execution", [sys.executable, "-c", "print('ordinary child')"], 5)
        self.assertEqual(measure.call_count, 2)
        cpu = owner.receipt["commands"][-1]["child_cpu"]
        self.assertEqual((cpu["user_seconds"], cpu["system_seconds"]), (0.25, 0.125))
        self.assertNotIn("maxrss", json.dumps(cpu))
        self.assertNotIn("child_cpu", owner.receipt["commands"][0])

    def test_cpu_unavailable_does_not_change_completed_child_result(self):
        owner = self.owner()
        (owner.root / "app-sdk").mkdir()
        owner.receipt["artifacts"]["workflow_execution"] = {"executable": sys.executable}
        with patch("resource.getrusage", side_effect=OSError("unavailable")):
            output = owner.command("prefix-execution", [sys.executable, "-c", "print('kept result')"], 5)
        self.assertIn("kept result", output)
        self.assertEqual(owner.receipt["commands"][-1]["exit"], 0)
        self.assertIn("unavailable", owner.receipt["commands"][-1]["child_cpu"])

    def test_retained_artifact_survives_original_binary_replacement(self):
        owner = AppWorkloads(self.root / "retain", "diagnostic", "capacity", retain_baseline=True)
        owner.receipt["source_before"] = {"candidate_sha256": "old", "lock_sha256": "lock"}
        with patch.object(owner, "command", return_value=self.build_output()), patch.object(owner, "runtime_identity", return_value={"fixture": "stable"}):
            original = owner.build("workflow_execution")
        artifact = owner.receipt["artifacts"]["workflow_execution"]
        self.assertNotEqual(original, [str(self.binary)])
        retained = Path(artifact["executable"])
        self.assertEqual(retained.read_bytes(), b"test artifact\n")
        self.binary.write_text("replacement\n")
        self.assertEqual(hashlib.sha256(retained.read_bytes()).hexdigest(), artifact["sha256"])
        self.assertEqual(owner.receipt["baseline_runtime"], {"fixture": "stable"})

    def baseline_receipt(self, compiler="release: 1.94.1\n"):
        artifact = json.loads(self.artifact())
        artifact = {"executable": str(self.binary), "sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                    "profile": artifact["profile"],
                    "runtime_profiles": {name: [dict(artifact["profile"], test=False)] for name in AppWorkloads.RUNTIME}}
        source = {"candidate_sha256": "old", "lock_sha256": "lock"}
        receipt = {"status": "PASS", "scope": "diagnostic", "profile": "capacity", "case": AppWorkloads.DIAGNOSTIC,
                   "source_before": source, "source_after": source, "retained_baseline": True,
                   "artifacts": {"workflow_execution": artifact}, "baseline_runtime": {"fixture": "stable"},
                   "compiler": compiler, "compiler_overrides": {},
                   "workflow_execution-full-inventory": [AppWorkloads.DIAGNOSTIC],
                   "process_cleanup_errors": [], "interrupted": None}
        path = self.root / "baseline.json"
        path.write_text(json.dumps(receipt))
        return path, receipt

    def test_frozen_baseline_keeps_old_source_and_never_builds_candidate(self):
        path, saved = self.baseline_receipt()
        owner = AppWorkloads(self.root / "reuse", "diagnostic", "capacity", baseline_receipt=path)
        owner.receipt.update(source_before={"candidate_sha256": "new", "lock_sha256": "lock"},
                             compiler=saved["compiler"], compiler_overrides={})
        with patch.object(owner, "runtime_identity", return_value={"fixture": "stable"}), patch.object(owner, "command") as command:
            self.assertEqual(owner.build("workflow_execution"), [str(self.binary)])
        command.assert_not_called()
        self.assertEqual(owner.receipt["executable_source"], saved["source_before"])
        self.assertEqual(owner.receipt["source_before"]["candidate_sha256"], "new")

    def test_frozen_baseline_rejects_version_hint_policy_change(self):
        path, saved = self.baseline_receipt()
        owner = AppWorkloads(self.root / "hint-reuse", "diagnostic", "capacity", baseline_receipt=path)
        owner.receipt.update(source_before={"candidate_sha256": "new", "lock_sha256": "lock"},
                             compiler=saved["compiler"], compiler_overrides={})
        for name in ("app-sdk/tests/workflow_execution.rs", "app-sdk/tests/observability_support/mod.rs"):
            fixture = owner.root / name
            fixture.parent.mkdir(parents=True, exist_ok=True)
            fixture.write_text("fixture\n")
        library = owner.root / "library.dylib"
        library.write_text("same library")
        output = MODULE.subprocess.CompletedProcess([], 0, "binary:\n\t" + str(library) + " (compatibility version 1.0.0)\n", "")
        with patch.dict(os.environ, {}, clear=True), patch.object(MODULE.subprocess, "run", return_value=output), patch.object(MODULE.subprocess, "check_output", return_value="build\n"):
            saved["baseline_runtime"] = owner.runtime_identity(self.binary)
            path.write_text(json.dumps(saved))
            self.assertEqual(owner.build("workflow_execution"), [str(self.binary)])
            with patch.dict(os.environ, {"LANCE_USE_VERSION_HINT": "0"}):
                changed = owner.runtime_identity(self.binary)
                self.assertEqual(changed["runtime_environment"]["LANCE_USE_VERSION_HINT"], hashlib.sha256(b"0").hexdigest())
                with self.assertRaisesRegex(ValueError, "runtime identity differs"):
                    owner.build("workflow_execution")
            self.assertEqual(owner.runtime_identity(self.binary), saved["baseline_runtime"])

    def test_frozen_baseline_rejects_drift_failed_source_and_bad_profile(self):
        for change in ("bytes", "lock", "runtime", "unstable", "failed", "profile", "compiler"):
            with self.subTest(change=change):
                path, saved = self.baseline_receipt()
                if change == "bytes":
                    saved["artifacts"]["workflow_execution"]["sha256"] = "incorrect"
                if change == "unstable":
                    saved["source_after"] = {"candidate_sha256": "changed"}
                if change == "failed":
                    saved["status"] = "FAIL"
                if change == "profile":
                    saved["artifacts"]["workflow_execution"]["runtime_profiles"]["lance"][0]["overflow_checks"] = False
                path.write_text(json.dumps(saved))
                owner = AppWorkloads(self.root / ("reuse-" + change), "diagnostic", "capacity", baseline_receipt=path)
                owner.receipt.update(source_before={"candidate_sha256": "new", "lock_sha256": "different" if change == "lock" else "lock"},
                                     compiler="release: 1.99.0\n" if change == "compiler" else saved["compiler"],
                                     compiler_overrides={})
                with patch.object(owner, "runtime_identity", return_value={"fixture": "changed" if change == "runtime" else "stable"}):
                    with self.assertRaises(ValueError):
                        owner.build("workflow_execution")

    def test_frozen_execution_preserves_inventory_and_source_attribution(self):
        for altered_inventory in (False, True):
            with self.subTest(altered_inventory=altered_inventory):
                # This synthetic retained artifact uses the selected current compiler;
                # other baseline controls retain their historical 1.94.1 identity.
                path, saved = self.baseline_receipt(compiler="release: 1.99.0\n")
                full = {name for target, name, _ in AppWorkloads.CAPACITY if target == "workflow_execution"}
                full.update((AppWorkloads.ROUTINE, AppWorkloads.DIAGNOSTIC, AppWorkloads.GROWTH))
                full.update(self.CALIBRATORS)
                saved["workflow_execution-full-inventory"] = sorted(full)
                path.write_text(json.dumps(saved))
                owner = AppWorkloads(self.root / str(altered_inventory), "diagnostic", "capacity", baseline_receipt=path)
                owner.receipt["compiler_overrides"] = {}
                commands = []
                def execute(label, arguments, budget):
                    commands.append(label)
                    self.assertNotIn("cargo", arguments)
                    if label == "compiler":
                        return saved["compiler"]
                    if "inventory" in label:
                        selected = {arguments[1]} if "--exact" in arguments else full | ({"unexpected"} if altered_inventory else set())
                        return "\n".join(name + ": test" for name in selected)
                    return f"test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; {len(full) - 1} filtered out\n"
                current = {"candidate_sha256": "new", "lock_sha256": "lock"}
                with patch.object(owner, "command", side_effect=execute), patch.object(owner, "identity", return_value=current), patch.object(owner, "runtime_identity", return_value={"fixture": "stable"}), patch.object(owner, "preflight"), patch.object(owner, "environment"), patch("signal.signal"):
                    self.assertEqual(owner.run(), int(altered_inventory))
                self.assertEqual(owner.receipt["executable_source"]["candidate_sha256"], "old")
                self.assertEqual(owner.receipt["source_after"], current)
                self.assertIn("not current-source", owner.receipt["qualification_scope"])
                calibrations = [label for label in commands if label.startswith("diagnostic-preflight-") and label.endswith("-check")]
                self.assertEqual(len(calibrations), 0 if altered_inventory else 4)

    def test_frozen_binary_is_revalidated_before_command_admission(self):
        path, saved = self.baseline_receipt()
        owner = AppWorkloads(self.root / "modified", "diagnostic", "capacity", baseline_receipt=path)
        owner.receipt["artifacts"] = saved["artifacts"]
        self.binary.write_text("changed after inventory")
        with patch.object(MODULE.subprocess, "Popen") as spawn:
            with self.assertRaisesRegex(ValueError, "executable changed"):
                owner.command("prefix-execution", [str(self.binary)], 5)
        spawn.assert_not_called()

    def test_frozen_artifacts_are_diagnostic_only(self):
        for scope in ("routine", "capacity"):
            with self.assertRaises(ValueError):
                AppWorkloads(self.root / scope, scope, retain_baseline=True)
        with self.assertRaises(ValueError):
            AppWorkloads(self.root / "both", "diagnostic", retain_baseline=True, baseline_receipt=self.root / "old.json")

    def test_cpu_end_failure_and_cleanup_failure_are_explicit(self):
        for failure in ("measurement", "cleanup"):
            with self.subTest(failure=failure):
                owner = self.owner()
                (owner.root / "app-sdk").mkdir()
                owner.receipt["artifacts"]["workflow_execution"] = {"executable": sys.executable}
                before = SimpleNamespace(ru_utime=10.0, ru_stime=1.0)
                with patch("resource.getrusage", side_effect=[before, OSError("late unavailable")]):
                    if failure == "measurement":
                        owner.command("prefix-execution", [sys.executable, "-c", "pass"], 5)
                    else:
                        settle = owner.settle
                        def failed_settlement():
                            settle()
                            raise OSError("joined but secondary settlement failure")
                        with patch.object(owner, "settle", side_effect=failed_settlement):
                            with self.assertRaisesRegex(RuntimeError, "exit=7"):
                                owner.command("prefix-execution", [sys.executable, "-c", "raise SystemExit(7)"], 5)
                self.assertIn("unavailable", owner.receipt["commands"][-1]["child_cpu"])

    def test_cpu_does_not_attribute_an_execution_wrapper_to_the_test(self):
        owner = self.owner()
        (owner.root / "app-sdk").mkdir()
        owner.receipt["artifacts"]["workflow_execution"] = {"executable": str(self.binary)}
        with patch("resource.getrusage") as measure:
            owner.command("prefix-execution", [sys.executable, "-c", "pass", str(self.binary)], 5)
        measure.assert_not_called()
        self.assertIn("unavailable", owner.receipt["commands"][-1]["child_cpu"])

    def test_runtime_identity_rejects_loader_and_fixture_drift(self):
        owner = self.owner()
        for name in ("app-sdk/tests/workflow_execution.rs", "app-sdk/tests/observability_support/mod.rs"):
            path = owner.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n")
        library = owner.root / "library.dylib"
        library.write_text("library one")
        output = MODULE.subprocess.CompletedProcess([], 0, "binary:\n\t" + str(library) + " (compatibility version 1.0.0)\n", "")
        with patch.dict(os.environ, {}, clear=True), patch.object(MODULE.subprocess, "run", return_value=output), patch.object(MODULE.subprocess, "check_output", return_value="build\n"):
            first = owner.runtime_identity(self.binary)
            library.write_text("library two")
            self.assertNotEqual(first, owner.runtime_identity(self.binary))
            library.write_text("library one")
            (owner.root / "app-sdk/tests/workflow_execution.rs").write_text("changed fixture")
            self.assertNotEqual(first, owner.runtime_identity(self.binary))
        with patch.dict(os.environ, {"LD_PRELOAD": "unqualified"}, clear=True):
            with self.assertRaisesRegex(ValueError, "loader"):
                owner.runtime_identity(self.binary)


if __name__ == "__main__":
    unittest.main()
