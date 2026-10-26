"""Release runner orchestration only; command doubles are not SDK evidence."""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class ReleaseStageHooksTests(unittest.TestCase):
    APP_TARGETS = (
        "public_surface mock_contracts workflow_mock_composition core_contract "
        "action_bindings action_contract projection_contract callable_definition_contract "
        "condition_contract condition_display_contract lifecycle_contract hook_execution_contract "
        "observability_contract tracking_contract tracking_checkpoint_contract checkpoint_history "
        "deletion_contract revision_contract child_executor_contract child_composition "
        "lance_conflict_control execution_controls bsp_execution_contract native_execution_contract "
        "outcome_recovery_contract native_streaming_contract typing_integration expression_parity "
        "graph_contract schema_contract docs_examples state_contract typed_state_contract "
        "state_serialization_contract workspace_contract"
    ).split()
    PROVIDER_TARGETS = (
        "compile_fail public_api provider_backend mock_contracts shutdown_lifecycle session_lifecycle "
        "provider_behavior wire_behavior error_contract transport_diagnostics strict_live_assertions "
        "strict_env_contract source_contract"
    ).split()
    TOOL_TARGETS = (
        "compile_fail public_api extension_backend mock_contracts shared_shutdown strict_env_contract "
        "source_contract local_execution schema_validation mcp_stdio mcp_streamable_http cancellation "
        "handler_step_flow mcp_form_interaction mcp_server_feature"
    ).split()
    PREPARE_CASES = [
        "prepare_keeps_checkpoint_zero_parked_through_observers_and_cancellation",
        "prepare_rejects_a_stale_workspace_before_allocating_a_visible_run",
        "prepared_checkpoint_zero_survives_process_loss",
        "staged_action_and_atomic_barrier_survive_forced_process_loss",
    ]

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = {
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "TRACE": str(self.root / "trace.jsonl"),
            "FIXTURE_ROOT": str(self.root),
            "PREPARE_CASES": json.dumps(self.PREPARE_CASES),
            "AI_KIT_WORKSPACE_OWNS_DOCS_SITE": str(self.root),
        }
        self.copy("scripts/release_stage.sh")
        self.executable("cargo", r'''
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['TRACE'], 'a') as stream:
    stream.write(json.dumps({'command': 'cargo', 'args': args}) + '\n')
if os.environ.get('CARGO_FAILURE'):
    raise SystemExit(int(os.environ['CARGO_FAILURE']))
if '--features' in args and {'sqlite', 'qdrant'} <= set(args[args.index('--features') + 1].split(',')):
    mode = os.environ.get('NEGATIVE_FAILURE_MODE')
    if mode == 'success': raise SystemExit(0)
    print('unrelated compiler error' if mode == 'unrelated' else 'error: sqlite and qdrant cannot coexist')
    raise SystemExit(101)
if '--test' in args and args[args.index('--test') + 1] == 'prepare_contract':
    cases = json.loads(os.environ['PREPARE_CASES'])
    if '--list' in args:
        for case in cases: print(case + ': test')
    else:
        for case in cases: print('test ' + case + ' ... ok')
        ignored = int(os.environ.get('PREPARE_IGNORED', '0'))
        print(f'test result: ok. {len(cases) - ignored} passed; 0 failed; {ignored} ignored; 0 measured; 0 filtered out')
elif args and args[0] == 'doc':
    crate = args[args.index('-p') + 1].replace('-', '_')
    if '--target-dir' in args:
        target = pathlib.Path(args[args.index('--target-dir') + 1])
    else:
        target = pathlib.Path(os.environ.get('CARGO_TARGET_DIR', 'target'))
    destination = target / 'doc' / crate / 'index.html'
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text('fixture generated page')
''')
        self.executable("python3", f'''
import json, os, sys
args = sys.argv[1:]
if (args[:2] == ['-m', 'unittest'] or args == ['app-sdk/scripts/check_app_workloads.py', 'routine']
        or args in [['agent-sdk/tests/precheck_contract.py'], ['agent-sdk/tests/question_grade_contract.py']]):
    with open(os.environ['TRACE'], 'a') as stream:
        stream.write(json.dumps({{'command': 'python3', 'args': args}}) + '\\n')
    if args[:2] == ['-m', 'unittest'] and os.environ.get('RUNNER_CONTRACT_EXIT'):
        raise SystemExit(int(os.environ['RUNNER_CONTRACT_EXIT']))
else:
    os.execv({sys.executable!r}, [{sys.executable!r}] + args)
''')
        self.write("scripts/release_evidence.py", r'''
import json, os, subprocess, sys
args = sys.argv[1:]
assert args[0] == 'stage' and args[2] == '--'
with open(os.environ['TRACE'], 'a') as stream:
    stream.write(json.dumps({'stage': args[1], 'args': args[3:],
                            'fresh': os.environ.get('AI_KIT_RELEASE_STAGE_FRESH')}) + '\n')
if os.environ.get('REUSE_ALL_NONFRESH') and os.environ.get('AI_KIT_RELEASE_STAGE_FRESH') != '1':
    raise SystemExit(0)
raise SystemExit(subprocess.call(args[3:]))
''')

    def copy(self, name):
        destination = self.root / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def executable(self, name, text):
        path = self.write("bin/" + name, f"#!{sys.executable}\n" + text)
        path.chmod(0o700)

    def run_script(self, name, *args, active=False):
        env = dict(self.env)
        if active:
            (self.root / "receipt-run").mkdir(exist_ok=True)
            env["AI_KIT_RELEASE_RECEIPT_RUN"] = str(self.root / "receipt-run")
        return subprocess.run(["bash", str(self.root / name), *args], cwd=self.root,
                              env=env, text=True, capture_output=True, timeout=30)

    def observations(self):
        path = Path(self.env["TRACE"])
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_inactive_helper_preserves_argv_output_and_failure(self):
        self.executable("observe", r'''
import json, sys
print(json.dumps(sys.argv[1:]))
print('child stderr', file=sys.stderr)
raise SystemExit(29)
''')
        args = ["value with spaces", "literal;$()", "", "line\nbreak", "--flag"]
        result = self.run_script("scripts/release_stage.sh", "fixture.command", "observe", *args)
        self.assertEqual(result.returncode, 29)
        self.assertEqual(json.loads(result.stdout), args)
        self.assertEqual(result.stderr, "child stderr\n")
        self.assertEqual(self.observations(), [])

    def test_explicit_run_passes_exact_stage_and_argv(self):
        result = self.run_script("scripts/release_stage.sh", "fixture.check", "cargo", "test", "--locked",
                                 "literal value", active=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.observations(), [
            {"stage": "fixture.check", "args": ["cargo", "test", "--locked", "literal value"], "fresh": None},
            {"command": "cargo", "args": ["test", "--locked", "literal value"]},
        ])

    def test_fresh_flag_is_scoped_and_does_not_leak(self):
        self.write("fixture.sh", '''source scripts/release_stage.sh
release_stage_fresh fixture.first cargo test
release_stage fixture.second cargo test
''')
        result = self.run_script("fixture.sh", active=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        stages = [row for row in self.observations() if "stage" in row]
        self.assertEqual([row["fresh"] for row in stages], ["1", None])

    def test_empty_activation_and_malformed_stage_fail_before_dispatch(self):
        cases = [([], {}), (["--fresh"], {}), (["bad/id", "cargo"], {}),
                 (["fixture.check", "cargo"], {"AI_KIT_RELEASE_RECEIPT_RUN": ""})]
        for args, extra in cases:
            with self.subTest(args=args, extra=extra):
                self.env.update(extra)
                result = self.run_script("scripts/release_stage.sh", *args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(self.observations(), [])
                self.env.pop("AI_KIT_RELEASE_RECEIPT_RUN", None)

    def test_active_engine_failure_is_not_a_silent_fallback(self):
        self.write("scripts/release_evidence.py", "raise SystemExit(37)\n")
        result = self.run_script("scripts/release_stage.sh", "fixture.check", "cargo", active=True)
        self.assertEqual(result.returncode, 37)
        self.assertEqual(self.observations(), [])

    def test_provider_and_tool_keep_complete_default_target_inventories(self):
        for crate, targets in [("provider", self.PROVIDER_TARGETS), ("tool", self.TOOL_TARGETS)]:
            with self.subTest(crate=crate):
                script = f"{crate}-sdk/scripts/check.sh"
                self.copy(script)
                result = self.run_script(script, active=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                calls = [row["args"] for row in self.observations() if row.get("command") == "cargo"]
                tests = [args for args in calls if "--test" in args]
                self.assertEqual([args[args.index("--test") + 1] for args in tests], targets)
                self.assertTrue(all("--all-features" in args and "--locked" in args for args in tests))
                self.assertTrue(all(args[-2:] == ["--", "--nocapture"] for args in tests))
                self.assertFalse(any("strict_live" in args for args in tests))
                Path(self.env["TRACE"]).unlink()

    def prepare_app(self):
        self.copy("app-sdk/scripts/check.sh")
        self.write("scripts/check_no_type_aliases.sh", "exit 0\n")
        self.write("scripts/check_no_path_wiring.sh", "exit 0\n")

    def test_app_targets_and_four_case_prepare_remain_required_and_fresh(self):
        self.prepare_app()
        result = self.run_script("app-sdk/scripts/check.sh", active=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        rows = self.observations()
        calls = [row["args"] for row in rows if row.get("command") == "cargo"]
        tests = [args for args in calls if "--test" in args]
        self.assertEqual([args[args.index("--test") + 1] for args in tests],
                         self.APP_TARGETS + ["prepare_contract", "prepare_contract"])
        self.assertTrue(all(args[-3:] == ["--", "--nocapture", "--test-threads=1"] for args in tests[:-2]))
        stages = [row for row in rows if row.get("stage", "").startswith("app.prepare.")]
        self.assertEqual([row["stage"] for row in stages], ["app.prepare.inventory", "app.prepare.execution"])
        self.assertEqual([row["fresh"] for row in stages], ["1", "1"])
        examples = [args[args.index("--example") + 1] for args in calls if "--example" in args]
        self.assertEqual(examples, "state graph pure_state actions_and_inputs controls_and_streaming history_and_recovery child_composition".split())
        workloads = [row for row in rows if row.get("command") == "python3"]
        self.assertEqual(workloads, [{"command": "python3", "args": ["app-sdk/scripts/check_app_workloads.py", "routine"]}])

    def test_app_missing_prepare_case_and_ignored_execution_fail(self):
        self.prepare_app()
        for key, value, message in [
            ("PREPARE_CASES", json.dumps(self.PREPARE_CASES[:-1]), "inventory differs"),
            ("PREPARE_IGNORED", "1", "did not pass all four"),
        ]:
            with self.subTest(key=key):
                old = self.env.get(key)
                self.env[key] = value
                result = self.run_script("app-sdk/scripts/check.sh", active=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertFalse(any(row.get("stage") == "app.workloads.routine" for row in self.observations()))
                Path(self.env["TRACE"]).unlink()
                if old is None:
                    del self.env[key]
                else:
                    self.env[key] = old

    def test_storage_default_and_explicit_feature_lanes_preserve_flags(self):
        self.copy("storage-sdk/scripts/check.sh")
        self.copy("storage-sdk/scripts/feature_policy.py")
        for arguments, expected_features in [((), None), (("--backend", "qdrant"), "qdrant")]:
            with self.subTest(arguments=arguments):
                result = self.run_script("storage-sdk/scripts/check.sh", *arguments, active=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                calls = [row["args"] for row in self.observations() if row.get("command") == "cargo"]
                self.assertFalse(any("--all-features" in args for args in calls))
                tests = [args for args in calls if "--test" in args]
                expected_targets = ("feature_contract public_api database_backend state_store_backend mock_contracts "
                                    "lancedb_native_feasibility lancedb_durability_feasibility lancedb_session_ownership "
                                    "lancedb_state_store strict_env_contract source_contract").split()
                if expected_features:
                    expected_targets = expected_targets + expected_targets + ["qdrant_rest_completion"]
                self.assertEqual([args[args.index("--test") + 1] for args in tests], expected_targets)
                for index, args in enumerate(tests):
                    self.assertIn("--no-default-features", args)
                    self.assertEqual(args[-3:], ["--", "--nocapture", "--test-threads=1"])
                    if expected_features and index >= 11:
                        self.assertEqual(args[args.index("--features") + 1], expected_features)
                    else:
                        self.assertNotIn("--features", args)
                Path(self.env["TRACE"]).unlink()

    def test_storage_negative_matrix_retains_all_eight_matching_failures(self):
        self.copy("storage-sdk/scripts/check.sh")
        self.copy("storage-sdk/scripts/feature_policy.py")
        self.env["REUSE_ALL_NONFRESH"] = "1"
        result = self.run_script("storage-sdk/scripts/check.sh", "--all-backends", active=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipts = list((self.root / "receipt-run").glob("storage-negative-features.*/*.json"))
        self.assertEqual(len(receipts), 8)
        for path in receipts:
            row = json.loads(path.read_text())
            self.assertEqual(row["status"], "PASS")
            self.assertEqual(row["returncode"], 101)
            self.assertIn("cannot coexist", row["expected_diagnostic"])
            self.assertTrue(Path(row["log"]["path"]).is_file())
            self.assertEqual(row["command"][-2:], ["--features", row["features"]])
        self.assertEqual(result.stdout.count("STORAGE_NEGATIVE_FEATURE_GRADE="), 8)

    def test_storage_negative_matrix_rejects_success_and_unrelated_failure(self):
        self.copy("storage-sdk/scripts/check.sh")
        self.copy("storage-sdk/scripts/feature_policy.py")
        self.env["REUSE_ALL_NONFRESH"] = "1"
        for mode, message in [("success", "unexpectedly compiled"), ("unrelated", "unrelated reason")]:
            with self.subTest(mode=mode):
                self.env["NEGATIVE_FAILURE_MODE"] = mode
                result = self.run_script("storage-sdk/scripts/check.sh", "--all-backends", active=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                receipts = list((self.root / "receipt-run").glob("storage-negative-features.*/*.json"))
                self.assertTrue(receipts)
                self.assertTrue(all(json.loads(path.read_text())["status"] == "FAIL" for path in receipts))

    def test_docs_keep_all_build_and_doctest_lanes(self):
        self.copy("scripts/check_docs_site.sh")
        self.write("docs/rustdoc-index.html", "fixture landing")
        self.write("docs/assets/ai-kit-logo.png", "fixture asset")
        result = self.run_script("scripts/check_docs_site.sh", active=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = [row["args"] for row in self.observations() if row.get("command") == "cargo"]
        builds = [args for args in calls if args[0] == "doc"]
        doctests = [args for args in calls if "--doc" in args]
        self.assertEqual(len(builds), 8)
        self.assertEqual(len(doctests), 7)
        build_stages = [row for row in self.observations() if ".rustdoc" in row.get("stage", "")]
        self.assertTrue(all(row["fresh"] == "1" for row in build_stages))
        self.assertEqual([args[args.index("-p") + 1] for args in builds],
                         ["provider-sdk", "tool-sdk", "storage-sdk", "storage-sdk", "storage-sdk", "storage-sdk", "agent-sdk", "app-sdk"])
        storage_lanes = [args[args.index("--features") + 1] for args in builds if "--features" in args]
        self.assertEqual(storage_lanes, ["redb,postgres,surrealdb,sqlite", "redb,postgres,surrealdb,qdrant"])
        self.assertTrue((self.root / "target/doc/app_sdk/index.html").is_file())

    def test_docs_artifact_builds_and_default_restore_execute_when_other_stages_reuse(self):
        self.copy("scripts/check_docs_site.sh")
        self.write("docs/rustdoc-index.html", "fixture landing")
        self.write("docs/assets/ai-kit-logo.png", "fixture asset")
        self.env["REUSE_ALL_NONFRESH"] = "1"
        result = self.run_script("scripts/check_docs_site.sh", active=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = [row["args"] for row in self.observations() if row.get("command") == "cargo"]
        self.assertEqual(len(calls), 8)
        self.assertTrue(all(args[0] == "doc" for args in calls))
        storage = [args for args in calls if args[args.index("-p") + 1] == "storage-sdk"]
        self.assertEqual(len(storage), 4)
        self.assertNotIn("--features", storage[-1])
        self.assertTrue((self.root / "target/doc/storage_sdk/index.html").is_file())

    def test_wrapper_resume_is_explicit_and_malformed_arguments_never_dispatch(self):
        self.copy("scripts/verify_workspace_strict.sh")
        self.write("scripts/release_qualification.py", "import json, sys\nprint(json.dumps(sys.argv[1:]))\n")
        valid = self.run_script("scripts/verify_workspace_strict.sh", "--resume", "bundle with spaces")
        self.assertEqual(valid.returncode, 0, valid.stderr)
        self.assertEqual(json.loads(valid.stdout), ["--resume", "bundle with spaces"])
        qualified = self.run_script("scripts/verify_workspace_strict.sh", "--resume", "bundle with spaces", "--skip-strict-live")
        self.assertEqual(qualified.returncode, 0, qualified.stderr)
        self.assertEqual(json.loads(qualified.stdout), ["--resume", "bundle with spaces", "--skip-strict-live"])
        for args in [("--resume",), ("--resume", ""), ("--resume", "--skip-long-provider-batches"),
                     ("--resume", "bundle", "extra"), ("unknown",)]:
            with self.subTest(args=args):
                result = self.run_script("scripts/verify_workspace_strict.sh", *args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(self.observations(), [])

    def test_qualified_nonlive_wrapper_dispatches_all_local_owners_without_env_loading(self):
        self.copy("scripts/verify_workspace_strict.sh")
        self.copy("storage-sdk/scripts/feature_policy.py")
        for name in ("check_no_path_wiring.sh", "check_no_type_aliases.sh", "check_docs_site.sh"):
            self.write("scripts/" + name, "exit 0\n")
        for crate in ("agent", "provider", "storage", "tool", "app"):
            self.write(f"{crate}-sdk/strict-tests.env", 'printf loaded >> "${FIXTURE_ROOT}/env-loaded"\nexit 93\n')
            self.write(f"{crate}-sdk/scripts/check.sh", f'printf "{crate} check\\n" >> "${{FIXTURE_ROOT}}/owners"\n')
            self.write(f"{crate}-sdk/scripts/verify_strict_live.sh",
                       ('[[ "$#" == 1 && "$1" == --skip-strict-live ]] || exit 94\n'
                        'printf "agent verify --skip-strict-live\\n" >> "${FIXTURE_ROOT}/owners"\n'
                        if crate == "agent" else 'printf launched >> "${FIXTURE_ROOT}/live-launched"\nexit 94\n'))
        for active in (False, True):
            with self.subTest(active=active):
                result = self.run_script("scripts/verify_workspace_strict.sh", "--skip-strict-live", active=active)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertCountEqual((self.root / "owners").read_text().splitlines(),
                                      ["agent verify --skip-strict-live", "provider check", "storage check", "tool check", "app check"])
                self.assertFalse((self.root / "env-loaded").exists())
                self.assertFalse((self.root / "live-launched").exists())
                self.assertIn("QUALIFIED coverage; all strict-live suites", result.stdout)
                self.assertIn("release qualification remains unproven", result.stdout)
                for suite in ("agent-sdk/strict_live", "provider-sdk/strict_live", "storage-sdk/strict_live",
                              "tool-sdk/strict_live", "app-sdk/workflow_live"):
                    self.assertIn(suite, result.stdout)
                self.assertNotIn("FULL crate coverage", result.stdout)
                (self.root / "owners").unlink()

    def test_agent_qualified_nonlive_mode_retains_all_local_targets_and_process_contracts(self):
        self.copy("agent-sdk/scripts/verify_strict_live.sh")
        self.write("agent-sdk/scripts/precheck_strict_live.sh", 'printf launched >> "${FIXTURE_ROOT}/live-launched"\nexit 94\n')
        self.write("agent-sdk/scripts/report_question_grades.py", "raise SystemExit('live reporter must not run')\n")
        result = self.run_script("agent-sdk/scripts/verify_strict_live.sh", "--skip-strict-live", active=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.root / "live-launched").exists())
        self.assertFalse((self.root / "target/agent-sdk-strict-live").exists())
        rows = self.observations()
        cargo = [row["args"] for row in rows if row.get("command") == "cargo"]
        targets = [args[args.index("--test") + 1] for args in cargo if "--test" in args]
        self.assertEqual(targets, ("source_contract final_audit public_api compile_fail agent_backend mock_contracts "
                                  "custom_mock_composition notification_stream error_contract prompt_stream builder_failure_cleanup "
                                  "native_cleanup_contract strict_live_assertions custom_prompt_runtime codex_prompt_stream opencode_event_ownership").split())
        self.assertTrue(any("--lib" in args for args in cargo))
        self.assertTrue(any("--doc" in args for args in cargo))
        self.assertTrue(any(args[0] == "doc" for args in cargo))
        self.assertTrue(any(args[0] == "clippy" for args in cargo))
        self.assertTrue(any(args[0] == "fmt" for args in cargo))
        python = [row["args"] for row in rows if row.get("command") == "python3"]
        self.assertEqual(python[:2], [["agent-sdk/tests/precheck_contract.py"], ["agent-sdk/tests/question_grade_contract.py"]])
        self.assertEqual(python[2], ["-m", "unittest", "-v", "scripts.tests.test_vendor_update.AgentSdkProtocolAuthorityTests"])
        self.assertIn("qualified non-live checks passed", result.stdout)
        for args in [("--unknown",), ("--skip-strict-live", "extra")]:
            invalid = self.run_script("agent-sdk/scripts/verify_strict_live.sh", *args)
            self.assertEqual(invalid.returncode, 2)

    def test_runner_bootstrap_failure_stops_before_cargo_and_owners_even_when_resuming(self):
        self.copy("scripts/verify_workspace_strict.sh")
        self.env["RUNNER_CONTRACT_EXIT"] = "47"
        for crate in ("agent", "provider", "storage", "tool", "app"):
            self.write(f"{crate}-sdk/strict-tests.env", "export PROVIDER_SDK_RUN_LONG_BATCHES=1\n")
            self.write(f"{crate}-sdk/scripts/verify_strict_live.sh",
                       f"printf '{crate}\\n' >> \"${{FIXTURE_ROOT}}/owners\"\n")
        for active in (False, True):
            with self.subTest(active=active):
                result = self.run_script("scripts/verify_workspace_strict.sh", active=active)
                self.assertEqual(result.returncode, 47, result.stdout + result.stderr)
                self.assertFalse((self.root / "owners").exists())
                rows = self.observations()
                self.assertFalse(any(row.get("command") == "cargo" for row in rows))
                self.assertNotIn("Workspace strict completion passed", result.stdout)
                self.assertEqual([row["args"] for row in rows if row.get("command") == "python3"], [[
                    "-m", "unittest", "scripts.tests.test_release_stage_hooks", "scripts.tests.test_release_evidence",
                    "scripts.tests.test_release_qualification", "scripts.tests.test_import_release_evidence",
                ]])
                if active:
                    stages = [row for row in rows if "stage" in row]
                    self.assertEqual([row["stage"] for row in stages], ["workspace.contract.release_runner"])
                    self.assertIsNone(stages[0]["fresh"])
                Path(self.env["TRACE"]).unlink()

    def test_wrapper_full_default_still_dispatches_every_owner_and_storage_clippy_lane(self):
        self.copy("scripts/verify_workspace_strict.sh")
        self.copy("storage-sdk/scripts/feature_policy.py")
        for name in ("check_no_path_wiring.sh", "check_no_type_aliases.sh", "check_docs_site.sh"):
            self.write("scripts/" + name, "exit 0\n")
        for crate in ("agent", "provider", "storage", "tool", "app"):
            self.write(f"{crate}-sdk/strict-tests.env", "export PROVIDER_SDK_RUN_LONG_BATCHES=1\n")
            self.write(f"{crate}-sdk/scripts/verify_strict_live.sh",
                       f"printf '{crate}\\n' >> \"${{FIXTURE_ROOT}}/owners\"\n")
        result = self.run_script("scripts/verify_workspace_strict.sh", active=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual((self.root / "owners").read_text().splitlines(),
                         ["agent", "storage", "tool", "app", "provider"])
        self.assertIn("FULL crate coverage", result.stdout)
        calls = [row["args"] for row in self.observations() if row.get("command") == "cargo"]
        self.assertEqual(calls[0], ["fmt", "--all", "--check"])
        self.assertEqual(calls[1], ["clippy", "--locked", "--workspace", "--all-targets", "--all-features",
                                   "--exclude", "storage-sdk", "--exclude", "app-sdk", "--", "-D", "warnings"])
        storage = calls[2:]
        self.assertEqual(len(storage), 8)
        self.assertTrue(all("--no-default-features" in args and "--all-features" not in args for args in storage))
        self.assertEqual([args[args.index("--features") + 1] if "--features" in args else None for args in storage],
                         [None, "redb", "postgres", "surrealdb", "sqlite", "qdrant",
                          "redb,postgres,surrealdb,sqlite", "redb,postgres,surrealdb,qdrant"])

    def test_resume_owner_failure_continues_independent_owners_but_default_stops(self):
        self.copy("scripts/verify_workspace_strict.sh")
        self.copy("storage-sdk/scripts/feature_policy.py")
        for name in ("check_no_path_wiring.sh", "check_no_type_aliases.sh", "check_docs_site.sh"):
            self.write("scripts/" + name, "exit 0\n")
        for crate in ("agent", "provider", "storage", "tool", "app"):
            self.write(f"{crate}-sdk/strict-tests.env", "export PROVIDER_SDK_RUN_LONG_BATCHES=1\n")
            self.write(f"{crate}-sdk/scripts/verify_strict_live.sh",
                       f"printf '{crate}\\n' >> \"${{FIXTURE_ROOT}}/owners\"\n" +
                       ("exit 43\n" if crate == "agent" else "exit 0\n"))
        for active, expected in [(False, ["agent"]), (True, ["agent", "storage", "tool", "app", "provider"])]:
            with self.subTest(active=active):
                result = self.run_script("scripts/verify_workspace_strict.sh", active=active)
                self.assertEqual(result.returncode, 43, result.stdout + result.stderr)
                self.assertEqual((self.root / "owners").read_text().splitlines(), expected)
                self.assertNotIn("Workspace strict completion passed", result.stdout)
                (self.root / "owners").unlink()

    def test_compiler_failure_stops_owner_without_a_pass(self):
        self.copy("provider-sdk/scripts/check.sh")
        self.env["CARGO_FAILURE"] = "41"
        result = self.run_script("provider-sdk/scripts/check.sh", active=True)
        self.assertEqual(result.returncode, 41)
        calls = [row for row in self.observations() if row.get("command") == "cargo"]
        self.assertEqual(calls, [{"command": "cargo", "args": ["fmt", "--all", "--check"]}])

    def test_every_ordinary_cargo_leaf_is_receipt_aware(self):
        paths = ["scripts/verify_workspace_strict.sh", "scripts/check_docs_site.sh",
                 "agent-sdk/scripts/verify_strict_live.sh"]
        paths += [f"{crate}-sdk/scripts/{script}.sh" for crate in ("provider", "storage", "tool", "app")
                  for script in ("check", "verify_strict_live")]
        for name in paths:
            with self.subTest(script=name):
                source = (ROOT / name).read_text()
                for line in source.splitlines():
                    self.assertIsNone(re.match(r"\s*(?:cargo |env [^\n]* cargo |RUSTDOCFLAGS=.* cargo )", line), line)
                self.assertNotIn("release_stage ", "\n".join(line for line in source.splitlines()
                                    if re.search(r"bash (?:[\w-]+/)?scripts/(?:check|verify_strict_live|check_docs_site)\.sh", line)))


if __name__ == "__main__":
    unittest.main()
