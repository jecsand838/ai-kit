"""Orchestration contracts; command doubles never qualify backend behavior."""
import importlib.util
import copy
import pathlib
import os
import shutil
import signal
import time
import subprocess
import tempfile
import unittest
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[3]
SELECTOR = ROOT / "storage-kit/scripts/feature_policy.py"


class FeaturePolicyTests(unittest.TestCase):
    def test_default_graph_declares_only_lance_native_dependencies(self):
        manifest = tomllib.loads((ROOT / "storage-kit/Cargo.toml").read_text())
        self.assertEqual(manifest["features"]["default"], [])
        self.assertEqual(set(manifest["features"]), {"default", "redb", "postgres", "surrealdb", "sqlite"})
        for package in ("redb", "tokio-postgres", "surrealdb", "surrealdb-core", "turso"):
            self.assertTrue(manifest["dependencies"][package]["optional"], package)
        self.assertFalse(manifest["dependencies"]["lancedb"].get("optional", False))
        packages = {value.get("package", name) for (_, name), value in self.policy().manifest_edges(manifest).items()}
        self.assertTrue({"qdrant-edge", "webbrowser"}.isdisjoint(packages))

    def test_default_selects_lance_only(self):
        output = subprocess.run(["python3", str(SELECTOR)], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(output.returncode, 0, output.stderr)
        import json
        self.assertEqual(json.loads(output.stdout)["backends"], ["lance"])

    def test_full_matrix_covers_each_retained_backend_and_their_composition(self):
        output = subprocess.run(["python3", str(SELECTOR), "--all-backends"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(output.returncode, 0, output.stderr)
        import json
        selection = json.loads(output.stdout)
        self.assertEqual(selection["backends"], ["lance", "redb", "postgres", "surrealdb", "sqlite"])
        self.assertEqual(selection["lanes"], [[], ["redb"], ["postgres"], ["surrealdb"], ["sqlite"], ["redb", "postgres", "surrealdb", "sqlite"]])

    def test_unknown_selection_fails(self):
        output = subprocess.run(["python3", str(SELECTOR), "--backend", "unknown"], cwd=ROOT, capture_output=True, text=True)
        self.assertNotEqual(output.returncode, 0)

    def policy(self, root=ROOT):
        spec = importlib.util.spec_from_file_location("storage_policy", SELECTOR)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.StorageSelection(root)

    def test_matrix_counts_and_no_backendless_build(self):
        policy = self.policy()
        self.assertEqual(len(list(policy.matrix(True))), 16)
        self.assertEqual(len(list(policy.matrix(False))), 0)
        self.assertEqual(policy.result()["lanes"], [[]])

    def test_direct_backend_and_shared_abstraction_selection(self):
        cases = {
            "storage-kit/src/state_stores/postgres/actor.rs": {"lance", "postgres"},
            "storage-kit/src/databases/surrealdb/client.rs": {"lance", "surrealdb"},
            "storage-kit/tests/lancedb_surrealdb_coexistence.rs": {"lance", "surrealdb"},
            "storage-kit/src/state_handle.rs": {"lance", "redb", "postgres", "sqlite"},
            "storage-kit/tests/database_cases/mod.rs": {"lance", "surrealdb"},
            "agents-kit/tests/strict_live.rs": {"lance"},
            "storage-kit/docs/testing.md": {"lance"},
        }
        for path, expected in cases.items():
            policy = self.policy()
            policy.classify(path)
            self.assertEqual(set(policy.result()["backends"]), expected, path)

    def test_changed_since_includes_deleted_staged_unstaged_and_untracked_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            def git(*args):
                return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, text=True)
            git("init")
            git("config", "user.email", "fixture@example.invalid")
            git("config", "user.name", "Fixture")
            for path in ("storage-kit/src/state_stores/redb/mod.rs", "storage-kit/src/databases/surrealdb/mod.rs"):
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("original")
            git("add", ".")
            git("commit", "-m", "fixture")
            base = git("rev-parse", "HEAD").stdout.strip()
            (root / "storage-kit/src/state_stores/redb/mod.rs").unlink()
            git("add", "-u")
            (root / "storage-kit/src/databases/surrealdb/mod.rs").write_text("changed")
            path = root / "storage-kit/src/state_stores/postgres/with space\nand newline.rs"
            path.parent.mkdir(parents=True)
            path.write_text("new")
            policy = self.policy(root)
            policy.changed_since(base)
            self.assertEqual(set(policy.result()["backends"]), {"lance", "redb", "surrealdb", "postgres"})
            self.assertIn(path.relative_to(root).as_posix(), policy.paths)

    def test_invalid_base_fails_instead_of_reducing_selection(self):
        for revision in ("not-a-real-ref", "", " "):
            with self.subTest(revision=revision):
                output = subprocess.run(["python3", str(SELECTOR), "--changed-since", revision], cwd=ROOT, capture_output=True, text=True)
                self.assertNotEqual(output.returncode, 0)
                self.assertIn("selection failed", output.stderr)
                self.assertEqual(output.stdout, "")

    def test_dependency_closure_resolves_versions_and_removed_edges(self):
        policy = self.policy()
        lock = {"package": [
            {"name": "storage-kit", "version": "0.0.1", "dependencies": ["redb 4.1.0"]},
            {"name": "redb", "version": "4.1.0", "dependencies": ["shared 1.0.0"]},
            {"name": "redb", "version": "4.2.0", "dependencies": ["shared 2.0.0"]},
            {"name": "shared", "version": "1.0.0"},
            {"name": "shared", "version": "2.0.0"},
            {"name": "unrelated", "version": "1.0.0"},
        ]}
        packages = policy.packages(lock)
        self.assertEqual(policy.closure(packages, ["redb"]), {("redb", "4.1.0", ""), ("shared", "1.0.0", "")})
        with self.assertRaisesRegex(ValueError, "unresolved dependency"):
            policy.dependency_key("shared", packages)

    def test_provider_only_duplicate_dependency_update_does_not_select_services(self):
        policy = self.policy()
        old = {"package": [
            {"name": "storage-kit", "version": "0.0.1", "dependencies": ["reqwest 0.13.2"]},
            {"name": "providers-kit", "version": "0.0.1", "dependencies": ["reqwest 0.12.28"]},
            {"name": "reqwest", "version": "0.13.2"},
            {"name": "reqwest", "version": "0.12.28"},
        ]}
        new = copy.deepcopy(old)
        new["package"][1]["dependencies"] = ["reqwest 0.12.29"]
        new["package"][3]["version"] = "0.12.29"
        manifest = {"dependencies": {"reqwest": {"version": "0.13.2", "optional": True}},
                    "dev-dependencies": {"reqwest": {"version": "0.13.2"}}}
        policy.document = lambda path, revision=None: (old if revision else new) if path == "Cargo.lock" else manifest
        policy.dependencies("base")
        self.assertEqual(policy.result()["backends"], ["lance"])

    def test_direct_dependency_retarget_and_removal_select_its_owner(self):
        old = {"package": [
            {"name": "storage-kit", "version": "0.0.1", "dependencies": ["redb 4.1.0"]},
            {"name": "other-sdk", "version": "0.1.0", "dependencies": ["redb 4.1.0", "redb 4.2.0"]},
            {"name": "redb", "version": "4.1.0"},
            {"name": "redb", "version": "4.2.0"},
        ]}
        manifest = {"dependencies": {"redb": {"version": "4", "optional": True}}}
        for dependencies in (["redb 4.2.0"], []):
            with self.subTest(dependencies=dependencies):
                policy = self.policy()
                new = copy.deepcopy(old)
                new["package"][0]["dependencies"] = dependencies
                policy.document = lambda path, revision=None: (old if revision else new) if path == "Cargo.lock" else manifest
                policy.dependencies("base")
                self.assertEqual(policy.result()["backends"], ["lance", "redb"])

    def test_direct_dependency_source_identity_is_required_and_resolved(self):
        policy = self.policy()
        lock = {"package": [
            {"name": "storage-kit", "version": "0.0.1", "dependencies": ["redb 4.1.0"]},
            {"name": "redb", "version": "4.1.0", "source": "registry+https://registry-a.invalid/index"},
            {"name": "redb", "version": "4.1.0", "source": "registry+https://registry-b.invalid/index"},
        ]}
        with self.assertRaisesRegex(ValueError, "unresolved dependency identity"):
            policy.closure(policy.packages(lock), ["redb"])
        lock["package"][0]["dependencies"] = ["redb 4.1.0 (registry+https://registry-a.invalid/index)"]
        self.assertEqual(policy.closure(policy.packages(lock), ["redb"]),
                         {("redb", "4.1.0", "registry+https://registry-a.invalid/index")})


    def test_default_verifier_runs_both_lance_products_without_remote_prerequisites(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / "storage-kit/scripts"
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ("check.sh", "verify_strict_live.sh", "feature_policy.py"):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            self.install_script_contract_fixture(root)
            binary = root / "bin"
            binary.mkdir()
            cargo = binary / "cargo"
            cargo.write_text(r"""#!/usr/bin/env python3
import os,sys
with open(os.environ['CALLS'], 'a') as out: out.write(' '.join(sys.argv[1:]) + '\n')
if os.environ.get('FAIL_LIVE') and 'strict_live' in sys.argv and '--list' not in sys.argv:
    print('fixture live failure', file=sys.stderr)
    sys.exit(29)
if '--list' in sys.argv: print('fixture_case: test')
else:
    count = 0 if os.environ.get('EMPTY_LIVE') and 'strict_live' in sys.argv else 1
    print(f'test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out')
""")
            cargo.chmod(0o700)
            for name in ("docker", "curl"):
                tool = binary / name
                tool.write_text("#!/bin/sh\nexit 99\n")
                tool.chmod(0o700)
            environment = {"PATH": str(binary) + os.pathsep + os.environ["PATH"], "CALLS": str(root / "calls"), "TMPDIR": str(root)}
            run = subprocess.run(["bash", str(scripts / "verify_strict_live.sh")], env=environment, capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertIn("execution PASS", run.stdout)
            calls = (root / "calls").read_text()
            for target in ("strict_live", "lancedb_state_store", "lancedb_native_feasibility", "lancedb_durability_feasibility", "lancedb_session_ownership"):
                self.assertIn('--test ' + target, calls)
            self.assertNotIn('--features', calls)
            self.assertEqual(list(root.glob('storage-lance.*')), [])
            environment['EMPTY_LIVE'] = '1'
            empty = subprocess.run(["bash", str(scripts / "verify_strict_live.sh")], env=environment, capture_output=True, text=True)
            self.assertNotEqual(empty.returncode, 0)
            self.assertIn('complete nonempty inventory', empty.stderr)
            self.assertNotIn('execution PASS', empty.stdout)
            self.assertEqual(list(root.glob('storage-lance.*')), [])
            removal = binary / "rm"
            removal.write_text("#!/bin/sh\nprintf 'fixture removal failure\n' >&2\nexit 41\n")
            removal.chmod(0o700)
            failed_cleanup = subprocess.run(["bash", str(scripts / "verify_strict_live.sh")], env=environment, capture_output=True, text=True)
            self.assertEqual(failed_cleanup.returncode, empty.returncode, 'cleanup must preserve the primary live inventory failure')
            self.assertIn('fixture removal failure', failed_cleanup.stderr)
            self.assertNotIn('execution PASS', failed_cleanup.stdout)
            environment.pop('EMPTY_LIVE')
            environment['FAIL_LIVE'] = '1'
            live_failure = subprocess.run(["bash", str(scripts / "verify_strict_live.sh")], env=environment, capture_output=True, text=True)
            self.assertEqual(live_failure.returncode, 29)
            self.assertIn('fixture live failure', live_failure.stderr)
            self.assertIn('fixture removal failure', live_failure.stderr)
            self.assertNotIn('execution PASS', live_failure.stdout)




    def test_manifest_metadata_does_not_select_alternatives(self):
        policy = self.policy()
        old = {"package": {"version": "0.1.0", "description": "old"}, "dependencies": {"redb": {"version": "4.1.0", "optional": True}}}
        new = {"package": {"version": "0.2.0", "description": "new"}, "dependencies": old["dependencies"]}
        policy.document = lambda path, revision=None: old if revision else new
        policy.manifest_dependencies("base")
        self.assertEqual(policy.result()["backends"], ["lance"])

    def test_manifest_backend_requirement_selects_only_its_owner(self):
        policy = self.policy()
        old = {"dependencies": {"redb": {"version": "4.1.0", "optional": True}}}
        new = {"dependencies": {"redb": {"version": "=4.1.0", "optional": True}}}
        policy.document = lambda path, revision=None: old if revision else new
        policy.manifest_dependencies("base")
        self.assertEqual(policy.result()["backends"], ["lance", "redb"])

    def test_target_scoped_common_dependency_changes_select_all(self):
        policy = self.policy()
        old = {"target": {"cfg(unix)": {"dependencies": {"tokio": {"version": "1", "features": ["fs"]}}}}}
        new = {"target": {"cfg(unix)": {"dependencies": {"tokio": {"version": "1", "features": ["fs", "time"]}}}}}
        policy.document = lambda path, revision=None: old if revision else new
        policy.manifest_dependencies("base")
        self.assertEqual(policy.result()["backends"], list(policy.BACKENDS))

    def test_lance_only_direct_requirements_select_lance(self):
        for name in ("arrow-array", "arrow-schema", "rustix", "lance-io"):
            with self.subTest(dependency=name):
                policy = self.policy()
                old = {"dependencies": {name: {"version": "1"}}}
                new = {"dependencies": {name: {"version": "=1"}}}
                if name == "rustix":
                    old, new = {"target": {"cfg(unix)": old}}, {"target": {"cfg(unix)": new}}
                policy.document = lambda path, revision=None: old if revision else new
                policy.manifest_dependencies("base")
                self.assertEqual(policy.result()["backends"], ["lance"])

    def test_lance_only_locked_dependencies_preserve_shared_reachability(self):
        for name in ("arrow-array", "arrow-schema", "rustix", "lance-io"):
            for shared in (False, True):
                with self.subTest(dependency=name, shared=shared):
                    policy = self.policy()
                    old = {"package": [
                        {"name": "storage-kit", "version": "0.0.1", "dependencies": [name]},
                        {"name": name, "version": "1.0.0"},
                    ]}
                    manifest = {"dependencies": {name: {"version": "1"}}}
                    if name == "rustix":
                        manifest = {"target": {"cfg(unix)": manifest}}
                    if shared:
                        old["package"][0]["dependencies"].append("tempfile")
                        old["package"].append({"name": "tempfile", "version": "1.0.0", "dependencies": [name]})
                        manifest["dev-dependencies"] = {"tempfile": {"version": "1"}}
                    new = copy.deepcopy(old)
                    new["package"][1]["version"] = "1.0.1"
                    policy.document = lambda path, revision=None: (old if revision else new) if path == "Cargo.lock" else manifest
                    policy.dependencies("base")
                    expected = list(policy.BACKENDS) if shared else ["lance"]
                    self.assertEqual(policy.result()["backends"], expected)

    def test_dependency_order_is_not_a_lock_graph_change(self):
        policy = self.policy()
        old = {"package": [{"name": "native", "version": "1.0.0", "dependencies": ["alpha", "beta"]}]}
        new = {"package": [{"name": "native", "version": "1.0.0", "dependencies": ["beta", "alpha"]}]}
        self.assertEqual(policy.packages(old), policy.packages(new))

    def test_root_patch_configuration_selects_all(self):
        policy = self.policy()
        old = {"workspace": {"dependencies": {}}}
        new = {"workspace": {"dependencies": {}}, "patch": {"crates-io": {"redb": {"path": "local"}}}}
        policy.document = lambda path, revision=None: old if revision else new
        policy.workspace_dependencies("base")
        self.assertEqual(policy.result()["backends"], list(policy.BACKENDS))


    def test_interrupted_verifier_settles_its_owned_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / "storage-kit/scripts"
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ("check.sh", "verify_strict_live.sh", "feature_policy.py"):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            self.install_script_contract_fixture(root)
            binary = root / "bin"
            binary.mkdir()
            cargo = binary / "cargo"
            cargo.write_text(r"""#!/usr/bin/env python3
import os,pathlib,signal,time
signal.signal(signal.SIGTERM, lambda *_: (pathlib.Path(os.environ['SETTLED']).write_text('settled'), exit(143)))
pathlib.Path(os.environ['READY']).write_text(str(os.getpid()))
while True: time.sleep(0.01)
""")
            cargo.chmod(0o700)
            environment = {"PATH": str(binary) + os.pathsep + os.environ["PATH"], "READY": str(root / "ready"), "SETTLED": str(root / "settled"), "TMPDIR": str(root)}
            process = subprocess.Popen(["bash", str(scripts / "verify_strict_live.sh")], env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                deadline = time.monotonic() + 5
                while not (root / "ready").exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue((root / "ready").exists(), "command must have started")
                process.terminate()
                process.wait(timeout=8)
                self.assertEqual(process.returncode, 143)
                self.assertTrue((root / "settled").exists(), "accepted subprocess must settle before verifier exit")
            finally:
                try: os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError: pass
                if (root / "ready").exists() and not (root / "settled").exists():
                    try: os.kill(int((root / "ready").read_text()), signal.SIGTERM)
                    except ProcessLookupError: pass
                process.wait(timeout=3)


    def test_feature_only_development_dependency_change_selects_alternatives(self):
        policy = self.policy()
        old = {"dev-dependencies": {"reqwest": {"version": "0.13", "features": ["json"]}}}
        new = {"dev-dependencies": {"reqwest": {"version": "0.13", "features": ["json", "blocking"]}}}
        policy.document = lambda path, revision=None: old if revision else new
        policy.manifest_dependencies("base")
        self.assertEqual(policy.result()["backends"], list(policy.BACKENDS))

    def test_rustdoc_only_changes_preserve_implementation_identity(self):
        policy = self.policy()
        old = '/// old example\npub struct Test;\n'
        new = '/// new example\n//! added docs\npub struct Test;\n'
        self.assertEqual(policy.rust_without_docs(old), policy.rust_without_docs(new))
        self.assertNotEqual(policy.rust_without_docs(old), policy.rust_without_docs('/// old example\npub struct Different;\n'))
        self.assertIsNone(policy.rust_without_docs('let value = r#"\n/// actual string payload\n"#;'))
        self.assertIsNone(policy.rust_without_docs('let value = "\n/// actual string payload\n";'))


    def test_inherited_shared_test_dependency_selects_all(self):
        policy = self.policy()
        old = {"workspace": {"dependencies": {"reqwest": {"version": "0.13", "features": ["json"]}}}}
        new = {"workspace": {"dependencies": {"reqwest": {"version": "0.13", "features": ["json", "blocking"]}}}}
        storage = {"dev-dependencies": {"reqwest": {"workspace": True}}}
        policy.document = lambda path, revision=None: storage if path.startswith('storage-kit/') else (old if revision else new)
        policy.workspace_dependencies("base")
        self.assertEqual(policy.result()["backends"], list(policy.BACKENDS))


    def install_release_stage_fixture(self, root):
        (root / "scripts").mkdir(exist_ok=True)
        shutil.copy2(ROOT / "scripts/release_stage.sh", root / "scripts/release_stage.sh")

    def install_script_contract_fixture(self, root):
        # The copied verifier exercises orchestration, not this full suite again.
        tests = root / 'storage-kit/scripts/tests'
        tests.mkdir(parents=True)
        (tests / 'test_fixture_contract.py').write_text('''import importlib.util, os, pathlib, unittest
class FixtureContract(unittest.TestCase):
    def test_copied_selector_default(self):
        root = pathlib.Path(__file__).resolve().parents[3]
        spec = importlib.util.spec_from_file_location('fixture_policy', root / 'storage-kit/scripts/feature_policy.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.StorageSelection(root).result()['backends'], ['lance'])
        (root / 'script-contract-ran').write_text('executed')
        self.assertNotEqual(os.environ.get('STORAGE_FAILURE'), 'script_contract')
''')

    def command_owner(self, root, behavior):
        # Execute the existing script-owned class under ordinary process models.
        # This establishes control flow only; the real-child test below covers OS execution.
        import ast
        import io
        import re
        import types
        source = (SELECTOR.parent / 'verify_strict_live.sh').read_text()
        embedded = re.search(r"<<'PY' &\n(.*?)\nPY\n", source, re.S).group(1)
        definition = next(node for node in ast.parse(embedded).body if isinstance(node, ast.ClassDef))
        tree = ast.Module(body=[definition], type_ignores=[])
        trace = {'clock': 0, 'killed': False, 'post_kill': 0, 'signals': [], 'permission_errors': 0, 'group_absent': False, 'stderr': io.StringIO()}
        receipt = root / 'ownership'
        receipt.write_text('unsettled')
        def monotonic():
            trace['clock'] += 1
            return trace['clock']
        def killpg(_pid, signum):
            trace['signals'].append(signum)
            if behavior == 'during_spawn' and signum == 0:
                raise ProcessLookupError()
            if signum == signal.SIGKILL:
                trace['killed'] = True
                if behavior == 'disappeared':
                    raise ProcessLookupError()
            if signum == 0 and trace['killed']:
                trace['post_kill'] += 1
                if behavior in ('transient_permission', 'persistent_permission'):
                    if behavior == 'persistent_permission' or trace['post_kill'] == 1:
                        trace['permission_errors'] += 1
                        raise PermissionError(1, 'fixture group observation denied')
                    trace['group_absent'] = True
                    raise ProcessLookupError()
                if behavior == 'disappeared' or (behavior == 'settled' and trace['post_kill'] >= 3):
                    raise ProcessLookupError()
        def poll():
            if trace['permission_errors']:
                process.returncode = -signal.SIGKILL
            return process.returncode
        pending_reap = behavior in ('transient_permission', 'persistent_permission')
        process = types.SimpleNamespace(pid=4242, returncode=None if pending_reap else 47,
                                        wait=lambda timeout=None: 47, poll=poll)
        def start(*_arguments, **_options):
            if behavior == 'during_spawn' or pending_reap:
                owner.interrupted(signal.SIGTERM, None)
            return process
        namespace = {
            'os': types.SimpleNamespace(killpg=killpg, environ={}),
            'signal': types.SimpleNamespace(SIGTERM=signal.SIGTERM, SIGINT=signal.SIGINT, SIGKILL=signal.SIGKILL, SIG_IGN=signal.SIG_IGN, signal=lambda *_: None),
            'subprocess': types.SimpleNamespace(Popen=start, TimeoutExpired=subprocess.TimeoutExpired),
            'sys': types.SimpleNamespace(stderr=trace['stderr']),
            'time': types.SimpleNamespace(monotonic=monotonic, sleep=lambda _: None),
            'Path': pathlib.Path,
        }
        exec(compile(tree, 'verifier-command-owner', 'exec'), namespace)
        owner = namespace[definition.name](['0', str(receipt), 'fixture'])
        return owner, trace, receipt

    def test_owned_command_observes_group_after_forced_termination(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'settled')
            self.assertEqual(owner.execute(), 47)
            self.assertGreaterEqual(trace['post_kill'], 3)
            self.assertFalse(receipt.exists(), 'only observed settlement releases ownership')

    def test_owned_command_preserves_primary_if_group_disappears_during_kill(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'disappeared')
            self.assertEqual(owner.execute(), 47)
            self.assertGreater(trace['post_kill'], 0)
            self.assertFalse(receipt.exists())

    def test_owned_command_retains_resources_for_unacknowledged_release_child(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'settled')
            marker = receipt.with_name(receipt.name + '.release-fixture')
            marker.write_text('child group settlement is unknown')
            self.assertEqual(owner.execute(), 47, 'cleanup never erases the primary failure')
            self.assertGreaterEqual(trace['post_kill'], 3, 'the parent group itself was settled')
            self.assertTrue(receipt.exists(), 'separate child authority must keep parent ownership')
            self.assertTrue(marker.exists())
            self.assertIn('unacknowledged', trace['stderr'].getvalue())

    def test_owned_command_retains_ownership_for_unsettled_group(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'unsettled')
            self.assertEqual(owner.execute(), 47)
            self.assertGreater(trace['post_kill'], 0)
            self.assertTrue(receipt.exists(), 'unknown settlement must retain ownership')

    def test_owned_command_rechecks_transient_permission_before_releasing_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'transient_permission')
            self.assertEqual(owner.execute(), 143, 'retain the original interruption')
            self.assertEqual(trace['permission_errors'], 1)
            self.assertTrue(trace['group_absent'], 'permission failure alone cannot establish settlement')
            self.assertIsNotNone(owner.process.returncode, 'direct child must be reaped')
            self.assertFalse(receipt.exists(), 'subsequent observed absence releases ownership')

    def test_owned_command_retains_ownership_after_persistent_permission_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'persistent_permission')
            self.assertEqual(owner.execute(), 143, 'retain the original interruption')
            self.assertGreater(trace['permission_errors'], 0)
            self.assertLess(trace['clock'], 70, 'persistent permission failure remains bounded')
            self.assertFalse(trace['group_absent'])
            self.assertTrue(receipt.exists(), 'permission failure never permits resource deletion')
            self.assertIn('fixture group observation denied', trace['stderr'].getvalue())
            self.assertEqual(trace['stderr'].getvalue().count('settlement unknown:'), 1,
                             'report the retained permission error once, after bounded observation')

    def test_interruption_during_process_creation_preserves_child_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, trace, receipt = self.command_owner(pathlib.Path(directory), 'during_spawn')
            self.assertEqual(owner.execute(), 143)
            self.assertIn(signal.SIGTERM, trace['signals'], 'created child must enter owned cleanup')
            self.assertFalse(receipt.exists(), 'observed child settlement releases ownership')

    def test_embedded_cleanup_reobserves_transient_permission_and_bounds_persistent_failure(self):
        import ast
        import re
        import types
        source = (SELECTOR.parent / 'verify_lancedb_state_store.sh').read_text()
        embedded = re.search(r"<<'PY'\n(.*?)\nPY", source, re.S).group(1)
        definitions = ast.Module(body=[node for node in ast.parse(embedded).body
                                      if isinstance(node, (ast.Import, ast.ImportFrom, ast.ClassDef))], type_ignores=[])
        for mode in ('transient', 'persistent', 'recovered-live', 'cleanup-failures'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                namespace = {}
                exec(compile(definitions, 'embedded-command-owner', 'exec'), namespace)
                root = pathlib.Path(directory)
                owner = namespace['EmbeddedLanceRun'](root / 'storage-kit/scripts/verifier.sh', root / 'evidence')
                trace = {'clock': 0, 'probes': 0, 'waited': False}
                def monotonic():
                    trace['clock'] += 1
                    return trace['clock']
                def killpg(_pid, signum):
                    if signum == signal.SIGKILL and mode == 'cleanup-failures':
                        raise PermissionError(1, 'fixture termination denied')
                    if signum == 0:
                        trace['probes'] += 1
                        if mode in ('persistent', 'cleanup-failures') or trace['probes'] == 1:
                            raise PermissionError(1, 'fixture observation denied')
                        if mode != 'recovered-live':
                            raise ProcessLookupError()
                def wait(timeout=None):
                    trace['waited'] = True
                    if mode == 'cleanup-failures':
                        raise subprocess.TimeoutExpired('fixture child', timeout)
                    process.returncode = -signal.SIGTERM
                    return process.returncode
                process = types.SimpleNamespace(pid=4242, returncode=None, poll=lambda: None, wait=wait)
                namespace['os'] = types.SimpleNamespace(killpg=killpg)
                namespace['time'] = types.SimpleNamespace(monotonic=monotonic, sleep=lambda _: None)
                if mode != 'transient':
                    with self.assertRaisesRegex(RuntimeError, 'owned command required forced cleanup'):
                        owner.settle(process)
                    self.assertGreater(trace['probes'], 1)
                    self.assertLess(trace['clock'], 40, 'cleanup budget must not restart')
                    self.assertTrue(owner.receipt['errors'], 'unsettled group remains failed')
                    unavailable = 'owned command group observation remained unavailable'
                    self.assertEqual(unavailable in owner.receipt['errors'], mode != 'recovered-live')
                    if mode == 'cleanup-failures':
                        self.assertIn('owned command forced termination failed (PermissionError)', owner.receipt['errors'])
                        self.assertIn('owned command child reap failed (TimeoutExpired)', owner.receipt['errors'])
                else:
                    owner.settle(process)
                    self.assertEqual(trace['probes'], 2)
                self.assertTrue(trace['waited'], 'reap the direct child even when group observation fails')

    def test_embedded_verifier_owns_launch_interruption_and_preserves_errors(self):
        import ast
        import json
        import re
        import sys
        import types

        source = (SELECTOR.parent / 'verify_lancedb_state_store.sh').read_text()
        embedded = re.search(r"<<'PY'\n(.*?)\nPY", source, re.S).group(1)
        definitions = ast.Module(body=[node for node in ast.parse(embedded).body
                                      if isinstance(node, (ast.Import, ast.ImportFrom, ast.ClassDef))],
                                 type_ignores=[])
        cases = (
            ('interrupt_int', signal.SIGINT, None, False, False),
            ('interrupt_term', signal.SIGTERM, None, False, False),
            ('interrupt_cleanup', signal.SIGTERM, None, False, True),
            ('command_cleanup', None, 23, False, True),
            ('launch_failure', None, None, True, False),
            ('interrupted_launch_failure', signal.SIGTERM, None, True, False),
        )
        for name, signum, child_exit, launch_failure, cleanup_failure in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                (root / 'Cargo.lock').write_text('fixture lock\n')
                namespace = {}
                exec(compile(definitions, str(SELECTOR.parent / 'verify_lancedb_state_store.sh'), 'exec'), namespace)
                owner = namespace['EmbeddedLanceRun'](root / 'storage-kit/scripts/verifier.sh', root / 'evidence')
                # These doubles isolate process supervision, not compiler or backend behavior.
                owner.metadata = lambda command: 'rustc 1.99.0 (fixture)' if command[0] == 'rustc' else 'fixture'
                owner.filesystem = lambda: {'status': 'unavailable', 'reason': 'orchestration fixture'}
                namespace['shutil'] = types.SimpleNamespace(which=lambda command: command)
                children = []
                settled = []
                ready = root / 'ready'

                def start(_arguments, **options):
                    if launch_failure:
                        if signum is not None:
                            os.kill(os.getpid(), signum)
                        raise OSError('fixture launch failure')
                    child = subprocess.Popen([
                        sys.executable, '-c',
                        'import pathlib, signal, sys\n'
                        'pathlib.Path(sys.argv[1]).write_text("ready")\n'
                        'if sys.argv[2] == "wait": signal.pause()\n'
                        'else: sys.exit(int(sys.argv[2]))\n',
                        str(ready), 'wait' if child_exit is None else str(child_exit),
                    ], **options)
                    children.append(child)  # Own fallback cleanup even against the broken verifier.
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists(), 'child must start before interruption')
                    if signum is not None:
                        os.kill(os.getpid(), signum)
                    return child  # The verifier has not received this handle when the signal fires.

                namespace['subprocess'] = types.SimpleNamespace(Popen=start, STDOUT=subprocess.STDOUT)
                actual_settle = owner.settle

                def settle(child):
                    settled.append(child.pid)
                    # Repeated interruption cannot cancel already-admitted cleanup.
                    os.kill(os.getpid(), signal.SIGINT)
                    os.kill(os.getpid(), signal.SIGTERM)
                    actual_settle(child)
                    if cleanup_failure:
                        raise RuntimeError('fixture cleanup failure after real settlement')

                owner.settle = settle
                handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
                try:
                    status = owner.execute()
                    # Observe before fallback cleanup: the old code leaves a live orphan here.
                    for child in children:
                        self.assertIsNotNone(child.poll(), 'verifier must reap its launched child')
                        with self.assertRaises(ProcessLookupError):
                            os.killpg(child.pid, 0)
                    self.assertEqual(settled, [child.pid for child in children])
                    receipt = json.loads((owner.output / 'summary.json').read_text())
                    self.assertEqual(status, child_exit if child_exit is not None else 2)
                    self.assertEqual(receipt['execution'], 'FAIL' if child_exit is not None else 'INCOMPLETE')
                    self.assertEqual(len(receipt['commands']), 1, 'failure must prevent later targets')
                    record = receipt['commands'][0]
                    self.assertGreaterEqual(record['seconds'], 0)
                    self.assertEqual(receipt['source'], receipt['source_after'])
                    if children:
                        self.assertEqual(record['exit_code'], children[0].returncode)
                    else:
                        self.assertNotIn('exit_code', record, 'failed spawn has no child outcome')
                    errors = '\n'.join(receipt['errors'])
                    if signum is not None:
                        self.assertIn(f'interrupted by signal {signum}', errors)
                    if launch_failure:
                        self.assertIn('fixture launch failure', errors)
                    if cleanup_failure:
                        self.assertIn('fixture cleanup failure', errors)
                    if child_exit is not None:
                        self.assertIn(f'exited {child_exit}', errors)
                finally:
                    try:
                        for child in children:
                            try:
                                os.killpg(child.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            child.wait(timeout=5)
                    finally:
                        for number, handler in handlers.items():
                            signal.signal(number, handler)

    def test_script_contract_failure_precedes_cargo_or_services(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / 'storage-kit/scripts'
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ('check.sh', 'verify_strict_live.sh', 'feature_policy.py'):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            self.install_script_contract_fixture(root)
            binary = root / 'bin'
            binary.mkdir()
            for name in ('cargo', 'docker', 'curl'):
                tool = binary / name
                tool.write_text('#!/bin/sh\nprintf invoked >> "${CALLS}"\nexit 91\n')
                tool.chmod(0o700)
            environment = {'PATH': str(binary) + os.pathsep + os.environ['PATH'], 'CALLS': str(root / 'calls'), 'TMPDIR': str(root), 'STORAGE_FAILURE': 'script_contract'}
            run = subprocess.run(['bash', str(scripts / 'verify_strict_live.sh')], env=environment, capture_output=True, text=True)
            self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
            self.assertTrue((root / 'script-contract-ran').exists())
            self.assertFalse((root / 'calls').exists(), 'script-contract failure must precede Cargo and services')
            self.assertNotIn('execution PASS', run.stdout)

    def test_verifier_reaps_stubborn_direct_child_before_removing_datasets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / 'storage-kit/scripts'
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ('verify_strict_live.sh', 'feature_policy.py'):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            # A direct, invocation-owned child isolates run_owned's real OS lifecycle.
            (scripts / 'check.sh').write_text('#!/bin/sh\nexec cargo\n')
            binary = root / 'bin'
            binary.mkdir()
            cargo = binary / 'cargo'
            cargo.write_text('''#!/usr/bin/env python3
import os, pathlib, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
pathlib.Path(os.environ['READY']).write_text(str(os.getpid()))
while True: time.sleep(0.01)
''')
            cargo.chmod(0o700)
            environment = {'PATH': str(binary) + os.pathsep + os.environ['PATH'], 'READY': str(root / 'ready'), 'TMPDIR': str(root)}
            process = subprocess.Popen(['bash', str(scripts / 'verify_strict_live.sh')], env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
            child = None
            try:
                deadline = time.monotonic() + 5
                while not (root / 'ready').exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue((root / 'ready').exists(), 'owned child must have actually started')
                child = int((root / 'ready').read_text())
                self.assertTrue(list(root.glob('storage-lance.*')))
                process.terminate()
                stdout, stderr = process.communicate(timeout=45)
                self.assertEqual(process.returncode, 143, stdout + stderr)
                self.assertIn('cleanup deadline', stderr, 'TERM refusal must exercise forced cleanup')
                with self.assertRaises(ProcessLookupError, msg=stdout + stderr):
                    os.kill(child, 0)
                self.assertEqual(list(root.glob('storage-lance.*')), [], stdout + stderr)
                self.assertNotIn('execution PASS', stdout)
            finally:
                if child is not None:
                    try: os.kill(child, signal.SIGKILL)
                    except ProcessLookupError: pass
                if process.poll() is None:
                    process.terminate()
                process.communicate(timeout=35)


    def test_unsettled_live_command_retains_owned_dataset_and_services(self):
        # A command-runner double leaves its ownership receipt unsettled. The
        # actual verifier must preserve the primary status and owned resources.
        # No backend service or unknown process is started by this fixture.
        import sys
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / 'storage-kit/scripts'
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ('check.sh', 'verify_strict_live.sh', 'feature_policy.py'):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            self.install_script_contract_fixture(root)
            binary = root / 'bin'
            binary.mkdir()
            python = binary / 'python3'
            python.write_text(f'#!{sys.executable}\n' + '''import os, subprocess, sys
if len(sys.argv) > 1 and sys.argv[1] == '-':
    source = sys.stdin.read()
    if 'strict_live' in sys.argv and '--list' not in sys.argv:
        print('fixture cannot establish owned command settlement', file=sys.stderr)
        sys.exit(47)
    sys.exit(subprocess.run([sys.executable, *sys.argv[1:]], input=source, text=True).returncode)
os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
''')
            python.chmod(0o700)
            cargo = binary / 'cargo'
            cargo.write_text(f'#!{sys.executable}\n' + '''import sys
if '--list' in sys.argv: print('fixture_case: test')
else: print('test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out')
''')
            cargo.chmod(0o700)
            docker = binary / 'docker'
            docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "${DOCKER_CALLS}"\n')
            docker.chmod(0o700)
            environment = {
                'PATH': str(binary) + os.pathsep + os.environ['PATH'],
                'DOCKER_CALLS': str(root / 'docker-calls'), 'TMPDIR': str(root),
                'TEST_POSTGRES_STATE_STORE_HOST': '127.0.0.1',
                'TEST_POSTGRES_STATE_STORE_PORT': '15432',
                'TEST_POSTGRES_STATE_STORE_DB': 'fixture',
                'TEST_POSTGRES_STATE_STORE_USER': 'fixture',
                'TEST_POSTGRES_STATE_STORE_PASS': 'fixture',
            }
            run = subprocess.run(['bash', str(scripts / 'verify_strict_live.sh'), '--backend', 'postgres'], env=environment, capture_output=True, text=True)
            self.assertEqual(run.returncode, 47, run.stdout + run.stderr)
            calls = (root / 'docker-calls').read_text()
            self.assertIn(' up -d postgres', calls)
            self.assertNotIn(' down ', calls, 'unsettled commands retain service ownership')
            self.assertEqual(len(list(root.glob('storage-lance.*'))), 1)
            self.assertIn('retaining owned datasets and services', run.stderr)
            self.assertNotIn('execution PASS', run.stdout)


    def test_receipt_allocation_failure_prevents_command_launch(self):
        import shlex
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / 'storage-kit/scripts'
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ('verify_strict_live.sh', 'feature_policy.py'):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            (scripts / 'check.sh').write_text('#!/bin/sh\nexit 0\n')
            binary = root / 'bin'
            binary.mkdir()
            actual_mktemp = shutil.which('mktemp')
            self.assertIsNotNone(actual_mktemp)
            mktemp = binary / 'mktemp'
            mktemp.write_text('''#!/bin/sh
case "$*" in
    *command-ownership.*)
        if [ -e "$INVENTORY_COMPLETED" ]; then exit 23; fi
        ;;
esac
exec ''' + shlex.quote(actual_mktemp) + ' "$@"\n')
            mktemp.chmod(0o700)
            cargo = binary / 'cargo'
            cargo.write_text('''#!/bin/sh
case "$*" in
    *--list*) printf 'fixture_case: test\\n'; printf complete > "$INVENTORY_COMPLETED";;
    *) printf launched > "$LAUNCHED"
       printf 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\\n';;
esac
''')
            cargo.chmod(0o700)
            environment = {'PATH': str(binary) + os.pathsep + os.environ['PATH'],
                           'TMPDIR': str(root), 'LAUNCHED': str(root / 'launched'),
                           'INVENTORY_COMPLETED': str(root / 'inventory-completed')}
            result = subprocess.run(['bash', str(scripts / 'verify_strict_live.sh')], env=environment,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
            self.assertFalse((root / 'launched').exists(), 'commands require an ownership receipt')
            self.assertEqual(list(root.glob('storage-lance.*')), [])
            self.assertNotIn('execution PASS', result.stdout)


    def test_interruption_during_launch_still_joins_the_recorded_command(self):
        # Signal during ordinary receipt allocation, before the background
        # process identifier can be assigned. Record actual waits and delegate
        # to the shell builtin; this does not intercept operating-system calls.
        import shlex
        import sys
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / 'storage-kit/scripts'
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ('verify_strict_live.sh', 'feature_policy.py'):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            (scripts / 'check.sh').write_text('#!/bin/sh\nexit 0\n')
            binary = root / 'bin'
            binary.mkdir()
            actual_mktemp = shutil.which('mktemp')
            self.assertIsNotNone(actual_mktemp)
            mktemp = binary / 'mktemp'
            mktemp.write_text(f'#!{sys.executable}\n' + f'ACTUAL = {actual_mktemp!r}\n' + '''import os, pathlib, signal, subprocess, sys, time
result = subprocess.run([ACTUAL, *sys.argv[1:]], text=True, capture_output=True)
if result.returncode == 0 and '/command-ownership.' in sys.argv[-1]:
    target = pathlib.Path(os.environ['SIGNAL_TARGET'])
    deadline = time.monotonic() + 5
    while not target.exists() and time.monotonic() < deadline: time.sleep(0.01)
    os.kill(int(target.read_text()), signal.SIGTERM)
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
sys.exit(result.returncode)
''')
            mktemp.chmod(0o700)
            environment_file = root / 'shell-environment'
            environment_file.write_text('wait() {\n  printf "%s\\n" "$1" >> '
                                        + shlex.quote(str(root / 'waited'))
                                        + '\n  builtin wait "$@"\n}\n')
            environment = {
                'PATH': str(binary) + os.pathsep + os.environ['PATH'],
                'TMPDIR': str(root), 'BASH_ENV': str(environment_file),
                'SIGNAL_TARGET': str(root / 'signal-target'),
            }
            process = subprocess.Popen(['bash', str(scripts / 'verify_strict_live.sh')], env=environment,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                       start_new_session=True)
            (root / 'signal-target').write_text(str(process.pid))
            try:
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 143, stdout + stderr)
                self.assertTrue((root / 'waited').exists(), 'deferred launch interruption must join the recorded command')
                waited = (root / 'waited').read_text().splitlines()
                self.assertEqual(len(waited), 1, waited)
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(waited[0]), 0)
                self.assertNotIn('execution PASS', stdout)
                # Interrupted startup may leave no Python settlement receipt.
                # In that case the owned dataset must remain protected.
                if list(root.glob('storage-strict.*/command-ownership.*')):
                    self.assertEqual(len(list(root.glob('storage-lance.*'))), 1)
            finally:
                if process.poll() is None: process.terminate()
                process.communicate(timeout=35)


    def test_selected_live_lanes_isolate_paths_and_bind_owned_service_endpoints(self):
        import json
        import sys
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            scripts = root / "storage-kit/scripts"
            scripts.mkdir(parents=True)
            self.install_release_stage_fixture(root)
            for name in ("verify_strict_live.sh", "feature_policy.py"):
                shutil.copy2(SELECTOR.parent / name, scripts / name)
            (scripts / "check.sh").write_text("#!/bin/sh\nexit 0\n")
            binary = root / "bin"
            binary.mkdir()
            for name, source in {
                "cargo": r"""import json, os, pathlib, sys
if '--list' in sys.argv:
    print('fixture_case: test')
else:
    names = ('TEST_LANCEDB_EMBEDDED_PATH', 'TEST_SURREALDB_ROCKSDB_PATH', 'TEST_SURREALDB_SURREALKV_PATH')
    paths = {name: os.environ.get(name) for name in names}
    with pathlib.Path(os.environ['TRACE']).open('a') as stream: stream.write(json.dumps(paths) + '\n')
    print('test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out')
""",
                "docker": r"""import os, pathlib, sys
with pathlib.Path(os.environ['SERVICE_TRACE']).open('a') as stream: stream.write(' '.join(sys.argv[1:]) + '\n')
""",
                "curl": r"""import os, pathlib, sys
with pathlib.Path(os.environ['HTTP_TRACE']).open('a') as stream: stream.write(sys.argv[-1] + '\n')
""",
            }.items():
                tool = binary / name
                tool.write_text('#!' + sys.executable + '\n' + source)
                tool.chmod(0o700)
            names = ('TEST_LANCEDB_EMBEDDED_PATH', 'TEST_SURREALDB_ROCKSDB_PATH', 'TEST_SURREALDB_SURREALKV_PATH')
            environment = {
                'PATH': str(binary) + os.pathsep + os.environ['PATH'], 'TMPDIR': str(root),
                'TRACE': str(root / 'paths.jsonl'), 'SERVICE_TRACE': str(root / 'services'), 'HTTP_TRACE': str(root / 'http'),
                'TEST_SURREALDB_REMOTE_HTTP': 'http://127.0.0.1:28000', 'TEST_SURREALDB_REMOTE_WS': 'ws://127.0.0.1:28000',
                'TEST_SURREALDB_REMOTE_PORT': '28000', 'TEST_SURREALDB_ROOT_USER': 'fixture', 'TEST_SURREALDB_ROOT_PASS': 'fixture',
                'TEST_SURREALDB_NS': 'fixture', 'TEST_SURREALDB_DB': 'fixture',
            }
            for name in names:
                base = root / ('caller ' + name)
                base.mkdir()
                (base / 'sentinel').write_text('preserved')
                environment[name] = str(base)
            arguments = ['bash', str(scripts / 'verify_strict_live.sh'), '--backend', 'surrealdb', '--backend', 'sqlite']
            rejected = dict(environment, TEST_SURREALDB_REMOTE_HTTP='http://example.invalid:28000')
            result = subprocess.run(arguments, env=rejected, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0, 'foreign endpoint must fail before service mutation')
            self.assertFalse((root / 'services').exists(), result.stdout + result.stderr)
            result = subprocess.run(arguments, env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            rows = [json.loads(line) for line in (root / 'paths.jsonl').read_text().splitlines()]
            self.assertEqual(len(rows), 3)
            for name in names:
                base = pathlib.Path(environment[name])
                selected = [pathlib.Path(row[name]) for row in rows if row[name] is not None]
                self.assertEqual(len(selected), len(set(selected)), name)
                for path in selected:
                    self.assertNotEqual(path, base)
                    self.assertTrue(path.is_relative_to(base))
                    self.assertFalse(path.exists(), 'only owned child directories are removed')
                self.assertEqual((base / 'sentinel').read_text(), 'preserved')
            self.assertIn('http://127.0.0.1:28000/ready', (root / 'http').read_text().splitlines())
            service_calls = (root / 'services').read_text()
            self.assertIn('images', service_calls)
            self.assertIn('/surreal version', service_calls)

if __name__ == "__main__":
    unittest.main()
