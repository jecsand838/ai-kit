import json
import contextlib
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from release_qualification import ReleaseQualification
from release_evidence import Command, EvidenceError, JsonFile, RuntimeIdentity


class ReleaseQualificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / 'bundle.json'
        self.spec = {'schema': 1, 'root': str(self.root), 'compiler': 'rustc test',
                     'branch': 'codex/test', 'stages': {}}
        JsonFile.write(self.bundle, self.spec)
        self.git_patch = patch.object(ReleaseQualification, 'git', side_effect=self.git)
        self.git_patch.start()
        self.addCleanup(self.git_patch.stop)
        self.compiler_patch = patch('release_qualification.Compiler.read', return_value='rustc test')
        self.compiler_patch.start()
        self.addCleanup(self.compiler_patch.stop)
        self.source_patch = patch('release_qualification.SourceIdentity.capture', return_value={'sha256': 'unchanged', 'files': {}})
        self.source_patch.start()
        self.addCleanup(self.source_patch.stop)

    @staticmethod
    def git(*arguments):
        if arguments[0] == 'branch':
            return 'codex/test'
        if arguments[0] == 'rev-parse':
            return 'revision'
        return ''

    def test_foreign_branch_rejected(self):
        self.spec['branch'] = 'main'
        self.bundle.write_text(json.dumps(self.spec))
        with self.assertRaisesRegex(EvidenceError, 'branch'):
            ReleaseQualification(self.root, self.bundle)

    def test_different_compiler_rejected(self):
        self.spec['compiler'] = 'older compiler'
        self.bundle.write_text(json.dumps(self.spec))
        with self.assertRaisesRegex(EvidenceError, 'compiler'):
            ReleaseQualification(self.root, self.bundle)

    def test_failed_owner_does_not_skip_independent_owner(self):
        runner = ReleaseQualification(self.root, self.bundle)
        calls = []
        def execute(command, root, output, timeout, cleanup_timeout, stream):
            self.assertGreaterEqual(cleanup_timeout, 30)
            calls.append(command)
            output.write(b'complete\n')
            return 1 if len(calls) == 2 else 0
        with patch('release_qualification.OwnedProcess.run', side_effect=execute):
            self.assertEqual(runner.run(), 1)
        report = JsonFile.read(runner.directory / 'report.json')
        self.assertEqual([entry['status'] for entry in report['commands']], ['PASS', 'FAIL', 'PASS'])
        self.assertEqual(len(calls), 3)
        self.assertEqual(report['status'], 'FAIL')

    def test_success_still_requires_final_clean_checkout(self):
        runner = ReleaseQualification(self.root, self.bundle)
        with patch('release_qualification.OwnedProcess.run', return_value=0):
            self.assertEqual(runner.run(), 0)
        report = JsonFile.read(runner.directory / 'report.json')
        self.assertEqual(report['clean_checkout'], 'REQUIRED_SEPARATELY_AT_FINAL_COMMIT')
        self.assertEqual(report['status'], 'PASS')

    def test_same_status_but_changed_content_invalidates_receipt(self):
        runner = ReleaseQualification(self.root, self.bundle)
        with patch('release_qualification.OwnedProcess.run', return_value=0), patch(
                'release_qualification.SourceIdentity.capture', side_effect=[{'sha256':'before'}, {'sha256':'after'}]):
            self.assertEqual(runner.run(), 1)
        self.assertEqual(JsonFile.read(runner.directory / 'report.json')['status'], 'INCOMPLETE')

    def test_environment_restored_after_failed_execution(self):
        runner = ReleaseQualification(self.root, self.bundle)
        with patch.dict(os.environ, {'CARGO_BUILD_JOBS': 'sentinel'}), patch(
                'release_qualification.OwnedProcess.run', side_effect=OSError('capture failed')):
            self.assertEqual(runner.run(), 1)
            self.assertEqual(os.environ['CARGO_BUILD_JOBS'], 'sentinel')
        report = JsonFile.read(runner.directory / 'report.json')
        self.assertTrue(all(entry['status'] == 'INCOMPLETE' for entry in report['commands']))
        self.assertEqual(report['status'], 'INCOMPLETE')

    def test_failed_bootstrap_cannot_authorize_cargo_or_reuse(self):
        runner = ReleaseQualification(self.root, self.bundle)
        with patch('release_qualification.OwnedProcess.run', return_value=1) as execute:
            self.assertEqual(runner.run(), 1)
        self.assertEqual(execute.call_count, 1)
        report = JsonFile.read(runner.directory / 'report.json')
        self.assertEqual(report['status'], 'FAIL')
        self.assertEqual(len(report['commands']), 1)

    def test_interruption_settles_then_stops_before_next_owner(self):
        runner = ReleaseQualification(self.root, self.bundle)
        with patch('release_qualification.OwnedProcess.run', side_effect=KeyboardInterrupt()) as execute:
            with self.assertRaises(KeyboardInterrupt):
                runner.run()
        self.assertEqual(execute.call_count, 1)
        report = JsonFile.read(runner.directory / 'report.json')
        self.assertEqual(report['status'], 'INCOMPLETE')
        self.assertEqual(len(report['commands']), 1)
        self.assertEqual(report['commands'][0]['status'], 'INCOMPLETE')

    def test_existing_coordinator_rejected(self):
        import fcntl
        runner = ReleaseQualification(self.root, self.bundle)
        runner.base.mkdir(parents=True)
        with (runner.base / 'coordinator.lock').open('a') as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(EvidenceError, 'another release coordinator'):
                runner.run()

    def test_explicit_nonlive_request_is_recorded_without_release_claim(self):
        runner = ReleaseQualification(self.root, self.bundle, skip_strict_live=True)
        with patch('release_qualification.OwnedProcess.run', return_value=0):
            self.assertEqual(runner.run(), 0)
        report = JsonFile.read(runner.directory / 'report.json')
        self.assertEqual(report['scope'], 'QUALIFIED_NONLIVE')
        self.assertEqual(len(report['excluded_live_targets']), 5)
        self.assertIn('STRICT_LIVE_DEFERRED_BY_USER', report['qualification'])
        self.assertEqual(report['commands'][-1]['command'][-1], '--skip-strict-live')

    def test_no_partial_provider_scope_or_storage_all_features(self):
        commands = ReleaseQualification.commands()
        self.assertEqual(commands[0][0], 'release-runner-contracts')
        self.assertEqual(commands[1][1], ['bash', 'storage-kit/scripts/check.sh', '--all-backends'])
        self.assertEqual(commands[2][1], ['bash', 'scripts/verify_workspace_strict.sh'])

    def storage_certificate(self):
        """Actual receipt validation over isolated, clearly synthetic evidence."""
        evidence = self.root / 'evidence'
        evidence.mkdir()
        review = evidence / 'review.json'
        JsonFile.write(review, {'fixture': 'not release qualification evidence'})
        def reference(path):
            return {'path': str(path), 'sha256': JsonFile.digest(path)}
        def receipt(ident, command):
            kind = Command.kind(command)
            inventory = ['case'] if kind == 'tests' else None
            counts = None
            if kind == 'tests':
                output = 'running 1 test\ntest case ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n'
                counts = {'passed': 1, 'failed': 0, 'ignored': 0, 'measured': 0, 'filtered': 0}
            elif kind == 'python-tests':
                output = 'Ran 1 test in 0.001s\n\nOK\n'
                counts = {'passed': 1, 'failed': 0, 'ignored': 0, 'filtered': 0}
            else:
                output = 'completed fixture command\n'
            log = evidence / (ident + '.log')
            log.write_text(output)
            source = {'sha256': 'unchanged', 'files': {}}
            data = {'schema': 1, 'stage_id': ident, 'command': command, 'cwd': '.', 'compiler': 'rustc test',
                    'kind': kind, 'source_before': source, 'source_after': source, 'runtime': {},
                    'build_environment': RuntimeIdentity.capture(Command.build_names(command), Command.environment(command)),
                    'returncode': 0, 'status': 'PASS', 'cleanup_settled': True, 'execution': 'EXECUTED',
                    'inventory': inventory, 'counts': counts, 'log': reference(log)}
            path = evidence / (ident + '.json')
            JsonFile.write(path, data)
            return path, data
        lanes, legal, negative, stages = ReleaseQualification.storage_inventory()
        nested = {}
        for ident, command in stages.items():
            path, data = receipt(ident, command)
            definition = {'command': command, 'reuse': {'receipt': str(path), 'sha256': JsonFile.digest(path)}}
            self.spec['stages'][ident] = definition
            nested[ident] = {'definition': definition, 'receipt': reference(path)}
        checks = []
        command = ['bash', 'storage-kit/scripts/check.sh', '--all-backends']
        owner_path, owner = receipt('storage.owner.local_matrix', command)
        owner_log = Path(owner['log']['path'])
        owner_log.write_text(''.join('STORAGE_NEGATIVE_FEATURE_GRADE=' + json.dumps(JsonFile.read(item['receipt']['path'])) + '\n' for item in checks))
        owner['log'] = reference(owner_log)
        report_path = evidence / 'original-report.json'
        JsonFile.write(report_path, {'source_before': {'sha256': 'original'}, 'source_after': {'sha256': 'original'},
                         'revision': 'original-commit', 'revision_after': 'original-commit',
                         'working_tree_before': '', 'working_tree_after': '',
                         'commands': [{'name': 'storage-local-matrix', 'command': command,
                         'status': 'PASS', 'returncode': 0, 'cleanup_settled': True, 'log': owner['log']}]})
        owner.update(execution='IMPORTED', owner_proof={'schema': 1, 'original_run_report': reference(report_path),
                     'original_owner_name': 'storage-local-matrix', 'lanes': lanes, 'legal': legal,
                     'negative': checks, 'nested_stages': nested},
                     origin={'artifact': reference(review), 'reviewer': 'fixture-reviewer', 'description': 'synthetic fixture',
                             'cleanup_evidence': {'settled': True, 'basis': 'observed_process_group_settlement',
                                                  'reason': 'fixture', 'artifacts': [reference(report_path)]}})
        self.save_owner(owner_path, owner)
        return owner_path, owner

    def save_owner(self, path, owner):
        JsonFile.write(path, owner)
        self.spec['stages']['storage.owner.local_matrix'] = {
            'command': ['bash', 'storage-kit/scripts/check.sh', '--all-backends'],
            'reuse': {'receipt': str(path), 'sha256': JsonFile.digest(path)}}
        JsonFile.write(self.bundle, self.spec)

    def test_storage_owner_inventory_matches_existing_feature_policy(self):
        path = SCRIPTS.parent / 'storage-kit/scripts/feature_policy.py'
        spec = importlib.util.spec_from_file_location('storage_policy', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        policy = module.StorageSelection(self.root)
        policy.add(policy.BACKENDS, 'fixture complete request')
        lanes, legal, negative, stages = ReleaseQualification.storage_inventory()
        render = lambda values: [','.join(parts) or '-' for parts in values]
        self.assertEqual(lanes, render(policy.result(True)['lanes']))
        self.assertEqual(legal, render(policy.matrix(True)))
        self.assertEqual(negative, render(policy.matrix(False)))
        self.assertEqual((len(lanes), len(legal), len(negative), len(stages)), (6, 16, 0, 113))

    def test_complete_storage_owner_reuses_only_after_fresh_bootstrap(self):
        owner_path, _ = self.storage_certificate()
        runner = ReleaseQualification(self.root, self.bundle)
        with contextlib.chdir(self.root), patch('release_qualification.OwnedProcess.run', return_value=0) as execute:
            self.assertEqual(runner.run(), 0)
        self.assertEqual([call.args[0] for call in execute.call_args_list],
                         [runner.commands()[0][1], runner.commands()[2][1]])
        entries = JsonFile.read(runner.directory / 'report.json')['commands']
        self.assertEqual(entries[0]['execution'], 'EXECUTED')
        self.assertEqual(entries[1]['execution'], 'REUSED')
        self.assertEqual(entries[1]['reused_from']['path'], str(owner_path))
        self.assertEqual(entries[1]['completion']['negative_feature_checks'], 0)
        self.assertEqual(entries[1]['completion']['nested_stages'], 113)

    def test_bootstrap_failure_prevents_storage_certificate_admission(self):
        self.storage_certificate()
        runner = ReleaseQualification(self.root, self.bundle)
        with patch('release_qualification.OwnedProcess.run', return_value=1), patch.object(runner, 'storage_reuse') as admission:
            self.assertEqual(runner.run(), 1)
        admission.assert_not_called()

    def test_declared_invalid_storage_owner_is_incomplete_without_silent_rerun(self):
        variants = ('obsolete_negative', 'incomplete_lanes', 'incomplete_legal', 'wrong_owner', 'malformed_proof',
                    'failed_nested', 'source_mismatch', 'tampered_log', 'missing_nested', 'changed_original_source')
        for variant in variants:
            with self.subTest(variant=variant):
                # Each scenario has independent receipt paths and cannot retry a failed run.
                with tempfile.TemporaryDirectory() as directory:
                    previous_root, previous_bundle, previous_spec = self.root, self.bundle, self.spec
                    self.root = Path(directory)
                    self.bundle = self.root / 'bundle.json'
                    self.spec = {'schema': 1, 'root': str(self.root), 'compiler': 'rustc test', 'branch': 'codex/test', 'stages': {}}
                    try:
                        path, owner = self.storage_certificate()
                        proof = owner['owner_proof']
                        if variant == 'obsolete_negative':
                            proof['negative'].append({'obsolete': True})
                        elif variant == 'incomplete_lanes':
                            proof['lanes'].pop()
                        elif variant == 'incomplete_legal':
                            proof['legal'].pop()
                        elif variant == 'wrong_owner':
                            proof['original_owner_name'] = 'workspace-wrapper'
                        elif variant == 'malformed_proof':
                            owner['owner_proof'] = []
                        elif variant == 'missing_nested':
                            proof['nested_stages'].pop('storage.check.lance')
                        elif variant == 'source_mismatch':
                            owner['source_before'] = {'sha256': 'changed', 'files': {}}
                        elif variant == 'tampered_log':
                            Path(owner['log']['path']).write_text('altered')
                        elif variant == 'changed_original_source':
                            original_report = JsonFile.read(proof['original_run_report']['path'])
                            original_report['source_after'] = {'sha256': 'changed'}
                            JsonFile.write(proof['original_run_report']['path'], original_report)
                            proof['original_run_report']['sha256'] = JsonFile.digest(proof['original_run_report']['path'])
                            owner['origin']['cleanup_evidence']['artifacts'][0] = proof['original_run_report']
                        else:
                            item = proof['nested_stages']['storage.test.lance.public_api']
                            bad = JsonFile.read(item['receipt']['path'])
                            bad['status'] = 'FAIL'
                            JsonFile.write(item['receipt']['path'], bad)
                            item['receipt']['sha256'] = JsonFile.digest(item['receipt']['path'])
                            item['definition']['reuse']['sha256'] = item['receipt']['sha256']
                        self.save_owner(path, owner)
                        runner = ReleaseQualification(self.root, self.bundle)
                        with contextlib.chdir(self.root), patch('release_qualification.OwnedProcess.run', return_value=0) as execute:
                            self.assertEqual(runner.run(), 1)
                        report = JsonFile.read(runner.directory / 'report.json')
                        self.assertEqual([entry['status'] for entry in report['commands']], ['PASS', 'INCOMPLETE', 'PASS'])
                        self.assertEqual(report['status'], 'INCOMPLETE')
                        self.assertEqual(execute.call_count, 2)
                    finally:
                        self.root, self.bundle, self.spec = previous_root, previous_bundle, previous_spec


if __name__ == '__main__':
    unittest.main()
