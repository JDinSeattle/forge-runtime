"""Offline positive and mutation tests for immutable batch and case identity."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location('binding_fixtures', HERE / 'test_deployment.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
e = f.evaluate
c = e.common


class CollectionBindingTests(unittest.TestCase):
    def test_batch_envelope_rejects_every_redundant_authority_substitution(self):
        r = f.fixtures.registration()
        manifest, seal = {'sha256': 'a'*64}, {'registration_sha256': 'b'*64}
        binding = {'descriptor': {'total_budget_microusd': 4000}, 'sha256': 'd'*64}
        batch = e.batch_envelope(manifest, seal, r, binding, '2026-09-12T00:00:00Z')
        e.validate_batch(batch, manifest, seal, r, binding)
        changes = {'total_budget_microusd': 999999999, 'allocated_total_microusd': 999999999, 'corpus_sha256': 'f'*64, 'task_order': list(reversed(c.CASES)), 'created_at': '2026-09-12', 'unexpected': True, 'deployment_binding': None}
        for key, value in changes.items():
            bad = dict(batch, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                e.validate_batch(bad, manifest, seal, r, binding)
        bad = copy.deepcopy(batch)
        bad.pop('created_at')
        with self.assertRaises(ValueError): e.validate_batch(bad, manifest, seal, r, binding)

    def test_cli_receipt_body_uses_production_submit_json(self):
        source = {'hash': 'a'*64, 'path': str(c.CORPUS/c.CASES[0]/'source'), 'profile_id': c.SOURCE_PREFIX+c.CASES[0]}
        body = e.submit_body(c.CASES[0], source, f.fixtures.registration())
        body['task'] += '\n<>& Ω\u2028\u2029'
        raw = c.local_output(['go', 'run', str(HERE/'hash-sources.go'), '--submit-json'], input=json.dumps(body), cwd=c.REPO, env=c.go_environment())
        self.assertEqual(e.cli_body_bytes(body)+b'\n', raw.encode())

    def test_valid_case_and_each_saved_or_authoritative_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case = c.CASES[0]
            directory = root/case
            directory.mkdir()
            registration = f.fixtures.registration()
            source = {'path': str(c.CORPUS/case/'source'), 'hash': 'a'*64, 'profile_id': c.SOURCE_PREFIX+case}
            tenant, project_id, run_id = 'tenant_fixture', 'project_expected', 'run_expected'
            body = e.submit_body(case, source, registration)
            project = {'id': project_id, 'tenant_id': tenant, 'source_id': c.SOURCE_PREFIX+case, 'profile_id': source['profile_id']}
            intent = {'project_id': project_id, 'budget_microusd': 1000, 'idempotency_key': 'fixed-key', 'submitted_at': '2026-09-12T00:00:00Z'}
            submitted = {'run_id': run_id, 'reused': False}
            class CLI:
                env = {'FORGE_API_URL': 'http://127.0.0.1:19097'}
            cli = CLI()
            cli.tenant = tenant
            scope = cli.env['FORGE_API_URL']+'\n'+tenant+'\n/v1/projects/'+project_id+'/runs\n'
            receipt_dir = root/'cli-receipts'
            receipt_dir.mkdir()
            receipt_path = receipt_dir/('key-'+hashlib.sha256((scope+'fixed-key').encode()).hexdigest()+'.json')
            receipt = {'key': 'fixed-key', 'body_hash': hashlib.sha256(scope.encode()+e.cli_body_bytes(body)).hexdigest(), 'created_at': intent['submitted_at'], 'response': submitted}
            files = {'project.json': project, 'submission-intent.json': intent, 'submission.json': submitted}
            for name, value in files.items(): (directory/name).write_text(json.dumps(value))
            receipt_path.write_text(json.dumps(receipt))
            expected = e.submission_binding(cli, case, directory, registration, source)
            config = dict(body['budget'], provider=registration['provider'], model=registration['model_id'])
            run = dict(expected, config=config, state={'status': 'completed', 'run_id': run_id, 'tenant_id': tenant})
            admission = {'schema_version': 1, 'project_id': project_id, 'task': body['task'], 'base_commit': source['hash'], 'source_id': c.SOURCE_PREFIX+case, 'profile_id': source['profile_id']}
            sql = dict(expected, state='completed', config_snapshot=config, input_snapshot=json.dumps(admission))
            e.check_collected_input(run, {'run': sql}, expected, case, source, registration)
            for name, key, changed in [('project.json', 'source_id', c.SOURCE_PREFIX+c.CASES[1]), ('project.json', 'id', 'project_other'), ('project.json', 'tenant_id', 'other'), ('submission-intent.json', 'budget_microusd', 999999), ('submission-intent.json', 'project_id', 'project_other'), ('submission-intent.json', 'idempotency_key', 'other-key'), ('submission.json', 'run_id', 'run_other')]:
                altered = dict(files[name], **{key: changed})
                (directory/name).write_text(json.dumps(altered))
                with self.subTest(saved=name, key=key), self.assertRaises((ValueError, FileNotFoundError)):
                    e.submission_binding(cli, case, directory, registration, source)
                (directory/name).write_text(json.dumps(files[name]))
            for which, key, value in [('get', 'project_id', 'other'), ('sql', 'project_id', 'other'), ('get', 'task', 'different task'), ('sql', 'base_commit', 'b'*64), ('sql', 'id', 'other'), ('get', 'tenant_id', 'other')]:
                get, authoritative = copy.deepcopy(run), copy.deepcopy(sql)
                (get if which=='get' else authoritative)[key] = value
                with self.subTest(which=which, key=key), self.assertRaises(ValueError):
                    e.check_collected_input(get, {'run': authoritative}, expected, case, source, registration)
            for key in ['source_id', 'profile_id', 'task', 'project_id']:
                altered = dict(admission, **{key: 'other'})
                with self.subTest(input_snapshot=key), self.assertRaises(ValueError):
                    e.check_collected_input(run, {'run': dict(sql, input_snapshot=json.dumps(altered))}, expected, case, source, registration)
            bad = dict(receipt, body_hash='f'*64)
            receipt_path.write_text(json.dumps(bad))
            with self.assertRaises(ValueError): e.submission_binding(cli, case, directory, registration, source)

    def test_duplicate_run_ids_cannot_be_credited_to_two_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for case in c.CASES[:2]:
                (root/case).mkdir()
                (root/case/'submission.json').write_text(json.dumps({'run_id': 'same_run'}))
            with self.assertRaises(ValueError): e.unique_saved_runs(root)
            (root/c.CASES[1]/'submission.json').write_text(json.dumps({'run_id': 'different_run'}))
            e.unique_saved_runs(root)


if __name__=='__main__': unittest.main()
