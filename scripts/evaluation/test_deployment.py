#!/usr/bin/python3
"""Offline collector authority tests; synthetic files/DSNs, no sockets or keys."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).absolute().parent
spec = importlib.util.spec_from_file_location('evaluation_tests', HERE / 'test_evaluation.py')
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
evaluate = fixtures.evaluate
d = evaluate.deployment


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / 'bundle'
        self.bundle.mkdir()
        self.output = self.root / 'eval-01'
        self.cli = self.root / 'forge'
        self.cli.write_bytes(b'offline identity fixture; never executed')
        self.cli.chmod(0o700)
        self.registration = fixtures.registration()
        self.registration.update(provider='deepseek', model_id='deepseek-v4-flash', total_budget_microusd=2000000, task_budgets_microusd={case: 500000 for case in fixtures.common.CASES})
        self.seal = {'registration_sha256': 'b' * 64, 'corpus_sha256': 'c' * 64}
        (self.bundle / 'candidate.json').write_text(json.dumps(self.seal))
        self.value = {'version': 1, 'purpose': 'isolated-eval-v1', 'authorization_id': 'deepseek-flash-2usd', 'output_dir': str(self.output), 'bundle_dir': str(self.bundle), 'candidate_sha256': d.digest(self.bundle / 'candidate.json'), 'registration_sha256': 'b' * 64, 'corpus_sha256': 'c' * 64, 'provider': 'deepseek', 'model_id': 'deepseek-v4-flash', 'total_budget_microusd': 2000000, 'tenant': 'eval_fixture', 'api_url': 'http://127.0.0.1:19097', 'database': {'host': '127.0.0.1', 'port': 32773, 'name': 'forge', 'schema': 'eval_ds_0123456789abcdef', 'schema_oid': 123456, 'audit_role': 'eval_audit_fixture'}, 'cli': {'path': str(self.cli), 'sha256': d.digest(self.cli)}}
        self.descriptor = self.root / 'deployment.json'
        self.write_descriptor()
        self.scope = {'database': 'forge', 'schema': self.value['database']['schema'], 'schema_oid': 123456, 'current_user': 'eval_audit_fixture', 'session_user': 'eval_audit_fixture', 'read_only': True, 'select_allowed': True, 'dml_allowed': False, 'unsafe_role': False}
        self.dsn = 'postgres://eval_audit_fixture:synthetic-not-a-credential@127.0.0.1:32773/forge?sslmode=disable'

    def write_descriptor(self):
        self.descriptor.write_text(json.dumps(self.value))
        self.descriptor.chmod(0o600)

    def binding(self):
        return d.load(self.descriptor, self.bundle, self.output, self.seal, self.registration)

    def test_fixed_bundle_output_and_full_descriptor_hash(self):
        binding = self.binding()
        self.assertEqual(binding['sha256'], hashlib.sha256(self.descriptor.read_bytes()).hexdigest())
        self.assertFalse(self.output.exists())
        for bundle, output in ((self.bundle, self.root / 'another' / 'eval-01'), (self.root, self.output)):
            with self.assertRaises(ValueError):
                d.load(self.descriptor, bundle, output, self.seal, self.registration)
        changed = copy.deepcopy(self.registration)
        changed['task_budgets_microusd'][fixtures.common.CASES[0]] -= 1
        with self.assertRaises(ValueError):
            d.load(self.descriptor, self.bundle, self.output, self.seal, changed)
        (self.bundle / 'candidate.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'binding changed'):
            self.binding()

    def test_strict_fields_types_aliases_and_authorization(self):
        mutations = [lambda x: x.update(api_key='forbidden synthetic field'), lambda x: x.pop('output_dir'), lambda x: x.update(version=True), lambda x: x.update(total_budget_microusd=2000001), lambda x: x.update(provider='openai'), lambda x: x.update(model_id='deepseek-flash'), lambda x: x.update(output_dir=str(self.root / 'eval-02')), lambda x: x['database'].update(schema='public'), lambda x: x['database'].update(schema='eval_ds_x;DROP'), lambda x: x['database'].update(schema_oid=True), lambda x: x['database'].update(port=5432), lambda x: x['database'].update(audit_role='role";SELECT'), lambda x: x['database'].update(password='forbidden'), lambda x: x.update(api_url='http://127.0.0.1:19097/'), lambda x: x.update(api_url='http://user:secret@127.0.0.1:19097'), lambda x: x.update(api_url='http://127.0.0.1:19097?unused=1')]
        for mutate in mutations:
            candidate = copy.deepcopy(self.value)
            mutate(candidate)
            with self.subTest(candidate=candidate), self.assertRaises((ValueError, TypeError)):
                d.validate(candidate)
        self.descriptor.write_text(json.dumps(self.value)[:-1] + ',"tenant":"duplicate"}')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            self.binding()

    def test_symlink_public_file_binary_swap_rejected(self):
        self.descriptor.chmod(0o644)
        with self.assertRaises(ValueError):
            self.binding()
        self.descriptor.chmod(0o600)
        alias = self.root / 'alias'
        alias.symlink_to(self.bundle, target_is_directory=True)
        self.value['bundle_dir'] = str(alias)
        with self.assertRaises(ValueError):
            d.validate(self.value)
        self.cli.write_bytes(b'swapped executable')
        self.write_descriptor()
        with self.assertRaises(ValueError):
            self.binding()

    def test_audit_dsn_exact_role_port_and_minimal_environment(self):
        binding = self.binding()
        with patch.dict(os.environ, {'FORGE_EVAL_AUDIT_DSN': self.dsn, 'DEEPSEEK_API_KEY': 'must-not-forward', 'PGSERVICE': 'foreign', 'PGOPTIONS': '-c search_path=public', 'OPENAI_API_KEY': 'must-not-forward'}, clear=True):
            env = evaluate.audit_environment(binding)
        self.assertEqual(set(env), {'PATH', 'LC_ALL', 'TMPDIR', 'PGCONNECT_TIMEOUT', 'PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD', 'PGSSLMODE', 'PGOPTIONS'})
        self.assertEqual(env['PGPORT'], '32773')
        self.assertNotIn('public', env['PGOPTIONS'])
        for bad in (self.dsn.replace(':32773', ':5432'), self.dsn.replace('eval_audit_fixture:', 'other:'), self.dsn + '&sslmode=disable', self.dsn + '#fragment', self.dsn.replace('sslmode=disable', 'search_path=public'), self.dsn.replace('sslmode=disable', '%73slmode=disable')):
            with patch.dict(os.environ, {'FORGE_EVAL_AUDIT_DSN': bad}), self.assertRaises(ValueError):
                evaluate.audit_environment(binding)

    def test_schema_oid_role_proof_fails_before_cli_submission(self):
        binding = self.binding()
        d.check_scope(self.scope, binding)
        for key, value in (('schema', 'public'), ('schema_oid', 654321), ('current_user', 'other'), ('session_user', 'other'), ('database', 'other'), ('read_only', False), ('dml_allowed', True), ('unsafe_role', True), ('select_allowed', False)):
            bad = dict(self.scope, **{key: value})
            with patch.dict(os.environ, {'FORGE_EVAL_AUDIT_DSN': self.dsn}), patch.object(evaluate.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(bad), '')) as run, self.assertRaises(ValueError):
                evaluate.audit_preflight(binding)
            self.assertEqual(run.call_args.args[0][0], 'psql')
            self.assertIn('schema=' + self.value['database']['schema'], run.call_args.args[0])

    def test_audit_uses_qualified_identifiers_and_rejects_wrong_scope_or_run(self):
        binding = self.binding()
        ledger = {'scope': self.scope, 'run': {'id': 'run_fixture', 'tenant_id': 'eval_fixture'}, 'attempts': [], 'reservations': []}
        for wrong in ('none', 'schema', 'tenant', 'run'):
            raw = copy.deepcopy(ledger)
            if wrong == 'schema': raw['scope']['schema'] = 'public'
            if wrong == 'tenant': raw['run']['tenant_id'] = 'other'
            if wrong == 'run': raw['run']['id'] = 'other'
            with patch.dict(os.environ, {'FORGE_EVAL_AUDIT_DSN': self.dsn}), patch.object(evaluate.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(raw), '')) as run:
                if wrong == 'none':
                    self.assertEqual(evaluate.audit_run('eval_fixture', 'run_fixture', self.root, binding), ledger)
                else:
                    with self.assertRaises(ValueError): evaluate.audit_run('eval_fixture', 'run_fixture', self.root, binding)
            query = run.call_args.kwargs['input']
            for table in d.TABLES:
                self.assertIn(':"schema".' + table, query)
            self.assertNotIn('__AUDIT_SCOPE__', query)
            self.assertNotIn('FROM public.', query)
        with patch.object(evaluate.subprocess, 'run') as run, self.assertRaises(ValueError):
            evaluate.audit_run('other', 'run_fixture', self.root, binding)
        run.assert_not_called()

    def test_cli_uses_pinned_binary_only_and_never_inherits_keys(self):
        binding = self.binding()
        client = self.root / 'client.env'
        client.write_text('FORGE_API_URL=http://127.0.0.1:19097\nFORGE_TENANT=eval_fixture\nFORGE_TOKEN=synthetic-local-cli-token\n')
        client.chmod(0o600)
        with patch.dict(os.environ, {'DEEPSEEK_API_KEY': 'must-not-forward', 'FORGE_EVAL_AUDIT_DSN': self.dsn, 'FORGE_DATABASE_URL': 'must-not-forward'}, clear=True):
            cli = evaluate.CLI(client, self.output, binding)
        self.assertEqual(set(cli.env), {'PATH', 'LC_ALL', 'TMPDIR', 'FORGE_API_URL', 'FORGE_TENANT', 'FORGE_TOKEN'})
        self.assertEqual(cli.command[0], str(self.cli))
        self.cli.write_bytes(b'replaced after initialization')
        with patch.object(evaluate.subprocess, 'run') as run, self.assertRaises(ValueError):
            cli.call('run', 'submit')
        run.assert_not_called()
        client.write_text(client.read_text().replace(':19097', ':8097'))
        with self.assertRaises(ValueError): evaluate.CLI(client, self.output, binding)

    def test_missing_duplicate_and_orphan_reservations_never_report_zero(self):
        r = fixtures.registration()
        quote = dict(r['model_spec'], schema_version=1, provider=r['provider'], model=r['model_id'])
        attempt = {'attempt_id': 'one', 'provider': r['provider'], 'model_id': r['model_id'], 'pricing': quote}
        ledger = {'run': {'config_snapshot': {'provider': r['provider'], 'model': r['model_id']}}, 'attempts': [attempt], 'reservations': []}
        for rows in ([], [{'id': 'one'}, {'id': 'one'}], [{'id': 'other'}]):
            ledger['reservations'] = rows
            with self.assertRaises(ValueError): evaluate.summarize_ledger(ledger, r)

    def test_reservation_group_or_unknown_money_cannot_disappear(self):
        r = fixtures.registration()
        quote = dict(r['model_spec'], schema_version=1, provider=r['provider'], model=r['model_id'])
        row = {'attempt_id': 'one', 'provider': r['provider'], 'model_id': r['model_id'], 'pricing': quote, 'step_seq': 1, 'attempt': 1, 'status': 'failed', 'error_code': 'timeout', 'request_id': None, 'usage': None, 'response_persisted_at': None}
        reservation = {'id': 'one', 'credential_group': 'unit_test', 'status': 'unknown', 'actual_microusd': None, 'microusd': 99, 'dispatched_at': '2026-09-11T00:00:00Z'}
        ledger = {'run': {'config_snapshot': {'provider': r['provider'], 'model': r['model_id']}}, 'attempts': [row], 'reservations': [reservation]}
        self.assertEqual(evaluate.summarize_ledger(ledger, r)['unknown_or_unsettled_reserved_microusd'], 99)
        for changes in ({'credential_group': 'other'}, {'microusd': None}, {'microusd': -1}, {'microusd': True}, {'status': 'settled'}, {'status': 'discarded'}):
            bad = copy.deepcopy(ledger)
            bad['reservations'][0].update(changes)
            with self.assertRaises(ValueError): evaluate.summarize_ledger(bad, r)

    def test_collect_cannot_drop_or_replace_binding_and_execute_is_single_output(self):
        binding = self.binding()
        manifest = {'sha256': 'c' * 64}
        bundle_result = (manifest, self.seal, self.registration, {})
        args = ['evaluate.py', '--bundle', str(self.bundle), '--output', str(self.output), '--client-env', str(self.root / 'client.env')]
        self.output.mkdir(mode=0o700)
        batch = {'seal': self.seal, 'registration': self.registration, 'deployment_binding': binding}
        (self.output / 'batch.json').write_text(json.dumps(batch))
        with patch.object(evaluate, 'load_bundle', return_value=bundle_result), patch.object(evaluate, 'audit_environment'), patch.object(evaluate, 'CLI') as cli:
            for mode in (['--collect'], ['--deployment', str(self.descriptor), '--execute']):
                with patch.object(sys, 'argv', args + mode), self.assertRaises((ValueError, FileExistsError)):
                    evaluate.main()
                cli.assert_not_called()
            batch['deployment_binding']['descriptor']['tenant'] = 'changed'
            (self.output / 'batch.json').write_text(json.dumps(batch))
            with patch.object(sys, 'argv', args + ['--deployment', str(self.descriptor), '--collect']), self.assertRaises(ValueError):
                evaluate.main()
            cli.assert_not_called()


if __name__ == '__main__':
    unittest.main()
