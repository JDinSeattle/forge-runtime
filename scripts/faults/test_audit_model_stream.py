import copy
import importlib.util
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    'model_stream_audit', ROOT / 'benchmarks/audit_model_stream_fault.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
FIXTURES = {
    'openai': ROOT / 'benchmarks/results/model-stream-interrupted-openai-20260911T202817.952133210Z',
    'anthropic': ROOT / 'benchmarks/results/model-stream-interrupted-anthropic-20260911T202826.363552405Z',
}


class ModelStreamAuditTests(unittest.TestCase):
    def check_mutation_rejected(self, mutate):
        for native, directory in FIXTURES.items():
            with self.subTest(native=native):
                report = json.loads((directory / 'report.json').read_bytes())
                changed = copy.deepcopy(report)
                mutate(changed)
                with self.assertRaises(AssertionError):
                    audit.audit(directory, changed)

    def test_actual_native_evidence_is_accepted(self):
        for native, directory in FIXTURES.items():
            with self.subTest(native=native):
                report = json.loads((directory / 'report.json').read_bytes())
                result = audit.audit(directory, report)
                self.assertTrue(result['passed'])
                self.assertEqual(result['native'], native)
                self.assertEqual(result['effect_count'], 0)
                self.assertEqual(result['retained_microusd'], 10240)

    def test_attempt_from_another_run_is_rejected(self):
        def mutate(report):
            for snapshot in report['snapshots']:
                snapshot['attempts'][0]['run_id'] = 'run_unrelated'
        self.check_mutation_rejected(mutate)

    def test_fake_model_ledger_for_native_request_is_rejected(self):
        def mutate(report):
            for snapshot in report['snapshots']:
                attempt = snapshot['attempts'][0]
                attempt['provider'] = attempt['model_id'] = 'fake'
                attempt['pricing']['provider'] = attempt['pricing']['model'] = 'fake'
        self.check_mutation_rejected(mutate)

    def test_unrelated_quota_group_is_rejected(self):
        def mutate(report):
            for snapshot in report['snapshots']:
                snapshot['quotas'][0]['credential_group'] = 'unrelated-provider-account'
        self.check_mutation_rejected(mutate)

    def test_wire_without_observed_text_is_rejected(self):
        def mutate(report):
            report['wire_requests'][0]['frames'] = [
                frame for frame in report['wire_requests'][0]['frames']
                if frame['type'] != 'response.output_text.delta'
                and not (frame['type'] == 'content_block_delta'
                         and frame.get('delta', {}).get('type') == 'text_delta')]
        self.check_mutation_rejected(mutate)

    def test_observed_tool_payload_not_sent_on_wire_is_rejected(self):
        def mutate(report):
            for event in report['adapter']['events']:
                if event['type'] == 'model.tool_arguments_delta':
                    event['tool_name'] = 'unrelated_tool'
                    event['delta'] = '{"unrelated":"'
        self.check_mutation_rejected(mutate)

    def test_durable_failure_for_another_attempt_is_rejected(self):
        def mutate(report):
            for snapshot in report['snapshots']:
                for event in snapshot['events']:
                    if event['type'] == 'model.attempt_failed':
                        event['payload']['attempt_id'] = 'attempt_unrelated'
        self.check_mutation_rejected(mutate)

    def test_summary_cannot_claim_unexpired_database_lease_expired(self):
        def mutate(report):
            for snapshot in report['snapshots'][1:]:
                snapshot['run']['lease_until'] = '2026-09-12T20:28:21+00:00'
                snapshot['run']['snapshot']['lease']['until'] = '2026-09-12T20:28:21+00:00'
        self.check_mutation_rejected(mutate)

    def test_summary_cannot_invent_second_claim_owner_or_epoch(self):
        def mutate(report):
            for snapshot in report['snapshots'][1:]:
                snapshot['run']['lease_owner'] = 'f03-first-worker'
                snapshot['run']['lease_epoch'] = 1
                snapshot['run']['snapshot']['lease']['owner'] = 'f03-first-worker'
                snapshot['run']['snapshot']['lease']['epoch'] = 1
        self.check_mutation_rejected(mutate)


if __name__ == '__main__':
    unittest.main()
