import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('network_audit', HERE / 'audit-network.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
FIXTURES = HERE.parents[1] / 'benchmarks/results/application-network-20260911T203949Z'


class NetworkAuditTests(unittest.TestCase):
    def check_mutation_rejected(self, mode, file, mutate):
        directory = FIXTURES / mode
        original_read = audit.read

        def changed(path):
            value = original_read(path)
            if path == directory / file:
                mutate(value)
            return value

        with patch.object(audit, 'read', side_effect=changed):
            with self.assertRaises(AssertionError):
                audit.audit(directory)

    def test_actual_network_evidence_is_accepted(self):
        cases = {'F10_cancel_first': (27, 64), 'F10_complete_first': (26, 62),
                 'F11': (27, 65), 'F12': (27, 68)}
        for mode, (artifacts, events) in cases.items():
            with self.subTest(mode=mode):
                result = audit.audit(FIXTURES / mode)
                self.assertTrue(result['passed'])
                self.assertEqual(result['artifacts_checked'], artifacts)
                self.assertEqual(result['events_after_cleanup'], events)

    def test_f10_cancellation_transition_must_match_stop_proof(self):
        def mutate(value):
            for snapshot in value['run_snapshots']:
                event = snapshot.get('input_event') or {}
                if event.get('kind') == 'cancellation_confirmed':
                    event['expected_version'] = 1
                    event['epoch'] = 999
                    event['stop']['workspace_revision'] = 999
                    event['stop']['no_active_operations'] = False
        self.check_mutation_rejected('F10_cancel_first', 'cleanup-released-postgres.json', mutate)

    def test_f10_finalization_requires_original_version_and_lease(self):
        def mutate(value):
            for snapshot in value['run_snapshots']:
                event = snapshot.get('input_event') or {}
                if event.get('kind') == 'finalized':
                    event['expected_version'] = 1
                    event['epoch'] = 999
        self.check_mutation_rejected('F10_complete_first', 'cleanup-released-postgres.json', mutate)

    def test_f10_prior_settled_effect_cannot_change_with_cancellation(self):
        def mutate(value):
            for effect in value['effects']:
                if effect['operation_id'].endswith('step_1_op_0'):
                    effect['status'] = 'cancelled'
        self.check_mutation_rejected('F10_cancel_first', 'before-cancel-race-postgres.json', mutate)

    def test_f11_database_cut_cannot_hide_new_durable_event(self):
        def mutate(value):
            event = dict(value['run_events'][-1])
            event.update(seq=event['seq'] + 1, type='tool.planned',
                         payload={'operation_id': 'unpersisted-new-operation'})
            value['run_events'].append(event)
        self.check_mutation_rejected('F11', 'during-database-outage-postgres.json', mutate)

    def test_f12_unknown_allocation_cannot_be_released_despite_counters(self):
        def mutate(value):
            value['runner_allocations'][0]['state'] = 'released'
        self.check_mutation_rejected('F12', 'during-runner-outage-postgres.json', mutate)

    def test_f12_http_snapshot_must_expose_original_pending_effect(self):
        def mutate(value):
            for response in value:
                if response['method'] == 'GET' and response['path'].startswith('/v1/runs/'):
                    response['response']['state']['pending_effect']['args_hash'] = '0' * 64
        self.check_mutation_rejected('F12', 'http-observations.json', mutate)


if __name__ == '__main__':
    unittest.main()
