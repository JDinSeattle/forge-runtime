import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('application_audit', HERE / 'audit-application.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
FIXTURE = HERE.parents[1] / 'benchmarks/results/application-faults-20260911T194600Z/F08'
CONTINUATION = HERE.parents[1] / 'benchmarks/results/application-continuation-20260911T200939Z'


class EvidenceAuditTests(unittest.TestCase):
    def test_actual_continuation_evidence_is_accepted(self):
        for mode in ('F02', 'F06', 'F09'):
            with self.subTest(mode=mode):
                result = audit.audit(CONTINUATION / mode, mode)
                self.assertEqual(result['case'], mode)
                self.assertEqual(result['synthetic_cost_microusd'], 570)

    def test_f02_replaced_task_after_claim_is_rejected(self):
        original_read = audit.read

        def changed(path):
            value = original_read(path)
            if path == CONTINUATION / 'F02/cleanup-released-postgres.json':
                value['runs'][0]['task'] = 'REPLACED TASK after the original claim'
            return value

        with patch.object(audit, 'read', side_effect=changed):
            with self.assertRaises(AssertionError):
                audit.audit(CONTINUATION / 'F02', 'F02')

    def test_f06_summary_hashes_and_revision_must_match_archived_patch_receipt(self):
        original_read = audit.read

        def changed(path):
            value = original_read(path)
            if path in (CONTINUATION / 'F06/patch-before-death.json', CONTINUATION / 'F06/patch-after-recovery.json'):
                # Internally consistent summaries, while the immutable receipt
                # bytes still prove the real hashes and revision 3 -> 4.
                value['request']['expected_revision'] = 103
                value['after_revision'] = 104
                value['before_hash'] = '0' * 64
                value['after_hash'] = value['expected_after_hash'] = '1' * 64
            return value

        with patch.object(audit, 'read', side_effect=changed):
            with self.assertRaises(AssertionError):
                audit.audit(CONTINUATION / 'F06', 'F06')

    def test_f09_original_approval_must_match_the_actual_resumed_action(self):
        original_read = audit.read

        def changed(path):
            value = original_read(path)
            if path in (CONTINUATION / 'F09/before-worker-death-postgres.json', CONTINUATION / 'F09/approval-after-restart-postgres.json'):
                # Restart snapshots agree with each other, but this fabricated
                # pending approval authorizes different args from the real job.
                value['approvals'][0]['args_hash'] = '0' * 64
                value['runs'][0]['snapshot']['approval']['args_hash'] = '0' * 64
            return value

        with patch.object(audit, 'read', side_effect=changed):
            with self.assertRaises(AssertionError):
                audit.audit(CONTINUATION / 'F09', 'F09')

    def test_actual_writer_evidence_crosses_expiry_and_orders_container_death(self):
        result = audit.audit(FIXTURE, 'F08')
        self.assertGreater(result['old_write_after_expiry_ns'], 0)
        self.assertGreater(result['new_write_after_old_die_ns'], 0)

    def test_summary_with_both_writers_before_expiry_is_rejected(self):
        original_read = audit.read
        proof = copy.deepcopy(original_read(FIXTURE / 'acceptance.json'))
        expiry = audit.nanos(proof['original_lease_until'])
        proof['old_writer_ns'] = [expiry - 2_000_000_000, expiry - 1_000_000_000]
        proof['new_writer_first_ns'] = expiry - 500_000_000

        def changed(path):
            return proof if path == FIXTURE / 'acceptance.json' else original_read(path)

        with patch.object(audit, 'read', side_effect=changed):
            with self.assertRaises(AssertionError):
                audit.audit(FIXTURE, 'F08')


if __name__ == '__main__':
    unittest.main()
