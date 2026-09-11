#!/usr/bin/python3
"""Independently cross-check retained application-fault evidence; no services or DB writes."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path


def read(path):
    return json.loads(path.read_text())


def instant(value):
    return datetime.datetime.fromisoformat(value)


def audit(directory, mode):
    proof = read(directory / 'acceptance.json')
    final = read(directory / 'cleanup-released-postgres.json')
    before = read(directory / 'before-worker-death-postgres.json')
    dead = read(directory / 'after-worker1-sigkill-postgres.json')
    operation = read(directory / 'original-operation.json')
    role = read(directory / 'worker-role-preflight.json')
    run = final['runs'][0]
    run_id, tenant = run['id'], run['tenant_id']
    assert len(final['runs']) == 1
    assert run['state'] == run['snapshot']['status'] == proof['terminal_status'] == 'completed'
    assert run['snapshot']['verification_status'] == proof['verification'] == 'verified'
    assert role['superuser'] is False and role['explicit_bypassrls'] is True
    assert role['current_user'] == role['session_user']
    assert all(role[key] is False for key in ('create_role', 'create_database', 'create_schema', 'member_of_runs_owner'))
    assert proof['worker1_pid'] != proof['worker2_pid']
    assert all(before[table][0][field] == 1 for table, field in (('tenant_runtime', 'active_count'), ('runners', 'reserved_slots')))
    lease = dead['runs'][0]
    assert lease['lease_epoch'] == proof['original_epoch']
    assert instant(lease['lease_until']) == instant(proof['original_lease_until'])
    claims = [row['input_event'] for row in final['run_snapshots'] if (row['input_event'] or {}).get('kind') == 'claimed' and row['input_event']['epoch'] > proof['original_epoch']]
    claim = min(claims, key=lambda event: event['epoch'])
    assert instant(claim['at']) == instant(proof['replacement_claim_db_time'])
    assert instant(claim['at']) >= instant(lease['lease_until']) > instant(proof['worker1_sigkill_at'])
    attempts = final['model_attempts']
    reservations = final['quota_reservations']
    assert len(attempts) == len(reservations) == 4
    assert {row['attempt_id'] for row in attempts} == {row['id'] for row in reservations}
    assert all(row['provider'] == 'fake' and row['status'] == 'completed' for row in attempts)
    assert all(row['status'] == 'settled' and row['request_slot_released'] for row in reservations)
    cost = sum(row['actual_microusd'] for row in reservations)
    tokens = sum(row['actual_tokens'] for row in reservations)
    quota = final['provider_quotas'][0]
    assert cost == tokens == quota['committed_microusd'] == quota['committed_tokens'] == run['snapshot']['cost_microusd'] == 570
    assert all(quota[key] == 0 for key in ('active_requests', 'reserved_microusd', 'reserved_tokens'))
    assert final['tenant_runtime'][0]['active_count'] == final['runners'][0]['reserved_slots'] == 0
    target = [row for row in final['effects'] if row['operation_id'] == proof['target_operation']]
    assert len(target) == 1
    target = target[0]
    assert target['epoch'] == operation['request']['epoch'] == proof['original_epoch']
    assert target['args_hash'] == operation['request']['args_hash']
    assert target['status'] == operation['status'] == ('cancelled' if mode == 'F08' else 'succeeded')
    artifacts = {row['id']: row for row in final['artifacts']}
    assert artifacts[target['receipt_ref']]['sha256'] == operation['receipt']['sha256']
    for identity, metadata in artifacts.items():
        raw = (directory / 'artifacts' / (identity + '.json')).read_bytes()
        assert metadata['state'] == 'ready' and metadata['tenant_id'] == tenant and metadata['run_id'] == run_id
        assert hashlib.sha256(raw).hexdigest() == metadata['sha256'] and len(raw) == metadata['byte_size']
    verification = read(directory / 'artifacts' / (run['snapshot']['verification_report_ref'] + '.json'))
    assert all(verification['evidence'][key] is True for key in ('trusted', 'baseline_target_failed', 'target_passed', 'regression_passed'))
    assert verification['evidence']['workspace_revision'] == run['snapshot']['workspace_revision']
    assert all(identity in artifacts for identity in verification['receipts'])
    cleanup = final['workspace_cleanup'][0]
    assert cleanup['phase'] == proof['cleanup']['phase'] == 'released'
    snapshot = artifacts[cleanup['snapshot_ref']]
    assert instant(snapshot['created_at']) <= instant(cleanup['released_at'])
    events = sorted(row['seq'] for row in final['run_events'])
    assert events == list(range(1, run['next_event_seq']))
    docker = [json.loads(line) for line in (directory / 'original-docker-events.jsonl').read_text().splitlines()]
    assert all(event['Actor']['Attributes']['name'] == operation['job_id'] for event in docker)
    assert sum(event['Action'] == 'create' for event in docker) == sum(event['Action'] == 'start' for event in docker) == 1
    extra = {}
    if mode == 'F07':
        assert before['runs'][0]['state'] == 'needs_reconciliation'
        assert any(row['operation_id'] == proof['target_operation'] and row['status'] == 'unknown' for row in before['effects'])
    if mode == 'F08':
        times = proof['old_writer_ns']
        assert times and all(a < b for a, b in zip(times, times[1:]))
        assert times[-1] < proof['new_writer_first_ns']
        extra = {'old_writer_samples': len(times), 'writer_gap_ns': proof['new_writer_first_ns'] - times[-1]}
    else:
        assert proof['counter'] == '1\n'
    return {'case': mode, 'run_id': run_id, 'ready_artifacts_checked': len(artifacts), 'events_after_cleanup': len(events), 'synthetic_cost_microusd': cost, 'claim_after_expiry_seconds': (instant(claim['at']) - instant(lease['lease_until'])).total_seconds(), **extra}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    results = [audit(args.directory / mode, mode) for mode in ('F05', 'F07', 'F08')]
    print(json.dumps({'passed': True, 'scope': 'Independent recomputation from retained fixture records; not a second execution or independent OS signal observer. Phase exports are sequential table reads, not atomic database snapshots. Model costs are synthetic.', 'cases': results}, indent=2))


if __name__ == '__main__':
    main()
