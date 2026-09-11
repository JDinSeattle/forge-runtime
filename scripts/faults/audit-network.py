#!/usr/bin/python3
"""Recompute F10/F11/F12 assertions from retained records; no live mutations."""
import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location('application_audit', Path(__file__).with_name('audit-application.py'))
shared = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared)
instant, nanos = shared.instant, shared.nanos


def read(path):
    return json.loads(path.read_text())


def effects_by_id(rows):
    result = {row['operation_id']: row for row in rows}
    assert len(result) == len(rows), 'duplicate effect identity'
    return result


def state_matches(actual, expected):
    # The SQL lease and serialized snapshot can use different timezone spellings.
    # Compare the entire state, normalizing only the instant in its lease.
    assert {k: v for k, v in actual.items() if k != 'lease'} == {k: v for k, v in expected.items() if k != 'lease'}
    assert actual['lease']['owner'] == expected['lease']['owner']
    assert actual['lease']['epoch'] == expected['lease']['epoch']
    assert instant(actual['lease']['until']) == instant(expected['lease']['until'])


def terminal_transition_matches(snapshot, run, history):
    event, incoming, outgoing = snapshot['input_event'], snapshot['input_state'], snapshot['body']
    assert snapshot['run_id'] == incoming['run_id'] == outgoing['run_id'] == run['id']
    assert snapshot['tenant_id'] == incoming['tenant_id'] == outgoing['tenant_id'] == run['tenant_id']
    assert snapshot['schema_version'] == incoming['schema_version'] == outgoing['schema_version']
    assert event['expected_version'] == incoming['version'] == snapshot['version'] - 1
    assert snapshot['version'] == outgoing['version'] == run['version']
    assert snapshot['step_seq'] == incoming['step_seq'] == outgoing['step_seq']
    assert event['owner'] == incoming['lease']['owner'] == outgoing['lease']['owner']
    assert event['epoch'] == incoming['lease']['epoch'] == outgoing['lease']['epoch']
    assert event['owner'] and event['epoch'] > 0
    assert instant(event['at']) < instant(incoming['lease']['until'])
    assert instant(incoming['lease']['until']) == instant(outgoing['lease']['until'])
    assert instant(event['at']) <= instant(snapshot['created_at'])
    prior = [s for s in history if s['version'] == incoming['version']]
    assert len(prior) == 1
    # Heartbeat can advance the authoritative lease without rewriting a saved
    # reducer snapshot. Every other input-state field must match its predecessor.
    assert {k: v for k, v in incoming.items() if k != 'lease'} == {k: v for k, v in prior[0]['body'].items() if k != 'lease'}
    assert incoming['lease']['epoch'] == prior[0]['body']['lease']['epoch']
    assert incoming['lease']['owner'] == prior[0]['body']['lease']['owner']
    assert instant(incoming['lease']['until']) >= instant(prior[0]['body']['lease']['until'])
    state_matches(outgoing, run['snapshot'])


def audit(directory):
    proof = read(directory / 'acceptance.json')
    mode = directory.name
    assert proof['passed'] and proof['mode'] == mode
    final = read(directory / 'cleanup-released-postgres.json')
    completed = read(directory / 'completed-postgres.json')
    before = read(directory / ('before-cancel-race-postgres.json' if mode.startswith('F10_') else 'before-outage-postgres.json'))
    assert len(final['runs']) == len(before['runs']) == len(completed['runs']) == 1
    run = final['runs'][0]
    run_id, tenant = run['id'], run['tenant_id']
    winner = 'cancelled' if mode == 'F10_cancel_first' else 'completed'
    assert run['state'] == run['snapshot']['status'] == completed['runs'][0]['state'] == proof['terminal_status'] == winner
    for phase in (before, completed):
        for field in ('id', 'tenant_id', 'project_id', 'principal_id', 'parent_run_id', 'task', 'base_commit', 'input_snapshot', 'config_snapshot', 'created_at', 'runner_id', 'workspace_id'):
            assert phase['runs'][0][field] == run[field]
    assert run['version'] == completed['runs'][0]['version']
    api_role = read(directory / 'api-role-preflight.json')
    assert all(api_role[k] is False for k in ('superuser', 'bypassrls', 'create_database', 'create_role', 'member_of_runs_owner'))
    assert api_role['current_user'] == api_role['session_user']
    assert read(directory / 'api-ready.json')['pid'] == proof['api_pid']
    worker_role = read(directory / 'worker-role-preflight.json')
    assert worker_role['superuser'] is False and worker_role['explicit_bypassrls'] is True
    assert worker_role['current_user'] == worker_role['session_user']
    attempts, reservations = final['model_attempts'], final['quota_reservations']
    assert len(attempts) == len(reservations) == 4
    assert len({r['attempt_id'] for r in attempts}) == 4
    assert {r['attempt_id'] for r in attempts} == {r['id'] for r in reservations}
    assert all(r['run_id'] == run_id and r['tenant_id'] == tenant and r['provider'] == 'fake' and r['status'] == 'completed' for r in attempts)
    assert all(r['run_id'] == run_id and r['tenant_id'] == tenant and r['status'] == 'settled' and r['request_slot_released'] for r in reservations)
    quota = final['provider_quotas'][0]
    assert sum(r['actual_tokens'] for r in reservations) == sum(r['actual_microusd'] for r in reservations) == quota['committed_tokens'] == quota['committed_microusd'] == run['snapshot']['cost_microusd'] == 570
    assert all(quota[k] == 0 for k in ('active_requests', 'reserved_tokens', 'reserved_microusd'))
    assert final['tenant_runtime'][0]['active_count'] == final['runners'][0]['reserved_slots'] == 0
    assert all(r['state'] == 'released' for r in final['runner_allocations'])
    assert not any(r['status'] in ('unknown', 'in_flight') for r in final['effects'])
    # Workspace cleanup cannot rewrite a previously settled operation receipt or
    # its immutable dispatch identity, even if aggregate counters still agree.
    assert effects_by_id(completed['effects']) == effects_by_id(final['effects'])
    artifacts = {r['id']: r for r in final['artifacts']}
    for identity, metadata in artifacts.items():
        raw = (directory / 'artifacts' / (identity + '.json')).read_bytes()
        assert metadata['state'] == 'ready' and metadata['tenant_id'] == tenant and metadata['run_id'] == run_id
        assert len(raw) == metadata['byte_size'] and hashlib.sha256(raw).hexdigest() == metadata['sha256']
    op = read(directory / 'original-operation.json')
    target = [r for r in final['effects'] if r['operation_id'] == proof['target_operation']]
    assert len(target) == 1
    target = target[0]
    shared.effect_matches(target, op['request'])
    assert target['epoch'] == proof['target_dispatch_epoch']
    assert target['status'] == op['status'] == 'succeeded'
    assert artifacts[target['receipt_ref']]['sha256'] == op['receipt']['sha256']
    shared.receipt_matches(op, read(directory / 'artifacts' / (target['receipt_ref'] + '.json')))
    assert (directory / 'original-command.stdout').read_bytes() == base64.b64decode(op['result']['output'], validate=True)
    assert json.loads(base64.b64decode(op['result']['output'])) == {'counter': 1}
    docker = [json.loads(line) for line in (directory / 'original-docker-events.jsonl').read_text().splitlines()]
    assert all(row['Actor']['Attributes']['name'] == op['job_id'] for row in docker)
    assert sum(row['Action'] == 'create' for row in docker) == sum(row['Action'] == 'start' for row in docker) == 1
    verification = read(directory / 'artifacts' / (run['snapshot']['verification_report_ref'] + '.json'))
    assert all(verification['evidence'][k] is True for k in ('trusted', 'baseline_target_failed', 'target_passed', 'regression_passed'))
    assert verification['evidence']['workspace_revision'] == run['snapshot']['workspace_revision']
    assert all(ref in artifacts for ref in verification['receipts'])
    assert len(final['workspace_cleanup']) == 1
    cleanup = final['workspace_cleanup'][0]
    assert cleanup['phase'] == proof['cleanup']['phase'] == 'released'
    assert cleanup['id'] == proof['cleanup']['id'] and cleanup['terminal_version'] == run['version']
    assert instant(artifacts[cleanup['snapshot_ref']]['created_at']) <= instant(cleanup['released_at'])
    events = final['run_events']
    assert sorted(e['seq'] for e in events) == list(range(1, run['next_event_seq']))
    transitions = [s['input_event'] for s in final['run_snapshots'] if s['input_event']]
    terminal_snapshots = [s for s in final['run_snapshots'] if s['body']['status'] in ('completed', 'cancelled', 'failed', 'budget_exhausted')]
    assert len(terminal_snapshots) == 1 and terminal_snapshots[0]['version'] == run['version']
    http = read(directory / 'http-observations.json')
    pause = read(directory / 'worker1-paused.json')
    assert pause['pid'] == read(directory / 'worker1-ready.json')['pid'] == proof['worker1_pid']
    extra = {}
    if mode.startswith('F10_'):
        assert before['runs'][0]['version'] == proof['race_boundary_version']
        assert before['runs'][0]['state'] == 'running' and before['runs'][0]['snapshot']['stage'] == 'finalize'
        assert pause['phase'] == 'final_diff_durable_before_driver_return'
        pending = pause['operation']
        effect = next(e for e in final['effects'] if e['operation_id'] == pending['request']['operation_id'])
        assert effect['kind'] == 'get_diff' and effect['status'] == 'succeeded'
        shared.effect_matches(effect, pending['request'])
        shared.receipt_matches(pending, read(directory / 'artifacts' / (effect['receipt_ref'] + '.json')))
        assert artifacts[effect['receipt_ref']]['sha256'] == pending['receipt']['sha256']
        before_effects = effects_by_id(before['effects'])
        final_effects = effects_by_id(final['effects'])
        assert before_effects.keys() == final_effects.keys()
        for operation_id, previous in before_effects.items():
            if operation_id == pending['request']['operation_id']:
                assert previous['status'] == 'in_flight' and previous['receipt_ref'] is None
                shared.effect_matches(previous, pending['request'])
            else:
                assert previous['status'] in ('succeeded', 'failed', 'cancelled', 'skipped')
                assert previous == final_effects[operation_id], 'settled effect changed during cancellation/finalization'
        terminal = terminal_snapshots[0]
        terminal_transition_matches(terminal, run, final['run_snapshots'])
        assert terminal['input_state']['stage'] == 'finalize' and terminal['body']['stage'] == 'stopped'
        cancellations = [e for e in transitions if e['kind'] == 'cancel_requested']
        finalized = [e for e in transitions if e['kind'] == 'finalized']
        replies = [h for h in http if h['path'] == '/v1/runs/' + run_id + '/cancel']
        assert len(replies) == (3 if winner == 'cancelled' else 2)
        assert all(h['status'] == 202 and h['response']['id'] == run_id for h in replies)
        assert all(h['response']['state']['status'] == winner and h['response']['state']['version'] == run['version'] for h in replies[-2:])
        if winner == 'cancelled':
            intermediate = read(directory / 'cancel-committed-before-finalization-return-postgres.json')['runs'][0]
            assert intermediate['state'] == replies[0]['response']['state']['status'] == 'cancel_requested'
            assert intermediate['version'] == before['runs'][0]['version'] + 1
            assert len(cancellations) == 1 and not finalized
            stop = terminal_snapshots[0]['input_event']
            assert stop['kind'] == 'cancellation_confirmed'
            assert stop['expected_version'] == intermediate['version'] == proof['race_boundary_version'] + 1
            assert terminal['input_state']['status'] == 'cancel_requested'
            state_matches(terminal['input_state'], intermediate['snapshot'])
            assert instant(pause['at']) < instant(cancellations[0]['at']) < instant(stop['at'])
            assert stop['stop']['no_active_operations'] is True
            assert stop['stop']['ref'] == terminal['body']['output_ref']
            assert stop['stop']['ref'] in artifacts
            receipt = read(directory / 'artifacts' / (stop['stop']['ref'] + '.json'))
            assert receipt['no_active_operations'] is True and receipt['workspace']['stopped'] is True
            assert receipt['workspace']['run_id'] == run_id and receipt['workspace']['tenant_id'] == tenant
            assert receipt['workspace']['id'] == run['workspace_id']
            assert receipt['workspace']['epoch'] == stop['epoch']
            assert receipt['workspace']['revision'] == stop['stop']['workspace_revision'] == terminal['input_state']['workspace_revision'] == terminal['body']['workspace_revision']
        else:
            assert not cancellations and len(finalized) == 1
            assert finalized[0] == terminal['input_event']
            assert finalized[0]['expected_version'] == proof['race_boundary_version']
            assert terminal['input_state']['status'] == 'running'
            state_matches(terminal['input_state'], before['runs'][0]['snapshot'])
            assert finalized[0]['output_ref'] == terminal['body']['output_ref'] and finalized[0]['output_ref'] in artifacts
            assert instant(pause['at']) < instant(finalized[0]['at']) < min(instant(h['at']) for h in replies)
        extra['terminal_winner'] = winner
    else:
        assert pause['phase'] == 'original_start_accepted_before_driver_return'
        assert pause['operation']['request'] == op['request']
        assert op == read(directory / 'original-before-outage.json')
        previous = next(e for e in before['effects'] if e['operation_id'] == target['operation_id'])
        assert previous['status'] == 'in_flight' and previous['receipt_ref'] is None
        shared.effect_matches(previous, op['request'])
        during = read(directory / ('during-database-outage-postgres.json' if mode == 'F11' else 'during-runner-outage-postgres.json'))
        assert len(during['runs']) == 1
        r = during['runs'][0]
        for field in ('id', 'tenant_id', 'project_id', 'principal_id', 'task', 'base_commit', 'input_snapshot', 'config_snapshot', 'runner_id', 'workspace_id'):
            assert r[field] == run[field]
        assert r['lease_epoch'] == proof['original_epoch']
        assert instant(r['lease_until']) == instant(proof['original_lease_until'])
        original = next(e for e in during['effects'] if e['operation_id'] == target['operation_id'])
        shared.effect_matches(original, op['request'])
        claims = [e for e in transitions if e['kind'] == 'claimed' and e['epoch'] > r['lease_epoch']]
        claim = min(claims, key=lambda e: e['epoch'])
        assert claim['owner'] == claim['lease']['owner'] == 'worker2-0'
        assert claim['epoch'] == claim['lease']['epoch'] == proof['first_replacement_epoch'] == r['lease_epoch'] + 1
        assert instant(claim['at']) == instant(proof['replacement_claim_db_time']) >= instant(r['lease_until'])
        assert proof['worker1_pid'] != proof['worker2_pid'] == read(directory / 'worker2-ready.json')['pid']
        if mode == 'F11':
            # The cut applies to every application DB connection. Readiness and
            # request failures cannot coexist with invented durable progress.
            # These captured phases are identical across every exported table.
            assert before == during, 'durable PostgreSQL state changed while database proxy was cut'
            cut, restore = read(directory / 'database-proxy-events.json')
            assert cut == proof['database_cut'] and restore == proof['database_restore']
            assert cut['action'] == 'cut_all_and_refuse_new' and cut['connections'] > 0 and restore['action'] == 'restore'
            start, end = instant(cut['at']), instant(restore['at'])
            assert start < instant(proof['worker1_sigkill_at']) < end < instant(claim['at'])
            assert all(not (nanos(cut['at']) <= e['timeNano'] <= nanos(restore['at'])) for e in docker if e['Action'] in ('create', 'start'))
            assert original['status'] == 'in_flight' and original['receipt_ref'] is None
            assert before['effects'] == during['effects'] and before['model_attempts'] == during['model_attempts']
            assert sum(e['type'] == 'run.created' for e in during['run_events']) == 1
            failed = [h for h in http if start <= instant(h['at']) <= end]
            assert {h['method'] for h in failed} == {'POST', 'GET'} and all(500 <= h['status'] <= 599 for h in failed)
            assert any(h['path'] == '/v1/projects/' + run['project_id'] + '/runs' for h in failed)
            assert any(h['status'] == 200 and h['response'].get('id') == run_id and instant(h['at']) > end for h in http)
            extra['database_cut_seconds'] = (end - start).total_seconds()
        elif mode == 'F12':
            assert r['state'] == 'needs_reconciliation' and original['status'] == 'unknown' and original['receipt_ref'] is None
            assert before['runner_allocations'] == during['runner_allocations']
            assert len(during['runner_allocations']) == len(during['tenant_runtime']) == len(during['runners']) == 1
            allocation = during['runner_allocations'][0]
            assert allocation['tenant_id'] == during['tenant_runtime'][0]['tenant_id'] == tenant
            assert allocation['run_id'] == run_id
            assert allocation['runner_id'] == during['runners'][0]['id'] == r['runner_id']
            assert allocation['lease_epoch'] == r['lease_epoch'] == op['request']['epoch']
            assert allocation['slots'] == 1 and allocation['state'] == 'reserved'
            assert during['tenant_runtime'][0]['active_count'] == during['runners'][0]['reserved_slots'] == 1
            assert proof['runner1_pid'] != proof['runner2_pid']
            start, end = instant(proof['runner_sigkill_at']), instant(proof['worker1_sigkill_at'])
            observed = [h for h in http if start < instant(h['at']) < end]
            assert [(h['method'], h['status']) for h in observed] == [('GET', 200), ('GET', 200), ('POST', 202)]
            snapshot = observed[1]['response']
            assert snapshot['id'] == run_id and snapshot['runner_id'] == r['runner_id'] and snapshot['state']['status'] == r['state']
            assert observed[1]['path'] == '/v1/runs/' + run_id
            assert snapshot['tenant_id'] == tenant
            authoritative = dict(r['snapshot'], lease={'owner': r['lease_owner'], 'epoch': r['lease_epoch'], 'until': r['lease_until']})
            state_matches(snapshot['state'], authoritative)
            pending = snapshot['state']['pending_effect']
            for field in ('operation_id', 'kind', 'args', 'args_hash', 'expected_revision', 'policy_version'):
                assert pending[field] == original[field] == op['request'][field]
            assert pending['dispatch_epoch'] == original['epoch'] == op['request']['epoch']
            assert pending['status'] == original['status'] == 'unknown'
            assert pending['requires_approval'] is True
            approval = snapshot['state']['approval']
            assert approval['granted'] is True and approval['effect_id'] == pending['operation_id']
            assert approval['args_hash'] == pending['args_hash'] and approval['workspace_revision'] == pending['expected_revision']
            assert approval['policy_version'] == pending['policy_version']
            stream = read(directory / 'sse-during-runner-outage.json')
            by_seq = {e['seq']: e for e in during['run_events']}
            assert [e['seq'] for e in stream] == list(range(1, len(stream) + 1))
            for event in stream:
                saved = by_seq[event['seq']]
                assert all(event[k] == saved[k] for k in ('run_id', 'type', 'schema_version', 'payload'))
                assert instant(event['created_at']) == instant(saved['created_at'])
            message = stream[-1]
            assert message['type'] == 'run.message_added' and start < instant(message['created_at']) < end
            assert message['payload'] == observed[-1]['response']['message']
            extra['sse_during_outage'] = len(stream)
        else:
            raise AssertionError('unexpected case')
    return {'case': mode, 'passed': True, 'run_id': run_id, 'artifacts_checked': len(artifacts), 'events_after_cleanup': len(events), 'synthetic_microusd': 570, **extra}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    result = [audit(args.directory / mode) for mode in ('F10_cancel_first', 'F10_complete_first', 'F11', 'F12')]
    print(json.dumps({'passed': True, 'scope': 'Independent saved-record recomputation, not another OS execution. Phase PG captures are sequential reads; historical process signals and SQLite counts remain harness observations. Models and prices are synthetic.', 'cases': result}, indent=2))


if __name__ == '__main__':
    main()
