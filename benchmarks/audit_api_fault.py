#!/usr/bin/env python3
"""Offline audit of F01's raw response-loss/explicit-retry and durable snapshots."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re


def require(value, why):
    if not value:
        raise AssertionError(why)


def time_ns(value):
    match = re.fullmatch(r'(.*T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})', value)
    require(match is not None, ('timestamp', value))
    base, fraction, offset = match.groups()
    seconds = int(datetime.datetime.fromisoformat(base + offset.replace('Z', '+00:00')).timestamp())
    return seconds * 1_000_000_000 + int((fraction or '').ljust(9, '0'))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(directory):
    report = json.loads((directory / 'report.json').read_text())
    require(report['invariants']['passed'] and not report['problems'], 'original report is not passing')
    first, retry, conflict, forbidden = report['attempts']
    proxy, request = report['proxy_fault'], report['request']
    require(first['http_status'] == 0 and not first['http_response_received']
            and first['client_error'] and not first.get('response_body'), 'first client received acceptance')
    body = json.dumps(request['body'], ensure_ascii=False, separators=(',', ':')).encode()
    require(hashlib.sha256(body).hexdigest() == first['request_sha256'] == retry['request_sha256'], 'retry body differs')
    require(first['tenant'] == retry['tenant'] == request['tenant'], 'retry tenant differs')
    run = proxy['upstream_body']['run_id']
    require(proxy['upstream_status'] == 202 and proxy['upstream_body']['reused'] is False
            and proxy['client_socket_closed_without_http_response'] and not proxy.get('fixture_error'), 'incorrect injection boundary')
    require(retry['http_status'] == 202 and retry['http_response_received'] and not retry.get('client_error')
            and retry['response_body'] == {'run_id': run, 'reused': True}
            and retry['response_location'] == proxy['upstream_location'] == '/v1/runs/' + run, 'retry did not reuse acceptance')
    require(conflict['http_status'] == 409 and conflict['request_sha256'] != first['request_sha256'], 'different-body key conflict')
    require(forbidden['http_status'] == 403 and forbidden['tenant'] != first['tenant']
            and forbidden['request_sha256'] == first['request_sha256'], 'unrelated tenant boundary')
    require(report['calls_after_first_client_failure'] == {'proxy': 1, 'upstream': 1}, 'hidden initial transport replay')
    require(report['calls_at_end'] == {'proxy': 4, 'upstream': 4}, 'unexpected extra calls')
    snapshots = [proxy['durable_proof_before_connection_close'], report['pg_after_first_client_failure'],
                 report['pg_after_retry'], report['pg_final']]
    immutable = None
    for i, snapshot in enumerate(snapshots):
        require(len(snapshot['runs']) == len(snapshot['events']) == len(snapshot['idempotency_keys']) == 1,
                ('creation/key/event duplicated', i))
        require(snapshot['effects'] == snapshot['model_attempts'] == snapshot['runner_allocations'] == 0,
                ('unexpected execution', i))
        row, event, key = snapshot['runs'][0], snapshot['events'][0], snapshot['idempotency_keys'][0]
        require(row['id'] == event['run_id'] == key['resource_id'] == key['response']['run_id'] == run, ('run identity', i))
        require(row['tenant_id'] == event['tenant_id'] == key['tenant_id'] == request['tenant'], ('tenant identity', i))
        require(row['principal_id'] == key['principal_id'] == request['principal'], ('principal identity', i))
        require(key['route'] == request['route'] == '/v1/projects/' + row['project_id'] + '/runs'
                and key['key'] == request['key'], ('key/route identity', i))
        require(re.fullmatch('[0-9a-f]{64}', key['request_hash']) is not None, ('bound request hash', i))
        require(row['state'] == 'queued' and row['version'] == 1 and row['lease_epoch'] == 0
                and row['runner_id'] is None and row['covered_seq'] == event['seq'] == 1
                and event['type'] == 'run.created' and event['payload']['run_id'] == run
                and event['payload']['tenant_id'] == request['tenant'], ('initial durable state', i))
        normalized = {key: value for key, value in snapshot.items() if key != 'captured_at'}
        if immutable is None:
            immutable = normalized
        require(normalized == immutable, ('records changed after retry or rejected controls', i))
    times = [first['started_at'], proxy['upstream_response_at'],
             snapshots[0]['captured_at'], proxy['durable_proof_at'],
             proxy['client_connection_closed_at'], first['finished_at'],
             report['pg_after_first_client_failure']['captured_at'], retry['started_at'],
             retry['finished_at'], report['pg_after_retry']['captured_at'],
             conflict['started_at'], conflict['finished_at'], forbidden['started_at'],
             forbidden['finished_at'], report['pg_final']['captured_at']]
    parsed = [time_ns(value) for value in times]
    require(parsed == sorted(parsed), 'commit proof / close / failure / explicit retry timestamps out of order')
    source_hashes = {}
    for source, expected in report['manifest']['source_sha256'].items():
        actual = digest(directory / 'source-snapshot' / (source + '.txt'))
        require(actual == expected, ('source snapshot mismatch', source))
        source_hashes[source] = actual
    require('frg_' not in (directory / 'report.json').read_text(), 'bearer token leaked into report')
    return {'passed': True, 'scope': 'Independent raw request/response, timestamp and all four PostgreSQL snapshot checks; no network or database access',
            'run_id': run, 'snapshots_checked': len(snapshots), 'unchanged_runs': 1,
            'unchanged_run_created_events': 1, 'unchanged_bound_idempotency_keys': 1,
            'model_attempts_effects_allocations_each': 0,
            'initial_http_response_received': False, 'retry_status': 202, 'retry_reused_same_run': True,
            'different_body_status': 409, 'unrelated_tenant_status': 403,
            'initial_upstream_calls': 1, 'total_upstream_calls': 4,
            'ordered_timestamp_checks': len(parsed),
            'initial_client_duration_ms': (time_ns(first['finished_at']) - time_ns(first['started_at'])) / 1e6,
            'explicit_retry_duration_ms': (time_ns(retry['finished_at']) - time_ns(retry['started_at'])) / 1e6,
            'proof_to_recorded_socket_close_ms': (time_ns(proxy['client_connection_closed_at']) - time_ns(proxy['durable_proof_at'])) / 1e6,
            'request_body_sha256': first['request_sha256'], 'report_sha256': digest(directory / 'report.json'),
            'source_snapshot_sha256': source_hashes, 'audit_source_sha256': digest(Path(__file__)),
            'limitations': 'One fixture-injected after-commit/before-response transport cut through real HTTP/PG; no production proxy change, worker/runner/model execution or general packet-loss-rate claim.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    result = audit(args.directory)
    (args.directory / 'independent-audit.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
