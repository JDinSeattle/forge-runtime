#!/usr/bin/env python3
"""Independently audit the immutable P02 report and every compressed raw sample.

Usage: python3 benchmarks/audit_sse_p02.py benchmarks/results/<p02-directory>
No database, network access or report mutation is involved. The separate
independent-audit.json records recomputed facts and the raw files' SHA256 values.
"""
import argparse
import collections
import datetime
import gzip
import hashlib
import json
import math
from pathlib import Path
import re


def require(condition, detail):
    if not condition:
        raise AssertionError(detail)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        while chunk := source.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def time_ns(value):
    match = re.fullmatch(r'(.*T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})', value)
    require(match is not None, ('invalid timestamp', value))
    base, fraction, offset = match.groups()
    seconds = int(datetime.datetime.fromisoformat(base + offset.replace('Z', '+00:00')).timestamp())
    return seconds * 1_000_000_000 + int((fraction or '').ljust(9, '0'))


def records(path):
    with gzip.open(path, 'rt') as source:
        for line in source:
            yield json.loads(line)


def distribution(values):
    values.sort()
    return {
        'count': len(values),
        'min_ms': values[0],
        'p50_ms': values[math.ceil(len(values) * .5) - 1],
        'p95_ms': values[math.ceil(len(values) * .95) - 1],
        'p99_ms': values[math.ceil(len(values) * .99) - 1],
        'max_ms': values[-1],
    }


def audit(directory):
    report = json.loads((directory / 'report.json').read_text())
    config, invariant = report['configuration'], report['invariants']
    ticks = config['actual_ticks_per_run']
    failures = []
    if not invariant['passed']:
        failures.append('original frozen acceptance failed')
    for key, expected in {'runs': 20, 'connections': 200, 'healthy': 180, 'slow': 20, 'per_run_hz': 5,
                          'stored_payload_bytes': 1024, 'production_poll_ms': 100,
                          'production_queue_size': 128, 'production_history_size': 512,
                          'production_write_deadline_seconds': 5}.items():
        require(config[key] == expected, (key, config[key]))
    require(config['server_socket_buffers'] == 'unmodified defaults', 'server socket tuning')
    require(config['healthy_client_socket_buffers'] == 'unmodified defaults', 'healthy socket tuning')
    require(config['slow_client_requested_receive_bytes'] == 4096, 'slow receive-window declaration')
    require(config['actual_publish_seconds'] <= ticks / 5 + 1, 'scheduled duration bound')
    clients = {client['client']: client for client in report['clients']}
    require(len(clients) == 200, 'not exactly 200 unique original clients')
    require(sum(c['slow'] for c in clients.values()) == 20, 'slow client count')
    for i, client in clients.items():
        require(client['run_index'] == i // 10 and client['slow'] == (i % 10 == 9), ('client topology', i))
        require(client['connected'] and not client.get('error'), ('original client failed', i))
        if not client['slow']:
            require(client['received'] == ticks and client['last_seq'] == client['initial_seq'] + ticks,
                    ('incomplete original cursor', i))
    # First verify the real 5Hz schedule and that each run publishes every tick.
    publications = collections.Counter()
    committed = {}
    origin = None
    maximum_lateness = 0
    for row in records(directory / 'publication.jsonl.gz'):
        run, tick = row['run_index'], row['tick']
        require(0 <= run < 20 and not row.get('error'), ('publication error', row))
        publications[run] += 1
        require(tick == publications[run], ('publication tick gap or duplicate', run, tick))
        scheduled = time_ns(row['scheduled_at'])
        if origin is None:
            origin = scheduled - tick * 200_000_000
        require(scheduled == origin + tick * 200_000_000, ('not absolute 5Hz schedule', run, tick))
        maximum_lateness = max(maximum_lateness, row['schedule_lateness_ms'])
        committed[run, tick] = time_ns(row['committed_at'])
    require(len(publications) == 20 and set(publications.values()) == {ticks}, 'incomplete publication set')
    require(maximum_lateness < 1000, 'schedule lateness >=1s')
    require(abs(maximum_lateness - config['max_schedule_lateness_ms']) < 1e-6, 'lateness summary mismatch')
    # Inspect every healthy sample, not only final aggregate counters.
    delivered = collections.Counter()
    creation_times = {}
    latencies = []
    for row in records(directory / 'deliveries.jsonl.gz'):
        client, run, tick = row['client'], row['run'], row['tick']
        require(client in clients and not clients[client]['slow'] and run == client // 10, ('healthy identity', row))
        delivered[client] += 1
        require(tick == delivered[client] and row['seq'] == clients[client]['initial_seq'] + tick,
                ('healthy duplicate or sequence gap', client, tick))
        require(1 <= tick <= ticks, ('unexpected healthy tick', tick))
        previous = creation_times.setdefault((run, tick), row['created_unix_ns'])
        require(previous == row['created_unix_ns'], ('inconsistent event timestamp', run, tick))
        require(row['created_unix_ns'] <= committed[run, tick], ('event created after committed timestamp', run, tick))
        ms = (row['received_unix_ns'] - row['created_unix_ns']) / 1e6
        require(abs(ms - row['latency_ms']) < 1e-6, ('latency mismatch', client, tick))
        latencies.append(ms)
    require(len(delivered) == 180 and set(delivered.values()) == {ticks}, 'incomplete healthy raw set')
    measured = distribution(latencies)
    for key, value in measured.items():
        require(abs(value - report['healthy_latency'][key]) < 1e-6, ('latency distribution mismatch', key))
    require(measured['p95_ms'] < 1000, 'healthy p95>=1s')
    require(len(creation_times) == 20 * ticks, 'not every durable event observed')
    # Reconnect evidence must contain every formerly missed event, separately.
    resumed = {client['client']: client for client in report['slow_reconnect']['clients']}
    require(set(resumed) == {i for i in clients if clients[i]['slow']}, 'resumed wrong clients')
    replayed = collections.Counter()
    for row in records(directory / 'slow-resume.jsonl.gz'):
        client, run, tick = row['client'], row['run'], row['tick']
        require(client in resumed and run == client // 10, ('replay identity', row))
        replayed[client] += 1
        require(tick == replayed[client] and row['seq'] == clients[client]['initial_seq'] + tick,
                ('replay duplicate or gap', client, tick))
        require(row['created_unix_ns'] == creation_times[run, tick], ('replayed different event', client, tick))
    require(len(replayed) == 20 and set(replayed.values()) == {ticks}, 'incomplete replay raw set')
    for i, client in resumed.items():
        require(client['connected'] and not client.get('error') and client['received'] == ticks
                and client['initial_seq'] == clients[i]['initial_seq']
                and client['last_seq'] == clients[i]['initial_seq'] + ticks, ('replay cursor mismatch', i))
    slow = report['slow_observation']
    require(slow['tcp_closes_before_harness_close'] == 20 and slow['handler_returns_before_harness_close'] == 20,
            'actual 20-client slow close not proven')
    if slow['slow_subscriber_metric'] != 20:
        failures.append('slow_subscriber metric is ' + str(slow['slow_subscriber_metric']) + ', expected 20')
    require(ticks - slow['all_slow_closed_observed_at_tick'] >= 50, 'missing post-close committed tail')
    hint = report['notification_fault']
    require(hint['positive_hints_committed'] == hint['notifications_received'] == 200, 'NOTIFY positive control')
    require(hint['hint_transactions_rolled_back_after_event_commit'] == 20 * (ticks - 10)
            and hint['dropped_hints_unexpectedly_received'] == 0 and not hint['listener_error'], 'NOTIFY loss proof')
    require(invariant['durable_events'] == 20 * ticks and invariant['durable_gaps'] == 0
            and invariant['payload_min_bytes'] == invariant['payload_max_bytes'] == 1024
            and invariant['all_api_cursors_match'] and invariant['publish_errors'] == 0
            and invariant['tcp_after_cleanup'] == invariant['handlers_after_cleanup'] == 0, 'durable/cleanup invariants')
    source_checks = {}
    for name, expected in report['manifest']['source_sha256'].items():
        actual = sha256(directory / 'source-snapshot' / (name + '.txt'))
        require(actual == expected, ('source snapshot mismatch', name))
        source_checks[name] = actual
    provenance = json.loads((directory / 'execution-provenance.json').read_text())
    for name, fact in provenance['sources'].items():
        require(fact['last_modified_before_binary'] and fact['sha256'] == source_checks[name], ('execution provenance', name))
    hashes = {name: sha256(directory / name) for name in ('report.json', 'deliveries.jsonl.gz', 'publication.jsonl.gz', 'slow-resume.jsonl.gz')}
    return {'scope': 'Independent audit of every compressed raw delivery, publication and replay; no database changes or network access',
            'passed': not failures, 'failures': failures, 'ticks_per_run': ticks, 'durable_events': 20 * ticks, 'healthy_deliveries': len(latencies),
            'replayed_events': sum(replayed.values()), 'healthy_clients': len(delivered), 'reconnected_clients': len(replayed),
            'healthy_latency': measured, 'maximum_schedule_lateness_ms': maximum_lateness,
            'exact_scheduled_interval_ns': 200_000_000, 'actual_per_run_hz': ticks / config['actual_publish_seconds'],
            'events_per_run_after_observed_all_slow_close': ticks - slow['all_slow_closed_observed_at_tick'],
            'slow_tcp_closed_before_cancellation': 20, 'lost_optional_hints': 20 * (ticks - 10),
            'audit_source_sha256': sha256(Path(__file__)), 'original_report_and_raw_sha256': hashes, 'source_snapshot_sha256': source_checks,
            'limitations': 'Finite client-window/default-server local workload. Production fanout is polling-only; notification rollback is explicitly fixture-only. No paid model, container, outage or retention-deletion claim.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    result = audit(args.directory)
    (args.directory / 'independent-audit.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
